package usp_gmail

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- service account key parsing --------------------------------------------

func TestParseServiceAccountKey(t *testing.T) {
	validJSON, _ := generateServiceAccount(t)

	t.Run("parses a valid key and its RSA private key", func(t *testing.T) {
		key, rsaKey, err := parseServiceAccountKey([]byte(validJSON))
		require.NoError(t, err)
		assert.Equal(t, "collector@test-project.iam.gserviceaccount.com", key.ClientEmail)
		assert.Equal(t, "kid-123", key.PrivateKeyID)
		require.NotNil(t, rsaKey)
		require.NoError(t, rsaKey.Validate())
	})

	t.Run("rejects invalid JSON", func(t *testing.T) {
		_, _, err := parseServiceAccountKey([]byte("not json"))
		assert.Error(t, err)
	})

	t.Run("rejects a key missing client_email", func(t *testing.T) {
		_, _, err := parseServiceAccountKey([]byte(`{"private_key":"x"}`))
		assert.Error(t, err)
	})

	t.Run("rejects a key missing private_key", func(t *testing.T) {
		_, _, err := parseServiceAccountKey([]byte(`{"client_email":"x@y.test"}`))
		assert.Error(t, err)
	})

	t.Run("rejects a non-PEM private_key", func(t *testing.T) {
		_, _, err := parseServiceAccountKey([]byte(`{"client_email":"x@y.test","private_key":"-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----"}`))
		assert.Error(t, err)
	})
}

// --- service account assertion signing --------------------------------------

func TestSignServiceAccountAssertion(t *testing.T) {
	saJSON, pub := generateServiceAccount(t)
	key, rsaKey, err := parseServiceAccountKey([]byte(saJSON))
	require.NoError(t, err)

	verify := func(assertion string) jwt.MapClaims {
		claims := jwt.MapClaims{}
		tok, err := jwt.ParseWithClaims(assertion, claims, func(*jwt.Token) (interface{}, error) {
			return pub, nil
		})
		require.NoError(t, err)
		require.True(t, tok.Valid)
		return claims
	}

	t.Run("signs an RS256 assertion carrying the delegation claims", func(t *testing.T) {
		assertion, err := signServiceAccountAssertion(key, rsaKey, "victim@example.test",
			[]string{gmailReadonlyScope}, "https://oauth2.googleapis.com/token")
		require.NoError(t, err)

		claims := verify(assertion)
		assert.Equal(t, key.ClientEmail, claims["iss"])
		assert.Equal(t, "victim@example.test", claims["sub"])
		assert.Equal(t, gmailReadonlyScope, claims["scope"])
		assert.Equal(t, "https://oauth2.googleapis.com/token", claims["aud"])
		assert.Contains(t, claims, "iat")
		assert.Contains(t, claims, "exp")

		// The key id is advertised in the JWT header.
		parts, _, err := jwt.NewParser().ParseUnverified(assertion, jwt.MapClaims{})
		require.NoError(t, err)
		assert.Equal(t, "kid-123", parts.Header["kid"])
		assert.Equal(t, "RS256", parts.Header["alg"])
	})

	t.Run("omits the sub claim when no subject is set", func(t *testing.T) {
		assertion, err := signServiceAccountAssertion(key, rsaKey, "",
			[]string{gmailReadonlyScope}, "https://oauth2.googleapis.com/token")
		require.NoError(t, err)
		claims := verify(assertion)
		assert.NotContains(t, claims, "sub")
	})
}

// --- refresh-token token source ---------------------------------------------

// TestRefreshTokenSource verifies the source posts a well-formed refresh_token
// grant, caches the access token, and re-mints it on a forced refresh.
func TestRefreshTokenSource(t *testing.T) {
	var mu sync.Mutex
	var gotGrant, gotClientID, gotClientSecret, gotRefresh string
	hits := 0

	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		mu.Lock()
		hits++
		gotGrant = r.Form.Get("grant_type")
		gotClientID = r.Form.Get("client_id")
		gotClientSecret = r.Form.Get("client_secret")
		gotRefresh = r.Form.Get("refresh_token")
		n := hits
		mu.Unlock()
		writeJSON(w, http.StatusOK,
			fmt.Sprintf(`{"access_token":"AT-%d","token_type":"Bearer","expires_in":3600}`, n))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	ts := newRefreshTokenSource(server.URL+"/token", "cid", "csec", "rtok", newAuthHTTPClient())
	defer ts.Close()
	ctx := context.Background()

	tok, err := ts.Token(ctx, false)
	require.NoError(t, err)
	assert.Equal(t, "AT-1", tok)

	mu.Lock()
	assert.Equal(t, "refresh_token", gotGrant)
	assert.Equal(t, "cid", gotClientID)
	assert.Equal(t, "csec", gotClientSecret)
	assert.Equal(t, "rtok", gotRefresh)
	mu.Unlock()

	// A second non-forced call is served from cache: no new mint.
	_, err = ts.Token(ctx, false)
	require.NoError(t, err)
	mu.Lock()
	assert.Equal(t, 1, hits, "cached token must not trigger a new mint")
	mu.Unlock()

	// A forced call re-mints.
	_, err = ts.Token(ctx, true)
	require.NoError(t, err)
	mu.Lock()
	assert.Equal(t, 2, hits, "forced refresh must mint a new token")
	mu.Unlock()
}

// TestTokenSourceErrorsAreWrapped verifies a non-2xx token response surfaces as
// a TokenError wrapping an HTTPError, so callers can classify it.
func TestTokenSourceErrorsAreWrapped(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusBadRequest, `{"error":"invalid_grant"}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	ts := newRefreshTokenSource(server.URL+"/token", "cid", "csec", "rtok", newAuthHTTPClient())
	defer ts.Close()

	_, err := ts.Token(context.Background(), false)
	require.Error(t, err)

	var tokenErr *TokenError
	require.ErrorAs(t, err, &tokenErr)
	var httpErr *HTTPError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, http.StatusBadRequest, httpErr.StatusCode)
	// A bad-grant token failure must be classified non-transient (do not retry).
	assert.False(t, isTransientError(err))
}
