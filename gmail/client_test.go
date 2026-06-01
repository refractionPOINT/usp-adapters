package usp_gmail

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClientOptions returns ClientOptions wired for a sink (no real LimaCharlie
// connection) with the logging callbacks pointed at the test log.
func testClientOptions(t *testing.T) uspclient.ClientOptions {
	t.Helper()
	return uspclient.ClientOptions{
		Identity: uspclient.Identity{
			Oid:             "11111111-1111-1111-1111-111111111111",
			InstallationKey: "test-installation-key",
		},
		Platform:     "json",
		TestSinkMode: true,
		DebugLog:     func(msg string) { t.Logf("DBG: %s", msg) },
		OnWarning:    func(msg string) { t.Logf("WRN: %s", msg) },
		OnError:      func(err error) { t.Logf("ERR: %v", err) },
	}
}

// --- unit tests: config validation ------------------------------------------

func TestValidate(t *testing.T) {
	refresh := func() GmailConfig {
		return GmailConfig{
			ClientOptions: testClientOptions(t),
			ClientID:      "id", ClientSecret: "secret", RefreshToken: "rt",
		}
	}
	serviceAccount := func() GmailConfig {
		return GmailConfig{
			ClientOptions:             testClientOptions(t),
			ServiceAccountCredentials: `{"client_email":"x"}`,
			Subject:                   "user@example.test",
		}
	}

	t.Run("refresh flow requires every credential field", func(t *testing.T) {
		for _, mut := range []func(*GmailConfig){
			func(c *GmailConfig) { c.ClientID = "" },
			func(c *GmailConfig) { c.ClientSecret = "" },
			func(c *GmailConfig) { c.RefreshToken = "" },
		} {
			c := refresh()
			mut(&c)
			assert.Error(t, c.Validate())
		}
	})

	t.Run("service account flow requires a subject", func(t *testing.T) {
		c := serviceAccount()
		c.Subject = ""
		assert.Error(t, c.Validate())
	})

	t.Run("rejects mixing both credential modes", func(t *testing.T) {
		c := refresh()
		c.ServiceAccountCredentials = `{"client_email":"x"}`
		c.Subject = "user@example.test"
		assert.Error(t, c.Validate())
	})

	t.Run("rejects no credentials at all", func(t *testing.T) {
		c := GmailConfig{ClientOptions: testClientOptions(t)}
		assert.Error(t, c.Validate())
	})

	t.Run("rejects both inline and file service account", func(t *testing.T) {
		c := serviceAccount()
		c.ServiceAccountFile = "/tmp/key.json"
		assert.Error(t, c.Validate())
	})

	t.Run("applies defaults", func(t *testing.T) {
		c := refresh()
		require.NoError(t, c.Validate())
		assert.Equal(t, defaultUserID, c.UserID)
		assert.Equal(t, defaultQuery, c.Query)
		assert.Equal(t, defaultFormat, c.Format)
		assert.Equal(t, defaultMaxResults, c.MaxResults)
		assert.Equal(t, defaultPollInterval, c.PollInterval)
		assert.Equal(t, defaultOverlap, c.Overlap)
		assert.Equal(t, defaultDedupeTTL, c.DedupeTTL)
		assert.Equal(t, defaultMaxRetryAttempts, c.MaxRetryAttempts)
		assert.Equal(t, []string{gmailReadonlyScope}, c.Scopes)
	})

	t.Run("rejects an invalid format", func(t *testing.T) {
		c := refresh()
		c.Format = "html"
		assert.Error(t, c.Validate())
	})

	t.Run("clamps max_results to the API ceiling", func(t *testing.T) {
		c := refresh()
		c.MaxResults = 9000
		require.NoError(t, c.Validate())
		assert.Equal(t, maxAllowedResults, c.MaxResults)
	})
}

// --- unit tests: query building ---------------------------------------------

func TestBuildQuery(t *testing.T) {
	start := time.Unix(1716200000, 0)

	t.Run("appends an epoch-seconds time bound to the base query", func(t *testing.T) {
		assert.Equal(t, "in:inbox after:1716200000", buildQuery("in:inbox", start))
	})
	t.Run("handles an empty base query", func(t *testing.T) {
		assert.Equal(t, "after:1716200000", buildQuery("  ", start))
	})
	t.Run("preserves a compound base query", func(t *testing.T) {
		assert.Equal(t, "in:inbox -from:me after:1716200000", buildQuery("in:inbox -from:me", start))
	})
}

// --- unit tests: event time -------------------------------------------------

func TestEventTime(t *testing.T) {
	t.Run("parses internalDate epoch milliseconds", func(t *testing.T) {
		assert.Equal(t, uint64(1726234567000), eventTime(utils.Dict{"internalDate": "1726234567000"}))
	})
	t.Run("falls back to now when internalDate is missing", func(t *testing.T) {
		before := uint64(time.Now().UnixMilli())
		got := eventTime(utils.Dict{})
		assert.GreaterOrEqual(t, got, before)
	})
	t.Run("falls back to now when internalDate is not numeric", func(t *testing.T) {
		before := uint64(time.Now().UnixMilli())
		got := eventTime(utils.Dict{"internalDate": "not-a-number"})
		assert.GreaterOrEqual(t, got, before)
	})
}

// --- unit tests: list parsing -----------------------------------------------

func TestParseListMessages(t *testing.T) {
	t.Run("parses message refs and the page token", func(t *testing.T) {
		raw := []byte(`{"messages":[{"id":"a","threadId":"t1"},{"id":"b","threadId":"t2"}],"nextPageToken":"NPT","resultSizeEstimate":42}`)
		resp, err := parseListMessages(raw)
		require.NoError(t, err)
		require.Len(t, resp.Messages, 2)
		assert.Equal(t, "a", resp.Messages[0].ID)
		assert.Equal(t, "t2", resp.Messages[1].ThreadID)
		assert.Equal(t, "NPT", resp.NextPageToken)
		assert.Equal(t, int64(42), resp.ResultSizeEstimate)
	})
	t.Run("an empty result set yields no messages", func(t *testing.T) {
		// Gmail omits the messages field entirely when nothing matches.
		resp, err := parseListMessages([]byte(`{"resultSizeEstimate":0}`))
		require.NoError(t, err)
		assert.Empty(t, resp.Messages)
	})
	t.Run("an empty body yields no messages", func(t *testing.T) {
		resp, err := parseListMessages([]byte("  "))
		require.NoError(t, err)
		assert.Empty(t, resp.Messages)
	})
	t.Run("invalid json errors", func(t *testing.T) {
		_, err := parseListMessages([]byte("not json"))
		assert.Error(t, err)
	})
}

// --- unit tests: error classification ---------------------------------------

func TestIsTransientError(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		isTransient bool
	}{
		{"500", &HTTPError{StatusCode: 500}, true},
		{"503", &HTTPError{StatusCode: 503}, true},
		{"429", &HTTPError{StatusCode: 429}, true},
		{"403 rate limit", &HTTPError{StatusCode: 403, Reason: "rateLimitExceeded"}, true},
		{"403 user rate limit", &HTTPError{StatusCode: 403, Reason: "userRateLimitExceeded"}, true},
		{"403 permission denied", &HTTPError{StatusCode: 403, Reason: "forbidden"}, false},
		{"401", &HTTPError{StatusCode: 401}, false},
		{"404", &HTTPError{StatusCode: 404}, false},
		{"400", &HTTPError{StatusCode: 400}, false},
		{"token error wrapping 400", &TokenError{Err: &HTTPError{StatusCode: 400}}, false},
		{"token error wrapping 500", &TokenError{Err: &HTTPError{StatusCode: 500}}, true},
		{"network", fmt.Errorf("failed to execute request %q: connection refused", "x"), true},
		{"context canceled", context.Canceled, false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.isTransient, isTransientError(c.err))
		})
	}
}

func TestParseHTTPError(t *testing.T) {
	t.Run("extracts the reason from the errors array", func(t *testing.T) {
		body := `{"error":{"code":403,"message":"Rate Limit Exceeded","errors":[{"reason":"rateLimitExceeded"}],"status":"PERMISSION_DENIED"}}`
		e := parseHTTPError(403, "http://x", body)
		assert.Equal(t, 403, e.StatusCode)
		assert.Equal(t, "rateLimitExceeded", e.Reason)
	})
	t.Run("falls back to the status when no reason array", func(t *testing.T) {
		body := `{"error":{"code":401,"message":"Invalid Credentials","status":"UNAUTHENTICATED"}}`
		e := parseHTTPError(401, "http://x", body)
		assert.Equal(t, "UNAUTHENTICATED", e.Reason)
	})
	t.Run("tolerates a non-envelope body", func(t *testing.T) {
		e := parseHTTPError(500, "http://x", "upstream boom")
		assert.Equal(t, 500, e.StatusCode)
		assert.Empty(t, e.Reason)
	})
}

func TestResolveURLs(t *testing.T) {
	assert.Equal(t, gmailAPIBaseURL, resolveBaseURL(GmailConfig{}))
	assert.Equal(t, "https://example.test", resolveBaseURL(GmailConfig{BaseURL: "https://example.test/"}))
	assert.Equal(t, googleTokenEndpoint, resolveTokenURL(GmailConfig{}))
	assert.Equal(t, "https://token.test", resolveTokenURL(GmailConfig{TokenURL: "https://token.test"}))
}

// --- integration test: request shape ----------------------------------------

// TestRequestShape asserts the adapter authenticates and issues well-formed
// Gmail API requests: a bearer-token GET to users.messages.list carrying the
// time-bounded query and page size, then a bearer-token GET to
// users.messages.get carrying the requested format.
func TestRequestShape(t *testing.T) {
	var mu sync.Mutex
	var listMethod, listAuth, listQ, listMax string
	var getMethod, getAuth, getFormat, getPath string
	listHits, getHits := 0, 0

	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"AT-1","token_type":"Bearer","expires_in":3600}`))
	})
	mux.HandleFunc("/gmail/v1/users/me/messages", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		listHits++
		listMethod = r.Method
		listAuth = r.Header.Get("Authorization")
		listQ = r.URL.Query().Get("q")
		listMax = r.URL.Query().Get("maxResults")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"messages":[{"id":"m1","threadId":"t1"}]}`))
	})
	mux.HandleFunc("/gmail/v1/users/me/messages/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		getHits++
		getMethod = r.Method
		getAuth = r.Header.Get("Authorization")
		getFormat = r.URL.Query().Get("format")
		getPath = r.URL.Path
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"m1","threadId":"t1","internalDate":"1726234567000","payload":{"headers":[]}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := GmailConfig{
		ClientOptions: testClientOptions(t),
		ClientID:      "cid", ClientSecret: "csec", RefreshToken: "rt",
		BaseURL:      server.URL,
		TokenURL:     server.URL + "/token",
		PollInterval: 40 * time.Millisecond,
		MaxResults:   25,
		Query:        "in:inbox",
	}
	adapter, _, err := NewGmailAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return listHits >= 1 && getHits >= 1
	}, 3*time.Second, 20*time.Millisecond, "adapter never completed an authenticated list+get")

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, http.MethodGet, listMethod)
	assert.Equal(t, "Bearer AT-1", listAuth)
	assert.Equal(t, "25", listMax)
	assert.True(t, strings.HasPrefix(listQ, "in:inbox after:"), "query must carry the base query and a time bound, got %q", listQ)
	assert.Equal(t, http.MethodGet, getMethod)
	assert.Equal(t, "Bearer AT-1", getAuth)
	assert.Equal(t, "full", getFormat)
	assert.Equal(t, "/gmail/v1/users/me/messages/m1", getPath)
}
