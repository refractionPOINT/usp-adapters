package usp_o365

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagementCloudTokenRequests(t *testing.T) {
	for _, tc := range []struct {
		cloud, feedHost, tokenHost, tokenPath, resource, scope string
	}{
		{"enterprise", "manage.office.com", "login.windows.net", "/" + testDomain + "/oauth2/token", "https://manage.office.com", ""},
		{"gcc-gov", "manage-gcc.office.com", "login.windows.net", "/" + testDomain + "/oauth2/token", "https://manage-gcc.office.com", ""},
		{"gcc-high-gov", "manage.office365.us", "login.microsoftonline.us", "/" + testTenantID + "/oauth2/v2.0/token", "", "https://manage.office365.us/.default"},
		{"dod-gov", "manage.protection.apps.mil", "login.microsoftonline.us", "/" + testTenantID + "/oauth2/v2.0/token", "", "https://manage.protection.apps.mil/.default"},
	} {
		t.Run(tc.cloud, func(t *testing.T) {
			var tokens, feeds int
			a := mockManagementAuth(t, tc.cloud, nil, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Host == tc.tokenHost {
					tokens++
					assert.Equal(t, http.MethodPost, r.Method)
					assert.Equal(t, tc.tokenPath, r.URL.Path)
					version := ""
					if tc.resource != "" {
						version = "1.0"
					}
					assert.Equal(t, version, r.URL.Query().Get("api-version"))
					require.NoError(t, r.ParseForm())
					assert.Equal(t, "client_credentials", r.PostForm.Get("grant_type"))
					assert.Equal(t, tc.resource, r.PostForm.Get("resource"))
					assert.Equal(t, tc.scope, r.PostForm.Get("scope"))
					tokenSuccess(w, 3600)
					return
				}
				feeds++
				assert.Equal(t, tc.feedHost, r.URL.Host)
				assert.Equal(t, "Bearer "+testAccessToken, r.Header.Get("Authorization"))
				_, _ = io.WriteString(w, "{}")
			})
			_, err := a.makeOneRegistrationRequest(a.endpoint + testTenantID + "/activity/feed/subscriptions/start")
			require.NoError(t, err)
			assert.Equal(t, 1, tokens)
			assert.Equal(t, 1, feeds)
		})
	}
}

func TestGCCTokenAudienceFallback(t *testing.T) {
	for _, rejection := range []string{"token endpoint", "feed body", "generic 401"} {
		t.Run(rejection, func(t *testing.T) {
			var audiences, usedTokens, warnings []string
			var legacyTokens int
			a := mockManagementAuth(t, "gcc-gov", func(message string) { warnings = append(warnings, message) }, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Host == "login.windows.net" {
					require.NoError(t, r.ParseForm())
					audience := r.PostForm.Get("resource")
					audiences = append(audiences, audience)
					switch audience {
					case "https://manage-gcc.office.com":
						if rejection == "token endpoint" {
							writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_resource"})
							return
						}
						tokenSuccess(w, 3600)
					case "https://manage.office.com":
						legacyTokens++
						// The first legacy token is already inside OAuth2's expiry leeway.
						expires := 1
						if legacyTokens > 1 {
							expires = 3600
						}
						writeJSON(w, http.StatusOK, map[string]interface{}{
							"access_token": "fixture-legacy-token", "token_type": "Bearer", "expires_in": expires,
						})
					default:
						t.Fatalf("unexpected resource %q", audience)
					}
					return
				}
				assert.Equal(t, "manage-gcc.office.com", r.URL.Host)
				if r.Method == http.MethodPost {
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					assert.JSONEq(t, `{"fixture":true}`, string(body))
				}
				auth := r.Header.Get("Authorization")
				usedTokens = append(usedTokens, auth)
				if auth == "Bearer "+testAccessToken {
					if rejection == "generic 401" {
						w.WriteHeader(http.StatusUnauthorized)
						_, _ = io.WriteString(w, "Unauthorized")
					} else {
						w.WriteHeader(http.StatusUnauthorized)
						_, _ = io.WriteString(w, `{"error":{"message":"Access token validation failure. Invalid audience."}}`)
					}
					return
				}
				assert.Equal(t, "Bearer fixture-legacy-token", auth)
				_, _ = io.WriteString(w, "{}")
			})

			req, err := http.NewRequestWithContext(a.stopCtx, http.MethodPost,
				a.endpoint+testTenantID+"/activity/feed/subscriptions/start", bytes.NewBufferString(`{"fixture":true}`))
			require.NoError(t, err)
			resp, err := a.httpClient.Do(req)
			require.NoError(t, err)
			resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			for range 2 {
				_, _, err := a.get(a.stopCtx, a.endpoint+testTenantID+"/activity/feed/subscriptions/content")
				require.NoError(t, err)
			}

			require.NotEmpty(t, audiences)
			assert.Equal(t, "https://manage-gcc.office.com", audiences[0])
			assert.Equal(t, []string{"https://manage.office.com", "https://manage.office.com"}, audiences[len(audiences)-2:])
			assert.Equal(t, 2, legacyTokens, "refresh must keep the legacy audience")
			wantTokens := []string{"Bearer fixture-legacy-token", "Bearer fixture-legacy-token", "Bearer fixture-legacy-token"}
			if rejection != "token endpoint" {
				wantTokens = append([]string{"Bearer " + testAccessToken}, wantTokens...)
			}
			assert.Equal(t, wantTokens, usedTokens)
			require.Len(t, warnings, 1)
			assert.Contains(t, warnings[0], "retrying with the commercial audience")
		})
	}
}

func TestGCCTokenAudienceDoesNotFallbackOnUnrelatedErrors(t *testing.T) {
	for _, tc := range []struct {
		name, host, tokenError, feedError string
		status                            int
	}{
		{name: "bad client", tokenError: "invalid_client"},
		{name: "bad request", host: "manage-gcc.office.com", status: http.StatusBadRequest, feedError: `{"error":{"message":"Invalid audience"}}`},
		{name: "different host", host: "example.com", status: http.StatusUnauthorized, feedError: `{"error":{"message":"Invalid audience"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tokens int
			a := mockManagementAuth(t, "gcc-gov", func(message string) { t.Errorf("unexpected fallback: %s", message) }, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Host == "login.windows.net" {
					tokens++
					require.NoError(t, r.ParseForm())
					assert.Equal(t, "https://manage-gcc.office.com", r.PostForm.Get("resource"))
					if tc.tokenError != "" {
						writeJSON(w, http.StatusBadRequest, map[string]string{"error": tc.tokenError})
						return
					}
					tokenSuccess(w, 3600)
					return
				}
				require.NotEmpty(t, tc.feedError)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.feedError)
			})
			if tc.tokenError != "" {
				_, _, err := a.get(a.stopCtx, a.endpoint)
				require.ErrorContains(t, err, tc.tokenError)
			} else {
				_, _, err := a.get(a.stopCtx, "https://"+tc.host+"/content")
				require.ErrorContains(t, err, tc.feedError)
			}
			assert.Positive(t, tokens)
		})
	}
}

func TestGCCFeedCredentialError(t *testing.T) {
	for _, tc := range []struct {
		name           string
		establishedGCC bool
	}{
		{name: "both audiences forbidden"},
		{name: "after GCC success", establishedGCC: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var audiences []string
			var warnings int
			a := mockManagementAuth(t, "gcc-gov", func(string) { warnings++ }, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Host == "login.windows.net" {
					require.NoError(t, r.ParseForm())
					audience := r.PostForm.Get("resource")
					audiences = append(audiences, audience)
					writeJSON(w, http.StatusOK, map[string]interface{}{
						"access_token": audience, "token_type": "Bearer", "expires_in": 3600,
					})
					return
				}
				if r.URL.Path == "/accepted" {
					assert.Equal(t, "Bearer https://manage-gcc.office.com", r.Header.Get("Authorization"))
					_, _ = io.WriteString(w, "{}")
					return
				}
				w.WriteHeader(http.StatusForbidden)
				if r.Header.Get("Authorization") == "Bearer https://manage.office.com" {
					_, _ = io.WriteString(w, "commercial request forbidden")
				} else if tc.establishedGCC {
					_, _ = io.WriteString(w, `{"error":{"message":"Invalid audience"}}`)
				} else {
					_, _ = io.WriteString(w, "GCC request forbidden")
				}
			})
			if tc.establishedGCC {
				_, _, err := a.get(a.stopCtx, "https://manage-gcc.office.com/accepted")
				require.NoError(t, err)
			}
			_, _, err := a.getWithRetry("https://manage-gcc.office.com/denied", "content api", &failureState{}, 0)
			var apiErr *apiError
			require.ErrorAs(t, err, &apiErr)
			assert.False(t, apiErr.transient)
			assert.Contains(t, apiErr.Error(), "403 Forbidden")
			if tc.establishedGCC {
				assert.Contains(t, apiErr.Error(), "Invalid audience")
				assert.Equal(t, []string{"https://manage-gcc.office.com"}, audiences)
				assert.Zero(t, warnings)
			} else {
				assert.Contains(t, apiErr.Error(), "commercial request forbidden")
				assert.Equal(t, []string{"https://manage-gcc.office.com", "https://manage.office.com"}, audiences)
				assert.Equal(t, 1, warnings)
			}
		})
	}
}

func TestGCCConcurrentFallbackSharesCommercialToken(t *testing.T) {
	const collectors = 8
	var gccRequests, legacyTokens, legacyRequests, warnings atomic.Int32
	allCollectors := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a := mockManagementAuth(t, "gcc-gov", func(string) { warnings.Add(1) }, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Host == "login.windows.net" {
			if err := r.ParseForm(); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			audience := r.PostForm.Get("resource")
			if audience == "https://manage.office.com" {
				legacyTokens.Add(1)
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"access_token": audience, "token_type": "Bearer", "expires_in": 3600,
			})
			return
		}
		if r.Header.Get("Authorization") == "Bearer https://manage-gcc.office.com" {
			if gccRequests.Add(1) == collectors {
				close(allCollectors)
			}
			select {
			case <-allCollectors:
				w.WriteHeader(http.StatusUnauthorized)
			case <-r.Context().Done():
				w.WriteHeader(http.StatusGatewayTimeout)
			}
			return
		}
		assert.Equal(t, "Bearer https://manage.office.com", r.Header.Get("Authorization"))
		legacyRequests.Add(1)
		_, _ = io.WriteString(w, "{}")
	})
	results := make(chan error, collectors)
	for range collectors {
		go func() {
			_, _, err := a.get(ctx, "https://manage-gcc.office.com/content")
			results <- err
		}()
	}
	for range collectors {
		select {
		case err := <-results:
			assert.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("collectors timed out")
		}
	}
	assert.EqualValues(t, collectors, gccRequests.Load())
	assert.EqualValues(t, collectors, legacyRequests.Load())
	assert.EqualValues(t, 1, legacyTokens.Load())
	assert.EqualValues(t, 1, warnings.Load())
}

func mockManagementAuth(t *testing.T, cloud string, warn func(string), handler http.HandlerFunc) *Office365Adapter {
	t.Helper()
	conf := validConfig(t)
	conf.ClientOptions.OnWarning = warn
	a := &Office365Adapter{
		conf: conf, endpointType: cloud, endpoint: URL[cloud], stopCtx: context.Background(),
		httpClient: &http.Client{Transport: mockManagementRoundTripper(handler)},
		doStop:     utils.NewEvent(),
	}
	require.NoError(t, a.updateBearerToken())
	return a
}

type mockManagementRoundTripper http.HandlerFunc

func (mock mockManagementRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		defer req.Body.Close()
	}
	recorder := httptest.NewRecorder()
	mock(recorder, req)
	return recorder.Result(), nil
}
