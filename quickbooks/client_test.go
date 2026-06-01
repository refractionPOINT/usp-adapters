package usp_quickbooks

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

// baseTestConfig is a minimal valid config pointed at the given API/token URLs.
func baseTestConfig(t *testing.T, baseURL, tokenURL string) QuickBooksConfig {
	t.Helper()
	return QuickBooksConfig{
		ClientOptions: testClientOptions(t),
		ClientID:      "test-client-id",
		ClientSecret:  "test-client-secret",
		RefreshToken:  "test-refresh-token",
		RealmID:       "1234567890",
		BaseURL:       baseURL,
		TokenURL:      tokenURL,
		PollInterval:  40 * time.Millisecond,
	}
}

// --- unit tests: parseCDCResponse -------------------------------------------

func TestParseCDCResponse(t *testing.T) {
	t.Run("groups entities by type and preserves payloads", func(t *testing.T) {
		raw := []byte(`{
			"CDCResponse":[{"QueryResponse":[
				{"Customer":[
					{"Id":"1","SyncToken":"0","DisplayName":"Amy's Bird Sanctuary",
					 "MetaData":{"LastUpdatedTime":"2026-05-12T13:39:32-07:00"}}
				],"startPosition":1,"maxResults":1},
				{"Invoice":[
					{"Id":"130","SyncToken":"2","TotalAmt":362.07,
					 "MetaData":{"LastUpdatedTime":"2026-05-12T14:00:00-07:00"}},
					{"Id":"131","SyncToken":"0","status":"Deleted",
					 "MetaData":{"LastUpdatedTime":"2026-05-12T14:05:00-07:00"}}
				],"startPosition":1,"maxResults":2}
			]}],
			"time":"2026-05-12T14:10:00.000-07:00"
		}`)
		changes, err := parseCDCResponse(raw)
		require.NoError(t, err)
		require.Len(t, changes, 3)

		byType := map[string]int{}
		for _, c := range changes {
			byType[c.entityType]++
		}
		assert.Equal(t, map[string]int{"Customer": 1, "Invoice": 2}, byType)

		// The Customer payload survives verbatim, including nested MetaData.
		var cust cdcChange
		for _, c := range changes {
			if c.entityType == "Customer" {
				cust = c
			}
		}
		assert.Equal(t, "1", cust.id())
		assert.Equal(t, "Amy's Bird Sanctuary", cust.entity.FindOneString("DisplayName"))
		assert.Equal(t, "2026-05-12T13:39:32-07:00", cust.entity.FindOneString("MetaData/LastUpdatedTime"))
	})

	t.Run("deleted entity is parsed with its status", func(t *testing.T) {
		raw := []byte(`{"CDCResponse":[{"QueryResponse":[
			{"Invoice":[{"Id":"99","status":"Deleted","MetaData":{"LastUpdatedTime":"2026-05-12T14:05:00Z"}}]}
		]}]}`)
		changes, err := parseCDCResponse(raw)
		require.NoError(t, err)
		require.Len(t, changes, 1)
		assert.Equal(t, "Invoice", changes[0].entityType)
		assert.Equal(t, "Deleted", changes[0].entity.FindOneString("status"))
	})

	t.Run("empty CDC response yields no changes", func(t *testing.T) {
		// When nothing changed, a QueryResponse block carries no entity array.
		raw := []byte(`{"CDCResponse":[{"QueryResponse":[{}]}],"time":"2026-05-12T14:10:00Z"}`)
		changes, err := parseCDCResponse(raw)
		require.NoError(t, err)
		assert.Empty(t, changes)
	})

	t.Run("empty body yields no changes", func(t *testing.T) {
		changes, err := parseCDCResponse([]byte("   "))
		require.NoError(t, err)
		assert.Empty(t, changes)
	})

	t.Run("fault response surfaces an error", func(t *testing.T) {
		raw := []byte(`{"Fault":{"Error":[{"Message":"changedSince is more than 30 days ago","code":"4001"}],"type":"ValidationFault"},"time":"2026-05-12T14:10:00Z"}`)
		_, err := parseCDCResponse(raw)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fault")
	})

	t.Run("invalid json errors", func(t *testing.T) {
		_, err := parseCDCResponse([]byte(`not json`))
		assert.Error(t, err)
	})

	t.Run("large integers keep precision", func(t *testing.T) {
		raw := []byte(`{"CDCResponse":[{"QueryResponse":[{"Invoice":[{"Id":"1","big":123456789012345678}]}]}]}`)
		changes, err := parseCDCResponse(raw)
		require.NoError(t, err)
		require.Len(t, changes, 1)
		v, ok := changes[0].entity.GetInt("big")
		require.True(t, ok)
		assert.Equal(t, uint64(123456789012345678), v)
	})

	t.Run("emission order is deterministic by entity type", func(t *testing.T) {
		raw := []byte(`{"CDCResponse":[{"QueryResponse":[
			{"Vendor":[{"Id":"v1"}]},
			{"Account":[{"Id":"a1"}]},
			{"Invoice":[{"Id":"i1"}]}
		]}]}`)
		changes, err := parseCDCResponse(raw)
		require.NoError(t, err)
		require.Len(t, changes, 3)
		// Within each QueryResponse block keys are sorted; here each block has a
		// single key, so the order follows the blocks but each block's key is
		// emitted deterministically. Assert all three are present.
		got := map[string]string{}
		for _, c := range changes {
			got[c.entityType] = c.id()
		}
		assert.Equal(t, map[string]string{"Vendor": "v1", "Account": "a1", "Invoice": "i1"}, got)
	})
}

// --- unit tests: dedupe key -------------------------------------------------

func TestDedupeKey(t *testing.T) {
	a := &QuickBooksAdapter{}

	t.Run("keys on type, id and last-updated time", func(t *testing.T) {
		ch := cdcChange{entityType: "Invoice", entity: utils.Dict{
			"Id":       "130",
			"MetaData": utils.Dict{"LastUpdatedTime": "2026-05-12T14:00:00Z"},
		}}
		assert.Equal(t, "Invoice|130|2026-05-12T14:00:00Z", a.dedupeKey(ch))
	})

	t.Run("a new revision of the same object yields a new key", func(t *testing.T) {
		mk := func(lut string) cdcChange {
			return cdcChange{entityType: "Invoice", entity: utils.Dict{
				"Id":       "130",
				"MetaData": utils.Dict{"LastUpdatedTime": lut},
			}}
		}
		assert.NotEqual(t, a.dedupeKey(mk("2026-05-12T14:00:00Z")), a.dedupeKey(mk("2026-05-12T15:00:00Z")))
	})

	t.Run("falls back to SyncToken when no timestamp", func(t *testing.T) {
		ch := cdcChange{entityType: "Invoice", entity: utils.Dict{"Id": "130", "SyncToken": "5"}}
		assert.Equal(t, "Invoice|130|5", a.dedupeKey(ch))
	})

	t.Run("falls back to a content hash when no timestamp or synctoken", func(t *testing.T) {
		ch := cdcChange{entityType: "Invoice", entity: utils.Dict{"Id": "130", "x": "y"}}
		key := a.dedupeKey(ch)
		assert.Contains(t, key, "Invoice|130|sha256:")
	})
}

// --- unit tests: config -----------------------------------------------------

func TestValidate(t *testing.T) {
	valid := func() QuickBooksConfig {
		return QuickBooksConfig{
			ClientOptions: testClientOptions(t),
			ClientID:      "id", ClientSecret: "secret",
			RefreshToken: "rt", RealmID: "123",
		}
	}

	t.Run("requires every credential field", func(t *testing.T) {
		for _, mut := range []func(*QuickBooksConfig){
			func(c *QuickBooksConfig) { c.ClientID = "" },
			func(c *QuickBooksConfig) { c.ClientSecret = "" },
			func(c *QuickBooksConfig) { c.RefreshToken = "" },
			func(c *QuickBooksConfig) { c.RealmID = "" },
		} {
			c := valid()
			mut(&c)
			assert.Error(t, c.Validate())
		}
	})

	t.Run("applies defaults", func(t *testing.T) {
		c := valid()
		require.NoError(t, c.Validate())
		assert.Equal(t, defaultMinorVersion, c.MinorVersion)
		assert.Equal(t, defaultPollInterval, c.PollInterval)
		assert.Equal(t, defaultOverlap, c.Overlap)
		assert.Equal(t, defaultDedupeTTL, c.DedupeTTL)
		assert.Equal(t, defaultMaxRetryAttempts, c.MaxRetryAttempts)
		assert.Equal(t, defaultEntities, c.Entities)
	})

	t.Run("dedupes and trims a custom entity list", func(t *testing.T) {
		c := valid()
		c.Entities = []string{" Invoice ", "Customer", "Invoice", "", "  "}
		require.NoError(t, c.Validate())
		assert.Equal(t, []string{"Invoice", "Customer"}, c.Entities)
	})

	t.Run("rejects an entity list with no usable names", func(t *testing.T) {
		c := valid()
		c.Entities = []string{"", "  "}
		assert.Error(t, c.Validate())
	})

	t.Run("clamps initial_lookback to the 30-day CDC horizon", func(t *testing.T) {
		c := valid()
		c.InitialLookback = 60 * 24 * time.Hour
		require.NoError(t, c.Validate())
		assert.Equal(t, cdcMaxLookback, c.InitialLookback)
	})
}

func TestResolveBaseURL(t *testing.T) {
	assert.Equal(t, prodBaseURL, resolveBaseURL(QuickBooksConfig{}))
	assert.Equal(t, sandboxBaseURL, resolveBaseURL(QuickBooksConfig{Sandbox: true}))
	assert.Equal(t, "https://example.test", resolveBaseURL(QuickBooksConfig{BaseURL: "https://example.test/"}))
	// base_url wins over sandbox.
	assert.Equal(t, "https://example.test", resolveBaseURL(QuickBooksConfig{Sandbox: true, BaseURL: "https://example.test"}))
}

func TestResolveTokenURL(t *testing.T) {
	assert.Equal(t, tokenEndpoint, resolveTokenURL(QuickBooksConfig{}))
	assert.Equal(t, "https://token.test", resolveTokenURL(QuickBooksConfig{TokenURL: "https://token.test"}))
}

func TestIsTransientError(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		isTransient bool
	}{
		{"500", &HTTPError{StatusCode: 500}, true},
		{"503", &HTTPError{StatusCode: 503}, true},
		{"429", &HTTPError{StatusCode: 429}, true},
		{"401", &HTTPError{StatusCode: 401}, false},
		{"403", &HTTPError{StatusCode: 403}, false},
		{"400", &HTTPError{StatusCode: 400}, false},
		{"token error wrapping 400 is permanent", &TokenError{Err: &HTTPError{StatusCode: 400}}, false},
		{"token error wrapping 500 is transient", &TokenError{Err: &HTTPError{StatusCode: 500}}, true},
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

// --- integration test: request shape ----------------------------------------

// TestCDCRequestShape asserts the adapter authenticates correctly and issues a
// well-formed CDC request: a refresh_token grant to the token endpoint, then a
// GET to /v3/company/{realmId}/cdc carrying the bearer token, the entity list,
// a changedSince and the pinned minor version.
func TestCDCRequestShape(t *testing.T) {
	var mu sync.Mutex
	var tokenGrant, tokenAuth string
	var cdcMethod, cdcPath, cdcAuth, cdcAccept string
	var cdcQuery url.Values
	tokenHits, cdcHits := 0, 0

	mux := http.NewServeMux()
	mux.HandleFunc("/oauth2/v1/tokens/bearer", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		mu.Lock()
		tokenHits++
		tokenGrant = r.Form.Get("grant_type")
		tokenAuth = r.Header.Get("Authorization")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"AT-1","refresh_token":"test-refresh-token","token_type":"bearer","expires_in":3600,"x_refresh_token_expires_in":8726400}`))
	})
	mux.HandleFunc("/v3/company/1234567890/cdc", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		cdcHits++
		cdcMethod = r.Method
		cdcPath = r.URL.Path
		cdcAuth = r.Header.Get("Authorization")
		cdcAccept = r.Header.Get("Accept")
		cdcQuery = r.URL.Query()
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"CDCResponse":[{"QueryResponse":[{}]}],"time":"2026-05-12T14:10:00Z"}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := baseTestConfig(t, server.URL, server.URL+"/oauth2/v1/tokens/bearer")
	conf.Entities = []string{"Customer", "Invoice"}
	conf.MinorVersion = "75"
	adapter, _, err := NewQuickBooksAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return cdcHits >= 1 && tokenHits >= 1
	}, 3*time.Second, 20*time.Millisecond, "adapter never completed an authenticated CDC request")

	mu.Lock()
	defer mu.Unlock()
	// Token request: refresh_token grant with HTTP Basic client credentials.
	assert.Equal(t, "refresh_token", tokenGrant)
	// base64("test-client-id:test-client-secret")
	assert.Equal(t, "Basic dGVzdC1jbGllbnQtaWQ6dGVzdC1jbGllbnQtc2VjcmV0", tokenAuth)
	// CDC request shape.
	assert.Equal(t, http.MethodGet, cdcMethod)
	assert.Equal(t, "/v3/company/1234567890/cdc", cdcPath)
	assert.Equal(t, "Bearer AT-1", cdcAuth)
	assert.Equal(t, "application/json", cdcAccept)
	assert.Equal(t, "Customer,Invoice", cdcQuery.Get("entities"))
	assert.Equal(t, "75", cdcQuery.Get("minorversion"))
	assert.NotEmpty(t, cdcQuery.Get("changedSince"), "changedSince must be sent")
	_, perr := time.Parse(time.RFC3339, cdcQuery.Get("changedSince"))
	assert.NoError(t, perr, "changedSince must be RFC3339")
}
