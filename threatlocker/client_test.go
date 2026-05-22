package usp_threatlocker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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

// approvalItem builds a minimal ThreatLocker approval-request-shaped record.
func approvalItem(id string) map[string]interface{} {
	return map[string]interface{}{
		"approvalRequestId": id,
		"dateTime":          "2026-05-21T12:00:00Z",
		"statusId":          1,
		"hostname":          "WIN-" + id,
	}
}

func decodeRequestBody(t *testing.T, r *http.Request) map[string]interface{} {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	m := map[string]interface{}{}
	require.NoError(t, json.Unmarshal(body, &m))
	return m
}

// --- unit tests -------------------------------------------------------------

func TestExtractItems(t *testing.T) {
	t.Run("bare array", func(t *testing.T) {
		items, err := extractItems([]byte(`[{"id":"a"},{"id":"b"}]`), "")
		require.NoError(t, err)
		require.Len(t, items, 2)
		assert.Equal(t, "a", items[0].FindOneString("id"))
		assert.Equal(t, "b", items[1].FindOneString("id"))
	})

	t.Run("object envelope auto-detect data", func(t *testing.T) {
		items, err := extractItems([]byte(`{"data":[{"id":"a"}],"totalRecords":1}`), "")
		require.NoError(t, err)
		require.Len(t, items, 1)
		assert.Equal(t, "a", items[0].FindOneString("id"))
	})

	t.Run("object envelope auto-detect pageItems", func(t *testing.T) {
		items, err := extractItems([]byte(`{"pageItems":[{"id":"a"},{"id":"b"}]}`), "")
		require.NoError(t, err)
		require.Len(t, items, 2)
	})

	t.Run("object envelope explicit items_path", func(t *testing.T) {
		items, err := extractItems([]byte(`{"customList":[{"id":"a"}]}`), "customList")
		require.NoError(t, err)
		require.Len(t, items, 1)
	})

	t.Run("object envelope missing array errors", func(t *testing.T) {
		_, err := extractItems([]byte(`{"totalRecords":0}`), "")
		assert.Error(t, err)
	})

	t.Run("explicit items_path missing errors", func(t *testing.T) {
		_, err := extractItems([]byte(`{"data":[]}`), "notHere")
		assert.Error(t, err)
	})

	t.Run("empty body yields no items", func(t *testing.T) {
		items, err := extractItems([]byte("   "), "")
		require.NoError(t, err)
		assert.Empty(t, items)
	})

	t.Run("non-json errors", func(t *testing.T) {
		_, err := extractItems([]byte(`not json`), "")
		assert.Error(t, err)
	})

	t.Run("large integers keep precision", func(t *testing.T) {
		items, err := extractItems([]byte(`[{"big":123456789012345678}]`), "")
		require.NoError(t, err)
		require.Len(t, items, 1)
		v, ok := items[0].GetInt("big")
		require.True(t, ok)
		assert.Equal(t, uint64(123456789012345678), v)
	})
}

func TestRecordID(t *testing.T) {
	t.Run("uses configured id field", func(t *testing.T) {
		feed := ThreatLockerFeed{IDField: "approvalRequestId"}
		assert.Equal(t, "abc", recordID(feed, utils.Dict{"approvalRequestId": "abc", "id": "other"}))
	})

	t.Run("falls back to common id field", func(t *testing.T) {
		feed := ThreatLockerFeed{}
		assert.Equal(t, "xyz", recordID(feed, utils.Dict{"id": "xyz"}))
	})

	t.Run("accepts numeric ids", func(t *testing.T) {
		feed := ThreatLockerFeed{IDField: "actionId"}
		assert.Equal(t, "42", recordID(feed, utils.Dict{"actionId": uint64(42)}))
	})

	t.Run("content hash fallback is stable and distinct", func(t *testing.T) {
		feed := ThreatLockerFeed{}
		a1 := recordID(feed, utils.Dict{"foo": "bar"})
		a2 := recordID(feed, utils.Dict{"foo": "bar"})
		b := recordID(feed, utils.Dict{"foo": "baz"})
		assert.Equal(t, a1, a2)
		assert.NotEqual(t, a1, b)
		assert.Contains(t, a1, "sha256:")
	})
}

func TestResolveBaseURL(t *testing.T) {
	assert.Equal(t, "https://portalapi.g.threatlocker.com/portalapi",
		resolveBaseURL(ThreatLockerConfig{Instance: "g"}))
	assert.Equal(t, "https://custom.example.com/portalapi",
		resolveBaseURL(ThreatLockerConfig{BaseURL: "https://custom.example.com/portalapi/"}))
	// base_url wins over instance.
	assert.Equal(t, "https://custom.example.com/portalapi",
		resolveBaseURL(ThreatLockerConfig{Instance: "g", BaseURL: "https://custom.example.com/portalapi"}))
}

func TestBuildRequestBody(t *testing.T) {
	a := &ThreatLockerAdapter{conf: ThreatLockerConfig{PageSize: 50}}

	t.Run("applies pagination and defaults", func(t *testing.T) {
		feed := ThreatLockerFeed{OrderBy: "dateTime", Parameters: utils.Dict{"statusId": 1}}
		body := a.buildRequestBody(feed, 3)
		assert.Equal(t, 3, body["pageNumber"])
		assert.Equal(t, 50, body["pageSize"])
		assert.Equal(t, "dateTime", body["orderBy"])
		assert.Equal(t, false, body["isAscending"])
		assert.Equal(t, 1, body["statusId"])
	})

	t.Run("feed parameters can override sort but not pagination", func(t *testing.T) {
		feed := ThreatLockerFeed{
			OrderBy:    "dateTime",
			Parameters: utils.Dict{"orderBy": "custom", "isAscending": true, "pageNumber": 99},
		}
		body := a.buildRequestBody(feed, 7)
		assert.Equal(t, 7, body["pageNumber"], "adapter must own pageNumber")
		assert.Equal(t, "custom", body["orderBy"])
		assert.Equal(t, true, body["isAscending"])
	})
}

func TestParseTimestamp(t *testing.T) {
	cases := []string{
		"2026-05-21T12:00:00Z",
		"2026-05-21T12:00:00.123456Z",
		"2026-05-21T12:00:00",
		"2026-05-21 12:00:00",
	}
	for _, c := range cases {
		_, ok := parseTimestamp(c)
		assert.True(t, ok, "expected %q to parse", c)
	}
	_, ok := parseTimestamp("not a date")
	assert.False(t, ok)
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
		{"404", &HTTPError{StatusCode: 404}, false},
		{"400", &HTTPError{StatusCode: 400}, false},
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

func TestValidate(t *testing.T) {
	t.Run("requires api_key", func(t *testing.T) {
		c := ThreatLockerConfig{ClientOptions: testClientOptions(t), Instance: "g"}
		assert.Error(t, c.Validate())
	})

	t.Run("requires instance or base_url", func(t *testing.T) {
		c := ThreatLockerConfig{ClientOptions: testClientOptions(t), APIKey: "k"}
		assert.Error(t, c.Validate())
	})

	t.Run("applies defaults and the default feed", func(t *testing.T) {
		c := ThreatLockerConfig{ClientOptions: testClientOptions(t), APIKey: "k", Instance: "g"}
		require.NoError(t, c.Validate())
		assert.Equal(t, defaultPageSize, c.PageSize)
		assert.Equal(t, defaultPollInterval, c.PollInterval)
		require.Len(t, c.Feeds, 1)
		assert.Equal(t, "approval_request", c.Feeds[0].Name)
		assert.Equal(t, defaultMaxPages, c.Feeds[0].MaxPages)
	})

	t.Run("rejects feed without url", func(t *testing.T) {
		c := ThreatLockerConfig{
			ClientOptions: testClientOptions(t), APIKey: "k", Instance: "g",
			Feeds: []ThreatLockerFeed{{Name: "x"}},
		}
		assert.Error(t, c.Validate())
	})

	t.Run("rejects duplicate feed names", func(t *testing.T) {
		c := ThreatLockerConfig{
			ClientOptions: testClientOptions(t), APIKey: "k", Instance: "g",
			Feeds: []ThreatLockerFeed{
				{Name: "x", URL: "A/AGetByParameters"},
				{Name: "x", URL: "B/BGetByParameters"},
			},
		}
		assert.Error(t, c.Validate())
	})
}

// --- integration tests ------------------------------------------------------

// TestApprovalRequestFeed verifies the default feed targets the approval
// request endpoint with the expected method, auth header and request body.
func TestApprovalRequestFeed(t *testing.T) {
	var mu sync.Mutex
	var gotPath, gotAuth, gotMethod, gotManagedOrg string
	var gotBody map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotPath = r.URL.Path
		gotMethod = r.Method
		gotAuth = r.Header.Get("Authorization")
		gotManagedOrg = r.Header.Get("managedOrganizationId")
		gotBody = decodeRequestBody(t, r)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode([]map[string]interface{}{
			approvalItem("req-1"), approvalItem("req-2"),
		})
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ThreatLockerConfig{
		ClientOptions:         testClientOptions(t),
		APIKey:                "secret-token",
		BaseURL:               server.URL,
		ManagedOrganizationID: "org-123",
		PollInterval:          200 * time.Millisecond,
	}
	adapter, chStopped, err := NewThreatLockerAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotPath != ""
	}, 3*time.Second, 20*time.Millisecond, "adapter never called the API")

	select {
	case <-chStopped:
		t.Fatal("adapter stopped unexpectedly")
	default:
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "/ApprovalRequest/ApprovalRequestGetByParameters", gotPath)
	assert.Equal(t, "secret-token", gotAuth)
	assert.Equal(t, "org-123", gotManagedOrg)
	assert.Equal(t, float64(1), gotBody["pageNumber"])
	assert.Equal(t, float64(defaultPageSize), gotBody["pageSize"])
	assert.Equal(t, "dateTime", gotBody["orderBy"])
	assert.Equal(t, false, gotBody["isAscending"])
	assert.Equal(t, float64(1), gotBody["statusId"], "default feed should query pending requests")
}

// pagingServer serves a fixed number of full pages then empty pages, counting
// requests per page number. envelope controls bare-array vs object output.
type pagingServer struct {
	mu        sync.Mutex
	pageHits  map[int]int
	itemsByPg map[int][]map[string]interface{}
	envelope  bool
}

func newPagingServer(envelope bool, itemsByPg map[int][]map[string]interface{}) *pagingServer {
	return &pagingServer{
		pageHits:  map[int]int{},
		itemsByPg: itemsByPg,
		envelope:  envelope,
	}
}

func (s *pagingServer) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body := decodeRequestBody(t, r)
		page := int(body["pageNumber"].(float64))

		s.mu.Lock()
		s.pageHits[page]++
		items := s.itemsByPg[page]
		s.mu.Unlock()

		if items == nil {
			items = []map[string]interface{}{}
		}
		w.WriteHeader(http.StatusOK)
		if s.envelope {
			json.NewEncoder(w).Encode(map[string]interface{}{"data": items, "totalRecords": len(items)})
		} else {
			json.NewEncoder(w).Encode(items)
		}
	}
}

func (s *pagingServer) hits(page int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pageHits[page]
}

// TestPaginationAndDedup verifies the adapter walks pages while there is new
// data, and that on later polls it deduplicates and early-stops (so deeper
// pages are not re-fetched once their content has all been seen).
func TestPaginationAndDedup(t *testing.T) {
	ps := newPagingServer(false, map[int][]map[string]interface{}{
		1: {approvalItem("a"), approvalItem("b")},
		2: {approvalItem("c"), approvalItem("d")},
	})
	server := httptest.NewServer(ps.handler(t))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ThreatLockerConfig{
		ClientOptions: testClientOptions(t),
		APIKey:        "k",
		BaseURL:       server.URL,
		PageSize:      2, // full page == 2 items, forcing multi-page walks
		PollInterval:  80 * time.Millisecond,
		DedupeTTL:     time.Hour,
	}
	adapter, _, err := NewThreatLockerAdapter(ctx, conf)
	require.NoError(t, err)

	// Allow several poll cycles to run.
	time.Sleep(400 * time.Millisecond)
	require.NoError(t, adapter.Close())

	// First poll: page1 (full, new) -> page2 (full, new) -> page3 (empty).
	// Later polls: page1 is entirely duplicates -> early-stop before page2.
	assert.GreaterOrEqual(t, ps.hits(1), 2, "page 1 should be polled repeatedly")
	assert.Equal(t, 1, ps.hits(2), "page 2 should be fetched once (only the first poll has new data)")
	assert.Equal(t, 1, ps.hits(3), "page 3 should be fetched once (terminates the first poll)")
}

// TestObjectEnvelopeResponse verifies the same incremental behaviour when the
// API wraps results in an object envelope instead of a bare array.
func TestObjectEnvelopeResponse(t *testing.T) {
	ps := newPagingServer(true, map[int][]map[string]interface{}{
		1: {approvalItem("a")},
		2: {approvalItem("b")},
	})
	server := httptest.NewServer(ps.handler(t))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ThreatLockerConfig{
		ClientOptions: testClientOptions(t),
		APIKey:        "k",
		BaseURL:       server.URL,
		PageSize:      1,
		PollInterval:  80 * time.Millisecond,
		DedupeTTL:     time.Hour,
	}
	adapter, _, err := NewThreatLockerAdapter(ctx, conf)
	require.NoError(t, err)

	time.Sleep(400 * time.Millisecond)
	require.NoError(t, adapter.Close())

	// page2/page3 fetched exactly once proves the envelope was parsed (records
	// extracted) AND deduplicated on later polls.
	assert.GreaterOrEqual(t, ps.hits(1), 2)
	assert.Equal(t, 1, ps.hits(2))
	assert.Equal(t, 1, ps.hits(3))
}

// TestTransientErrorRetry verifies the adapter retries 5xx responses rather
// than terminating.
func TestTransientErrorRetry(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requestCount.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":"transient"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ThreatLockerConfig{
		ClientOptions:  testClientOptions(t),
		APIKey:         "k",
		BaseURL:        server.URL,
		PollInterval:   100 * time.Millisecond,
		RetryBaseDelay: 20 * time.Millisecond,
		MaxRetryDelay:  40 * time.Millisecond,
	}
	adapter, chStopped, err := NewThreatLockerAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(500 * time.Millisecond)

	select {
	case <-chStopped:
		t.Fatal("adapter stopped on a transient error - it should have retried")
	default:
	}
	assert.GreaterOrEqual(t, requestCount.Load(), int32(3), "expected retries then success")
	assert.False(t, adapter.doStop.IsSet())
}

// TestPermanentAuthErrorStops verifies a 401/403 stops the whole adapter.
func TestPermanentAuthErrorStops(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				w.Write([]byte(`{"error":"denied"}`))
			}))
			defer server.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			conf := ThreatLockerConfig{
				ClientOptions: testClientOptions(t),
				APIKey:        "bad-key",
				BaseURL:       server.URL,
				PollInterval:  100 * time.Millisecond,
			}
			adapter, chStopped, err := NewThreatLockerAdapter(ctx, conf)
			require.NoError(t, err)
			defer adapter.Close()

			select {
			case <-chStopped:
				// Expected: an auth failure terminates the adapter.
			case <-time.After(3 * time.Second):
				t.Fatalf("adapter should have stopped on HTTP %d", status)
			}
			assert.True(t, adapter.doStop.IsSet())
		})
	}
}
