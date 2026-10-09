package usp_threatlocker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

// approvalRequestFeed is the single feed used by integration tests that focus
// on adapter behavior rather than the default feed set. Tests pin the config
// to this feed so they don't accidentally exercise unrelated default feeds
// (unified_audit, system_audit) on shared httptest servers.
func approvalRequestFeed() []ThreatLockerFeed {
	return []ThreatLockerFeed{{
		Name:           "approval_request",
		URL:            "ApprovalRequest/ApprovalRequestGetByParameters",
		Parameters:     utils.Dict{"statusId": 1},
		OrderBy:        "dateTime",
		TimestampField: "dateTime",
		IDField:        "approvalRequestId",
	}}
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

// decodeRequestBody decodes a JSON request body. It runs inside httptest
// handler goroutines, so it must not use require (whose FailNow/Goexit is only
// valid on the test goroutine); assert reports failures safely from any
// goroutine.
func decodeRequestBody(t *testing.T, r *http.Request) map[string]interface{} {
	t.Helper()
	m := map[string]interface{}{}
	body, err := io.ReadAll(r.Body)
	if !assert.NoError(t, err) {
		return m
	}
	assert.NoError(t, json.Unmarshal(body, &m))
	return m
}

// countingDeduper wraps a Deduper and records, per key, how many times the key
// was reported as new (CheckAndAdd returned false). The adapter ships a record
// exactly when its key is new, so a count above 1 means a record was shipped
// more than once -- a dedup failure.
type countingDeduper struct {
	inner utils.Deduper
	mu    sync.Mutex
	new   map[string]int
}

func newCountingDeduper(t *testing.T) *countingDeduper {
	t.Helper()
	// window == ttl gives a single bucket that never rotates within a test.
	inner, err := utils.NewLocalDeduper(time.Hour, time.Hour)
	require.NoError(t, err)
	return &countingDeduper{inner: inner, new: map[string]int{}}
}

func (d *countingDeduper) CheckAndAdd(key string) bool {
	exists := d.inner.CheckAndAdd(key)
	if !exists {
		d.mu.Lock()
		d.new[key]++
		d.mu.Unlock()
	}
	return exists
}

func (d *countingDeduper) Close() { d.inner.Close() }

func (d *countingDeduper) newCount(key string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.new[key]
}

func (d *countingDeduper) distinctKeys() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.new)
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

	t.Run("non-object records are skipped, valid ones kept", func(t *testing.T) {
		items, err := extractItems([]byte(`[{"id":"a"},"junk",123,null,{"id":"b"}]`), "")
		require.NoError(t, err)
		require.Len(t, items, 2)
		assert.Equal(t, "a", items[0].FindOneString("id"))
		assert.Equal(t, "b", items[1].FindOneString("id"))
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

	t.Run("accepts a numeric id of zero", func(t *testing.T) {
		// A real id of 0 must not be mistaken for "absent".
		feed := ThreatLockerFeed{IDField: "actionId"}
		assert.Equal(t, "0", recordID(feed, utils.Dict{"actionId": uint64(0)}))
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

	t.Run("rolling window injects start/end on every poll", func(t *testing.T) {
		aw := &ThreatLockerAdapter{conf: ThreatLockerConfig{PageSize: 50, PollInterval: 60 * time.Second}}
		feed := ThreatLockerFeed{
			Window:         5 * time.Minute,
			StartDateField: defaultStartDateField,
			EndDateField:   defaultEndDateField,
		}

		before := time.Now().UTC()
		body := aw.buildRequestBody(feed, 1)
		after := time.Now().UTC()

		startStr, ok := body[defaultStartDateField].(string)
		require.True(t, ok, "startDate must be a string, got %T", body[defaultStartDateField])
		endStr, ok := body[defaultEndDateField].(string)
		require.True(t, ok, "endDate must be a string, got %T", body[defaultEndDateField])

		start, err := time.Parse(windowTimestampLayout, startStr)
		require.NoError(t, err)
		end, err := time.Parse(windowTimestampLayout, endStr)
		require.NoError(t, err)

		// end should be ~ now, start should be window + poll_interval before end
		assert.False(t, end.Before(before.Truncate(time.Millisecond)), "end %v < before %v", end, before)
		assert.False(t, end.After(after.Add(time.Second)), "end %v > after+1s %v", end, after)
		gap := end.Sub(start)
		assert.Equal(t, feed.Window+aw.conf.PollInterval, gap,
			"start/end gap must equal window+poll_interval")
	})

	t.Run("no window means no date fields injected", func(t *testing.T) {
		feed := ThreatLockerFeed{OrderBy: "dateTime"}
		body := a.buildRequestBody(feed, 1)
		_, hasStart := body[defaultStartDateField]
		_, hasEnd := body[defaultEndDateField]
		assert.False(t, hasStart, "startDate must be absent when window is unset")
		assert.False(t, hasEnd, "endDate must be absent when window is unset")
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

	t.Run("applies defaults and the default feed set", func(t *testing.T) {
		c := ThreatLockerConfig{ClientOptions: testClientOptions(t), APIKey: "k", Instance: "g"}
		require.NoError(t, c.Validate())
		assert.Equal(t, defaultPageSize, c.PageSize)
		assert.Equal(t, defaultPollInterval, c.PollInterval)

		names := make([]string, 0, len(c.Feeds))
		for _, f := range c.Feeds {
			names = append(names, f.Name)
			assert.Equal(t, defaultMaxPages, f.MaxPages, "feed %q must inherit max_pages default", f.Name)
		}
		assert.ElementsMatch(t, []string{"approval_request", "unified_audit", "system_audit"}, names)

		// Feeds with a Window must also have start/end field defaults applied.
		for _, f := range c.Feeds {
			if f.Window > 0 {
				assert.Equal(t, defaultStartDateField, f.StartDateField, "feed %q", f.Name)
				assert.Equal(t, defaultEndDateField, f.EndDateField, "feed %q", f.Name)
			}
		}
	})

	t.Run("include_child_organizations toggles the child-org flags in the default feeds", func(t *testing.T) {
		// Off (the default): every child-org flag is false.
		off := ThreatLockerConfig{ClientOptions: testClientOptions(t), APIKey: "k", Instance: "g"}
		require.NoError(t, off.Validate())
		for _, f := range off.Feeds {
			switch f.Name {
			case "approval_request", "unified_audit":
				assert.Equal(t, false, f.Parameters["showChildOrganizations"], "feed %q", f.Name)
			case "system_audit":
				assert.Equal(t, false, f.Parameters["viewChildOrganizations"], "feed %q", f.Name)
			}
		}

		// On: the flags flip to true so a parent-org token sees its children.
		on := ThreatLockerConfig{ClientOptions: testClientOptions(t), APIKey: "k", Instance: "g", IncludeChildOrganizations: true}
		require.NoError(t, on.Validate())
		for _, f := range on.Feeds {
			switch f.Name {
			case "approval_request", "unified_audit":
				assert.Equal(t, true, f.Parameters["showChildOrganizations"], "feed %q", f.Name)
			case "system_audit":
				assert.Equal(t, true, f.Parameters["viewChildOrganizations"], "feed %q", f.Name)
			}
		}
	})

	t.Run("collect_* toggles select which default feeds run", func(t *testing.T) {
		feedNames := func(c ThreatLockerConfig) []string {
			require.NoError(t, c.Validate())
			names := make([]string, 0, len(c.Feeds))
			for _, f := range c.Feeds {
				names = append(names, f.Name)
			}
			return names
		}
		falsePtr := false

		// Dropping one feed leaves the other two.
		got := feedNames(ThreatLockerConfig{
			ClientOptions: testClientOptions(t), APIKey: "k", Instance: "g",
			CollectSystemAudit: &falsePtr,
		})
		assert.ElementsMatch(t, []string{"approval_request", "unified_audit"}, got)

		// Dropping two leaves exactly one.
		got = feedNames(ThreatLockerConfig{
			ClientOptions: testClientOptions(t), APIKey: "k", Instance: "g",
			CollectUnifiedAudit: &falsePtr, CollectSystemAudit: &falsePtr,
		})
		assert.ElementsMatch(t, []string{"approval_request"}, got)

		// Disabling all default feeds is an error.
		all := ThreatLockerConfig{
			ClientOptions: testClientOptions(t), APIKey: "k", Instance: "g",
			CollectApprovalRequests: &falsePtr, CollectUnifiedAudit: &falsePtr, CollectSystemAudit: &falsePtr,
		}
		assert.Error(t, all.Validate())

		// The toggles do not apply when a custom feeds list is supplied.
		custom := ThreatLockerConfig{
			ClientOptions: testClientOptions(t), APIKey: "k", Instance: "g",
			CollectApprovalRequests: &falsePtr, CollectUnifiedAudit: &falsePtr, CollectSystemAudit: &falsePtr,
			Feeds: []ThreatLockerFeed{{Name: "custom", URL: "X/XGetByParameters"}},
		}
		assert.Equal(t, []string{"custom"}, feedNames(custom))
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
		Feeds:                 approvalRequestFeed(),
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
		page := 1
		if pn, ok := body["pageNumber"].(float64); ok {
			page = int(pn)
		}

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

// TestPaginationAndDedup verifies the adapter walks every page on every poll
// and, despite re-fetching pages, ships each record exactly once.
func TestPaginationAndDedup(t *testing.T) {
	ps := newPagingServer(false, map[int][]map[string]interface{}{
		1: {approvalItem("a"), approvalItem("b")},
		2: {approvalItem("c"), approvalItem("d")},
	})
	server := httptest.NewServer(ps.handler(t))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dd := newCountingDeduper(t)
	conf := ThreatLockerConfig{
		ClientOptions: testClientOptions(t),
		APIKey:        "k",
		BaseURL:       server.URL,
		PageSize:      2, // a full page is 2 records, forcing a multi-page walk
		PollInterval:  40 * time.Millisecond,
		Deduper:       dd,
		Feeds:         approvalRequestFeed(),
	}
	adapter, _, err := NewThreatLockerAdapter(ctx, conf)
	require.NoError(t, err)

	// Wait for at least three polls to complete.
	require.Eventually(t, func() bool { return ps.hits(1) >= 3 },
		5*time.Second, 20*time.Millisecond)
	require.NoError(t, adapter.Close())

	// Every poll re-walks every page (there is no early-stop): pages 2 and 3
	// are fetched on every completed poll, not just the first.
	assert.GreaterOrEqual(t, ps.hits(2), 2, "every poll re-walks page 2")
	assert.GreaterOrEqual(t, ps.hits(3), 2, "every poll re-walks page 3 (empty, ends the walk)")

	// Despite being re-fetched on every poll, each record ships exactly once.
	for _, id := range []string{"a", "b", "c", "d"} {
		assert.Equal(t, 1, dd.newCount("approval_request|"+id),
			"record %q must ship exactly once", id)
	}
	assert.Equal(t, 4, dd.distinctKeys())
}

// TestObjectEnvelopeResponse verifies the adapter parses an object-envelope
// response ({"data": [...]}) and ships each record exactly once.
func TestObjectEnvelopeResponse(t *testing.T) {
	ps := newPagingServer(true, map[int][]map[string]interface{}{
		1: {approvalItem("a"), approvalItem("b")},
	})
	server := httptest.NewServer(ps.handler(t))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dd := newCountingDeduper(t)
	conf := ThreatLockerConfig{
		ClientOptions: testClientOptions(t),
		APIKey:        "k",
		BaseURL:       server.URL,
		PollInterval:  40 * time.Millisecond,
		Deduper:       dd,
		Feeds:         approvalRequestFeed(),
	}
	adapter, _, err := NewThreatLockerAdapter(ctx, conf)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return ps.hits(1) >= 3 },
		5*time.Second, 20*time.Millisecond)
	require.NoError(t, adapter.Close())

	// The envelope was parsed and its records extracted; each ships exactly
	// once even though page 1 is re-fetched on every poll.
	assert.Equal(t, 1, dd.newCount("approval_request|a"))
	assert.Equal(t, 1, dd.newCount("approval_request|b"))
	assert.Equal(t, 2, dd.distinctKeys())
}

// TestMaxPagesCap verifies a feed stops paginating at max_pages rather than
// walking an unbounded result set.
func TestMaxPagesCap(t *testing.T) {
	var mu sync.Mutex
	hits := map[int]int{}
	var idCounter atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := decodeRequestBody(t, r)
		page := 1
		if pn, ok := body["pageNumber"].(float64); ok {
			page = int(pn)
		}
		mu.Lock()
		hits[page]++
		mu.Unlock()
		// Always return a full page of brand-new records, so pagination would
		// never terminate on its own.
		items := []map[string]interface{}{
			approvalItem(fmt.Sprintf("rec-%d", idCounter.Add(1))),
			approvalItem(fmt.Sprintf("rec-%d", idCounter.Add(1))),
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(items)
	}))
	defer server.Close()

	var maxPagesWarned atomic.Bool
	opts := testClientOptions(t)
	opts.OnWarning = func(msg string) {
		t.Logf("WRN: %s", msg)
		if strings.Contains(msg, "max_pages") {
			maxPagesWarned.Store(true)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ThreatLockerConfig{
		ClientOptions: opts,
		APIKey:        "k",
		BaseURL:       server.URL,
		PageSize:      2,
		PollInterval:  1 * time.Hour, // only the initial poll runs during the test
		Feeds: []ThreatLockerFeed{
			{Name: "capped", URL: "Capped/CappedGetByParameters", MaxPages: 3},
		},
	}
	adapter, _, err := NewThreatLockerAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	// The poll must stop at the cap; the warning is emitted when it does.
	require.Eventually(t, maxPagesWarned.Load, 5*time.Second, 20*time.Millisecond,
		"adapter should warn when max_pages is reached")

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 0, hits[4], "page 4 must not be fetched (max_pages=3)")
	for p := 1; p <= 3; p++ {
		assert.GreaterOrEqual(t, hits[p], 1, "page %d should be fetched", p)
	}
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
		Feeds:          approvalRequestFeed(),
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

// TestPermanentSourceErrorDoesNotStop verifies that a permanent source-side
// error (401/403/500 from ThreatLocker) is reported as a warning and does NOT
// stop the adapter. The cloud-sensor host treats any fatal OnError as a reason
// to tear down, relaunch and eventually disable the whole adapter, so a problem
// on ThreatLocker's side (which a restart cannot fix) must stay non-fatal and
// let the feed retry on the next poll.
func TestPermanentSourceErrorDoesNotStop(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				w.Write([]byte(`{"error":"denied"}`))
			}))
			defer server.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var warnings, fatalErrors atomic.Int32
			opts := testClientOptions(t)
			opts.OnWarning = func(msg string) { warnings.Add(1); t.Logf("WRN: %s", msg) }
			opts.OnError = func(err error) { fatalErrors.Add(1); t.Logf("ERR: %v", err) }

			conf := ThreatLockerConfig{
				ClientOptions:  opts,
				APIKey:         "bad-key",
				BaseURL:        server.URL,
				PollInterval:   100 * time.Millisecond,
				RetryBaseDelay: 10 * time.Millisecond,
				MaxRetryDelay:  20 * time.Millisecond,
				Feeds:          approvalRequestFeed(),
			}
			adapter, chStopped, err := NewThreatLockerAdapter(ctx, conf)
			require.NoError(t, err)
			defer adapter.Close()

			// The feed should warn about the failure...
			require.Eventually(t, func() bool { return warnings.Load() > 0 },
				3*time.Second, 20*time.Millisecond, "expected a warning for the source error")

			// ...but the adapter must keep running and never escalate to a fatal
			// OnError or stop itself.
			select {
			case <-chStopped:
				t.Fatalf("adapter stopped on HTTP %d - source errors must be non-fatal", status)
			case <-time.After(300 * time.Millisecond):
			}
			assert.False(t, adapter.doStop.IsSet(), "doStop must not be set on a source error")
			assert.Equal(t, int32(0), fatalErrors.Load(),
				"source errors must be warnings, not fatal OnError")
		})
	}
}
