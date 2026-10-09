package usp_itglue

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock IT Glue API,
// capturing the exact messages it ships so their content can be asserted.
//
// The mock reproduces the IT Glue logs API (GET /logs, documented at
// https://api.itglue.com/developer/ under "Logs") as the adapter uses it:
// authenticated with the x-api-key header, taking sort=created_at (the only
// documented sort for /logs) and page[size]/page[number] query parameters
// (page size capped at 1000 per the docs), and returning the JSON:API
// envelope: {"data": [...], "links": {...}, "meta": {...}} -- IT Glue states
// it conforms to the JSON API spec (jsonapi.org), which nests pagination URLs
// under top-level "links", and the official pagination article
// (https://help.itglue.kaseya.com/help/Content/1-admin/it-glue-api/pagination-in-the-it-glue-api.html)
// documents the "meta" page counters (total-pages, total-count). Note the real
// /logs endpoint is additionally "limited to 5 pages of results" (page[number]
// "Must be a value between 1 and 5 (inclusive)"); the documented way to iterate
// further is filter[created_at] plus the created_at sort. The mock does not
// enforce that cap -- the tests never page past 3 -- but it matters when
// reasoning about the real API.
//
// filter[created_at]: the official docs only document a comma-separated range
// ("start_date, end_date", with "*" for an unbounded side). The adapter sends
// a single timestamp instead -- an undocumented form -- so the mock implements
// the semantics the adapter's 30s-lookback watermark relies on: "created at or
// after this RFC3339 time".

// itglueCreatedAtLayout matches the created-at format in the documented /logs
// example response ("2021-07-24T00:55:31.000Z": RFC3339, UTC, milliseconds).
const itglueCreatedAtLayout = "2006-01-02T15:04:05.000Z07:00"

// --- in-memory USP sink -------------------------------------------------------

// captureSink is an in-memory uspSink that records every shipped message.
type captureSink struct {
	mu       sync.Mutex
	messages []*protocol.DataMessage
}

func (s *captureSink) Ship(m *protocol.DataMessage, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, m)
	return nil
}

func (s *captureSink) Drain(time.Duration) error               { return nil }
func (s *captureSink) Close() ([]*protocol.DataMessage, error) { return nil, nil }

func (s *captureSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.messages)
}

func (s *captureSink) snapshot() []*protocol.DataMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*protocol.DataMessage, len(s.messages))
	copy(out, s.messages)
	return out
}

// idsShipped returns the distinct log ids shipped so far.
func (s *captureSink) idsShipped() map[string]int {
	ids := map[string]int{}
	for _, m := range s.snapshot() {
		if id, ok := m.JsonPayload["id"].(string); ok {
			ids[id]++
		}
	}
	return ids
}

// --- mock IT Glue API ---------------------------------------------------------

// recordedRequest captures the request properties the tests assert on.
type recordedRequest struct {
	method      string
	path        string
	apiKey      string
	contentType string
	sortParam   string
	pageSize    string
	pageNumber  int
	filter      string
	receivedAt  time.Time
}

// mockITGlue is an in-memory stand-in for the IT Glue logs API.
type mockITGlue struct {
	mu     sync.Mutex
	apiKey string
	logs   []utils.Dict // JSON:API "logs" resources, each with attributes/created-at

	// serverPageSize, when > 0, caps the effective page size below what the
	// client requested -- the lever used to make the dataset span multiple
	// pages without 1000+ fixtures.
	serverPageSize int

	requests []recordedRequest
	pageHits map[int]int // page[number] -> request count
}

func newMockITGlue(apiKey string) *mockITGlue {
	return &mockITGlue{apiKey: apiKey, pageHits: map[int]int{}}
}

func (m *mockITGlue) setLogs(logs []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = logs
}

func (m *mockITGlue) addLog(record utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, record)
}

func (m *mockITGlue) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

func (m *mockITGlue) lastRequest() recordedRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.requests) == 0 {
		return recordedRequest{}
	}
	return m.requests[len(m.requests)-1]
}

func (m *mockITGlue) hitsForPage(n int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pageHits[n]
}

func createdAtOf(record utils.Dict) time.Time {
	attrs, _ := record.GetDict("attributes")
	s, _ := attrs.GetString("created-at")
	ts, _ := time.Parse(time.RFC3339Nano, s)
	return ts
}

func (m *mockITGlue) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		pageNumber := 1
		if v, err := strconv.Atoi(q.Get("page[number]")); err == nil && v >= 1 {
			pageNumber = v
		}
		rec := recordedRequest{
			method:      r.Method,
			path:        r.URL.Path,
			apiKey:      r.Header.Get("x-api-key"),
			contentType: r.Header.Get("Content-Type"),
			sortParam:   q.Get("sort"),
			pageSize:    q.Get("page[size]"),
			pageNumber:  pageNumber,
			filter:      q.Get("filter[created_at]"),
			receivedAt:  time.Now(),
		}

		m.mu.Lock()
		m.requests = append(m.requests, rec)
		m.pageHits[pageNumber]++
		logs := make([]utils.Dict, len(m.logs))
		copy(logs, m.logs)
		serverPageSize := m.serverPageSize
		m.mu.Unlock()

		if r.Method != http.MethodGet || r.URL.Path != logsURL {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"errors":[{"status":"404","title":"Not Found"}]}`))
			return
		}
		// IT Glue authenticates with the key in the x-api-key header. Per the
		// developer docs ("API Key" section), "Trying to access the API with
		// an incorrect key will result in an HTTP 403 error."
		if rec.apiKey != m.apiKey {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"errors":[{"status":"403","title":"Forbidden","detail":"incorrect api key"}]}`))
			return
		}

		// filter[created_at]: keep logs created at or after the watermark.
		var since time.Time
		if rec.filter != "" {
			ts, err := time.Parse(time.RFC3339Nano, rec.filter)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"errors":[{"status":"400","title":"Bad Request","detail":"invalid filter[created_at]"}]}`))
				return
			}
			since = ts
		}
		matched := make([]utils.Dict, 0, len(logs))
		for _, record := range logs {
			if !createdAtOf(record).Before(since) {
				matched = append(matched, record)
			}
		}
		// sort=created_at: ascending by creation time.
		sort.Slice(matched, func(i, j int) bool {
			return createdAtOf(matched[i]).Before(createdAtOf(matched[j]))
		})

		// Paginate. The effective page size is the client's page[size] capped
		// by the server (the real API caps at 1000).
		pageSize := 1000
		if v, err := strconv.Atoi(rec.pageSize); err == nil && v >= 1 && v < pageSize {
			pageSize = v
		}
		if serverPageSize > 0 && serverPageSize < pageSize {
			pageSize = serverPageSize
		}
		totalPages := (len(matched) + pageSize - 1) / pageSize
		if totalPages == 0 {
			totalPages = 1
		}
		page := []utils.Dict{}
		if start := (pageNumber - 1) * pageSize; start < len(matched) {
			end := start + pageSize
			if end > len(matched) {
				end = len(matched)
			}
			page = matched[start:end]
		}

		// JSON:API envelope: pagination URLs live under top-level "links", and
		// page counters under "meta" -- not at the top level.
		pageURL := func(n int) string {
			v := url.Values{}
			v.Set("filter[created_at]", rec.filter)
			v.Set("sort", "created_at")
			v.Set("page[number]", strconv.Itoa(n))
			v.Set("page[size]", strconv.Itoa(pageSize))
			return "http://" + r.Host + logsURL + "?" + v.Encode()
		}
		links := map[string]interface{}{"self": pageURL(pageNumber)}
		meta := map[string]interface{}{
			"current-page": pageNumber,
			"prev-page":    nil,
			"next-page":    nil,
			"total-pages":  totalPages,
			"total-count":  len(matched),
			"filters":      map[string]interface{}{"created_at": rec.filter},
		}
		if pageNumber > 1 {
			links["prev"] = pageURL(pageNumber - 1)
			meta["prev-page"] = pageNumber - 1
		}
		if pageNumber < totalPages {
			links["next"] = pageURL(pageNumber + 1)
			links["last"] = pageURL(totalPages)
			meta["next-page"] = pageNumber + 1
		}

		w.Header().Set("Content-Type", "application/vnd.api+json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data":  page,
			"links": links,
			"meta":  meta,
		})
	}
}

// --- realistic fixtures -------------------------------------------------------

// realisticLogRecord returns a JSON:API resource shaped like an IT Glue
// activity log: a string id, type "logs", and exactly the kebab-case
// attributes shown in the documented GET /logs example response at
// https://api.itglue.com/developer/ (created-at, level, account-id, user-id,
// organization-id, action, resource-type, resource-name, ip-address, ip-city,
// ip-region, ip-country, user-agent). All identifying values are clearly fake
// (RFC 5737 test addresses, example-style names).
func realisticLogRecord(id string, createdAt time.Time) utils.Dict {
	return utils.Dict{
		"id":   id,
		"type": "logs",
		"attributes": utils.Dict{
			"created-at":      createdAt.UTC().Format(itglueCreatedAtLayout),
			"level":           2,
			"account-id":      1111111,
			"user-id":         3333333,
			"organization-id": 2222222,
			"action":          "viewed",
			"resource-type":   "Password",
			"resource-name":   "Example Firewall Admin",
			"ip-address":      "203.0.113.10",
			"ip-city":         "Example City",
			"ip-region":       "EX",
			"ip-country":      "Exampleland",
			"user-agent":      "Mozilla/5.0 (TestRunner) ExampleBrowser/1.0",
		},
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// startAdapter spins up the adapter against the mock server with a fast poll
// interval and returns the sink it ships into.
func startAdapter(t *testing.T, serverURL, token string) (*ITGlueAdapter, chan struct{}, *captureSink) {
	t.Helper()
	sink := &captureSink{}
	conf := ITGlueConfig{
		ClientOptions: testClientOptions(t),
		Token:         token,
		BaseURL:       serverURL,
		PollInterval:  40 * time.Millisecond,
	}
	adapter, chStopped, err := newITGlueAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	return adapter, chStopped, sink
}

// --- tests --------------------------------------------------------------------

// TestMockLogsEndToEnd drives the adapter against the mock API and asserts the
// exact events shipped: the payload is the verbatim JSON:API log resource, the
// timestamp is the ingestion time (the adapter stamps time.Now, it does not
// parse created-at), no EventType is set, and the request the adapter makes
// carries the documented auth header, content type, sort, page size and a
// created_at watermark 30 seconds in the past.
func TestMockLogsEndToEnd(t *testing.T) {
	const token = "itg.fake-api-key-1111"

	now := time.Now()
	want := []utils.Dict{
		realisticLogRecord("9000001", now.Add(-5*time.Second)),
		realisticLogRecord("9000002", now.Add(-3*time.Second)),
		realisticLogRecord("9000003", now.Add(-1*time.Second)),
	}
	mock := newMockITGlue(token)
	mock.setLogs(want)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	testStart := time.Now()
	adapter, _, sink := startAdapter(t, server.URL, token)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() >= 3 },
		5*time.Second, 10*time.Millisecond, "expected all 3 logs to ship")

	// The first three messages are the first poll's page, in created_at order.
	first := sink.snapshot()[:3]
	for i, msg := range first {
		assert.Empty(t, msg.EventType, "the adapter does not tag an event type")
		require.NotNil(t, msg.JsonPayload)

		// Payload is shipped verbatim: the whole JSON:API resource, id, type
		// and nested attributes included.
		assert.JSONEq(t, mustJSON(t, want[i]), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original IT Glue log resource")

		// The adapter stamps ingestion time, not the log's created-at.
		assert.GreaterOrEqual(t, msg.TimestampMs, uint64(testStart.UnixMilli()))
		assert.LessOrEqual(t, msg.TimestampMs, uint64(time.Now().UnixMilli()))
	}

	// The request the adapter built matches the IT Glue logs API contract.
	req := mock.lastRequest()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, "/logs", req.path)
	assert.Equal(t, token, req.apiKey)
	assert.Equal(t, "application/vnd.api+json", req.contentType)
	assert.Equal(t, "created_at", req.sortParam)
	assert.Equal(t, "1000", req.pageSize)
	filterTime, err := time.Parse(time.RFC3339Nano, req.filter)
	require.NoError(t, err, "filter[created_at] must be a valid RFC3339 time")
	lookback := req.receivedAt.Sub(filterTime)
	assert.Greater(t, lookback, 29*time.Second, "watermark should be ~30s in the past")
	assert.Less(t, lookback, 35*time.Second, "watermark should be ~30s in the past")
}

// TestMockRepollReshipsEventsStillInWindow pins the adapter's actual re-poll
// behavior: it has no deduplication, and every poll re-requests everything
// created in the last 30 seconds. A log still inside that lookback window is
// therefore shipped again on each poll. (In production the poll interval
// equals the lookback, so a log ships once or twice; here the fast test poll
// makes the duplication obvious.)
func TestMockRepollReshipsEventsStillInWindow(t *testing.T) {
	const token = "itg.fake-api-key-1111"

	now := time.Now()
	mock := newMockITGlue(token)
	mock.setLogs([]utils.Dict{
		realisticLogRecord("9000001", now.Add(-2*time.Second)),
		realisticLogRecord("9000002", now.Add(-1*time.Second)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	adapter, _, sink := startAdapter(t, server.URL, token)
	defer adapter.Close()

	// At least three polls' worth of shipments: both logs re-ship every poll.
	require.Eventually(t, func() bool { return sink.count() >= 6 },
		5*time.Second, 10*time.Millisecond,
		"in-window logs are re-shipped on every poll (the adapter does not dedupe)")

	ids := sink.idsShipped()
	assert.GreaterOrEqual(t, ids["9000001"], 2, "log 9000001 re-ships while in the lookback window")
	assert.GreaterOrEqual(t, ids["9000002"], 2, "log 9000002 re-ships while in the lookback window")
	assert.Len(t, ids, 2, "no other ids ship")
}

// TestMockOldEventsNeverShip verifies the created_at watermark works end to
// end: logs older than the 30s lookback are filtered out by the API and never
// ship, poll after poll.
func TestMockOldEventsNeverShip(t *testing.T) {
	const token = "itg.fake-api-key-1111"

	mock := newMockITGlue(token)
	mock.setLogs([]utils.Dict{
		realisticLogRecord("9000001", time.Now().Add(-5*time.Minute)),
		realisticLogRecord("9000002", time.Now().Add(-10*time.Minute)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	adapter, _, sink := startAdapter(t, server.URL, token)
	defer adapter.Close()

	// Let several polls complete before concluding nothing shipped.
	require.Eventually(t, func() bool { return mock.requestCount() >= 3 },
		5*time.Second, 10*time.Millisecond, "the adapter should keep polling")
	assert.Equal(t, 0, sink.count(), "logs older than the lookback window must not ship")
}

// TestMockMidRunEventShips verifies a log that appears while the adapter is
// running is picked up by a subsequent poll and shipped. (It ships at least
// once; this adapter re-ships while a log remains inside the 30s lookback, as
// pinned by TestMockRepollReshipsEventsStillInWindow.)
func TestMockMidRunEventShips(t *testing.T) {
	const token = "itg.fake-api-key-1111"

	mock := newMockITGlue(token)
	mock.setLogs([]utils.Dict{
		realisticLogRecord("9000001", time.Now().Add(-1*time.Second)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	adapter, _, sink := startAdapter(t, server.URL, token)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.idsShipped()["9000001"] >= 1 },
		5*time.Second, 10*time.Millisecond, "the initial log should ship")

	// A new activity log is written mid-run.
	mock.addLog(realisticLogRecord("9000099", time.Now()))

	require.Eventually(t, func() bool { return sink.idsShipped()["9000099"] >= 1 },
		5*time.Second, 10*time.Millisecond, "the mid-run log should ship on a later poll")
}

// TestMockPaginationOnlyFirstPageConsumed pins the adapter's actual pagination
// behavior against the real IT Glue envelope -- a known limitation. IT Glue,
// per the JSON API spec, returns the next-page URL nested under top-level
// "links" ({"links": {"next": ...}}), but the adapter looks the cursor up with
// FindOneString("next"), which only matches a *top-level* "next" key. The
// cursor is therefore never found: only page[number]=1 is ever requested, and
// records beyond the first page are never shipped. If the adapter is ever
// fixed to read "links/next", this test must be updated to assert full
// multi-page consumption.
func TestMockPaginationOnlyFirstPageConsumed(t *testing.T) {
	const token = "itg.fake-api-key-1111"

	now := time.Now()
	mock := newMockITGlue(token)
	mock.serverPageSize = 2 // 5 in-window logs => 3 pages
	mock.setLogs([]utils.Dict{
		realisticLogRecord("9000001", now.Add(-5*time.Second)),
		realisticLogRecord("9000002", now.Add(-4*time.Second)),
		realisticLogRecord("9000003", now.Add(-3*time.Second)),
		realisticLogRecord("9000004", now.Add(-2*time.Second)),
		realisticLogRecord("9000005", now.Add(-1*time.Second)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	adapter, _, sink := startAdapter(t, server.URL, token)
	defer adapter.Close()

	// Wait for several polls so the adapter had every opportunity to follow
	// the links.next URL it was given.
	require.Eventually(t, func() bool { return mock.hitsForPage(1) >= 3 },
		5*time.Second, 10*time.Millisecond)

	assert.Equal(t, 0, mock.hitsForPage(2),
		"the JSON:API links.next cursor is never followed (FindOneString(\"next\") only matches a top-level key)")

	ids := sink.idsShipped()
	assert.NotContains(t, ids, "9000003", "second-page records never ship")
	assert.NotContains(t, ids, "9000004", "second-page records never ship")
	assert.NotContains(t, ids, "9000005", "third-page records never ship")
	assert.GreaterOrEqual(t, ids["9000001"], 1, "first-page records ship")
	assert.GreaterOrEqual(t, ids["9000002"], 1, "first-page records ship")
}

// TestMockBadAPIKeyShipsNothing verifies that with a rejected API key nothing
// is ever shipped, and pins the adapter's actual error handling: a non-200 is
// reported through OnWarning and the adapter keeps polling -- it does not stop.
func TestMockBadAPIKeyShipsNothing(t *testing.T) {
	mock := newMockITGlue("itg.correct-key-1111")
	mock.setLogs([]utils.Dict{
		realisticLogRecord("9000001", time.Now()),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	var non200Warnings atomic.Int32
	opts := testClientOptions(t)
	opts.OnWarning = func(msg string) {
		t.Logf("WRN: %s", msg)
		if strings.Contains(msg, "non-200") {
			non200Warnings.Add(1)
		}
	}

	sink := &captureSink{}
	conf := ITGlueConfig{
		ClientOptions: opts,
		Token:         "itg.wrong-key-9999",
		BaseURL:       server.URL,
		PollInterval:  40 * time.Millisecond,
	}
	adapter, chStopped, err := newITGlueAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// The adapter keeps polling (and warning) despite the 403s.
	require.Eventually(t, func() bool { return mock.requestCount() >= 3 },
		5*time.Second, 10*time.Millisecond, "the adapter should keep retrying on auth failure")
	require.Eventually(t, func() bool { return non200Warnings.Load() >= 1 },
		5*time.Second, 10*time.Millisecond, "the 403 should be surfaced via OnWarning")

	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
	select {
	case <-chStopped:
		t.Fatal("the adapter stopped on an auth failure; its actual behavior is to keep polling")
	default:
	}
	assert.False(t, adapter.doStop.IsSet())
}

// TestMockMalformedResponseKeepsPolling pins the handling of a non-JSON 200
// response: the decode error is reported through OnError, nothing ships, and
// the adapter keeps polling.
func TestMockMalformedResponseKeepsPolling(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/vnd.api+json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("this is not json"))
	}))
	defer server.Close()

	var jsonErrors atomic.Int32
	opts := testClientOptions(t)
	opts.OnError = func(err error) {
		t.Logf("ERR: %v", err)
		if strings.Contains(err.Error(), "invalid json") {
			jsonErrors.Add(1)
		}
	}

	sink := &captureSink{}
	conf := ITGlueConfig{
		ClientOptions: opts,
		Token:         "itg.fake-api-key-1111",
		BaseURL:       server.URL,
		PollInterval:  40 * time.Millisecond,
	}
	adapter, chStopped, err := newITGlueAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return requests.Load() >= 3 },
		5*time.Second, 10*time.Millisecond, "the adapter should keep polling")
	require.Eventually(t, func() bool { return jsonErrors.Load() >= 1 },
		5*time.Second, 10*time.Millisecond, "the decode failure should be surfaced via OnError")

	assert.Equal(t, 0, sink.count())
	select {
	case <-chStopped:
		t.Fatal("the adapter stopped on a malformed response; its actual behavior is to keep polling")
	default:
	}
}

// TestMockCloseIsClean verifies Close stops the poller and the stopped channel
// closes.
func TestMockCloseIsClean(t *testing.T) {
	const token = "itg.fake-api-key-1111"

	mock := newMockITGlue(token)
	mock.setLogs([]utils.Dict{realisticLogRecord("9000001", time.Now())})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	adapter, chStopped, sink := startAdapter(t, server.URL, token)

	require.Eventually(t, func() bool { return sink.count() >= 1 },
		5*time.Second, 10*time.Millisecond)

	require.NoError(t, adapter.Close())
	select {
	case <-chStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("chStopped should close after Close()")
	}

	// No further polls happen after Close.
	n := mock.requestCount()
	require.Never(t, func() bool { return mock.requestCount() != n },
		200*time.Millisecond, 20*time.Millisecond)
}
