package usp_pandadoc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock PandaDoc API
// (GET /public/v1/logs), capturing the exact messages it ships so their
// content -- timestamp and verbatim payload -- can be asserted.
//
// Note: PandaDoc marks /public/v1/logs deprecated (sunset 2027-02-04) in
// favor of the identically-shaped /public/v2/logs
// (https://developers.pandadoc.com/reference/list-api-logs,
// https://developers.pandadoc.com/reference/listlogsv2); the adapter still
// targets v1, so the mock does too.

// mockPageSize mirrors the PandaDoc API's default page size of 100 records;
// the adapter does not send a `count` parameter and assumes a short page
// (fewer than 100 records) means the result set is exhausted.
const mockPageSize = 100

// queryTimeLayout is the layout the adapter renders `since`/`to` in: ISO-8601
// with millisecond precision and no zone designator (interpreted as UTC).
const queryTimeLayout = "2006-01-02T15:04:05.000"

// --- in-memory USP sink -----------------------------------------------------

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

// --- mock PandaDoc API --------------------------------------------------------

// recordedRequest captures the shape of one request the adapter made, so tests
// can pin the wire contract (method, auth header, query parameters).
type recordedRequest struct {
	method string
	auth   string
	since  string
	to     string
	page   int
}

// mockPandaDoc is an in-memory stand-in for the PandaDoc audit-logs API
// (GET /public/v1/logs). It validates the `Authorization: Api-Key <key>`
// header, honours the adapter's `since`/`to` time window and 1-based `page`
// parameter, paginates over an in-memory log set in pages of 100 (the API's
// default `count`), and returns the {"results": [...]} envelope.
type mockPandaDoc struct {
	mu       sync.Mutex
	apiKey   string
	logs     []utils.Dict // ordered oldest-first
	requests []recordedRequest
}

func newMockPandaDoc(apiKey string) *mockPandaDoc {
	return &mockPandaDoc{apiKey: apiKey}
}

func (m *mockPandaDoc) addLogs(entries ...utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, entries...)
}

func (m *mockPandaDoc) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

func (m *mockPandaDoc) requestsSnapshot() []recordedRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]recordedRequest, len(m.requests))
	copy(out, m.requests)
	return out
}

// maxPageSeen reports the largest `page` query value any request carried.
func (m *mockPandaDoc) maxPageSeen() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	max := 0
	for _, r := range m.requests {
		if r.page > max {
			max = r.page
		}
	}
	return max
}

// checkAuth validates the Authorization header. The adapter sends
// "Api-Key <key>"; PandaDoc documents the scheme as "API-Key"
// (https://developers.pandadoc.com/reference/api-key-authentication-process),
// and HTTP auth schemes are case-insensitive (RFC 9110 §11.1), so the scheme
// is matched case-insensitively while the key itself must match exactly.
func (m *mockPandaDoc) checkAuth(header string) bool {
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 {
		return false
	}
	return strings.EqualFold(parts[0], "api-key") && parts[1] == m.apiKey
}

func (m *mockPandaDoc) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	q := r.URL.Query()
	page := 1
	if p, err := strconv.Atoi(q.Get("page")); err == nil && p >= 1 {
		page = p
	}
	m.requests = append(m.requests, recordedRequest{
		method: r.Method,
		auth:   r.Header.Get("Authorization"),
		since:  q.Get("since"),
		to:     q.Get("to"),
		page:   page,
	})

	if r.Method != http.MethodGet {
		writeJSONStatus(w, http.StatusMethodNotAllowed, utils.Dict{"detail": "method not allowed"})
		return
	}
	if !m.checkAuth(r.Header.Get("Authorization")) {
		writeJSONStatus(w, http.StatusUnauthorized, utils.Dict{
			"type":   "authorization_error",
			"detail": "Authentication credentials were not provided or are incorrect.",
		})
		return
	}

	since, err := parseQueryTime(q, "since")
	if err != nil {
		writeJSONStatus(w, http.StatusBadRequest, utils.Dict{"type": "request_error", "detail": err.Error()})
		return
	}
	to, err := parseQueryTime(q, "to")
	if err != nil {
		writeJSONStatus(w, http.StatusBadRequest, utils.Dict{"type": "request_error", "detail": err.Error()})
		return
	}

	// Filter the dataset to the requested window (inclusive bounds).
	var window []utils.Dict
	for _, entry := range m.logs {
		rt, ok := entry["request_time"].(string)
		if !ok {
			continue
		}
		t, perr := time.Parse(time.RFC3339Nano, rt)
		if perr != nil {
			continue
		}
		if t.Before(since) || t.After(to) {
			continue
		}
		window = append(window, entry)
	}

	// Paginate; page is 1-based, pages hold mockPageSize records.
	var pageItems []utils.Dict
	if start := (page - 1) * mockPageSize; start < len(window) {
		end := start + mockPageSize
		if end > len(window) {
			end = len(window)
		}
		pageItems = window[start:end]
	}
	if pageItems == nil {
		pageItems = []utils.Dict{}
	}

	writeJSONStatus(w, http.StatusOK, utils.Dict{"results": pageItems})
}

// parseQueryTime parses a `since`/`to` query value in the exact layout the
// adapter renders, interpreted as UTC.
func parseQueryTime(q url.Values, key string) (time.Time, error) {
	v := q.Get(key)
	if v == "" {
		return time.Time{}, fmt.Errorf("missing %q query parameter", key)
	}
	t, err := time.ParseInLocation(queryTimeLayout, v, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid %q query parameter %q: %v", key, v, err)
	}
	return t, nil
}

func writeJSONStatus(w http.ResponseWriter, status int, d utils.Dict) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(d)
}

// --- realistic log fixtures ---------------------------------------------------

// fixtureTimeLayout renders fixture timestamps with millisecond precision and
// an explicit zone designator. PandaDoc's documented example timestamps
// ("2024-07-15T18:59:38.000") carry millisecond precision but no zone; the
// fixture appends one because the adapter parses request_time with
// time.RFC3339Nano (see makeOneRequest in client.go), which requires it.
const fixtureTimeLayout = "2006-01-02T15:04:05.000Z07:00"

// auditLogEntry returns a record shaped like a documented PandaDoc API log
// list entry (https://developers.pandadoc.com/reference/list-api-logs):
// exactly id, url, method, status, request_time and response_time. Richer
// fields (request_body, token_type, application, ...) only appear on the log
// details endpoint (/public/v1/logs/{id}), which the adapter never calls.
func auditLogEntry(id string, requestTime time.Time) utils.Dict {
	return utils.Dict{
		"id":            id,
		"url":           "/public/v1/documents/00000000000000000000000/send",
		"method":        "POST",
		"status":        201,
		"request_time":  requestTime.UTC().Format(fixtureTimeLayout),
		"response_time": requestTime.Add(150 * time.Millisecond).UTC().Format(fixtureTimeLayout),
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// startTestAdapter builds the adapter against the mock server with the capture
// sink injected and a fast poll interval.
func startTestAdapter(t *testing.T, serverURL string, apiKey string, sink uspSink) (*PandaDocAdapter, chan struct{}) {
	t.Helper()
	conf := PandaDocConfig{
		ClientOptions: testClientOptions(t),
		ApiKey:        apiKey,
		URL:           serverURL,
		PollInterval:  20 * time.Millisecond,
	}
	adapter, chStopped, err := newPandaDocAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	return adapter, chStopped
}

// shippedIDCounts tallies how many times each log id was shipped.
func shippedIDCounts(msgs []*protocol.DataMessage) map[string]int {
	counts := map[string]int{}
	for _, m := range msgs {
		id, _ := m.JsonPayload["id"].(string)
		counts[id]++
	}
	return counts
}

// --- tests ---------------------------------------------------------------------

// TestMockLogsEndToEnd drives the adapter against the mock API and asserts the
// exact events shipped: the payload preserved verbatim, TimestampMs stamped at
// ship time (the adapter's actual behavior -- it does not use the record's
// request_time for the message timestamp), no EventType set, and that
// re-polling the same window does not re-ship.
func TestMockLogsEndToEnd(t *testing.T) {
	const apiKey = "fake-test-api-key-0000000000000000"

	mock := newMockPandaDoc(apiKey)
	server := httptest.NewServer(mock)
	defer server.Close()

	// The adapter only collects logs newer than its start time, so fixtures are
	// stamped slightly in the future of the adapter's startup instant.
	base := time.Now().Add(100 * time.Millisecond)
	want := []utils.Dict{
		auditLogEntry("log-aaaaaaaaaaaaaaaaaaaa01", base),
		auditLogEntry("log-aaaaaaaaaaaaaaaaaaaa02", base.Add(2*time.Millisecond)),
		auditLogEntry("log-aaaaaaaaaaaaaaaaaaaa03", base.Add(4*time.Millisecond)),
	}
	mock.addLogs(want...)

	before := uint64(time.Now().UnixMilli())
	sink := &captureSink{}
	adapter, _ := startTestAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond, "expected all 3 log entries to ship")
	after := uint64(time.Now().UnixMilli())

	// Re-polling must not re-ship: the count stays at 3 across many polls.
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 20*time.Millisecond, "records were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		require.NotNil(t, msg.JsonPayload)
		assert.Empty(t, msg.EventType, "the adapter does not tag an EventType")
		// The adapter stamps messages at ship time, not from request_time.
		assert.GreaterOrEqual(t, msg.TimestampMs, before)
		assert.LessOrEqual(t, msg.TimestampMs, after)
		id, _ := msg.JsonPayload["id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "record %s was not shipped", id)
		// The payload is shipped verbatim, every documented field intact.
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original PandaDoc log entry")
	}
}

// TestMockRequestShape pins the wire contract the adapter speaks: GET requests
// carrying the Api-Key Authorization header, a millisecond-precision since/to
// window, and a 1-based page counter starting at 1.
func TestMockRequestShape(t *testing.T) {
	const apiKey = "fake-test-api-key-1111111111111111"

	mock := newMockPandaDoc(apiKey)
	server := httptest.NewServer(mock)
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startTestAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return mock.requestCount() >= 2 },
		5*time.Second, 10*time.Millisecond, "the adapter should poll repeatedly")

	for _, req := range mock.requestsSnapshot() {
		assert.Equal(t, http.MethodGet, req.method)
		assert.Equal(t, "Api-Key "+apiKey, req.auth)
		assert.Equal(t, 1, req.page, "an empty result set never advances past page 1")

		since, err := time.ParseInLocation(queryTimeLayout, req.since, time.UTC)
		require.NoError(t, err, "since must use the %s layout", queryTimeLayout)
		to, err := time.ParseInLocation(queryTimeLayout, req.to, time.UTC)
		require.NoError(t, err, "to must use the %s layout", queryTimeLayout)
		assert.False(t, to.Before(since), "the window must not be inverted")
	}
}

// TestMockMidRunEventShipsOnce verifies a log entry that appears mid-run is
// shipped exactly once, and already-shipped entries are never re-sent.
func TestMockMidRunEventShipsOnce(t *testing.T) {
	const apiKey = "fake-test-api-key-2222222222222222"

	mock := newMockPandaDoc(apiKey)
	server := httptest.NewServer(mock)
	defer server.Close()

	base := time.Now().Add(100 * time.Millisecond)
	mock.addLogs(
		auditLogEntry("log-bbbbbbbbbbbbbbbbbbbb01", base),
		auditLogEntry("log-bbbbbbbbbbbbbbbbbbbb02", base.Add(2*time.Millisecond)),
	)

	sink := &captureSink{}
	adapter, _ := startTestAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 10*time.Millisecond)

	// A new API call gets logged while the adapter is running.
	mock.addLogs(auditLogEntry("log-bbbbbbbbbbbbbbbbbbbb03", time.Now().Add(100*time.Millisecond)))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond, "the new log entry should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 20*time.Millisecond)

	assert.Equal(t, map[string]int{
		"log-bbbbbbbbbbbbbbbbbbbb01": 1,
		"log-bbbbbbbbbbbbbbbbbbbb02": 1,
		"log-bbbbbbbbbbbbbbbbbbbb03": 1,
	}, shippedIDCounts(sink.snapshot()), "every log entry must ship exactly once")
}

// TestMockPaginationFullDataset verifies a window holding more than one page of
// records (the adapter assumes 100-record pages) is walked completely and every
// record ships exactly once.
func TestMockPaginationFullDataset(t *testing.T) {
	const apiKey = "fake-test-api-key-3333333333333333"
	const total = 250 // 3 pages: 100 + 100 + 50

	mock := newMockPandaDoc(apiKey)
	server := httptest.NewServer(mock)
	defer server.Close()

	// All records share one request_time so a single poll window holds the
	// entire dataset, forcing a multi-page walk.
	at := time.Now().Add(100 * time.Millisecond)
	entries := make([]utils.Dict, total)
	for i := range entries {
		entries[i] = auditLogEntry(fmt.Sprintf("log-page-%03d", i), at)
	}
	mock.addLogs(entries...)

	sink := &captureSink{}
	adapter, _ := startTestAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 10*time.Millisecond, "all paginated records should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 20*time.Millisecond, "no record may ship twice")

	counts := shippedIDCounts(sink.snapshot())
	assert.Len(t, counts, total, "every distinct record should ship")
	for id, n := range counts {
		assert.Equalf(t, 1, n, "record %s shipped %d times", id, n)
	}

	// The adapter walked past page 2: a 100-record page means "maybe more".
	assert.GreaterOrEqual(t, mock.maxPageSeen(), 3, "a 250-record window requires 3 pages")
}

// TestMockBadAPIKeyShipsNothing pins the adapter's actual behavior on an auth
// failure: nothing is shipped, the error is surfaced via OnError, and the
// adapter does not stop -- it keeps polling on its interval.
func TestMockBadAPIKeyShipsNothing(t *testing.T) {
	mock := newMockPandaDoc("the-correct-fake-key")
	server := httptest.NewServer(mock)
	defer server.Close()

	mock.addLogs(auditLogEntry("log-cccccccccccccccccccc01", time.Now().Add(100*time.Millisecond)))

	var mu sync.Mutex
	var apiErrors int
	opts := testClientOptions(t)
	innerOnError := opts.OnError
	opts.OnError = func(err error) {
		innerOnError(err)
		mu.Lock()
		apiErrors++
		mu.Unlock()
	}

	sink := &captureSink{}
	conf := PandaDocConfig{
		ClientOptions: opts,
		ApiKey:        "the-wrong-fake-key",
		URL:           server.URL,
		PollInterval:  20 * time.Millisecond,
	}
	adapter, chStopped, err := newPandaDocAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// The adapter keeps polling despite the 401s...
	require.Eventually(t, func() bool { return mock.requestCount() >= 3 },
		5*time.Second, 10*time.Millisecond, "the adapter should keep polling after a 401")
	// ...reports the failure...
	mu.Lock()
	gotErrors := apiErrors
	mu.Unlock()
	assert.GreaterOrEqual(t, gotErrors, 1, "a 401 must be surfaced via OnError")
	// ...does not stop on its own...
	select {
	case <-chStopped:
		t.Fatal("the adapter is not expected to stop on an auth failure (current behavior)")
	default:
	}
	// ...and never ships anything.
	assert.Equal(t, 0, sink.count(), "nothing may ship when authentication fails")
}
