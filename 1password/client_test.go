package usp_1password

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests exercise the adapter against a mock 1Password Events API. The
// mock honours the documented contract: a POST with a Bearer token whose body
// is either a reset cursor ({start_time, limit}) for the first call or a
// {cursor} for continuation, returning {cursor, has_more, items} with
// cursor-based pagination.

const testToken = "test-bearer-token"

// --- in-memory USP sink -----------------------------------------------------

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

// --- mock 1Password Events API ----------------------------------------------

// mockOnePassword is an in-memory stand-in for the 1Password Events API. Each
// endpoint path serves its own (chronologically ordered) event set, paginated
// via a cursor that encodes the next offset and effective page size. maxPage
// emulates the server capping the requested limit so multi-page draining can be
// exercised. pathological makes every response advertise has_more with a frozen
// (empty) cursor, to prove the drain loop does not spin.
type mockOnePassword struct {
	mu           sync.Mutex
	token        string
	events       map[string][]utils.Dict
	maxPage      int
	pathological bool

	lastReq    map[string]opRequest
	badAuth    bool // last request carried a wrong/missing bearer token
	wrongCT    bool // last request had a non-JSON content type
	wrongVerb  bool // last request used a non-POST verb
	failStatus int  // when non-zero, respond with this status and no body
}

func newMockOnePassword(token string) *mockOnePassword {
	return &mockOnePassword{
		token:   token,
		events:  map[string][]utils.Dict{},
		lastReq: map[string]opRequest{},
	}
}

func (m *mockOnePassword) setEvents(path string, events []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events[path] = events
}

func (m *mockOnePassword) lastRequest(path string) opRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastReq[path]
}

func parseTestCursor(c string) (offset, size int) {
	parts := strings.SplitN(c, ":", 2)
	if len(parts) == 2 {
		offset, _ = strconv.Atoi(parts[0])
		size, _ = strconv.Atoi(parts[1])
	}
	return offset, size
}

func (m *mockOnePassword) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if r.Method != http.MethodPost {
		m.wrongVerb = true
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("Authorization") != fmt.Sprintf("Bearer %s", m.token) {
		m.badAuth = true
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "application/json" {
		m.wrongCT = true
	}
	if m.failStatus != 0 {
		w.WriteHeader(m.failStatus)
		return
	}

	var req opRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	m.lastReq[r.URL.Path] = req

	if m.pathological {
		writeJSON(w, utils.Dict{
			"cursor":   "",
			"has_more": true,
			"items":    m.events[r.URL.Path],
		})
		return
	}

	all := m.events[r.URL.Path]

	var offset, size int
	if req.Cursor == "" {
		// Reset cursor: start at the beginning using the requested limit.
		offset = 0
		size = req.Limit
		if size == 0 {
			size = 100
		}
	} else {
		offset, size = parseTestCursor(req.Cursor)
	}
	// Emulate the server capping the page size.
	if m.maxPage > 0 && size > m.maxPage {
		size = m.maxPage
	}

	end := offset + size
	if end > len(all) {
		end = len(all)
	}
	if offset > len(all) {
		offset = len(all)
	}

	writeJSON(w, utils.Dict{
		"cursor":   fmt.Sprintf("%d:%d", end, size),
		"has_more": end < len(all),
		"items":    all[offset:end],
	})
}

func writeJSON(w http.ResponseWriter, d utils.Dict) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(d)
}

// --- helpers ----------------------------------------------------------------

func testClientOptions(t *testing.T) uspclient.ClientOptions {
	return uspclient.ClientOptions{
		DebugLog:  func(s string) { t.Logf("debug: %s", s) },
		OnError:   func(e error) { t.Logf("error: %v", e) },
		OnWarning: func(s string) { t.Logf("warning: %s", s) },
	}
}

// newDirectAdapter builds an adapter wired to the mock without starting any
// goroutines -- for unit testing makeOneRequest in isolation.
func newDirectAdapter(t *testing.T, endpoint string) *OnePasswordAdapter {
	return &OnePasswordAdapter{
		conf: OnePasswordConfig{
			ClientOptions: testClientOptions(t),
			Token:         testToken,
			Endpoint:      endpoint,
		},
		httpClient:   &http.Client{Timeout: 5 * time.Second},
		endpoint:     endpoint,
		doStop:       utils.NewEvent(),
		pollInterval: time.Hour,
	}
}

// startTestAdapter builds an adapter against the mock with an injected capture
// sink and a short poll interval, then starts the fetch goroutines exactly as
// the real constructor does. The mock is http (not https), so it cannot go
// through NewOnePasswordpAdapter's https-only endpoint gate; we wire it
// directly instead.
func startTestAdapter(t *testing.T, endpoint string, sink uspSink, pollInterval time.Duration) *OnePasswordAdapter {
	a := &OnePasswordAdapter{
		conf: OnePasswordConfig{
			ClientOptions: testClientOptions(t),
			Token:         testToken,
			Endpoint:      endpoint,
		},
		uspClient:    sink,
		httpClient:   &http.Client{Timeout: 5 * time.Second},
		endpoint:     endpoint,
		doStop:       utils.NewEvent(),
		pollInterval: pollInterval,
	}
	a.chStopped = make(chan struct{})

	a.wgSenders.Add(3)
	go a.fetchEvents(auditURL)
	go a.fetchEvents(itemsURL)
	go a.fetchEvents(usersURL)
	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()
	return a
}

func (a *OnePasswordAdapter) stop() {
	a.doStop.Set()
	a.wgSenders.Wait()
	a.httpClient.CloseIdleConnections()
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func event(uuid, timestamp string) utils.Dict {
	d := utils.Dict{"uuid": uuid}
	if timestamp != "" {
		d["timestamp"] = timestamp
	}
	return d
}

// --- tests: makeOneRequest --------------------------------------------------

func TestMakeOneRequest_InitialRequestIsResetCursor(t *testing.T) {
	mock := newMockOnePassword(testToken)
	mock.setEvents(auditURL, []utils.Dict{event("a-1", "2026-06-04T12:00:00Z")})
	srv := httptest.NewServer(mock)
	defer srv.Close()

	a := newDirectAdapter(t, srv.URL)

	items, cursor, hasMore := a.makeOneRequest(auditURL, "")

	require.Len(t, items, 1)
	assert.False(t, hasMore)
	assert.NotEmpty(t, cursor)

	// The first call must be a reset cursor: start_time set, limit set, no cursor.
	req := mock.lastRequest(auditURL)
	assert.Empty(t, req.Cursor, "initial request must not carry a cursor")
	assert.Equal(t, 1000, req.Limit, "initial request should request the configured limit")
	require.NotEmpty(t, req.StartTime, "initial request must set start_time")
	_, err := time.Parse(time.RFC3339, req.StartTime)
	assert.NoError(t, err, "start_time must be RFC3339")

	// Auth/verb/content-type contract.
	assert.False(t, mock.badAuth, "bearer token must be sent")
	assert.False(t, mock.wrongVerb, "must POST")
	assert.False(t, mock.wrongCT, "must send application/json")
}

func TestMakeOneRequest_ContinuationUsesCursorOnly(t *testing.T) {
	mock := newMockOnePassword(testToken)
	mock.setEvents(auditURL, []utils.Dict{event("a-1", "2026-06-04T12:00:00Z")})
	srv := httptest.NewServer(mock)
	defer srv.Close()

	a := newDirectAdapter(t, srv.URL)

	_, cursor, _ := a.makeOneRequest(auditURL, "")
	require.NotEmpty(t, cursor)

	// Second call with the returned cursor: must send cursor and omit the reset fields.
	_, _, _ = a.makeOneRequest(auditURL, cursor)

	req := mock.lastRequest(auditURL)
	assert.Equal(t, cursor, req.Cursor, "continuation must echo the cursor")
	assert.Empty(t, req.StartTime, "continuation must not set start_time")
	assert.Zero(t, req.Limit, "continuation must not resend limit")
}

func TestMakeOneRequest_ParsesItemsCursorAndHasMore(t *testing.T) {
	mock := newMockOnePassword(testToken)
	mock.maxPage = 2 // force the server to chunk
	mock.setEvents(auditURL, []utils.Dict{
		event("a-1", "2026-06-04T12:00:00Z"),
		event("a-2", "2026-06-04T12:00:01Z"),
		event("a-3", "2026-06-04T12:00:02Z"),
	})
	srv := httptest.NewServer(mock)
	defer srv.Close()

	a := newDirectAdapter(t, srv.URL)

	items, cursor, hasMore := a.makeOneRequest(auditURL, "")
	require.Len(t, items, 2, "should receive one capped page")
	assert.True(t, hasMore, "has_more must be surfaced when more data remains")
	assert.NotEmpty(t, cursor)

	// Drain the remainder with the cursor.
	items2, _, hasMore2 := a.makeOneRequest(auditURL, cursor)
	require.Len(t, items2, 1)
	assert.False(t, hasMore2, "has_more must be false once drained")
}

func TestMakeOneRequest_Non200KeepsCursorAndWarns(t *testing.T) {
	mock := newMockOnePassword(testToken)
	mock.failStatus = http.StatusInternalServerError
	srv := httptest.NewServer(mock)
	defer srv.Close()

	var warned bool
	a := newDirectAdapter(t, srv.URL)
	a.conf.ClientOptions.OnWarning = func(string) { warned = true }

	items, cursor, hasMore := a.makeOneRequest(auditURL, "cursor-xyz")

	assert.Nil(t, items, "no items on error")
	assert.Equal(t, "cursor-xyz", cursor, "cursor must be preserved on a non-200 so we retry the same position")
	assert.False(t, hasMore)
	assert.True(t, warned, "a non-200 should raise a warning")
}

func TestMakeOneRequest_RejectedAuth(t *testing.T) {
	mock := newMockOnePassword("the-real-token")
	srv := httptest.NewServer(mock)
	defer srv.Close()

	a := newDirectAdapter(t, srv.URL)
	a.conf.Token = "wrong-token"

	items, cursor, hasMore := a.makeOneRequest(auditURL, "keep-me")
	assert.Nil(t, items)
	assert.Equal(t, "keep-me", cursor)
	assert.False(t, hasMore)
	assert.True(t, mock.badAuth, "mock should have seen a bad bearer token")
}

// --- tests: fetchEvents end-to-end -----------------------------------------

func TestFetchEvents_DrainsAllPagesInOneTick(t *testing.T) {
	const total = 250
	mock := newMockOnePassword(testToken)
	mock.maxPage = 100 // 250 events -> pages of 100, 100, 50

	events := make([]utils.Dict, total)
	for i := range events {
		events[i] = event(fmt.Sprintf("a-%d", i), "2026-06-04T12:00:00Z")
	}
	mock.setEvents(auditURL, events)
	srv := httptest.NewServer(mock)
	defer srv.Close()

	sink := &captureSink{}
	a := startTestAdapter(t, srv.URL, sink, 20*time.Millisecond)
	defer a.stop()

	// All 250 events must be shipped, and via has_more draining they should
	// arrive within roughly a single poll tick rather than 100-per-tick.
	waitFor(t, func() bool { return sink.count() >= total }, 3*time.Second, "all events shipped")

	// Give a couple more ticks to ensure nothing is duplicated or re-shipped.
	time.Sleep(80 * time.Millisecond)
	assert.Equal(t, total, sink.count(), "exactly the available events should be shipped, no duplicates")

	// Confirm every uuid arrived exactly once.
	seen := map[string]int{}
	for _, m := range sink.snapshot() {
		uuid, _ := utils.Dict(m.JsonPayload).GetString("uuid")
		seen[uuid]++
	}
	assert.Len(t, seen, total)
	for uuid, n := range seen {
		assert.Equalf(t, 1, n, "uuid %s shipped %d times", uuid, n)
	}
}

func TestFetchEvents_UsesEventTimestamp(t *testing.T) {
	mock := newMockOnePassword(testToken)

	known := "2026-06-04T12:34:56Z"
	knownTS, err := time.Parse(time.RFC3339, known)
	require.NoError(t, err)
	wantKnownMs := uint64(knownTS.UnixNano() / int64(time.Millisecond))

	before := uint64(time.Now().UnixNano() / int64(time.Millisecond))
	mock.setEvents(auditURL, []utils.Dict{
		event("has-ts", known),
		event("no-ts", ""),       // missing timestamp -> fallback to ingestion time
		event("bad-ts", "not-a-timestamp"), // unparseable -> fallback to ingestion time
	})
	srv := httptest.NewServer(mock)
	defer srv.Close()

	sink := &captureSink{}
	a := startTestAdapter(t, srv.URL, sink, 20*time.Millisecond)
	defer a.stop()

	waitFor(t, func() bool { return sink.count() >= 3 }, 3*time.Second, "events shipped")
	after := uint64(time.Now().UnixNano() / int64(time.Millisecond))

	byUUID := map[string]uint64{}
	for _, m := range sink.snapshot() {
		uuid, _ := utils.Dict(m.JsonPayload).GetString("uuid")
		byUUID[uuid] = m.TimestampMs
	}

	// The event with a real timestamp uses it verbatim.
	assert.Equal(t, wantKnownMs, byUUID["has-ts"], "must stamp from the event's RFC3339 timestamp")

	// Missing / unparseable timestamps fall back to ~ingestion time.
	for _, uuid := range []string{"no-ts", "bad-ts"} {
		ts := byUUID[uuid]
		assert.GreaterOrEqualf(t, ts, before, "%s fallback should be >= test start", uuid)
		assert.LessOrEqualf(t, ts, after, "%s fallback should be <= now", uuid)
	}
}

// TestFetchEvents_GuardAgainstNonAdvancingCursor proves the drain loop does not
// spin when the API claims has_more but never advances the cursor: a broken
// guard would ship thousands of events near-instantly inside a single tick.
func TestFetchEvents_GuardAgainstNonAdvancingCursor(t *testing.T) {
	mock := newMockOnePassword(testToken)
	mock.pathological = true // every response: has_more=true, empty cursor, 1 item
	mock.setEvents(auditURL, []utils.Dict{event("loop-1", "2026-06-04T12:00:00Z")})
	srv := httptest.NewServer(mock)
	defer srv.Close()

	sink := &captureSink{}
	a := startTestAdapter(t, srv.URL, sink, 20*time.Millisecond)
	defer a.stop()

	// Let several ticks elapse.
	time.Sleep(150 * time.Millisecond)
	n := sink.count()

	// With the guard, we ship ~1 item per poll tick (a handful over 150ms).
	// Without it, this would be a tight loop with thousands of ships.
	assert.Greater(t, n, 0, "should ship at least one item per tick")
	assert.Less(t, n, 100, "drain loop must not spin on a non-advancing cursor (got %d)", n)
}
