package usp_hubspot

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock HubSpot Account
// Activity API (GET /account-info/v3/activity/audit-logs), capturing the exact
// messages it ships so their content -- timestamp and verbatim payload -- can
// be asserted.
//
// The mock reproduces the API as the adapter uses it: it authenticates the
// Bearer token, filters the in-memory audit-log set by the
// occurredAfter/occurredBefore window, and paginates with the documented
// {"results": [...], "paging": {"next": {"after", "link"}}} envelope. One
// deliberate deviation from the real service: the adapter feeds paging.next.
// after back through the occurredAfter query parameter (not the documented
// `after` parameter), so the mock accepts a cursor token there too.

// testToken is a clearly fake HubSpot private-app access token.
const testToken = "fake-hubspot-token-1111"

// occurredAtLayout is the millisecond-precision UTC timestamp format HubSpot
// uses for occurredAt (e.g. "2023-07-18T15:59:26.920Z").
const occurredAtLayout = "2006-01-02T15:04:05.000Z"

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

// --- mock HubSpot Account Activity API ----------------------------------------

// mockHubSpot is an in-memory stand-in for the HubSpot account audit-logs
// endpoint.
type mockHubSpot struct {
	mu       sync.Mutex
	token    string
	events   []utils.Dict // audit-log entries, each with an RFC3339 occurredAt
	pageSize int          // 0 means "no pagination" (everything in one page)

	cursorSeq int
	cursors   map[string][]utils.Dict // outstanding paging.next.after token -> remaining records

	requests     int
	cursorHits   int // requests that presented a paging cursor
	authFailures int
	lastAfter    string // occurredAfter of the last non-cursor request
	lastBefore   string // occurredBefore of the last non-cursor request
	lastContent  string // Content-Type header of the last request
}

func newMockHubSpot(token string) *mockHubSpot {
	return &mockHubSpot{token: token, cursors: map[string][]utils.Dict{}}
}

func (m *mockHubSpot) setEvents(events []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = events
}

// addEvent appends a new audit-log entry, as the real API would surface a new
// account activity.
func (m *mockHubSpot) addEvent(e utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
}

func (m *mockHubSpot) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func (m *mockHubSpot) cursorHitCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cursorHits
}

func (m *mockHubSpot) authFailureCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authFailures
}

func (m *mockHubSpot) lastWindow() (string, string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastAfter, m.lastBefore, m.lastContent
}

func (m *mockHubSpot) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.requests++
		m.lastContent = r.Header.Get("Content-Type")

		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte(`{"status":"error","message":"Method not allowed","category":"VALIDATION_ERROR","correlationId":"22222222-2222-2222-2222-222222222222"}`))
			return
		}
		// HubSpot private-app auth: Authorization: Bearer <token>.
		if r.Header.Get("Authorization") != "Bearer "+m.token {
			m.authFailures++
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"status":"error","message":"Authentication credentials not found.","category":"INVALID_AUTHENTICATION","correlationId":"11111111-1111-1111-1111-111111111111"}`))
			return
		}

		q := r.URL.Query()
		afterRaw := q.Get("occurredAfter")
		beforeRaw := q.Get("occurredBefore")

		// Continuation: the adapter echoes paging.next.after back through the
		// occurredAfter parameter, so a cursor token shows up here.
		if remaining, ok := m.cursors[afterRaw]; ok {
			m.cursorHits++
			delete(m.cursors, afterRaw)
			m.writePage(w, r, remaining)
			return
		}

		m.lastAfter, m.lastBefore = afterRaw, beforeRaw
		after, err := time.Parse(time.RFC3339, afterRaw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"status":"error","message":"Invalid occurredAfter","category":"VALIDATION_ERROR","correlationId":"33333333-3333-3333-3333-333333333333"}`))
			return
		}
		before, err := time.Parse(time.RFC3339, beforeRaw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"status":"error","message":"Invalid occurredBefore","category":"VALIDATION_ERROR","correlationId":"44444444-4444-4444-4444-444444444444"}`))
			return
		}

		// Select the audit-log entries inside the requested window.
		var window []utils.Dict
		for _, e := range m.events {
			occurredAt, _ := e["occurredAt"].(string)
			ts, terr := time.Parse(time.RFC3339, occurredAt)
			if terr != nil {
				continue
			}
			if ts.After(after) && !ts.After(before) {
				window = append(window, e)
			}
		}
		m.writePage(w, r, window)
	}
}

// writePage emits one page of `set` using the documented envelope. Must be
// called with m.mu held.
func (m *mockHubSpot) writePage(w http.ResponseWriter, r *http.Request, set []utils.Dict) {
	resp := map[string]interface{}{}
	page := set
	if m.pageSize > 0 && len(set) > m.pageSize {
		page = set[:m.pageSize]
		m.cursorSeq++
		// An opaque cursor, like the real API's base64-ish tokens.
		token := fmt.Sprintf("VFZSWk5FOVVX%d", m.cursorSeq)
		m.cursors[token] = set[m.pageSize:]
		resp["paging"] = map[string]interface{}{
			"next": map[string]interface{}{
				"after": token,
				"link":  fmt.Sprintf("http://%s%s?after=%s", r.Host, r.URL.Path, token),
			},
		}
	}
	if page == nil {
		page = []utils.Dict{}
	}
	resp["results"] = page
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// --- realistic record fixtures ------------------------------------------------

// realisticAuditLog returns a record shaped like a real HubSpot account
// audit-log entry (developers.hubspot.com Account Activity API): id, category/
// subCategory/action, targetObjectId, millisecond-precision occurredAt, and the
// nested actingUser object.
func realisticAuditLog(id string, occurredAt time.Time) utils.Dict {
	return utils.Dict{
		"id":             id,
		"category":       "LOGIN",
		"subCategory":    "LOGIN_SUCCEEDED",
		"action":         "PERFORM",
		"targetObjectId": "240375137",
		"occurredAt":     occurredAt.UTC().Format(occurredAtLayout),
		"actingUser": utils.Dict{
			"userId":    int64(1234567),
			"userEmail": "jane.doe@example.com",
		},
	}
}

// realisticSecurityActivityLog returns an audit-log entry of a different
// category ("Security Activity" in HubSpot's documented category list), to
// keep the dataset heterogeneous.
func realisticSecurityActivityLog(id string, occurredAt time.Time) utils.Dict {
	return utils.Dict{
		"id":             id,
		"category":       "SECURITY_ACTIVITY",
		"subCategory":    "TWO_FACTOR_AUTHENTICATION",
		"action":         "UPDATE",
		"targetObjectId": "240375137",
		"occurredAt":     occurredAt.UTC().Format(occurredAtLayout),
		"actingUser": utils.Dict{
			"userId":    int64(7654321),
			"userEmail": "admin@example.com",
		},
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// startMockAdapter wires an adapter to the mock server with a fast poll
// interval and an injected capture sink.
func startMockAdapter(t *testing.T, serverURL string, token string, sink uspSink) (*HubSpotAdapter, chan struct{}) {
	t.Helper()
	conf := HubSpotConfig{
		ClientOptions: testClientOptions(t),
		AccessToken:   token,
		URL:           serverURL,
		PollInterval:  25 * time.Millisecond,
	}
	adapter, chStopped, err := newHubSpotAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	return adapter, chStopped
}

// --- tests ----------------------------------------------------------------------

// Note on fixture timestamps: on its first ~30 minutes of life the adapter
// queries occurredAfter = its own start time, so only events that occur after
// the adapter starts are in scope. Fixtures are therefore stamped slightly in
// the future (~1s); they enter the rolling occurredBefore=now window within a
// second or two of wall-clock time, which the fast poll interval picks up.

// TestMockAuditLogsEndToEnd drives the adapter against the mock API and asserts
// the exact events shipped: payload preserved verbatim (nested actingUser
// included), ingestion-time TimestampMs, no EventType tagging, the
// occurredAfter/occurredBefore request contract, and that re-polling the same
// window does not re-ship.
func TestMockAuditLogsEndToEnd(t *testing.T) {
	mock := newMockHubSpot(testToken)
	now := time.Now()
	want := []utils.Dict{
		realisticAuditLog("8392545957", now.Add(900*time.Millisecond)),
		realisticAuditLog("8392545958", now.Add(950*time.Millisecond)),
		realisticSecurityActivityLog("8392545959", now.Add(1*time.Second)),
	}
	mock.setEvents(want)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	testStartMs := uint64(time.Now().UnixMilli())
	adapter, _ := startMockAdapter(t, server.URL, testToken, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		8*time.Second, 20*time.Millisecond, "expected all 3 audit-log entries to ship")

	// Re-polling the same window must not re-ship: the count stays at 3.
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 30*time.Millisecond, "records were re-shipped on a later poll")

	doneMs := uint64(time.Now().UnixMilli())

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		// The adapter does not tag an EventType (platform-level parsing applies).
		assert.Empty(t, msg.EventType)
		// The adapter stamps ingestion time, not the record's occurredAt.
		assert.GreaterOrEqual(t, msg.TimestampMs, testStartMs)
		assert.LessOrEqual(t, msg.TimestampMs, doneMs)
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	// The payload is shipped verbatim -- nested actingUser object included.
	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "record %s was not shipped", id)
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original HubSpot record")
	}

	// Request contract: a JSON GET carrying an RFC3339 occurredAfter (around the
	// adapter's start) and occurredBefore (around now).
	afterRaw, beforeRaw, contentType := mock.lastWindow()
	assert.Equal(t, "application/json", contentType)
	after, err := time.Parse(time.RFC3339, afterRaw)
	require.NoError(t, err, "occurredAfter must be RFC3339")
	before, err := time.Parse(time.RFC3339, beforeRaw)
	require.NoError(t, err, "occurredBefore must be RFC3339")
	assert.False(t, before.Before(after), "occurredBefore must not precede occurredAfter")
	assert.WithinDuration(t, now, after, 5*time.Second,
		"on a fresh adapter, occurredAfter should be the adapter's start time")
	assert.WithinDuration(t, time.Now(), before, 5*time.Second,
		"occurredBefore should be the poll time")
}

// TestMockNewEventMidRunShipsOnce verifies an audit-log entry appearing while
// the adapter is running is shipped exactly once, and already-shipped entries
// are never re-sent.
func TestMockNewEventMidRunShipsOnce(t *testing.T) {
	mock := newMockHubSpot(testToken)
	now := time.Now()
	mock.setEvents([]utils.Dict{
		realisticAuditLog("1000000001", now.Add(800*time.Millisecond)),
		realisticAuditLog("1000000002", now.Add(850*time.Millisecond)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, testToken, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		8*time.Second, 20*time.Millisecond)

	// A new account activity occurs mid-run.
	mock.addEvent(realisticSecurityActivityLog("1000000003", time.Now().Add(700*time.Millisecond)))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		8*time.Second, 20*time.Millisecond, "the new entry should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 30*time.Millisecond)

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["id"].(string)]++
	}
	assert.Equal(t, map[string]int{"1000000001": 1, "1000000002": 1, "1000000003": 1},
		shippedPerID, "every entry must ship exactly once")
}

// TestMockPaginationFullDataset verifies a window larger than one page is
// walked completely by following paging.next cursors, and every record ships
// exactly once.
func TestMockPaginationFullDataset(t *testing.T) {
	const total = 25

	mock := newMockHubSpot(testToken)
	mock.pageSize = 10 // 25 records => pages of 10, 10, 5
	now := time.Now()
	events := make([]utils.Dict, total)
	for i := 0; i < total; i++ {
		events[i] = realisticAuditLog(
			fmt.Sprintf("90000000%02d", i),
			now.Add(1*time.Second+time.Duration(i)*time.Millisecond))
	}
	mock.setEvents(events)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, testToken, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		8*time.Second, 20*time.Millisecond, "all paginated records should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 30*time.Millisecond)

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		ids[msg.JsonPayload["id"].(string)] = true
	}
	assert.Len(t, ids, total, "every distinct record should be shipped once")

	assert.GreaterOrEqual(t, mock.cursorHitCount(), 2,
		"the adapter must have followed paging.next cursors to fetch pages 2 and 3")
}

// TestMockRejectsBadTokenShipsNothing verifies a rejected token ships nothing.
// The adapter's behavior on a 401 is to report the error and keep polling (it
// does not stop), so the test also pins that: repeated requests, no shutdown.
func TestMockRejectsBadTokenShipsNothing(t *testing.T) {
	mock := newMockHubSpot(testToken)
	mock.setEvents([]utils.Dict{
		realisticAuditLog("4040404040", time.Now().Add(500*time.Millisecond)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	var apiErrors atomic.Int32
	opts := testClientOptions(t)
	prevOnError := opts.OnError
	opts.OnError = func(err error) {
		apiErrors.Add(1)
		prevOnError(err)
	}

	sink := &captureSink{}
	conf := HubSpotConfig{
		ClientOptions: opts,
		AccessToken:   "fake-hubspot-token-0000", // wrong token
		URL:           server.URL,
		PollInterval:  25 * time.Millisecond,
	}
	adapter, chStopped, err := newHubSpotAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// The adapter keeps polling (and keeps being rejected) rather than stopping.
	require.Eventually(t, func() bool { return mock.authFailureCount() >= 2 },
		5*time.Second, 20*time.Millisecond, "the adapter should keep retrying on 401")
	require.Eventually(t, func() bool { return apiErrors.Load() >= 1 },
		5*time.Second, 20*time.Millisecond, "the 401 must be surfaced via OnError")

	select {
	case <-chStopped:
		t.Fatal("the adapter stopped on a 401; its documented behavior is to keep polling")
	default:
	}

	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
	assert.GreaterOrEqual(t, mock.requestCount(), 2)
}
