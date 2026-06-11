package usp_sublime

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock of the Sublime
// Security platform API (GET /v0/audit-log/events), capturing the exact
// messages it ships so their content -- timestamp, event type and verbatim
// payload -- can be asserted.

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

// --- mock Sublime platform API -----------------------------------------------

// mockSublime is an in-memory stand-in for the Sublime Security platform API's
// audit log listing. It honours the contract the adapter relies on: a GET on
// /v0/audit-log/events authenticated with a Bearer API key, offset/limit query
// pagination (limit capped at 500 by the real API), and an object envelope
// {"events": [...], "count": N} around the page.
type mockSublime struct {
	mu     sync.Mutex
	apiKey string
	events []utils.Dict

	requests      int
	authFailures  int
	maxOffsetSeen int
	lastMethod    string
	lastPath      string
	lastAccept    string
	lastLimit     int
}

func newMockSublime(apiKey string) *mockSublime {
	return &mockSublime{apiKey: apiKey}
}

func (m *mockSublime) setEvents(events []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = events
}

// appendEvent adds a newly-occurred event to the audit log.
func (m *mockSublime) appendEvent(event utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
}

func (m *mockSublime) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func (m *mockSublime) authFailureCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authFailures
}

func (m *mockSublime) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()

		m.requests++
		m.lastMethod = r.Method
		m.lastPath = r.URL.Path
		m.lastAccept = r.Header.Get("Accept")

		if r.Method != http.MethodGet {
			http.Error(w, `{"message":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != logsPath {
			http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
			return
		}
		// The platform API authenticates with a Bearer API key.
		if r.Header.Get("Authorization") != "Bearer "+m.apiKey {
			m.authFailures++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"invalid or missing API key"}`))
			return
		}

		limit := pageLimit
		if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 {
			limit = v
		}
		offset := 0
		if v, err := strconv.Atoi(r.URL.Query().Get("offset")); err == nil && v >= 0 {
			offset = v
		}
		m.lastLimit = limit
		if offset > m.maxOffsetSeen {
			m.maxOffsetSeen = offset
		}

		page := []utils.Dict{}
		if offset < len(m.events) {
			end := offset + limit
			if end > len(m.events) {
				end = len(m.events)
			}
			page = m.events[offset:end]
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"events": page,
			"count":  len(page),
		})
	}
}

// --- realistic event fixtures -------------------------------------------------

// realisticAuditEvent returns an event shaped like a real Sublime Security
// audit log entry: id/type/created_at plus the created_by user object and the
// data.request details of the action recorded. All identifiers are fake.
func realisticAuditEvent(id, eventType, createdAt string) utils.Dict {
	return utils.Dict{
		"id":         id,
		"type":       eventType,
		"created_at": createdAt,
		"created_by": utils.Dict{
			"id":                      "11111111-1111-1111-1111-111111111111",
			"email_address":           "analyst@example.com",
			"first_name":              "Alex",
			"last_name":               "Analyst",
			"role":                    "admin",
			"active":                  true,
			"google_oauth_user_id":    "",
			"microsoft_oauth_user_id": "",
			"created_at":              "2026-01-01T00:00:00Z",
			"updated_at":              "2026-01-02T00:00:00Z",
		},
		"data": utils.Dict{
			"request": utils.Dict{
				"id":                    "22222222-2222-2222-2222-222222222222",
				"method":                "POST",
				"path":                  "/v0/message-groups/33333333-3333-3333-3333-333333333333/review",
				"user_agent":            "Mozilla/5.0 (X11; Linux x86_64) Example/1.0",
				"ip":                    "203.0.113.10",
				"authentication_method": "user_session",
				"query":                 utils.Dict{},
				"body":                  nil,
			},
			"message": utils.Dict{
				"id":          "44444444-4444-4444-4444-444444444444",
				"external_id": "55555555-5555-5555-5555-555555555555",
			},
		},
	}
}

// futureTS returns an RFC3339Nano timestamp d into the future. The adapter
// starts its polling window at time.Now(), so only events whose created_at is
// after adapter start are shipped; fixtures use future timestamps to land
// inside the window deterministically.
func futureTS(d time.Duration) string {
	return time.Now().Add(d).UTC().Format(time.RFC3339Nano)
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func shippedIDs(msgs []*protocol.DataMessage) map[string]int {
	out := map[string]int{}
	for _, m := range msgs {
		id, _ := m.JsonPayload["id"].(string)
		out[id]++
	}
	return out
}

// startMockAdapter wires the adapter to the mock server with the capture sink
// and a fast poll interval.
func startMockAdapter(t *testing.T, serverURL, apiKey string, sink uspSink) (*SublimeAdapter, chan struct{}) {
	t.Helper()
	conf := SublimeConfig{
		ClientOptions: testClientOptions(t),
		ApiKey:        apiKey,
		BaseURL:       serverURL,
		PollInterval:  25 * time.Millisecond,
	}
	adapter, chStopped, err := newSublimeAdapter(t.Context(), conf, sink)
	require.NoError(t, err)
	return adapter, chStopped
}

// --- tests --------------------------------------------------------------------

// TestMockAuditLogEndToEnd drives the adapter against the mock API and asserts
// the exact messages shipped: ingestion-time TimestampMs, empty EventType (the
// adapter does not tag audit events), and the payload preserved verbatim. It
// also pins the request contract: GET on /v0/audit-log/events with the Bearer
// API key, Accept: application/json and limit=500.
func TestMockAuditLogEndToEnd(t *testing.T) {
	const apiKey = "sublime-test-api-key-000000000000"

	mock := newMockSublime(apiKey)
	want := []utils.Dict{
		realisticAuditEvent("aaaaaaaa-1111-1111-1111-111111111111", "message_group.review", futureTS(1*time.Hour)),
		realisticAuditEvent("aaaaaaaa-2222-2222-2222-222222222222", "message.view_contents", futureTS(1*time.Hour+1*time.Minute)),
		realisticAuditEvent("aaaaaaaa-3333-3333-3333-333333333333", "user.session.create", futureTS(1*time.Hour+2*time.Minute)),
	}
	mock.setEvents(want)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	before := uint64(time.Now().UnixMilli())
	adapter, _ := startMockAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond, "expected all 3 audit events to ship")
	after := uint64(time.Now().UnixMilli())

	// Wait for at least one more full poll, then verify nothing re-ships.
	polled := mock.requestCount()
	require.Eventually(t, func() bool { return mock.requestCount() >= polled+2 },
		5*time.Second, 10*time.Millisecond, "expected further polls to happen")
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 25*time.Millisecond, "events were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		// The adapter ships audit events without an EventType tag.
		assert.Equal(t, "", msg.EventType)
		// The adapter stamps ingestion time, not the event's created_at.
		assert.GreaterOrEqual(t, msg.TimestampMs, before, "TimestampMs should be ingestion time")
		assert.LessOrEqual(t, msg.TimestampMs, after, "TimestampMs should be ingestion time")
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "event %s was not shipped", id)
		// The payload is shipped verbatim -- nested objects included.
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Sublime audit event")
	}

	// Request contract.
	mock.mu.Lock()
	defer mock.mu.Unlock()
	assert.Equal(t, http.MethodGet, mock.lastMethod)
	assert.Equal(t, logsPath, mock.lastPath)
	assert.Equal(t, "application/json", mock.lastAccept)
	assert.Equal(t, pageLimit, mock.lastLimit, "the adapter should request the full page limit")
	assert.Zero(t, mock.authFailures, "the Bearer API key must be sent on every request")
}

// TestMockNewEventMidRunShipsOnce verifies an audit event that appears while
// the adapter is running ships exactly once, and already-shipped events are
// never re-sent.
func TestMockNewEventMidRunShipsOnce(t *testing.T) {
	const apiKey = "sublime-test-api-key-000000000000"

	mock := newMockSublime(apiKey)
	mock.setEvents([]utils.Dict{
		realisticAuditEvent("bbbbbbbb-1111-1111-1111-111111111111", "message_group.review", futureTS(1*time.Hour)),
		realisticAuditEvent("bbbbbbbb-2222-2222-2222-222222222222", "rule.update", futureTS(1*time.Hour+1*time.Minute)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 10*time.Millisecond)

	// A new event occurs mid-run, later than everything already seen.
	mock.appendEvent(realisticAuditEvent(
		"bbbbbbbb-3333-3333-3333-333333333333", "user.session.create", futureTS(2*time.Hour)))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond, "the new event should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 25*time.Millisecond)

	assert.Equal(t, map[string]int{
		"bbbbbbbb-1111-1111-1111-111111111111": 1,
		"bbbbbbbb-2222-2222-2222-222222222222": 1,
		"bbbbbbbb-3333-3333-3333-333333333333": 1,
	}, shippedIDs(sink.snapshot()), "every event must ship exactly once")
}

// TestMockPaginationFullDataset verifies a dataset larger than one page
// (pageLimit=500) is walked completely via offset pagination and every event
// ships exactly once.
func TestMockPaginationFullDataset(t *testing.T) {
	const apiKey = "sublime-test-api-key-000000000000"
	const total = 750 // 500 + 250 => two pages

	mock := newMockSublime(apiKey)
	events := make([]utils.Dict, total)
	for i := 0; i < total; i++ {
		events[i] = realisticAuditEvent(
			fmt.Sprintf("cccccccc-0000-0000-0000-%012d", i),
			"message.view_contents",
			futureTS(time.Hour+time.Duration(i)*time.Millisecond))
	}
	mock.setEvents(events)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 25*time.Millisecond, "all paginated events should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 25*time.Millisecond)

	ids := shippedIDs(sink.snapshot())
	assert.Len(t, ids, total, "every distinct event should ship")
	for id, n := range ids {
		assert.Equalf(t, 1, n, "event %s shipped %d times", id, n)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	assert.GreaterOrEqual(t, mock.maxOffsetSeen, pageLimit,
		"the adapter should have requested the second page (offset=%d)", pageLimit)
}

// TestMockEventsBeforeStartDoNotShip pins the adapter's startup window: it only
// ships audit events whose created_at is after the adapter started; the
// pre-existing backlog is not replayed.
func TestMockEventsBeforeStartDoNotShip(t *testing.T) {
	const apiKey = "sublime-test-api-key-000000000000"

	mock := newMockSublime(apiKey)
	mock.setEvents([]utils.Dict{
		realisticAuditEvent("dddddddd-1111-1111-1111-111111111111", "rule.create", time.Now().Add(-2*time.Hour).UTC().Format(time.RFC3339Nano)),
		realisticAuditEvent("dddddddd-2222-2222-2222-222222222222", "rule.update", time.Now().Add(-1*time.Hour).UTC().Format(time.RFC3339Nano)),
		realisticAuditEvent("dddddddd-3333-3333-3333-333333333333", "message_group.review", futureTS(1*time.Hour)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 10*time.Millisecond, "the post-start event should ship")
	// Let several more polls happen; the historical events must stay unshipped.
	polled := mock.requestCount()
	require.Eventually(t, func() bool { return mock.requestCount() >= polled+2 },
		5*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return sink.count() != 1 },
		300*time.Millisecond, 25*time.Millisecond, "historical events must not be replayed")

	assert.Equal(t, map[string]int{"dddddddd-3333-3333-3333-333333333333": 1},
		shippedIDs(sink.snapshot()))
}

// TestMockBadAPIKeyShipsNothing pins the adapter's behavior on auth failure:
// a 401 from the API is reported via OnError and nothing ships, but the
// adapter does not stop -- it keeps polling on its interval.
func TestMockBadAPIKeyShipsNothing(t *testing.T) {
	mock := newMockSublime("the-correct-api-key")
	mock.setEvents([]utils.Dict{
		realisticAuditEvent("eeeeeeee-1111-1111-1111-111111111111", "rule.create", futureTS(1*time.Hour)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	var mu sync.Mutex
	errorCount := 0
	opts := testClientOptions(t)
	opts.OnError = func(err error) {
		t.Logf("ERR: %v", err)
		mu.Lock()
		errorCount++
		mu.Unlock()
	}

	sink := &captureSink{}
	conf := SublimeConfig{
		ClientOptions: opts,
		ApiKey:        "the-wrong-api-key",
		BaseURL:       server.URL,
		PollInterval:  25 * time.Millisecond,
	}
	adapter, chStopped, err := newSublimeAdapter(t.Context(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// The adapter keeps polling despite the 401s (it does not treat auth
	// failure as fatal) ...
	require.Eventually(t, func() bool { return mock.authFailureCount() >= 3 },
		5*time.Second, 10*time.Millisecond, "the adapter should keep polling on auth failure")
	select {
	case <-chStopped:
		t.Fatal("the adapter is not expected to stop on auth failure")
	default:
	}

	// ... reports the failures, and ships nothing.
	mu.Lock()
	assert.GreaterOrEqual(t, errorCount, 1, "auth failures must be reported via OnError")
	mu.Unlock()
	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
}
