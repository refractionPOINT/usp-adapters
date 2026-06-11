package usp_box

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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

// This file exercises the adapter end-to-end against a mock Box API (OAuth2
// token endpoint + enterprise events endpoint), capturing the exact messages
// it ships so their content -- timestamp and verbatim payload -- can be
// asserted.

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

// --- mock Box API -------------------------------------------------------------

// mockBoxBasePosition is an arbitrary realistic Box stream position; the real
// API hands out large opaque integers, never starting at 0.
const mockBoxBasePosition = int64(1152922976252290800)

// mockBox is an in-memory stand-in for the Box API as the adapter uses it:
//
//   - POST /oauth2/token: validates a client_credentials grant scoped to an
//     enterprise (box_subject_type/box_subject_id) and issues a bearer token,
//     exactly like https://api.box.com/oauth2/token.
//   - GET /2.0/events?stream_type=admin_logs: validates the bearer token and
//     serves the enterprise event stream. A created_after request (the
//     adapter's bootstrap) returns events newer than that time; a
//     stream_position request returns events recorded after that position.
//     Every response carries next_stream_position pointing past the last
//     entry returned (or the unchanged position when there is nothing new),
//     mirroring the real stream paging contract
//     (https://developer.box.com/reference/get-events/).
type mockBox struct {
	mu sync.Mutex

	clientID     string
	clientSecret string
	subjectID    string

	issuedTokens map[string]bool
	tokenCounter int

	events     []utils.Dict // ordered admin_logs stream, oldest first
	chunkLimit int          // max entries per response (Box default: 100)

	// rewindNext, when set, makes the next stream_position request start one
	// event early, redelivering the last already-served entry once -- the
	// at-least-once behavior a Box stream consumer must tolerate.
	rewindNext bool

	tokenRequests int
	eventRequests int
	initRequests  int // events requests using created_after (the bootstrap)
}

func newMockBox(clientID, clientSecret, subjectID string) *mockBox {
	return &mockBox{
		clientID:     clientID,
		clientSecret: clientSecret,
		subjectID:    subjectID,
		issuedTokens: map[string]bool{},
		chunkLimit:   100,
	}
}

func (m *mockBox) addEvent(e utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
}

func (m *mockBox) setChunkLimit(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chunkLimit = n
}

func (m *mockBox) redeliverLastOnce() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rewindNext = true
}

func (m *mockBox) tokenRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenRequests
}

func (m *mockBox) eventRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.eventRequests
}

func (m *mockBox) initRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.initRequests
}

// server starts an httptest server exposing the mock at the same paths as the
// real API and returns it (caller closes it).
func (m *mockBox) server(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth2/token", m.handleToken)
	mux.HandleFunc("/2.0/events", m.handleEvents)
	return httptest.NewServer(mux)
}

func (m *mockBox) handleToken(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokenRequests++

	if r.Method != http.MethodPost {
		writeBoxError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method Not Allowed")
		return
	}
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		writeBoxError(w, http.StatusBadRequest, "bad_request", "expected form-encoded body")
		return
	}
	if err := r.ParseForm(); err != nil {
		writeBoxError(w, http.StatusBadRequest, "bad_request", "malformed form body")
		return
	}

	// The real endpoint rejects anything but a well-formed client_credentials
	// grant for the enterprise subject.
	if r.PostForm.Get("grant_type") != "client_credentials" ||
		r.PostForm.Get("box_subject_type") != "enterprise" ||
		r.PostForm.Get("box_subject_id") != m.subjectID ||
		r.PostForm.Get("client_id") != m.clientID ||
		r.PostForm.Get("client_secret") != m.clientSecret {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error":             "invalid_client",
			"error_description": "The client credentials are not valid",
		})
		return
	}

	m.tokenCounter++
	token := fmt.Sprintf("mock-access-token-%d", m.tokenCounter)
	m.issuedTokens[token] = true

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"access_token":  token,
		"expires_in":    3600,
		"restricted_to": []interface{}{},
		"token_type":    "bearer",
	})
}

func (m *mockBox) handleEvents(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventRequests++

	if r.Method != http.MethodGet {
		writeBoxError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method Not Allowed")
		return
	}

	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if token == "" || !m.issuedTokens[token] {
		writeBoxError(w, http.StatusUnauthorized, "unauthorized", "Unauthorized")
		return
	}

	q := r.URL.Query()
	if q.Get("stream_type") != "admin_logs" {
		writeBoxError(w, http.StatusBadRequest, "bad_request", "unsupported stream_type")
		return
	}

	// Resolve where in the stream this request starts.
	var idx int
	usedCreatedAfter := false
	switch {
	case q.Get("stream_position") != "":
		pos, err := strconv.ParseInt(q.Get("stream_position"), 10, 64)
		if err != nil {
			writeBoxError(w, http.StatusBadRequest, "bad_request", "invalid stream_position")
			return
		}
		idx = int(pos - mockBoxBasePosition)
		if idx < 0 {
			idx = 0
		}
		if idx > len(m.events) {
			idx = len(m.events)
		}
		if m.rewindNext && idx > 0 {
			idx--
			m.rewindNext = false
		}
	case q.Get("created_after") != "":
		m.initRequests++
		usedCreatedAfter = true
		after, err := time.Parse(time.RFC3339, q.Get("created_after"))
		if err != nil {
			writeBoxError(w, http.StatusBadRequest, "bad_request", "invalid created_after")
			return
		}
		idx = len(m.events)
		for i, e := range m.events {
			createdAt, _ := e["created_at"].(string)
			ts, perr := time.Parse(time.RFC3339, createdAt)
			if perr == nil && ts.After(after) {
				idx = i
				break
			}
		}
	default:
		writeBoxError(w, http.StatusBadRequest, "bad_request", "missing stream_position or created_after")
		return
	}

	end := idx + m.chunkLimit
	if end > len(m.events) {
		end = len(m.events)
	}
	chunk := m.events[idx:end]
	if chunk == nil {
		chunk = []utils.Dict{}
	}

	// next_stream_position is documented as anyOf string|integer
	// (https://developer.box.com/reference/get-events/ -- the schema example is
	// the string "1152922976252290886") and live responses use both forms, so
	// serve the bootstrap (created_after) response as a string and in-stream
	// responses as an integer to exercise the adapter against both.
	var nextPos interface{} = mockBoxBasePosition + int64(end)
	if usedCreatedAfter {
		nextPos = strconv.FormatInt(mockBoxBasePosition+int64(end), 10)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"chunk_size":           len(chunk),
		"next_stream_position": nextPos,
		"entries":              chunk,
	})
}

// writeBoxError emits an error body shaped like the real Box API's.
func writeBoxError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"type":       "error",
		"status":     status,
		"code":       code,
		"message":    message,
		"help_url":   "https://developer.box.com/guides/api-calls/permissions-and-errors/common-errors/",
		"request_id": "abcdef123456",
	})
}

// --- realistic fixtures -------------------------------------------------------

// realisticBoxEvent returns an entry shaped like a real Box enterprise
// (admin_logs) event per the documented Event resource
// (https://developer.box.com/reference/resources/event/): the documented
// top-level fields, a created_by user mini, a nested event source with a
// parent folder mini and owner, a freeform additional_details object with
// mixed scalar types, and a null session_id (not all events populate it). All
// identifiers are clearly fake. created_at must be recent (the adapter's
// dedupe window is one hour).
func realisticBoxEvent(eventID, eventType string, createdAt time.Time) utils.Dict {
	return utils.Dict{
		"type":     "event",
		"event_id": eventID,
		"created_by": utils.Dict{
			"type":  "user",
			"id":    "11111111111",
			"name":  "Jane Example",
			"login": "jane@example.com",
		},
		"created_at":  createdAt.UTC().Format(time.RFC3339),
		"recorded_at": createdAt.UTC().Add(2 * time.Second).Format(time.RFC3339),
		"event_type":  eventType,
		"session_id":  nil,
		"source": utils.Dict{
			"item_type": "file",
			"item_id":   "1111111111111",
			"item_name": "quarterly-report.pdf",
			"parent": utils.Dict{
				"type": "folder",
				"id":   "0",
				"name": "All Files",
			},
			"owned_by": utils.Dict{
				"type":  "user",
				"id":    "11111111111",
				"login": "jane@example.com",
			},
		},
		"additional_details": utils.Dict{
			"size":         1048576,
			"version_id":   "1111111111112",
			"service_id":   "1111",
			"service_name": "Example Integration",
			"shared_link":  false,
		},
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// --- test scaffolding ---------------------------------------------------------

const (
	testBoxClientID     = "test-box-client-id"
	testBoxClientSecret = "test-box-client-secret"
	testBoxSubjectID    = "1111111111"
)

// startMockAdapter wires an adapter to the mock server with an injected capture
// sink and a short poll interval, and registers cleanup. It returns once the
// adapter has completed its bootstrap request (the created_after call whose
// results are discarded), so events added afterwards are picked up through the
// stream_position mechanism.
func startMockAdapter(t *testing.T, mock *mockBox, serverURL string, sink *captureSink, conf BoxConfig) (*BoxAdapter, chan struct{}) {
	t.Helper()

	if conf.ClientID == "" {
		conf.ClientID = testBoxClientID
	}
	if conf.ClientSecret == "" {
		conf.ClientSecret = testBoxClientSecret
	}
	if conf.SubjectID == "" {
		conf.SubjectID = testBoxSubjectID
	}
	conf.ClientOptions = testClientOptions(t)
	conf.EventsURL = serverURL + "/2.0/events"
	conf.TokenURL = serverURL + "/oauth2/token"
	if conf.PollInterval <= 0 {
		conf.PollInterval = 25 * time.Millisecond
	}

	adapter, chStopped, err := newBoxAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = adapter.Close() })

	require.Eventually(t, func() bool { return mock.initRequestCount() >= 1 },
		5*time.Second, 5*time.Millisecond, "adapter never made its bootstrap created_after request")

	return adapter, chStopped
}

// --- tests ----------------------------------------------------------------------

// TestBoxEventsEndToEnd drives the adapter against the mock Box API and asserts
// the exact events shipped: the payload preserved verbatim (nested objects,
// nulls and arrays included), a ship-time TimestampMs, and an untagged
// EventType (the Box adapter does not set one). It also asserts the adapter's
// bootstrap behavior: events that predate startup are not shipped, and every
// poll performs a fresh, validated token exchange.
func TestBoxEventsEndToEnd(t *testing.T) {
	mock := newMockBox(testBoxClientID, testBoxClientSecret, testBoxSubjectID)
	// An event already in the stream before the adapter starts: the bootstrap
	// request consumes (and discards) it, so it must never ship.
	mock.addEvent(realisticBoxEvent("00000000-0000-0000-0000-00000000pre1", "LOGIN", time.Now().Add(-5*time.Minute)))

	server := mock.server(t)
	defer server.Close()

	sink := &captureSink{}
	_, chStopped := startMockAdapter(t, mock, server.URL, sink, BoxConfig{})

	beforeMs := uint64(time.Now().UnixMilli())
	want := []utils.Dict{
		realisticBoxEvent("11111111-1111-1111-1111-111111111101", "LOGIN", time.Now().Add(-3*time.Minute)),
		realisticBoxEvent("11111111-1111-1111-1111-111111111102", "UPLOAD", time.Now().Add(-2*time.Minute)),
		realisticBoxEvent("11111111-1111-1111-1111-111111111103", "DELETE", time.Now().Add(-1*time.Minute)),
	}
	for _, e := range want {
		mock.addEvent(e)
	}

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond, "expected all 3 events to ship")

	// Re-polling must not re-ship: the stream position has advanced past the
	// shipped events, so the count stays at 3 across many further polls.
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 20*time.Millisecond, "events were re-shipped on a later poll")

	afterMs := uint64(time.Now().UnixMilli())

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		require.NotNil(t, msg.JsonPayload)
		// The Box adapter ships events untagged; the platform comes from the
		// client options.
		assert.Equal(t, "", msg.EventType)
		// The adapter stamps ship time, not the event's created_at.
		assert.GreaterOrEqual(t, msg.TimestampMs, beforeMs)
		assert.LessOrEqual(t, msg.TimestampMs, afterMs)

		id, _ := msg.JsonPayload["event_id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)
	assert.NotContains(t, byID, "00000000-0000-0000-0000-00000000pre1",
		"events predating startup are discarded by the bootstrap request")

	for _, src := range want {
		id := src["event_id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "event %s was not shipped", id)
		// The payload is shipped verbatim -- nested objects, nulls and mixed
		// types included.
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Box entry")
	}

	// The adapter authenticates on every poll; each exchange was validated by
	// the mock (an invalid one would have failed the whole flow).
	assert.GreaterOrEqual(t, mock.tokenRequestCount(), 2)

	select {
	case <-chStopped:
		t.Fatal("adapter stopped unexpectedly")
	default:
	}
}

// TestBoxMidRunEventShipsExactlyOnce verifies an event appearing while the
// adapter is running ships exactly once, and already-shipped events are never
// re-sent as the stream advances.
func TestBoxMidRunEventShipsExactlyOnce(t *testing.T) {
	mock := newMockBox(testBoxClientID, testBoxClientSecret, testBoxSubjectID)
	server := mock.server(t)
	defer server.Close()

	sink := &captureSink{}
	startMockAdapter(t, mock, server.URL, sink, BoxConfig{})

	mock.addEvent(realisticBoxEvent("11111111-1111-1111-1111-11111111aaaa", "LOGIN", time.Now().Add(-4*time.Minute)))
	mock.addEvent(realisticBoxEvent("11111111-1111-1111-1111-11111111bbbb", "DOWNLOAD", time.Now().Add(-3*time.Minute)))

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 10*time.Millisecond)

	// A new admin event occurs mid-run.
	mock.addEvent(realisticBoxEvent("11111111-1111-1111-1111-11111111cccc", "FAILED_LOGIN", time.Now()))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond, "the mid-run event should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 20*time.Millisecond)

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["event_id"].(string)]++
	}
	assert.Equal(t, map[string]int{
		"11111111-1111-1111-1111-11111111aaaa": 1,
		"11111111-1111-1111-1111-11111111bbbb": 1,
		"11111111-1111-1111-1111-11111111cccc": 1,
	}, shippedPerID, "every event must ship exactly once")
}

// TestBoxMultiBatchFullyConsumed verifies a dataset larger than one chunk is
// drained across successive polls -- the stream position advancing chunk by
// chunk -- with every event shipped exactly once.
func TestBoxMultiBatchFullyConsumed(t *testing.T) {
	const total = 8

	mock := newMockBox(testBoxClientID, testBoxClientSecret, testBoxSubjectID)
	mock.setChunkLimit(3) // 8 events => chunks of 3+3+2
	server := mock.server(t)
	defer server.Close()

	sink := &captureSink{}
	startMockAdapter(t, mock, server.URL, sink, BoxConfig{})

	want := map[string]bool{}
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("11111111-1111-1111-1111-1111111111%02d", i)
		want[id] = true
		mock.addEvent(realisticBoxEvent(id, "UPLOAD", time.Now().Add(-time.Duration(total-i)*time.Minute)))
	}

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 10*time.Millisecond, "all chunks should be consumed")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 20*time.Millisecond)

	got := map[string]bool{}
	for _, msg := range sink.snapshot() {
		got[msg.JsonPayload["event_id"].(string)] = true
	}
	assert.Equal(t, want, got, "every distinct event should ship exactly once")
}

// TestBoxRedeliveryIsDeduped verifies that when the stream redelivers an
// already-served entry (Box streams are at-least-once), the adapter's dedupe
// suppresses the duplicate rather than shipping it twice.
func TestBoxRedeliveryIsDeduped(t *testing.T) {
	mock := newMockBox(testBoxClientID, testBoxClientSecret, testBoxSubjectID)
	server := mock.server(t)
	defer server.Close()

	sink := &captureSink{}
	startMockAdapter(t, mock, server.URL, sink, BoxConfig{})

	mock.addEvent(realisticBoxEvent("11111111-1111-1111-1111-11111111dddd", "LOGIN", time.Now().Add(-2*time.Minute)))
	mock.addEvent(realisticBoxEvent("11111111-1111-1111-1111-11111111eeee", "DELETE", time.Now().Add(-1*time.Minute)))

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 10*time.Millisecond)

	// The next poll redelivers the last entry; the dedupe map must absorb it.
	reqsBefore := mock.eventRequestCount()
	mock.redeliverLastOnce()

	require.Eventually(t, func() bool { return mock.eventRequestCount() >= reqsBefore+2 },
		5*time.Second, 10*time.Millisecond, "the adapter should keep polling")
	require.Never(t, func() bool { return sink.count() != 2 },
		300*time.Millisecond, 20*time.Millisecond, "a redelivered entry must not ship twice")
}

// TestBoxBadCredentialsShipNothing verifies that when the token endpoint
// rejects the credentials nothing is ever shipped and the events endpoint is
// never reached. The Box adapter does not terminate on auth failure -- it
// keeps retrying the token exchange on its poll interval -- so the test also
// pins that it stays alive (and quiet) across several polls.
func TestBoxBadCredentialsShipNothing(t *testing.T) {
	mock := newMockBox(testBoxClientID, testBoxClientSecret, testBoxSubjectID)
	mock.addEvent(realisticBoxEvent("11111111-1111-1111-1111-11111111ffff", "LOGIN", time.Now().Add(-1*time.Minute)))
	server := mock.server(t)
	defer server.Close()

	sink := &captureSink{}
	conf := BoxConfig{
		ClientOptions: testClientOptions(t),
		ClientID:      testBoxClientID,
		ClientSecret:  "wrong-secret",
		SubjectID:     testBoxSubjectID,
		EventsURL:     server.URL + "/2.0/events",
		TokenURL:      server.URL + "/oauth2/token",
		PollInterval:  25 * time.Millisecond,
	}
	adapter, chStopped, err := newBoxAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// Several polls' worth of rejected token exchanges.
	require.Eventually(t, func() bool { return mock.tokenRequestCount() >= 3 },
		5*time.Second, 10*time.Millisecond)

	assert.Equal(t, 0, sink.count(), "nothing should ship with bad credentials")
	assert.Equal(t, 0, mock.eventRequestCount(), "the events endpoint must not be called without a token")

	select {
	case <-chStopped:
		t.Fatal("the Box adapter does not stop on auth failure; it retries on its poll interval")
	default:
	}
}
