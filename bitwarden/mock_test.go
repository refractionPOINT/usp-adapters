package usp_bitwarden

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock of the Bitwarden
// Public (Events) API and its Identity OAuth2 endpoint, capturing the exact
// messages it ships so their content -- timestamp and verbatim payload -- can
// be asserted without live credentials.

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

// --- mock Bitwarden API -----------------------------------------------------

// mockPageSize mirrors the real API, which returns at most 50 event logs per
// page (the default PageOptions.PageSize in bitwarden/server) and a
// continuationToken when more are available.
const mockPageSize = 50

// mockBitwarden is an in-memory stand-in for the two Bitwarden endpoints the
// adapter uses:
//
//   - POST /connect/token -- the Identity OAuth2 endpoint. It validates the
//     client_credentials exchange (grant type, scope, client id/secret, form
//     encoding) the way the real identity server does and issues bearer
//     tokens, returning 400 invalid_client on a bad secret.
//   - GET /public/events -- the Events API. It requires a previously issued
//     bearer token, filters the in-memory dataset by the start/end query
//     parameters (defaulting to the last 30 days when absent, as documented),
//     paginates with an opaque continuationToken, and returns the documented
//     {"object":"list","data":[...],"continuationToken":...} envelope.
//
// Time filtering treats the window as [start, end], inclusive on both ends.
// The official docs don't state boundary inclusivity, but both event-store
// implementations in bitwarden/server filter Date >= start AND Date <= end
// (SQL: Event_ReadPageByOrganizationId.sql; Azure Tables: reversed-tick
// RowKey le/ge in TableStorage/EventRepository.cs). The adapter reuses each
// cycle's end as the next start (truncated to whole seconds by RFC3339
// formatting), so only an event stamped exactly on that whole-second boundary
// could ever be returned in two windows; the sub-second fixture timestamps in
// these tests never hit that edge.
type mockBitwarden struct {
	mu           sync.Mutex
	clientID     string
	clientSecret string

	events []utils.Dict // each carries a "date" field, RFC3339Nano

	issuedTokens   map[string]bool
	tokenSeq       int
	tokenExpiresIn int // seconds; default 3600 like the real API

	cursors   map[string][]utils.Dict // continuationToken -> remaining records
	cursorSeq int

	rejectBearer bool // when true, /public/events answers 401 to any token

	tokenRequests        int
	eventRequests        int
	continuationRequests int
}

func newMockBitwarden(clientID, clientSecret string) *mockBitwarden {
	return &mockBitwarden{
		clientID:       clientID,
		clientSecret:   clientSecret,
		issuedTokens:   map[string]bool{},
		cursors:        map[string][]utils.Dict{},
		tokenExpiresIn: 3600,
	}
}

func (m *mockBitwarden) setEvents(events []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = events
}

// addEvent appends a new event, as the real API would surface one that just
// occurred.
func (m *mockBitwarden) addEvent(e utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
}

func (m *mockBitwarden) tokenRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenRequests
}

func (m *mockBitwarden) eventRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.eventRequests
}

func (m *mockBitwarden) continuationRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.continuationRequests
}

func (m *mockBitwarden) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/connect/token", m.handleToken)
	mux.HandleFunc("/public/events", m.handleEvents)
	return mux
}

func (m *mockBitwarden) handleToken(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.tokenRequests++
	m.mu.Unlock()

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !strings.Contains(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": "invalid_request"})
		return
	}
	if err := r.ParseForm(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": "invalid_request"})
		return
	}
	if r.PostFormValue("grant_type") != "client_credentials" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": "unsupported_grant_type"})
		return
	}
	if r.PostFormValue("scope") != "api.organization" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": "invalid_scope"})
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if r.PostFormValue("client_id") != m.clientID || r.PostFormValue("client_secret") != m.clientSecret {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "invalid_client",
		})
		return
	}

	m.tokenSeq++
	token := fmt.Sprintf("mock-access-token-%d", m.tokenSeq)
	m.issuedTokens[token] = true
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"access_token": token,
		"expires_in":   m.tokenExpiresIn,
		"token_type":   "Bearer",
		"scope":        "api.organization",
	})
}

func (m *mockBitwarden) handleEvents(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.eventRequests++
	m.mu.Unlock()

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	m.mu.Lock()
	authorized := m.issuedTokens[token] && !m.rejectBearer
	m.mu.Unlock()
	if !authorized {
		// ErrorResponseModel shape; "Unauthorized." is the message the API's
		// exception filter uses for 401s.
		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"object":  "error",
			"message": "Unauthorized.",
		})
		return
	}

	q := r.URL.Query()
	var matched []utils.Dict

	if ct := q.Get("continuationToken"); ct != "" {
		m.mu.Lock()
		m.continuationRequests++
		remaining, ok := m.cursors[ct]
		delete(m.cursors, ct)
		m.mu.Unlock()
		if !ok {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"object":  "error",
				"message": "Invalid continuation token.",
			})
			return
		}
		matched = remaining
	} else {
		var start, end time.Time
		var hasStart, hasEnd bool
		if s := q.Get("start"); s != "" {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]interface{}{
					"object":  "error",
					"message": "Invalid start date.",
				})
				return
			}
			start, hasStart = t, true
		}
		if s := q.Get("end"); s != "" {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]interface{}{
					"object":  "error",
					"message": "Invalid end date.",
				})
				return
			}
			end, hasEnd = t, true
		}

		// "If no filters are provided, it will return the last 30 days of
		// event for the organization" (official API reference). The server
		// defaults to start = today-30d, end = end of today (UTC) whenever
		// either bound is missing (EventFilterRequestModel.ToDateRange).
		if !hasStart || !hasEnd {
			today := time.Now().UTC().Truncate(24 * time.Hour)
			start = today.AddDate(0, 0, -30)
			end = today.AddDate(0, 0, 1).Add(-time.Millisecond)
		}

		m.mu.Lock()
		for _, e := range m.events {
			d, ok := eventDate(e)
			if !ok {
				continue
			}
			// Inclusive on both ends, like the real event stores.
			if d.Before(start) || d.After(end) {
				continue
			}
			matched = append(matched, e)
		}
		m.mu.Unlock()
	}

	page := matched
	var next interface{} // null when there are no further pages
	if len(matched) > mockPageSize {
		page = matched[:mockPageSize]
		m.mu.Lock()
		m.cursorSeq++
		ct := fmt.Sprintf("mock-continuation-%d", m.cursorSeq)
		m.cursors[ct] = matched[mockPageSize:]
		m.mu.Unlock()
		next = ct
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"object":            "list",
		"data":              page,
		"continuationToken": next,
	})
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// eventDate parses an event's "date" field.
func eventDate(e utils.Dict) (time.Time, bool) {
	s, _ := e["date"].(string)
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// --- realistic event fixtures -------------------------------------------------

// realisticBitwardenEvent returns a record shaped like a real Bitwarden
// organization event: the documented EventResponseModel field set with its mix
// of int enums (type, device), strings (uuids, ipAddress) and nulls, and a
// sub-second-precision date. All identifiers are clearly fake.
func realisticBitwardenEvent(eventType int, itemID string, date time.Time) utils.Dict {
	return utils.Dict{
		"object":           "event",
		"type":             eventType, // e.g. 1000 User_LoggedIn, 1107 Cipher_ClientViewed
		"itemId":           itemID,
		"collectionId":     nil,
		"groupId":          nil,
		"policyId":         nil,
		"memberId":         "11111111-1111-1111-1111-111111111111",
		"actingUserId":     "22222222-2222-2222-2222-222222222222",
		"installationId":   nil,
		"date":             date.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		"device":           9, // DeviceType 9 = ChromeBrowser
		"ipAddress":        "198.51.100.10",
		"secretId":         nil,
		"projectId":        nil,
		"serviceAccountId": nil,
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

const (
	testMockClientID     = "organization.33333333-3333-3333-3333-333333333333"
	testMockClientSecret = "fake-test-client-secret"
)

// --- tests --------------------------------------------------------------------

// TestMockEventsEndToEnd drives the adapter against the mock API and asserts
// the exact messages shipped: the payload preserved verbatim (nulls, int enums
// and string ids included), an ingestion-time TimestampMs, no EventType (the
// adapter does not set one), a single cached OAuth token, and that re-polling
// the time-windowed API does not re-ship anything.
func TestMockEventsEndToEnd(t *testing.T) {
	mock := newMockBitwarden(testMockClientID, testMockClientSecret)
	now := time.Now()
	want := []utils.Dict{
		realisticBitwardenEvent(1000, "aaaaaaaa-1111-1111-1111-000000000001", now.Add(-10*time.Minute)),
		realisticBitwardenEvent(1107, "aaaaaaaa-1111-1111-1111-000000000002", now.Add(-8*time.Minute)),
		realisticBitwardenEvent(1115, "aaaaaaaa-1111-1111-1111-000000000003", now.Add(-5*time.Minute)),
	}
	mock.setEvents(want)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	startMs := uint64(time.Now().UnixMilli())
	sink := &captureSink{}
	a := startTestAdapter(t, server.URL, sink, 50*time.Millisecond, testMockClientID, testMockClientSecret)
	defer a.stop()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 events to ship")

	// Re-polling must not re-ship: later polls query [lastEnd, now] windows
	// whose start is after the fixtures' dates.
	require.Never(t, func() bool { return sink.count() != 3 },
		400*time.Millisecond, 30*time.Millisecond, "events were re-shipped on a later poll")

	// The OAuth token is requested once and then cached for its lifetime.
	assert.Equal(t, 1, mock.tokenRequestCount(), "token should be fetched once and cached")
	assert.GreaterOrEqual(t, mock.eventRequestCount(), 2, "the adapter should keep polling")

	endMs := uint64(time.Now().UnixMilli())
	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		// The adapter ships raw events without an EventType, and stamps them
		// with ingestion time (not the event's own date).
		assert.Empty(t, msg.EventType, "bitwarden adapter does not set an EventType")
		assert.GreaterOrEqual(t, msg.TimestampMs, startMs)
		assert.LessOrEqual(t, msg.TimestampMs, endMs)
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["itemId"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	for _, src := range want {
		id := src["itemId"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "event %s was not shipped", id)
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Bitwarden event verbatim")
	}
}

// TestMockMidRunEventShipsExactlyOnce verifies an event appearing after the
// first poll is picked up by a subsequent start/end window and shipped exactly
// once, while already-shipped events are never re-sent.
func TestMockMidRunEventShipsExactlyOnce(t *testing.T) {
	mock := newMockBitwarden(testMockClientID, testMockClientSecret)
	now := time.Now()
	mock.setEvents([]utils.Dict{
		realisticBitwardenEvent(1000, "aaaaaaaa-1111-1111-1111-00000000000a", now.Add(-10*time.Minute)),
		realisticBitwardenEvent(1107, "aaaaaaaa-1111-1111-1111-00000000000b", now.Add(-9*time.Minute)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	a := startTestAdapter(t, server.URL, sink, 50*time.Millisecond, testMockClientID, testMockClientSecret)
	defer a.stop()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "initial events should ship")

	// A new event occurs mid-run. The adapter formats its start/end query
	// parameters at second precision, so it can take up to ~1s of polls before
	// a window covers the new event -- but it must then ship exactly once.
	mock.addEvent(realisticBitwardenEvent(1114, "aaaaaaaa-1111-1111-1111-00000000000c", time.Now()))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "the new event should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		500*time.Millisecond, 30*time.Millisecond, "the new event must not ship twice")

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["itemId"].(string)]++
	}
	assert.Equal(t, map[string]int{
		"aaaaaaaa-1111-1111-1111-00000000000a": 1,
		"aaaaaaaa-1111-1111-1111-00000000000b": 1,
		"aaaaaaaa-1111-1111-1111-00000000000c": 1,
	}, shippedPerID, "every event must ship exactly once")
}

// TestMockPaginationFullDataset verifies a dataset larger than the API's page
// size is fully consumed through the continuationToken mechanism within a
// single poll cycle, with every event shipped exactly once.
func TestMockPaginationFullDataset(t *testing.T) {
	const total = 120 // 3 pages at the API's 50-per-page limit

	mock := newMockBitwarden(testMockClientID, testMockClientSecret)
	events := make([]utils.Dict, total)
	base := time.Now().Add(-time.Hour)
	for i := 0; i < total; i++ {
		events[i] = realisticBitwardenEvent(
			1107,
			fmt.Sprintf("aaaaaaaa-1111-1111-1111-%012d", i),
			base.Add(time.Duration(i)*time.Second))
	}
	mock.setEvents(events)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	a := startTestAdapter(t, server.URL, sink, 50*time.Millisecond, testMockClientID, testMockClientSecret)
	defer a.stop()

	require.Eventually(t, func() bool { return sink.count() == total },
		5*time.Second, 20*time.Millisecond, "all paginated events should ship")
	require.Never(t, func() bool { return sink.count() != total },
		400*time.Millisecond, 30*time.Millisecond, "no event may ship twice")

	assert.GreaterOrEqual(t, mock.continuationRequestCount(), 2,
		"the adapter should have followed the continuation token for pages 2 and 3")

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		ids[msg.JsonPayload["itemId"].(string)] = true
	}
	assert.Len(t, ids, total, "every distinct event should be shipped once")
}

// TestMockBadCredentialsShipNothing verifies that when the identity endpoint
// rejects the client credentials, nothing ships and the events endpoint is
// never reached. This adapter's behavior on bad credentials is to keep
// retrying on its poll interval (it does not stop), so the test also asserts
// it stays alive and keeps re-attempting the token exchange.
func TestMockBadCredentialsShipNothing(t *testing.T) {
	mock := newMockBitwarden(testMockClientID, testMockClientSecret)
	mock.setEvents([]utils.Dict{
		realisticBitwardenEvent(1000, "aaaaaaaa-1111-1111-1111-000000000001", time.Now().Add(-time.Minute)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	a := startTestAdapter(t, server.URL, sink, 50*time.Millisecond, testMockClientID, "wrong-secret")
	defer a.stop()

	require.Eventually(t, func() bool { return mock.tokenRequestCount() >= 3 },
		5*time.Second, 20*time.Millisecond, "the adapter should keep retrying the token exchange")

	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
	assert.Equal(t, 0, mock.eventRequestCount(), "the events endpoint should never be reached without a token")
	select {
	case <-a.chStopped:
		t.Fatal("bad credentials should not stop this adapter; it retries each poll")
	default:
	}
	assert.False(t, a.doStop.IsSet())
}

// TestMockUnauthorizedEventsKeepsPolling verifies that a 401 from the events
// endpoint (e.g. a revoked token) ships nothing, keeps the adapter alive, and
// does not advance its time checkpoint (it keeps retrying).
func TestMockUnauthorizedEventsKeepsPolling(t *testing.T) {
	mock := newMockBitwarden(testMockClientID, testMockClientSecret)
	mock.rejectBearer = true
	mock.setEvents([]utils.Dict{
		realisticBitwardenEvent(1000, "aaaaaaaa-1111-1111-1111-000000000001", time.Now().Add(-time.Minute)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	a := startTestAdapter(t, server.URL, sink, 50*time.Millisecond, testMockClientID, testMockClientSecret)
	defer a.stop()

	require.Eventually(t, func() bool { return mock.eventRequestCount() >= 3 },
		5*time.Second, 20*time.Millisecond, "the adapter should keep polling through 401s")

	assert.Equal(t, 0, sink.count(), "nothing should ship when the events endpoint rejects the token")
	select {
	case <-a.chStopped:
		t.Fatal("a 401 from the events endpoint should not stop this adapter")
	default:
	}

	// Once the token is honored again, the backlog ships: the checkpoint was
	// never advanced by the failing polls, so the first successful poll still
	// sends no start/end and gets the API's default last-30-days window,
	// which covers the fixture.
	mock.mu.Lock()
	mock.rejectBearer = false
	mock.mu.Unlock()
	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond, "the event should ship once the API accepts the token again")
}

// TestMockTokenRefresh verifies the adapter requests a fresh OAuth token when
// the previous one is about to expire (the adapter refreshes within a 5 minute
// buffer of expiry) and keeps collecting events across token rotations.
func TestMockTokenRefresh(t *testing.T) {
	mock := newMockBitwarden(testMockClientID, testMockClientSecret)
	mock.tokenExpiresIn = 1 // always inside the adapter's 5-minute refresh buffer
	now := time.Now()
	mock.setEvents([]utils.Dict{
		realisticBitwardenEvent(1000, "aaaaaaaa-1111-1111-1111-000000000001", now.Add(-10*time.Minute)),
		realisticBitwardenEvent(1107, "aaaaaaaa-1111-1111-1111-000000000002", now.Add(-9*time.Minute)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	a := startTestAdapter(t, server.URL, sink, 50*time.Millisecond, testMockClientID, testMockClientSecret)
	defer a.stop()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "events should ship despite short-lived tokens")
	require.Eventually(t, func() bool { return mock.tokenRequestCount() >= 3 },
		5*time.Second, 20*time.Millisecond, "near-expiry tokens should be refreshed on each poll")
	require.Never(t, func() bool { return sink.count() != 2 },
		400*time.Millisecond, 30*time.Millisecond, "token refreshes must not cause re-shipping")
}
