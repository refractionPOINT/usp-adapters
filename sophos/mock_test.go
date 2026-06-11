package usp_sophos

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

// This file exercises the adapter end-to-end against a mock of the Sophos
// Central SIEM API (OAuth2 token endpoint + /siem/v1/events), capturing the
// exact messages it ships so their content -- timestamp and verbatim payload --
// can be asserted.

const (
	mockTokenPath = "/api/v2/oauth2/token"
	mockCursorFmt = "cur-%d"
)

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

// --- mock Sophos Central ------------------------------------------------------

// mockSophosCentral is an in-memory stand-in for Sophos Central. It serves:
//
//   - POST /api/v2/oauth2/token -- the OAuth2 client-credentials exchange
//     (grant_type=client_credentials&scope=token&client_id&client_secret as a
//     form body), returning {"access_token": ..., "token_type": "bearer", ...}.
//   - GET /siem/v1/events -- the SIEM events endpoint. It authenticates the
//     Bearer token and the X-Tenant-ID header, honours either ?from_date=<epoch>
//     (first poll) or ?cursor=<opaque> (subsequent polls), and answers with the
//     real envelope {"items": [...], "has_more": <bool>, "next_cursor": <string>}.
//
// Events are kept oldest-first; a cursor is an opaque "cur-<index>" marking the
// next event to serve, so a repeated poll at the tail returns no items and the
// same cursor -- exactly how the real API lets a client tail the event stream.
type mockSophosCentral struct {
	t *testing.T

	clientID     string
	clientSecret string
	tenantID     string

	mu          sync.Mutex
	events      []utils.Dict
	pageSize    int // server-side page cap; 0 means honor the request's limit
	validTokens map[string]bool
	tokenSeq    int

	tokenRequests     int
	fromDateRequests  int
	cursorRequests    int
	rejectedEventReqs int
	lastFromDate      string
	lastTokenForm     map[string]string
	lastEventsAuth    string
	lastEventsTenant  string
}

func newMockSophosCentral(t *testing.T, clientID, clientSecret, tenantID string) *mockSophosCentral {
	return &mockSophosCentral{
		t:            t,
		clientID:     clientID,
		clientSecret: clientSecret,
		tenantID:     tenantID,
		validTokens:  map[string]bool{},
	}
}

func (m *mockSophosCentral) setEvents(events []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = events
}

// appendEvent adds a new event at the tail of the stream, as Sophos Central
// does when new telemetry arrives.
func (m *mockSophosCentral) appendEvent(event utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
}

func (m *mockSophosCentral) counts() (token, fromDate, cursor, rejected int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenRequests, m.fromDateRequests, m.cursorRequests, m.rejectedEventReqs
}

func (m *mockSophosCentral) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(mockTokenPath, m.handleToken)
	mux.HandleFunc(eventsURL, m.handleEvents)
	return mux
}

func (m *mockSophosCentral) handleToken(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.tokenRequests++
	m.mu.Unlock()

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/x-www-form-urlencoded") {
		writeJSON(w, http.StatusBadRequest, utils.Dict{"error": "invalid_request", "message": "expected form body"})
		return
	}
	if err := r.ParseForm(); err != nil {
		writeJSON(w, http.StatusBadRequest, utils.Dict{"error": "invalid_request"})
		return
	}

	m.mu.Lock()
	m.lastTokenForm = map[string]string{
		"grant_type":    r.PostFormValue("grant_type"),
		"scope":         r.PostFormValue("scope"),
		"client_id":     r.PostFormValue("client_id"),
		"client_secret": r.PostFormValue("client_secret"),
	}
	m.mu.Unlock()

	if r.PostFormValue("grant_type") != "client_credentials" ||
		r.PostFormValue("scope") != "token" {
		writeJSON(w, http.StatusBadRequest, utils.Dict{
			"error": "unsupported_grant_type", "message": "grant_type must be client_credentials with scope token",
		})
		return
	}
	if r.PostFormValue("client_id") != m.clientID || r.PostFormValue("client_secret") != m.clientSecret {
		writeJSON(w, http.StatusUnauthorized, utils.Dict{
			"errorCode": "oauth.invalid_client_secret",
			"message":   "Invalid client id or secret",
		})
		return
	}

	m.mu.Lock()
	m.tokenSeq++
	token := fmt.Sprintf("sophos-jwt-%d", m.tokenSeq)
	m.validTokens[token] = true
	m.mu.Unlock()

	writeJSON(w, http.StatusOK, utils.Dict{
		"access_token": token,
		"token_type":   "bearer",
		"expires_in":   3600,
		"message":      "OK",
		"errorCode":    "success",
	})
}

func (m *mockSophosCentral) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	auth := r.Header.Get("Authorization")
	tenant := r.Header.Get("X-Tenant-ID")
	token := strings.TrimPrefix(auth, "Bearer ")

	m.mu.Lock()
	m.lastEventsAuth = auth
	m.lastEventsTenant = tenant
	authOK := strings.HasPrefix(auth, "Bearer ") && m.validTokens[token] && tenant == m.tenantID
	if !authOK {
		m.rejectedEventReqs++
		m.mu.Unlock()
		writeJSON(w, http.StatusUnauthorized, utils.Dict{"error": "Unauthorized", "message": "invalid token or tenant"})
		return
	}

	defer m.mu.Unlock()

	// Resolve the starting index from either the cursor or from_date.
	start := 0
	if cur := r.URL.Query().Get("cursor"); cur != "" {
		m.cursorRequests++
		idx, err := strconv.Atoi(strings.TrimPrefix(cur, "cur-"))
		if err != nil || !strings.HasPrefix(cur, "cur-") || idx < 0 {
			writeJSON(w, http.StatusBadRequest, utils.Dict{"error": "Bad Request", "message": "invalid cursor"})
			return
		}
		start = idx
	} else {
		fromDate := r.URL.Query().Get("from_date")
		m.fromDateRequests++
		m.lastFromDate = fromDate
		epoch, err := strconv.ParseInt(fromDate, 10, 64)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, utils.Dict{"error": "Bad Request", "message": "invalid from_date"})
			return
		}
		// Skip events created before from_date.
		for start < len(m.events) {
			created, perr := time.Parse(time.RFC3339, m.events[start].FindOneString("created_at"))
			if perr == nil && !created.Before(time.Unix(epoch, 0)) {
				break
			}
			start++
		}
	}
	if start > len(m.events) {
		start = len(m.events)
	}

	// Page the result set: the server caps how many items one response carries.
	limit := 200
	if l, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && l > 0 {
		limit = l
	}
	if m.pageSize > 0 && m.pageSize < limit {
		limit = m.pageSize
	}
	end := start + limit
	if end > len(m.events) {
		end = len(m.events)
	}

	items := make([]utils.Dict, 0, end-start)
	items = append(items, m.events[start:end]...)

	writeJSON(w, http.StatusOK, utils.Dict{
		"items":       items,
		"has_more":    end < len(m.events),
		"next_cursor": fmt.Sprintf(mockCursorFmt, end),
	})
}

func writeJSON(w http.ResponseWriter, status int, body utils.Dict) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// --- realistic event fixtures ---------------------------------------------------

// sophosTime renders a timestamp the way Sophos Central does: RFC 3339 UTC with
// millisecond precision.
func sophosTime(ts time.Time) string {
	return ts.UTC().Format("2006-01-02T15:04:05.000Z")
}

// realisticThreatEvent returns an event shaped like a real Sophos Central SIEM
// endpoint threat detection.
func realisticThreatEvent(id string, createdAt time.Time) utils.Dict {
	when := sophosTime(createdAt)
	return utils.Dict{
		"id":            id,
		"created_at":    when,
		"when":          when,
		"severity":      "high",
		"type":          "Event::Endpoint::Threat::Detected",
		"name":          `Malware detected: 'EICAR-AV-Test' at 'C:\Users\jdoe\Downloads\eicar.com'`,
		"group":         "MALWARE",
		"datastream":    "event",
		"endpoint_id":   "11111111-1111-1111-1111-111111111111",
		"endpoint_type": "computer",
		"customer_id":   "11111111-1111-1111-1111-222222222222",
		"user_id":       "11111111-1111-1111-1111-333333333333",
		"source":        `EXAMPLE\jdoe`,
		"suser":         `EXAMPLE\jdoe`,
		"location":      "WIN-EXAMPLE-01",
		"dhost":         "WIN-EXAMPLE-01",
		"threat":        "EICAR-AV-Test",
		"source_info":   utils.Dict{"ip": "10.1.2.3"},
	}
}

// realisticWebEvent returns an event shaped like a Sophos Central SIEM web
// control event.
func realisticWebEvent(id string, createdAt time.Time) utils.Dict {
	when := sophosTime(createdAt)
	return utils.Dict{
		"id":            id,
		"created_at":    when,
		"when":          when,
		"severity":      "medium",
		"type":          "Event::Endpoint::WebFilteringBlocked",
		"name":          "Access to 'blocked.example.com' was blocked by policy",
		"group":         "WEB",
		"datastream":    "event",
		"endpoint_id":   "11111111-1111-1111-1111-111111111111",
		"endpoint_type": "computer",
		"customer_id":   "11111111-1111-1111-1111-222222222222",
		"user_id":       "11111111-1111-1111-1111-333333333333",
		"source":        `EXAMPLE\jdoe`,
		"suser":         `EXAMPLE\jdoe`,
		"location":      "WIN-EXAMPLE-01",
		"dhost":         "WIN-EXAMPLE-01",
		"source_info":   utils.Dict{"ip": "10.1.2.3"},
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// startMockAdapter wires a config at the mock server and starts the adapter
// with a capture sink and a fast poll interval.
func startMockAdapter(t *testing.T, server *httptest.Server, conf SophosConfig) (*SophosAdapter, chan struct{}, *captureSink) {
	t.Helper()
	conf.URL = server.URL
	conf.AuthURL = server.URL + mockTokenPath
	if conf.PollInterval == 0 {
		conf.PollInterval = 30 * time.Millisecond
	}
	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	adapter, chStopped, err := newSophosAdapter(ctx, conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = adapter.Close() })
	return adapter, chStopped, sink
}

// --- tests ---------------------------------------------------------------------

// TestMockEventsEndToEnd drives the adapter against the mock Sophos Central API
// and asserts the exact events shipped: ingestion-time timestamps and payloads
// preserved verbatim, with the OAuth exchange, Bearer token, tenant header and
// from_date/cursor query parameters all validated by the mock. It also asserts
// that re-polling advances to cursor mode and never re-ships an event.
func TestMockEventsEndToEnd(t *testing.T) {
	conf := testConfig(t)
	mock := newMockSophosCentral(t, conf.ClientId, conf.ClientSecret, conf.TenantId)

	now := time.Now()
	want := []utils.Dict{
		realisticThreatEvent("11111111-0000-0000-0000-000000000001", now.Add(-20*time.Second)),
		realisticWebEvent("11111111-0000-0000-0000-000000000002", now.Add(-15*time.Second)),
		realisticThreatEvent("11111111-0000-0000-0000-000000000003", now.Add(-10*time.Second)),
	}
	mock.setEvents(want)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	startMs := uint64(time.Now().UnixMilli())
	_, chStopped, sink := startMockAdapter(t, server, conf)

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 events to ship")

	// Re-polling: wait for several cursor polls past the tail, then verify
	// nothing was re-shipped.
	require.Eventually(t, func() bool { _, _, cursor, _ := mock.counts(); return cursor >= 3 },
		5*time.Second, 20*time.Millisecond, "adapter should keep polling with the cursor")
	assert.Equal(t, 3, sink.count(), "re-polling must not re-ship events")

	select {
	case <-chStopped:
		t.Fatal("adapter stopped unexpectedly")
	default:
	}

	// The OAuth exchange carried the client-credentials form.
	mock.mu.Lock()
	tokenForm := mock.lastTokenForm
	eventsAuth := mock.lastEventsAuth
	eventsTenant := mock.lastEventsTenant
	lastFromDate := mock.lastFromDate
	mock.mu.Unlock()
	require.NotNil(t, tokenForm, "the token endpoint was never called")
	assert.Equal(t, "client_credentials", tokenForm["grant_type"])
	assert.Equal(t, "token", tokenForm["scope"])
	assert.Equal(t, conf.ClientId, tokenForm["client_id"])
	assert.Equal(t, conf.ClientSecret, tokenForm["client_secret"])

	// The events requests carried the issued Bearer token and the tenant header.
	assert.True(t, strings.HasPrefix(eventsAuth, "Bearer sophos-jwt-"),
		"events request must carry the issued bearer token, got %q", eventsAuth)
	assert.Equal(t, conf.TenantId, eventsTenant)

	// The first poll used from_date = now-30s (epoch seconds); later polls use
	// the cursor.
	tokenN, fromDateN, _, rejectedN := mock.counts()
	assert.GreaterOrEqual(t, tokenN, 2, "a JWT is fetched on every poll")
	assert.Equal(t, 1, fromDateN, "only the first poll uses from_date")
	assert.Equal(t, 0, rejectedN, "no request should have failed authentication")
	fd, err := strconv.ParseInt(lastFromDate, 10, 64)
	require.NoError(t, err, "from_date must be epoch seconds, got %q", lastFromDate)
	assert.GreaterOrEqual(t, fd, now.Unix()-31, "from_date should be ~30s before the poll")
	assert.LessOrEqual(t, fd, time.Now().Unix()-29, "from_date should be ~30s before the poll")

	// Each shipped message preserves the event verbatim and is stamped with
	// ingestion time (the adapter does not parse event timestamps).
	endMs := uint64(time.Now().UnixMilli())
	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		assert.Equal(t, "", msg.EventType, "the adapter does not tag an event type")
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
		assert.GreaterOrEqual(t, msg.TimestampMs, startMs)
		assert.LessOrEqual(t, msg.TimestampMs, endMs)
	}
	require.Len(t, byID, 3)
	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "event %s was not shipped", id)
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Sophos event verbatim")
	}
}

// TestMockMidRunEventShipsOnce verifies an event arriving after the adapter has
// caught up is picked up via the cursor and shipped exactly once.
func TestMockMidRunEventShipsOnce(t *testing.T) {
	conf := testConfig(t)
	mock := newMockSophosCentral(t, conf.ClientId, conf.ClientSecret, conf.TenantId)

	now := time.Now()
	mock.setEvents([]utils.Dict{
		realisticThreatEvent("11111111-0000-0000-0000-00000000000a", now.Add(-12*time.Second)),
		realisticThreatEvent("11111111-0000-0000-0000-00000000000b", now.Add(-6*time.Second)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	_, _, sink := startMockAdapter(t, server, conf)

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)

	// A new event arrives in Sophos Central mid-run.
	mock.appendEvent(realisticThreatEvent("11111111-0000-0000-0000-00000000000c", time.Now()))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "the new event should ship")

	// Let a few more cursor polls happen, then check nothing re-shipped.
	_, _, cursorBase, _ := mock.counts()
	require.Eventually(t, func() bool { _, _, cursor, _ := mock.counts(); return cursor >= cursorBase+3 },
		5*time.Second, 20*time.Millisecond)
	require.Equal(t, 3, sink.count(), "no event may ship more than once")

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["id"].(string)]++
	}
	assert.Equal(t, map[string]int{
		"11111111-0000-0000-0000-00000000000a": 1,
		"11111111-0000-0000-0000-00000000000b": 1,
		"11111111-0000-0000-0000-00000000000c": 1,
	}, shippedPerID, "every event must ship exactly once")
}

// TestMockMultiPagePagination verifies a dataset spanning several has_more pages
// is fully consumed (one page per poll tick, following next_cursor) and every
// event ships exactly once.
func TestMockMultiPagePagination(t *testing.T) {
	conf := testConfig(t)
	mock := newMockSophosCentral(t, conf.ClientId, conf.ClientSecret, conf.TenantId)
	mock.pageSize = 2 // server-side cap: 5 events => 3 pages

	now := time.Now()
	const total = 5
	events := make([]utils.Dict, total)
	for i := 0; i < total; i++ {
		events[i] = realisticThreatEvent(
			fmt.Sprintf("11111111-0000-0000-0000-0000000000%02d", i),
			now.Add(time.Duration(i-25)*time.Second))
	}
	mock.setEvents(events)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	_, _, sink := startMockAdapter(t, server, conf)

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 20*time.Millisecond, "the whole multi-page dataset should ship")

	_, fromDateN, cursorN, _ := mock.counts()
	assert.Equal(t, 1, fromDateN, "only the first request uses from_date")
	assert.GreaterOrEqual(t, cursorN, 2, "the remaining pages are walked via next_cursor")

	// A few more polls past the tail must not re-ship anything.
	require.Eventually(t, func() bool { _, _, cursor, _ := mock.counts(); return cursor >= 5 },
		5*time.Second, 20*time.Millisecond)
	assert.Equal(t, total, sink.count())

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		ids[msg.JsonPayload["id"].(string)] = true
	}
	assert.Len(t, ids, total, "every distinct event should ship exactly once")
}

// TestMockBadCredentialsShipNothing pins the adapter's behavior on a failed
// OAuth exchange: it ships nothing and keeps retrying (it does not stop). Each
// poll fails to obtain a JWT, falls through to the events endpoint with an
// empty bearer token and is rejected there too.
func TestMockBadCredentialsShipNothing(t *testing.T) {
	conf := testConfig(t)
	mock := newMockSophosCentral(t, conf.ClientId, "the-real-secret", conf.TenantId)
	mock.setEvents([]utils.Dict{
		realisticThreatEvent("11111111-0000-0000-0000-000000000001", time.Now().Add(-5*time.Second)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	// conf.ClientSecret does not match the mock's secret.
	_, chStopped, sink := startMockAdapter(t, server, conf)

	// Wait for several failed polls.
	require.Eventually(t, func() bool {
		token, _, _, rejected := mock.counts()
		return token >= 2 && rejected >= 2
	}, 5*time.Second, 20*time.Millisecond, "the adapter should keep retrying the exchange")

	assert.Equal(t, 0, sink.count(), "nothing may ship when authentication fails")

	select {
	case <-chStopped:
		t.Fatal("the adapter does not stop on auth failure; it retries every poll")
	default:
	}
}
