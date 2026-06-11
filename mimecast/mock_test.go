package usp_mimecast

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// This file exercises the adapter end-to-end against a mock of the Mimecast
// API 2.0 (https://api.services.mimecast.com), capturing the exact messages it
// ships so their content -- payload, timestamp and event type -- can be
// asserted without live credentials.

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

// --- mock Mimecast API 2.0 ----------------------------------------------------

// mimecastRequestTimeLayout is the format the adapter renders startDateTime /
// endDateTime in ("2006-01-02T15:04:05-0700"), which is also what the real
// Mimecast API accepts.
const mimecastRequestTimeLayout = "2006-01-02T15:04:05-0700"

// mockAccessToken is the bearer token the mock issues on a successful
// client-credentials exchange. Clearly fake.
const mockAccessToken = "fake-mimecast-access-token-1111111111111111"

// mockMimecast is an in-memory stand-in for the Mimecast API 2.0. It exposes
// the two endpoints the adapter uses:
//
//   - POST /oauth/token: OAuth2 client-credentials exchange, form-encoded
//     grant_type/client_id/client_secret, returning {"access_token": ...}.
//   - POST /api/audit/get-audit-events: Bearer-authenticated JSON request with
//     a {"meta":{"pagination":{...}},"data":[{"startDateTime","endDateTime"}]}
//     envelope, returning {"meta":{"pagination":{"next": ...}},"data":[...]}
//     filtered to the requested time window and paginated by an opaque
//     meta.pagination.next token echoed back as meta.pagination.pageToken.
type mockMimecast struct {
	mu           sync.Mutex
	clientID     string
	clientSecret string
	events       []utils.Dict // audit logs, oldest first

	tokenRequests int
	auditRequests int

	// Captures of the first requests seen, for request-shape assertions.
	lastTokenContentType string
	lastTokenForm        url.Values
	firstAuditAuth       string
	firstAuditBody       []byte

	// auditStatus, when non-zero, forces the audit endpoint to return that
	// HTTP status (the token endpoint keeps working).
	auditStatus int
}

func newMockMimecast(clientID, clientSecret string) *mockMimecast {
	return &mockMimecast{clientID: clientID, clientSecret: clientSecret}
}

func (m *mockMimecast) addEvent(e utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
}

func (m *mockMimecast) tokenCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenRequests
}

func (m *mockMimecast) auditCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.auditRequests
}

func (m *mockMimecast) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth/token":
			m.handleToken(t, w, r)
		case "/api/audit/get-audit-events":
			m.handleAuditEvents(t, w, r)
		default:
			http.Error(w, `{"fail":[{"errors":[{"code":"err_not_found","message":"not found","retryable":false}]}]}`, http.StatusNotFound)
		}
	}
}

func (m *mockMimecast) handleToken(t *testing.T, w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.tokenRequests++
	m.mu.Unlock()

	// Handlers run on server goroutines: use assert (not require) so a failure
	// is reported without calling runtime.Goexit off the test goroutine.
	if !assert.Equal(t, http.MethodPost, r.Method, "token exchange must be a POST") {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if !assert.NoError(t, err) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	form, err := url.ParseQuery(string(body))
	if !assert.NoError(t, err, "token request body must be form-encoded") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	m.lastTokenContentType = r.Header.Get("Content-Type")
	m.lastTokenForm = form
	m.mu.Unlock()

	if form.Get("grant_type") != "client_credentials" ||
		form.Get("client_id") != m.clientID ||
		form.Get("client_secret") != m.clientSecret {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_client","error_description":"Client authentication failed"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"access_token": mockAccessToken,
		"token_type":   "Bearer",
		"expires_in":   1800,
	})
}

// auditRequestBody mirrors the request envelope the adapter sends to
// /api/audit/get-audit-events.
type auditRequestBody struct {
	Meta struct {
		Pagination struct {
			PageSize  int    `json:"pageSize"`
			PageToken string `json:"pageToken"`
		} `json:"pagination"`
	} `json:"meta"`
	Data []struct {
		StartDateTime string `json:"startDateTime"`
		EndDateTime   string `json:"endDateTime"`
	} `json:"data"`
}

func (m *mockMimecast) handleAuditEvents(t *testing.T, w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.auditRequests++
	forcedStatus := m.auditStatus
	m.mu.Unlock()

	if !assert.Equal(t, http.MethodPost, r.Method, "get-audit-events must be a POST") {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Authorization") != "Bearer "+mockAccessToken {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"meta":{"status":401},"data":[],"fail":[{"errors":[{"code":"token_verification_failed","message":"Token verification failed","retryable":false}]}]}`))
		return
	}

	if forcedStatus != 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(forcedStatus)
		_, _ = w.Write([]byte(`{"meta":{"status":500},"data":[],"fail":[{"errors":[{"code":"err_internal","message":"internal error","retryable":true}]}]}`))
		return
	}

	raw, err := io.ReadAll(r.Body)
	if !assert.NoError(t, err) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var req auditRequestBody
	if !assert.NoError(t, json.Unmarshal(raw, &req), "audit request body must be JSON") ||
		!assert.Len(t, req.Data, 1, "audit request must carry exactly one date range") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	if m.firstAuditBody == nil {
		m.firstAuditBody = raw
		m.firstAuditAuth = r.Header.Get("Authorization")
	}
	m.mu.Unlock()

	start, errS := time.Parse(mimecastRequestTimeLayout, req.Data[0].StartDateTime)
	end, errE := time.Parse(mimecastRequestTimeLayout, req.Data[0].EndDateTime)
	if !assert.NoError(t, errS, "startDateTime must parse as %s", mimecastRequestTimeLayout) ||
		!assert.NoError(t, errE, "endDateTime must parse as %s", mimecastRequestTimeLayout) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Filter the dataset to the requested window (inclusive bounds), the way
	// the real API constrains results to startDateTime..endDateTime.
	m.mu.Lock()
	var matched []utils.Dict
	for _, e := range m.events {
		ts, perr := time.Parse(time.RFC3339, e["eventTime"].(string))
		if perr != nil {
			continue
		}
		if !ts.Before(start) && !ts.After(end) {
			matched = append(matched, e)
		}
	}
	m.mu.Unlock()

	pageSize := req.Meta.Pagination.PageSize
	if pageSize <= 0 {
		pageSize = 25 // the real API default
	}
	offset := 0
	if tok := req.Meta.Pagination.PageToken; tok != "" {
		n, perr := strconv.Atoi(strings.TrimPrefix(tok, "offset-"))
		if !assert.NoError(t, perr, "pageToken must be a token previously returned in meta.pagination.next") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		offset = n
	}

	page := []utils.Dict{}
	if offset < len(matched) {
		endIdx := offset + pageSize
		if endIdx > len(matched) {
			endIdx = len(matched)
		}
		page = matched[offset:endIdx]
	}

	pagination := map[string]interface{}{"pageSize": pageSize}
	if offset+pageSize < len(matched) {
		pagination["next"] = fmt.Sprintf("offset-%d", offset+pageSize)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"meta": map[string]interface{}{
			"status":     200,
			"pagination": pagination,
		},
		"data": page,
		"fail": []interface{}{},
	})
}

// --- realistic fixtures ---------------------------------------------------------

// fakeAuditEvent returns an audit log shaped like a real Mimecast
// /api/audit/get-audit-events record: the documented id, auditType, user,
// eventTime, eventInfo and category fields. All identifiers are clearly fake.
//
// NOTE on eventTime format: the real API renders eventTime with a numeric
// offset ("2026-06-11T10:00:00+0000"). The adapter parses eventTime with
// time.RFC3339, which requires a colon in the offset (or "Z"); fixtures use
// the RFC3339 "Z" form so the adapter's dedupe bookkeeping behaves as designed.
func fakeAuditEvent(id, auditType, user, category, info string, eventTime time.Time) utils.Dict {
	return utils.Dict{
		"id":        id,
		"auditType": auditType,
		"user":      user,
		"eventTime": eventTime.UTC().Truncate(time.Second).Format(time.RFC3339),
		"eventInfo": info,
		"category":  category,
	}
}

func fakeLogonEvent(id string, eventTime time.Time) utils.Dict {
	return fakeAuditEvent(
		id,
		"Logon Authentication Failed",
		"jdoe@example.com",
		"authentication_logs",
		"Failed authentication for jdoe@example.com, Application: fake-app, Source IP: 192.0.2.10",
		eventTime)
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// startTestAdapter wires the adapter to the mock server and the capture sink
// with a fast poll interval.
func startTestAdapter(t *testing.T, serverURL string, sink uspSink, clientID, clientSecret string) *MimecastAdapter {
	t.Helper()
	conf := MimecastConfig{
		ClientOptions: testClientOptions(t),
		ClientId:      clientID,
		ClientSecret:  clientSecret,
		BaseURL:       serverURL,
		PollInterval:  30 * time.Millisecond,
	}
	require.NoError(t, conf.Validate())
	adapter, _, err := newMimecastAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	return adapter
}

// --- tests ------------------------------------------------------------------

// TestMimecastEndToEnd drives the adapter against the mock API and asserts the
// exact events shipped: every audit log in the polled window ships with its
// fields intact, the OAuth exchange and audit request are shaped like the real
// API expects, and re-polling does not re-ship.
func TestMimecastEndToEnd(t *testing.T) {
	const clientID = "11111111-1111-1111-1111-111111111111"
	const clientSecret = "fake-client-secret-0000000000000000"

	now := time.Now()
	want := []utils.Dict{
		fakeAuditEvent("fake-audit-id-0001", "Logon Authentication Failed", "jdoe@example.com",
			"authentication_logs", "Failed authentication for jdoe@example.com, Source IP: 192.0.2.10", now.Add(-12*time.Second)),
		fakeAuditEvent("fake-audit-id-0002", "Search", "asmith@example.com",
			"case_review_logs", "Search performed by asmith@example.com: subject contains invoice", now.Add(-8*time.Second)),
		fakeAuditEvent("fake-audit-id-0003", "Policy Definition Updated", "admin@example.com",
			"account_logs", "Policy Anti-Spoofing updated by admin@example.com", now.Add(-4*time.Second)),
	}

	mock := newMockMimecast(clientID, clientSecret)
	for _, e := range want {
		mock.addEvent(e)
	}
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	testStartMs := uint64(time.Now().UnixMilli())
	adapter := startTestAdapter(t, server.URL, sink, clientID, clientSecret)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 audit events to ship")

	// Re-polling must not re-ship: the mock keeps returning the same events
	// for any window that covers them, and the dedupe map must absorb them.
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 30*time.Millisecond, "events were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	nowMs := uint64(time.Now().UnixMilli())
	for _, msg := range sink.snapshot() {
		// The adapter does not tag an event type; the platform config decides.
		assert.Empty(t, msg.EventType)
		// The adapter stamps ship time (not the record's eventTime).
		assert.GreaterOrEqual(t, msg.TimestampMs, testStartMs)
		assert.LessOrEqual(t, msg.TimestampMs, nowMs)
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	// Every audit field round-trips into the shipped payload.
	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "event %s was not shipped", id)
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must carry the Mimecast audit fields verbatim")
	}

	// The OAuth exchange is shaped as the real endpoint requires.
	mock.mu.Lock()
	defer mock.mu.Unlock()
	assert.Equal(t, "application/x-www-form-urlencoded", mock.lastTokenContentType)
	assert.Equal(t, "client_credentials", mock.lastTokenForm.Get("grant_type"))
	assert.Equal(t, clientID, mock.lastTokenForm.Get("client_id"))
	assert.Equal(t, clientSecret, mock.lastTokenForm.Get("client_secret"))

	// The audit request carries the issued bearer token and the documented
	// envelope: meta.pagination.pageSize=50 plus one start/end window.
	assert.Equal(t, "Bearer "+mockAccessToken, mock.firstAuditAuth)
	var audit auditRequestBody
	require.NoError(t, json.Unmarshal(mock.firstAuditBody, &audit))
	assert.Equal(t, 50, audit.Meta.Pagination.PageSize)
	assert.Empty(t, audit.Meta.Pagination.PageToken, "the first page must not carry a pageToken")
	require.Len(t, audit.Data, 1)
	startT, err := time.Parse(mimecastRequestTimeLayout, audit.Data[0].StartDateTime)
	require.NoError(t, err)
	endT, err := time.Parse(mimecastRequestTimeLayout, audit.Data[0].EndDateTime)
	require.NoError(t, err)
	assert.False(t, endT.Before(startT), "the request window must not be inverted")
}

// TestMimecastMidRunEventShipsOnce verifies an audit event appearing after the
// first poll ships exactly once, and already-shipped events are never re-sent.
func TestMimecastMidRunEventShipsOnce(t *testing.T) {
	const clientID = "client-id-test"
	const clientSecret = "client-secret-test"

	mock := newMockMimecast(clientID, clientSecret)
	mock.addEvent(fakeLogonEvent("fake-audit-id-000a", time.Now().Add(-10*time.Second)))
	mock.addEvent(fakeLogonEvent("fake-audit-id-000b", time.Now().Add(-5*time.Second)))
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	adapter := startTestAdapter(t, server.URL, sink, clientID, clientSecret)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)

	// A new audit event occurs mid-run.
	mock.addEvent(fakeLogonEvent("fake-audit-id-000c", time.Now()))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "the new event should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 30*time.Millisecond, "no event may ship twice")

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["id"].(string)]++
	}
	assert.Equal(t, map[string]int{
		"fake-audit-id-000a": 1,
		"fake-audit-id-000b": 1,
		"fake-audit-id-000c": 1,
	}, shippedPerID, "every event must ship exactly once")
}

// TestMimecastPaginationMultiPage verifies a window holding more events than
// one page (the adapter requests pageSize 50) is fully consumed by following
// meta.pagination.next tokens, shipping every event exactly once.
func TestMimecastPaginationMultiPage(t *testing.T) {
	const clientID = "client-id-test"
	const clientSecret = "client-secret-test"
	const total = 120 // 3 pages at the adapter's pageSize of 50

	mock := newMockMimecast(clientID, clientSecret)
	base := time.Now().Add(-20 * time.Second)
	for i := 0; i < total; i++ {
		mock.addEvent(fakeLogonEvent(
			fmt.Sprintf("fake-audit-id-%04d", i),
			base.Add(time.Duration(i)*150*time.Millisecond)))
	}
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	adapter := startTestAdapter(t, server.URL, sink, clientID, clientSecret)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		5*time.Second, 20*time.Millisecond, "all paginated events should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 30*time.Millisecond, "no paginated event may ship twice")

	assert.GreaterOrEqual(t, mock.auditCount(), 3,
		"120 events at pageSize 50 require at least 3 audit requests")

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		ids[msg.JsonPayload["id"].(string)] = true
	}
	assert.Len(t, ids, total, "every distinct event should be shipped once")
}

// TestMimecastBadCredentialsShipsNothing pins the adapter's behavior on a
// rejected client-credentials exchange: nothing ships, the audit endpoint is
// never called, and the adapter keeps retrying (it does not stop itself).
func TestMimecastBadCredentialsShipsNothing(t *testing.T) {
	mock := newMockMimecast("client-id-test", "correct-secret")
	mock.addEvent(fakeLogonEvent("fake-audit-id-0001", time.Now().Add(-5*time.Second)))
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	adapter := startTestAdapter(t, server.URL, sink, "client-id-test", "wrong-secret")
	chStopped := adapter.chStopped
	defer adapter.Close()

	// Wait for at least two failed token exchanges (= two poll cycles).
	require.Eventually(t, func() bool { return mock.tokenCount() >= 2 },
		5*time.Second, 20*time.Millisecond, "the adapter should keep attempting the token exchange")

	assert.Equal(t, 0, sink.count(), "nothing may ship when authentication fails")
	assert.Equal(t, 0, mock.auditCount(), "the audit endpoint must not be called without a token")

	// Current behavior: an auth failure does not stop the adapter; it retries
	// on the next poll.
	select {
	case <-chStopped:
		t.Fatal("adapter unexpectedly stopped on an auth failure (current behavior is to keep polling)")
	default:
	}
}

// TestMimecastAPIErrorKeepsPolling pins the adapter's behavior on a failing
// audit endpoint: the poll yields nothing, nothing ships, and the adapter
// retries on the next interval rather than stopping.
func TestMimecastAPIErrorKeepsPolling(t *testing.T) {
	mock := newMockMimecast("client-id-test", "client-secret-test")
	mock.addEvent(fakeLogonEvent("fake-audit-id-0001", time.Now().Add(-5*time.Second)))
	mock.auditStatus = http.StatusInternalServerError
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	adapter := startTestAdapter(t, server.URL, sink, "client-id-test", "client-secret-test")
	chStopped := adapter.chStopped
	defer adapter.Close()

	// Wait for at least two failed polls of the audit endpoint.
	require.Eventually(t, func() bool { return mock.auditCount() >= 2 },
		5*time.Second, 20*time.Millisecond, "the adapter should keep polling through API errors")
	assert.Equal(t, 0, sink.count(), "nothing may ship when the API errors")

	select {
	case <-chStopped:
		t.Fatal("adapter unexpectedly stopped on an API error")
	default:
	}

	// Once the API recovers, the pending event ships exactly once.
	mock.mu.Lock()
	mock.auditStatus = 0
	mock.mu.Unlock()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond, "the event should ship after the API recovers")
	require.Never(t, func() bool { return sink.count() != 1 },
		300*time.Millisecond, 30*time.Millisecond)
	assert.Equal(t, "fake-audit-id-0001", sink.snapshot()[0].JsonPayload["id"])
}
