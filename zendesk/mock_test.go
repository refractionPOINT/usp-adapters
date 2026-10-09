package usp_zendesk

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock Zendesk Audit Logs
// API (GET /api/v2/audit_logs), capturing the exact messages it ships so their
// content -- event type, timestamp and verbatim payload -- can be asserted.

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

// --- mock Zendesk Audit Logs API ----------------------------------------------

// basicAuthFor reproduces the Zendesk API-token authentication scheme: HTTP
// basic auth with the username "<email>/token" and the API token as password.
func basicAuthFor(email, token string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s/token:%s", email, token)))
}

// cursorState remembers where a paginated walk left off so the next page can
// resume from the same created_at window.
type cursorState struct {
	start  time.Time
	until  time.Time
	offset int
}

// mockZendesk is an in-memory stand-in for the Zendesk Audit Logs API as the
// adapter consumes it: GET /api/v2/audit_logs authenticated with
// "{email_address}/token:{api_token}" basic auth, filtered by a created_at
// window, paginated by page[size] with a {audit_logs, meta{has_more,
// after_cursor, before_cursor}, links{next, prev}} envelope.
//
// Fidelity notes vs the official docs
// (https://developer.zendesk.com/api-reference/ticketing/account-configuration/audit_logs/):
//   - The docs spell the range param "filter[created_at]", supplied twice
//     ("first with the start time and again with an end time"); the adapter
//     sends the Rails array spelling "filter[created_at][]", which the mock
//     mirrors. The docs do not say whether the bounds are inclusive; the mock
//     treats them as inclusive.
//   - The real API's links.next is a full URL carrying a page[after] cursor
//     (https://developer.zendesk.com/api-reference/introduction/pagination/).
//     This adapter instead feeds links.next back verbatim as the *first*
//     filter[created_at][] value of the next request (see makeOneRequest), so
//     the mock returns an opaque token and recognizes it there -- matching how
//     the adapter actually consumes pagination.
type mockZendesk struct {
	mu    sync.Mutex
	email string
	token string

	logs []utils.Dict // insertion order; created_at drives window matching

	cursors    map[string]cursorState
	nextCursor int

	requests       int // total requests received
	authFailures   int // requests rejected with 401
	cursorRequests int // requests that resumed from a links.next cursor

	lastMethod string
	lastPath   string
	lastAuth   string
	lastQuery  url.Values
}

func newMockZendesk(email, token string) *mockZendesk {
	return &mockZendesk{
		email:   email,
		token:   token,
		cursors: map[string]cursorState{},
	}
}

func (m *mockZendesk) addLog(record utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, record)
}

func (m *mockZendesk) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func (m *mockZendesk) authFailureCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authFailures
}

func (m *mockZendesk) cursorRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cursorRequests
}

func (m *mockZendesk) lastRequest() (method, path, auth string, query url.Values) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastMethod, m.lastPath, m.lastAuth, m.lastQuery
}

func (m *mockZendesk) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.requests++
		m.lastMethod = r.Method
		m.lastPath = r.URL.Path
		m.lastAuth = r.Header.Get("Authorization")
		m.lastQuery = r.URL.Query()
		m.mu.Unlock()

		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/api/v2/audit_logs" {
			http.Error(w, `{"error":"InvalidEndpoint"}`, http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != basicAuthFor(m.email, m.token) {
			m.mu.Lock()
			m.authFailures++
			m.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"Couldn't authenticate you"}`))
			return
		}

		q := r.URL.Query()
		pageSize := 100
		if v, err := strconv.Atoi(q.Get("page[size]")); err == nil && v > 0 {
			pageSize = v
		}

		created := q["filter[created_at][]"]
		var cs cursorState
		isCursor := false
		m.mu.Lock()
		if len(created) >= 1 {
			if state, ok := m.cursors[created[0]]; ok {
				cs, isCursor = state, true
				m.cursorRequests++
			}
		}
		m.mu.Unlock()

		if !isCursor {
			// A fresh window query carries exactly two created_at bounds.
			if !assert.Len(t, created, 2, "window query must carry two filter[created_at][] values") {
				http.Error(w, `{"error":"InvalidPaginationParameter"}`, http.StatusBadRequest)
				return
			}
			start, err1 := time.Parse(time.RFC3339, created[0])
			until, err2 := time.Parse(time.RFC3339, created[1])
			if !assert.NoError(t, err1, "start bound must be RFC3339") ||
				!assert.NoError(t, err2, "until bound must be RFC3339") {
				http.Error(w, `{"error":"InvalidValue"}`, http.StatusBadRequest)
				return
			}
			cs = cursorState{start: start, until: until}
		}

		// Select the logs inside the window (inclusive bounds) in insertion
		// order, then serve the requested page.
		m.mu.Lock()
		var matching []utils.Dict
		for _, rec := range m.logs {
			tsStr, _ := rec["created_at"].(string)
			ts, err := time.Parse(time.RFC3339, tsStr)
			if !assert.NoError(t, err, "fixture created_at must be RFC3339") {
				continue
			}
			if !ts.Before(cs.start) && !ts.After(cs.until) {
				matching = append(matching, rec)
			}
		}

		page := []utils.Dict{}
		if cs.offset < len(matching) {
			end := cs.offset + pageSize
			if end > len(matching) {
				end = len(matching)
			}
			page = matching[cs.offset:end]
		}
		hasMore := cs.offset+len(page) < len(matching)
		next := ""
		if hasMore {
			m.nextCursor++
			next = fmt.Sprintf("cursor_%06d", m.nextCursor)
			m.cursors[next] = cursorState{start: cs.start, until: cs.until, offset: cs.offset + len(page)}
		}
		m.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"audit_logs": page,
			"meta": map[string]interface{}{
				"has_more":      hasMore,
				"after_cursor":  next,
				"before_cursor": "",
			},
			"links": map[string]interface{}{
				"next": next,
				"prev": "",
			},
		})
	}
}

// --- realistic record fixtures ------------------------------------------------

const (
	mockEmail  = "jdoe@example.com"
	mockToken  = "zd-test-api-token-0000000000000000"
	mockDomain = "example.zendesk.com"
)

// auditLogFixture returns a record shaped like a real Zendesk audit log entry
// (the documented fields of GET /api/v2/audit_logs, per
// https://developer.zendesk.com/api-reference/ticketing/account-configuration/audit_logs/).
//
// NOTE: the docs type "id" (like actor_id/source_id) as an integer, i.e. a
// JSON number. The adapter dedupes on `item["id"].(string)`, so string ids are
// required for it to function; the fixtures use string ids to match what the
// adapter can actually consume (see the caveat in the test report / adapter
// review). actor_id/source_id stay numeric as documented.
func auditLogFixture(id string, createdAt time.Time, action string) utils.Dict {
	return utils.Dict{
		"id":                 id,
		"url":                fmt.Sprintf("https://%s/api/v2/audit_logs/%s.json", mockDomain, id),
		"action_label":       "Updated",
		"actor_id":           11111111,
		"actor_name":         mockEmail,
		"source_id":          22222222,
		"source_type":        "user",
		"source_label":       "John Doe",
		"action":             action,
		"change_description": "Role changed from End User to Administrator",
		"ip_address":         "192.0.2.10",
		"created_at":         createdAt.UTC().Format(time.RFC3339),
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// startMockAdapter wires the adapter to the mock server through the captured
// sink with a fast poll interval.
func startMockAdapter(t *testing.T, serverURL string, sink uspSink) (*ZendeskAdapter, chan struct{}) {
	t.Helper()
	conf := ZendeskConfig{
		ClientOptions: testClientOptions(t),
		ApiToken:      mockToken,
		ZendeskDomain: mockDomain,
		ZendeskEmail:  mockEmail,
		BaseURL:       serverURL,
		PollInterval:  50 * time.Millisecond,
	}
	a, chStopped, err := newZendeskAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	return a, chStopped
}

// fixtureTime returns a created_at safely inside the adapter's filter window.
// The adapter's window starts at the time the adapter was constructed
// (truncated to the second by RFC3339 formatting), so a fixture timestamped a
// couple of seconds in the future is guaranteed to fall inside [start, until]
// once the wall clock catches up -- without any flaky same-second boundary.
func fixtureTime() time.Time {
	return time.Now().Add(2 * time.Second)
}

// --- tests ----------------------------------------------------------------

// TestMockRequestShape verifies the on-the-wire request the adapter sends:
// GET /api/v2/audit_logs with "<email>/token:<api_token>" basic auth, two
// RFC3339 filter[created_at][] bounds (start <= until) and page[size]=100.
func TestMockRequestShape(t *testing.T) {
	mock := newMockZendesk(mockEmail, mockToken)
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use the public constructor: TestSinkMode gives a real usp client wired
	// to a test sink, proving the production wiring works end to end.
	conf := ZendeskConfig{
		ClientOptions: testClientOptions(t),
		ApiToken:      mockToken,
		ZendeskDomain: mockDomain,
		ZendeskEmail:  mockEmail,
		BaseURL:       server.URL,
		PollInterval:  50 * time.Millisecond,
	}
	adapter, chStopped, err := NewZendeskAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return mock.requestCount() >= 1 },
		5*time.Second, 20*time.Millisecond, "adapter never called the API")

	select {
	case <-chStopped:
		t.Fatal("adapter stopped unexpectedly")
	default:
	}

	method, path, auth, query := mock.lastRequest()
	assert.Equal(t, http.MethodGet, method)
	assert.Equal(t, "/api/v2/audit_logs", path)
	assert.Equal(t, basicAuthFor(mockEmail, mockToken), auth,
		"auth must be basic auth of '<email>/token:<api_token>'")
	assert.Equal(t, "100", query.Get("page[size]"))

	created := query["filter[created_at][]"]
	require.Len(t, created, 2, "expected a two-valued created_at window")
	start, err := time.Parse(time.RFC3339, created[0])
	require.NoError(t, err)
	until, err := time.Parse(time.RFC3339, created[1])
	require.NoError(t, err)
	assert.False(t, until.Before(start), "window start must be <= until")
	assert.Equal(t, 0, mock.authFailureCount())
}

// TestMockAuditLogsEndToEnd drives the adapter against the mock API and
// asserts the exact events shipped: every audit log entry ships exactly once
// with its payload preserved verbatim, an ingestion-time TimestampMs and an
// empty EventType (the adapter sets neither from the record -- pinned
// behavior). Re-polling the same window must not re-ship.
func TestMockAuditLogsEndToEnd(t *testing.T) {
	mock := newMockZendesk(mockEmail, mockToken)
	ts := fixtureTime()
	want := []utils.Dict{
		auditLogFixture("900000000001", ts, "update"),
		auditLogFixture("900000000002", ts.Add(100*time.Millisecond), "create"),
		auditLogFixture("900000000003", ts.Add(200*time.Millisecond), "login"),
	}
	for _, rec := range want {
		mock.addLog(rec)
	}

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	startMs := uint64(time.Now().UnixMilli())
	adapter, _ := startMockAdapter(t, server.URL, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		8*time.Second, 25*time.Millisecond, "expected all 3 audit logs to ship")

	// Re-polling must not re-ship: the count stays at 3 even though the mock
	// keeps returning the same window contents on every poll.
	require.Never(t, func() bool { return sink.count() != 3 },
		400*time.Millisecond, 25*time.Millisecond, "records were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		// Pinned behavior: the adapter does not set EventType, and stamps the
		// message with the ingestion time rather than the record's created_at.
		assert.Equal(t, "", msg.EventType)
		assert.GreaterOrEqual(t, msg.TimestampMs, startMs,
			"TimestampMs is the ingestion time, set after the test started")
		assert.LessOrEqual(t, msg.TimestampMs, uint64(time.Now().UnixMilli()))

		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3, "each record must ship exactly once")

	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "record %s was not shipped", id)
		// The payload is shipped verbatim.
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Zendesk record")
	}
}

// TestMockNewEntryShipsOnce verifies an audit log entry that appears mid-run
// ships exactly once, and already-shipped entries are never re-sent.
func TestMockNewEntryShipsOnce(t *testing.T) {
	mock := newMockZendesk(mockEmail, mockToken)
	mock.addLog(auditLogFixture("900000000010", fixtureTime(), "update"))
	mock.addLog(auditLogFixture("900000000011", fixtureTime(), "create"))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		8*time.Second, 25*time.Millisecond)

	// A new audit log entry appears while the adapter is running.
	mock.addLog(auditLogFixture("900000000012", fixtureTime(), "destroy"))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		8*time.Second, 25*time.Millisecond, "the new entry should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		400*time.Millisecond, 25*time.Millisecond)

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["id"].(string)]++
	}
	assert.Equal(t, map[string]int{
		"900000000010": 1,
		"900000000011": 1,
		"900000000012": 1,
	}, shippedPerID, "every entry must ship exactly once")
}

// TestMockPaginationFullDataset verifies a window larger than one page
// (page[size]=100) is walked to the end via meta.has_more/links.next and every
// record ships exactly once.
func TestMockPaginationFullDataset(t *testing.T) {
	const total = 250 // 3 pages at the adapter's hardcoded page[size]=100

	mock := newMockZendesk(mockEmail, mockToken)
	ts := fixtureTime()
	for i := 0; i < total; i++ {
		mock.addLog(auditLogFixture(fmt.Sprintf("9100000%05d", i), ts, "update"))
	}

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 25*time.Millisecond, "all paginated records should ship")
	require.Never(t, func() bool { return sink.count() != total },
		400*time.Millisecond, 25*time.Millisecond)

	assert.GreaterOrEqual(t, mock.cursorRequestCount(), 2,
		"a 250-record window must be fetched through at least 2 follow-up pages")

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		ids[msg.JsonPayload["id"].(string)] = true
	}
	assert.Len(t, ids, total, "every distinct record should be shipped once")
}

// TestMockBadCredentials pins the adapter's behavior on auth failure: nothing
// ships, and the adapter does NOT stop -- a non-200 response only logs an
// error and the polling loop keeps retrying on the next tick.
func TestMockBadCredentials(t *testing.T) {
	mock := newMockZendesk(mockEmail, mockToken)
	mock.addLog(auditLogFixture("900000000099", fixtureTime(), "update"))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	conf := ZendeskConfig{
		ClientOptions: testClientOptions(t),
		ApiToken:      "wrong-token",
		ZendeskDomain: mockDomain,
		ZendeskEmail:  mockEmail,
		BaseURL:       server.URL,
		PollInterval:  50 * time.Millisecond,
	}
	adapter, chStopped, err := newZendeskAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// The mock rejects the credentials on every poll; wait for several polls
	// to prove the adapter keeps retrying rather than shipping or crashing.
	require.Eventually(t, func() bool { return mock.authFailureCount() >= 3 },
		5*time.Second, 20*time.Millisecond, "adapter should keep polling on 401")

	select {
	case <-chStopped:
		t.Fatal("adapter unexpectedly stopped on auth failure (pinned behavior: it keeps polling)")
	default:
	}
	assert.Equal(t, 0, sink.count(), "nothing must ship when authentication fails")
}
