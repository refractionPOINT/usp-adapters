package usp_slack

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock of the Slack Audit
// Logs API (https://api.slack.com/audit/v1/logs), capturing the exact messages
// it ships so their content -- event type, timestamp and verbatim payload --
// can be asserted without live Slack credentials.

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

// --- mock Slack Audit Logs API ----------------------------------------------

// mockSlack is an in-memory stand-in for the Slack Audit Logs API. It honours
// the contract the adapter relies on: a GET with a "Bearer <token>"
// Authorization header and "oldest"/"cursor" query parameters, answered with
// the {"entries": [...], "response_metadata": {"next_cursor": "..."}}
// envelope. The "oldest" filter is inclusive (date_create >= oldest), as
// documented by Slack, and cursors are opaque continuation tokens over a
// snapshot of the filtered result set -- supplying a cursor resumes that
// snapshot regardless of the other parameters, as with the real API.
type mockSlack struct {
	mu       sync.Mutex
	token    string
	pageSize int
	entries  []utils.Dict // ascending by date_create

	cursors   map[string][]utils.Dict // continuation token -> remaining entries
	cursorSeq int

	requests      int
	pagedRequests int
	authFailures  int
	oldestSeen    []int64

	// First-request capture, for asserting the request shape.
	gotMethod string
	gotPath   string
	gotAuth   string
}

func newMockSlack(token string, pageSize int) *mockSlack {
	return &mockSlack{
		token:    token,
		pageSize: pageSize,
		cursors:  map[string][]utils.Dict{},
	}
}

// addEntry appends an audit log entry to the dataset. Entries must be added in
// ascending date_create order (the order the adapter's incremental "oldest"
// tracking requires to not re-ship).
func (m *mockSlack) addEntry(e utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, e)
}

func (m *mockSlack) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func (m *mockSlack) pagedRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pagedRequests
}

func (m *mockSlack) authFailureCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authFailures
}

func (m *mockSlack) firstRequest() (method, path, auth string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gotMethod, m.gotPath, m.gotAuth
}

func (m *mockSlack) firstOldest() (int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.oldestSeen) == 0 {
		return 0, false
	}
	return m.oldestSeen[0], true
}

func (m *mockSlack) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()

		m.requests++
		if m.gotMethod == "" {
			m.gotMethod = r.Method
			m.gotPath = r.URL.Path
			m.gotAuth = r.Header.Get("Authorization")
		}

		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+m.token {
			m.authFailures++
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"ok":false,"error":"not_authed"}`))
			return
		}

		q := r.URL.Query()
		var page []utils.Dict
		next := ""

		if cur := q.Get("cursor"); cur != "" {
			// Continuation of a previous snapshot; the cursor is opaque and
			// self-contained, the other parameters do not re-filter it.
			m.pagedRequests++
			remaining, ok := m.cursors[cur]
			if !ok {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"ok":false,"error":"invalid_cursor"}`))
				return
			}
			page, remaining = splitPage(remaining, m.pageSize)
			if len(remaining) > 0 {
				m.cursors[cur] = remaining
				next = cur
			} else {
				delete(m.cursors, cur)
			}
		} else {
			// Fresh query: filter by oldest (inclusive) and snapshot the rest
			// behind a new cursor when it does not fit in one page.
			oldest, _ := strconv.ParseInt(q.Get("oldest"), 10, 64)
			m.oldestSeen = append(m.oldestSeen, oldest)

			var matched []utils.Dict
			for _, e := range m.entries {
				ts, _ := e.GetInt("date_create")
				if int64(ts) >= oldest {
					matched = append(matched, e)
				}
			}
			var remaining []utils.Dict
			page, remaining = splitPage(matched, m.pageSize)
			if len(remaining) > 0 {
				m.cursorSeq++
				tok := fmt.Sprintf("dXNlcjpVMEc5V0Z%d=", m.cursorSeq)
				m.cursors[tok] = remaining
				next = tok
			}
		}

		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"entries": page,
			"response_metadata": map[string]interface{}{
				"next_cursor": next,
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func splitPage(entries []utils.Dict, n int) ([]utils.Dict, []utils.Dict) {
	if n <= 0 || len(entries) <= n {
		return entries, nil
	}
	return entries[:n], entries[n:]
}

// --- fixtures ----------------------------------------------------------------

// auditEntry builds a realistic Slack audit log entry, shaped like the real
// API's payloads: a UUID id, an epoch date_create, an action, and nested
// actor/entity/context objects.
func auditEntry(id string, dateCreate int64, action string) utils.Dict {
	return utils.Dict{
		"id":          id,
		"date_create": dateCreate,
		"action":      action,
		"actor": utils.Dict{
			"type": "user",
			"user": utils.Dict{
				"id":    "W111AA111",
				"name":  "John Doe",
				"email": "jdoe@example.com",
				"team":  "T111AA111",
			},
		},
		"entity": utils.Dict{
			"type": "workspace",
			"workspace": utils.Dict{
				"id":     "T111AA111",
				"name":   "Example Workspace",
				"domain": "example",
			},
		},
		"context": utils.Dict{
			"location": utils.Dict{
				"type":   "enterprise",
				"id":     "E111AA111",
				"name":   "Example Enterprise",
				"domain": "example",
			},
			"ua":         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ExampleAgent/1.0",
			"ip_address": "198.51.100.1",
			"session_id": 1111111111,
		},
		"details": utils.Dict{
			"is_internal_integration": false,
			"app_owner_id":            "W111AA111",
			"md5":                     "11111111111111111111111111111111",
		},
	}
}

// fixtureID returns the all-1s UUID with the given 2-digit suffix.
func fixtureID(n int) string {
	return fmt.Sprintf("11111111-1111-1111-1111-1111111111%02d", n)
}

// testConf wires a SlackConfig at the mock server with a fast poll interval.
func testConf(t *testing.T, serverURL string, token string) SlackConfig {
	t.Helper()
	return SlackConfig{
		ClientOptions: testClientOptions(t),
		Token:         token,
		ApiURL:        serverURL + "/audit/v1/logs",
		PollInterval:  50 * time.Millisecond,
	}
}

// shippedByID indexes shipped messages by the payload "id" field, failing the
// test if an id ships more than once.
func shippedByID(t *testing.T, msgs []*protocol.DataMessage) map[string]*protocol.DataMessage {
	t.Helper()
	out := map[string]*protocol.DataMessage{}
	for _, m := range msgs {
		id, _ := m.JsonPayload["id"].(string)
		require.NotEmpty(t, id, "shipped payload has no id: %#v", m.JsonPayload)
		require.NotContains(t, out, id, "entry %q shipped more than once", id)
		out[id] = m
	}
	return out
}

// --- end-to-end tests ---------------------------------------------------------

// TestAllEntriesShipVerbatim verifies every available audit log entry is
// shipped with the verbatim Slack payload, a ship-time TimestampMs and the
// (empty) EventType the adapter actually sets, and that the request the
// adapter sends has the right shape: GET, Bearer token, and an "oldest"
// parameter anchored at adapter start time.
func TestAllEntriesShipVerbatim(t *testing.T) {
	const token = "xoxp-1111111111-fake-test-token"

	beforeStart := time.Now()
	base := beforeStart.Unix() + 5
	fixtures := []utils.Dict{
		auditEntry(fixtureID(1), base, "user_login"),
		auditEntry(fixtureID(2), base+1, "file_downloaded"),
		auditEntry(fixtureID(3), base+2, "user_logout"),
	}

	mock := newMockSlack(token, 100)
	for _, f := range fixtures {
		mock.addEntry(f)
	}
	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, chStopped, err := newSlackAdapter(t.Context(), testConf(t, server.URL, token), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == len(fixtures) },
		5*time.Second, 10*time.Millisecond, "expected all %d entries to ship", len(fixtures))
	afterShip := time.Now()

	select {
	case <-chStopped:
		t.Fatal("adapter stopped unexpectedly")
	default:
	}

	// Request shape.
	method, path, auth := mock.firstRequest()
	assert.Equal(t, http.MethodGet, method)
	assert.Equal(t, "/audit/v1/logs", path)
	assert.Equal(t, "Bearer "+token, auth)
	oldest, ok := mock.firstOldest()
	require.True(t, ok, "first request must carry an oldest parameter")
	assert.GreaterOrEqual(t, oldest, beforeStart.Unix(), "oldest anchors at adapter start")
	assert.LessOrEqual(t, oldest, afterShip.Unix())

	// Shipped content.
	byID := shippedByID(t, sink.snapshot())
	require.Len(t, byID, len(fixtures))
	for _, f := range fixtures {
		id := f["id"].(string)
		msg, ok := byID[id]
		require.True(t, ok, "entry %q never shipped", id)

		// Verbatim payload: exactly the JSON the API returned, unreshaped.
		want, err := json.Marshal(f)
		require.NoError(t, err)
		got, err := json.Marshal(msg.JsonPayload)
		require.NoError(t, err)
		assert.JSONEq(t, string(want), string(got), "payload for %q must ship verbatim", id)

		// The adapter stamps ship time (not date_create) and no event type.
		assert.Equal(t, "", msg.EventType, "adapter does not set an EventType")
		assert.GreaterOrEqual(t, msg.TimestampMs, uint64(beforeStart.UnixMilli()))
		assert.LessOrEqual(t, msg.TimestampMs, uint64(afterShip.UnixMilli()))
	}
}

// TestRepollDoesNotReship verifies that once entries have shipped, subsequent
// polls (which carry an advanced "oldest") do not ship them again.
func TestRepollDoesNotReship(t *testing.T) {
	const token = "xoxp-1111111111-fake-test-token"
	base := time.Now().Unix() + 5

	mock := newMockSlack(token, 100)
	mock.addEntry(auditEntry(fixtureID(1), base, "user_login"))
	mock.addEntry(auditEntry(fixtureID(2), base+1, "user_login"))
	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _, err := newSlackAdapter(t.Context(), testConf(t, server.URL, token), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 10*time.Millisecond)

	// Let at least three more polls happen, then confirm nothing re-shipped.
	reqs := mock.requestCount()
	require.Eventually(t, func() bool { return mock.requestCount() >= reqs+3 },
		5*time.Second, 10*time.Millisecond, "adapter should keep polling")
	assert.Equal(t, 2, sink.count(), "re-polls must not re-ship already-shipped entries")
	shippedByID(t, sink.snapshot()) // each id exactly once
}

// TestMidRunEntryShipsExactlyOnce verifies an entry that appears while the
// adapter is already polling is picked up and shipped exactly once.
func TestMidRunEntryShipsExactlyOnce(t *testing.T) {
	const token = "xoxp-1111111111-fake-test-token"
	base := time.Now().Unix() + 5

	mock := newMockSlack(token, 100)
	mock.addEntry(auditEntry(fixtureID(1), base, "user_login"))
	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _, err := newSlackAdapter(t.Context(), testConf(t, server.URL, token), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 10*time.Millisecond, "initial entry should ship")

	// A new audit event occurs mid-run, strictly newer than what shipped.
	late := auditEntry(fixtureID(2), base+10, "user_channel_join")
	mock.addEntry(late)

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 10*time.Millisecond, "mid-run entry should ship")

	// Several polls later it still shipped only once.
	reqs := mock.requestCount()
	require.Eventually(t, func() bool { return mock.requestCount() >= reqs+3 },
		5*time.Second, 10*time.Millisecond)
	assert.Equal(t, 2, sink.count(), "mid-run entry must ship exactly once")
	byID := shippedByID(t, sink.snapshot())
	require.Contains(t, byID, fixtureID(2))

	want, err := json.Marshal(late)
	require.NoError(t, err)
	got, err := json.Marshal(byID[fixtureID(2)].JsonPayload)
	require.NoError(t, err)
	assert.JSONEq(t, string(want), string(got))
}

// TestMultiPagePaginationFullyConsumed verifies the adapter exhausts the
// next_cursor chain: a dataset spanning several pages is fully shipped, each
// entry exactly once, and stays shipped-once across later polls.
func TestMultiPagePaginationFullyConsumed(t *testing.T) {
	const token = "xoxp-1111111111-fake-test-token"
	base := time.Now().Unix() + 5

	// 5 entries with a page size of 2 -> 3 pages (two of them cursor-driven).
	mock := newMockSlack(token, 2)
	fixtures := make([]utils.Dict, 0, 5)
	for i := 0; i < 5; i++ {
		f := auditEntry(fixtureID(i+1), base+int64(i), "user_login")
		fixtures = append(fixtures, f)
		mock.addEntry(f)
	}
	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _, err := newSlackAdapter(t.Context(), testConf(t, server.URL, token), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == len(fixtures) },
		5*time.Second, 10*time.Millisecond, "all pages should be consumed")
	assert.GreaterOrEqual(t, mock.pagedRequestCount(), 2,
		"the dataset spans 3 pages, so at least 2 cursor requests were required")

	byID := shippedByID(t, sink.snapshot())
	for _, f := range fixtures {
		assert.Contains(t, byID, f["id"].(string))
	}

	// Later polls must not re-ship any page.
	reqs := mock.requestCount()
	require.Eventually(t, func() bool { return mock.requestCount() >= reqs+3 },
		5*time.Second, 10*time.Millisecond)
	assert.Equal(t, len(fixtures), sink.count())
}

// TestBadTokenShipsNothing pins the adapter's actual behavior on auth failure:
// the Audit Logs API answers 401, the adapter reports the error and ships
// nothing -- but it does NOT stop; it keeps polling (a non-200 is reported via
// OnError and surfaced as a nil-error empty result to the poll loop).
func TestBadTokenShipsNothing(t *testing.T) {
	const token = "xoxp-1111111111-fake-test-token"
	base := time.Now().Unix() + 5

	mock := newMockSlack(token, 100)
	mock.addEntry(auditEntry(fixtureID(1), base, "user_login"))
	server := httptest.NewServer(mock.handler())
	defer server.Close()

	var apiErrors atomic.Int32
	conf := testConf(t, server.URL, "xoxp-2222222222-wrong-token")
	conf.ClientOptions.OnError = func(err error) {
		t.Logf("ERR: %v", err)
		apiErrors.Add(1)
	}

	sink := &captureSink{}
	adapter, chStopped, err := newSlackAdapter(t.Context(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// Let several polls fail authentication.
	require.Eventually(t, func() bool { return mock.authFailureCount() >= 3 },
		5*time.Second, 10*time.Millisecond, "adapter keeps polling despite 401s")

	assert.Equal(t, 0, sink.count(), "nothing may ship with a bad token")
	assert.GreaterOrEqual(t, apiErrors.Load(), int32(1), "401s must be reported via OnError")
	select {
	case <-chStopped:
		t.Fatal("adapter stopped on 401; current behavior is to keep polling")
	default:
	}
}

// TestUnreachableAPIStopsAdapter pins the adapter's behavior on transport
// errors: a failed HTTP request terminates the polling loop (chStopped closes)
// and nothing ships.
func TestUnreachableAPIStopsAdapter(t *testing.T) {
	// Grab a URL that refuses connections.
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := server.URL
	server.Close()

	sink := &captureSink{}
	adapter, chStopped, err := newSlackAdapter(t.Context(),
		testConf(t, deadURL, "xoxp-1111111111-fake-test-token"), sink)
	require.NoError(t, err)
	defer adapter.Close()

	select {
	case <-chStopped:
		// Expected: a transport error terminates the fetch loop.
	case <-time.After(5 * time.Second):
		t.Fatal("adapter should stop when the API is unreachable")
	}
	assert.Equal(t, 0, sink.count())
}
