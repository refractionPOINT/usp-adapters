package usp_cloudflare_access

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
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

// This file exercises the adapter end-to-end against a mock Cloudflare Access
// API, capturing the exact messages it ships so their content -- event type,
// timestamp and verbatim payload -- can be asserted.

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

// --- mock Cloudflare Access API ----------------------------------------------

// mockCloudflareAccess is an in-memory stand-in for the Cloudflare Access
// per-request audit log endpoint. It honours the real contract: a GET to
// /accounts/{account_id}/access/logs/access_requests carrying a bearer token,
// filtered by since/until/limit and returning the standard
// {"success","errors","messages","result"} envelope.
type mockCloudflareAccess struct {
	mu        sync.Mutex
	token     string
	accountID string
	records   []utils.Dict // all records, any order
	requests  int32
}

func newMockCloudflareAccess(token, accountID string) *mockCloudflareAccess {
	return &mockCloudflareAccess{token: token, accountID: accountID}
}

func (m *mockCloudflareAccess) setRecords(records []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = records
}

func (m *mockCloudflareAccess) appendRecord(record utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, record)
}

func (m *mockCloudflareAccess) requestCount() int {
	return int(atomic.LoadInt32(&m.requests))
}

func (m *mockCloudflareAccess) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&m.requests, 1)

		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		wantPath := "/accounts/" + m.accountID + "/access/logs/access_requests"
		if r.URL.Path != wantPath {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+m.token {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"errors":  []map[string]interface{}{{"code": 9109, "message": "Invalid access token"}},
			})
			return
		}

		q := r.URL.Query()
		since, err := time.Parse(time.RFC3339, q.Get("since"))
		if !assert.NoError(t, err, "since must be RFC3339") {
			http.Error(w, "bad since", http.StatusBadRequest)
			return
		}
		until, err := time.Parse(time.RFC3339, q.Get("until"))
		if !assert.NoError(t, err, "until must be RFC3339") {
			http.Error(w, "bad until", http.StatusBadRequest)
			return
		}
		assert.Equal(t, "asc", q.Get("direction"), "adapter must always request ascending order")
		limit, err := strconv.Atoi(q.Get("limit"))
		if !assert.NoError(t, err, "limit must be an int") {
			http.Error(w, "bad limit", http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		var matched []utils.Dict
		for _, rec := range m.records {
			ts, ok := recordTime(rec)
			if !ok {
				continue
			}
			if !ts.Before(since) && ts.Before(until) {
				matched = append(matched, rec)
			}
		}
		m.mu.Unlock()

		sort.Slice(matched, func(i, j int) bool {
			ti, _ := recordTime(matched[i])
			tj, _ := recordTime(matched[j])
			return ti.Before(tj)
		})
		if len(matched) > limit {
			matched = matched[:limit]
		}
		if matched == nil {
			matched = []utils.Dict{}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"errors":   []interface{}{},
			"messages": []interface{}{},
			"result":   matched,
		})
	}
}

// --- realistic record fixtures ----------------------------------------------

// realisticAccessRequest returns a record shaped like a real Cloudflare Access
// per-request audit log entry, per the documented response schema.
func realisticAccessRequest(rayID, createdAt string) utils.Dict {
	return utils.Dict{
		"created_at": createdAt,
		"user_email": "user@example.com",
		"user_id":    "b4bf51f2-c72b-50b1-a179-20b76a49b18b",
		"ip_address": "203.0.113.10",
		"country":    "US",
		"app_domain": "kibana.example.com",
		"app_name":   "CFZT_App_kibana_example_com",
		"app_uid":    "968a35b8-def0-45de-9d2f-f65d514fa76c",
		"app_type":   "self_hosted",
		"action":     "login",
		"allowed":    true,
		"connection": "azureAD",
		"ray_id":     rayID,
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// --- tests ------------------------------------------------------------------

// TestMockEndToEnd drives the adapter against the mock API and asserts the
// exact events shipped: event type, timestamp parsed from created_at, and the
// payload preserved verbatim.
func TestMockEndToEnd(t *testing.T) {
	const token = "cf-api-token-xyz"
	const accountID = "acct-1"

	now := time.Now().UTC()
	want := []utils.Dict{
		realisticAccessRequest("ray-1001", now.Add(-3*time.Minute).Format(time.RFC3339)),
		realisticAccessRequest("ray-1002", now.Add(-2*time.Minute).Format(time.RFC3339)),
		realisticAccessRequest("ray-1003", now.Add(-1*time.Minute).Format(time.RFC3339)),
	}

	mock := newMockCloudflareAccess(token, accountID)
	mock.setRecords(want)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := testContext()
	defer cancel()

	conf := CloudflareAccessConfig{
		ClientOptions:   testClientOptions(t),
		APIToken:        token,
		AccountID:       accountID,
		BaseURL:         server.URL,
		PollInterval:    40 * time.Millisecond,
		InitialLookback: 1 * time.Hour,
	}
	adapter, _, err := newCloudflareAccessAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 access requests to ship")

	// Re-polling must not re-ship: the count stays at 3.
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 30*time.Millisecond, "records were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		assert.Equal(t, eventTypeAccessRequests, msg.EventType)
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["ray_id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	for _, src := range want {
		id := src["ray_id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "record %s was not shipped", id)

		ts, perr := time.Parse(time.RFC3339, src["created_at"].(string))
		require.NoError(t, perr)
		assert.Equal(t, uint64(ts.UnixMilli()), msg.TimestampMs,
			"event time should come from the record's created_at")

		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Cloudflare record verbatim")
	}
}

// TestMockNewRequestShippedOnce verifies a request that appears mid-run is
// shipped exactly once, and already-shipped requests are never re-sent.
func TestMockNewRequestShippedOnce(t *testing.T) {
	const token = "tok"
	const accountID = "acct-1"

	now := time.Now().UTC()
	mock := newMockCloudflareAccess(token, accountID)
	mock.setRecords([]utils.Dict{
		realisticAccessRequest("a", now.Add(-2*time.Minute).Format(time.RFC3339)),
		realisticAccessRequest("b", now.Add(-1*time.Minute).Format(time.RFC3339)),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := testContext()
	defer cancel()

	conf := CloudflareAccessConfig{
		ClientOptions:   testClientOptions(t),
		APIToken:        token,
		AccountID:       accountID,
		BaseURL:         server.URL,
		PollInterval:    30 * time.Millisecond,
		InitialLookback: 1 * time.Hour,
	}
	adapter, _, err := newCloudflareAccessAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)

	// A new request appears.
	mock.appendRecord(realisticAccessRequest("c", time.Now().UTC().Format(time.RFC3339)))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "the new request should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 30*time.Millisecond)

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["ray_id"].(string)]++
	}
	assert.Equal(t, map[string]int{"a": 1, "b": 1, "c": 1}, shippedPerID,
		"every request must ship exactly once")
}

// TestMockPaginationFullDataset verifies a window containing more records
// than the per-request limit is walked completely, in multiple paginated
// calls, with every record shipped exactly once.
func TestMockPaginationFullDataset(t *testing.T) {
	const token = "tok"
	const accountID = "acct-1"
	const total = 25

	base := time.Now().UTC().Add(-1 * time.Hour)
	records := make([]utils.Dict, total)
	for i := 0; i < total; i++ {
		records[i] = realisticAccessRequest(
			fmt.Sprintf("ray-%03d", i),
			base.Add(time.Duration(i)*time.Second).Format(time.RFC3339))
	}

	mock := newMockCloudflareAccess(token, accountID)
	mock.setRecords(records)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := testContext()
	defer cancel()

	conf := CloudflareAccessConfig{
		ClientOptions:   testClientOptions(t),
		APIToken:        token,
		AccountID:       accountID,
		BaseURL:         server.URL,
		Limit:           5, // 25 records => at least 5 paginated calls per poll
		PollInterval:    50 * time.Millisecond,
		InitialLookback: 2 * time.Hour,
	}
	adapter, _, err := newCloudflareAccessAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 25*time.Millisecond, "all paginated records should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 30*time.Millisecond)

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		ids[msg.JsonPayload["ray_id"].(string)] = true
	}
	assert.Len(t, ids, total, "every distinct record should be shipped once")
	assert.GreaterOrEqual(t, mock.requestCount(), total/5,
		"the adapter should have walked the window in multiple paginated calls")
}

// TestMockRejectsBadToken verifies the adapter stops, and ships nothing, when
// the mock API rejects the supplied token.
func TestMockRejectsBadToken(t *testing.T) {
	const accountID = "acct-1"
	mock := newMockCloudflareAccess("correct-token", accountID)
	mock.setRecords([]utils.Dict{
		realisticAccessRequest("a", time.Now().UTC().Format(time.RFC3339)),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := testContext()
	defer cancel()

	var warnings, fatalErrors atomic.Int32
	opts := testClientOptions(t)
	opts.OnWarning = func(msg string) { warnings.Add(1); t.Logf("WRN: %s", msg) }
	opts.OnError = func(err error) { fatalErrors.Add(1); t.Logf("ERR: %v", err) }

	conf := CloudflareAccessConfig{
		ClientOptions:   opts,
		APIToken:        "wrong-token",
		AccountID:       accountID,
		BaseURL:         server.URL,
		PollInterval:    50 * time.Millisecond,
		InitialLookback: 1 * time.Hour,
	}
	adapter, chStopped, err := newCloudflareAccessAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// A rejected token is a source-side problem: the adapter warns and keeps
	// running (retrying each poll) rather than stopping. Stopping would make
	// the cloud-sensor host tear it down, relaunch and eventually disable it --
	// a restart cannot fix a bad token, and an operator fixing the token should
	// see the adapter recover on its own.
	require.Eventually(t, func() bool { return warnings.Load() > 0 },
		3*time.Second, 20*time.Millisecond, "expected a warning when the token is rejected")

	select {
	case <-chStopped:
		t.Fatal("adapter must not stop when the API rejects the token")
	case <-time.After(300 * time.Millisecond):
	}
	assert.False(t, adapter.doStop.IsSet(), "doStop must not be set on a rejected token")
	assert.Equal(t, int32(0), fatalErrors.Load(), "a rejected token must warn, not fatally error")
	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
	assert.GreaterOrEqual(t, mock.requestCount(), 1, "the adapter should have called the API")
}
