package usp_threatlocker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock ThreatLocker Portal
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

// --- mock ThreatLocker Portal API -------------------------------------------

// mockThreatLocker is an in-memory stand-in for the ThreatLocker Portal API. It
// honours the "*GetByParameters" contract: a POST carrying the Authorization
// header and a JSON body with pageNumber/pageSize, returning a paginated slice
// of the dataset registered for that path.
type mockThreatLocker struct {
	mu       sync.Mutex
	token    string
	datasets map[string][]utils.Dict // request path -> records (newest-first)
	envelope bool                    // wrap results as {"data": [...]} instead of a bare array
	requests int
}

func newMockThreatLocker(token string) *mockThreatLocker {
	return &mockThreatLocker{token: token, datasets: map[string][]utils.Dict{}}
}

// setDataset registers the records a feed URL serves. The request path the
// adapter hits is "/" + feedURL (BaseURL has no trailing path in these tests).
func (m *mockThreatLocker) setDataset(feedURL string, records []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.datasets["/"+feedURL] = records
}

// appendRecord inserts a record at the top of a feed (newest-first), as the
// real API would when a new event occurs.
func (m *mockThreatLocker) appendRecord(feedURL string, record utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	path := "/" + feedURL
	m.datasets[path] = append([]utils.Dict{record}, m.datasets[path]...)
}

func (m *mockThreatLocker) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func (m *mockThreatLocker) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.requests++
		m.mu.Unlock()

		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// The Portal API authenticates with the token in the Authorization header.
		if r.Header.Get("Authorization") != m.token {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"invalid or missing API token"}`))
			return
		}

		body := decodeRequestBody(t, r)
		pageNumber := 1
		if v, ok := body["pageNumber"].(float64); ok && v >= 1 {
			pageNumber = int(v)
		}
		pageSize := defaultPageSize
		if v, ok := body["pageSize"].(float64); ok && v > 0 {
			pageSize = int(v)
		}

		m.mu.Lock()
		records := m.datasets[r.URL.Path]
		m.mu.Unlock()

		// Paginate; pageNumber is 1-based.
		var page []utils.Dict
		if start := (pageNumber - 1) * pageSize; start < len(records) {
			end := start + pageSize
			if end > len(records) {
				end = len(records)
			}
			page = records[start:end]
		}
		if page == nil {
			page = []utils.Dict{}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if m.envelope {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data":         page,
				"totalRecords": len(records),
				"pageNumber":   pageNumber,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(page)
	}
}

// --- realistic record fixtures ----------------------------------------------

// realisticApprovalRequest returns a record shaped like a real ThreatLocker
// Application Control approval request: the documented top-level fields plus
// the nested threatLockerActionDto with a certs array and assorted scalar
// types (strings, ints, bools, arrays).
func realisticApprovalRequest(id, dateTime string) utils.Dict {
	return utils.Dict{
		"approvalRequestId": id,
		"organizationId":    "5b7e3c9a-1d2f-4a6b-8c0e-9f1a2b3c4d5e",
		"computerId":        "a1b2c3d4-e5f6-4071-9a0b-1c2d3e4f5061",
		"hostname":          "WIN-DESKTOP-" + id,
		"username":          `ACME\jdoe`,
		"path":              `C:\Users\jdoe\Downloads\setup.exe`,
		"hash":              "9F86D081884C7D659A2FEAA0C55AD015A3BF4F1B2B0B822CD15D6C15B0F00A08",
		"statusId":          1,
		"dateTime":          dateTime,
		"threatLockerActionDto": utils.Dict{
			"fullPath":    `C:\Users\jdoe\Downloads\setup.exe`,
			"hash":        "TL-9F86D081",
			"sha256":      "9F86D081884C7D659A2FEAA0C55AD015A3BF4F1B2B0B822CD15D6C15B0F00A08",
			"processName": "explorer.exe",
			"osType":      1,
			"fileSize":    18874368,
			"certs": []interface{}{
				utils.Dict{
					"sha":       "3C5F8E2A9B1D4C6E7F8A0B1C2D3E4F5061728394",
					"subject":   "CN=Acme Software Inc., O=Acme, C=US",
					"validCert": true,
				},
			},
			"installedBy": []interface{}{"explorer.exe", "chrome.exe"},
		},
	}
}

// realisticActionLog returns a record shaped like a ThreatLocker Unified Audit
// (ActionLog) entry.
func realisticActionLog(id, dateTime string) utils.Dict {
	return utils.Dict{
		"actionId":     id,
		"dateTime":     dateTime,
		"action":       "Execute",
		"actionType":   2,
		"computerName": "WIN-SRV-01",
		"fullPath":     `C:\Windows\System32\cmd.exe`,
		"hash":         "ABCD1234EF567890",
		"username":     `ACME\svc-backup`,
		"policyName":   "Default Deny",
		"wasBlocked":   true,
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// --- tests ------------------------------------------------------------------

// TestMockApprovalRequestEndToEnd drives the adapter against the mock API and
// asserts the exact events shipped: event type, timestamp parsed from the
// record's dateTime, and the payload preserved verbatim (nested objects and
// arrays included).
func TestMockApprovalRequestEndToEnd(t *testing.T) {
	const token = "tl-api-token-xyz"
	const feedURL = "ApprovalRequest/ApprovalRequestGetByParameters"

	mock := newMockThreatLocker(token)
	want := []utils.Dict{
		realisticApprovalRequest("req-1001", "2026-05-21T13:45:30Z"),
		realisticApprovalRequest("req-1002", "2026-05-21T13:50:05Z"),
		realisticApprovalRequest("req-1003", "2026-05-21T14:02:11Z"),
	}
	mock.setDataset(feedURL, want)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ThreatLockerConfig{
		ClientOptions: testClientOptions(t),
		APIKey:        token,
		BaseURL:       server.URL,
		PollInterval:  40 * time.Millisecond,
	}
	adapter, _, err := newThreatLockerAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 approval requests to ship")

	// Re-polling must not re-ship: the count stays at 3.
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 30*time.Millisecond, "records were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		assert.Equal(t, "approval_request", msg.EventType)
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["approvalRequestId"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	for _, src := range want {
		id := src["approvalRequestId"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "record %s was not shipped", id)

		// The event timestamp is parsed from the record's dateTime field.
		ts, perr := time.Parse(time.RFC3339, src["dateTime"].(string))
		require.NoError(t, perr)
		assert.Equal(t, uint64(ts.UnixMilli()), msg.TimestampMs,
			"event time should come from the record's dateTime")

		// The payload is shipped verbatim -- nested objects and arrays included.
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original ThreatLocker record")
	}
}

// TestMockNewRequestShippedOnce verifies a request that appears mid-run is
// shipped exactly once, and already-shipped requests are never re-sent.
func TestMockNewRequestShippedOnce(t *testing.T) {
	const token = "tok"
	const feedURL = "ApprovalRequest/ApprovalRequestGetByParameters"

	mock := newMockThreatLocker(token)
	mock.setDataset(feedURL, []utils.Dict{
		realisticApprovalRequest("a", "2026-05-21T10:00:00Z"),
		realisticApprovalRequest("b", "2026-05-21T10:01:00Z"),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ThreatLockerConfig{
		ClientOptions: testClientOptions(t),
		APIKey:        token,
		BaseURL:       server.URL,
		PollInterval:  30 * time.Millisecond,
	}
	adapter, _, err := newThreatLockerAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)

	// A new pending request appears at the top of the feed.
	mock.appendRecord(feedURL, realisticApprovalRequest("c", "2026-05-21T10:05:00Z"))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "the new request should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 30*time.Millisecond)

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["approvalRequestId"].(string)]++
	}
	assert.Equal(t, map[string]int{"a": 1, "b": 1, "c": 1}, shippedPerID,
		"every request must ship exactly once")
}

// TestMockPaginationFullDataset verifies a dataset larger than one page is
// walked completely and every record is shipped exactly once.
func TestMockPaginationFullDataset(t *testing.T) {
	const token = "tok"
	const feedURL = "ApprovalRequest/ApprovalRequestGetByParameters"
	const total = 250

	mock := newMockThreatLocker(token)
	records := make([]utils.Dict, total)
	for i := 0; i < total; i++ {
		records[i] = realisticApprovalRequest(
			fmt.Sprintf("req-%03d", i),
			time.Date(2026, 5, 21, 0, 0, i, 0, time.UTC).Format(time.RFC3339))
	}
	mock.setDataset(feedURL, records)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conf := ThreatLockerConfig{
		ClientOptions: testClientOptions(t),
		APIKey:        token,
		BaseURL:       server.URL,
		PageSize:      100, // 250 records => 3 pages
		PollInterval:  50 * time.Millisecond,
	}
	adapter, _, err := newThreatLockerAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 25*time.Millisecond, "all paginated records should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 30*time.Millisecond)

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		ids[msg.JsonPayload["approvalRequestId"].(string)] = true
	}
	assert.Len(t, ids, total, "every distinct record should be shipped once")
}

// TestMockMultipleFeeds verifies that several feeds collect concurrently and
// each event is tagged with its feed's name.
func TestMockMultipleFeeds(t *testing.T) {
	const token = "tok"

	mock := newMockThreatLocker(token)
	mock.setDataset("ApprovalRequest/ApprovalRequestGetByParameters", []utils.Dict{
		realisticApprovalRequest("ar-1", "2026-05-21T09:00:00Z"),
		realisticApprovalRequest("ar-2", "2026-05-21T09:01:00Z"),
	})
	mock.setDataset("ActionLog/ActionLogGetByParametersV2", []utils.Dict{
		realisticActionLog("al-1", "2026-05-21T09:02:00Z"),
		realisticActionLog("al-2", "2026-05-21T09:03:00Z"),
		realisticActionLog("al-3", "2026-05-21T09:04:00Z"),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ThreatLockerConfig{
		ClientOptions: testClientOptions(t),
		APIKey:        token,
		BaseURL:       server.URL,
		PollInterval:  40 * time.Millisecond,
		Feeds: []ThreatLockerFeed{
			{
				Name:       "approval_request",
				URL:        "ApprovalRequest/ApprovalRequestGetByParameters",
				Parameters: utils.Dict{"statusId": 1},
				IDField:    "approvalRequestId",
			},
			{
				Name:    "unified_audit",
				URL:     "ActionLog/ActionLogGetByParametersV2",
				IDField: "actionId",
			},
		},
	}
	adapter, _, err := newThreatLockerAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 5 },
		5*time.Second, 20*time.Millisecond)
	require.Never(t, func() bool { return sink.count() != 5 },
		300*time.Millisecond, 30*time.Millisecond)

	byType := map[string]int{}
	for _, msg := range sink.snapshot() {
		byType[msg.EventType]++
	}
	assert.Equal(t, map[string]int{"approval_request": 2, "unified_audit": 3}, byType)
}

// TestMockObjectEnvelopeResponse verifies the adapter handles a mock that wraps
// results in an object envelope ({"data": [...]}).
func TestMockObjectEnvelopeResponse(t *testing.T) {
	const token = "tok"

	mock := newMockThreatLocker(token)
	mock.envelope = true
	mock.setDataset("ApprovalRequest/ApprovalRequestGetByParameters", []utils.Dict{
		realisticApprovalRequest("a", "2026-05-21T10:00:00Z"),
		realisticApprovalRequest("b", "2026-05-21T10:01:00Z"),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ThreatLockerConfig{
		ClientOptions: testClientOptions(t),
		APIKey:        token,
		BaseURL:       server.URL,
		PollInterval:  40 * time.Millisecond,
	}
	adapter, _, err := newThreatLockerAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "records inside the envelope should ship")
	require.Never(t, func() bool { return sink.count() != 2 },
		300*time.Millisecond, 30*time.Millisecond)
}

// TestMockRejectsBadToken verifies the adapter stops, and ships nothing, when
// the mock API rejects the supplied token.
func TestMockRejectsBadToken(t *testing.T) {
	mock := newMockThreatLocker("correct-token")
	mock.setDataset("ApprovalRequest/ApprovalRequestGetByParameters", []utils.Dict{
		realisticApprovalRequest("a", "2026-05-21T10:00:00Z"),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ThreatLockerConfig{
		ClientOptions: testClientOptions(t),
		APIKey:        "wrong-token",
		BaseURL:       server.URL,
		PollInterval:  50 * time.Millisecond,
	}
	adapter, chStopped, err := newThreatLockerAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	select {
	case <-chStopped:
		// Expected: a rejected token stops the adapter.
	case <-time.After(3 * time.Second):
		t.Fatal("adapter should stop when the API rejects the token")
	}
	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
	assert.GreaterOrEqual(t, mock.requestCount(), 1, "the adapter should have called the API")
}
