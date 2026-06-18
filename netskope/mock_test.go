package usp_netskope

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock Netskope dataexport
// iterator API, capturing the exact messages it ships so their content -- event
// type, timestamp and verbatim payload -- can be asserted.

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

// --- mock Netskope dataexport iterator API ----------------------------------

// mockNetskope is an in-memory stand-in for the Netskope REST API v2 dataexport
// iterator. It honours the iterator contract: a GET carrying the
// Netskope-Api-Token header, an "index" cursor name and an "operation"
// (next/resend/epoch), returning a {"ok":1,"result":[...],"wait_time":N}
// envelope and advancing the server-side position for that index.
type mockNetskope struct {
	mu        sync.Mutex
	token     string
	datasets  map[string][]utils.Dict // "kind/type" -> records (oldest-first)
	cursors   map[string]int          // index name -> next read position
	lastStart map[string]int          // index name -> start of last returned page (for resend)
	pageSize  int
	waitTime  int // wait_time (seconds) to return; 0 keeps tests fast
	requests  int
}

func newMockNetskope(token string) *mockNetskope {
	return &mockNetskope{
		token:     token,
		datasets:  map[string][]utils.Dict{},
		cursors:   map[string]int{},
		lastStart: map[string]int{},
		pageSize:  100,
	}
}

// setDataset registers the records a (kind, type) stream serves, oldest-first.
func (m *mockNetskope) setDataset(kind, typ string, records []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.datasets[kind+"/"+typ] = records
}

// appendRecord adds a record to the end (newest) of a stream, as the real API
// would when a new event occurs after the consumer has caught up.
func (m *mockNetskope) appendRecord(kind, typ string, record utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := kind + "/" + typ
	m.datasets[key] = append(m.datasets[key], record)
}

func (m *mockNetskope) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func (m *mockNetskope) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.requests++
		m.mu.Unlock()

		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// v2 authenticates with the token in the Netskope-Api-Token header.
		if r.Header.Get("Netskope-Api-Token") != m.token {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"ok":0,"error":"invalid or unauthorized api token"}`))
			return
		}

		key := strings.TrimPrefix(r.URL.Path, "/api/v2/events/dataexport/")
		index := r.URL.Query().Get("index")
		op := r.URL.Query().Get("operation")
		if index == "" || op == "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"ok":0,"error":"index and operation are required"}`))
			return
		}

		m.mu.Lock()
		records := m.datasets[key]
		start := m.cursors[index]
		advance := true
		switch op {
		case operationNext:
			start = m.cursors[index]
		case "resend":
			start = m.lastStart[index]
			advance = false
		default: // an epoch-seconds seed
			if epoch, err := strconv.ParseInt(op, 10, 64); err == nil {
				start = seekByTimestamp(records, epoch)
			}
		}
		if start > len(records) {
			start = len(records)
		}
		end := start + m.pageSize
		if end > len(records) {
			end = len(records)
		}
		page := append([]utils.Dict(nil), records[start:end]...)
		if advance {
			m.lastStart[index] = start
			m.cursors[index] = end
		}
		wait := m.waitTime
		m.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":        1,
			"result":    page,
			"wait_time": wait,
		})
	}
}

// seekByTimestamp returns the index of the first record whose "timestamp" is >=
// epoch. Records are assumed oldest-first.
func seekByTimestamp(records []utils.Dict, epoch int64) int {
	for i, rec := range records {
		if ts, ok := rec.GetInt("timestamp"); ok && int64(ts) >= epoch {
			return i
		}
	}
	return len(records)
}

// --- realistic record fixtures ----------------------------------------------

// realisticDLPAlert returns a record shaped like a real Netskope v2 DLP alert:
// the documented top-level fields plus a nested dlp_rules array and assorted
// scalar types.
func realisticDLPAlert(id string, ts int64) utils.Dict {
	return utils.Dict{
		"_id":            id,
		"alert":          "yes",
		"alert_name":     "PCI Data Exfiltration",
		"alert_type":     "dlp",
		"type":           "nspolicy",
		"timestamp":      ts,
		"user":           "jdoe@acme.example",
		"userip":         "10.0.1.38",
		"app":            "Google Drive",
		"appcategory":    "Cloud Storage",
		"ccl":            "high",
		"activity":       "Upload",
		"action":         "block",
		"severity":       "high",
		"policy":         "Block PCI to Unsanctioned",
		"dlp_profile":    "PCI",
		"dlp_rule":       "INTL-PAN-Name",
		"dlp_rule_count": int64(1),
		"object":         "card-numbers.xlsx",
		"file_size":      int64(10256549),
		"src_location":   "London",
		"dst_location":   "Mountain View",
		"dlp_rules": []interface{}{
			utils.Dict{"dlp_rule_name": "INTL-PAN-Name", "dlp_rule_severity": "Critical"},
		},
	}
}

// realisticPageEvent returns a record shaped like a Netskope v2 page event.
func realisticPageEvent(id string, ts int64) utils.Dict {
	return utils.Dict{
		"_id":          id,
		"type":         "page",
		"timestamp":    ts,
		"user":         "jdoe@acme.example",
		"app":          "YouTube",
		"url":          "socialimpact.youtube.com",
		"site":         "Youtube",
		"srcip":        "18.170.239.5",
		"dstip":        "142.250.187.238",
		"src_location": "London",
		"dst_location": "Mountain View",
		"numbytes":     int64(309293),
	}
}

// realisticAuditEvent returns a record shaped like a Netskope v2 audit event.
func realisticAuditEvent(id string, ts int64) utils.Dict {
	return utils.Dict{
		"_id":             id,
		"type":            "admin_audit_logs",
		"timestamp":       ts,
		"user":            "admin@acme.example",
		"audit_log_event": "Edit Real-time Protection Policy",
		"supporting_data": utils.Dict{
			"data_type":   "policy",
			"data_values": []interface{}{"Block PCI to Unsanctioned"},
		},
		"ur_normalized":     "admin@acme.example",
		"organization_unit": "",
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// newAdapterAgainst builds an adapter pointed at the mock server with a capture
// sink and short intervals for fast tests.
func newAdapterAgainst(t *testing.T, ctx context.Context, server *httptest.Server, sink uspSink, conf NetskopeConfig) *NetskopeAdapter {
	t.Helper()
	conf.ClientOptions = testClientOptions(t)
	conf.Token = "tok"
	conf.BaseURL = server.URL + "/api/v2"
	if conf.PollInterval == 0 {
		conf.PollInterval = 40 * time.Millisecond
	}
	adapter, _, err := newNetskopeAdapter(ctx, conf, sink)
	require.NoError(t, err)
	return adapter
}

// --- tests ------------------------------------------------------------------

// TestMockAlertsEndToEnd drives the adapter against the mock API and asserts the
// exact events shipped: event type, timestamp parsed from the record's epoch
// "timestamp", and the payload preserved verbatim (nested objects and arrays
// included).
func TestMockAlertsEndToEnd(t *testing.T) {
	mock := newMockNetskope("tok")
	want := []utils.Dict{
		realisticDLPAlert("a1", 1748000000),
		realisticDLPAlert("a2", 1748000305),
		realisticDLPAlert("a3", 1748000931),
	}
	mock.setDataset("alerts", "dlp", want)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter := newAdapterAgainst(t, ctx, server, sink, NetskopeConfig{
		AlertTypes: []string{"dlp"}, EventTypes: []string{},
	})
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 DLP alerts to ship")

	// Re-polling (the cursor advances forward) must not re-ship: count stays 3.
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 30*time.Millisecond, "records were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		assert.Equal(t, "alert_dlp", msg.EventType)
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["_id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	for _, src := range want {
		id := src["_id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "record %s was not shipped", id)

		// The event timestamp is the record's epoch-seconds "timestamp" in ms.
		ts := src["timestamp"].(int64)
		assert.Equal(t, uint64(ts*1000), msg.TimestampMs,
			"event time should come from the record's timestamp (epoch seconds)")

		// The payload is shipped verbatim -- nested objects and arrays included.
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Netskope record")
	}
}

// TestMockNewRecordShippedOnce verifies a record that appears mid-run (after the
// consumer caught up) is shipped exactly once.
func TestMockNewRecordShippedOnce(t *testing.T) {
	mock := newMockNetskope("tok")
	mock.setDataset("alerts", "malware", []utils.Dict{
		realisticDLPAlert("m1", 1748000000),
		realisticDLPAlert("m2", 1748000060),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter := newAdapterAgainst(t, ctx, server, sink, NetskopeConfig{
		AlertTypes: []string{"malware"}, EventTypes: []string{},
	})
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)

	// A new alert appears once the consumer has caught up.
	mock.appendRecord("alerts", "malware", realisticDLPAlert("m3", 1748000300))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "the new record should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 30*time.Millisecond)

	shipped := map[string]int{}
	for _, msg := range sink.snapshot() {
		shipped[msg.JsonPayload["_id"].(string)]++
	}
	assert.Equal(t, map[string]int{"m1": 1, "m2": 1, "m3": 1}, shipped,
		"every record must ship exactly once")
}

// TestMockPaginationFullDataset verifies a dataset larger than one page is walked
// completely (the iterator advances page by page) and every record ships once.
func TestMockPaginationFullDataset(t *testing.T) {
	const total = 250
	mock := newMockNetskope("tok")
	mock.pageSize = 100 // 250 records => 3 pages
	records := make([]utils.Dict, total)
	for i := 0; i < total; i++ {
		records[i] = realisticPageEvent(fmt.Sprintf("p-%03d", i), 1748000000+int64(i))
	}
	mock.setDataset("events", "page", records)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	adapter := newAdapterAgainst(t, ctx, server, sink, NetskopeConfig{
		AlertTypes: []string{}, EventTypes: []string{"page"},
		PollInterval: 50 * time.Millisecond,
	})
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 25*time.Millisecond, "all paginated records should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 30*time.Millisecond)

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		assert.Equal(t, "event_page", msg.EventType)
		ids[msg.JsonPayload["_id"].(string)] = true
	}
	assert.Len(t, ids, total, "every distinct record should be shipped once")
}

// TestMockMultipleFeeds verifies several streams collect concurrently and each
// event is tagged with its feed's name.
func TestMockMultipleFeeds(t *testing.T) {
	mock := newMockNetskope("tok")
	mock.setDataset("alerts", "dlp", []utils.Dict{
		realisticDLPAlert("d1", 1748000000),
		realisticDLPAlert("d2", 1748000060),
	})
	mock.setDataset("events", "audit", []utils.Dict{
		realisticAuditEvent("au1", 1748000120),
		realisticAuditEvent("au2", 1748000180),
		realisticAuditEvent("au3", 1748000240),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter := newAdapterAgainst(t, ctx, server, sink, NetskopeConfig{
		AlertTypes: []string{"dlp"}, EventTypes: []string{"audit"},
	})
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 5 },
		5*time.Second, 20*time.Millisecond)
	require.Never(t, func() bool { return sink.count() != 5 },
		300*time.Millisecond, 30*time.Millisecond)

	byType := map[string]int{}
	for _, msg := range sink.snapshot() {
		byType[msg.EventType]++
	}
	assert.Equal(t, map[string]int{"alert_dlp": 2, "event_audit": 3}, byType)
}

// TestMockDuplicateIDShippedOnce verifies the deduper suppresses a record whose
// id repeats (covers the iterator resend / re-seed overlap).
func TestMockDuplicateIDShippedOnce(t *testing.T) {
	mock := newMockNetskope("tok")
	// Two distinct records plus a third that repeats the first record's _id.
	mock.setDataset("alerts", "dlp", []utils.Dict{
		realisticDLPAlert("dup", 1748000000),
		realisticDLPAlert("uniq", 1748000060),
		realisticDLPAlert("dup", 1748000120),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter := newAdapterAgainst(t, ctx, server, sink, NetskopeConfig{
		AlertTypes: []string{"dlp"}, EventTypes: []string{},
	})
	defer adapter.Close()

	// Only two distinct ids should ever ship.
	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)
	require.Never(t, func() bool { return sink.count() != 2 },
		400*time.Millisecond, 30*time.Millisecond, "the duplicate _id must not ship twice")

	ids := map[string]int{}
	for _, msg := range sink.snapshot() {
		ids[msg.JsonPayload["_id"].(string)]++
	}
	assert.Equal(t, map[string]int{"dup": 1, "uniq": 1}, ids)
}

// TestMockStartTimeSeed verifies start_time seeds the iterator: only records at
// or after the seed timestamp are consumed.
func TestMockStartTimeSeed(t *testing.T) {
	mock := newMockNetskope("tok")
	mock.setDataset("alerts", "dlp", []utils.Dict{
		realisticDLPAlert("old1", 1748000000),
		realisticDLPAlert("old2", 1748000100),
		realisticDLPAlert("new1", 1748000500),
		realisticDLPAlert("new2", 1748000600),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Seed at 1748000500 -> only new1/new2 should be consumed.
	adapter := newAdapterAgainst(t, ctx, server, sink, NetskopeConfig{
		AlertTypes: []string{"dlp"}, EventTypes: []string{},
		StartTime: "1748000500",
	})
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)
	require.Never(t, func() bool { return sink.count() != 2 },
		300*time.Millisecond, 30*time.Millisecond)

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		ids[msg.JsonPayload["_id"].(string)] = true
	}
	assert.Equal(t, map[string]bool{"new1": true, "new2": true}, ids)
}

// TestMockRejectsBadToken verifies the adapter warns, keeps running, and ships
// nothing when the API rejects the token. (Source-side failures are non-fatal:
// stopping would make the cloud-sensor host tear the adapter down and eventually
// disable it, and a restart cannot fix a bad token.)
func TestMockRejectsBadToken(t *testing.T) {
	mock := newMockNetskope("correct-token")
	mock.setDataset("alerts", "dlp", []utils.Dict{realisticDLPAlert("a1", 1748000000)})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var warnings, fatalErrors atomic.Int32
	opts := testClientOptions(t)
	opts.OnWarning = func(msg string) { warnings.Add(1); t.Logf("WRN: %s", msg) }
	opts.OnError = func(err error) { fatalErrors.Add(1); t.Logf("ERR: %v", err) }

	conf := NetskopeConfig{
		ClientOptions: opts,
		Token:         "wrong-token",
		BaseURL:       server.URL + "/api/v2",
		PollInterval:  50 * time.Millisecond,
		AlertTypes:    []string{"dlp"}, EventTypes: []string{},
	}
	adapter, chStopped, err := newNetskopeAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

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

// TestTransientErrorRetry verifies the adapter retries 5xx responses rather than
// terminating.
func TestTransientErrorRetry(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requestCount.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"ok":0,"error":"transient"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":1,"result":[],"wait_time":0}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := NetskopeConfig{
		ClientOptions:  testClientOptions(t),
		Token:          "tok",
		BaseURL:        server.URL + "/api/v2",
		PollInterval:   100 * time.Millisecond,
		RetryBaseDelay: 20 * time.Millisecond,
		MaxRetryDelay:  40 * time.Millisecond,
		AlertTypes:     []string{"dlp"}, EventTypes: []string{},
	}
	adapter, chStopped, err := newNetskopeAdapter(ctx, conf, &captureSink{})
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(500 * time.Millisecond)

	select {
	case <-chStopped:
		t.Fatal("adapter stopped on a transient error - it should have retried")
	default:
	}
	assert.GreaterOrEqual(t, requestCount.Load(), int32(3), "expected retries then success")
	assert.False(t, adapter.doStop.IsSet())
}
