package usp_proofpoint_isolation

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

// This file exercises the adapter end-to-end against a mock Proofpoint
// Isolation Reporting API, capturing the exact messages it ships so their
// content -- event type, timestamp and verbatim payload -- can be asserted.

// recordDateLayout is the format of the "date" field in real usage records,
// e.g. "2025-01-01T19:44:35.000+0000".
const recordDateLayout = "2006-01-02T15:04:05.000-0700"

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

// --- mock Reporting API -----------------------------------------------------

// mockJob is one report job: the records matching its from/to range, frozen
// when the job was created, and how many more polls it stays incomplete.
type mockJob struct {
	records     []utils.Dict
	pendingLeft int
}

// mockIsolation is an in-memory stand-in for one product's Reporting API. It
// honours the usage-data contract as Proofpoint's own client (iso2web) drives
// it:
//
//   - GET /api/v2/reporting/usage-data with Authorization: Bearer <key> and
//     from, to, pageSize query parameters.
//   - A request without jobId starts a job and returns {jobId, status}. While
//     the job is not COMPLETED the caller re-polls with the jobId.
//   - A COMPLETED response carries one page of data plus a pageToken when
//     more pages follow; the caller echoes jobId and pageToken to get the
//     next page. The last page has no pageToken.
type mockIsolation struct {
	mu      sync.Mutex
	key     string
	records []utils.Dict

	// pendingPolls is how many polls a new job answers with status RUNNING
	// before it completes.
	pendingPolls int
	// failStatus, when set, is returned as the status of every job.
	failStatus string
	// failNext answers the next N requests with this HTTP status.
	failNext       int
	failNextStatus int

	jobs     map[string]*mockJob
	nextJob  int
	requests int
	// echoed counts follow-up requests that carried a jobId / pageToken.
	echoedJobID     int
	echoedPageToken int
}

func newMockIsolation(key string) *mockIsolation {
	return &mockIsolation{key: key, jobs: map[string]*mockJob{}}
}

func (m *mockIsolation) addRecords(records ...utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, records...)
}

func (m *mockIsolation) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (m *mockIsolation) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.requests++

		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != usageDataPath {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		// Mirrors the real API: no key is a 400, a wrong key a 403 whose
		// message echoes the (upper-cased) key back.
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") || strings.TrimPrefix(auth, "Bearer ") == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"message": "API key is not provided", "url": r.URL.String()})
			return
		}
		if got := strings.TrimPrefix(auth, "Bearer "); got != m.key {
			writeJSON(w, http.StatusForbidden, map[string]string{
				"message": "API key " + strings.ToUpper(got) + " is not valid", "url": r.URL.String()})
			return
		}
		if m.failNext > 0 {
			m.failNext--
			writeJSON(w, m.failNextStatus, map[string]string{"error": "injected failure"})
			return
		}

		q := r.URL.Query()
		from, err1 := time.Parse(queryTimestampLayout, q.Get("from"))
		to, err2 := time.Parse(queryTimestampLayout, q.Get("to"))
		pageSize, err3 := strconv.Atoi(q.Get("pageSize"))
		if err1 != nil || err2 != nil || err3 != nil || pageSize < 1 || pageSize > maxPageSize {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad from/to/pageSize"})
			return
		}

		jobID := q.Get("jobId")
		if jobID == "" {
			// Start a new job over [from, to].
			m.nextJob++
			jobID = fmt.Sprintf("job-%d", m.nextJob)
			job := &mockJob{pendingLeft: m.pendingPolls}
			for _, rec := range m.records {
				d, err := time.Parse(recordDateLayout, rec["date"].(string))
				require.NoError(t, err)
				if !d.Before(from) && !d.After(to) {
					job.records = append(job.records, rec)
				}
			}
			m.jobs[jobID] = job
		} else {
			m.echoedJobID++
		}
		job, ok := m.jobs[jobID]
		if !ok {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unknown jobId"})
			return
		}

		if m.failStatus != "" {
			writeJSON(w, http.StatusOK, map[string]interface{}{"jobId": jobID, "status": m.failStatus})
			return
		}
		if job.pendingLeft > 0 {
			job.pendingLeft--
			writeJSON(w, http.StatusOK, map[string]interface{}{"jobId": jobID, "status": "RUNNING"})
			return
		}

		offset := 0
		if tok := q.Get("pageToken"); tok != "" {
			m.echoedPageToken++
			offset, _ = strconv.Atoi(tok)
		}
		end := offset + pageSize
		if end > len(job.records) {
			end = len(job.records)
		}
		page := []utils.Dict{}
		if offset < len(job.records) {
			page = job.records[offset:end]
		}
		resp := map[string]interface{}{
			"jobId":  jobID,
			"status": "COMPLETED",
			"total":  len(job.records),
		}
		// Like the real API, an empty result has no "data" key at all.
		if len(page) > 0 {
			resp["data"] = page
		}
		if end < len(job.records) {
			resp["pageToken"] = strconv.Itoa(end)
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

// --- realistic record fixtures ----------------------------------------------

// realisticUsageRecord returns a record shaped like a real usage-data entry:
// the documented fields (user, URL, date, region/zone, classification,
// disposition, categories array).
func realisticUsageRecord(user, url string, when time.Time) utils.Dict {
	return utils.Dict{
		"userId":         "uid-" + user,
		"userName":       user + "@example.com",
		"url":            url,
		"date":           when.UTC().Format(recordDateLayout),
		"region":         "US",
		"zone":           "CENTRAL",
		"classification": "SUSPICIOUS",
		"disposition":    "ISOLATED",
		"categories":     []interface{}{"Newly Registered Domains", "Uncategorized"},
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// fastConfig returns a config pointed at the given mock servers with
// millisecond timings so tests run quickly.
func fastConfig(t *testing.T) ProofpointIsolationConfig {
	return ProofpointIsolationConfig{
		ClientOptions:   testClientOptions(t),
		pollInterval:    40 * time.Millisecond,
		jobPollInterval: 5 * time.Millisecond,
		retryBaseDelay:  5 * time.Millisecond,
		maxRetryDelay:   20 * time.Millisecond,
	}
}

// --- tests ------------------------------------------------------------------

// TestMockBothProductsEndToEnd drives the adapter against both mock products
// and asserts the exact events shipped: event type per product, timestamp
// parsed from the record's date, and the payload preserved verbatim.
func TestMockBothProductsEndToEnd(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)

	web := newMockIsolation("web-key")
	webRecords := []utils.Dict{
		realisticUsageRecord("alice", "https://phish.example.net/login", now.Add(-30*time.Minute)),
		realisticUsageRecord("bob", "https://docs.example.org/", now.Add(-20*time.Minute)),
		realisticUsageRecord("carol", "https://files.example.io/x.zip", now.Add(-10*time.Minute)),
	}
	web.addRecords(webRecords...)
	webSrv := httptest.NewServer(web.handler(t))
	defer webSrv.Close()

	urlIso := newMockIsolation("url-key")
	urlRecords := []utils.Dict{
		realisticUsageRecord("dave", "https://click.example.com/abc", now.Add(-15*time.Minute)),
		realisticUsageRecord("erin", "https://click.example.com/def", now.Add(-5*time.Minute)),
	}
	urlIso.addRecords(urlRecords...)
	urlSrv := httptest.NewServer(urlIso.handler(t))
	defer urlSrv.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := fastConfig(t)
	conf.WebAPIKey = "web-key"
	conf.WebBaseURL = webSrv.URL
	conf.URLAPIKey = "url-key"
	conf.URLBaseURL = urlSrv.URL
	adapter, _, err := newProofpointIsolationAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 5 },
		5*time.Second, 10*time.Millisecond, "expected all 5 records to ship")
	// Overlapping re-queries must not re-ship.
	require.Never(t, func() bool { return sink.count() != 5 },
		300*time.Millisecond, 30*time.Millisecond, "records were re-shipped on a later poll")
	assert.Greater(t, web.requestCount(), 1, "the web product should have been polled repeatedly")

	byURL := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		byURL[msg.JsonPayload["url"].(string)] = msg
	}
	check := func(src utils.Dict, wantType string) {
		msg := byURL[src["url"].(string)]
		require.NotNil(t, msg, "record %v was not shipped", src["url"])
		assert.Equal(t, wantType, msg.EventType)

		ts, perr := time.Parse(recordDateLayout, src["date"].(string))
		require.NoError(t, perr)
		assert.Equal(t, uint64(ts.UnixMilli()), msg.TimestampMs, "event time should come from the record's date")

		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original usage record")
	}
	for _, r := range webRecords {
		check(r, eventTypeWeb)
	}
	for _, r := range urlRecords {
		check(r, eventTypeURL)
	}
}

// TestMockNewRecordShippedOnce verifies a record that appears mid-run is
// shipped exactly once, and already-shipped records are never re-sent.
func TestMockNewRecordShippedOnce(t *testing.T) {
	now := time.Now().UTC()
	mock := newMockIsolation("k")
	mock.addRecords(
		realisticUsageRecord("a", "https://a.example/", now.Add(-5*time.Minute)),
		realisticUsageRecord("b", "https://b.example/", now.Add(-4*time.Minute)),
	)
	srv := httptest.NewServer(mock.handler(t))
	defer srv.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := fastConfig(t)
	conf.WebAPIKey = "k"
	conf.WebBaseURL = srv.URL
	adapter, _, err := newProofpointIsolationAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 10*time.Millisecond)

	// A new request is isolated now.
	mock.addRecords(realisticUsageRecord("c", "https://c.example/", time.Now().UTC()))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond, "the new record should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 30*time.Millisecond)

	perURL := map[string]int{}
	for _, msg := range sink.snapshot() {
		perURL[msg.JsonPayload["url"].(string)]++
	}
	assert.Equal(t, map[string]int{"https://a.example/": 1, "https://b.example/": 1, "https://c.example/": 1}, perURL,
		"every record must ship exactly once")
}

// TestMockLateIndexedRecordShipped verifies a record whose date falls inside
// an already-queried window, but which the API only indexes later, is still
// picked up thanks to the overlap between consecutive queries.
func TestMockLateIndexedRecordShipped(t *testing.T) {
	mock := newMockIsolation("k")
	srv := httptest.NewServer(mock.handler(t))
	defer srv.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := fastConfig(t)
	conf.WebAPIKey = "k"
	conf.WebBaseURL = srv.URL
	adapter, _, err := newProofpointIsolationAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// Let a few empty polls go by so the window has moved past this date.
	late := time.Now().UTC().Add(-1 * time.Minute)
	require.Eventually(t, func() bool { return mock.requestCount() >= 3 },
		5*time.Second, 10*time.Millisecond)
	mock.addRecords(realisticUsageRecord("late", "https://late.example/", late))

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 10*time.Millisecond, "a late-indexed record inside the overlap should ship")
	require.Never(t, func() bool { return sink.count() > 1 },
		300*time.Millisecond, 30*time.Millisecond)
}

// TestMockJobPollingAndPagination verifies the job flow: the adapter re-polls
// an incomplete job with its jobId, then walks every page by echoing the
// pageToken, shipping each record once.
func TestMockJobPollingAndPagination(t *testing.T) {
	const total = 25
	now := time.Now().UTC()

	mock := newMockIsolation("k")
	mock.pendingPolls = 2
	for i := 0; i < total; i++ {
		mock.addRecords(realisticUsageRecord(
			fmt.Sprintf("user%02d", i), fmt.Sprintf("https://site%02d.example/", i),
			now.Add(-time.Duration(total-i)*time.Second)))
	}
	srv := httptest.NewServer(mock.handler(t))
	defer srv.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := fastConfig(t)
	conf.WebAPIKey = "k"
	conf.WebBaseURL = srv.URL
	conf.PageSize = 10 // 25 records => 3 pages
	adapter, _, err := newProofpointIsolationAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		5*time.Second, 10*time.Millisecond, "all paginated records should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 30*time.Millisecond)

	urls := map[string]bool{}
	for _, msg := range sink.snapshot() {
		urls[msg.JsonPayload["url"].(string)] = true
	}
	assert.Len(t, urls, total, "every distinct record should be shipped once")

	mock.mu.Lock()
	defer mock.mu.Unlock()
	assert.GreaterOrEqual(t, mock.echoedJobID, 4, "job polls and page fetches must echo the jobId")
	assert.GreaterOrEqual(t, mock.echoedPageToken, 2, "pages 2 and 3 must be fetched with the pageToken")
}

// TestMockInitialLookback verifies records older than the initial lookback
// are not collected.
func TestMockInitialLookback(t *testing.T) {
	now := time.Now().UTC()
	mock := newMockIsolation("k")
	mock.addRecords(
		realisticUsageRecord("old", "https://old.example/", now.Add(-3*time.Hour)),
		realisticUsageRecord("new", "https://new.example/", now.Add(-30*time.Minute)),
	)
	srv := httptest.NewServer(mock.handler(t))
	defer srv.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := fastConfig(t)
	conf.WebAPIKey = "k"
	conf.WebBaseURL = srv.URL
	conf.InitialLookbackMinutes = 60
	adapter, _, err := newProofpointIsolationAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return sink.count() != 1 },
		300*time.Millisecond, 30*time.Millisecond)
	assert.Equal(t, "https://new.example/", sink.snapshot()[0].JsonPayload["url"])
}

// TestMockTransientErrorRetried verifies a 5xx is retried within the same
// poll and the records still ship once.
func TestMockTransientErrorRetried(t *testing.T) {
	mock := newMockIsolation("k")
	mock.failNext = 2
	mock.failNextStatus = http.StatusServiceUnavailable
	mock.addRecords(realisticUsageRecord("a", "https://a.example/", time.Now().UTC().Add(-time.Minute)))
	srv := httptest.NewServer(mock.handler(t))
	defer srv.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var fatalErrors atomic.Int32
	conf := fastConfig(t)
	conf.ClientOptions.OnError = func(err error) { fatalErrors.Add(1); t.Logf("ERR: %v", err) }
	conf.WebAPIKey = "k"
	conf.WebBaseURL = srv.URL
	adapter, _, err := newProofpointIsolationAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return sink.count() != 1 },
		200*time.Millisecond, 20*time.Millisecond)
	assert.Equal(t, int32(0), fatalErrors.Load())
}

// TestMockFailedJobDoesNotStop verifies a job that ends in a failure status
// is skipped with a warning, ships nothing, and the adapter keeps running.
func TestMockFailedJobDoesNotStop(t *testing.T) {
	mock := newMockIsolation("k")
	mock.failStatus = "FAILED"
	mock.addRecords(realisticUsageRecord("a", "https://a.example/", time.Now().UTC().Add(-time.Minute)))
	srv := httptest.NewServer(mock.handler(t))
	defer srv.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var warnings atomic.Int32
	conf := fastConfig(t)
	conf.ClientOptions.OnWarning = func(msg string) { warnings.Add(1); t.Logf("WRN: %s", msg) }
	conf.WebAPIKey = "k"
	conf.WebBaseURL = srv.URL
	adapter, chStopped, err := newProofpointIsolationAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return warnings.Load() >= 2 },
		5*time.Second, 10*time.Millisecond, "each failed job should warn, and polling should continue")
	select {
	case <-chStopped:
		t.Fatal("adapter must not stop on a failed report job")
	default:
	}
	assert.Equal(t, 0, sink.count())

	// Once the API recovers, the same window is retried and the record ships.
	mock.mu.Lock()
	mock.failStatus = ""
	mock.mu.Unlock()
	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 10*time.Millisecond, "the record should ship after the API recovers")
}

// TestMockRejectsBadKey verifies a rejected key warns, ships nothing and does
// not stop the adapter -- and does not affect the other product's feed.
func TestMockRejectsBadKey(t *testing.T) {
	now := time.Now().UTC()
	web := newMockIsolation("correct-web-key")
	web.addRecords(realisticUsageRecord("a", "https://a.example/", now.Add(-time.Minute)))
	webSrv := httptest.NewServer(web.handler(t))
	defer webSrv.Close()

	urlIso := newMockIsolation("url-key")
	urlIso.addRecords(realisticUsageRecord("b", "https://b.example/", now.Add(-time.Minute)))
	urlSrv := httptest.NewServer(urlIso.handler(t))
	defer urlSrv.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var warnings, fatalErrors atomic.Int32
	conf := fastConfig(t)
	var leaked atomic.Bool
	conf.ClientOptions.OnWarning = func(msg string) {
		warnings.Add(1)
		if strings.Contains(strings.ToUpper(msg), "WRONG-KEY") {
			leaked.Store(true)
		}
		t.Logf("WRN: %s", msg)
	}
	conf.ClientOptions.OnError = func(err error) { fatalErrors.Add(1); t.Logf("ERR: %v", err) }
	conf.WebAPIKey = "wrong-key"
	conf.WebBaseURL = webSrv.URL
	conf.URLAPIKey = "url-key"
	conf.URLBaseURL = urlSrv.URL
	adapter, chStopped, err := newProofpointIsolationAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return warnings.Load() > 0 },
		3*time.Second, 10*time.Millisecond, "expected a warning when the key is rejected")
	require.Eventually(t, func() bool { return sink.count() == 1 },
		3*time.Second, 10*time.Millisecond, "the healthy product should still ship")

	select {
	case <-chStopped:
		t.Fatal("adapter must not stop when the API rejects the key")
	case <-time.After(200 * time.Millisecond):
	}
	assert.False(t, adapter.doStop.IsSet())
	assert.Equal(t, int32(0), fatalErrors.Load(), "a rejected key must warn, not fatally error")
	assert.False(t, leaked.Load(), "the API echoes the rejected key in its 403 body; it must not reach the logs")
	msgs := sink.snapshot()
	require.Len(t, msgs, 1)
	assert.Equal(t, eventTypeURL, msgs[0].EventType, "nothing should ship from the product whose key is rejected")
	assert.GreaterOrEqual(t, web.requestCount(), 1)
}
