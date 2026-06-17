package usp_cato

// This file exercises the adapter end-to-end against a mock of the Cato
// Networks GraphQL eventsFeed API (https://api.catonetworks.com/api/v1/graphql2),
// capturing the exact messages it ships so their content -- verbatim payload,
// timestamp, marker handling -- can be asserted without live credentials.
//
// The mock reproduces the API the way the adapter consumes it:
//   - POST /api/v1/graphql2 with the x-api-key header and a {"query": "..."}
//     JSON body (the adapter inlines accountIDs/marker into the query string;
//     it does not use GraphQL variables).
//   - A gzip-compressed response body. The adapter sets Accept-Encoding
//     itself and gunzips the body unconditionally, exactly like the real API
//     behaves for it.
//   - The {"data":{"eventsFeed":{marker,fetchedCount,accounts:[{id,records:
//     [{time,fieldsMap}]}]}}} envelope, with an opaque marker cursor that
//     resumes the feed where the previous call left off.
//   - GraphQL errors as HTTP 200 + {"errors":[{"message":...}]} (bad API key
//     -- "permission denied" -- bad account, rate limiting), which is how
//     Cato reports failures.

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAPIKey    = "test-cato-api-key-1111111111111111"
	testAccountID = 11111111

	// mockMarkerPrefix prefixes the opaque cursor the mock hands out. The
	// suffix encodes the feed offset so the mock can resume.
	mockMarkerPrefix = "mock-marker-"
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

// --- mock Cato GraphQL API ----------------------------------------------------

// catoRecord is one event of the mock's in-memory feed, mirroring an
// eventsFeed EventRecord: an ISO-8601 time plus the fieldsMap of event fields.
type catoRecord struct {
	Time      string
	FieldsMap map[string]interface{}
}

// mockCato is an in-memory stand-in for the Cato eventsFeed API.
type mockCato struct {
	t         *testing.T
	apiKey    string
	accountID string

	mu           sync.Mutex
	events       []catoRecord
	batchSize    int    // max records per response; 0 means everything pending
	rateLimited  int    // number of upcoming requests answered with a rate-limit error
	forcedError  string // when set, every request gets this GraphQL error
	markersSeen  []string
	requests     int
	authFailures int
}

func newMockCato(t *testing.T) *mockCato {
	return &mockCato{
		t:         t,
		apiKey:    testAPIKey,
		accountID: strconv.Itoa(testAccountID),
	}
}

func (m *mockCato) addEvent(rec catoRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, rec)
}

func (m *mockCato) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func (m *mockCato) authFailureCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authFailures
}

func (m *mockCato) markers() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.markersSeen))
	copy(out, m.markersSeen)
	return out
}

var (
	// The adapter inlines its parameters into the query text; these extract
	// them the way a GraphQL server would after parsing.
	accountIDsRe = regexp.MustCompile(`accountIDs:\["([^"]*)"\]`)
	markerRe     = regexp.MustCompile(`marker:"([^"]*)"`)
)

func (m *mockCato) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.requests++
		m.mu.Unlock()

		// Request shape: POST to the graphql2 path with a JSON body. assert
		// (not require) because this runs on an httptest goroutine.
		if !assert.Equal(m.t, http.MethodPost, r.Method, "the adapter must POST") ||
			!assert.Equal(m.t, "/api/v1/graphql2", r.URL.Path, "wrong API path") {
			writeGzipJSON(m.t, w, errorsEnvelope("bad request"))
			return
		}
		assert.Equal(m.t, "application/json", r.Header.Get("Content-Type"))

		// Authentication: the x-api-key header. The real API answers a bad key
		// with HTTP 200 and a GraphQL errors envelope -- verbatim:
		// {"errors":[{"message":"permission denied","path":["eventsFeed"],
		// "extensions":{"code":"Code104"}}],"data":{"eventsFeed":null}} --
		// which the adapter detects via the "errors" key.
		if r.Header.Get("x-api-key") != m.apiKey {
			m.mu.Lock()
			m.authFailures++
			m.mu.Unlock()
			writeGzipJSON(m.t, w, map[string]interface{}{
				"errors": []interface{}{
					map[string]interface{}{
						"message":    "permission denied",
						"path":       []interface{}{"eventsFeed"},
						"extensions": map[string]interface{}{"code": "Code104"},
					},
				},
				"data": map[string]interface{}{"eventsFeed": nil},
			})
			return
		}

		body, err := io.ReadAll(r.Body)
		if !assert.NoError(m.t, err) {
			writeGzipJSON(m.t, w, errorsEnvelope("unreadable body"))
			return
		}
		var req struct {
			Query string `json:"query"`
		}
		if !assert.NoError(m.t, json.Unmarshal(body, &req), "body must be {\"query\": ...} JSON") ||
			!assert.Contains(m.t, req.Query, "eventsFeed", "query must target eventsFeed") {
			writeGzipJSON(m.t, w, errorsEnvelope("malformed query"))
			return
		}
		// The adapter must ask for everything it later consumes.
		for _, field := range []string{"marker", "fetchedCount", "accounts", "records", "time", "fieldsMap"} {
			assert.Contains(m.t, req.Query, field, "query must select %q", field)
		}

		accountMatch := accountIDsRe.FindStringSubmatch(req.Query)
		if !assert.NotNil(m.t, accountMatch, "query must carry accountIDs") {
			writeGzipJSON(m.t, w, errorsEnvelope("missing accountIDs"))
			return
		}
		if accountMatch[1] != m.accountID {
			writeGzipJSON(m.t, w, errorsEnvelope(fmt.Sprintf("account %s not found", accountMatch[1])))
			return
		}
		markerMatch := markerRe.FindStringSubmatch(req.Query)
		if !assert.NotNil(m.t, markerMatch, "query must carry a marker") {
			writeGzipJSON(m.t, w, errorsEnvelope("missing marker"))
			return
		}
		marker := markerMatch[1]

		m.mu.Lock()
		m.markersSeen = append(m.markersSeen, marker)

		if m.rateLimited > 0 {
			m.rateLimited--
			m.mu.Unlock()
			// Must match the exact prefix the adapter sniffs for.
			writeGzipRaw(m.t, w, []byte(`{"errors":[{"message":"rate limit for operation: eventsFeed. Try again in 5 seconds"}]}`))
			return
		}
		if m.forcedError != "" {
			msg := m.forcedError
			m.mu.Unlock()
			writeGzipJSON(m.t, w, errorsEnvelope(msg))
			return
		}

		// Resolve the marker to a feed offset; an empty marker starts from the
		// beginning of the retained feed, like a first call to the real API.
		offset := 0
		if marker != "" {
			n, convErr := strconv.Atoi(strings.TrimPrefix(marker, mockMarkerPrefix))
			if !assert.NoError(m.t, convErr, "unknown marker %q", marker) {
				m.mu.Unlock()
				writeGzipJSON(m.t, w, errorsEnvelope("invalid marker"))
				return
			}
			offset = n
		}
		if offset > len(m.events) {
			offset = len(m.events)
		}
		end := len(m.events)
		if m.batchSize > 0 && offset+m.batchSize < end {
			end = offset + m.batchSize
		}
		batch := m.events[offset:end]
		records := make([]interface{}, 0, len(batch))
		for _, e := range batch {
			records = append(records, map[string]interface{}{
				"time":      e.Time,
				"fieldsMap": e.FieldsMap,
			})
		}
		newMarker := mockMarkerPrefix + strconv.Itoa(end)
		m.mu.Unlock()

		writeGzipJSON(m.t, w, map[string]interface{}{
			"data": map[string]interface{}{
				"eventsFeed": map[string]interface{}{
					"marker":       newMarker,
					"fetchedCount": len(batch),
					"accounts": []interface{}{
						map[string]interface{}{
							"id":      m.accountID,
							"records": records,
						},
					},
				},
			},
		})
	}
}

func errorsEnvelope(msg string) map[string]interface{} {
	return map[string]interface{}{
		"errors": []interface{}{
			map[string]interface{}{"message": msg},
		},
	}
}

// writeGzipJSON writes v as a gzip-compressed JSON body. The adapter requests
// "Accept-Encoding: gzip" itself and always gunzips the raw body, so the mock
// must compress every response just like the real API does.
func writeGzipJSON(t *testing.T, w http.ResponseWriter, v interface{}) {
	b, err := json.Marshal(v)
	assert.NoError(t, err)
	writeGzipRaw(t, w, b)
}

func writeGzipRaw(t *testing.T, w http.ResponseWriter, b []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Encoding", "gzip")
	gz := gzip.NewWriter(w)
	_, err := gz.Write(b)
	assert.NoError(t, err)
	assert.NoError(t, gz.Close())
}

// --- realistic event fixtures ---------------------------------------------------

// securityEvent returns a record shaped like a real Cato eventsFeed Security /
// Internet Firewall event: the documented fieldsMap field names with clearly
// fake values (example.com users, RFC 1918 / RFC 5737 addresses, all-1s
// account id).
func securityEvent(i int, ts string) catoRecord {
	return catoRecord{
		Time: ts,
		FieldsMap: map[string]interface{}{
			"event_type":          "Security",
			"event_sub_type":      "Internet Firewall",
			"action":              "Block",
			"rule":                "Block Malicious Categories",
			"rule_name":           "Block Malicious Categories",
			"src_ip":              fmt.Sprintf("10.0.0.%d", 10+i),
			"src_site":            "branch-example-1",
			"src_is_site_or_vpn":  "Site",
			"dest_ip":             "203.0.113.55",
			"dest_port":           "443",
			"dest_country":        "United States",
			"dest_is_site_or_vpn": "Internet",
			"application":         "HTTPS",
			"os_type":             "OS_WINDOWS",
			"pop_name":            "Example-POP-1",
			"user_name":           "alice@example.com",
			"ISP_name":            "Example ISP Inc",
			"traffic_direction":   "OUTBOUND",
			"account_id":          strconv.Itoa(testAccountID),
			"internalId":          fmt.Sprintf("ZmFrZS1ldmVudC0x-%d", i),
			"time_str":            ts,
		},
	}
}

// connectivityEvent returns a record shaped like a Cato Connectivity event.
func connectivityEvent(i int, ts string) catoRecord {
	return catoRecord{
		Time: ts,
		FieldsMap: map[string]interface{}{
			"event_type":     "Connectivity",
			"event_sub_type": "Connected",
			"src_site":       fmt.Sprintf("branch-example-%d", i),
			"link_type":      "Cato",
			"pop_name":       "Example-POP-2",
			"ISP_name":       "Example ISP Inc",
			"account_id":     strconv.Itoa(testAccountID),
			"internalId":     fmt.Sprintf("ZmFrZS1jb25uLTE-%d", i),
			"time_str":       ts,
		},
	}
}

// expectedPayload is what the adapter ships for a record: the fieldsMap plus
// the record's time injected as "event_timestamp".
func expectedPayload(rec catoRecord) map[string]interface{} {
	out := make(map[string]interface{}, len(rec.FieldsMap)+1)
	for k, v := range rec.FieldsMap {
		out[k] = v
	}
	out["event_timestamp"] = rec.Time
	return out
}

// --- test harness ---------------------------------------------------------------

// resetAdapterGlobals resets the package-level state the adapter keeps between
// runs (the marker cursor, its on-disk persistence path) and shortens the poll
// interval so tests iterate quickly. It must only be called when no adapter
// goroutine is running.
func resetAdapterGlobals(t *testing.T) {
	t.Helper()
	marker = ""
	configFile = filepath.Join(t.TempDir(), "cato-marker.txt")
	pollInterval = 25 * time.Millisecond
}

// newMockServer starts an httptest server for the mock, closed at test end
// (after the adapter is stopped -- cleanups run LIFO).
func newMockServer(t *testing.T, mock *mockCato) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(mock.handler())
	t.Cleanup(server.Close)
	return server
}

// startTestAdapter resets package globals, optionally pre-seeds the persisted
// marker file, and starts the adapter against the mock with an in-memory sink.
// The adapter is stopped (and waited for) at test end.
func startTestAdapter(t *testing.T, mock *mockCato, server *httptest.Server, preMarker string) *captureSink {
	t.Helper()
	resetAdapterGlobals(t)
	if preMarker != "" {
		require.NoError(t, os.WriteFile(configFile, []byte(preMarker+"\n"), 0o644))
	}

	sink := &captureSink{}
	conf := CatoConfig{
		ClientOptions: testClientOptions(t),
		ApiKey:        mock.apiKey,
		AccountId:     testAccountID,
		Url:           server.URL + "/api/v1/graphql2",
	}
	require.NoError(t, conf.Validate())

	adapter, chStopped, err := newCatoAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { stopAdapter(t, adapter, chStopped) })
	return sink
}

// stopAdapter asks the polling goroutine to exit and waits until it has. The
// adapter's Close() is not usable as a stop mechanism (it calls Done() on a
// WaitGroup that was never Add()ed, which panics), so tests flip the
// isRunning flag the feed loop checks.
func stopAdapter(t *testing.T, a *CatoAdapter, chStopped chan struct{}) {
	t.Helper()
	a.mRunning.Lock()
	a.isRunning = 0
	a.mRunning.Unlock()
	select {
	case <-chStopped:
	case <-time.After(10 * time.Second):
		t.Fatal("adapter goroutine did not stop")
	}
}

func readPersistedMarker(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(configFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// --- end-to-end tests -------------------------------------------------------------

// TestCatoEventsFeedEndToEnd drives the adapter against the mock API and
// asserts the exact events shipped: each record's fieldsMap is forwarded
// verbatim with the record time injected as event_timestamp, the message is
// stamped with the ship time (this adapter does not parse the event time into
// TimestampMs and does not set an EventType), and the first request starts
// from an empty marker.
func TestCatoEventsFeedEndToEnd(t *testing.T) {
	mock := newMockCato(t)
	events := []catoRecord{
		securityEvent(1, "2026-06-11T10:00:00Z"),
		securityEvent(2, "2026-06-11T10:00:05Z"),
		connectivityEvent(3, "2026-06-11T10:00:09Z"),
	}
	for _, e := range events {
		mock.addEvent(e)
	}
	server := newMockServer(t, mock)

	startMs := uint64(time.Now().UnixMilli())
	sink := startTestAdapter(t, mock, server, "")

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond, "expected all 3 events to ship")

	msgs := sink.snapshot()
	require.Len(t, msgs, 3)
	for i, msg := range msgs {
		// Payload: the fieldsMap verbatim + event_timestamp from the record time.
		assert.JSONEq(t, mustJSON(t, expectedPayload(events[i])), mustJSON(t, msg.JsonPayload),
			"event %d payload must be the record's fieldsMap plus event_timestamp", i)
		// This adapter stamps messages with the ship time, not the event time.
		assert.GreaterOrEqual(t, msg.TimestampMs, startMs, "event %d TimestampMs", i)
		assert.LessOrEqual(t, msg.TimestampMs, uint64(time.Now().UnixMilli()), "event %d TimestampMs", i)
		// And it ships untyped JSON (the platform comes from ClientOptions).
		assert.Empty(t, msg.EventType, "event %d should have no per-event type", i)
	}

	markers := mock.markers()
	require.NotEmpty(t, markers)
	assert.Equal(t, "", markers[0], "the first poll must start from an empty marker")
	assert.Zero(t, mock.authFailureCount())
}

// TestCatoRepollDoesNotReship verifies the marker advances after the first
// batch and later polls carry it, so nothing is shipped twice.
func TestCatoRepollDoesNotReship(t *testing.T) {
	mock := newMockCato(t)
	for i := 1; i <= 3; i++ {
		mock.addEvent(securityEvent(i, fmt.Sprintf("2026-06-11T10:00:0%dZ", i)))
	}
	server := newMockServer(t, mock)
	sink := startTestAdapter(t, mock, server, "")

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond)

	// Let at least three more polls complete. Wait on the markers the mock
	// recorded (not its raw request counter, which is incremented before the
	// marker is parsed) so the snapshot below is guaranteed to include them.
	base := len(mock.markers())
	require.Eventually(t, func() bool { return len(mock.markers()) >= base+3 },
		5*time.Second, 10*time.Millisecond, "the adapter should keep polling")

	assert.Equal(t, 3, sink.count(), "re-polling must not re-ship events")

	markers := mock.markers()
	require.GreaterOrEqual(t, len(markers), 4)
	assert.Equal(t, "", markers[0])
	for i, mk := range markers[1:] {
		assert.Equal(t, mockMarkerPrefix+"3", mk, "poll %d must resume from the advanced marker", i+1)
	}

	// The advanced marker is also persisted to disk between polls.
	assert.Equal(t, mockMarkerPrefix+"3", readPersistedMarker(t))
}

// TestCatoMidRunEventShipsExactlyOnce verifies an event appearing while the
// adapter is running is picked up by a later poll and shipped exactly once.
func TestCatoMidRunEventShipsExactlyOnce(t *testing.T) {
	mock := newMockCato(t)
	mock.addEvent(securityEvent(1, "2026-06-11T10:00:00Z"))
	mock.addEvent(securityEvent(2, "2026-06-11T10:00:01Z"))
	server := newMockServer(t, mock)
	sink := startTestAdapter(t, mock, server, "")

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 10*time.Millisecond)

	// A new event arrives mid-run.
	late := connectivityEvent(9, "2026-06-11T10:05:00Z")
	mock.addEvent(late)

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond, "the mid-run event should ship")

	// Let more polls happen, then confirm nothing was duplicated.
	base := mock.requestCount()
	require.Eventually(t, func() bool { return mock.requestCount() >= base+2 },
		5*time.Second, 10*time.Millisecond)
	assert.Equal(t, 3, sink.count(), "no event may ship more than once")

	perID := map[string]int{}
	for _, msg := range sink.snapshot() {
		id, _ := msg.JsonPayload["internalId"].(string)
		require.NotEmpty(t, id)
		perID[id]++
	}
	assert.Equal(t, map[string]int{
		"ZmFrZS1ldmVudC0x-1": 1,
		"ZmFrZS1ldmVudC0x-2": 1,
		"ZmFrZS1jb25uLTE-9":  1,
	}, perID, "every event must ship exactly once")
}

// TestCatoMultiBatchFeedFullyConsumed verifies a backlog larger than one
// eventsFeed batch is drained across successive polls via the marker cursor,
// in order and without gaps or duplicates.
func TestCatoMultiBatchFeedFullyConsumed(t *testing.T) {
	mock := newMockCato(t)
	mock.batchSize = 2
	events := make([]catoRecord, 0, 5)
	for i := 1; i <= 5; i++ {
		e := securityEvent(i, fmt.Sprintf("2026-06-11T10:00:0%dZ", i))
		events = append(events, e)
		mock.addEvent(e)
	}
	server := newMockServer(t, mock)
	sink := startTestAdapter(t, mock, server, "")

	require.Eventually(t, func() bool { return sink.count() == 5 },
		5*time.Second, 10*time.Millisecond, "all batches should be consumed")

	msgs := sink.snapshot()
	require.Len(t, msgs, 5)
	for i, msg := range msgs {
		assert.JSONEq(t, mustJSON(t, expectedPayload(events[i])), mustJSON(t, msg.JsonPayload),
			"event %d must ship in feed order", i)
	}

	// The batches were walked through the marker cursor: "" -> 2 -> 4 -> 5.
	markers := mock.markers()
	require.GreaterOrEqual(t, len(markers), 3)
	assert.Equal(t, []string{"", mockMarkerPrefix + "2", mockMarkerPrefix + "4"}, markers[:3])

	// And the feed stays drained.
	base := mock.requestCount()
	require.Eventually(t, func() bool { return mock.requestCount() >= base+2 },
		5*time.Second, 10*time.Millisecond)
	assert.Equal(t, 5, sink.count())
}

// TestCatoMarkerPersistenceResume verifies the adapter resumes from the marker
// persisted on disk by a previous run: events before the marker are not
// re-shipped, and the file is updated as the feed advances.
func TestCatoMarkerPersistenceResume(t *testing.T) {
	mock := newMockCato(t)
	events := make([]catoRecord, 0, 4)
	for i := 1; i <= 4; i++ {
		e := securityEvent(i, fmt.Sprintf("2026-06-11T10:00:0%dZ", i))
		events = append(events, e)
		mock.addEvent(e)
	}
	server := newMockServer(t, mock)

	// A previous run consumed the first two events.
	sink := startTestAdapter(t, mock, server, mockMarkerPrefix+"2")

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 10*time.Millisecond, "only the events after the marker should ship")

	markers := mock.markers()
	require.NotEmpty(t, markers)
	assert.Equal(t, mockMarkerPrefix+"2", markers[0],
		"the first request must resume from the persisted marker")

	msgs := sink.snapshot()
	require.Len(t, msgs, 2)
	assert.JSONEq(t, mustJSON(t, expectedPayload(events[2])), mustJSON(t, msgs[0].JsonPayload))
	assert.JSONEq(t, mustJSON(t, expectedPayload(events[3])), mustJSON(t, msgs[1].JsonPayload))

	require.Eventually(t, func() bool { return readPersistedMarker(t) == mockMarkerPrefix+"4" },
		5*time.Second, 10*time.Millisecond, "the advanced marker must be persisted")
	assert.Equal(t, 2, sink.count(), "nothing may be re-shipped after resuming")
}

// --- send() behavior tests ----------------------------------------------------------
//
// Failure handling lives in the adapter's send(): a response carrying a
// GraphQL "errors" key is reported as a failure to the caller; a rate-limit
// error is retried in place. These are exercised directly (on an adapter
// struct that has no polling goroutine) because the feed loop's own failure
// path retries with multi-second sleeps and then crashes the process, which
// cannot be hosted in a unit test.

// bareAdapter builds a CatoAdapter without starting the polling goroutine, for
// driving send() directly.
func bareAdapter(t *testing.T, server *httptest.Server, apiKey string) *CatoAdapter {
	t.Helper()
	return &CatoAdapter{
		conf: CatoConfig{
			ClientOptions: testClientOptions(t),
			ApiKey:        apiKey,
			AccountId:     testAccountID,
			Url:           server.URL + "/api/v1/graphql2",
		},
	}
}

// eventsFeedQuery mirrors the query string the feed loop builds.
func eventsFeedQuery(accountID, marker string) string {
	return fmt.Sprintf(`{
  eventsFeed(accountIDs:[%q]
    marker:%q
    filters:[,])
  {
    marker
    fetchedCount
    accounts {
      id
      records {
        time
        fieldsMap
      }
    }
  }
}`, accountID, marker)
}

// TestCatoSendBadAPIKey verifies a rejected API key surfaces as a failed
// send() carrying the GraphQL errors envelope, with nothing retried.
func TestCatoSendBadAPIKey(t *testing.T) {
	mock := newMockCato(t)
	mock.addEvent(securityEvent(1, "2026-06-11T10:00:00Z"))
	server := newMockServer(t, mock)

	a := bareAdapter(t, server, "wrong-api-key")
	ok, resp := a.send(eventsFeedQuery(mock.accountID, ""), mock.accountID, "wrong-api-key")

	assert.False(t, ok, "a bad API key must fail the call")
	errs, isList := resp["errors"].([]interface{})
	require.True(t, isList, "errors must be the GraphQL error list, got %T", resp["errors"])
	require.Len(t, errs, 1)
	assert.Equal(t, "permission denied", errs[0].(map[string]interface{})["message"],
		"the error details Cato returns for a bad key must be preserved")
	assert.Equal(t, 1, mock.authFailureCount())
	assert.Equal(t, 1, mock.requestCount(), "a credential rejection must not be retried")
}

// TestCatoSendGraphQLError verifies any GraphQL errors envelope (e.g. an
// unknown account) is reported as a failure with the error details preserved.
func TestCatoSendGraphQLError(t *testing.T) {
	mock := newMockCato(t)
	mock.forcedError = "Validation error: account 22222222 not found"
	server := newMockServer(t, mock)

	a := bareAdapter(t, server, mock.apiKey)
	ok, resp := a.send(eventsFeedQuery(mock.accountID, ""), mock.accountID, mock.apiKey)

	assert.False(t, ok)
	errs, isList := resp["errors"].([]interface{})
	require.True(t, isList, "errors must be the GraphQL error list, got %T", resp["errors"])
	require.Len(t, errs, 1)
	assert.Equal(t, mock.forcedError, errs[0].(map[string]interface{})["message"])
	assert.Equal(t, 1, mock.requestCount(), "a GraphQL error must not be retried by send()")
}

// TestCatoSendRateLimitRetried verifies a Cato rate-limit error is retried in
// place (after the adapter's fixed 5s pause) rather than reported as a failure.
func TestCatoSendRateLimitRetried(t *testing.T) {
	mock := newMockCato(t)
	mock.rateLimited = 1
	mock.addEvent(securityEvent(1, "2026-06-11T10:00:00Z"))
	server := newMockServer(t, mock)

	a := bareAdapter(t, server, mock.apiKey)
	ok, resp := a.send(eventsFeedQuery(mock.accountID, ""), mock.accountID, mock.apiKey)

	assert.True(t, ok, "the call must succeed after the rate limit clears")
	assert.Equal(t, 2, mock.requestCount(), "rate-limited request then the successful retry")

	feed := resp["data"].(map[string]interface{})["eventsFeed"].(map[string]interface{})
	assert.Equal(t, float64(1), feed["fetchedCount"])
	assert.Equal(t, mockMarkerPrefix+"1", feed["marker"])
}
