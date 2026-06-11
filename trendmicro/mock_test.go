package usp_trendmicro

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

// This file exercises the adapter end-to-end against a mock Trend Micro Vision
// One API, capturing the exact messages it ships so their content -- event
// type, timestamp and verbatim payload -- can be asserted.

const alertsPath = "/v3.0/workbench/alerts"

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

// countByID returns how many shipped messages carry the given Workbench alert
// id.
func (s *captureSink) countByID(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, m := range s.messages {
		if v, _ := m.JsonPayload["id"].(string); v == id {
			n++
		}
	}
	return n
}

// --- mock Vision One API ----------------------------------------------------

// recordedRequest captures one request the adapter made, for assertions on the
// wire shape (method, path, auth header, query params).
type recordedRequest struct {
	method string
	path   string
	auth   string
	query  url.Values
}

// mockVisionOne is an in-memory stand-in for the Vision One Workbench alerts
// API as the adapter consumes it: a GET on /v3.0/workbench/alerts authenticated
// with "Authorization: Bearer <token>", filtered by the startDateTime /
// endDateTime query params (compared against each alert's createdDateTime,
// boundaries inclusive, like the real API's default dateTimeTarget), returning
// an {"items": [...], "count": N, "totalCount": M} envelope with a full
// "nextLink" URL when more pages remain (continuation via a skipToken query
// param, which the adapter follows verbatim).
type mockVisionOne struct {
	mu        sync.Mutex
	token     string
	pageSize  int
	serverURL string
	alerts    []utils.Dict
	requests  []recordedRequest
}

func newMockVisionOne(token string, pageSize int) *mockVisionOne {
	return &mockVisionOne{token: token, pageSize: pageSize}
}

func (m *mockVisionOne) addAlert(a utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alerts = append(m.alerts, a)
}

func (m *mockVisionOne) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

func (m *mockVisionOne) recordedRequests() []recordedRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]recordedRequest, len(m.requests))
	copy(out, m.requests)
	return out
}

// writeVisionOneError renders the Vision One error envelope.
func writeVisionOneError(w http.ResponseWriter, status int, code, message string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{"code": code, "message": message},
	})
}

// handler implements the mock API. It runs on httptest server goroutines, so
// it reports failures with assert (require's FailNow is only valid on the test
// goroutine).
func (m *mockVisionOne) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.requests = append(m.requests, recordedRequest{
			method: r.Method,
			path:   r.URL.Path,
			auth:   r.Header.Get("Authorization"),
			query:  r.URL.Query(),
		})
		alerts := make([]utils.Dict, len(m.alerts))
		copy(alerts, m.alerts)
		pageSize := m.pageSize
		serverURL := m.serverURL
		m.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path != alertsPath {
			writeVisionOneError(w, http.StatusNotFound, "NotFound", "The requested resource was not found.")
			return
		}
		if r.Method != http.MethodGet {
			writeVisionOneError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "The HTTP method is not supported.")
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+m.token {
			writeVisionOneError(w, http.StatusUnauthorized, "Unauthorized", "The provided authentication token is invalid.")
			return
		}

		q := r.URL.Query()
		start, errStart := time.Parse(time.RFC3339, q.Get("startDateTime"))
		end, errEnd := time.Parse(time.RFC3339, q.Get("endDateTime"))
		if !assert.NoError(t, errStart, "startDateTime must be ISO 8601") ||
			!assert.NoError(t, errEnd, "endDateTime must be ISO 8601") {
			writeVisionOneError(w, http.StatusBadRequest, "BadRequest", "startDateTime/endDateTime must be in ISO 8601 format.")
			return
		}
		skip := 0
		if s := q.Get("skipToken"); s != "" {
			v, err := strconv.Atoi(s)
			if !assert.NoError(t, err, "skipToken must be the token the mock issued") {
				writeVisionOneError(w, http.StatusBadRequest, "BadRequest", "invalid skipToken")
				return
			}
			skip = v
		}

		// Filter on createdDateTime within [startDateTime, endDateTime], both
		// boundaries inclusive (the real API's documented default target is
		// the alert creation time).
		filtered := []utils.Dict{}
		for _, a := range alerts {
			cs, _ := a["createdDateTime"].(string)
			c, err := time.Parse(time.RFC3339, cs)
			if !assert.NoError(t, err, "mock fixture has a bad createdDateTime") {
				continue
			}
			if !c.Before(start) && !c.After(end) {
				filtered = append(filtered, a)
			}
		}

		if skip > len(filtered) {
			skip = len(filtered)
		}
		pageEnd := skip + pageSize
		if pageEnd > len(filtered) {
			pageEnd = len(filtered)
		}
		page := filtered[skip:pageEnd]

		resp := map[string]interface{}{
			"totalCount": len(filtered),
			"count":      len(page),
			"items":      page,
		}
		if pageEnd < len(filtered) {
			next := url.Values{}
			next.Set("startDateTime", q.Get("startDateTime"))
			next.Set("endDateTime", q.Get("endDateTime"))
			next.Set("skipToken", strconv.Itoa(pageEnd))
			resp["nextLink"] = fmt.Sprintf("%s%s?%s", serverURL, alertsPath, next.Encode())
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// startMockVisionOne starts the httptest server and wires its URL back into
// the mock so nextLink can point at it.
func startMockVisionOne(t *testing.T, m *mockVisionOne) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(m.handler(t))
	t.Cleanup(server.Close)
	m.mu.Lock()
	m.serverURL = server.URL
	m.mu.Unlock()
	return server
}

// --- realistic record fixtures ------------------------------------------------

// visionOneAlert returns a record shaped like a real Vision One Workbench
// alert (GET /v3.0/workbench/alerts item): the documented top-level fields plus
// the nested impactScope/indicators/matchedRules structures with mixed scalar
// types, arrays and nested objects. All identifiers are clearly fake.
func visionOneAlert(id string, created time.Time) utils.Dict {
	ts := created.UTC().Format(time.RFC3339)
	return utils.Dict{
		"schemaVersion":       "1.12",
		"id":                  id,
		"investigationStatus": "New",
		"status":              "Open",
		"investigationResult": "No Findings",
		"workbenchLink":       "https://portal.xdr.trendmicro.com/index.html#/workbench/alerts/" + id,
		"alertProvider":       "SAE",
		"model":               "Possible Credential Dumping via Known Tool",
		"modelId":             "11111111-1111-1111-1111-111111111111",
		"modelType":           "preset",
		"score":               64,
		"severity":            "high",
		"createdDateTime":     ts,
		"updatedDateTime":     ts,
		"description":         "A high-privilege process accessed credential storage on an endpoint.",
		"impactScope": utils.Dict{
			"desktopCount":      1,
			"serverCount":       0,
			"accountCount":      1,
			"emailAddressCount": 0,
			"entities": []interface{}{
				utils.Dict{
					"entityType":          "account",
					"entityValue":         `EXAMPLE\jdoe`,
					"entityId":            `EXAMPLE\jdoe`,
					"relatedEntities":     []interface{}{"22222222-2222-2222-2222-222222222222"},
					"relatedIndicatorIds": []interface{}{1},
					"provenance":          []interface{}{"Alert"},
				},
				utils.Dict{
					"entityType": "host",
					"entityValue": utils.Dict{
						"guid": "22222222-2222-2222-2222-222222222222",
						"name": "workstation-01.example.com",
						"ips":  []interface{}{"203.0.113.10"},
					},
					"entityId":            "22222222-2222-2222-2222-222222222222",
					"relatedEntities":     []interface{}{},
					"relatedIndicatorIds": []interface{}{1},
					"provenance":          []interface{}{"Alert"},
				},
			},
		},
		"indicators": []interface{}{
			utils.Dict{
				"id":              1,
				"type":            "command_line",
				"field":           "processCmd",
				"value":           "powershell.exe -nop -w hidden -encodedcommand SQBFAFgA",
				"relatedEntities": []interface{}{"22222222-2222-2222-2222-222222222222"},
				"filterIds":       []interface{}{"33333333-3333-3333-3333-333333333333"},
				"provenance":      []interface{}{"Alert"},
			},
		},
		"matchedRules": []interface{}{
			utils.Dict{
				"id":   "44444444-4444-4444-4444-444444444444",
				"name": "Potential Credential Dumping",
				"matchedFilters": []interface{}{
					utils.Dict{
						"id":                "33333333-3333-3333-3333-333333333333",
						"name":              "Credential Dumping via Memory Access",
						"matchedDateTime":   ts,
						"mitreTechniqueIds": []interface{}{"V9013.T1003.001"},
						"matchedEvents": []interface{}{
							utils.Dict{
								"uuid":            "55555555-5555-5555-5555-555555555555",
								"matchedDateTime": ts,
								"type":            "TELEMETRY_PROCESS",
							},
						},
					},
				},
			},
		},
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// --- tests --------------------------------------------------------------------

// TestMockAlertsEndToEnd drives the adapter against the mock API and asserts
// the exact events shipped -- verbatim payloads, the adapter's (empty)
// EventType, a ship-time TimestampMs -- plus the wire shape of the first
// request (GET, bearer token, a 24h initial startDateTime/endDateTime window).
// It also verifies re-polling does not re-ship: the window advances past
// already-fetched alerts.
func TestMockAlertsEndToEnd(t *testing.T) {
	const token = "tmv1-fake-token-0000000000000000"

	mock := newMockVisionOne(token, 100)
	now := time.Now().UTC()
	want := []utils.Dict{
		visionOneAlert("WB-9002-20260611-00001", now.Add(-3*time.Hour)),
		visionOneAlert("WB-9002-20260611-00002", now.Add(-2*time.Hour)),
		visionOneAlert("WB-9002-20260611-00003", now.Add(-1*time.Hour)),
	}
	for _, a := range want {
		mock.addAlert(a)
	}
	server := startMockVisionOne(t, mock)

	sink := &captureSink{}
	beforeMs := uint64(time.Now().UnixMilli())
	conf := TrendMicroConfig{
		ClientOptions: testClientOptions(t),
		APIToken:      token,
		URL:           server.URL,
		PollInterval:  100 * time.Millisecond,
	}
	adapter, _, err := newTrendMicroAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 alerts to ship")

	// Re-polling must not re-ship: the time window advances past the alerts'
	// createdDateTime, so the count stays at 3 across several more polls.
	require.Never(t, func() bool { return sink.count() != 3 },
		500*time.Millisecond, 25*time.Millisecond, "alerts were re-shipped on a later poll")
	afterMs := uint64(time.Now().UnixMilli())

	// Wire shape of the first request.
	reqs := mock.recordedRequests()
	require.NotEmpty(t, reqs)
	first := reqs[0]
	assert.Equal(t, http.MethodGet, first.method)
	assert.Equal(t, alertsPath, first.path)
	assert.Equal(t, "Bearer "+token, first.auth)
	assert.Empty(t, first.query.Get("skipToken"), "the first request must not carry a continuation token")
	start, perr := time.Parse(time.RFC3339, first.query.Get("startDateTime"))
	require.NoError(t, perr, "startDateTime must be RFC3339")
	end, perr := time.Parse(time.RFC3339, first.query.Get("endDateTime"))
	require.NoError(t, perr, "endDateTime must be RFC3339")
	assert.WithinDuration(t, now.Add(-24*time.Hour), start, 2*time.Minute,
		"the initial window must start 24h in the past")
	assert.WithinDuration(t, now, end, 2*time.Minute, "the window must end at poll time")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		// The adapter does not tag an event type; LimaCharlie's platform
		// setting drives parsing. Pin that behavior.
		assert.Empty(t, msg.EventType)
		// The adapter stamps events with the ship time, not the alert's
		// createdDateTime. Pin that behavior.
		assert.GreaterOrEqual(t, msg.TimestampMs, beforeMs)
		assert.LessOrEqual(t, msg.TimestampMs, afterMs)
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "alert %s was not shipped", id)
		// The payload is shipped verbatim -- nested objects and arrays included.
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Vision One alert")
	}
}

// TestMockNewAlertMidRun verifies an alert created while the adapter is
// running is picked up by a subsequent poll, and that the advancing time
// window then stops it from being shipped over and over.
//
// The adapter has no deduplication: its only re-ship protection is the
// server-side startDateTime/endDateTime window, whose boundaries the real API
// treats inclusively at one-second granularity. An alert whose createdDateTime
// second coincides with a window boundary can therefore legitimately ship
// twice (against the real API too). The poll interval here is kept above one
// second so window starts strictly advance each poll, bounding the new alert
// at one possible boundary duplicate; the test pins "at least once, at most
// twice, then never again".
func TestMockNewAlertMidRun(t *testing.T) {
	const token = "tmv1-fake-token-0000000000000000"
	const newID = "WB-9002-20260611-00099"

	mock := newMockVisionOne(token, 100)
	now := time.Now().UTC()
	mock.addAlert(visionOneAlert("WB-9002-20260611-00010", now.Add(-2*time.Hour)))
	mock.addAlert(visionOneAlert("WB-9002-20260611-00011", now.Add(-1*time.Hour)))
	server := startMockVisionOne(t, mock)

	sink := &captureSink{}
	conf := TrendMicroConfig{
		ClientOptions: testClientOptions(t),
		APIToken:      token,
		URL:           server.URL,
		PollInterval:  1 * time.Second,
	}
	adapter, _, err := newTrendMicroAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "the two pre-existing alerts should ship")

	// A new alert is raised while the adapter runs.
	newAlert := visionOneAlert(newID, time.Now().UTC())
	mock.addAlert(newAlert)
	created, err := time.Parse(time.RFC3339, newAlert["createdDateTime"].(string))
	require.NoError(t, err)

	require.Eventually(t, func() bool { return sink.countByID(newID) >= 1 },
		5*time.Second, 25*time.Millisecond, "the new alert should ship on a later poll")

	// Wait for the polling window to move strictly past the new alert's
	// creation time; from then on no poll can return it again.
	require.Eventually(t, func() bool {
		for _, r := range mock.recordedRequests() {
			if s, perr := time.Parse(time.RFC3339, r.query.Get("startDateTime")); perr == nil && s.After(created) {
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "the poll window should advance past the new alert")

	stable := sink.count()
	require.Never(t, func() bool { return sink.count() != stable },
		1500*time.Millisecond, 50*time.Millisecond, "no alert may ship once the window has passed it")

	assert.Equal(t, 1, sink.countByID("WB-9002-20260611-00010"))
	assert.Equal(t, 1, sink.countByID("WB-9002-20260611-00011"))
	n := sink.countByID(newID)
	assert.GreaterOrEqual(t, n, 1, "the new alert must ship")
	assert.LessOrEqual(t, n, 2,
		"the new alert may ship at most twice (inclusive window boundary, no dedup in the adapter)")
}

// TestMockPaginationFullDataset verifies a result set larger than one page is
// fully consumed by following nextLink, with every alert shipped exactly once.
func TestMockPaginationFullDataset(t *testing.T) {
	const token = "tmv1-fake-token-0000000000000000"
	const total = 25

	mock := newMockVisionOne(token, 10) // 25 alerts => pages of 10, 10, 5
	now := time.Now().UTC()
	for i := 0; i < total; i++ {
		mock.addAlert(visionOneAlert(
			fmt.Sprintf("WB-9002-20260611-%05d", i),
			now.Add(-2*time.Hour).Add(time.Duration(i)*time.Second)))
	}
	server := startMockVisionOne(t, mock)

	sink := &captureSink{}
	conf := TrendMicroConfig{
		ClientOptions: testClientOptions(t),
		APIToken:      token,
		URL:           server.URL,
		PollInterval:  100 * time.Millisecond,
	}
	adapter, _, err := newTrendMicroAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		5*time.Second, 25*time.Millisecond, "all paginated alerts should ship")
	require.Never(t, func() bool { return sink.count() != total },
		500*time.Millisecond, 25*time.Millisecond, "no alert may ship twice")

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		ids[msg.JsonPayload["id"].(string)] = true
	}
	assert.Len(t, ids, total, "every distinct alert should ship exactly once")

	// The adapter must have followed the mock's nextLink continuations.
	skipTokens := map[string]bool{}
	for _, r := range mock.recordedRequests() {
		if s := r.query.Get("skipToken"); s != "" {
			skipTokens[s] = true
		}
	}
	assert.True(t, skipTokens["10"], "the second page should be fetched via nextLink (skipToken=10)")
	assert.True(t, skipTokens["20"], "the third page should be fetched via nextLink (skipToken=20)")
}

// TestMockBadTokenShipsNothing pins the adapter's behavior on authentication
// failure: nothing ships, the error is surfaced through OnError, and the
// adapter does NOT stop -- it keeps polling on its interval (a 401 is handled
// like any other poll error).
func TestMockBadTokenShipsNothing(t *testing.T) {
	mock := newMockVisionOne("correct-token", 100)
	mock.addAlert(visionOneAlert("WB-9002-20260611-00001", time.Now().UTC().Add(-1*time.Hour)))
	server := startMockVisionOne(t, mock)

	var authErrs atomic.Int32
	opts := testClientOptions(t)
	opts.OnError = func(err error) {
		t.Logf("ERR: %v", err)
		if strings.Contains(err.Error(), "authentication failed (401)") {
			authErrs.Add(1)
		}
	}

	sink := &captureSink{}
	conf := TrendMicroConfig{
		ClientOptions: opts,
		APIToken:      "wrong-token",
		URL:           server.URL,
		PollInterval:  100 * time.Millisecond,
	}
	adapter, chStopped, err := newTrendMicroAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// The adapter keeps retrying across poll cycles and reports the 401.
	require.Eventually(t, func() bool { return mock.requestCount() >= 3 },
		5*time.Second, 20*time.Millisecond, "the adapter should keep polling after a 401")
	require.Eventually(t, func() bool { return authErrs.Load() >= 1 },
		5*time.Second, 20*time.Millisecond, "the 401 should be reported through OnError")

	select {
	case <-chStopped:
		t.Fatal("the adapter stopped on a 401; its current behavior is to keep polling")
	default:
	}

	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
}
