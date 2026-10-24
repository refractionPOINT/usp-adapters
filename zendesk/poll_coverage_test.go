package usp_zendesk

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

// Exercise the production polling loop, including a successful poll that
// returns no new IDs before an older, newly visible record arrives.
func TestPollStartupAndDelayedArrival(t *testing.T) {
	mock := newMockZendesk(mockEmail, mockToken)
	h := newPollHarness(t, mock.handler(t))
	boot := h.now
	mock.addLog(auditLogFixture("10001", boot.Add(-5*time.Minute), "update"))
	mock.addLog(auditLogFixture("10005", boot.Add(-35*time.Minute), "update"))
	windows := [][2]time.Time{}
	transport := h.a.httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
		start, end := pollBounds(t, r)
		windows = append(windows, [2]time.Time{start, end})
		return transport.RoundTrip(r)
	})
	h.beforeWait = func(step int) {
		if step == 1 {
			mock.addLog(auditLogFixture("10002", h.now.Add(-time.Second), "update"))
		}
		if step == 2 {
			require.Equal(t, 2, h.sink.count(), "startup must include pre-boot records within the overlap, excluding older history")
		}
		if step == 3 {
			require.Equal(t, 2, h.sink.count())
			mock.addLog(auditLogFixture("10003", boot.Add(-15*time.Minute), "update"))
		}
	}
	h.run(4)
	require.Empty(t, h.fatal)
	require.Equal(t, 3, h.sink.count(), "a delayed record older than the last observed event must still ship")
	counts := map[string]int{}
	for _, msg := range h.sink.snapshot() {
		counts[recordID(msg.JsonPayload["id"])]++
	}
	require.Equal(t, map[string]int{"10001": 1, "10002": 1, "10003": 1}, counts)
	require.Len(t, windows, 4)
	require.Equal(t, overlapPeriod, windows[0][1].Sub(windows[0][0]))
	for i := 1; i < len(windows); i++ {
		require.Equal(t, windows[i-1][1].Add(-overlapPeriod), windows[i][0], "successful polls advance from the complete END, never the latest event")
	}
}

// Invalid timestamps still identify usable records. They must not be stamped
// at zero time, culled and re-shipped on every successful overlapping poll.
func TestMalformedEventTimeStillDedupes(t *testing.T) {
	entry := auditLogFixture("10006", time.Now(), "update")
	entry["created_at"] = "not-a-time"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(utils.Dict{"audit_logs": []utils.Dict{entry}, "meta": utils.Dict{"has_more": false}})
	})
	h := newPollHarness(t, handler)
	h.run(3)
	require.Empty(t, h.fatal)
	require.Equal(t, 1, h.sink.count(), "an invalid timestamp must not cause duplicate delivery on every poll")
	require.Len(t, h.warnings, 1, "warn once for the new malformed record")
	require.Contains(t, h.warnings[0], "unparseable")
}

// Correct event times bound dedupe retention. Using ingestion time instead
// would retain IDs after the requested window has moved beyond those records.
func TestPollRetainsEventTimeUntilWindowPasses(t *testing.T) {
	mock := newMockZendesk(mockEmail, mockToken)
	h := newPollHarness(t, mock.handler(t))
	eventTime := h.now.Add(-20 * time.Minute)
	mock.addLog(auditLogFixture("10004", eventTime, "update"))
	var firstStamp int64
	h.beforeWait = func(step int) {
		if step == 2 {
			firstStamp = h.a.dedupe["10004"]
			h.now = h.now.Add(25 * time.Minute)
		}
		if step == 3 {
			require.Contains(t, h.a.dedupe, "10004", "retain seen IDs while the completed window still reaches them")
		}
	}
	h.run(3)
	require.Empty(t, h.fatal)
	require.Equal(t, 1, h.sink.count())
	require.NotContains(t, h.a.dedupe, "10004", "expire the ID once the requested window has passed the event timestamp")
	require.Equal(t, eventTime.Unix(), firstStamp, "production timestamp parsing must retain the vendor event time, not hide parse failures behind now")
	require.Empty(t, h.warnings)
}

func TestPollHTTPErrorDiagnostics(t *testing.T) {
	const responseBody = `{"detail":"mock response, not request"}`
	h := newPollHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(responseBody))
	}))
	transport := h.a.httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	requestURL := ""
	h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
		requestURL = r.URL.String()
		return transport.RoundTrip(r)
	})
	h.run(1)
	require.Len(t, h.fatal, 1)
	sections := strings.SplitN(h.fatal[0].Error(), "\nREQUEST: ", 2)
	require.Len(t, sections, 2)
	require.Equal(t, "zendesk api non-200: 403 Forbidden", sections[0])
	diagnostics := strings.SplitN(sections[1], "\nRESPONSE: ", 2)
	require.Len(t, diagnostics, 2)
	require.Equal(t, requestURL, diagnostics[0], "REQUEST must identify the actual URL sent")
	require.Equal(t, responseBody, diagnostics[1], "RESPONSE must identify the returned body")
}

// These adjacent integers would collapse if routed through float64. The API
// payload and the dedupe key must both preserve the original JSON integer.
func TestPollLargeNumericIDsPreserveIdentity(t *testing.T) {
	mock := newMockZendesk(mockEmail, mockToken)
	h := newPollHarness(t, mock.handler(t))
	for _, id := range []string{"9007199254740992", "9007199254740993", "18446744073709551615"} {
		mock.addLog(auditLogFixture(id, h.now.Add(-time.Minute), "update"))
	}
	h.run(2)
	require.Empty(t, h.fatal)
	require.Equal(t, 3, h.sink.count())
	counts := map[uint64]int{}
	for _, msg := range h.sink.snapshot() {
		id, ok := msg.JsonPayload["id"].(uint64)
		require.True(t, ok, "utils.Dict must preserve the JSON integer as uint64")
		counts[id]++
		data, err := json.Marshal(msg.JsonPayload)
		require.NoError(t, err)
		var roundTrip utils.Dict
		require.NoError(t, json.Unmarshal(data, &roundTrip))
		require.Equal(t, id, roundTrip["id"], "shipped JSON must retain every integer bit")
	}
	require.Equal(t, map[uint64]int{9007199254740992: 1, 9007199254740993: 1, 18446744073709551615: 1}, counts)
}
