package usp_pandadoc

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
	mock := newMockPandaDoc("test-api-key")
	h := newPollHarness(t, mock)
	boot := h.now
	mock.addLogs(auditLogEntry("startup", boot.Add(-5*time.Minute)))
	mock.addLogs(auditLogEntry("outside", boot.Add(-35*time.Minute)))
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
			mock.addLogs(auditLogEntry("recent", h.now.Add(-time.Second)))
		}
		if step == 2 {
			require.Equal(t, 2, h.sink.count(), "startup must include pre-boot records within the overlap, excluding older history")
		}
		if step == 3 {
			require.Equal(t, 2, h.sink.count())
			mock.addLogs(auditLogEntry("delayed", boot.Add(-15*time.Minute)))
		}
	}
	h.run(4)
	require.Empty(t, h.fatal)
	require.Equal(t, 3, h.sink.count(), "a delayed record older than the last observed event must still ship")
	counts := map[string]int{}
	for _, msg := range h.sink.snapshot() {
		counts[msg.JsonPayload["id"].(string)]++
	}
	require.Equal(t, map[string]int{"startup": 1, "recent": 1, "delayed": 1}, counts)
	require.Len(t, windows, 4)
	require.Equal(t, overlapPeriod, windows[0][1].Sub(windows[0][0]))
	for i := 1; i < len(windows); i++ {
		require.Equal(t, windows[i-1][1].Add(-overlapPeriod), windows[i][0], "successful polls advance from the complete END, never the latest event")
	}
}

// Invalid timestamps still identify usable records. They must not be stamped
// at zero time, culled and re-shipped on every successful overlapping poll.
func TestMalformedEventTimeStillDedupes(t *testing.T) {
	entry := auditLogEntry("malformed", time.Now())
	entry["request_time"] = "not-a-time"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(utils.Dict{"results": []utils.Dict{entry}})
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
	mock := newMockPandaDoc("test-api-key")
	h := newPollHarness(t, mock)
	eventTime := h.now.Add(-20 * time.Minute)
	mock.addLogs(auditLogEntry("expiring", eventTime))
	var firstStamp int64
	h.beforeWait = func(step int) {
		if step == 2 {
			firstStamp = h.a.dedupe["expiring"]
			h.now = h.now.Add(25 * time.Minute)
		}
		if step == 3 {
			require.Contains(t, h.a.dedupe, "expiring", "retain seen IDs while the completed window still reaches them")
		}
	}
	h.run(3)
	require.Empty(t, h.fatal)
	require.Equal(t, 1, h.sink.count())
	require.NotContains(t, h.a.dedupe, "expiring", "expire the ID once the requested window has passed the event timestamp")
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
	require.Equal(t, "pandadoc api non-200: 403 Forbidden", sections[0])
	diagnostics := strings.SplitN(sections[1], "\nRESPONSE: ", 2)
	require.Len(t, diagnostics, 2)
	require.Equal(t, requestURL, diagnostics[0], "REQUEST must identify the actual URL sent")
	require.Equal(t, responseBody, diagnostics[1], "RESPONSE must identify the returned body")
}
