package usp_pandadoc

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The poll loop runs synchronously with a clock that advances only at waits.
// HTTP requests still go to the self-contained vendor mock.
type pollHarness struct {
	a          *PandaDocAdapter
	sink       *captureSink
	now        time.Time
	delays     []time.Duration
	fatal      []error
	warnings   []string
	beforeWait func(int)
}

func newPollHarness(t *testing.T, handler http.Handler) *pollHarness {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	h := &pollHarness{sink: &captureSink{}, now: time.Date(2024, 7, 15, 19, 0, 0, 0, time.UTC)}
	opts := testClientOptions(t)
	opts.DebugLog = func(string) {}
	opts.OnWarning = func(s string) { h.warnings = append(h.warnings, s) }
	opts.OnError = func(err error) { h.fatal = append(h.fatal, err); h.a.doStop.Set() }
	h.a = &PandaDocAdapter{
		conf:      PandaDocConfig{ClientOptions: opts, ApiKey: "test-api-key"},
		uspClient: h.sink, httpClient: server.Client(),
		logsURL: server.URL, pollInterval: defaultPollInterval,
		doStop: utils.NewEvent(), dedupe: map[string]int64{},
		now: func() time.Time { return h.now },
	}
	t.Cleanup(func() { _ = h.a.Close() })
	return h
}

func (h *pollHarness) run(polls int) {
	h.a.wait = func(delay time.Duration) bool {
		if h.a.doStop.IsSet() || len(h.delays) == polls {
			return true
		}
		h.delays = append(h.delays, delay)
		h.now = h.now.Add(delay)
		if h.beforeWait != nil {
			h.beforeWait(len(h.delays))
		}
		return h.a.doStop.IsSet()
	}
	h.a.wgSenders.Add(1)
	h.a.fetchEvents()
}

func firstPage(r *http.Request) bool { return r.URL.Query().Get("page") == "1" }

func pollBounds(t *testing.T, r *http.Request) (time.Time, time.Time) {
	t.Helper()
	start, err := time.ParseInLocation(documentedTimeLayout, r.URL.Query().Get("since"), time.UTC)
	require.NoError(t, err)
	end, err := time.ParseInLocation(documentedTimeLayout, r.URL.Query().Get("to"), time.UTC)
	require.NoError(t, err)
	return start, end
}

func pollResponse(status int, retryAfter string) *http.Response {
	return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d", status),
		Header: http.Header{"Retry-After": []string{retryAfter}}, Body: io.NopCloser(strings.NewReader(`{"error":"temporary"}`))}
}

// Retry callbacks must not be fatal. Partial pages must ship, subsequent
// complete polls must work, and every request must respect its retry deadline.
func TestTransientPollFailuresKeepState(t *testing.T) {
	h := newPollHarness(t, twoPageAPI(t))
	// Use fixtures relative to the harness clock, through the same vendor mock.
	mock := newMockPandaDoc("test-api-key")
	for i := 0; i < 1050; i++ {
		mock.addLogs(auditLogEntry(fmt.Sprintf("log-%d", i), h.now.Add(-20*time.Minute)))
	}
	server := httptest.NewServer(mock)
	defer server.Close()
	h.a.logsURL = server.URL
	transport := server.Client().Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	roots := []time.Time{}
	h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
		if firstPage(r) {
			roots = append(roots, h.now)
		}
		switch len(roots) {
		case 1:
			if !firstPage(r) {
				return pollResponse(429, "120"), nil
			}
		case 2:
			if !firstPage(r) {
				return pollResponse(503, h.now.Add(3*time.Minute).Format(http.TimeFormat)), nil
			}
		case 3:
			return nil, &net.DNSError{Err: "temporary timeout", IsTimeout: true, IsTemporary: true}
		}
		return transport.RoundTrip(r)
	})
	h.run(5)
	require.Empty(t, h.fatal)
	require.Len(t, h.warnings, 3)
	require.Equal(t, []time.Duration{30 * time.Second, 2 * time.Minute, 3 * time.Minute, 2 * time.Minute, 30 * time.Second}, h.delays)
	require.Len(t, roots, 5, "repeated complete polls must continue after transient failures")
	require.GreaterOrEqual(t, roots[1].Sub(roots[0]), 2*time.Minute)
	require.GreaterOrEqual(t, roots[2].Sub(roots[1]), 3*time.Minute)
	require.GreaterOrEqual(t, roots[3].Sub(roots[2]), 2*time.Minute)
	require.Equal(t, 1050, h.sink.count(), "dedupe state must survive transient failures")
	ids := map[string]int{}
	for _, msg := range h.sink.snapshot() {
		ids[msg.JsonPayload["id"].(string)]++
	}
	require.Len(t, ids, 1050)
	for id, count := range ids {
		assert.Equal(t, 1, count, "ID %s", id)
	}
}

// Unread records can age beyond the overlap during a shorter outage. Recover
// within the retry budget, then advance by the complete END for late arrivals.
func TestPollCatchupWithinRetryBudget(t *testing.T) {
	h := newPollHarness(t, twoPageAPI(t))
	fixtureTime := h.now.Add(-20 * time.Minute)
	mock := newMockPandaDoc("test-api-key")
	for i := 0; i < 1050; i++ {
		mock.addLogs(auditLogEntry(fmt.Sprintf("log-%d", i), fixtureTime))
	}
	server := httptest.NewServer(mock)
	defer server.Close()
	h.a.logsURL = server.URL
	transport := server.Client().Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	windows := [][2]time.Time{}
	h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
		if firstPage(r) {
			start, end := pollBounds(t, r)
			windows = append(windows, [2]time.Time{start, end})
		}
		if len(windows) <= 2 && !firstPage(r) {
			return pollResponse(503, ""), nil
		}
		return transport.RoundTrip(r)
	})
	h.beforeWait = func(step int) {
		if step == 2 {
			h.now = h.now.Add(25 * time.Minute)
		}
		if step == 4 {
			mock.addLogs(auditLogEntry("late", h.now.Add(-5*time.Minute)))
		}
	}
	h.run(4)
	require.Empty(t, h.fatal)
	require.Equal(t, 1051, h.sink.count(), "all unread and late records must ship without replaying the partial page")
	require.Len(t, windows, 4)
	require.Equal(t, windows[0][0], windows[1][0], "failure must preserve the unread start")
	require.Equal(t, windows[0][0], windows[2][0], "repeated failures must not advance the unread start")
	require.Greater(t, windows[1][1].Sub(fixtureTime), overlapPeriod)
	require.Equal(t, windows[2][1].Add(-overlapPeriod), windows[3][0], "advance only from the completed poll END, keeping the whole overlap")
	require.True(t, windows[3][0].After(windows[0][0]), "start must not stay frozen after success")
}

// A long pause between successful polls still bounds catch-up. This is not
// a continuously failing instance surviving beyond the fatal threshold.
func TestPollCatchupBoundWarns(t *testing.T) {
	h := newPollHarness(t, twoPageAPI(t))
	transport := h.a.httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	windows := [][2]time.Time{}
	h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
		if firstPage(r) {
			start, end := pollBounds(t, r)
			windows = append(windows, [2]time.Time{start, end})
		}
		return transport.RoundTrip(r)
	})
	h.beforeWait = func(step int) {
		if step == 2 {
			h.now = h.now.Add(25 * time.Hour)
		}
	}
	h.run(2)
	require.Empty(t, h.fatal)
	require.Len(t, windows, 2)
	require.Equal(t, pollMaxCatchup, windows[1][1].Sub(windows[1][0]))
	require.Contains(t, strings.Join(h.warnings, " "), "catch-up exceeded 24 hours; older unread records may be lost")
}

func TestPermanentPollFailuresAreFatal(t *testing.T) {
	for _, status := range []int{400, 401, 403} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			h := newPollHarness(t, twoPageAPI(t))
			requests := 0
			h.a.httpClient.Transport = pageRoundTripper(func(*http.Request) (*http.Response, error) {
				requests++
				resp := pollResponse(status, "600")
				if status == 401 {
					resp.Body = brokenPageBody{}
				}
				return resp, nil
			})
			h.run(3)
			require.Len(t, h.fatal, 1)
			require.Empty(t, h.warnings)
			require.Equal(t, 1, requests, "fatal errors must stop, not retry or busy-loop")
		})
	}
	for _, endpoint := range []string{"ftp://example.com/logs", "http://%"} {
		t.Run(endpoint, func(t *testing.T) {
			h := newPollHarness(t, twoPageAPI(t))
			h.a.logsURL = endpoint
			requests := 0
			h.a.httpClient.Transport = pageRoundTripper(func(*http.Request) (*http.Response, error) { requests++; return pollResponse(200, ""), nil })
			h.run(3)
			require.Len(t, h.fatal, 1)
			require.Empty(t, h.warnings)
			require.Zero(t, requests, "malformed configuration must fail before making requests")
		})
	}
}

func TestPartialPagesShipBeforeFatalCallback(t *testing.T) {
	h := newPollHarness(t, twoPageAPI(t))
	mock := newMockPandaDoc("test-api-key")
	for i := 0; i < 1050; i++ {
		mock.addLogs(auditLogEntry(fmt.Sprintf("log-%d", i), h.now.Add(-20*time.Minute)))
	}
	server := httptest.NewServer(mock)
	defer server.Close()
	h.a.logsURL = server.URL
	transport := server.Client().Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
		if !firstPage(r) {
			return pollResponse(401, ""), nil
		}
		return transport.RoundTrip(r)
	})
	onError := h.a.conf.ClientOptions.OnError
	h.a.conf.ClientOptions.OnError = func(err error) {
		require.Equal(t, 1000, h.sink.count(), "deliver partial pages before invoking a fatal callback")
		onError(err)
	}
	h.run(3)
	require.Len(t, h.fatal, 1)
	require.Empty(t, h.warnings)
	require.Equal(t, 1000, h.sink.count())
}

type trackedPageBody struct {
	io.ReadCloser
	closed *bool
}

func (b trackedPageBody) Close() error { *b.closed = true; return b.ReadCloser.Close() }

func TestPageBodiesCloseBeforeNextRequest(t *testing.T) {
	h := newPollHarness(t, twoPageAPI(t))
	mock := newMockPandaDoc("test-api-key")
	for i := 0; i < 1050; i++ {
		mock.addLogs(auditLogEntry(fmt.Sprintf("log-%d", i), h.now.Add(-20*time.Minute)))
	}
	server := httptest.NewServer(mock)
	defer server.Close()
	h.a.logsURL = server.URL
	transport := server.Client().Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	closed := true
	requests := 0
	h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
		require.True(t, closed, "previous page body must be closed before fetching another page")
		resp, err := transport.RoundTrip(r)
		if err == nil {
			closed = false
			resp.Body = trackedPageBody{resp.Body, &closed}
			requests++
		}
		return resp, err
	})
	h.run(2)
	require.Empty(t, h.fatal)
	require.Equal(t, 4, requests, "two two-page polls must close every body")
	require.True(t, closed)
}

func TestShutdownInterruptsRetryWait(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "3600")
		http.Error(w, "temporary", 429)
	}))
	defer server.Close()
	opts := testClientOptions(t)
	warned := make(chan struct{}, 1)
	opts.OnWarning = func(string) {
		select {
		case warned <- struct{}{}:
		default:
		}
	}
	opts.OnError = func(err error) { t.Errorf("transient error invoked fatal callback: %v", err) }
	sink := &captureSink{}
	a, _, err := newPandaDocAdapter(context.Background(), PandaDocConfig{ClientOptions: opts, ApiKey: "test-api-key", URL: server.URL, PollInterval: time.Millisecond}, sink)
	require.NoError(t, err)
	defer a.Close()
	select {
	case <-warned:
	case <-time.After(3 * time.Second):
		t.Fatal("no retry warning")
	}
	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("shutdown did not interrupt retry wait")
	}
	require.EqualValues(t, 1, requests.Load())
}

func TestTransientReadAndDecodeKeepState(t *testing.T) {
	for _, fault := range []string{"read", "decode"} {
		t.Run(fault, func(t *testing.T) {
			mock := newMockPandaDoc("test-api-key")
			h := newPollHarness(t, mock)
			for i := 0; i < 1050; i++ {
				mock.addLogs(auditLogEntry(fmt.Sprint(i), h.now.Add(-time.Minute)))
			}
			transport := h.a.httpClient.Transport
			if transport == nil {
				transport = http.DefaultTransport
			}
			failed := false
			h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
				if !firstPage(r) && !failed {
					failed = true
					resp := pollResponse(200, "")
					if fault == "read" {
						resp.Body = brokenPageBody{}
					} else {
						resp.Body = io.NopCloser(strings.NewReader("{"))
					}
					return resp, nil
				}
				return transport.RoundTrip(r)
			})
			h.run(3)
			require.Empty(t, h.fatal)
			require.Len(t, h.warnings, 1)
			require.Equal(t, 1050, h.sink.count())
			require.Equal(t, []time.Duration{30 * time.Second, 30 * time.Second, 30 * time.Second}, h.delays)
		})
	}
}
