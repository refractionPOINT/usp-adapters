package usp_pandadoc

import (
	"context"
	"errors"
	"fmt"
	"io"
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

type pageRoundTripper func(*http.Request) (*http.Response, error)

func (f pageRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type brokenPageBody struct{}

func (brokenPageBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (brokenPageBody) Close() error             { return nil }

func twoPageAPI(t *testing.T) http.Handler {
	mock := newMockPandaDoc("test-api-key")
	base := time.Now().Add(-time.Minute)
	for i := 0; i < 150; i++ {
		mock.addLogs(auditLogEntry(fmt.Sprintf("log-%d", i), base))
	}
	return mock
}

// Successful pages must be returned even when fetching a later page fails.
// Retrying the same window must deliver every ID once across both results.
func TestPaginationPreservesPartialResults(t *testing.T) {
	for _, fault := range []string{"status", "decode", "network", "read", "shutdown"} {
		t.Run(fault, func(t *testing.T) {
			server := httptest.NewServer(twoPageAPI(t))
			defer server.Close()
			a := newIdleAdapter(t, server.URL, &captureSink{})
			transport := a.httpClient.Transport
			failed := false
			a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
				if fault == "shutdown" && !failed {
					resp, err := transport.RoundTrip(r)
					failed = true
					a.doStop.Set()
					return resp, err
				}
				if r.URL.Query().Get("page") == "2" && !failed {
					failed = true
					if fault == "network" {
						return nil, errors.New("transient connection failure")
					}
					resp := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{")), Header: make(http.Header)}
					if fault == "read" {
						resp.Body = brokenPageBody{}
					}
					if fault == "status" {
						resp.StatusCode = http.StatusServiceUnavailable
						resp.Status = "503 Service Unavailable"
					}
					return resp, nil
				}
				return transport.RoundTrip(r)
			})
			since := time.Now().Add(-24 * time.Hour)
			first, _, err := a.makeOneRequest(since)
			require.Error(t, err)
			require.True(t, failed)
			require.Len(t, first, 100, "page 1 must survive the later failure")
			if fault == "shutdown" {
				require.ErrorIs(t, err, context.Canceled)
				a.doStop.Clear()
			}
			second, _, err := a.makeOneRequest(since)
			require.NoError(t, err)
			require.Len(t, second, 50, "only the remaining page should be new on retry")
			counts := map[string]int{}
			for _, item := range append(first, second...) {
				counts[item["id"].(string)]++
			}
			require.Len(t, counts, 150)
			for id, count := range counts {
				assert.Equal(t, 1, count, "ID %s", id)
			}
		})
	}
}

// Exercise the real poll loop: page 1 must ship before a failing page can
// recover, and must also drain when Close interrupts a paginated poll.
func TestFetchEventsShipsPartialResults(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprintf("shutdown=%t", shutdown), func(t *testing.T) {
			handler := twoPageAPI(t)
			var retryAllowed atomic.Bool
			pageStarted := make(chan struct{}, 1)
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("page") == "2" && !retryAllowed.Load() {
					if shutdown {
						pageStarted <- struct{}{}
						<-release
					}
					http.Error(w, "transient failure", http.StatusServiceUnavailable)
					return
				}
				handler.ServeHTTP(w, r)
			}))
			defer server.Close()
			if !shutdown {
				h := newPollHarness(t, server.Config.Handler)
				h.now = time.Now()
				h.beforeWait = func(step int) {
					if step == 2 {
						require.Equal(t, 100, h.sink.count(), "successful page must ship before retrying the failed page")
						retryAllowed.Store(true)
					}
				}
				h.run(3)
				require.Empty(t, h.fatal)
				require.Equal(t, 150, h.sink.count())
				counts := map[string]int{}
				for _, msg := range h.sink.snapshot() {
					counts[msg.JsonPayload["id"].(string)]++
				}
				require.Len(t, counts, 150)
				for id, count := range counts {
					assert.Equal(t, 1, count, "ID %s", id)
				}
				return
			}
			sink := &captureSink{}
			a, _ := startTestAdapter(t, server.URL, "test-api-key", sink)
			if shutdown {
				select {
				case <-pageStarted:
				case <-time.After(3 * time.Second):
					t.Fatal("second page was not requested")
				}
				closed := make(chan error, 1)
				go func() { closed <- a.Close() }()
				require.Eventually(t, a.doStop.IsSet, time.Second, time.Millisecond)
				close(release)
				select {
				case err := <-closed:
					require.NoError(t, err)
				case <-time.After(3 * time.Second):
					t.Fatal("Close did not drain partial results")
				}
				require.Equal(t, 100, sink.count())
				return
			}
		})
	}
}

// A full final legal page must be subdivided, never followed by forbidden
// page 101 or treated as proof that the time window is exhausted.
func TestPaginationWindowCeiling(t *testing.T) {
	for _, tc := range []struct {
		name      string
		total     int
		repeated  bool
		adjacent  bool
		transient bool
	}{
		{name: "exact_ceiling", total: 10000},
		{name: "over_ceiling", total: 12005},
		{name: "subdivision_failure", total: 12005, transient: true},
		{name: "same_millisecond_exact", total: 10000, repeated: true},
		{name: "same_millisecond_over", total: 10001, repeated: true},
		{name: "adjacent_milliseconds", total: 10001, adjacent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := newMockPandaDoc("test-api-key")
			base := time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond)
			if tc.repeated || tc.adjacent {
				base = time.Now().Add(-20 * time.Millisecond).UTC().Truncate(time.Millisecond)
			}
			entries := make([]utils.Dict, tc.total)
			for i := range entries {
				at := base.Add(time.Duration(i) * time.Millisecond)
				if tc.repeated {
					at = base
				}
				if tc.adjacent {
					at = base.Add(time.Duration(i%2) * time.Millisecond)
				}
				entries[i] = auditLogEntry(fmt.Sprintf("log-%d", i), at)
			}
			mock.addLogs(entries...)
			server := httptest.NewServer(mock)
			defer server.Close()
			opts := testClientOptions(t)
			opts.DebugLog = func(string) {}
			a, _, err := newPandaDocAdapter(context.Background(), PandaDocConfig{
				ClientOptions: opts, ApiKey: "test-api-key", URL: server.URL, PollInterval: time.Hour,
			}, &captureSink{})
			require.NoError(t, err)
			defer a.Close()
			firstWindow := ""
			failed := false
			if tc.transient {
				transport := a.httpClient.Transport
				a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
					window := r.URL.Query().Get("since") + "/" + r.URL.Query().Get("to")
					if firstWindow == "" {
						firstWindow = window
					}
					if window != firstWindow && r.URL.Query().Get("page") == "2" && !failed {
						failed = true
						return nil, errors.New("transient subdivision failure")
					}
					return transport.RoundTrip(r)
				})
			}
			items, _, err := a.makeOneRequest(base)
			if tc.transient {
				require.Error(t, err)
				require.True(t, failed)
				require.GreaterOrEqual(t, len(items), 10000, "keep the full parent window on child failure")
				retry, _, retryErr := a.makeOneRequest(base)
				require.NoError(t, retryErr)
				items = append(items, retry...)
			} else if tc.repeated {
				require.ErrorContains(t, err, "cannot subdivide below one millisecond")
				require.Len(t, items, 10000, "return accessible data even when the timestamp is saturated")
			} else {
				require.NoError(t, err)
			}
			want := tc.total
			if tc.repeated {
				want = 10000
			}
			require.Len(t, items, want)
			ids := map[string]int{}
			for _, item := range items {
				ids[item["id"].(string)]++
			}
			require.Len(t, ids, want)
			for id, count := range ids {
				require.Equal(t, 1, count, "ID %s", id)
			}
			requests := mock.requestsSnapshot()
			require.Greater(t, len(requests), 100, "a full last legal page must trigger subdivision")
			for _, r := range requests {
				require.LessOrEqual(t, r.page*r.count, 10000, "never request a forbidden page")
			}
			// A thirty-minute window needs at most 21 bisections to reach
			// milliseconds. A single saturated timestamp must terminate.
			require.Less(t, len(requests), 5000, "subdivision must be bounded")
		})
	}
}
