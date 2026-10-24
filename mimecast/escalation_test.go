package usp_mimecast

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContinuousFailureEscalatesAtThirtyMinutes(t *testing.T) {
	for _, endpoint := range []string{"/oauth/token", "/api/audit/get-audit-events"} {
		for _, extra := range []time.Duration{0, time.Second} {
			t.Run(fmt.Sprintf("%s/after=%s", endpoint, extra), func(t *testing.T) {
				mock := newMockMimecast("cid", "secret")
				handler := mock.handler(t)
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.URL.Path == endpoint {
						w.WriteHeader(503)
						return
					}
					handler(w, r)
				}))
				defer server.Close()
				opts, logs := recordingClientOptions(t)
				a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
				clock := usePollClock(a)
				origin := clock.current
				_, err := a.makeOneRequest()
				require.Error(t, err)
				assert.Zero(t, logs.errorCount())
				assert.Equal(t, 30*time.Second, a.retryAt.Sub(clock.current))
				clock.current = origin.Add(29 * time.Minute)
				_, err = a.makeOneRequest()
				require.Error(t, err)
				assert.Zero(t, logs.errorCount(), "transient errors must not become fatal before 30 minutes")
				assert.Equal(t, time.Minute, a.retryAt.Sub(clock.current), "preserve exponential backoff during the grace period")
				assert.Contains(t, logs.joinedWarnings(), "503")
				assert.Contains(t, logs.joinedWarnings(), "retrying")
				before := calls.Load()
				clock.current = origin.Add(30*time.Minute + extra)
				for i := 0; i < 3; i++ {
					_, err = a.makeOneRequest()
					require.Error(t, err)
				}
				assert.Equal(t, 1, logs.errorCount(), "escalation must be terminal and occur only once")
				assert.Contains(t, logs.joinedErrors(), "failed continuously for 30m0s")
				assert.Equal(t, before, calls.Load(), "do not request again at or after the failure deadline")
			})
		}
	}
}

func TestCompletePollIncludingEmptyResetsFailureClock(t *testing.T) {
	for _, withData := range []bool{false, true} {
		t.Run(fmt.Sprintf("with data=%v", withData), func(t *testing.T) {
			opts, logs := recordingClientOptions(t)
			mock, a := newIdleMock(t, func(c *MimecastConfig) { c.ClientOptions = opts })
			clock := usePollClock(a)
			origin := clock.current
			mock.setAuditStatus(503)
			_, err := a.makeOneRequest()
			require.Error(t, err)
			clock.current = origin.Add(29 * time.Minute)
			mock.setAuditStatus(0)
			if withData {
				mock.addEvent(fakeLogonEvent("recovered", clock.current.Add(-time.Minute)))
			}
			items, err := a.makeOneRequest()
			require.NoError(t, err)
			if withData {
				require.Len(t, items, 1)
			} else {
				require.Empty(t, items)
			}
			mock.setAuditStatus(503)
			clock.current = origin.Add(30 * time.Minute)
			_, err = a.makeOneRequest()
			require.Error(t, err)
			assert.Zero(t, logs.errorCount(), "a healthy complete poll must reset the old failure deadline")
			clock.current = origin.Add(59 * time.Minute)
			_, err = a.makeOneRequest()
			require.Error(t, err)
			assert.Zero(t, logs.errorCount())
			clock.current = origin.Add(60 * time.Minute)
			_, err = a.makeOneRequest()
			require.Error(t, err)
			assert.Equal(t, 1, logs.errorCount(), "the next uninterrupted failure gets its own 30-minute bound")
		})
	}
}

func TestPartialPollDoesNotResetFailureClock(t *testing.T) {
	for _, mode := range []string{"page two error", "fail array", "page cap", "cyclic token"} {
		t.Run(mode, func(t *testing.T) {
			mock := newMockMimecast("cid", "secret")
			handler := mock.handler(t)
			var partial atomic.Bool
			var calls atomic.Int32
			var pages atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path == "/oauth/token" {
					handler(w, r)
					return
				}
				if !partial.Load() {
					w.WriteHeader(503)
					return
				}
				n := pages.Add(1)
				if mode == "page two error" && n == 2 {
					w.WriteHeader(503)
					return
				}
				next := "next"
				var failures []FailureDetails
				switch mode {
				case "fail array":
					next = ""
					failures = []FailureDetails{{Errors: []ErrorDetail{{Code: "temporary", Retryable: true}}}}
				case "page cap":
					next = fmt.Sprintf("page-%d", n)
				case "cyclic token":
					if n%2 == 0 {
						next = "other"
					}
				}
				_ = json.NewEncoder(w).Encode(ApiResponse{Meta: MetaData{Pagination: Pagination{Next: next}}, Fail: failures,
					Data: []AuditLog{{ID: fmt.Sprintf("partial-%d", n), EventTime: "2026-01-01T00:00:00+0000"}}})
			}))
			defer server.Close()
			opts, logs := recordingClientOptions(t)
			a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
			clock := usePollClock(a)
			origin := clock.current
			_, err := a.makeOneRequest()
			require.Error(t, err)
			clock.current = origin.Add(29 * time.Minute)
			partial.Store(true)
			budget := boundAuditRequests(a, maxPagesPerPoll)
			items, err := a.makeOneRequest()
			require.Error(t, err)
			require.NotEmpty(t, items, "successful records do not make an incomplete poll healthy")
			assert.LessOrEqual(t, budget.Load(), int32(maxPagesPerPoll))
			assert.Zero(t, logs.errorCount())
			assert.True(t, a.lastComplete.IsZero())
			before := calls.Load()
			clock.current = origin.Add(30 * time.Minute)
			_, err = a.makeOneRequest()
			require.Error(t, err)
			assert.Equal(t, 1, logs.errorCount(), "partial success must retain the ORIGINAL failure deadline")
			assert.Equal(t, before, calls.Load())
		})
	}
}

func TestRetryHeadersCannotPostponeEscalation(t *testing.T) {
	for _, endpoint := range []string{"/oauth/token", "/api/audit/get-audit-events"} {
		for _, status := range []int{429, 503} {
			t.Run(fmt.Sprintf("%s/%d", endpoint, status), func(t *testing.T) {
				mock := newMockMimecast("cid", "secret")
				handler := mock.handler(t)
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.URL.Path == endpoint {
						w.Header().Set("Retry-After", "7200")
						w.Header().Set("X-RateLimit-Reset", "7200000")
						w.WriteHeader(status)
						return
					}
					handler(w, r)
				}))
				defer server.Close()
				opts, logs := recordingClientOptions(t)
				a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
				clock := usePollClock(a)
				origin := clock.current
				_, err := a.makeOneRequest()
				require.Error(t, err)
				assert.Equal(t, origin.Add(2*time.Hour), a.retryAt, "retain the server's request deadline")
				assert.Zero(t, logs.errorCount())
				before := calls.Load()
				_, err = a.makeOneRequest()
				require.Error(t, err)
				assert.Equal(t, origin.Add(30*time.Minute), clock.current, "wake for escalation instead of sleeping through a long server deadline")
				assert.Equal(t, []time.Duration{30 * time.Minute}, clock.waits)
				assert.Equal(t, 1, logs.errorCount())
				assert.Equal(t, before, calls.Load(), "escalation must not violate the server's request deadline")
			})
		}
	}
}

func TestInitial401RetryHeaderHasBoundedRefresh(t *testing.T) {
	mock := newMockMimecast("cid", "secret")
	handler := mock.handler(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/api/audit/get-audit-events" {
			w.Header().Set("Retry-After", "7200")
			w.WriteHeader(401)
			return
		}
		handler(w, r)
	}))
	defer server.Close()
	opts, logs := recordingClientOptions(t)
	a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
	clock := usePollClock(a)
	origin := clock.current
	_, err := a.makeOneRequest()
	require.Error(t, err)
	assert.Equal(t, origin.Add(30*time.Minute), clock.current)
	assert.Equal(t, []time.Duration{30 * time.Minute}, clock.waits)
	assert.Equal(t, 1, logs.errorCount())
	assert.Contains(t, logs.joinedWarnings(), "authentication refresh delayed")
	assert.Equal(t, int32(2), calls.Load(), "do not reauthenticate early or wait indefinitely for the first retry")
}

func TestSenderCadenceHonorsFailureDeadline(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	mock, a := newIdleMock(t, func(c *MimecastConfig) { c.ClientOptions = opts })
	mock.setAuditStatus(503)
	clock := usePollClock(a)
	a.pollInterval = 30 * time.Second
	waits := 0
	var lastWait time.Duration
	a.wait = func(d time.Duration) bool {
		waits++
		if waits == 2 {
			// Advance an active sender to just before its deadline. A subsequent
			// ordinary 30s cadence must be shortened to the last second.
			clock.current = a.failureSince.Add(29*time.Minute + 59*time.Second)
			return false
		}
		lastWait = d
		clock.current = clock.current.Add(d)
		return a.doStop.IsSet()
	}
	a.wgSenders.Add(1)
	done := make(chan struct{})
	go func() { defer close(done); a.fetchEvents() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		a.doStop.Set()
		<-done
		t.Fatal("sender did not use the interruptible bounded cadence")
	}
	assert.Equal(t, time.Second, lastWait)
	assert.Equal(t, a.failureSince.Add(30*time.Minute), clock.current)
	assert.Equal(t, 1, logs.errorCount())
	assert.Equal(t, 2, mock.auditCount(), "escalate without issuing a request at the deadline")
}

func TestActiveRequestsAreBoundedByFailureDeadline(t *testing.T) {
	for _, endpoint := range []string{"/oauth/token", "/api/audit/get-audit-events"} {
		t.Run(endpoint, func(t *testing.T) {
			mock := newMockMimecast("cid", "secret")
			handler := mock.handler(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == endpoint {
					w.WriteHeader(503)
					return
				}
				handler(w, r)
			}))
			defer server.Close()
			opts, logs := recordingClientOptions(t)
			a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
			clock := usePollClock(a)
			origin := clock.current
			_, err := a.makeOneRequest()
			require.Error(t, err)
			clock.current = origin.Add(29*time.Minute + 59*time.Second)
			inner := a.httpClient.Transport
			a.httpClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == endpoint {
					deadline, ok := r.Context().Deadline()
					assert.True(t, ok, "active requests must carry the remaining failure deadline")
					if ok {
						assert.InDelta(t, 1, time.Until(deadline).Seconds(), 0.2)
					}
					clock.current = origin.Add(30 * time.Minute)
					return nil, context.DeadlineExceeded
				}
				return inner.RoundTrip(r)
			})
			_, err = a.makeOneRequest()
			require.Error(t, err)
			assert.Equal(t, 1, logs.errorCount())
			assert.Contains(t, logs.joinedErrors(), "failed continuously")
		})
	}
}

func TestLateSuccessfulPollCannotEraseFailureDeadline(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	mock, a := newIdleMock(t, func(c *MimecastConfig) { c.ClientOptions = opts })
	clock := usePollClock(a)
	origin := clock.current
	mock.setAuditStatus(503)
	_, err := a.makeOneRequest()
	require.Error(t, err)
	clock.current = origin.Add(29*time.Minute + 59*time.Second)
	mock.setAuditStatus(0)
	inner := a.httpClient.Transport
	a.httpClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := inner.RoundTrip(r)
		if r.URL.Path == "/api/audit/get-audit-events" {
			clock.current = origin.Add(30 * time.Minute)
		}
		return response, err
	})
	_, err = a.makeOneRequest()
	require.Error(t, err, "a success finishing after the hard deadline is too late to reset it")
	assert.Equal(t, 1, logs.errorCount())
	assert.True(t, a.lastComplete.IsZero())
}

func TestTokenFailuresShareContinuousFailureClock(t *testing.T) {
	mock := newMockMimecast("cid", "secret")
	handler := mock.handler(t)
	var failToken atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/audit/get-audit-events" || failToken.Load() {
			w.WriteHeader(503)
			return
		}
		handler(w, r)
	}))
	defer server.Close()
	opts, logs := recordingClientOptions(t)
	a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
	clock := usePollClock(a)
	origin := clock.current
	_, err := a.makeOneRequest()
	require.Error(t, err)
	clock.current = origin.Add(29 * time.Minute)
	failToken.Store(true)
	a.invalidateAuthToken()
	_, err = a.makeOneRequest()
	require.Error(t, err)
	assert.Zero(t, logs.errorCount())
	assert.Contains(t, logs.joinedWarnings(), "mimecast auth")
	clock.current = origin.Add(30 * time.Minute)
	_, err = a.makeOneRequest()
	require.Error(t, err)
	assert.Equal(t, 1, logs.errorCount(), "a token endpoint failure must not get a fresh grace period")
}

func TestShutdownInterruptsEscalationWait(t *testing.T) {
	mock := newMockMimecast("cid", "secret")
	handler := mock.handler(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/audit/get-audit-events" {
			w.Header().Set("Retry-After", "7200")
			w.WriteHeader(429)
			return
		}
		handler(w, r)
	}))
	defer server.Close()
	opts, logs := recordingClientOptions(t)
	a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
	defer a.Close()
	productionWait := a.wait
	usePollClock(a)
	a.pollInterval = time.Millisecond
	waiting := make(chan time.Duration, 1)
	a.wait = func(d time.Duration) bool {
		if d > time.Second {
			waiting <- d
		}
		return productionWait(d)
	}
	a.wgSenders.Add(1)
	go a.fetchEvents()
	select {
	case delay := <-waiting:
		assert.Equal(t, 30*time.Minute, delay)
	case <-time.After(2 * time.Second):
		t.Fatal("sender did not reach the bounded escalation wait")
	}
	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		// Unblock a deliberately broken Close during mutation testing so failure
		// is an assertion, never the package timeout or an abandoned sender.
		a.doStop.Set()
		a.cancel()
		<-done
		t.Fatal("Close did not interrupt the escalation wait")
	}
	assert.Zero(t, logs.errorCount(), "shutdown is not sustained vendor failure")
}

func TestPermanentFailuresAreImmediatelyFatal(t *testing.T) {
	for _, mode := range []string{"HTTP 400", "HTTP 404", "HTTP 422", "nonretryable fail array"} {
		t.Run(mode, func(t *testing.T) {
			mock := newMockMimecast("cid", "secret")
			handler := mock.handler(t)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path == "/oauth/token" {
					handler(w, r)
					return
				}
				if mode == "nonretryable fail array" {
					_, _ = io.WriteString(w, `{"meta":{"status":200},"data":[],"fail":[{"errors":[{"code":"invalid_request","retryable":false}]}]}`)
					return
				}
				var status int
				_, _ = fmt.Sscanf(mode, "HTTP %d", &status)
				w.WriteHeader(status)
			}))
			defer server.Close()
			opts, logs := recordingClientOptions(t)
			a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
			clock := usePollClock(a)
			origin := clock.current
			_, err := a.makeOneRequest()
			require.Error(t, err)
			assert.Equal(t, 1, logs.errorCount())
			assert.Equal(t, origin, clock.current, "permanent failures have no grace period")
			before := calls.Load()
			for i := 0; i < 3; i++ {
				_, err = a.makeOneRequest()
				require.ErrorIs(t, err, context.Canceled)
			}
			assert.Equal(t, 1, logs.errorCount())
			assert.Equal(t, before, calls.Load())
		})
	}
}
