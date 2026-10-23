package usp_zendesk

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
)

func TestContinuousFailureEscalatesAtThirtyMinutes(t *testing.T) {
	for _, fault := range []string{"429", "503", "network"} {
		t.Run(fault, func(t *testing.T) {
			h := newPollHarness(t, twoPageAPI(t))
			var failedAt time.Time
			requests := []time.Time{}
			h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
				require.Empty(t, h.fatal, "retryable failures must warn until the failure budget is exhausted")
				if failedAt.IsZero() {
					failedAt = h.now
				}
				requests = append(requests, h.now)
				if fault == "network" {
					return nil, &net.DNSError{Err: "temporary timeout", IsTimeout: true}
				}
				status := 429
				if fault == "503" {
					status = 503
				}
				return pollResponse(status, ""), nil
			})
			h.run(30)
			require.Len(t, h.fatal, 1)
			require.Contains(t, h.fatal[0].Error(), "failed continuously for at least 30 minutes")
			require.Equal(t, failedAt.Add(pollFailureLimit), h.now)
			require.Len(t, h.warnings, len(requests), "each transient failure must warn before escalation")
			require.True(t, requests[len(requests)-1].Before(failedAt.Add(pollFailureLimit)), "do not send another request at the fatal deadline")
			require.GreaterOrEqual(t, len(h.delays), 4)
			for _, delay := range h.delays {
				require.Greater(t, delay, time.Duration(0))
				require.LessOrEqual(t, delay, 5*time.Minute)
			}
			require.True(t, h.a.doStop.IsSet(), "one fatal callback must terminate the loop")
		})
	}
}

func TestRetryAfterCannotPostponeEscalation(t *testing.T) {
	for _, kind := range []string{"seconds", "http_date", "exact_boundary", "large"} {
		t.Run(kind, func(t *testing.T) {
			h := newPollHarness(t, twoPageAPI(t))
			var failedAt time.Time
			requests := 0
			h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
				failedAt = h.now
				requests++
				header := "3600"
				if kind == "http_date" {
					header = h.now.Add(time.Hour).Format(http.TimeFormat)
				}
				if kind == "exact_boundary" {
					header = "1800"
				}
				if kind == "large" {
					header = "9223372036854775807"
				}
				return pollResponse(429, header), nil
			})
			h.run(3)
			require.Len(t, h.fatal, 1)
			require.Len(t, h.warnings, 1)
			require.Equal(t, 1, requests, "wake to escalate without making a request before Retry-After")
			require.Equal(t, failedAt.Add(pollFailureLimit), h.now)
			require.Equal(t, []time.Duration{30 * time.Second, 30 * time.Minute}, h.delays)
		})
	}
}

func TestCompletePollResetsFailureClock(t *testing.T) {
	for _, withData := range []bool{false, true} {
		t.Run(fmt.Sprintf("with_data=%t", withData), func(t *testing.T) {
			mock := newMockZendesk(mockEmail, mockToken)
			h := newPollHarness(t, mock.handler(t))
			if withData {
				mock.addLog(auditLogFixture("90001", h.now.Add(-time.Minute), "update"))
			}
			transport := h.a.httpClient.Transport
			if transport == nil {
				transport = http.DefaultTransport
			}
			requests := 0
			var firstFailure, newFailure time.Time
			h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
				requests++
				require.Empty(t, h.fatal)
				switch requests {
				case 1:
					firstFailure = h.now
					return pollResponse(503, "1770"), nil
				case 2:
					return transport.RoundTrip(r) // complete success, even with no data
				case 3:
					newFailure = h.now
					return pollResponse(503, "3600"), nil
				default:
					t.Fatalf("unexpected request %d", requests)
					return nil, nil
				}
			})
			h.run(6)
			require.Len(t, h.fatal, 1)
			require.Equal(t, 3, requests, "the complete successful poll must reset the first failure clock")
			require.Equal(t, firstFailure.Add(pollFailureLimit), newFailure)
			require.Equal(t, newFailure.Add(pollFailureLimit), h.now)
			require.Len(t, h.warnings, 2)
			want := 0
			if withData {
				want = 1
			}
			require.Equal(t, want, h.sink.count())
			require.Equal(t, []time.Duration{30 * time.Second, 29*time.Minute + 30*time.Second, 30 * time.Second, 30 * time.Minute}, h.delays)
		})
	}
}

func TestPartialPollDoesNotResetFailureClock(t *testing.T) {
	mock := newMockZendesk(mockEmail, mockToken)
	h := newPollHarness(t, mock.handler(t))
	for i := 0; i < 150; i++ {
		mock.addLog(auditLogFixture(fmt.Sprint(i), h.now.Add(-time.Minute), "update"))
	}
	transport := h.a.httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	roots := 0
	var failedAt time.Time
	h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
		if firstPage(r) {
			roots++
			return transport.RoundTrip(r)
		}
		if failedAt.IsZero() {
			failedAt = h.now
		}
		return pollResponse(503, "600"), nil
	})
	onError := h.a.conf.ClientOptions.OnError
	h.a.conf.ClientOptions.OnError = func(err error) {
		require.Equal(t, 100, h.sink.count(), "ship partial pages before fatal escalation")
		onError(err)
	}
	h.run(10)
	require.Len(t, h.fatal, 1)
	require.Equal(t, failedAt.Add(pollFailureLimit), h.now, "partial pages are not complete recovery")
	require.Equal(t, 3, roots)
	require.Len(t, h.warnings, 3)
	require.Equal(t, 100, h.sink.count())
}

func TestFailureClockStartsWhenPollFails(t *testing.T) {
	h := newPollHarness(t, twoPageAPI(t))
	var failedAt time.Time
	requests := 0
	h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
		requests++
		h.now = h.now.Add(5 * time.Minute)
		failedAt = h.now
		return pollResponse(503, "3600"), nil
	})
	h.run(3)
	require.Len(t, h.fatal, 1)
	require.Equal(t, 1, requests)
	require.Equal(t, failedAt.Add(pollFailureLimit), h.now, "start the clock at the first failed complete poll, not at launch")
}

func TestRetryRequestUsesRemainingFailureBudget(t *testing.T) {
	h := newPollHarness(t, twoPageAPI(t))
	var failedAt time.Time
	requests := 0
	h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
		requests++
		if requests == 1 {
			failedAt = h.now
			return pollResponse(503, "1799"), nil
		}
		deadline, ok := r.Context().Deadline()
		require.True(t, ok, "an in-flight retry must not outlive the failure deadline")
		remaining := time.Until(deadline)
		require.Greater(t, remaining, time.Duration(0))
		require.LessOrEqual(t, remaining, time.Second)
		h.now = h.now.Add(2 * time.Second)
		return nil, &net.DNSError{Err: "temporary timeout", IsTimeout: true}
	})
	h.run(4)
	require.Len(t, h.fatal, 1)
	require.Equal(t, 2, requests)
	require.Equal(t, failedAt.Add(pollFailureLimit+time.Second), h.now)
}

type failureBudgetSink struct {
	captureSink
	ship func(time.Duration) error
}

func (s *failureBudgetSink) Ship(_ *protocol.DataMessage, timeout time.Duration) error {
	return s.ship(timeout)
}

func TestPartialShippingUsesRemainingFailureBudget(t *testing.T) {
	mock := newMockZendesk(mockEmail, mockToken)
	h := newPollHarness(t, mock.handler(t))
	for i := 0; i < 150; i++ {
		mock.addLog(auditLogFixture(fmt.Sprint(i), h.now.Add(-time.Minute), "update"))
	}
	transport := h.a.httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	var failedAt time.Time
	h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
		if firstPage(r) {
			return transport.RoundTrip(r)
		}
		failedAt = h.now
		return pollResponse(503, ""), nil
	})
	timeouts := []time.Duration{}
	h.a.uspClient = &failureBudgetSink{ship: func(timeout time.Duration) error {
		require.Empty(t, h.fatal)
		timeouts = append(timeouts, timeout)
		h.now = h.now.Add(timeout)
		return uspclient.ErrorBufferFull
	}}
	h.run(3)
	require.Len(t, h.fatal, 1)
	require.Contains(t, h.fatal[0].Error(), "failed continuously for at least 30 minutes")
	require.Equal(t, failedAt.Add(pollFailureLimit), h.now, "back-pressure must not postpone escalation for an hour")
	require.Equal(t, []time.Duration{10 * time.Second, 29*time.Minute + 50*time.Second}, timeouts)
}

func TestLateSuccessfulPollCannotEraseFailureDeadline(t *testing.T) {
	h := newPollHarness(t, twoPageAPI(t))
	transport := h.a.httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	requests := 0
	var failedAt time.Time
	h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
		requests++
		if requests == 1 {
			failedAt = h.now
			return pollResponse(503, "1799"), nil
		}
		h.now = h.now.Add(2 * time.Second)
		return transport.RoundTrip(r)
	})
	h.run(5)
	require.Len(t, h.fatal, 1)
	require.Contains(t, h.fatal[0].Error(), "failed continuously for at least 30 minutes")
	require.Equal(t, 2, requests)
	require.Equal(t, failedAt.Add(pollFailureLimit+time.Second), h.now)
}

func TestEscalationStopsWithoutCallbackSideEffects(t *testing.T) {
	h := newPollHarness(t, twoPageAPI(t))
	h.a.conf.ClientOptions.OnError = func(err error) { h.fatal = append(h.fatal, err) }
	requests := 0
	h.a.httpClient.Transport = pageRoundTripper(func(*http.Request) (*http.Response, error) {
		requests++
		return pollResponse(503, "3600"), nil
	})
	h.run(10)
	require.Len(t, h.fatal, 1, "the loop itself must terminate after escalation")
	require.Equal(t, 1, requests)
}

func TestShutdownInterruptsInFlightRetry(t *testing.T) {
	var requests atomic.Int32
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	h := newPollHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			http.Error(w, "temporary", 503)
			return
		}
		close(started)
		select {
		case <-r.Context().Done():
			close(canceled)
		case <-release:
			http.Error(w, "released", 503)
		}
	}))
	h.a.ctx, h.a.cancel = context.WithCancel(context.Background())
	h.a.wait = func(delay time.Duration) bool { h.now = h.now.Add(delay); return h.a.doStop.IsSet() }
	h.a.wgSenders.Add(1)
	go h.a.fetchEvents()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("retry did not start")
	}
	done := make(chan error, 1)
	go func() { done <- h.a.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel the in-flight retry")
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("retry request context was not canceled")
	}
	require.Empty(t, h.fatal)
	require.EqualValues(t, 2, requests.Load())
}
