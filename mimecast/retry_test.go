package usp_mimecast

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Manual polling owns this clock; the background sender stays parked. No wall
// clock sleep is needed to exercise outages spanning hours or long Retry-After.
type pollClock struct {
	current time.Time
	waits   []time.Duration
}

func usePollClock(a *MimecastAdapter) *pollClock {
	c := &pollClock{current: time.Now().UTC().Truncate(time.Second).Add(-72 * time.Hour)}
	a.now = func() time.Time { return c.current }
	a.wait = func(d time.Duration) bool {
		c.waits = append(c.waits, d)
		if a.doStop.IsSet() {
			return true
		}
		c.current = c.current.Add(d)
		return false
	}
	return c
}

func TestRecoverableHTTPFailuresPreserveSession(t *testing.T) {
	for _, endpoint := range []string{"/oauth/token", "/api/audit/get-audit-events"} {
		for _, status := range []int{408, 429, 500, 502, 503, 504} {
			t.Run(fmt.Sprintf("%s/%d", endpoint, status), func(t *testing.T) {
				mock := newMockMimecast("cid", "secret")
				handler := mock.handler(t)
				var failure atomic.Bool
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == endpoint && failure.Load() {
						w.WriteHeader(status)
						return
					}
					handler(w, r)
				}))
				defer server.Close()
				opts, logs := recordingClientOptions(t)
				a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
				clock := usePollClock(a)
				mock.addEvent(fakeLogonEvent("already-delivered", clock.current.Add(-time.Minute)))
				first, err := a.makeOneRequest()
				require.NoError(t, err)
				require.Len(t, first, 1)
				checkpoint := a.lastComplete
				clock.current = clock.current.Add(time.Minute)
				failure.Store(true)
				if endpoint == "/oauth/token" {
					a.invalidateAuthToken()
				}
				_, err = a.makeOneRequest()
				require.Error(t, err)
				assert.Empty(t, logs.joinedErrors(), "recoverable errors must not terminate a consumer session")
				assert.NotEmpty(t, logs.joinedWarnings())
				assert.False(t, a.doStop.IsSet())
				assert.Equal(t, checkpoint, a.lastComplete)
				assert.Contains(t, a.dedupe, "already-delivered")
				failure.Store(false)
				items, err := a.makeOneRequest()
				require.NoError(t, err)
				assert.Empty(t, items, "recovery must keep dedupe state")
				assert.Equal(t, []time.Duration{defaultPollInterval}, clock.waits)
			})
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestNetworkAndTimeoutFailuresAreRecoverable(t *testing.T) {
	for _, endpoint := range []string{"/oauth/token", "/api/audit/get-audit-events"} {
		for _, failure := range []error{io.ErrUnexpectedEOF, &net.DNSError{IsTimeout: true, Err: "test timeout"}} {
			t.Run(fmt.Sprintf("%s/%v", endpoint, failure), func(t *testing.T) {
				opts, logs := recordingClientOptions(t)
				_, a := newIdleMock(t, func(c *MimecastConfig) { c.ClientOptions = opts })
				usePollClock(a)
				transport := a.httpClient.Transport
				a.httpClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == endpoint {
						return nil, failure
					}
					return transport.RoundTrip(r)
				})
				_, err := a.makeOneRequest()
				require.Error(t, err)
				assert.Empty(t, logs.joinedErrors())
				assert.NotEmpty(t, logs.joinedWarnings())
				assert.False(t, a.doStop.IsSet())
				assert.Equal(t, defaultPollInterval, a.retryBackoff)
			})
		}
	}
}

func TestRetryHeadersDelayEveryRequest(t *testing.T) {
	for _, endpoint := range []string{"/oauth/token", "/api/audit/get-audit-events"} {
		for _, tc := range []struct {
			name, after, reset string
			delay              time.Duration
		}{
			{"seconds", "90", "", 90 * time.Second},
			{"date", "date", "", 2 * time.Minute},
			{"milliseconds", "", "75000", 75 * time.Second},
			{"later reset", "90", "120000", 2 * time.Minute},
			{"later retry-after", "120", "75000", 2 * time.Minute},
			{"malformed", "invalid", "invalid", defaultPollInterval},
			{"negative", "-3", "-10", defaultPollInterval},
		} {
			t.Run(endpoint+"/"+tc.name, func(t *testing.T) {
				mock := newMockMimecast("cid", "secret")
				handler := mock.handler(t)
				var calls atomic.Int32
				var fail atomic.Bool
				fail.Store(true)
				// Fixed before starting the server; it matches usePollClock's origin.
				now := time.Now().UTC().Truncate(time.Second).Add(-72 * time.Hour)
				after := tc.after
				if after == "date" {
					after = now.Add(tc.delay).Format(http.TimeFormat)
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.URL.Path == endpoint && fail.Load() {
						w.Header().Set("Retry-After", after)
						w.Header().Set("X-RateLimit-Reset", tc.reset)
						w.WriteHeader(http.StatusTooManyRequests)
						return
					}
					handler(w, r)
				}))
				defer server.Close()
				a := newIdleAdapter(t, server.URL, &captureSink{})
				clock := usePollClock(a)
				clock.current = now
				_, err := a.makeOneRequest()
				require.Error(t, err)
				require.Equal(t, now.Add(tc.delay), a.retryAt)
				before := calls.Load()
				fail.Store(false)
				// A premature/spurious wake must re-check the deadline. No token or
				// audit request may be issued while either server deadline is pending.
				a.wait = func(d time.Duration) bool {
					assert.Equal(t, before, calls.Load(), "request issued before retry deadline")
					clock.waits = append(clock.waits, d)
					clock.current = clock.current.Add(d/2 + time.Nanosecond)
					return false
				}
				_, err = a.makeOneRequest()
				require.NoError(t, err)
				assert.False(t, clock.current.Before(now.Add(tc.delay)))
				assert.Greater(t, calls.Load(), before)
				assert.NotEmpty(t, clock.waits)
			})
		}
	}
}

func TestBackoffIsBoundedAndResetsAfterCompletePoll(t *testing.T) {
	mock, a := newIdleMock(t)
	clock := usePollClock(a)
	mock.setAuditStatus(503)
	want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 5 * time.Minute, 5 * time.Minute}
	for _, delay := range want {
		_, err := a.makeOneRequest()
		require.Error(t, err)
		assert.Equal(t, delay, a.retryAt.Sub(clock.current))
	}
	mock.setAuditStatus(0)
	_, err := a.makeOneRequest()
	require.NoError(t, err)
	mock.setAuditStatus(503)
	_, err = a.makeOneRequest()
	require.Error(t, err)
	assert.Equal(t, defaultPollInterval, a.retryAt.Sub(clock.current))
}

func TestPersistentAuthenticationFailureIsFatal(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		status   int
		tokens   int
	}{
		{"/api/audit/get-audit-events", 401, 2},
		{"/api/audit/get-audit-events", 403, 1},
		{"/oauth/token", 400, 0},
		{"/oauth/token", 401, 0},
		{"/oauth/token", 403, 0},
	} {
		t.Run(fmt.Sprintf("%s/%d", tc.endpoint, tc.status), func(t *testing.T) {
			mock := newMockMimecast("cid", "secret")
			handler := mock.handler(t)
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == tc.endpoint {
					if attempts.Add(1) > 2 {
						w.WriteHeader(503)
						return
					}
					w.WriteHeader(tc.status)
					return
				}
				handler(w, r)
			}))
			defer server.Close()
			opts, logs := recordingClientOptions(t)
			a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
			usePollClock(a)
			_, err := a.makeOneRequest()
			require.Error(t, err)
			assert.NotEmpty(t, logs.joinedErrors())
			assert.True(t, a.doStop.IsSet())
			assert.Equal(t, tc.tokens, mock.tokenCount())
			_, err = a.makeOneRequest()
			assert.ErrorIs(t, err, context.Canceled)
			want := int32(1)
			if tc.status == 401 && tc.tokens == 2 {
				want = 2
			}
			assert.Equal(t, want, attempts.Load(), "fatal authentication failure must not loop")
		})
	}
}

func TestPollingGapRecoveryPreservesRangeAndDedupe(t *testing.T) {
	mock, a := newIdleMock(t)
	clock := usePollClock(a)
	origin := clock.current
	mock.addEvent(fakeLogonEvent("baseline", origin.Add(-time.Minute)))
	first, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Equal(t, []string{"baseline"}, itemIDs(first))
	mock.addEvent(fakeLogonEvent("during-outage", origin.Add(20*time.Minute)))
	mock.setAuditStatus(503)
	clock.current = origin.Add(2 * time.Hour)
	_, err = a.makeOneRequest()
	require.Error(t, err)
	// A polling gap can exceed the overlap; once failure is observed, recovery
	// must happen within the bounded transient grace period.
	clock.current = origin.Add(2*time.Hour + 10*time.Minute)
	_, err = a.makeOneRequest()
	require.Error(t, err)
	assert.Contains(t, a.dedupe, "baseline", "an unread retry range still needs its dedupe entries")
	mock.setAuditStatus(0)
	recovered, err := a.makeOneRequest()
	require.NoError(t, err)
	recoveryEnd := clock.current
	assert.Equal(t, []string{"during-outage"}, itemIDs(recovered))
	windows := mock.seenWindows()
	require.Len(t, windows, 2)
	assert.Equal(t, origin.Add(-defaultOverlapPeriod), windows[1].start.UTC())
	assert.Equal(t, recoveryEnd, windows[1].end.UTC())
	// The next COMPLETE window must move forward from the recovery END; using
	// its old start forever would keep rereading an ever-growing history.
	clock.current = clock.current.Add(time.Minute)
	_, err = a.makeOneRequest()
	require.NoError(t, err)
	windows = mock.seenWindows()
	assert.Equal(t, recoveryEnd.Add(-defaultOverlapPeriod), windows[2].start.UTC())
	assert.Equal(t, recoveryEnd.Add(time.Minute), a.lastComplete)
}

func TestColdFailurePreservesInitialRangeAndBoundsRecovery(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	mock, a := newIdleMock(t, func(c *MimecastConfig) { c.ClientOptions = opts })
	clock := usePollClock(a)
	origin := clock.current
	mock.setAuditStatus(503)
	_, err := a.makeOneRequest()
	require.Error(t, err)
	clock.current = origin.Add(20 * time.Minute)
	mock.setAuditStatus(0)
	_, err = a.makeOneRequest()
	require.NoError(t, err)
	assert.Equal(t, origin.Add(-defaultOverlapPeriod), mock.seenWindows()[0].start.UTC())
	mock.setAuditStatus(503)
	clock.current = origin.Add(28 * time.Hour)
	_, err = a.makeOneRequest()
	require.Error(t, err)
	clock.current = origin.Add(28*time.Hour + 10*time.Minute)
	mock.setAuditStatus(0)
	mock.addEvent(fakeLogonEvent("too-old", origin.Add(time.Hour)))
	mock.addEvent(fakeLogonEvent("recoverable", origin.Add(5*time.Hour)))
	items, err := a.makeOneRequest()
	require.NoError(t, err)
	assert.Equal(t, []string{"recoverable"}, itemIDs(items))
	windows := mock.seenWindows()
	assert.Equal(t, clock.current.Add(-24*time.Hour), windows[len(windows)-1].start.UTC())
	assert.Contains(t, logs.joinedWarnings(), "truncated")
	assert.Contains(t, logs.joinedWarnings(), "24h")
}

func TestBaselineSuppressesHistoricalLagWarning(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	mock, a := newIdleMock(t, func(c *MimecastConfig) { c.ClientOptions = opts })
	clock := usePollClock(a)
	mock.addEvent(fakeLogonEvent("historical", clock.current.Add(-29*time.Minute)))
	items, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.NotContains(t, logs.joinedWarnings(), "overlap_period")
	mock.addEvent(fakeLogonEvent("newly-visible-old", clock.current.Add(-28*time.Minute)))
	items, err = a.makeOneRequest()
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Contains(t, logs.joinedWarnings(), "overlap_period")
}

func TestOutOfRangeRecordsDoNotLoopAndDedupeExpires(t *testing.T) {
	mock, a := newIdleMock(t)
	clock := usePollClock(a)
	origin := clock.current
	mock.addEventUnfiltered(fakeLogonEvent("outside-range", origin.Add(-48*time.Hour)))
	// Future timestamps must not pin a dedupe entry forever either.
	mock.addEventUnfiltered(fakeLogonEvent("future", origin.Add(365*24*time.Hour)))
	items, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Len(t, items, 2)
	for i := 0; i < 4; i++ {
		clock.current = clock.current.Add(20 * time.Minute)
		items, err = a.makeOneRequest()
		require.NoError(t, err)
		assert.Empty(t, items, "repeated out-of-range records must remain deduped")
	}
	mock.mu.Lock()
	mock.unfilteredEvents = nil
	mock.mu.Unlock()
	clock.current = clock.current.Add(2 * time.Hour)
	_, err = a.makeOneRequest()
	require.NoError(t, err)
	// First success closes the old retry range; the following poll can evict.
	clock.current = clock.current.Add(time.Minute)
	_, err = a.makeOneRequest()
	require.NoError(t, err)
	assert.Empty(t, a.dedupe, "unused entries must be bounded even with future timestamps")
}

func TestIncompletePaginationDoesNotAdvanceCheckpoint(t *testing.T) {
	for _, mode := range []string{"stuck", "cycle", "cap", "fail"} {
		t.Run(mode, func(t *testing.T) {
			mock := newMockMimecast("cid", "secret")
			handler := mock.handler(t)
			var incomplete atomic.Bool
			var pages atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/oauth/token" || !incomplete.Load() {
					handler(w, r)
					return
				}
				n := pages.Add(1)
				next := "a"
				var fail []FailureDetails
				switch mode {
				case "cycle":
					if n%2 == 0 {
						next = "b"
					}
				case "cap":
					next = fmt.Sprintf("page-%d", n)
				case "fail":
					next = ""
					fail = []FailureDetails{{Errors: []ErrorDetail{{Code: "temporary", Retryable: true}}}}
				}
				_ = json.NewEncoder(w).Encode(ApiResponse{Meta: MetaData{Pagination: Pagination{Next: next}}, Fail: fail,
					Data: []AuditLog{{ID: fmt.Sprintf("page-%d", n), EventTime: time.Now().Format(mockTimeLayout)}}})
			}))
			defer server.Close()
			a := newIdleAdapter(t, server.URL, &captureSink{})
			clock := usePollClock(a)
			_, err := a.makeOneRequest()
			require.NoError(t, err)
			checkpoint := a.lastComplete
			transport := a.httpClient.Transport
			calls := boundAuditRequests(a, maxPagesPerPoll)
			incomplete.Store(true)
			clock.current = clock.current.Add(2 * time.Hour)
			items, err := a.makeOneRequest()
			require.Error(t, err)
			assert.NotEmpty(t, items, "successfully read records must survive an incomplete poll")
			assert.Equal(t, checkpoint, a.lastComplete)
			if mode == "cap" {
				assert.Equal(t, int32(maxPagesPerPoll), calls.Load(), "cap must stop before the transport budget")
			}
			a.httpClient.Transport = transport
			if mode == "cycle" {
				assert.Equal(t, int32(3), pages.Load(), "detect a cycle without exhausting the cap")
			}
			incomplete.Store(false)
			_, err = a.makeOneRequest()
			require.NoError(t, err)
			windows := mock.seenWindows()
			assert.Equal(t, checkpoint.Add(-defaultOverlapPeriod), windows[len(windows)-1].start.UTC())
		})
	}
}

func TestPartialPagesShipAndStopInterruptsRetryWait(t *testing.T) {
	mock := newMockMimecast("client-id-test", "client-secret-test")
	for i := 0; i < 120; i++ {
		mock.addEvent(fakeLogonEvent(fmt.Sprintf("event-%d", i), time.Now().Add(-time.Minute)))
	}
	mock.failNextAuditRequests(0, 503, 503)
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()
	sink := &captureSink{}
	a := startTestAdapter(t, server.URL, sink, "client-id-test", "client-secret-test")
	defer a.Close()
	require.Eventually(t, func() bool { return sink.count() == 100 }, 5*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return mock.auditCount() > 2 }, 100*time.Millisecond, 10*time.Millisecond, "retry must respect its deadline")
	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not interrupt retry wait")
	}
	assert.Equal(t, 2, mock.auditCount(), "stop must not issue another request")
	assert.Len(t, sink.snapshot(), 100, "successful partial page must ship before waiting")
}

func TestReauthenticationTransientFailureKeepsPartialPage(t *testing.T) {
	mock := newMockMimecast("cid", "secret")
	handler := mock.handler(t)
	var auditCalls atomic.Int32
	var tokenCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" && tokenCalls.Add(1) == 2 {
			w.WriteHeader(503)
			return
		}
		if r.URL.Path == "/api/audit/get-audit-events" && auditCalls.Add(1) == 2 {
			w.WriteHeader(401)
			return
		}
		handler(w, r)
	}))
	defer server.Close()
	opts, logs := recordingClientOptions(t)
	a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
	clock := usePollClock(a)
	for i := 0; i < 120; i++ {
		mock.addEvent(fakeLogonEvent(fmt.Sprintf("event-%d", i), clock.current.Add(-time.Minute)))
	}
	partial, err := a.makeOneRequest()
	require.Error(t, err)
	require.Len(t, partial, 100)
	assert.Empty(t, logs.joinedErrors(), "a token refresh outage must preserve the session")
	assert.False(t, a.doStop.IsSet())
	assert.True(t, a.lastComplete.IsZero())
	clock.current = clock.current.Add(20 * time.Minute)
	rest, err := a.makeOneRequest()
	require.NoError(t, err)
	assert.Len(t, rest, 20)
	all := append(itemIDs(partial), itemIDs(rest)...)
	assert.Len(t, dedupeStrings(all), 120)
}

func TestCloseCancelsActiveHTTPRequest(t *testing.T) {
	for _, endpoint := range []string{"/oauth/token", "/api/audit/get-audit-events"} {
		t.Run(endpoint, func(t *testing.T) {
			mock := newMockMimecast("client-id-test", "client-secret-test")
			handler := mock.handler(t)
			started := make(chan struct{})
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == endpoint {
					_, _ = io.Copy(io.Discard, r.Body)
					close(started)
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				handler(w, r)
			}))
			defer server.Close()
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			a := startTestAdapter(t, server.URL, &captureSink{}, "client-id-test", "client-secret-test")
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("request did not start")
			}
			done := make(chan error, 1)
			go func() { done <- a.Close() }()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				close(release)
				<-done
				t.Fatal("Close did not cancel the active request")
			}
		})
	}
}

func TestExpiredTokenRefreshHonorsRetryDeadline(t *testing.T) {
	mock := newMockMimecast("cid", "secret")
	handler := mock.handler(t)
	var calls atomic.Int32
	var audits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/api/audit/get-audit-events" && audits.Add(1) == 1 {
			w.Header().Set("Retry-After", "90")
			w.WriteHeader(401)
			return
		}
		handler(w, r)
	}))
	defer server.Close()
	a := newIdleAdapter(t, server.URL, &captureSink{})
	clock := usePollClock(a)
	a.wait = func(d time.Duration) bool {
		assert.Equal(t, int32(2), calls.Load(), "token refresh must wait for the server deadline")
		assert.Equal(t, 90*time.Second, d)
		clock.waits = append(clock.waits, d)
		clock.current = clock.current.Add(d)
		return false
	}
	_, err := a.makeOneRequest()
	require.NoError(t, err)
	assert.Equal(t, []time.Duration{90 * time.Second}, clock.waits)
	assert.Equal(t, 2, mock.tokenCount(), "one bounded refresh must recover the page")
}

func TestAuthenticationFailureEnvelopeIsFatal(t *testing.T) {
	for _, code := range []string{"invalid_credentials", "app_forbidden"} {
		t.Run(code, func(t *testing.T) {
			mock := newMockMimecast("cid", "secret")
			handler := mock.handler(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/oauth/token" {
					handler(w, r)
					return
				}
				_ = json.NewEncoder(w).Encode(ApiResponse{Fail: []FailureDetails{{Errors: []ErrorDetail{{Code: code}}}}})
			}))
			defer server.Close()
			opts, logs := recordingClientOptions(t)
			a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
			_, err := a.makeOneRequest()
			require.Error(t, err)
			assert.NotEmpty(t, logs.joinedErrors())
			assert.True(t, a.doStop.IsSet())
			assert.True(t, a.lastComplete.IsZero())
		})
	}
}
