package usp_ms_graph

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/vendorerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type graphTestClock struct {
	mu      sync.Mutex
	current time.Time
	waits   []time.Duration
}

func (c *graphTestClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.current }
func (c *graphTestClock) runtime() graphRuntime {
	return graphRuntime{now: c.now, wait: func(ctx context.Context, delay time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		c.waits = append(c.waits, delay)
		c.current = c.current.Add(delay)
		c.mu.Unlock()
		return nil
	}}
}

func TestMsGraphRetryHealthBoundary(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	retry := vendorerr.Kind(errRetry, "synthetic retryable failure", nil)
	var h graphRetryHealth
	require.Nil(t, h.escalation(now.Add(24*time.Hour)), "no failures cannot escalate")
	h.note(now, retry)
	h.note(now.Add(20*time.Minute), retry)
	assert.Nil(t, h.escalation(now.Add(time.Hour-time.Nanosecond)))
	fatal := h.escalation(now.Add(time.Hour))
	require.Error(t, fatal)
	assert.True(t, errors.Is(fatal, errRetryWindow))
	assert.False(t, errors.Is(fatal, errRetry), "fatal escalation must not be retried again")
	assert.Contains(t, fatal.Error(), "synthetic retryable failure")
	h.reset()
	assert.Nil(t, h.escalation(now.Add(2*time.Hour)))
	h.note(now.Add(2*time.Hour), retry)
	assert.Nil(t, h.escalation(now.Add(3*time.Hour-time.Nanosecond)))
	assert.Error(t, h.escalation(now.Add(3*time.Hour)))
}

// This callback closes the actual instance, rather than merely logging fatal
// errors. The fake clock drives retries and the hour threshold without sleeping.
func TestMsGraphHostedFailureWindow(t *testing.T) {
	for _, mode := range []string{"escalate", "reset", "retry-after", "permanent-403", "expired-secret", "bad-config"} {
		t.Run(mode, func(t *testing.T) {
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			clock := &graphTestClock{current: start}
			var polls, lasts atomic.Int32
			var failureTimesMu sync.Mutex
			var failureTimes []time.Time
			var firstQuery string
			var secondWindow time.Time
			_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
				if mode == "permanent-403" {
					w.WriteHeader(403)
					return
				}
				if r.URL.Path != "/last" {
					n := polls.Add(1)
					if n == 1 {
						firstQuery = r.URL.Query().Get("$filter")
					}
					// A reset scenario succeeds on its third full poll (40 minutes after
					// failure began), then begins another hour of failure. Partial page
					// success must never reset the interval.
					if mode == "reset" && n == 3 {
						page(w, []map[string]interface{}{graphSecurityAlert("newest", graphTimestamp(start.Add(24*time.Hour)))}, "")
						return
					}
					if n > 1 && !(mode == "reset" && n > 3) {
						assert.Equal(t, firstQuery, r.URL.Query().Get("$filter"), "incomplete scans must retain the cursor")
					}
					page(w, []map[string]interface{}{graphSecurityAlert("newest", graphTimestamp(start.Add(24*time.Hour)))}, "/last")
					return
				}
				lasts.Add(1)
				failureTimesMu.Lock()
				failureTimes = append(failureTimes, clock.now())
				failureTimesMu.Unlock()
				if mode == "retry-after" {
					w.Header().Set("Retry-After", "7200")
					w.WriteHeader(429)
					return
				}
				if mode == "reset" && polls.Load() == 4 {
					secondWindow = clock.now()
				}
				// A transient envelope failure ends the scan immediately, allowing the
				// scripted 20-minute poll intervals to exercise the exact hour boundary.
				writeJSON(w, 200, map[string]interface{}{"value": nil})
			})
			conf.PollInterval = 20 * time.Minute
			if mode == "bad-config" {
				conf.URL = "../invalid?%not-encoded"
			}
			if mode == "expired-secret" {
				conf.ClientSecret = "synthetic-expired-secret"
			}
			var warnings, fatals atomic.Int32
			closed := make(chan struct{})
			ready := make(chan *MsGraphAdapter, 1)
			fatalAt := make(chan time.Time, 1)
			conf.ClientOptions.OnWarning = func(string) { warnings.Add(1) }
			conf.ClientOptions.OnError = func(err error) {
				fatals.Add(1)
				fatalAt <- clock.now()
				if mode == "escalate" || mode == "reset" || mode == "retry-after" {
					assert.Contains(t, err.Error(), "one hour")
				}
				a := <-ready
				_ = a.Close()
				close(closed)
			}
			sink := &captureSink{}
			a, _, err := newMsGraphAdapterWithRuntime(context.Background(), conf, sink, clock.runtime())
			require.NoError(t, err)
			ready <- a
			defer a.Close()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("fatal callback did not close instance")
			}
			at := <-fatalAt
			assert.EqualValues(t, 1, fatals.Load(), "reader must stop after the fatal callback, also on-prem")
			select {
			case <-a.chStopped:
			default:
				t.Fatal("reader did not stop")
			}
			if mode == "escalate" || mode == "reset" || mode == "retry-after" {
				failureTimesMu.Lock()
				first := failureTimes[0]
				failureTimesMu.Unlock()
				if mode == "reset" {
					assert.Equal(t, first.Add(2*time.Hour), at, "the intervening successful poll must reset the hour")
					assert.Equal(t, secondWindow.Add(time.Hour), at)
				} else {
					assert.Equal(t, first.Add(time.Hour), at)
				}
				assert.Positive(t, warnings.Load())
				ids := map[string]int{}
				for _, msg := range sink.snapshot() {
					ids[msg.JsonPayload["id"].(string)]++
				}
				assert.Equal(t, map[string]int{"newest": 1}, ids, "partial shipments survive retries and are not duplicated")
				if mode == "retry-after" {
					assert.EqualValues(t, 1, lasts.Load(), "escalate at one hour without retrying before the two-hour Retry-After")
				}
			} else {
				assert.Zero(t, warnings.Load(), "permanent failures must be fatal immediately")
			}
		})
	}
}

func TestMsGraphSuccessfulPollResetsTransientHistory(t *testing.T) {
	// The complete hosted tests above prove reset followed by a new escalation.
	// This checks that the internal window itself never carries the old cause.
	var h graphRetryHealth
	now := time.Now()
	h.note(now, vendorerr.Kind(errRetry, "first failure", nil))
	h.reset()
	h.note(now.Add(3*time.Hour), vendorerr.Kind(errRetry, "second failure", nil))
	err := h.escalation(now.Add(4 * time.Hour))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "second failure")
	assert.False(t, strings.Contains(err.Error(), "first failure"), fmt.Sprint(err))
}

func TestMsGraphFatalStopsWithoutHost(t *testing.T) {
	for _, mode := range []string{"permanent", "escalation"} {
		t.Run(mode, func(t *testing.T) {
			clock := &graphTestClock{current: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
				if mode == "permanent" {
					w.WriteHeader(403)
					return
				}
				writeJSON(w, 200, map[string]interface{}{"value": nil})
			})
			conf.PollInterval = 20 * time.Minute
			fatal := make(chan error, 1)
			var fatals atomic.Int32
			conf.ClientOptions.OnError = func(err error) {
				fatals.Add(1)
				select {
				case fatal <- err:
				default:
				}
			}
			conf.ClientOptions.OnWarning = func(string) {}
			a, stopped, err := newMsGraphAdapterWithRuntime(context.Background(), conf, &captureSink{}, clock.runtime())
			require.NoError(t, err)
			defer a.Close()
			select {
			case <-stopped:
			case <-time.After(5 * time.Second):
				t.Fatal("fatal reader kept running without a host closing it")
			}
			select {
			case <-fatal:
			case <-time.After(5 * time.Second):
				t.Fatal("fatal callback missing")
			}
			assert.EqualValues(t, 1, fatals.Load())
		})
	}
}
