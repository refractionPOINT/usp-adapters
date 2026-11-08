package usp_ms_graph

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/internal/vendorerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (c *graphTestClock) advance(delay time.Duration) {
	c.mu.Lock()
	c.current = c.current.Add(delay)
	c.mu.Unlock()
}

type progressSink struct {
	captureSink
	ship func(*protocol.DataMessage, time.Duration) error
}

func (s *progressSink) Ship(m *protocol.DataMessage, d time.Duration) error {
	if err := s.ship(m, d); err != nil {
		return err
	}
	return s.captureSink.Ship(m, d)
}

func TestMsGraphPageProgressRecovery(t *testing.T) {
	for _, mode := range []string{"draining", "final-ship", "buffered-final-ship", "fetch-crossing", "unshipped-tail", "partial-failure", "malformed-envelope", "malformed-continuation"} {
		t.Run(mode, func(t *testing.T) {
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			clock := &graphTestClock{current: start}
			var polls, waits, shipCalls atomic.Int32
			checked := make(chan string, 1)
			var fatals atomic.Int32
			failed := mode == "unshipped-tail" || mode == "partial-failure" || mode == "malformed-envelope" || mode == "malformed-continuation"
			_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/page2" {
					clock.advance(5 * time.Minute) // still draining at minute 65
					page(w, []map[string]interface{}{graphSecurityAlert("b", graphTimestamp(start.Add(25*time.Hour)))}, "/page3")
					return
				}
				if r.URL.Path == "/page3" {
					page(w, []map[string]interface{}{graphSecurityAlert("c", graphTimestamp(start.Add(26*time.Hour)))}, "")
					return
				}
				n := polls.Add(1)
				if n == 1 {
					writeJSON(w, 200, map[string]interface{}{"value": nil})
					return
				} // outage at 0
				if n >= 3 {
					select {
					case checked <- r.URL.Query().Get("$filter"):
					default:
					}
					page(w, []map[string]interface{}{}, "")
					return
				}
				assert.Equal(t, start.Add(55*time.Minute), clock.now(), "recovery must start at minute 55")
				if mode == "fetch-crossing" || mode == "malformed-envelope" || mode == "malformed-continuation" {
					clock.advance(10 * time.Minute)
				}
				if mode == "malformed-envelope" {
					writeJSON(w, 200, map[string]interface{}{"value": nil})
					return
				}
				records := []map[string]interface{}{graphSecurityAlert("a", graphTimestamp(start.Add(24*time.Hour)))}
				if mode == "unshipped-tail" || mode == "partial-failure" {
					records = append(records, graphSecurityAlert("tail", graphTimestamp(start.Add(25*time.Hour))))
				}
				next := ""
				if mode == "malformed-continuation" {
					next = "%invalid"
				}
				if mode == "draining" {
					next = "/page2"
				}
				page(w, records, next)
			})
			runtime := clock.runtime()
			runtime.wait = func(ctx context.Context, delay time.Duration) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				switch waits.Add(1) {
				case 1:
					return nil
				case 2:
					clock.advance(55 * time.Minute)
					return nil
				case 3:
					return nil
				default:
					<-ctx.Done()
					return ctx.Err()
				}
			}
			sink := &progressSink{ship: func(m *protocol.DataMessage, d time.Duration) error {
				assert.LessOrEqual(t, d, time.Minute, "each blocking shipment must be bounded")
				n := shipCalls.Add(1)
				switch mode {
				case "draining":
					if n == 1 {
						clock.advance(5 * time.Minute)
					}
				case "final-ship":
					clock.advance(50 * time.Minute) // successful final Ship crosses old hour
				case "buffered-final-ship":
					if n == 1 {
						return uspclient.ErrorBufferFull
					}
					clock.advance(10 * time.Minute)
				case "unshipped-tail":
					if n == 1 {
						clock.advance(10 * time.Minute)
					}
				case "partial-failure":
					if n == 2 {
						clock.advance(10 * time.Minute)
						return errors.New("synthetic shipment failure")
					}
				}
				return nil
			}}
			ready := make(chan *MsGraphAdapter, 1)
			closed := make(chan struct{})
			conf.ClientOptions.OnWarning = func(string) {}
			conf.ClientOptions.OnError = func(error) { fatals.Add(1); a := <-ready; _ = a.Close(); close(closed) }
			a, _, err := newMsGraphAdapterWithRuntime(context.Background(), conf, sink, runtime)
			require.NoError(t, err)
			ready <- a
			defer a.Close()
			if failed {
				select {
				case <-closed:
				case query := <-checked:
					t.Fatalf("incomplete page incorrectly cleared deadline; next filter %s", query)
				case <-time.After(5 * time.Second):
					t.Fatal("stalled page did not escalate")
				}
				assert.EqualValues(t, 1, fatals.Load())
				if mode == "malformed-envelope" {
					assert.Zero(t, sink.count())
				} else {
					assert.Equal(t, 1, sink.count(), "unshipped tail must not count as page progress")
				}
				return
			}
			var query string
			select {
			case query = <-checked:
			case <-closed:
				t.Fatal("successful page was killed by its previous deadline")
			case <-time.After(5 * time.Second):
				t.Fatal("recovery never completed")
			}
			require.NoError(t, a.Close())
			assert.Zero(t, fatals.Load())
			last := start.Add(24 * time.Hour)
			want := map[string]int{"a": 1}
			if mode == "draining" {
				last = start.Add(26 * time.Hour)
				want["b"] = 1
				want["c"] = 1
				assert.Equal(t, start.Add(65*time.Minute), clock.now())
			}
			assert.Equal(t, "createdDateTime ge "+last.Format("2006-01-02T15:04:05.0000000Z"), query, "successful final page must commit its cursor even after the old deadline")
			ids := map[string]int{}
			for _, msg := range sink.snapshot() {
				ids[msg.JsonPayload["id"].(string)]++
			}
			assert.Equal(t, want, ids)
		})
	}
}

func TestMsGraphStalledRequestNoProgress(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := &graphTestClock{current: start}
	var requests, fatals atomic.Int32
	_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "7200")
		w.WriteHeader(429)
	})
	runtime := clock.runtime()
	wait := runtime.wait
	var waits atomic.Int32
	runtime.wait = func(ctx context.Context, d time.Duration) error {
		if waits.Add(1) == 1 {
			return nil
		}
		return wait(ctx, d)
	}
	ready := make(chan *MsGraphAdapter, 1)
	closed := make(chan struct{})
	conf.ClientOptions.OnWarning = func(string) {}
	conf.ClientOptions.OnError = func(err error) {
		fatals.Add(1)
		assert.Contains(t, err.Error(), "page progress")
		a := <-ready
		_ = a.Close()
		close(closed)
	}
	a, _, err := newMsGraphAdapterWithRuntime(context.Background(), conf, &captureSink{}, runtime)
	require.NoError(t, err)
	ready <- a
	defer a.Close()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("no-progress hour did not escalate")
	}
	assert.Equal(t, start.Add(time.Hour), clock.now())
	assert.EqualValues(t, 1, requests.Load(), "no retry before Retry-After")
	assert.EqualValues(t, 1, fatals.Load())
}

func TestMsGraphCompletedPageReset(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var h graphRetryHealth
	h.note(start, vendorerr.Kind(errRetry, "synthetic failure", nil))
	h.pageComplete(start.Add(55*time.Minute), "page1", true)
	h.note(start.Add(56*time.Minute), vendorerr.Kind(errRetry, "synthetic failure", nil))
	assert.Nil(t, h.escalation(start.Add(65*time.Minute)), "a fully shipped page resets the window")
	// Replaying the completed head with no new shipments is not new progress.
	h.pageComplete(start.Add(110*time.Minute), "page1", false)
	assert.Error(t, h.escalation(start.Add(115*time.Minute)))
	h.pageComplete(start.Add(120*time.Minute), "page1", true)
	h.note(start.Add(121*time.Minute), vendorerr.Kind(errRetry, "new failure", nil))
	assert.Nil(t, h.escalation(start.Add(179*time.Minute)))
	err := h.escalation(start.Add(180 * time.Minute))
	require.Error(t, err)
	assert.False(t, strings.Contains(err.Error(), "synthetic failure"))
}
