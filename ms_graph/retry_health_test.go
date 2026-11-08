package usp_ms_graph

import (
	"context"
	"errors"
	"net/http"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type graphTestClock struct {
	mu      sync.Mutex
	current time.Time
}

func (c *graphTestClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.current }
func (c *graphTestClock) advance(d time.Duration) {
	c.mu.Lock()
	c.current = c.current.Add(d)
	c.mu.Unlock()
}
func (c *graphTestClock) runtime() graphRuntime {
	return graphRuntime{now: c.now, wait: func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.advance(d)
		return nil
	}}
}
func newGraphTestClock() *graphTestClock {
	return &graphTestClock{current: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

type requestClockSink struct {
	captureSink
	ship func(*protocol.DataMessage, time.Duration) error
}

func (s *requestClockSink) Ship(m *protocol.DataMessage, d time.Duration) error {
	if err := s.ship(m, d); err != nil {
		return err
	}
	return s.captureSink.Ship(m, d)
}

func TestMsGraphSuccessfulFetchResetsRequestClock(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid", true: "malformed"}[malformed], func(t *testing.T) {
			clock := newGraphTestClock()
			var fail atomic.Bool
			_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
				if fail.Load() {
					w.WriteHeader(503)
					return
				}
				if malformed {
					writeJSON(w, 200, map[string]interface{}{"value": nil})
					return
				}
				page(w, []map[string]interface{}{}, "")
			})
			a := manualAdapter(t, conf, &captureSink{}, clock.runtime())
			cursor := graphCursor{since: clock.now()}
			for i := 0; i < 3; i++ {
				clock.advance(55 * time.Minute)
				_, err := a.poll(conf.GraphEndpoint+testResource, cursor, map[graphRecordKey]bool{})
				if malformed {
					require.Error(t, err)
					assert.ErrorIs(t, err, errRetry)
				} else {
					require.NoError(t, err)
				}
				assert.Equal(t, clock.now(), a.lastSuccessfulFetch, "every successful Graph fetch resets the clock")
			}
			fail.Store(true)
			clock.advance(55 * time.Minute)
			_, err := a.poll(conf.GraphEndpoint+testResource, cursor, map[graphRecordKey]bool{})
			require.Error(t, err)
			assert.ErrorIs(t, err, errRetry, "a failed request less than one hour since a fetch stays retryable")
			clock.advance(6 * time.Minute)
			_, err = a.poll(conf.GraphEndpoint+testResource, cursor, map[graphRecordKey]bool{})
			assert.ErrorIs(t, err, errRetryWindow, "failed request over an hour since a fetch must escalate")
		})
	}
}

func TestMsGraphRetryAfterCapped(t *testing.T) {
	for _, header := range []string{"7200", "2147483647", "99999999999999999999999999999999", "Fri, 01 Jan 2100 00:00:00 GMT"} {
		t.Run(header, func(t *testing.T) {
			clock := newGraphTestClock()
			var calls atomic.Int32
			_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.Header().Set("Retry-After", header)
					w.WriteHeader(429)
					return
				}
				page(w, []map[string]interface{}{}, "")
			})
			a := manualAdapter(t, conf, &captureSink{}, clock.runtime())
			start := clock.now()
			_, err := a.poll(conf.GraphEndpoint+testResource, graphCursor{since: start}, map[graphRecordKey]bool{})
			require.NoError(t, err)
			assert.EqualValues(t, 2, calls.Load())
			assert.Equal(t, start.Add(15*time.Minute), clock.now(), "oversized delay must be capped before retrying")
			assert.Equal(t, clock.now(), a.lastSuccessfulFetch)
		})
	}
}

func TestMsGraphFetchedPageFinishesAcrossOldDeadline(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "buffered"}[buffered], func(t *testing.T) {
			clock := newGraphTestClock()
			start := clock.now()
			var polls, waits, ships, fatals atomic.Int32
			checked := make(chan string, 1)
			_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
				n := polls.Add(1)
				if n == 1 {
					writeJSON(w, 200, map[string]interface{}{"value": nil})
					return
				}
				if n >= 3 {
					select {
					case checked <- r.URL.Query().Get("$filter"):
					default:
					}
					page(w, []map[string]interface{}{}, "")
					return
				}
				clock.advance(5*time.Minute + 30*time.Second) // request at 55m succeeds at 60m30s
				page(w, []map[string]interface{}{
					graphSecurityAlert("a", graphTimestamp(start.Add(24*time.Hour))),
					graphSecurityAlert("b", graphTimestamp(start.Add(25*time.Hour))),
				}, "")
			})
			rt := clock.runtime()
			rt.wait = func(ctx context.Context, d time.Duration) error {
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
			sink := &requestClockSink{ship: func(m *protocol.DataMessage, d time.Duration) error {
				assert.LessOrEqual(t, d, time.Minute, "finish fetched pages with bounded sends")
				n := ships.Add(1)
				if buffered && n == 1 {
					return uspclient.ErrorBufferFull
				}
				clock.advance(70 * time.Minute)
				return nil
			}}
			ready := make(chan *MsGraphAdapter, 1)
			closed := make(chan struct{})
			conf.ClientOptions.OnWarning = func(string) {}
			conf.ClientOptions.OnError = func(error) { fatals.Add(1); a := <-ready; _ = a.Close(); close(closed) }
			a, _, err := newMsGraphAdapterWithRuntime(context.Background(), conf, sink, rt)
			require.NoError(t, err)
			ready <- a
			defer a.Close()
			var query string
			select {
			case query = <-checked:
			case <-closed:
				t.Fatal("fetched multi-record page went fatal before completion")
			case <-time.After(5 * time.Second):
				t.Fatal("page did not finish")
			}
			require.NoError(t, a.Close())
			assert.Zero(t, fatals.Load())
			assert.Equal(t, "createdDateTime ge "+start.Add(25*time.Hour).Format("2006-01-02T15:04:05.0000000Z"), query)
			ids := map[string]int{}
			for _, msg := range sink.snapshot() {
				ids[msg.JsonPayload["id"].(string)]++
			}
			assert.Equal(t, map[string]int{"a": 1, "b": 1}, ids)
		})
	}
}

func TestMsGraphOldClockDoesNotEscalateShippingFailure(t *testing.T) {
	clock := newGraphTestClock()
	start := clock.now()
	_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
		page(w, []map[string]interface{}{graphSecurityAlert("a", graphTimestamp(start.Add(24*time.Hour)))}, "")
	})
	var calls atomic.Int32
	sink := &requestClockSink{ship: func(m *protocol.DataMessage, d time.Duration) error {
		if calls.Add(1) == 1 {
			clock.advance(2 * time.Hour)
			return errors.New("synthetic shipment failure")
		}
		return nil
	}}
	a := manualAdapter(t, conf, sink, clock.runtime())
	cursor := graphCursor{since: start, ids: map[string]bool{}}
	delivered := map[graphRecordKey]bool{}
	next, err := a.poll(conf.GraphEndpoint+testResource, cursor, delivered)
	require.ErrorIs(t, err, errRetry)
	assert.NotErrorIs(t, err, errRetryWindow)
	assert.Equal(t, cursor, next)
	next, err = a.poll(conf.GraphEndpoint+testResource, cursor, delivered)
	require.NoError(t, err)
	assert.Equal(t, start.Add(24*time.Hour), next.since)
	assert.Equal(t, 1, sink.count())
}

func TestMsGraphFailedRequestEscalatesAndStops(t *testing.T) {
	clock := newGraphTestClock()
	var calls, fatals atomic.Int32
	_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		clock.advance(61 * time.Minute)
		w.WriteHeader(503)
	})
	conf.ClientOptions.OnWarning = func(string) {}
	fatal := make(chan error, 1)
	conf.ClientOptions.OnError = func(err error) { fatals.Add(1); fatal <- err }
	a, stopped, err := newMsGraphAdapterWithRuntime(context.Background(), conf, &captureSink{}, clock.runtime())
	require.NoError(t, err)
	defer a.Close()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("failed request did not stop the reader")
	}
	err = <-fatal
	assert.ErrorIs(t, err, errRetryWindow)
	assert.EqualValues(t, 1, calls.Load())
	assert.EqualValues(t, 1, fatals.Load())
}

func TestMsGraphFatalDeliveredBeforeStoppedOwnerClose(t *testing.T) {
	previous := goruntime.GOMAXPROCS(1)
	defer goruntime.GOMAXPROCS(previous)
	for i := 0; i < 100; i++ {
		conf := validTestConfig(t)
		conf.URL = "bad?%invalid-query" // permanent config failure, no network
		clock := newGraphTestClock()
		var count, before atomic.Int32
		conf.ClientOptions.OnError = func(error) { goruntime.Gosched(); goruntime.Gosched(); count.Add(1) }
		a, stopped, err := newMsGraphAdapterWithRuntime(context.Background(), conf, &captureSink{}, clock.runtime())
		require.NoError(t, err)
		ownerDone := make(chan struct{})
		go func() { <-stopped; before.Store(count.Load()); _ = a.Close(); close(ownerDone) }()
		select {
		case <-ownerDone:
		case <-time.After(5 * time.Second):
			t.Fatal("stopped-channel owner could not close")
		}
		require.EqualValues(t, 1, before.Load(), "fatal callback must be delivered before stopped-channel cleanup")
		require.EqualValues(t, 1, count.Load(), "fatal callback delivered exactly once")
	}
}
