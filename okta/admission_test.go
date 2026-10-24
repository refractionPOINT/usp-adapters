package usp_okta

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
)

// Admission timeouts behave like the SDK: a full buffer blocks for the supplied
// duration, and only successful admissions consume a sequence or retain data.
// BeginDrain is separately observable so a conflict cannot hide by closing it.
type timedAdmissionSink struct {
	ackSink
	blocked           chan struct{}
	release           chan struct{}
	signal, seal      sync.Once
	signalAttempt     int32
	immediateFailures int32
	attempts          atomic.Int32
	allowed           atomic.Bool
	seals             atomic.Int32
	maxTimeout        atomic.Int64
}

func newTimedAdmissionSink(signalAttempt, immediateFailures int32) *timedAdmissionSink {
	return &timedAdmissionSink{blocked: make(chan struct{}), release: make(chan struct{}), signalAttempt: signalAttempt, immediateFailures: immediateFailures}
}
func (s *timedAdmissionSink) Ship(m *protocol.DataMessage, timeout time.Duration) error {
	if m.JsonPayload["uuid"] != "blocked" || s.allowed.Load() {
		return s.ackSink.Ship(m, timeout)
	}
	n := s.attempts.Add(1)
	for old := s.maxTimeout.Load(); int64(timeout) > old; old = s.maxTimeout.Load() {
		if s.maxTimeout.CompareAndSwap(old, int64(timeout)) {
			break
		}
	}
	if n >= s.signalAttempt {
		s.signal.Do(func() { close(s.blocked) })
	}
	if n <= s.immediateFailures {
		return uspclient.ErrorBufferFull
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		return uspclient.ErrorBufferFull
	case <-s.release:
		return uspclient.ErrorClosed
	}
}
func (s *timedAdmissionSink) BeginDrain()               { s.seals.Add(1); s.seal.Do(func() { close(s.release) }) }
func (s *timedAdmissionSink) Drain(time.Duration) error { s.ack(^uint64(0)); return nil }

func TestProgressCanceledAdmissionKeepsIncompletePageUncommitted(t *testing.T) {
	for _, tc := range []struct {
		name               string
		attempt, immediate int32
	}{{"first-full-wait", 1, 0}, {"after-buffer-full", 2, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Link", fmt.Sprintf("<http://%s%s?after=incomplete>; rel=\"next\"", r.Host, logsURL))
				fmt.Fprint(w, `[{"uuid":"accepted"},{"uuid":"blocked"}]`)
			}))
			defer server.Close()
			sink := newTimedAdmissionSink(tc.attempt, tc.immediate)
			a, g := localHarness(t, server.URL, sink)
			var vendorErrors atomic.Int32
			a.conf.ClientOptions.OnError = func(error) { vendorErrors.Add(1) }
			initial := g.confirmed.Load()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); a.collect(ctx, g) }()
			defer func() { cancel(); sink.BeginDrain(); <-done }()
			select {
			case <-sink.blocked:
			case <-time.After(time.Second):
				t.Fatal("expected blocked admission")
			}
			started := time.Now()
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				sink.BeginDrain()
				<-done
				t.Fatal("generation cancellation did not join blocked admission promptly")
			}
			require.Less(t, time.Since(started), time.Second)
			require.Zero(t, sink.seals.Load(), "generation cancellation must keep the SDK usable")
			require.LessOrEqual(t, time.Duration(sink.maxTimeout.Load()), 250*time.Millisecond)
			require.Zero(t, vendorErrors.Load())
			require.Equal(t, 1, sink.count(), "accepted data must stay in the SDK")
			pending, err := sink.GetUnsent()
			require.NoError(t, err)
			require.Len(t, pending, 1)
			require.Equal(t, uint64(1), pending[0].SeqNum)
			require.Len(t, g.live, 1)
			_, suppressed := g.live[eventDigest("blocked")]
			require.False(t, suppressed, "failed Ship is not a dedupe success")
			require.NoError(t, a.confirm(g))
			require.Equal(t, initial, g.confirmed.Load())
			require.Zero(t, g.confirmed.Load(), "an interrupted page must not register a complete candidate")
		})
	}
}
