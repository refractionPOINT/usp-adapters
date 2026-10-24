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
	"github.com/refractionPOINT/usp-adapters/utils"
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

func TestCheckpointCanceledAdmissionKeepsIncompletePageUncommitted(t *testing.T) {
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
			a, g, store := checkpointHarness(t, server.URL, sink)
			var vendorErrors atomic.Int32
			a.conf.ClientOptions.OnError = func(error) { vendorErrors.Add(1) }
			initial := store.snapshot().Position
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
			require.NoError(t, a.saveConfirmed(context.Background(), g))
			require.Equal(t, initial, store.snapshot().Position)
			require.Nil(t, g.latest, "an interrupted page must not register a complete candidate")
		})
	}
}

type admissionConflictStore struct {
	*oktaMemoryStore
	conflict chan struct{}
	once     sync.Once
}

func (s *admissionConflictStore) CompareAndSwap(ctx context.Context, fence utils.CheckpointFence, next utils.Checkpoint) (bool, error) {
	applied, err := s.oktaMemoryStore.CompareAndSwap(ctx, fence, next)
	if !applied && err == nil {
		s.once.Do(func() { close(s.conflict) })
	}
	return applied, err
}

func TestCheckpointOwnershipConflictJoinsBlockedAdmissionAndReclaims(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next, body := "second", `[{"uuid":"first"}]`
		switch r.URL.Query().Get("after") {
		case "second":
			next, body = "done", `[{"uuid":"pending"},{"uuid":"blocked"}]`
		case "done":
			next, body = "done", `[]`
		}
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s?after=%s>; rel=\"next\"", r.Host, logsURL, next))
		fmt.Fprint(w, body)
	}))
	defer server.Close()
	base := &oktaMemoryStore{}
	store := &admissionConflictStore{oktaMemoryStore: base, conflict: make(chan struct{})}
	sink := newTimedAdmissionSink(1, 0)
	changed := make(chan struct{}, 1)
	var vendorErrors atomic.Int32
	opts := testClientOptions(t)
	opts.OnError = func(error) { vendorErrors.Add(1) }
	a, _, err := newOktaAdapter(context.Background(), OktaConfig{
		URL: server.URL, ApiKey: "dummy", ClientOptions: opts, PollInterval: time.Millisecond, CheckpointStore: store,
		CheckpointIssue: func(code string) {
			if code == "ownership_changed" {
				sink.allowed.Store(true)
				select {
				case changed <- struct{}{}:
				default:
				}
			}
		},
	}, sink)
	require.NoError(t, err)
	defer a.Close()
	select {
	case <-sink.blocked:
	case <-time.After(time.Second):
		t.Fatal("producer did not block")
	}
	require.Equal(t, 2, sink.count())
	// Only the first COMPLETE page can confirm. The next page has an accepted
	// pending record plus an unaccepted blocked record, so it cannot checkpoint.
	sink.ack(1)
	original := store.snapshot()
	successor, _, err := utils.OpenCheckpoint(context.Background(), store, 1, original.SourceFingerprint, a.initialPosition, func([]byte) error { return nil }, nil)
	require.NoError(t, err)
	claimed := successor.Current()
	select {
	case <-store.conflict:
	case <-time.After(36 * time.Second):
		t.Fatal("manager did not observe ownership conflict")
	}
	conflicted := time.Now()
	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("manager remained stuck joining blocked old generation")
	}
	require.Less(t, time.Since(conflicted), time.Second)
	require.Eventually(t, func() bool { return store.snapshot().Nonce != claimed.Nonce && sink.count() >= 5 }, 2*time.Second, time.Millisecond, "new generation must reclaim and admit on the same SDK")
	require.Zero(t, sink.seals.Load(), "ownership conflict must not seal the reused SDK")
	require.Zero(t, vendorErrors.Load())
	require.LessOrEqual(t, time.Duration(sink.maxTimeout.Load()), 250*time.Millisecond)
	require.Equal(t, original.Position, store.snapshot().Position, "the conflicting and incomplete pages must not advance state")
	pending, err := sink.GetUnsent()
	require.NoError(t, err)
	require.NotEmpty(t, pending)
	require.Equal(t, uint64(2), pending[0].SeqNum, "the older accepted record and sequence history must survive reclaim")
	require.Equal(t, "pending", pending[0].JsonPayload["uuid"])
	require.Greater(t, pending[len(pending)-1].SeqNum, pending[0].SeqNum)
}
