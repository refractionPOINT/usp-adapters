package usp_okta

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

type oktaMemoryStore struct {
	mu                 sync.Mutex
	value              utils.Checkpoint
	found              bool
	loadErr, errorSave error
	refused            bool
	claims             int
}

func (s *oktaMemoryStore) Load(ctx context.Context) (utils.Checkpoint, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return utils.Checkpoint{}, false, ctx.Err()
	}
	return s.value.Clone(), s.found, s.loadErr
}
func (s *oktaMemoryStore) CompareAndSwap(ctx context.Context, expected utils.CheckpointFence, next utils.Checkpoint) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if s.refused && !s.found {
		return false, utils.ErrCheckpointAllocationRefused
	}
	if s.errorSave != nil {
		return false, s.errorSave
	}
	if s.found && s.value.CheckpointFence == next.CheckpointFence && s.value.Attempt == next.Attempt && bytes.Equal(s.value.Position, next.Position) {
		return true, nil
	}
	if (!s.found && expected != (utils.CheckpointFence{})) || (s.found && expected != s.value.CheckpointFence) {
		return false, nil
	}
	s.claims++
	s.found = true
	s.value = next.Clone()
	return true, nil
}
func (s *oktaMemoryStore) snapshot() utils.Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value.Clone()
}

type ackSink struct {
	mu      sync.Mutex
	next    uint64
	pending []*protocol.DataMessage
	total   int
	fail    error
	readErr error
}

func (s *ackSink) Ship(m *protocol.DataMessage, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	s.next++
	m.SeqNum = s.next
	s.pending = append(s.pending, m)
	s.total++
	return nil
}
func (s *ackSink) GetUnsent() ([]*protocol.DataMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*protocol.DataMessage(nil), s.pending...), s.readErr
}
func (s *ackSink) Drain(time.Duration) error               { return nil } // Deliberately does not ACK.
func (s *ackSink) Close() ([]*protocol.DataMessage, error) { return s.GetUnsent() }
func (s *ackSink) count() int                              { s.mu.Lock(); defer s.mu.Unlock(); return s.total }
func (s *ackSink) ack(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.pending) > 0 && s.pending[0].SeqNum <= seq {
		s.pending = s.pending[1:]
	}
}

func checkpointHarness(t *testing.T, serverURL string, sink uspSink) (*OktaAdapter, *oktaGeneration, *oktaMemoryStore) {
	t.Helper()
	a := directAdapter(t, serverURL)
	a.uspClient = sink
	a.pollInterval = time.Millisecond
	tracker, err := utils.NewACKTracker(sink, 1, checkpointPositionLimit+16)
	require.NoError(t, err)
	a.tracker = tracker
	store := &oktaMemoryStore{}
	source := sha256.Sum256([]byte(strings.ToLower(a.endpoint.Scheme+"://"+a.endpoint.Host) + logsURL))
	session, raw, err := utils.OpenCheckpoint(context.Background(), store, 1, source, a.initialPosition, func(raw []byte) error { _, err := a.decodePosition(raw); return err }, nil)
	require.NoError(t, err)
	p, err := a.decodePosition(raw)
	require.NoError(t, err)
	return a, newOktaGeneration(1, session, p), store
}

func runCollector(t *testing.T, a *OktaAdapter, g *oktaGeneration) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.collect(ctx, g) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("collector did not join")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestCheckpointPendingDuplicateAndEmptyPageDependOnACK(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s?after=%d>; rel=\"next\"", r.Host, logsURL, n))
		if n <= 2 {
			_, _ = w.Write([]byte(`[{"uuid":"same"}]`))
		} else {
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer server.Close()
	sink := &ackSink{}
	a, g, store := checkpointHarness(t, server.URL, sink)
	stop := runCollector(t, a, g)
	require.Eventually(t, func() bool { return calls.Load() >= 4 }, time.Second, time.Millisecond)
	stop()
	require.Equal(t, 1, sink.count(), "pending duplicate must not be shipped twice")
	before := store.snapshot()
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, before.Position, store.snapshot().Position)
	// A successful Drain must not certify a pending event or subsequent empties.
	require.NoError(t, sink.Drain(time.Second))
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, before.Position, store.snapshot().Position)
	sink.ack(1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err := a.decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Contains(t, p.Next, "after=")
	require.Len(t, p.IDs, 1)
}

func TestCheckpointContinuousUnackedTailKeepsFrozenCompleteProbe(t *testing.T) {
	sink := &ackSink{}
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	a, g, store := checkpointHarness(t, server.URL, sink)
	first, err := a.tracker.Ship(&protocol.DataMessage{TextPayload: "first"}, time.Second)
	require.NoError(t, err)
	p := g.position
	p.Next = server.URL + logsURL + "?after=first"
	raw, err := encodeOktaPosition(p)
	require.NoError(t, err)
	require.NoError(t, a.tracker.Candidate("logs", candidateBytes(g.id, 1, raw), first))
	for i := 2; i <= 1000; i++ {
		dep, err := a.tracker.Ship(&protocol.DataMessage{TextPayload: "tail"}, time.Second)
		require.NoError(t, err)
		p.Next = fmt.Sprintf("%s%s?after=%d", server.URL, logsURL, i)
		raw, err := encodeOktaPosition(p)
		require.NoError(t, err)
		require.NoError(t, a.tracker.Candidate("logs", candidateBytes(g.id, uint64(i), raw), dep))
	}
	sink.ack(1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	saved, err := a.decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Contains(t, saved.Next, "after=first")
	sink.ack(1000)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	saved, err = a.decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Contains(t, saved.Next, "after=1000")
}

func TestCheckpointSaveFailureRetainsConsumedPositionWithoutOnError(t *testing.T) {
	sink := &ackSink{}
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	a, g, store := checkpointHarness(t, server.URL, sink)
	var errs atomic.Int32
	a.conf.ClientOptions.OnError = func(error) { errs.Add(1) }
	dep, err := a.tracker.Ship(&protocol.DataMessage{TextPayload: "event"}, time.Second)
	require.NoError(t, err)
	p := g.position
	p.Next = server.URL + logsURL + "?after=saved"
	raw, err := encodeOktaPosition(p)
	require.NoError(t, err)
	require.NoError(t, a.tracker.Candidate("logs", candidateBytes(g.id, 1, raw), dep))
	sink.ack(1)
	store.errorSave = errors.New("must never be logged: cursor/token/user data")
	require.Error(t, a.saveConfirmed(context.Background(), g))
	require.Zero(t, errs.Load())
	store.errorSave = nil
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	saved, err := a.decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, p.Next, saved.Next)
}

func TestCheckpointGenerationRejectsStaleConfirmAndPreservesSDKHistory(t *testing.T) {
	sink := &ackSink{}
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	a, old, store := checkpointHarness(t, server.URL, sink)
	dep, err := a.tracker.Ship(&protocol.DataMessage{TextPayload: "old"}, time.Second)
	require.NoError(t, err)
	old.position.Next = server.URL + logsURL + "?after=old-complete"
	raw, err := encodeOktaPosition(old.position)
	require.NoError(t, err)
	require.NoError(t, a.tracker.Candidate("logs", candidateBytes(old.id, 1, raw), dep))
	// Claim a successor, forcing the stale writer's save to conflict.
	successor, _, err := utils.OpenCheckpoint(context.Background(), store, 1, old.session.Current().SourceFingerprint, a.initialPosition, func([]byte) error { return nil }, nil)
	require.NoError(t, err)
	sink.ack(1)
	require.ErrorIs(t, a.saveConfirmed(context.Background(), old), utils.ErrCheckpointConflict)
	fresh := newOktaGeneration(2, successor, old.position)
	// The client and tracker are preserved rather than resetting local sequences.
	dep, err = a.tracker.Ship(&protocol.DataMessage{TextPayload: "new"}, time.Second)
	require.NoError(t, err)
	p := fresh.position
	p.Next = server.URL + logsURL + "?after=new"
	raw, err = encodeOktaPosition(p)
	require.NoError(t, err)
	require.NoError(t, a.tracker.Candidate("logs", candidateBytes(1, 2, raw), dep))
	sink.ack(2)
	require.NoError(t, a.saveConfirmed(context.Background(), fresh))
	require.Nil(t, fresh.latest, "late stale confirmation must be discarded")
	require.NoError(t, a.tracker.Candidate("logs", candidateBytes(2, 1, raw), dep))
	require.NoError(t, a.saveConfirmed(context.Background(), fresh))
	saved, err := a.decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, p.Next, saved.Next)
}

func TestCheckpointEncodedSizeNewestRestoreAndLiveHistoryIndependent(t *testing.T) {
	a := directAdapter(t, "https://example.okta.com")
	raw, err := a.initialPosition()
	require.NoError(t, err)
	p, err := a.decodePosition(raw)
	require.NoError(t, err)
	now := time.Now().Add(time.Minute).Unix()
	for i := 0; i < 1000; i++ {
		p.IDs = append(p.IDs, savedID{digest: eventDigest(fmt.Sprint(i)), expiry: now - int64(i)})
	}
	// A long opaque cursor reduces optional IDs; the complete position is kept.
	p.Next = "https://example.okta.com" + logsURL + "?after=" + strings.Repeat("a", 2200)
	raw, err = encodeOktaPosition(p)
	require.NoError(t, err)
	require.LessOrEqual(t, len(raw)+64, 8*1024)
	restored, err := a.decodePosition(raw)
	require.NoError(t, err)
	require.Equal(t, p.Next, restored.Next)
	require.Less(t, len(restored.IDs), savedIDLimit)
	require.Equal(t, p.IDs[:len(restored.IDs)], restored.IDs)
	require.Greater(t, restored.Coverage, int64(0))
	p.Next = "https://example.okta.com" + logsURL + "?after=" + strings.Repeat("a", 8*1024)
	_, err = encodeOktaPosition(p)
	require.ErrorIs(t, err, utils.ErrCheckpointLimit)
	g := newOktaGeneration(1, nil, oktaPosition{})
	for i := 0; i < 1000; i++ {
		id := eventDigest(fmt.Sprint(i))
		g.live[id] = liveID{expiry: now, page: 1}
		g.remember(savedID{digest: id, expiry: now})
	}
	require.Len(t, g.recent, 256)
	require.Len(t, g.live, 1000, "saved summary cap must never evict live IDs")
	// Expired pending IDs are not evicted and re-emitted every poll.
	require.True(t, g.makeRoom(time.Now().Add(time.Hour)))
	require.Len(t, g.live, 1000)
	g.confirmed.Store(1)
	require.True(t, g.makeRoom(time.Now().Add(time.Hour)))
	require.Empty(t, g.live)
}

func TestCheckpointBootstrapMissingNilUnavailableAndCredentialRotation(t *testing.T) {
	for _, kind := range []string{"nil", "missing", "valid", "unavailable", "refused", "source-change"} {
		t.Run(kind, func(t *testing.T) {
			requests := make(chan string, 10)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case requests <- r.URL.RequestURI():
				default:
				}
				w.Header().Set("Link", fmt.Sprintf("<http://%s%s>; rel=\"next\"", r.Host, r.URL.RequestURI()))
				_, _ = w.Write([]byte(`[]`))
			}))
			defer server.Close()
			a := directAdapter(t, server.URL)
			source := sha256.Sum256([]byte(strings.ToLower(a.endpoint.Scheme+"://"+a.endpoint.Host) + logsURL))
			store := &oktaMemoryStore{}
			p := oktaPosition{Boundary: server.URL + logsURL + "?since=2020-01-01T00%3A00%3A00Z&sortOrder=ASCENDING", Next: server.URL + logsURL + "?after=opaque%2fsaved&x=1"}
			if kind == "valid" || kind == "source-change" {
				raw, err := encodeOktaPosition(p)
				require.NoError(t, err)
				store.found = true
				store.value = utils.Checkpoint{SchemaVersion: 1, SourceFingerprint: source, Position: raw, CheckpointFence: utils.CheckpointFence{Nonce: "old", Revision: "old"}}
				if kind == "source-change" {
					store.value.SourceFingerprint = [32]byte{99}
				}
			}
			if kind == "unavailable" {
				store.loadErr = errors.New("not missing")
			}
			if kind == "refused" {
				store.refused = true
			}
			conf := OktaConfig{URL: server.URL, ApiKey: "rotated-credential", ClientOptions: testClientOptions(t), PollInterval: time.Millisecond}
			if kind != "nil" {
				conf.CheckpointStore = store
			}
			start := time.Now()
			adapter, _, err := newOktaAdapter(context.Background(), conf, &captureSink{})
			require.NoError(t, err)
			require.Less(t, time.Since(start), 100*time.Millisecond)
			defer adapter.Close()
			if kind == "unavailable" {
				select {
				case <-requests:
					t.Fatal("unavailable storage must not start vendor collection")
				case <-time.After(30 * time.Millisecond):
				}
				return
			}
			select {
			case raw := <-requests:
				if kind == "valid" {
					require.Contains(t, raw, "after=opaque%2fsaved&x=1")
				} else {
					require.Contains(t, raw, "since="+time.Now().UTC().Format("2006-01-02"))
					require.NotContains(t, raw, "2020-")
					require.NotContains(t, raw, "until=")
				}
			case <-time.After(time.Second):
				t.Fatal("vendor collection never began")
			}
		})
	}
}

func TestCheckpointConfigIsGoOnly(t *testing.T) {
	raw, err := json.Marshal(OktaConfig{CheckpointStore: &oktaMemoryStore{}, CheckpointIssue: func(string) {}})
	require.NoError(t, err)
	require.NotContains(t, string(raw), "Checkpoint")
	require.NotContains(t, string(raw), "checkpoint")
}

func TestCheckpointOversizePageNeverLosesItsAdmissionDependency(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		after := "complete"
		if n == 1 {
			after = strings.Repeat("z", 9*1024)
		}
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s?after=%s>; rel=\"next\"", r.Host, logsURL, after))
		if n == 1 {
			_, _ = w.Write([]byte(`[{"uuid":"pending-oversize-page"}]`))
		} else {
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer server.Close()
	sink := &ackSink{}
	a, g, store := checkpointHarness(t, server.URL, sink)
	stop := runCollector(t, a, g)
	require.Eventually(t, func() bool { return calls.Load() >= 3 }, time.Second, time.Millisecond)
	stop()
	before := store.snapshot().Position
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, before, store.snapshot().Position, "a later small empty page must retain the oversize page's dependency")
	sink.ack(1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err := a.decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Contains(t, p.Next, "after=complete")
}

func TestCheckpointFailedAdmissionCannotAdvancePage(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s?after=never>; rel=\"next\"", r.Host, logsURL))
		_, _ = w.Write([]byte(`[{"uuid":"failed"}]`))
	}))
	defer server.Close()
	sink := &ackSink{fail: errors.New("admission failed")}
	a, g, store := checkpointHarness(t, server.URL, sink)
	before := store.snapshot().Position
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a.collect(ctx, g)
	require.Equal(t, int32(1), calls.Load())
	require.Empty(t, g.live)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, before, store.snapshot().Position)
}

func TestCheckpointExpiredContinuationStallsWithoutReplayingUncoveredHistory(t *testing.T) {
	var attempts, boundaryRequests, stalls, vendorErrors atomic.Int32
	requests := make(chan string, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		select {
		case requests <- r.URL.RequestURI():
		default:
		}
		if r.URL.Query().Get("after") == "expired" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"errorCode":"E0000001"}`)
			return
		}
		boundaryRequests.Add(1)
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s?after=fresh>; rel=\"next\"", r.Host, logsURL))
		// This event was delivered before the retained suffix. Rewinding to the
		// original boundary would re-emit it because its ID is no longer saved.
		fmt.Fprint(w, `[{"uuid":"delivered-outside-saved-coverage"}]`)
	}))
	defer server.Close()
	a := directAdapter(t, server.URL)
	source := sha256.Sum256([]byte(strings.ToLower(a.endpoint.Scheme+"://"+a.endpoint.Host) + logsURL))
	p := oktaPosition{
		Boundary: server.URL + logsURL + "?since=" + time.Now().Add(-2*time.Hour).UTC().Format(time.RFC3339) + "&sortOrder=ASCENDING",
		Next:     server.URL + logsURL + "?after=expired",
	}
	for i := 0; i < savedIDLimit; i++ {
		p.IDs = append(p.IDs, savedID{digest: eventDigest(fmt.Sprintf("recent-%d", i)), expiry: time.Now().Add(overlapPeriod).Unix()})
	}
	raw, err := encodeOktaPosition(p)
	require.NoError(t, err)
	store := &oktaMemoryStore{found: true, value: utils.Checkpoint{
		SchemaVersion: 1, SourceFingerprint: source, Position: raw,
		CheckpointFence: utils.CheckpointFence{Nonce: "prior", Revision: "prior"},
	}}
	sink := &captureSink{}
	opts := testClientOptions(t)
	opts.OnError = func(error) { vendorErrors.Add(1) }
	adapter, stopped, err := newOktaAdapter(context.Background(), OktaConfig{
		URL: server.URL, ApiKey: "dummy", ClientOptions: opts, PollInterval: time.Millisecond, CheckpointStore: store,
		CheckpointIssue: func(code string) {
			if code == "continuation_stalled" {
				stalls.Add(1)
			}
		},
	}, sink)
	require.NoError(t, err)
	defer adapter.Close()
	// Observe a retry as well as the initial rejection. Every request must keep
	// the exact saved continuation; neither a newer clock nor Boundary is safe.
	require.Eventually(t, func() bool { return attempts.Load() >= 2 }, 3*time.Second, time.Millisecond)
	closing := time.Now()
	require.NoError(t, adapter.Close())
	require.Less(t, time.Since(closing), time.Second, "cancellation must join stalled collection")
	select {
	case <-stopped:
	default:
		t.Fatal("collection is still running after Close")
	}
	server.Close()
	close(requests)
	for request := range requests {
		require.Equal(t, logsURL+"?after=expired", request)
	}
	require.Zero(t, boundaryRequests.Load())
	require.Zero(t, sink.count(), "uncovered delivered history must not replay")
	require.Zero(t, vendorErrors.Load(), "an internal recovery stall is not a vendor failure")
	require.Positive(t, stalls.Load())
	require.Equal(t, raw, store.snapshot().Position, "the saved complete checkpoint must remain intact")
}

type shutdownSink struct {
	ackSink
	acknowledge bool
	store       *oktaMemoryStore
	atClose     utils.Checkpoint
}

func (s *shutdownSink) Drain(time.Duration) error {
	if s.acknowledge {
		s.ack(^uint64(0))
	}
	return nil
}
func (s *shutdownSink) Close() ([]*protocol.DataMessage, error) {
	s.atClose = s.store.snapshot()
	return s.GetUnsent()
}

func TestCheckpointShutdownConfirmsAfterDrainBeforeClose(t *testing.T) {
	for _, ack := range []bool{false, true} {
		t.Run(fmt.Sprint(ack), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)
				w.Header().Set("Link", fmt.Sprintf("<http://%s%s?after=%d>; rel=\"next\"", r.Host, logsURL, n))
				if n == 1 {
					fmt.Fprint(w, `[{"uuid":"shutdown"}]`)
				} else {
					fmt.Fprint(w, `[]`)
				}
			}))
			defer server.Close()
			store := &oktaMemoryStore{}
			sink := &shutdownSink{acknowledge: ack, store: store}
			adapter, _, err := newOktaAdapter(context.Background(), OktaConfig{URL: server.URL, ApiKey: "dummy", ClientOptions: testClientOptions(t), PollInterval: time.Millisecond, CheckpointStore: store}, sink)
			require.NoError(t, err)
			require.Eventually(t, func() bool { return calls.Load() >= 3 }, time.Second, time.Millisecond)
			initial := store.snapshot()
			if ack {
				require.NoError(t, adapter.Close())
				require.NotEqual(t, initial.Position, sink.atClose.Position)
			} else {
				require.Error(t, adapter.Close())
				require.Equal(t, initial.Position, sink.atClose.Position)
			}
			require.Equal(t, sink.atClose.Position, store.snapshot().Position)
		})
	}
}

func TestCheckpointPositionFitsWithEnvelopeAllowance(t *testing.T) {
	a := directAdapter(t, "https://example.okta.com")
	raw, err := a.initialPosition()
	require.NoError(t, err)
	p, err := a.decodePosition(raw)
	require.NoError(t, err)
	prefix := "https://example.okta.com" + logsURL + "?after="
	metadata := 1 + 2 + len(p.Boundary) + 2 + len(prefix) + 8 + 2
	p.Next = prefix + strings.Repeat("a", checkpointPositionLimit-metadata)
	raw, err = encodeOktaPosition(p)
	require.NoError(t, err)
	require.Equal(t, 8128, len(raw))
	require.Equal(t, 8*1024, len(raw)+64)
	restored, err := a.decodePosition(raw)
	require.NoError(t, err)
	require.Equal(t, p.Next, restored.Next)
	p.Next += "a"
	_, err = encodeOktaPosition(p)
	require.ErrorIs(t, err, utils.ErrCheckpointLimit)
}

type blockedShutdownSink struct {
	ackSink
	entered chan struct{}
	release chan struct{}
	begin   sync.Once
	store   *oktaMemoryStore
	atClose utils.Checkpoint
}

func (s *blockedShutdownSink) Ship(m *protocol.DataMessage, timeout time.Duration) error {
	if s.count() == 0 {
		return s.ackSink.Ship(m, timeout)
	}
	close(s.entered)
	<-s.release
	return uspclient.ErrorClosed
}
func (s *blockedShutdownSink) BeginDrain()               { s.begin.Do(func() { close(s.release) }) }
func (s *blockedShutdownSink) Drain(time.Duration) error { s.ack(^uint64(0)); return nil }
func (s *blockedShutdownSink) Close() ([]*protocol.DataMessage, error) {
	s.atClose = s.store.snapshot()
	return s.GetUnsent()
}

func TestCheckpointShutdownWakesBlockedProducerBeforeJoin(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s?after=partial-page>; rel=\"next\"", r.Host, logsURL))
		fmt.Fprint(w, `[{"uuid":"accepted"},{"uuid":"blocked"}]`)
	}))
	defer server.Close()
	store := &oktaMemoryStore{}
	sink := &blockedShutdownSink{entered: make(chan struct{}), release: make(chan struct{}), store: store}
	a, _, err := newOktaAdapter(context.Background(), OktaConfig{URL: server.URL, ApiKey: "dummy", ClientOptions: testClientOptions(t), PollInterval: time.Millisecond, CheckpointStore: store}, sink)
	require.NoError(t, err)
	defer a.Close()
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("producer did not block")
	}
	initial := store.snapshot().Position
	closed := make(chan error, 1)
	go func() { closed <- a.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		sink.BeginDrain()
		<-closed
		t.Fatal("shutdown did not wake producer before joining")
	}
	require.Equal(t, initial, sink.atClose.Position, "an interrupted page cannot advance, even after accepted data ACKs")
}

func TestCheckpointAllocationRefusalIsMetricOnly(t *testing.T) {
	requested := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requested <- struct{}{}:
		default:
		}
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s>; rel=\"next\"", r.Host, r.URL.RequestURI()))
		fmt.Fprint(w, `[]`)
	}))
	defer server.Close()
	var refusals, warnings, vendorErrors atomic.Int32
	opts := testClientOptions(t)
	opts.OnWarning = func(string) { warnings.Add(1) }
	opts.OnError = func(error) { vendorErrors.Add(1) }
	store := &oktaMemoryStore{refused: true}
	a, _, err := newOktaAdapter(context.Background(), OktaConfig{
		URL: server.URL, ApiKey: "dummy", ClientOptions: opts, PollInterval: time.Millisecond, CheckpointStore: store,
		CheckpointIssue: func(code string) {
			if code == "allocation_refused" {
				refusals.Add(1)
			}
		},
	}, &captureSink{})
	require.NoError(t, err)
	defer a.Close()
	select {
	case <-requested:
	case <-time.After(time.Second):
		t.Fatal("refused optional checkpoint adoption blocked collection")
	}
	require.Equal(t, int32(1), refusals.Load())
	require.Zero(t, warnings.Load())
	require.Zero(t, vendorErrors.Load())
	require.Equal(t, utils.Checkpoint{}, store.snapshot(), "refusal must not be reported as adopted state")
}
