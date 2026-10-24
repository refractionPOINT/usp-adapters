package usp_azure_event_hub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

// Runtime mocks expose the exact pinned SDK API plus the private observation
// timestamp wrapper; they never use Receive errors as recreation evidence.
type resetRuntimeHub struct {
	discoveryHub
	runtime  func(context.Context, string) (*eventhub.HubPartitionRuntimeInformation, error)
	queries  atomic.Int32
	observed func() time.Time
}

func (h *resetRuntimeHub) GetPartitionInformation(ctx context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
	h.queries.Add(1)
	if h.runtime != nil {
		return h.runtime(ctx, id)
	}
	return runtimePosition(id, 10), nil
}
func (h *resetRuntimeHub) PartitionRuntime(ctx context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, time.Time, error) {
	info, err := h.GetPartitionInformation(ctx, id)
	observed := time.Now()
	if h.observed != nil {
		observed = h.observed()
	}
	return info, observed, err
}
func runtimePosition(id string, last int64) *eventhub.HubPartitionRuntimeInformation {
	return &eventhub.HubPartitionRuntimeInformation{HubPath: "events", PartitionID: id, BeginningSequenceNumber: 0,
		LastSequenceNumber: last, LastEnqueuedOffset: fmt.Sprint(last + 20), LastEnqueuedTimeUtc: time.Unix(1700000000, 123000000)}
}
func resetStore(t *testing.T) *testStore {
	t.Helper()
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	raw, err := encodePosition(map[string]partitionPosition{"0": fixturePosition(1000, 100), "1": fixturePosition(5000, 500)})
	require.NoError(t, err)
	return &testStore{found: true, value: utils.Checkpoint{SchemaVersion: checkpointSchema, SourceFingerprint: source, Position: raw, CheckpointFence: utils.CheckpointFence{Nonce: "seed", Revision: "seed"}}}
}
func resetAdapter(t *testing.T, h adapterHub, c adapterClient, s *testStore, codes func(string), onError func(error)) *EventHubAdapter {
	t.Helper()
	a := newReviewAdapter(t, h, c, s, codes, onError)
	a.conf.ConnectionString = testConnection
	return a
}
func resetCall(t *testing.T, h *testHub, id string, ordinal int) receiverCall {
	t.Helper()
	var call receiverCall
	require.Eventually(t, func() bool {
		n := 0
		for _, c := range h.snapshot() {
			if c.id == id {
				n++
				if n == ordinal {
					call = c
					return true
				}
			}
		}
		return false
	}, time.Second, time.Millisecond, "partition receiver was not started")
	return call
}

func TestRecreatedPartitionOmissionAcrossTwoRestartsBeforeFirstACK(t *testing.T) {
	store := resetStore(t)
	var vendorErrors atomic.Int32
	for restart := 0; restart < 2; restart++ {
		h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0", "1"}}}}
		var last atomic.Int64
		last.Store(10)
		h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
			if id == "1" {
				return runtimePosition(id, 600), nil
			}
			return runtimePosition(id, last.Load()), nil
		}
		c := newTestClient()
		a := resetAdapter(t, h, c, store, nil, func(error) { vendorErrors.Add(1) })
		worker := startWorker(t, a)
		reset := resetCall(t, &h.testHub, "0", 1)
		other := resetCall(t, &h.testHub, "1", 1)
		require.Empty(t, reset.start.Offset, "positive runtime proof must abandon only the old partition offset")
		require.Equal(t, "5000", other.start.Offset)
		require.NotContains(t, savedPositions(t, store), "0", "omission must survive a restart before any new-incarnation ACK")
		require.Equal(t, "5000", savedPositions(t, store)["1"].Offset)
		if restart == 1 {
			last.Store(11)
			require.NoError(t, reset.handler(context.Background(), fixtureEvent(50, 11)))
			require.NoError(t, other.handler(context.Background(), fixtureEvent(5001, 501)))
			require.NotContains(t, savedPositions(t, store), "0", "receipt alone must not save new-incarnation progress")
			c.ack(1)
			require.Eventually(t, func() bool { return savedPositionOffset(store.snapshot().Position, "0") == "50" }, time.Second, time.Millisecond)
			require.Equal(t, "5000", savedPositions(t, store)["1"].Offset, "other pending message must stay unconfirmed")
			c.ack(2)
		}
		require.NoError(t, a.Close())
		<-worker
	}
	require.Zero(t, vendorErrors.Load())
}

func TestLivePartitionResetRetainsPendingHistoryAndRejectsLateOldACK(t *testing.T) {
	store := resetStore(t)
	h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0", "1"}}}}
	var recreated atomic.Bool
	var a *EventHubAdapter
	var claimed *utils.CheckpointSession
	var current atomic.Int64
	current.Store(10)
	h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		if claimed == nil {
			claimed = a.final.session
		}
		if id == "0" && recreated.Load() {
			return runtimePosition(id, current.Load()), nil
		}
		return runtimePosition(id, 600), nil
	}
	c := newTestClient()
	var resets, vendorErrors atomic.Int32
	a = resetAdapter(t, h, c, store, func(code string) {
		if code == "partition_reset" {
			resets.Add(1)
		}
	}, func(error) { vendorErrors.Add(1) })
	tracker := a.tracker
	worker := startWorker(t, a)
	old := resetCall(t, &h.testHub, "0", 1)
	other := resetCall(t, &h.testHub, "1", 1)
	require.Equal(t, "1000", old.start.Offset)
	require.NoError(t, old.handler(context.Background(), fixtureEvent(1001, 101))) // accepted, pending old candidate
	require.NoError(t, other.handler(context.Background(), fixtureEvent(5001, 501)))
	recreated.Store(true)
	replacement := resetCall(t, &h.testHub, "0", 2)
	require.Empty(t, replacement.start.Offset)
	require.NotContains(t, savedPositions(t, store), "0")
	require.Equal(t, "5000", savedPositions(t, store)["1"].Offset)
	require.Error(t, old.handler(context.Background(), fixtureEvent(1002, 102)), "late old callback must not admit")
	current.Store(11)
	require.NoError(t, replacement.handler(context.Background(), fixtureEvent(50, 11)))
	pending, err := c.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 3)
	require.EqualValues(t, 1, pending[0].SeqNum)
	require.EqualValues(t, 3, pending[2].SeqNum)
	c.ack(2)
	require.Eventually(t, func() bool { return savedPositionOffset(store.snapshot().Position, "1") == "5001" }, time.Second, time.Millisecond)
	require.NotContains(t, savedPositions(t, store), "0", "old candidate ACK must not resurrect abandoned state")
	require.NoError(t, other.handler(context.Background(), fixtureEvent(5002, 502)))
	c.ack(3)
	require.Eventually(t, func() bool { return savedPositionOffset(store.snapshot().Position, "0") == "50" }, time.Second, time.Millisecond)
	require.Equal(t, "5001", savedPositions(t, store)["1"].Offset)
	require.Len(t, h.snapshot(), 3, "only the reset partition may replace its receiver")
	select {
	case <-other.listener.Done():
		t.Fatal("reset retired another partition")
	default:
	}
	select {
	case <-c.sealed:
		t.Fatal("reset sealed the shared SDK")
	default:
	}
	c.ack(4)
	require.NoError(t, a.Close())
	<-worker
	require.Same(t, claimed, a.final.session, "reset must preserve the same checkpoint session")
	require.Same(t, tracker, a.tracker)
	require.Same(t, c, a.uspClient)
	require.Equal(t, "5002", savedPositions(t, store)["1"].Offset)
	require.EqualValues(t, 1, resets.Load())
	require.Zero(t, vendorErrors.Load())
}

func TestBackwardEventNeedsPositiveRuntimeProofBeforePartitionReset(t *testing.T) {
	store := resetStore(t)
	h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0", "1"}}}}
	var allow atomic.Bool
	h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		if id == "0" && allow.Load() {
			return runtimePosition(id, 10), nil
		}
		return runtimePosition(id, 600), nil
	}
	c := newTestClient()
	var vendorErrors atomic.Int32
	a := resetAdapter(t, h, c, store, nil, func(error) { vendorErrors.Add(1) })
	worker := startWorker(t, a)
	old := resetCall(t, &h.testHub, "0", 1)
	other := resetCall(t, &h.testHub, "1", 1)
	before := h.queries.Load()
	require.Error(t, old.handler(context.Background(), fixtureEvent(50, 11)))
	require.Eventually(t, func() bool { return h.queries.Load() > before }, time.Second, time.Millisecond, "backward observation must request runtime proof")
	require.Len(t, h.snapshot(), 2, "backward event alone cannot reset")
	require.NoError(t, a.ctx.Err(), "unproven reset must preserve other receivers")
	require.NoError(t, other.handler(context.Background(), fixtureEvent(5001, 501)))
	require.Error(t, old.handler(context.Background(), fixtureEvent(1001, 101)), "suspect source must stay paused before proof")
	allow.Store(true)
	replacement := resetCall(t, &h.testHub, "0", 2)
	require.Empty(t, replacement.start.Offset)
	require.NoError(t, replacement.handler(context.Background(), fixtureEvent(51, 12)))
	c.ack(2)
	require.NoError(t, a.Close())
	<-worker
	require.Zero(t, vendorErrors.Load())
}

func TestRuntimeNegativeEvidenceNeverOmitsOrResets(t *testing.T) {
	for _, mode := range []string{"equal", "higher", "nil", "default-zero", "empty-sentinel", "missing-offset", "bad-offset", "zero-offset", "missing-time", "missing-hub", "other-hub", "other-partition", "negative-begin", "begin-after-last", "error", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			s := resetStore(t)
			h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0"}}}}
			h.runtime = func(ctx context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
				p := runtimePosition(id, 10)
				switch mode {
				case "equal":
					p.LastSequenceNumber = 100
				case "higher":
					p.LastSequenceNumber = 101
				case "nil":
					return nil, nil
				case "default-zero":
					p.LastSequenceNumber = 0
				case "empty-sentinel":
					p.LastSequenceNumber = -1
					p.LastEnqueuedOffset = "-1"
				case "missing-offset":
					p.LastEnqueuedOffset = ""
				case "bad-offset":
					p.LastEnqueuedOffset = "not-numeric"
				case "zero-offset":
					p.LastEnqueuedOffset = "0"
				case "missing-time":
					p.LastEnqueuedTimeUtc = time.Time{}
				case "missing-hub":
					p.HubPath = ""
				case "other-hub":
					p.HubPath = "elsewhere"
				case "other-partition":
					p.PartitionID = "9"
				case "negative-begin":
					p.BeginningSequenceNumber = -1
				case "begin-after-last":
					p.BeginningSequenceNumber = 11
				case "error":
					return nil, errors.New("management unavailable")
				case "timeout":
					return nil, context.DeadlineExceeded
				}
				return p, nil
			}
			var errs atomic.Int32
			a := resetAdapter(t, h, newTestClient(), s, nil, func(error) { errs.Add(1) })
			worker := startWorker(t, a)
			call := resetCall(t, &h.testHub, "0", 1)
			require.Greater(t, h.queries.Load(), int32(0), "saved state must be checked against actual partition runtime")
			require.Equal(t, "1000", call.start.Offset)
			require.Equal(t, "1000", savedPositions(t, s)["0"].Offset)
			require.NoError(t, a.Close())
			<-worker
			require.Zero(t, errs.Load())
		})
	}
}

// Synchronize a query with cancellation; no fake Receive error stands in for
// partition runtime or for an expired/recreated source.
func TestRuntimeQueryCancellationDoesNotApplyReset(t *testing.T) {
	for _, closeAdapter := range []bool{false, true} {
		t.Run(fmt.Sprint(closeAdapter), func(t *testing.T) {
			h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0"}}}}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
				once.Do(func() { close(entered) })
				<-release
				return runtimePosition(id, 10), nil
			}
			s := resetStore(t)
			var errs, gaps atomic.Int32
			a := resetAdapter(t, h, newTestClient(), s, func(code string) {
				if code == "partition_history_gap" {
					gaps.Add(1)
				}
			}, func(error) { errs.Add(1) })
			worker := startWorker(t, a)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("no partition-runtime query")
			}
			done := make(chan struct{})
			if closeAdapter {
				go func() { defer close(done); _ = a.Close() }()
				require.Eventually(t, a.closing.Load, time.Second, time.Millisecond)
			} else {
				a.cancel()
				close(done)
			}
			close(release)
			<-worker
			<-done
			require.Empty(t, h.snapshot())
			require.Equal(t, "1000", savedPositions(t, s)["0"].Offset)
			require.NoError(t, a.Close())
			require.Equal(t, "1000", savedPositions(t, s)["0"].Offset)
			require.Zero(t, gaps.Load(), "canceled evidence cannot create reset state or a gap verdict")
			require.Zero(t, errs.Load())
		})
	}
}

func TestCachedOrFutureRuntimeEvidenceCannotReset(t *testing.T) {
	for _, delta := range []time.Duration{-time.Hour, time.Hour} {
		t.Run(delta.String(), func(t *testing.T) {
			s := resetStore(t)
			h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0"}}}}
			h.observed = func() time.Time { return time.Now().Add(delta) }
			var stale, vendorErrors atomic.Int32
			a := resetAdapter(t, h, newTestClient(), s, func(code string) {
				if code == "partition_runtime_stale" {
					stale.Add(1)
				}
			}, func(error) { vendorErrors.Add(1) })
			worker := startWorker(t, a)
			call := resetCall(t, &h.testHub, "0", 1)
			require.Greater(t, h.queries.Load(), int32(0))
			require.Greater(t, stale.Load(), int32(0))
			require.Equal(t, "1000", call.start.Offset)
			require.Equal(t, "1000", savedPositions(t, s)["0"].Offset)
			require.NoError(t, a.Close())
			<-worker
			require.Zero(t, vendorErrors.Load())
		})
	}
}

func TestRuntimeEvidenceIsBoundToSourceGenerationPartitionAndSession(t *testing.T) {
	for _, mode := range []string{"source", "generation", "partition", "session"} {
		t.Run(mode, func(t *testing.T) {
			s := resetStore(t)
			h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0"}}}}
			var a *EventHubAdapter
			var changed atomic.Bool
			var rounds atomic.Int32
			h.partitions = func(ctx context.Context) ([]string, error) { rounds.Add(1); return h.testHub.Partitions(ctx) }
			h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
				// This callback runs on the source worker; the simulated change introduces
				// no competing map access. The result belongs to the preceding snapshot.
				if !changed.CompareAndSwap(false, true) {
					return runtimePosition(id, 600), nil
				}
				{
					switch mode {
					case "source":
						a.source[0] ^= 1
					case "generation":
						a.final.id++
					case "partition":
						a.final.parts[id] = &partitionState{}
					case "session":
						a.final.session = nil
					}
				}
				return runtimePosition(id, 10), nil
			}
			var vendorErrors atomic.Int32
			a = resetAdapter(t, h, newTestClient(), s, nil, func(error) { vendorErrors.Add(1) })
			worker := startWorker(t, a)
			require.Eventually(t, changed.Load, time.Second, time.Millisecond, "runtime query never ran")
			require.Eventually(t, func() bool { return rounds.Load() > 1 }, time.Second, time.Millisecond, "source worker did not finish handling the stale result")
			// Join before inspecting worker-owned state and all possible callbacks.
			require.NoError(t, a.Close())
			<-worker
			for _, call := range h.snapshot() {
				require.Equal(t, "1000", call.start.Offset, "old-snapshot evidence selected latest")
			}
			require.Equal(t, "1000", savedPositions(t, s)["0"].Offset)
			require.Zero(t, vendorErrors.Load())
		})
	}
}

func TestRuntimeDeadlineRejectsPositiveResponseAfterExpiry(t *testing.T) {
	s := resetStore(t)
	h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0"}}}}
	var budgetOK atomic.Bool
	h.runtime = func(ctx context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		deadline, ok := ctx.Deadline()
		budgetOK.Store(ok && time.Until(deadline) <= 2*time.Second)
		<-ctx.Done()
		return runtimePosition(id, 10), nil
	}
	a := resetAdapter(t, h, newTestClient(), s, nil, nil)
	a.discoverEvery = time.Hour
	worker := startWorker(t, a)
	require.Eventually(t, func() bool { return len(h.snapshot()) == 1 }, 3*time.Second, time.Millisecond)
	require.True(t, budgetOK.Load(), "partition runtime requires an independent bounded context")
	require.Greater(t, h.queries.Load(), int32(0))
	require.Equal(t, "1000", h.snapshot()[0].start.Offset)
	require.Equal(t, "1000", savedPositions(t, s)["0"].Offset)
	require.NoError(t, a.Close())
	<-worker
}

func TestWholeHubDiscoveryFailureCannotTriggerPartitionReset(t *testing.T) {
	s := resetStore(t)
	h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0"}}}}
	var failing, recreated atomic.Bool
	h.partitions = func(ctx context.Context) ([]string, error) {
		if failing.Load() {
			return nil, errors.New("management unavailable")
		}
		return h.testHub.Partitions(ctx)
	}
	h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		if recreated.Load() {
			return runtimePosition(id, 10), nil
		}
		return runtimePosition(id, 600), nil
	}
	var retries, vendorErrors atomic.Int32
	c := newTestClient()
	a := resetAdapter(t, h, c, s, func(code string) {
		if code == "discovery_retry" {
			retries.Add(1)
		}
	}, func(error) { vendorErrors.Add(1) })
	worker := startWorker(t, a)
	call := resetCall(t, &h.testHub, "0", 1)
	require.Greater(t, h.queries.Load(), int32(0))
	failing.Store(true)
	require.Eventually(t, func() bool { return retries.Load() > 0 }, time.Second, time.Millisecond)
	queries := h.queries.Load()
	recreated.Store(true)
	require.Eventually(t, func() bool { return retries.Load() > 2 }, time.Second, time.Millisecond)
	require.Equal(t, queries, h.queries.Load(), "whole-hub failure cannot supply partition evidence")
	require.Len(t, h.snapshot(), 1)
	require.NoError(t, call.handler(context.Background(), fixtureEvent(1001, 101)))
	c.ack(1)
	require.NoError(t, a.Close())
	<-worker
	require.Equal(t, "1001", savedPositions(t, s)["0"].Offset)
	require.Zero(t, vendorErrors.Load())
}

func TestResetWaitsForCompleteOmissionBeforeOpeningLatest(t *testing.T) {
	s := resetStore(t)
	h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0", "1"}}}}
	var armed atomic.Bool
	h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		if id == "0" && armed.CompareAndSwap(false, true) {
			s.mu.Lock()
			s.errorCAS = errors.New("unknown outcome")
			s.loadErr = errors.New("unavailable")
			s.mu.Unlock()
		}
		if id == "1" {
			return runtimePosition(id, 600), nil
		}
		return runtimePosition(id, 10), nil
	}
	var waits, vendorErrors atomic.Int32
	c := newTestClient()
	a := resetAdapter(t, h, c, s, func(code string) {
		if code == "partition_reset_wait" {
			waits.Add(1)
		}
	}, func(error) { vendorErrors.Add(1) })
	worker := startWorker(t, a)
	other := resetCall(t, &h.testHub, "1", 1)
	require.Eventually(t, func() bool { return waits.Load() > 0 }, time.Second, time.Millisecond, "reset did not wait for its complete omission")
	require.Len(t, h.snapshot(), 1, "latest cannot open behind an unresolved old checkpoint")
	require.Equal(t, "1000", savedPositions(t, s)["0"].Offset)
	require.NoError(t, other.handler(context.Background(), fixtureEvent(5001, 501)))
	s.mu.Lock()
	s.errorCAS = nil
	s.loadErr = nil
	s.mu.Unlock()
	replacement := resetCall(t, &h.testHub, "0", 1)
	require.Empty(t, replacement.start.Offset)
	require.NotContains(t, savedPositions(t, s), "0")
	c.ack(1)
	require.NoError(t, a.Close())
	<-worker
	require.Equal(t, "5001", savedPositions(t, s)["1"].Offset)
	require.Zero(t, vendorErrors.Load())
}

func TestResetOmissionFenceLossKeepsExistingReceiverAndPendingSDK(t *testing.T) {
	s := resetStore(t)
	h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0"}}}}
	var recreated atomic.Bool
	h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		if recreated.Load() {
			s.mu.Lock()
			s.value.Nonce = "successor"
			s.value.Revision = "successor"
			s.mu.Unlock()
			return runtimePosition(id, 10), nil
		}
		return runtimePosition(id, 600), nil
	}
	var conflicts, vendorErrors atomic.Int32
	c := newTestClient()
	a := resetAdapter(t, h, c, s, func(code string) {
		if code == "ownership_conflict" {
			conflicts.Add(1)
		}
	}, func(error) { vendorErrors.Add(1) })
	worker := startWorker(t, a)
	old := resetCall(t, &h.testHub, "0", 1)
	require.NoError(t, old.handler(context.Background(), fixtureEvent(1001, 101)))
	recreated.Store(true)
	require.Eventually(t, func() bool { return conflicts.Load() > 0 }, time.Second, time.Millisecond, "reset omission never encountered its lost fence")
	require.Len(t, h.snapshot(), 1)
	require.NoError(t, old.handler(context.Background(), fixtureEvent(1002, 102)), "fence loss must keep the old traversal running")
	pending, err := c.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	s.mu.Lock()
	swaps := s.swaps
	s.mu.Unlock()
	c.ack(2)
	require.NoError(t, a.Close())
	<-worker
	s.mu.Lock()
	require.Equal(t, swaps, s.swaps)
	s.mu.Unlock()
	require.Equal(t, "1000", savedPositions(t, s)["0"].Offset)
	require.Len(t, h.snapshot(), 1)
	require.EqualValues(t, 1, conflicts.Load())
	require.Zero(t, vendorErrors.Load())
}

func TestVerifiedResetJoinsBlockedOldProducerWithoutSealingSDK(t *testing.T) {
	s := resetStore(t)
	h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0", "1"}}}}
	var recreated atomic.Bool
	h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		if id == "0" && recreated.Load() {
			return runtimePosition(id, 10), nil
		}
		return runtimePosition(id, 600), nil
	}
	c := newTestClient()
	var vendorErrors atomic.Int32
	a := resetAdapter(t, h, c, s, nil, func(error) { vendorErrors.Add(1) })
	worker := startWorker(t, a)
	old := resetCall(t, &h.testHub, "0", 1)
	other := resetCall(t, &h.testHub, "1", 1)
	require.NoError(t, old.handler(context.Background(), fixtureEvent(1001, 101)))
	c.mu.Lock()
	c.blocked = true
	c.mu.Unlock()
	blocked := make(chan error, 1)
	go func() { blocked <- old.handler(context.Background(), fixtureEvent(1002, 102)) }()
	select {
	case <-c.blockedStarted:
	case <-time.After(time.Second):
		t.Fatal("old producer did not block")
	}
	recreated.Store(true)
	var replacement receiverCall
	require.Eventually(t, func() bool {
		for _, call := range h.snapshot() {
			if call.id == "0" && call.start.Offset == "" {
				replacement = call
				return true
			}
		}
		return false
	}, 1500*time.Millisecond, time.Millisecond, "reset did not cancel and join its blocked old source")
	select {
	case err := <-blocked:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("old handler leaked")
	}
	require.Empty(t, replacement.start.Offset)
	require.Error(t, old.handler(context.Background(), fixtureEvent(1003, 103)))
	c.mu.Lock()
	c.blocked = false
	c.mu.Unlock()
	require.NoError(t, other.handler(context.Background(), fixtureEvent(5001, 501)))
	pending, err := c.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	require.Len(t, h.snapshot(), 3)
	select {
	case <-c.sealed:
		t.Fatal("partition reset sealed the shared SDK")
	default:
	}
	c.ack(2)
	require.NoError(t, a.Close())
	<-worker
	require.Zero(t, vendorErrors.Load())
}

type ambiguousResetStore struct {
	*testStore
	arm atomic.Bool
}

func (s *ambiguousResetStore) CompareAndSwap(ctx context.Context, f utils.CheckpointFence, next utils.Checkpoint) (bool, error) {
	positions, err := decodePosition(next.Position)
	_, hasOld := positions["0"]
	ambiguous := err == nil && !hasOld && s.arm.CompareAndSwap(true, false)
	if ambiguous {
		s.mu.Lock()
		s.ambiguous = true
		s.mu.Unlock()
	}
	ok, e := s.testStore.CompareAndSwap(ctx, f, next)
	if ambiguous {
		s.mu.Lock()
		s.errorCAS = errors.New("still unknown")
		s.mu.Unlock()
	}
	return ok, e
}
func TestUnknownCommittedResetOmissionCannotRestoreOldCheckpoint(t *testing.T) {
	base := resetStore(t)
	s := &ambiguousResetStore{testStore: base}
	h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0", "1"}}}}
	var last atomic.Int64
	last.Store(600)
	h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		if id == "0" {
			return runtimePosition(id, last.Load()), nil
		}
		return runtimePosition(id, 600), nil
	}
	c := newTestClient()
	var waits, vendorErrors atomic.Int32
	a := resetAdapter(t, h, c, base, func(code string) {
		if code == "partition_reset_wait" {
			waits.Add(1)
		}
	}, func(error) { vendorErrors.Add(1) })
	a.conf.CheckpointStore = s
	worker := startWorker(t, a)
	old := resetCall(t, &h.testHub, "0", 1)
	other := resetCall(t, &h.testHub, "1", 1)
	require.NoError(t, old.handler(context.Background(), fixtureEvent(1001, 101)))
	s.arm.Store(true)
	last.Store(10)
	require.Eventually(t, func() bool { return waits.Load() > 0 }, time.Second, time.Millisecond, "omission outcome was not held as unknown")
	require.NotContains(t, savedPositions(t, base), "0", "the test write actually committed before its readback failed")
	require.Len(t, h.snapshot(), 2, "a committed but unresolved write is not known-safe reset completion")
	select {
	case <-old.listener.Done():
		t.Fatal("old source retired before its omission was confirmed")
	default:
	}
	require.NoError(t, other.handler(context.Background(), fixtureEvent(5001, 501)))
	base.mu.Lock()
	base.errorCAS = nil
	base.loadErr = nil
	base.mu.Unlock()
	replacement := resetCall(t, &h.testHub, "0", 2)
	require.Empty(t, replacement.start.Offset)
	pending, err := c.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	c.ack(2)
	require.Eventually(t, func() bool { return savedPositionOffset(base.snapshot().Position, "1") == "5001" }, time.Second, time.Millisecond)
	require.NotContains(t, savedPositions(t, base), "0", "resolving an older frozen save cannot resurrect the abandoned position")
	last.Store(11)
	require.NoError(t, replacement.handler(context.Background(), fixtureEvent(50, 11)))
	require.NotContains(t, savedPositions(t, base), "0")
	c.ack(3)
	require.Eventually(t, func() bool { return savedPositionOffset(base.snapshot().Position, "0") == "50" }, time.Second, time.Millisecond)
	require.NoError(t, a.Close())
	<-worker
	require.Zero(t, vendorErrors.Load())
}

func TestResetDoesNotMistakeResolvedOlderSaveForOmission(t *testing.T) {
	c := newTestClient()
	a, g, s := harness(t, c)
	a.conf.ConnectionString = testConnection
	h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0"}}}}
	a.hub = h
	require.NoError(t, process(t, a, g, "0", 1000, 100))
	c.ack(1)
	s.mu.Lock()
	s.ambiguous = true
	s.mu.Unlock()
	require.Error(t, a.saveConfirmed(context.Background(), g)) // committed older CP, unknown to session
	require.NoError(t, process(t, a, g, "0", 1001, 101))
	c.ack(2)
	require.NoError(t, a.discover(g, false))
	require.Greater(t, h.queries.Load(), int32(0))
	require.Len(t, h.snapshot(), 1)
	require.Empty(t, h.snapshot()[0].start.Offset)
	require.NotContains(t, savedPositions(t, s), "0", "resolving the frozen old write must be followed by a COMPLETE reset omission")
	require.NotContains(t, g.confirmed, "0", "late confirmed old candidates cannot replace the reset intent")
	a.stopGeneration(g)
	a.final = g
	close(a.chStopped)
	require.NoError(t, a.Close())
}

func TestRuntimePeriodicInspectionIsSequentialAndBounded(t *testing.T) {
	c := newTestClient()
	a, g, _ := harness(t, c)
	a.conf.ConnectionString = testConnection
	require.NoError(t, process(t, a, g, "0", 1000, 100))
	require.NoError(t, process(t, a, g, "1", 1000, 100))
	c.ack(2)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0", "1"}}}}
	queried := map[string]int{}
	h.runtime = func(ctx context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), 2*time.Second)
		queried[id]++
		return runtimePosition(id, 600), nil
	}
	a.hub = h
	require.NoError(t, a.discover(g, false))
	require.EqualValues(t, 1, h.queries.Load(), "periodic detection must not fan out over all partitions")
	require.NoError(t, a.discover(g, false))
	require.EqualValues(t, 2, h.queries.Load())
	require.Len(t, queried, 2, "periodic detection must rotate rather than starve later partitions")
	a.stopGeneration(g)
	a.final = g
	close(a.chStopped)
	require.NoError(t, a.Close())
}
