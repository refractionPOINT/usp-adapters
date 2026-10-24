package usp_azure_event_hub

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
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

// Prepare ordinary latest receivers, then establish confirmed live positions
// through actual successful admission and cumulative ACK. No startup state exists.
func resetHarness(t *testing.T) (*EventHubAdapter, *generation, *resetRuntimeHub, *testClient) {
	t.Helper()
	c := newTestClient()
	a, g := harness(t, c)
	h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0", "1"}}}}
	h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		return runtimePosition(id, 600), nil
	}
	a.hub = h
	require.NoError(t, a.discover(g, true))
	require.Zero(t, h.queries.Load(), "ordinary latest startup has no reset evidence to inspect")
	for _, call := range h.snapshot() {
		require.Empty(t, call.start.Offset)
		offset, seq := int64(1000), int64(100)
		if call.id == "1" {
			offset, seq = 5000, 500
		}
		require.NoError(t, call.handler(context.Background(), fixtureEvent(offset, seq)))
	}
	c.ack(2)
	require.NoError(t, a.confirm(g))
	require.Equal(t, map[string]int64{"0": 100, "1": 500}, g.confirmed)
	t.Cleanup(func() { a.stopGeneration(g) })
	return a, g, h, c
}
func callFor(t *testing.T, h *testHub, id string, ordinal int) receiverCall {
	t.Helper()
	n := 0
	for _, call := range h.snapshot() {
		if call.id == id {
			n++
			if n == ordinal {
				return call
			}
		}
	}
	t.Fatalf("partition %s has no receiver %d", id, ordinal)
	return receiverCall{}
}
func TestLivePartitionResetRetainsPendingHistoryAndRejectsLateOldACK(t *testing.T) {
	a, g, h, c := resetHarness(t)
	tracker := a.tracker
	old, other := callFor(t, &h.testHub, "0", 1), callFor(t, &h.testHub, "1", 1)
	require.NoError(t, old.handler(context.Background(), fixtureEvent(1001, 101)))
	require.NoError(t, other.handler(context.Background(), fixtureEvent(5001, 501)))
	h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		if id == "0" {
			return runtimePosition(id, 10), nil
		}
		return runtimePosition(id, 600), nil
	}
	g.runtimePreferred = "0"
	require.NoError(t, a.discover(g, false))
	replacement := callFor(t, &h.testHub, "0", 2)
	require.Empty(t, replacement.start.Offset)
	require.NotContains(t, g.confirmed, "0")
	require.EqualValues(t, 500, g.confirmed["1"])
	require.Error(t, old.handler(context.Background(), fixtureEvent(1002, 102)))
	require.NoError(t, replacement.handler(context.Background(), fixtureEvent(50, 11)))
	pending, err := c.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 3)
	require.EqualValues(t, 3, pending[0].SeqNum)
	require.EqualValues(t, 5, pending[2].SeqNum)
	c.ack(4)
	require.NoError(t, a.confirm(g))
	require.NotContains(t, g.confirmed, "0", "late old ACK cannot restore the retired source")
	require.EqualValues(t, 501, g.confirmed["1"])
	require.NoError(t, other.handler(context.Background(), fixtureEvent(5002, 502)))
	c.ack(5)
	require.NoError(t, a.confirm(g))
	require.EqualValues(t, 11, g.confirmed["0"])
	require.EqualValues(t, 501, g.confirmed["1"])
	require.Len(t, h.snapshot(), 3)
	select {
	case <-other.listener.Done():
		t.Fatal("reset retired another partition")
	default:
	}
	select {
	case <-c.sealed:
		t.Fatal("reset sealed shared SDK")
	default:
	}
	require.Same(t, tracker, a.tracker)
	require.Same(t, c, a.uspClient)
	c.ack(6)
	require.NoError(t, a.confirm(g))
	require.EqualValues(t, 502, g.confirmed["1"])
}
func TestBackwardEventNeedsPositiveRuntimeProofBeforePartitionReset(t *testing.T) {
	a, g, h, c := resetHarness(t)
	old, other := callFor(t, &h.testHub, "0", 1), callFor(t, &h.testHub, "1", 1)
	require.Error(t, old.handler(context.Background(), fixtureEvent(50, 11)))
	require.True(t, g.parts["0"].suspect)
	require.NoError(t, a.discover(g, false))
	require.Len(t, h.snapshot(), 2, "backward observation is not reset proof")
	require.NoError(t, a.ctx.Err())
	require.Error(t, old.handler(context.Background(), fixtureEvent(1001, 101)))
	require.NoError(t, other.handler(context.Background(), fixtureEvent(5001, 501)))
	h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		return runtimePosition(id, 10), nil
	}
	g.runtimePreferred = "0"
	require.NoError(t, a.discover(g, false))
	replacement := callFor(t, &h.testHub, "0", 2)
	require.Empty(t, replacement.start.Offset)
	require.NoError(t, replacement.handler(context.Background(), fixtureEvent(51, 12)))
	c.ack(4)
	require.NoError(t, a.confirm(g))
	require.EqualValues(t, 12, g.confirmed["0"])
}
func TestRuntimeNegativeEvidenceNeverOmitsOrResets(t *testing.T) {
	for _, mode := range []string{"equal", "higher", "nil", "default-zero", "empty-sentinel", "missing-offset", "bad-offset", "zero-offset", "missing-time", "missing-hub", "other-hub", "other-partition", "negative-begin", "begin-after-last", "error", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			a, g, h, _ := resetHarness(t)
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
			a.inspectPartitionReset(g, "0")
			require.EqualValues(t, 100, g.confirmed["0"])
			require.Len(t, h.snapshot(), 2)
			require.Empty(t, g.tags)
			require.NoError(t, a.ctx.Err())
		})
	}
}

func TestRuntimeQueryCancellationDoesNotApplyReset(t *testing.T) {
	a, g, h, _ := resetHarness(t)
	entered, release := make(chan struct{}), make(chan struct{})
	h.runtime = func(context.Context, string) (*eventhub.HubPartitionRuntimeInformation, error) {
		close(entered)
		<-release
		return runtimePosition("0", 10), nil
	}
	done := make(chan struct{})
	go func() { defer close(done); a.inspectPartitionReset(g, "0") }()
	<-entered
	a.cancel()
	close(release)
	<-done
	require.EqualValues(t, 100, g.confirmed["0"])
	require.Len(t, h.snapshot(), 2)
	require.Empty(t, g.tags)
}
func TestCachedOrFutureRuntimeEvidenceCannotReset(t *testing.T) {
	for _, delta := range []time.Duration{-time.Hour, time.Hour} {
		t.Run(delta.String(), func(t *testing.T) {
			a, g, h, _ := resetHarness(t)
			h.observed = func() time.Time { return time.Now().Add(delta) }
			h.runtime = func(context.Context, string) (*eventhub.HubPartitionRuntimeInformation, error) {
				return runtimePosition("0", 10), nil
			}
			a.inspectPartitionReset(g, "0")
			require.EqualValues(t, 100, g.confirmed["0"])
			require.Empty(t, g.tags)
			require.Len(t, h.snapshot(), 2)
		})
	}
}
func TestRuntimeEvidenceIsBoundToSourceGenerationAndPartition(t *testing.T) {
	for _, mode := range []string{"source", "generation", "partition"} {
		t.Run(mode, func(t *testing.T) {
			a, g, h, _ := resetHarness(t)
			h.runtime = func(context.Context, string) (*eventhub.HubPartitionRuntimeInformation, error) {
				switch mode {
				case "source":
					a.source[0] ^= 1
				case "generation":
					g.id++
				case "partition":
					g.parts["0"] = &partitionState{}
				}
				return runtimePosition("0", 10), nil
			}
			a.inspectPartitionReset(g, "0")
			require.EqualValues(t, 100, g.confirmed["0"])
			require.Empty(t, g.tags)
			require.Len(t, h.snapshot(), 2)
		})
	}
}
func TestRuntimeDeadlineRejectsPositiveResponseAfterExpiry(t *testing.T) {
	a, g, h, _ := resetHarness(t)
	h.runtime = func(ctx context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), 2*time.Second)
		<-ctx.Done()
		return runtimePosition(id, 10), nil
	}
	a.inspectPartitionReset(g, "0")
	require.EqualValues(t, 100, g.confirmed["0"])
	require.Empty(t, g.tags)
	require.Len(t, h.snapshot(), 2)
}
func TestWholeHubDiscoveryFailureCannotTriggerPartitionReset(t *testing.T) {
	a, g, h, c := resetHarness(t)
	h.partitions = func(context.Context) ([]string, error) { return nil, errors.New("management unavailable") }
	h.runtime = func(context.Context, string) (*eventhub.HubPartitionRuntimeInformation, error) {
		return runtimePosition("0", 10), nil
	}
	require.NoError(t, a.discover(g, false))
	require.Zero(t, h.queries.Load())
	require.Len(t, h.snapshot(), 2)
	require.Empty(t, g.tags)
	require.NoError(t, callFor(t, &h.testHub, "0", 1).handler(context.Background(), fixtureEvent(1001, 101)))
	c.ack(3)
	require.NoError(t, a.confirm(g))
	require.EqualValues(t, 101, g.confirmed["0"])
}
func TestVerifiedResetJoinsBlockedOldProducerWithoutSealingSDK(t *testing.T) {
	a, g, h, c := resetHarness(t)
	old, other := callFor(t, &h.testHub, "0", 1), callFor(t, &h.testHub, "1", 1)
	require.NoError(t, old.handler(context.Background(), fixtureEvent(1001, 101)))
	c.mu.Lock()
	c.blocked = true
	c.mu.Unlock()
	blocked := make(chan error, 1)
	go func() { blocked <- old.handler(context.Background(), fixtureEvent(1002, 102)) }()
	<-c.blockedStarted
	h.runtime = func(context.Context, string) (*eventhub.HubPartitionRuntimeInformation, error) {
		return runtimePosition("0", 10), nil
	}
	started := time.Now()
	g.runtimePreferred = "0"
	require.NoError(t, a.discover(g, false))
	require.Less(t, time.Since(started), 1500*time.Millisecond)
	require.ErrorIs(t, <-blocked, context.Canceled)
	replacement := callFor(t, &h.testHub, "0", 2)
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
		t.Fatal("reset sealed shared SDK")
	default:
	}
}

func TestRuntimePeriodicInspectionIsSequentialAndBounded(t *testing.T) {
	c := newTestClient()
	a, g := harness(t, c)
	a.conf.ConnectionString = testConnection
	require.NoError(t, process(t, a, g, "0", 1000, 100))
	require.NoError(t, process(t, a, g, "1", 1000, 100))
	c.ack(2)
	require.NoError(t, a.confirm(g))
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
