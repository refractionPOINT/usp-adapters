package usp_azure_event_hub

import (
	"context"
	"errors"
	"testing"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/stretchr/testify/require"
)

// The SDK moves past errors (ModifyMessage DeliveryFailed), so a callback error
// must not be mistaken for a retained delivery. Feed the whole vendor sequence.
func TestBackwardDeliveryKeepsCollectingWithoutResetProof(t *testing.T) {
	for _, mode := range []string{"no-ACK", "equal", "higher", "error", "zero"} {
		t.Run(mode, func(t *testing.T) {
			c := newTestClient()
			a, g := harness(t, c)
			h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0", "1"}}}}
			h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
				return runtimePosition(id, 600), nil
			}
			a.hub = h
			require.NoError(t, a.discover(g, true))
			t.Cleanup(func() { a.stopGeneration(g) })
			require.NoError(t, callFor(t, &h.testHub, "0", 1).handler(context.Background(), fixtureEvent(1000, 100)))
			require.NoError(t, callFor(t, &h.testHub, "1", 1).handler(context.Background(), fixtureEvent(5000, 500)))
			if mode != "no-ACK" {
				c.ack(2)
				require.NoError(t, a.confirm(g))
			}
			require.Equal(t, mode == "no-ACK", len(g.confirmed) == 0, "no ACK case must retain actual pending SDK records")
			old, other := callFor(t, &h.testHub, "0", 1), callFor(t, &h.testHub, "1", 1)
			h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
				if mode == "error" {
					return nil, errors.New("management unavailable")
				}
				last := int64(100)
				if mode == "higher" {
					last = 200
				}
				if mode == "zero" {
					last = 0
				}
				return runtimePosition(id, last), nil
			}
			tracker := a.tracker
			rejected := 0
			for _, event := range []*eventhub.Event{fixtureEvent(999, 99), fixtureEvent(1000, 100)} {
				if err := old.handler(context.Background(), event); err != nil {
					rejected++
				}
			}
			g.runtimePreferred = "0"
			require.NoError(t, a.discover(g, false))
			require.Empty(t, g.tags, "negative/no ACK evidence cannot reset")
			require.Len(t, h.snapshot(), 2)
			if err := old.handler(context.Background(), fixtureEvent(1001, 101)); err != nil {
				rejected++
			}
			require.NoError(t, other.handler(context.Background(), fixtureEvent(5001, 501)))
			require.Zero(t, rejected, "backward/reordered deliveries must be shipped, not rejected while the SDK moves on")
			require.Equal(t, 6, c.calls(), "both reordered records and later forward data must be admitted")
			require.EqualValues(t, 101, g.parts["0"].last.Sequence, "high-water must advance only with forward data")
			require.Same(t, tracker, a.tracker)
			require.Same(t, c, a.uspClient)
			require.NoError(t, a.confirm(g))
			if mode == "no-ACK" {
				require.Empty(t, g.confirmed, "all actual admissions remain pending before ACK")
			}
			c.ack(3)
			require.NoError(t, a.confirm(g))
			require.EqualValues(t, 100, g.confirmed["0"], "partial prefix ACK cannot bless the later forward admission")
			require.EqualValues(t, 500, g.confirmed["1"], "other partition's pending tail is independent")
			c.ack(5)
			require.NoError(t, a.confirm(g))
			require.EqualValues(t, 101, g.confirmed["0"])
			require.EqualValues(t, 500, g.confirmed["1"])
			c.ack(6)
			require.NoError(t, a.confirm(g))
			require.NoError(t, a.confirm(g))
			require.EqualValues(t, 101, g.confirmed["0"])
		})
	}
}

func TestReorderedDeliveryDoesNotLowerACKHighWater(t *testing.T) {
	a, g, h, c := resetHarness(t)
	old := callFor(t, &h.testHub, "0", 1)
	require.NoError(t, old.handler(context.Background(), fixtureEvent(50, 11)), "ambiguous backward record must retain ordinary collection")
	c.ack(3)
	require.NoError(t, a.confirm(g))
	require.EqualValues(t, 100, g.confirmed["0"], "ACK of reordered data cannot lower existing reset comparison proof")
	require.EqualValues(t, 11, g.parts["0"].last.Sequence, "SDK retry cursor retains the actual last admitted callback")
	require.Equal(t, "50", g.parts["0"].last.Offset)
}

// A failed partial setup must retain both the exact SDK cursor and the high
// admission proof. Neither can be substituted for the other after reordering.
func TestReorderedPartialSetupRetainsCursorAndACKHighWater(t *testing.T) {
	a, g, h, c := resetHarness(t)
	h.setIDs("0", "1", "2")
	attempts := 0
	h.receive = func(ctx context.Context, id string, handler eventhub.Handler, start receiverStart) (hubListener, error) {
		if id != "2" {
			return h.testHub.Receive(ctx, id, handler, start)
		}
		attempts++
		if attempts == 1 {
			require.NoError(t, handler(ctx, fixtureEvent(1000, 100)))
			require.NoError(t, handler(ctx, fixtureEvent(50, 11)))
			listener, _ := h.testHub.Receive(ctx, id, handler, start)
			return listener, errors.New("partial setup unavailable")
		}
		require.Equal(t, "50", start.Offset, "retry must use exact last admitted offset, not the larger proof offset")
		return h.testHub.Receive(ctx, id, handler, start)
	}
	require.NoError(t, a.discover(g, false))
	require.NoError(t, a.discover(g, false))
	replacement := callFor(t, &h.testHub, "2", 2)
	require.NoError(t, replacement.handler(context.Background(), fixtureEvent(51, 12)))
	c.ack(5)
	require.NoError(t, a.confirm(g))
	require.NoError(t, a.confirm(g))
	require.EqualValues(t, 100, g.confirmed["2"], "coalescing after partial setup cannot lose the ACKed high admission proof")
	require.Equal(t, "51", g.parts["2"].last.Offset)
	select {
	case <-c.sealed:
		t.Fatal("partial setup sealed shared SDK")
	default:
	}
	require.LessOrEqual(t, len(g.retryPositions), maxPartitions)
}
