package usp_azure_event_hub

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/refractionPOINT/usp-adapters/internal/acktrack"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-event-hubs-go/v3/persist"
	"github.com/stretchr/testify/require"
)

// This SDK-shaped mock uses the real MemoryPersister. It isolates routing and
// late writes, without claiming to run an AMQP service or join SDK internals.
type resetSDKHub struct {
	memory *persist.MemoryPersister
	calls  []string
	closes int
}

func newResetSDKHub() *resetSDKHub { return &resetSDKHub{memory: persist.NewMemoryPersister()} }
func (h *resetSDKHub) GetRuntimeInformation(context.Context) (*eventhub.HubRuntimeInformation, error) {
	return &eventhub.HubRuntimeInformation{PartitionIDs: []string{"0", "1"}}, nil
}
func (h *resetSDKHub) GetPartitionInformation(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
	return runtimePosition(id, 10), nil
}
func (h *resetSDKHub) Receive(_ context.Context, id string, _ eventhub.Handler, options ...eventhub.ReceiveOption) (*eventhub.ListenerHandle, error) {
	h.calls = append(h.calls, id)
	option := reflect.ValueOf(options[0])
	receiver := reflect.New(option.Type().In(0).Elem())
	option.Call([]reflect.Value{receiver})
	offset := receiver.Elem().FieldByName("checkpoint").FieldByName("Offset").String()
	_ = h.memory.Write("namespace", "events", eventhub.DefaultConsumerGroup, id, persist.Checkpoint{Offset: offset})
	return nil, errors.New("mock receiver setup")
}
func (h *resetSDKHub) Close(context.Context) error { h.closes++; return nil }

func TestVerifiedResetRoutesOnlyItsPartitionToFreshDefaultMemory(t *testing.T) {
	ctx := context.Background()
	base, first, second := newResetSDKHub(), newResetSDKHub(), newResetSDKHub()
	router := newAzureHub(testConnection, base)
	created := 0
	router.newHub = func() (sdkHub, error) {
		created++
		if created == 1 {
			return first, nil
		}
		return second, nil
	}
	_, _ = router.Receive(ctx, "0", nil, receiverStart{Offset: "1000"})
	_, _ = router.Receive(ctx, "1", nil, receiverStart{Offset: "5000"})
	_, _ = router.Receive(ctx, "0", nil, receiverStart{resetTag: 2})
	require.Equal(t, 1, created)
	require.Equal(t, []string{"0", "1"}, base.calls)
	require.Equal(t, []string{"0"}, first.calls)
	// A post-callback write on the unjoined old SDK cannot touch the new store.
	require.NoError(t, base.memory.Write("namespace", "events", eventhub.DefaultConsumerGroup, "0", persist.Checkpoint{Offset: "1001"}))
	cp, err := first.memory.Read("namespace", "events", eventhub.DefaultConsumerGroup, "0")
	require.NoError(t, err)
	require.Equal(t, "@latest", cp.Offset)
	// Partial setup retry uses the same new SDK/memory; ordinary reconnects are
	// still entirely owned by that SDK and do not create more hubs.
	_, _ = router.Receive(ctx, "0", nil, receiverStart{Offset: "50", resetTag: 2})
	require.Equal(t, 1, created)
	_, _ = router.Receive(ctx, "1", nil, receiverStart{Offset: "5001"})
	require.Equal(t, []string{"0", "1", "1"}, base.calls)
	_, _ = router.Receive(ctx, "0", nil, receiverStart{resetTag: 3})
	require.Equal(t, 2, created)
	require.Equal(t, 1, first.closes)
	require.Equal(t, []string{"0"}, second.calls)
	require.NoError(t, router.Close(ctx))
	require.Equal(t, 1, base.closes)
	require.Equal(t, 1, second.closes)
}

func TestResetTagsRemainInsideRuntimePartitionBound(t *testing.T) {
	a, g := harness(t, newTestClient())
	g.tags = make(map[string]uint64)
	for n := 0; n < maxPartitions; n++ {
		g.tags[fmt.Sprint(n)] = uint64(n + 2)
	}
	a.hub = &testHub{ids: []string{fmt.Sprint(maxPartitions)}}
	require.ErrorIs(t, a.discover(g, false), acktrack.ErrLimit)
	require.Empty(t, a.hub.(*testHub).snapshot(), "reset tags may not evade the lifetime runtime bound")
}
