package usp_azure_event_hub

import (
	"context"
	"errors"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
)

// These small seams let tests exercise receiver lifetime/selection without
// connecting to a vendor or replacing the SDK's in-memory persister.
type hubListener interface {
	Close(context.Context) error
	Done() <-chan struct{}
	Err() error
}
type receiverStart struct {
	Offset   string
	resetTag uint64
} // empty means R1 latest
func (s receiverStart) options() []eventhub.ReceiveOption {
	if s.Offset != "" {
		return []eventhub.ReceiveOption{eventhub.ReceiveWithStartingOffset(s.Offset)}
	}
	return []eventhub.ReceiveOption{eventhub.ReceiveWithLatestOffset()}
}

type adapterHub interface {
	Partitions(context.Context) ([]string, error)
	Receive(context.Context, string, eventhub.Handler, receiverStart) (hubListener, error)
	Close(context.Context) error
}

// sdkHub preserves the actual pinned SDK API while allowing local routing tests.
type sdkHub interface {
	GetRuntimeInformation(context.Context) (*eventhub.HubRuntimeInformation, error)
	GetPartitionInformation(context.Context, string) (*eventhub.HubPartitionRuntimeInformation, error)
	Receive(context.Context, string, eventhub.Handler, ...eventhub.ReceiveOption) (*eventhub.ListenerHandle, error)
	Close(context.Context) error
}
type resetHub struct {
	tag uint64
	hub sdkHub
}

// Only the source worker touches this router, including its final Close.
type azureHub struct {
	hub         sdkHub
	replacement map[string]resetHub
	newHub      func() (sdkHub, error)
}

func newAzureHub(connection string, hub sdkHub) *azureHub {
	return &azureHub{hub: hub, replacement: make(map[string]resetHub), newHub: func() (sdkHub, error) { return eventhub.NewHubFromConnectionString(connection) }}
}
func (h *azureHub) Partitions(ctx context.Context) ([]string, error) {
	info, err := h.hub.GetRuntimeInformation(ctx)
	if err != nil {
		return nil, err
	}
	return info.PartitionIDs, nil
}
func (h *azureHub) Receive(ctx context.Context, id string, handler eventhub.Handler, start receiverStart) (hubListener, error) {
	target := h.hub
	if current, ok := h.replacement[id]; ok {
		target = current.hub
	}
	if start.resetTag != 0 && h.replacement[id].tag != start.resetTag {
		// A verified vendor reset needs a separate default MemoryPersister. The
		// old SDK can write after its callback returns, beyond our handler join;
		// it must not overwrite the new incarnation's reconnect position.
		fresh, err := h.newHub()
		if err != nil {
			return nil, err
		}
		if previous, ok := h.replacement[id]; ok {
			closeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_ = previous.hub.Close(closeCtx)
			cancel()
		}
		h.replacement[id] = resetHub{tag: start.resetTag, hub: fresh}
		target = fresh
	}
	return sdkListenerResult(target.Receive(ctx, id, handler, start.options()...))
}
func (h *azureHub) Close(ctx context.Context) error {
	err := h.hub.Close(ctx)
	for _, current := range h.replacement {
		err = errors.Join(err, current.hub.Close(ctx))
	}
	return err
}

// Do not convert a nil SDK pointer into a non-nil hubListener interface. A
// failed Receive normally has no handle for the caller to close.
func sdkListenerResult(listener *eventhub.ListenerHandle, err error) (hubListener, error) {
	if listener == nil {
		return nil, err
	}
	return listener, err
}
