package usp_azure_event_hub

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-event-hubs-go/v3"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultWriteTimeout = 60 * 10
)

type EventHubAdapter struct {
	conf      EventHubConfig
	uspClient utils.Shipper

	hub       *eventhub.Hub
	listeners []*eventhub.ListenerHandle

	ctx       context.Context
	chStopped chan struct{}
}

type EventHubConfig struct {
	ClientOptions    uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ConnectionString string                  `json:"connection_string" yaml:"connection_string"`
	Filters          []string                `json:"filters,omitempty" yaml:"filters,omitempty"`
}

func (c *EventHubConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ConnectionString == "" {
		return errors.New("missing connection_string")
	}
	return nil
}

func NewEventHubAdapter(conf EventHubConfig) (*EventHubAdapter, chan struct{}, error) {
	a := &EventHubAdapter{
		conf: conf,
		ctx:  context.Background(),
	}

	var err error
	a.hub, err = eventhub.NewHubFromConnectionString(a.conf.ConnectionString)
	if err != nil {
		return nil, nil, err
	}

	runtimeInfo, err := a.hub.GetRuntimeInformation(a.ctx)
	if err != nil {
		a.hub.Close(a.ctx)
		return nil, nil, err
	}

	client, err := uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		a.hub.Close(a.ctx)
		return nil, nil, err
	}

	// Wrap with filtering if configured
	if len(conf.Filters) > 0 {
		filtered, err := utils.NewFilteredClient(client, conf.Filters, conf.ClientOptions.DebugLog)
		if err != nil {
			a.hub.Close(a.ctx)
			return nil, nil, fmt.Errorf("failed to create filter: %w", err)
		}
		a.uspClient = filtered
	} else {
		a.uspClient = client
	}

	a.chStopped = make(chan struct{})

	for _, partitionID := range runtimeInfo.PartitionIDs {
		// Start receiving messages
		//
		// Receive blocks while attempting to connect to hub, then runs until listenerHandle.Close() is called
		// <- listenerHandle.Done() signals listener has stopped
		// listenerHandle.Err() provides the last error the receiver encountered
		listenerHandle, err := a.hub.Receive(a.ctx, partitionID, a.processEvent, eventhub.ReceiveWithLatestOffset())
		if err != nil {
			for _, l := range a.listeners {
				l.Close(a.ctx)
			}
			a.hub.Close(a.ctx)
			a.uspClient.Close()
			a.conf.ClientOptions.OnError(err)
			return nil, nil, err
		}
		a.listeners = append(a.listeners, listenerHandle)

		// Listen for termination of this handle. Whenever
		// any of them fails, we will stop the client.
		go func(h *eventhub.ListenerHandle) {
			ch := h.Done()
			<-ch
			if err := h.Err(); err != nil && !errors.Is(err, context.Canceled) {
				a.conf.ClientOptions.OnError(fmt.Errorf("ListenerHandle.Err(): %v", err))
			}
			a.chStopped <- struct{}{}
		}(listenerHandle)
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("partition listener for %s started", partitionID))
	}

	return a, a.chStopped, nil
}

func (a *EventHubAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	for _, l := range a.listeners {
		l.Close(a.ctx)
	}
	err1 := a.hub.Close(a.ctx)
	err2 := a.uspClient.Drain(1 * time.Minute)
	_, err3 := a.uspClient.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return err3
}

func (a *EventHubAdapter) processEvent(ctx context.Context, message *eventhub.Event) error {
	msg := &protocol.DataMessage{
		TextPayload: string(message.Data),
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.DebugLog("stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
		}
		return err
	}
	return nil
}
