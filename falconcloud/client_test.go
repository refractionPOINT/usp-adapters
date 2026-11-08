package usp_falconcloud

import (
	"context"
	"testing"
	"time"

	"github.com/crowdstrike/gofalcon/falcon"
	"github.com/crowdstrike/gofalcon/falcon/models/streaming_models"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

func TestFailedShipStopsConsumingStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &FalconCloudAdapter{ctx: ctx}
	a.conf.ClientOptions.OnError = func(error) {}
	stream := &falcon.StreamingHandle{Events: make(chan *streaming_models.EventItem, 2)}
	stream.Events <- &streaming_models.EventItem{}
	stream.Events <- &streaming_models.EventItem{}
	calls := 0
	a.consumeStream(stream, time.Time{}, func(*protocol.DataMessage, time.Duration) error {
		calls++
		if calls == 2 {
			cancel()
		}
		return uspclient.ErrorClosed
	})
	if calls != 1 || len(stream.Events) != 1 {
		t.Fatalf("failed Ship consumed later events: calls=%d remaining=%d", calls, len(stream.Events))
	}
}

func TestBufferPressureRetriesSameEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &FalconCloudAdapter{ctx: ctx, writeTimeout: time.Second}
	a.conf.ClientOptions.OnWarning = func(string) {}
	stream := &falcon.StreamingHandle{Events: make(chan *streaming_models.EventItem, 1)}
	stream.Events <- &streaming_models.EventItem{}
	close(stream.Events)
	calls := 0
	var first *protocol.DataMessage
	a.consumeStream(stream, time.Time{}, func(msg *protocol.DataMessage, timeout time.Duration) error {
		calls++
		if calls == 1 {
			first = msg
			return uspclient.ErrorBufferFull
		}
		if msg != first || timeout != 0 {
			t.Fatal("retry did not retain the original message and wait for admission")
		}
		cancel()
		return nil
	})
	if calls != 2 {
		t.Fatalf("Ship calls = %d, want 2", calls)
	}
}
