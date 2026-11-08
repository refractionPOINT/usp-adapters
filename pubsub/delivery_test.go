package usp_pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"testing"
	"time"
)

func TestDeliveryConfirmedSourceAck(t *testing.T) {
	srv, topic, project, sub := newFakePubSub(t)
	conf := testConfig(project, sub)
	conf.ClientOptions.TestSinkMode = false
	conf.ClientOptions.DestURL = newNeverAckServer(t)
	conf.ClientOptions.Hostname = "test"
	a, _, err := NewPubSubAdapter(context.Background(), conf)
	if err != nil {
		t.Fatal(err)
	}
	// Avoid a minute-long drain against a deliberately non-acking sink.
	defer func() { a.stopSub(); a.uspClient.Close(); a.psClient.Close() }()
	id, err := topic.Publish(context.Background(), &pubsub.Message{Data: []byte("test")}).Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if srv.Message(id).Acks != 0 {
		t.Fatal("source acked before delivery confirmation")
	}
}
