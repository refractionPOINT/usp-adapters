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
	received := make(chan struct{})
	conf.ClientOptions.DestURL = newNeverAckServer(t, received)
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
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("local USP server never received the event")
	}
	pending, err := a.uspClient.GetUnsent()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].TextPayload != "test" {
		t.Fatal("source callback did not admit the expected record")
	}
	// Pub/Sub batches source acknowledgements: observe the negative window only
	// after proving both source receipt and actual SDK admission/transmission.
	time.Sleep(2 * time.Second)
	if srv.Message(id).Acks != 0 {
		t.Fatal("source acked before delivery confirmation")
	}
}
