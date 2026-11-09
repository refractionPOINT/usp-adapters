package usp_pubsub

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

type shutdownReceiptClient struct {
	*receiptClient
	closing  atomic.Bool
	draining chan struct{}
	release  chan struct{}
}

func (c *shutdownReceiptClient) BeginDrain() { c.closing.Store(true) }
func (c *shutdownReceiptClient) Ship(m *protocol.DataMessage, d time.Duration) error {
	if c.closing.Load() {
		return uspclient.ErrorClosed
	}
	return c.receiptClient.Ship(m, d)
}
func (c *shutdownReceiptClient) Drain(d time.Duration) error {
	close(c.draining)
	select {
	case <-c.release:
	case <-time.After(d):
		return context.DeadlineExceeded
	}
	c.mu.Lock()
	c.pending = nil
	c.mu.Unlock()
	return nil
}
func TestShutdownStopsPullBeforeDrainAndConfirmsTail(t *testing.T) {
	srv, topic, project, sub := newFakePubSub(t)
	a, _, err := NewPubSubAdapter(context.Background(), testConfig(project, sub))
	if err != nil {
		t.Fatal(err)
	}
	a.uspClient.Close()
	c := &shutdownReceiptClient{receiptClient: &receiptClient{}, draining: make(chan struct{}), release: make(chan struct{})}
	a.uspClient = c
	stopCalled := make(chan struct{})
	stop := a.stopSub
	a.stopSub = func() { stop(); close(stopCalled) }
	var release sync.Once
	closed := make(chan error, 1)
	defer func() {
		release.Do(func() { close(c.release) })
		select {
		case <-closed:
		case <-time.After(10 * time.Second):
			t.Error("shutdown did not join")
		}
	}()
	id, err := topic.Publish(context.Background(), &pubsub.Message{Data: []byte("tail")}).Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waitAdmissions(t, c.receiptClient, 1)
	a.admissionMu.Lock()
	a.observationMu.Lock()
	a.nextObservation = time.Now().Add(time.Hour)
	a.observationMu.Unlock()
	a.admissionMu.Unlock()
	go func() { closed <- a.Close() }()
	select {
	case <-c.draining:
	case <-time.After(5 * time.Second):
		t.Fatal("Drain did not start")
	}
	select {
	case <-stopCalled:
	default:
		t.Fatal("subscription still pulling during Drain")
	}
	nacks := 0
	for _, m := range srv.Message(id).Modacks {
		if m.AckDeadline == 0 {
			nacks++
		}
	}
	if got := nacks; got != 0 {
		t.Fatalf("accepted waiter Nacked during Drain: %d", got)
	}
	release.Do(func() { close(c.release) })
	// Cleanup waits for Close; observe the source record after Receive flushes.
	waitClosed(t, a.receiveStopped, 5*time.Second, "source flush")
	if got := srv.Message(id).Acks; got != 1 {
		t.Fatalf("confirmed tail replayed: Acks=%d Deliveries=%d", got, srv.Message(id).Deliveries)
	}
}
