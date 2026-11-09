package usp_pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"strconv"
	"sync"
	"testing"
	"time"
)

type receiptClient struct {
	mu                  sync.Mutex
	next                uint64
	observations        uint64
	pending             []*protocol.DataMessage
	shipErr, observeErr error
}

func (c *receiptClient) Ship(m *protocol.DataMessage, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.shipErr != nil {
		return c.shipErr
	}
	c.next++
	m.SeqNum = c.next
	c.pending = append(c.pending, m)
	return nil
}
func (c *receiptClient) GetUnsent() ([]*protocol.DataMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observations++
	return append([]*protocol.DataMessage(nil), c.pending...), c.observeErr
}
func (c *receiptClient) BeginDrain()                             {}
func (c *receiptClient) Drain(time.Duration) error               { return nil }
func (c *receiptClient) Close() ([]*protocol.DataMessage, error) { return c.GetUnsent() }

func TestConcurrentHandlersFollowReceiptOrder(t *testing.T) {
	c := &receiptClient{}
	a := &PubSubAdapter{uspClient: c}
	a.conf.ClientOptions = uspclient.ClientOptions{DebugLog: func(string) {}, OnError: func(error) {}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan struct{})
	second := make(chan struct{})
	go func() { a.processEvent(ctx, &pubsub.Message{Data: []byte("first")}); close(first) }()
	waitAdmissions(t, c, 1)
	go func() { a.processEvent(ctx, &pubsub.Message{Data: []byte("second")}); close(second) }()
	waitAdmissions(t, c, 2)
	select {
	case <-first:
		t.Fatal("first acked before receipt")
	default:
	}
	select {
	case <-second:
		t.Fatal("second acked before receipt")
	default:
	}
	c.mu.Lock()
	c.pending = c.pending[1:]
	c.mu.Unlock()
	waitClosed(t, first, time.Second, "first receipt")
	select {
	case <-second:
		t.Fatal("later source ack crossed pending receipt")
	default:
	}
	c.mu.Lock()
	c.pending = nil
	c.mu.Unlock()
	waitClosed(t, second, time.Second, "second receipt")
}
func waitAdmissions(t *testing.T, c *receiptClient, n uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		next := c.next
		c.mu.Unlock()
		if next >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("missing admission")
}
func TestUnconfirmedHandlerStopsOnCancellation(t *testing.T) {
	c := &receiptClient{}
	a := &PubSubAdapter{uspClient: c}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.processEvent(ctx, &pubsub.Message{Data: []byte("test")}); close(done) }()
	waitAdmissions(t, c, 1)
	cancel()
	waitClosed(t, done, time.Second, "cancelled handler")
}
func TestFailedAdmissionReturnsWithoutConfirmation(t *testing.T) {
	c := &receiptClient{shipErr: errors.New("failed admission")}
	a := &PubSubAdapter{uspClient: c}
	a.conf.ClientOptions.OnError = func(error) {}
	done := make(chan struct{})
	go func() { a.processEvent(context.Background(), &pubsub.Message{Data: []byte("test")}); close(done) }()
	waitClosed(t, done, time.Second, "failed admission")
	confirmed, err := a.tracker.Confirm()
	if err != nil || len(confirmed) != 0 {
		t.Fatalf("failed admission confirmed: %v %v", confirmed, err)
	}
}

// Exercise the default Pub/Sub maximum outstanding callback count. Staggered
// cumulative receipts must never release a callback beyond the ACK frontier.
func TestThousandCallbacksShareObservationsAndStaggeredReceipts(t *testing.T) {
	const count = 1000
	c := &receiptClient{}
	a := &PubSubAdapter{uspClient: c}
	a.conf.ClientOptions.OnError = func(error) {}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make([]chan struct{}, count)
	var workers sync.WaitGroup
	for i := range done {
		done[i] = make(chan struct{})
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			a.processEvent(ctx, &pubsub.Message{Data: []byte(strconv.Itoa(i))})
			close(done[i])
		}(i)
	}
	defer workers.Wait()
	defer cancel()
	waitAdmissions(t, c, count)
	c.mu.Lock()
	messages := append([]*protocol.DataMessage(nil), c.pending...)
	before := c.observations
	c.mu.Unlock()
	started := time.Now()
	time.Sleep(100 * time.Millisecond)
	c.mu.Lock()
	reads := c.observations - before
	c.mu.Unlock()
	bound := uint64(time.Since(started)/deliveryObservationInterval) + 2
	t.Logf("1000 callbacks: %d full observations in %v (bound %d)", reads, time.Since(started), bound)
	if reads > bound {
		t.Fatalf("full snapshots scale with callback count: got %d, interval bound %d", reads, bound)
	}

	for _, frontier := range []int{1, 250, 750} {
		c.mu.Lock()
		c.pending = messages[frontier:]
		c.mu.Unlock()
		time.Sleep(3 * deliveryObservationInterval)
		for _, message := range messages[frontier:] {
			index, _ := strconv.Atoi(message.TextPayload)
			select {
			case <-done[index]:
				t.Fatalf("callback for sequence %d crossed receipt frontier %d", message.SeqNum, frontier)
			default:
			}
		}
	}
	c.mu.Lock()
	c.pending = nil
	c.mu.Unlock()
	for _, ch := range done {
		waitClosed(t, ch, 2*time.Second, "final cumulative receipt")
	}
}
func TestObservationErrorIsCachedForConcurrentWaiters(t *testing.T) {
	failure := errors.New("observation failed")
	c := &receiptClient{observeErr: failure}
	a := &PubSubAdapter{uspClient: c}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { a.processEvent(ctx, &pubsub.Message{Data: []byte("test")}); close(done) }()
	waitClosed(t, done, time.Second, "failed observation")
	// Freeze the cache window so scheduler delays cannot expire it mid-check.
	a.observationMu.Lock()
	a.nextObservation = time.Now().Add(time.Hour)
	a.observationMu.Unlock()
	c.mu.Lock()
	c.observeErr = nil
	c.pending = nil
	reads := c.observations
	c.mu.Unlock()
	if err := a.observeDelivery(); !errors.Is(err, failure) {
		t.Fatalf("observation error not cached: %v", err)
	}
	c.mu.Lock()
	after := c.observations
	c.mu.Unlock()
	if after != reads || a.confirmed.Load() != 0 {
		t.Fatal("cached error was treated as confirmation")
	}
}
