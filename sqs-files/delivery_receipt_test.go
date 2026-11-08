package usp_sqs_files

import (
	"context"
	"errors"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"sync"
	"testing"
	"time"
)

type receiptClient struct {
	mu                  sync.Mutex
	next                uint64
	autoACK             bool
	pending             []*protocol.DataMessage
	shipErr, observeErr error
	observed            chan struct{}
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
	if c.observed != nil {
		select {
		case c.observed <- struct{}{}:
		default:
		}
	}
	if c.autoACK {
		return nil, c.observeErr
	}
	return append([]*protocol.DataMessage(nil), c.pending...), c.observeErr
}
func (c *receiptClient) BeginDrain()                             {}
func (c *receiptClient) Drain(time.Duration) error               { return nil }
func (c *receiptClient) Close() ([]*protocol.DataMessage, error) { return c.GetUnsent() }

func TestBatchWaitsForLastReceipt(t *testing.T) {
	c := &receiptClient{observed: make(chan struct{}, 1)}
	a := &SQSFilesAdapter{uspClient: c}
	a.ensureDelivery()
	_, err := a.tracker.Ship(&protocol.DataMessage{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	dep, err := a.tracker.Ship(&protocol.DataMessage{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.tracker.Candidate("0", []byte{0, 0, 0, 0, 0, 0, 0, 2}, dep); err != nil {
		t.Fatal(err)
	}
	c.pending = c.pending[1:]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.awaitDeliveryContext(ctx, 0, 2) }()
	select {
	case <-c.observed:
	case <-time.After(5 * time.Second):
		t.Fatal("no production ACK observation")
	}
	select {
	case err := <-done:
		t.Fatalf("partial ACK authorized cleanup: %v", err)
	default:
	}
	c.mu.Lock()
	c.pending = nil
	c.mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("final receipt not confirmed")
	}
}
func TestObservationFailureAndCancelledContext(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		failure := errors.New("observation failed")
		c := &receiptClient{observeErr: failure}
		a := &SQSFilesAdapter{uspClient: c}
		a.ensureDelivery()
		dep, _ := a.tracker.Ship(&protocol.DataMessage{}, time.Second)
		a.tracker.Candidate("0", []byte{0, 0, 0, 0, 0, 0, 0, 1}, dep)
		ctx, cancel := context.WithCancel(context.Background())
		if cancelled {
			cancel()
		}
		err := a.awaitDeliveryContext(ctx, 0, 1)
		cancel()
		expected := failure
		if cancelled {
			expected = context.Canceled
		}
		if !errors.Is(err, expected) {
			t.Fatalf("uncertain delivery authorized: %v", err)
		}
	}
}
func TestFailedShipCannotConfirm(t *testing.T) {
	c := &receiptClient{shipErr: errors.New("failed admission")}
	tracker, _ := acktrack.NewACKTracker(c, 1, 16)
	a := &SQSFilesAdapter{uspClient: c, tracker: tracker}
	a.conf.ClientOptions = uspclient.ClientOptions{OnError: func(error) {}, DebugLog: func(string) {}, OnWarning: func(string) {}}
	_, err := a.shipEventContext(context.Background(), []byte("test"), false)
	if err == nil {
		t.Fatal("failed Ship reported success")
	}
	confirmed, err := tracker.Confirm()
	if err != nil || len(confirmed) != 0 {
		t.Fatalf("failed Ship confirmed: %v %v", confirmed, err)
	}
}
