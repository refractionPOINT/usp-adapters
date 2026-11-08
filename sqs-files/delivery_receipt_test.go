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
	if c.autoACK {
		return nil, c.observeErr
	}
	return append([]*protocol.DataMessage(nil), c.pending...), c.observeErr
}
func (c *receiptClient) BeginDrain()                             {}
func (c *receiptClient) Drain(time.Duration) error               { return nil }
func (c *receiptClient) Close() ([]*protocol.DataMessage, error) { return c.GetUnsent() }
func TestBatchWaitsForLastReceipt(t *testing.T) {
	c := &receiptClient{}
	tracker, _ := acktrack.NewACKTracker(c, 1, 16)
	_, err := tracker.Ship(&protocol.DataMessage{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	dep, err := tracker.Ship(&protocol.DataMessage{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = tracker.Candidate("batch", []byte{1}, dep); err != nil {
		t.Fatal(err)
	}
	a := &SQSFilesAdapter{uspClient: c, tracker: tracker}
	a.conf.ClientOptions = uspclient.ClientOptions{OnError: func(error) {}, DebugLog: func(string) {}, OnWarning: func(string) {}}
	c.pending = c.pending[1:] // only the first message is acknowledged
	if err = a.awaitDelivery(time.Now().Add(40 * time.Millisecond)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("partial ACK authorized cleanup: %v", err)
	}
	c.pending = nil
	if err = a.awaitDelivery(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
}
func TestObservationFailureAndExpiredLease(t *testing.T) {
	for _, expired := range []bool{false, true} {
		c := &receiptClient{}
		tracker, _ := acktrack.NewACKTracker(c, 1, 16)
		dep, _ := tracker.Ship(&protocol.DataMessage{}, time.Second)
		tracker.Candidate("batch", []byte{1}, dep)
		a := &SQSFilesAdapter{uspClient: c, tracker: tracker}
		a.conf.ClientOptions = uspclient.ClientOptions{OnError: func(error) {}, DebugLog: func(string) {}, OnWarning: func(string) {}}
		c.pending = nil
		deadline := time.Now().Add(time.Second)
		if expired {
			deadline = time.Now().Add(-time.Second)
		} else {
			c.observeErr = errors.New("observation failed")
		}
		if err := a.awaitDelivery(deadline); err == nil {
			t.Fatal("uncertain delivery authorized cleanup")
		}
	}
}
func TestFailedShipCannotConfirm(t *testing.T) {
	c := &receiptClient{shipErr: errors.New("failed admission")}
	tracker, _ := acktrack.NewACKTracker(c, 1, 16)
	a := &SQSFilesAdapter{uspClient: c, tracker: tracker}
	a.conf.ClientOptions = uspclient.ClientOptions{OnError: func(error) {}, DebugLog: func(string) {}, OnWarning: func(string) {}}
	_, err := a.shipEvent([]byte("test"), false)
	if err == nil {
		t.Fatal("failed Ship reported success")
	}
	confirmed, err := tracker.Confirm()
	if err != nil || len(confirmed) != 0 {
		t.Fatalf("failed Ship confirmed: %v %v", confirmed, err)
	}
}

func TestCloseJoinsFinalReceiptBeforeClientClose(t *testing.T) {
	c := &receiptClient{}
	tracker, _ := acktrack.NewACKTracker(c, 1, 16)
	dep, _ := tracker.Ship(&protocol.DataMessage{}, time.Second)
	tracker.Candidate("batch", []byte{1}, dep)
	a := &SQSFilesAdapter{uspClient: c, tracker: tracker}
	a.conf.ClientOptions = uspclient.ClientOptions{DebugLog: func(string) {}, OnWarning: func(string) {}}
	a.wg.Add(1)
	go func() {
		if err := a.awaitDelivery(time.Now().Add(time.Second)); err != nil {
			t.Error(err)
		}
		a.wg.Done()
	}()
	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	select {
	case err := <-done:
		t.Fatalf("closed before final receipt: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	c.mu.Lock()
	c.pending = nil
	c.mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown failed to flush")
	}
}
