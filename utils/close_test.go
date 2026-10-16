package utils

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

type shutdownSink struct {
	sealed             bool
	drained            bool
	closed             bool
	drainErr, closeErr error
	pending            []*protocol.DataMessage
}

func (s *shutdownSink) BeginDrain() { s.sealed = true }
func (s *shutdownSink) Drain(time.Duration) error {
	s.drained = true
	s.sealed = true
	return s.drainErr
}
func (s *shutdownSink) Close() ([]*protocol.DataMessage, error) {
	s.closed = true
	return s.pending, s.closeErr
}

func TestShutdownReportsUnconfirmedMessagesWithoutPayloads(t *testing.T) {
	original := errors.New("socket failure")
	s := &shutdownSink{closeErr: original, pending: []*protocol.DataMessage{{SeqNum: 3, TextPayload: "private-payload"}, {SeqNum: 4, TextPayload: "private-payload"}}}
	warning := ""
	opts := uspclient.ClientOptions{OnWarning: func(v string) { warning = v }}
	pending, err := CloseUSP(s, opts)
	var unacked *UnackedMessagesError
	if !errors.Is(err, original) || !errors.As(err, &unacked) || unacked.Count != 2 || unacked.FirstSeq != 3 || unacked.LastSeq != 4 {
		t.Fatalf("Close=%v", err)
	}
	if len(pending) != 2 || !s.closed {
		t.Fatal("Close lost payloads")
	}
	if !strings.Contains(warning, "2 unacknowledged") || strings.Contains(warning, "private-payload") || strings.Contains(err.Error(), "private-payload") {
		t.Fatalf("unsafe or absent warning: %s, %v", warning, err)
	}
}

func TestShutdownDrainsAdaptersThatPreviouslyClosedImmediately(t *testing.T) {
	expected := errors.New("drain deadline")
	s := &shutdownSink{drainErr: expected}
	stop := StopBlockedProducers(s)
	defer stop()
	_, err := DrainCloseUSP(s, uspclient.ClientOptions{})
	if !s.sealed || !s.drained || !s.closed || !errors.Is(err, expected) {
		t.Fatalf("shutdown sequence: %+v, %v", s, err)
	}
}

type admissionStopSink struct {
	stopped chan struct{}
	once    sync.Once
}

func (s *admissionStopSink) BeginDrain() { s.once.Do(func() { close(s.stopped) }) }

func TestProducerStopGraceAllowsProgressAndBoundsBlockedShip(t *testing.T) {
	t.Run("blocked", func(t *testing.T) {
		sink := &admissionStopSink{stopped: make(chan struct{})}
		cancel := stopBlockedProducersAfter(sink, 80*time.Millisecond)
		defer cancel()
		select {
		case <-sink.stopped:
			t.Fatal("healthy producer admission was closed immediately")
		case <-time.After(20 * time.Millisecond):
		}
		select {
		case <-sink.stopped:
		case <-time.After(time.Second):
			t.Fatal("blocked producer was not released at its deadline")
		}
	})
	t.Run("healthy", func(t *testing.T) {
		sink := &admissionStopSink{stopped: make(chan struct{})}
		cancel := stopBlockedProducersAfter(sink, 80*time.Millisecond)
		cancel() // producers finished normally before their grace expired
		select {
		case <-sink.stopped:
			t.Fatal("cancelled shutdown timer still closed admission")
		case <-time.After(120 * time.Millisecond):
		}
	})
}
