package usp_gmail

import (
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
)

type parallelDrainSink struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (s *parallelDrainSink) Ship(*protocol.DataMessage, time.Duration) error { return nil }
func (s *parallelDrainSink) Drain(time.Duration) error {
	s.started <- struct{}{}
	<-s.release
	return nil
}
func (s *parallelDrainSink) Close() ([]*protocol.DataMessage, error) { return nil, nil }

// Thousands of mailboxes inside one adapter must also share one drain interval;
// a16-client semaphore multiplies a one-minute deadline into hours.
func TestThousandsOfMailboxDrainsShareOneInterval(t *testing.T) {
	const n = 2000
	started := make(chan struct{}, n)
	release := make(chan struct{})
	done := make(chan error, 1)
	defer func() {
		close(release)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("mailbox cleanup leaked workers")
		}
	}()
	owner := &GmailAdapter{}
	collectors := make([]*mailboxCollector, n)
	for i := range collectors {
		collectors[i] = &mailboxCollector{a: owner, uspClient: &parallelDrainSink{started: started, release: release}}
	}
	go func() { done <- cleanupCollectors(collectors) }()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for i := 0; i < n; i++ {
		select {
		case <-started:
		case <-timer.C:
			t.Fatalf("only%d/%d mailbox clients began draining concurrently", i, n)
		}
	}
}
