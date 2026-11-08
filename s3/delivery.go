package usp_s3

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
)

const maxPendingObjectDeletes = 32
const maxConcurrentObjectDeletes = 8

// No payloads or localFile pointers belong in this bounded source identity queue.
// Only the collector mutates it; Close joins that collector before final flush.
type pendingObjectDelete struct {
	marker     uint64
	dependency acktrack.ACKDependency
	remove     func() error
}

// ACKTracker coalesces intermediate candidates. To retire an exact partial
// prefix, submit each ordered marker against ONE frozen GetUnsent observation.
// No admission may overlap this view: the collector is single-threaded and
// Close first joins it. All dependencies predate the snapshot, so reusing it
// cannot bless a new admission. Later ACKs only make the snapshot conservative.
// The tracker still validates the full sequence history and each dependency.
type objectACKView struct {
	client    acktrack.Client
	snapshot  []*protocol.DataMessage
	observing bool
}

func (v *objectACKView) Ship(msg *protocol.DataMessage, timeout time.Duration) error {
	return v.client.Ship(msg, timeout)
}
func (v *objectACKView) GetUnsent() ([]*protocol.DataMessage, error) {
	if v.observing {
		return v.snapshot, nil
	}
	return v.client.GetUnsent()
}

// limit=32 waits for admission room; limit=1 flushes the listing/end. Polling
// happens once per window/end observation rather than once per source object.
func (a *S3Adapter) waitForDelivery(limit int) error {
	for len(a.pendingDeletes) >= limit {
		if atomic.LoadUint32(&a.isStop) == 1 {
			return nil
		}
		if err := a.confirmDelete(); err != nil {
			return err
		}
		if len(a.pendingDeletes) < limit {
			return nil
		}
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	return nil
}

func (a *S3Adapter) confirmDelete() error {
	if a.deleteErr != nil {
		return a.deleteErr
	}
	if len(a.pendingDeletes) == 0 {
		return nil
	}
	count, err := a.confirmedDeletePrefix()
	if err != nil {
		return err
	}
	return a.deleteConfirmedPrefix(count)
}

// Finish all ACK observations before starting network cleanup. Every delete in
// this batch is covered by the same frozen observation, including partial ACKs.
func (a *S3Adapter) confirmedDeletePrefix() (int, error) {
	// Fail closed on snapshot errors, including during the final shutdown drain.
	snapshot, err := a.uspClient.GetUnsent()
	if err != nil {
		return 0, err
	}
	// GetUnsent returns a copy. Keep sequence-only cells so ACKs arriving
	// during source DELETEs do not leave large payloads pinned by this view.
	// Preserve nil entries for ACKTracker to reject as invalid evidence.
	for i, msg := range snapshot {
		if msg != nil {
			snapshot[i] = &protocol.DataMessage{SeqNum: msg.SeqNum}
		}
	}
	a.ackView.snapshot, a.ackView.observing = snapshot, true
	defer func() { a.ackView.snapshot = nil; a.ackView.observing = false }()
	count := 0
	for _, next := range a.pendingDeletes {
		if err := a.tracker.Candidate("objects", binary.BigEndian.AppendUint64(nil, next.marker), next.dependency); err != nil {
			return 0, err
		}
		confirmed, err := a.tracker.Confirm()
		if err != nil {
			return 0, err
		}
		raw := confirmed["objects"]
		if len(raw) != 8 || binary.BigEndian.Uint64(raw) < next.marker {
			return count, nil
		}
		count++
	}
	return count, nil
}

// Workers touch only their own result slots. Join them before mutating pending
// metadata or reporting errors, so neither collection nor Close leaves cleanup
// running in the background. A fatal error stops future batches; already ACKed
// identities in this batch may finish cleanup alongside the failing request.
func (a *S3Adapter) deleteConfirmedPrefix(count int) error {
	if count == 0 {
		return nil
	}
	results := make([]error, count)
	jobs := make(chan int, count)
	for i := 0; i < count; i++ {
		jobs <- i
	}
	close(jobs)
	var wg sync.WaitGroup
	for w := 0; w < min(count, maxConcurrentObjectDeletes); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				results[i] = a.pendingDeletes[i].remove()
			}
		}()
	}
	wg.Wait()
	retained := a.pendingDeletes[:0]
	var failures []error
	for i, entry := range a.pendingDeletes {
		if i < count {
			err := results[i]
			if err == nil || objectDeleteRace(err) {
				continue
			}
			failures = append(failures, fmt.Errorf("s3.DeleteObject(): %w", err))
		}
		retained = append(retained, entry)
	}
	clear(a.pendingDeletes[len(retained):])
	a.pendingDeletes = retained
	if len(failures) > 0 {
		a.deleteErr = errors.Join(failures...)
		a.conf.ClientOptions.OnError(a.deleteErr)
		return a.deleteErr
	}
	return nil
}

func objectDeleteRace(err error) bool {
	var requestFailure awserr.RequestFailure
	if errors.As(err, &requestFailure) && (requestFailure.StatusCode() == http.StatusNotFound || requestFailure.StatusCode() == http.StatusPreconditionFailed) {
		return true
	}
	var apiError awserr.Error
	if errors.As(err, &apiError) {
		switch apiError.Code() {
		case "NoSuchKey", "NotFound", "PreconditionFailed":
			return true
		}
	}
	return false
}
