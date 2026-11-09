package usp_gcs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"google.golang.org/api/googleapi"
)

const maxPendingObjectDeletes = 32

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
func (a *GCSAdapter) waitForDelivery(limit int) error {
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

func (a *GCSAdapter) confirmDelete() error {
	if a.deleteErr != nil {
		return a.deleteErr
	}
	if len(a.pendingDeletes) == 0 {
		return nil
	}
	// Fail closed on snapshot errors, including during the final shutdown drain.
	snapshot, err := a.uspClient.GetUnsent()
	if err != nil {
		return err
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
	for len(a.pendingDeletes) > 0 {
		next := a.pendingDeletes[0]
		if err := a.tracker.Candidate("objects", binary.BigEndian.AppendUint64(nil, next.marker), next.dependency); err != nil {
			return err
		}
		confirmed, err := a.tracker.Confirm()
		if err != nil {
			return err
		}
		raw := confirmed["objects"]
		if len(raw) != 8 || binary.BigEndian.Uint64(raw) < next.marker {
			return nil
		}
		// A changed or externally removed object is normal source churn.
		// Retire only this delivered identity and let the next listing discover
		// a replacement. Other delete failures still stop this client lifetime.
		if err := next.remove(); err != nil && !objectDeleteRace(err) {
			a.deleteErr = fmt.Errorf("gcs.DeleteObject(): %w", err)
			a.conf.ClientOptions.OnError(a.deleteErr)
			return a.deleteErr
		}
		// Clear references in the backing array before advancing the prefix.
		a.pendingDeletes[0] = pendingObjectDelete{}
		a.pendingDeletes = a.pendingDeletes[1:]
	}
	return nil
}

func objectDeleteRace(err error) bool {
	if errors.Is(err, storage.ErrObjectNotExist) {
		return true
	}
	var apiError *googleapi.Error
	return errors.As(err, &apiError) && (apiError.Code == http.StatusNotFound || apiError.Code == http.StatusPreconditionFailed)
}
