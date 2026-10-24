package utils

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
)

var (
	ErrCheckpointSequence   = errors.New("checkpoint sequence ordering is invalid or exhausted")
	ErrCheckpointDependency = errors.New("checkpoint dependency is invalid")
	ErrCheckpointLimit      = errors.New("checkpoint tracker limit exceeded")
)

// CheckpointClient is the admission/ACK portion of a USP client. Ship must stamp
// consecutive uint64 SeqNum values only on successful admission. GetUnsent must
// return an ordered copy of all unacknowledged messages; cumulative ACKs remove
// a prefix. The normal go-uspclient client implements this; TestSinkMode does not.
type CheckpointClient interface {
	Ship(*protocol.DataMessage, time.Duration) error
	GetUnsent() ([]*protocol.DataMessage, error)
}

// ACKDependency is a process-local successful admission, never persistent data.
// Only ACKTracker.Ship creates valid dependencies; a failed or in-flight Ship
// has none. Retain the token while suppressing a pending duplicate of that event.
type ACKDependency struct {
	owner *ACKTracker
	index uint64
}

// Include accumulates the latest dependency in constant space. A zero receiver
// starts an accumulator, but next must be a successful admission from the same
// tracker. Thus passing the result of a failed Ship fails closed.
func (d ACKDependency) Include(next ACKDependency) (ACKDependency, error) {
	if next.owner == nil || (d.owner != nil && d.owner != next.owner) {
		return d, ErrCheckpointDependency
	}
	if d.index > next.index {
		return d, nil
	}
	return next, nil
}

type ackCandidate struct {
	position []byte
	index    uint64
}

type ackStream struct {
	// last also makes an empty page inherit its preceding admission boundary.
	last         uint64
	probe, newer *ackCandidate
}

// ACKTracker keeps one frozen probe and one coalesced newer position per stream.
// Memory is O(maxStreams * maxPositionBytes), independent of event count. It is
// safe for concurrent calls; positions are copied on input and output.
//
// All sends on one fresh client MUST go through Ship. Do not reuse/mutate a
// shipped message (the SDK retains its pointer), use TestSinkMode, or close/reset
// the client during Confirm. A replacement client requires a new tracker and
// replay from the saved source position. Confirm never waits for an in-flight
// Ship. It proves only the upstream ACK contract, not downstream processing.
//
// The adapter must complete pages in source order, include the maximum of ALL
// successful own admissions AND suppressed-pending dependencies, and submit no
// candidate across a failed/in-flight Ship, parse failure or incomplete page.
// Wait for a pending duplicate's admission outcome before suppressing it; a
// failed send cannot be suppressed as delivered. Confirmed duplicates need no
// dependency. Freeze/retry a partition on failure so later positions cannot
// cross that hole. These source traversal duties cannot be inferred here.
type ACKTracker struct {
	client                       CheckpointClient
	maxStreams, maxPositionBytes int
	shipMu                       sync.Mutex // serializes admission, never held by Confirm
	confirmMu                    sync.Mutex // observations must not overtake one another
	mu                           sync.Mutex
	streams                      map[string]*ackStream
	first, count                 uint64
	inFlight                     bool
	broken                       bool
}

// NewACKTracker requires explicit bounds; it never truncates an opaque position
// or evicts a stream to fit. Bounds cover only tracker memory, not the SDK's
// GetUnsent pointer-slice allocation or the adapter's live dedupe cache.
func NewACKTracker(client CheckpointClient, maxStreams, maxPositionBytes int) (*ACKTracker, error) {
	if client == nil || maxStreams <= 0 || maxPositionBytes <= 0 {
		return nil, ErrCheckpointLimit
	}
	return &ACKTracker{client: client, maxStreams: maxStreams, maxPositionBytes: maxPositionBytes, streams: make(map[string]*ackStream)}, nil
}

const ackSequenceHalf = uint64(1) << 63

// Ship returns a dependency only after successful SDK admission. Admission is
// serialized to validate EVERY consecutive sequence, including uint64 wrap.
// Fewer than 2^63 lifetime admissions are allowed on this tracker/client. This
// hard bound proves that every retained dependency and observed sequence lies
// within one unambiguous half-range; checking pairs alone would not prove that.
// Reaching the bound requires a new client/tracker and replay from saved state.
func (t *ACKTracker) Ship(message *protocol.DataMessage, timeout time.Duration) (ACKDependency, error) {
	if message == nil {
		return ACKDependency{}, ErrCheckpointDependency
	}
	t.shipMu.Lock()
	defer t.shipMu.Unlock()
	t.mu.Lock()
	if t.broken || t.count >= ackSequenceHalf-1 {
		t.mu.Unlock()
		return ACKDependency{}, ErrCheckpointSequence
	}
	t.inFlight = true
	t.mu.Unlock()
	if err := t.client.Ship(message, timeout); err != nil {
		t.mu.Lock()
		t.inFlight = false
		t.mu.Unlock()
		return ACKDependency{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inFlight = false
	if t.count == 0 {
		t.first = message.SeqNum
	} else if message.SeqNum != t.first+t.count {
		t.broken = true
		return ACKDependency{}, ErrCheckpointSequence
	}
	t.count++
	return ACKDependency{owner: t, index: t.count}, nil
}

// Candidate queues a COMPLETE source position in that stream's traversal order.
// No dependencies means an empty page, inheriting the preceding candidate's
// boundary. Initial empty pages are eligible after a successful observation.
// Include dependencies for records admitted since the preceding page and for
// pending duplicates, even if those records belong to another stream.
func (t *ACKTracker) Candidate(stream string, position []byte, dependencies ...ACKDependency) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.broken {
		return ErrCheckpointSequence
	}
	if len(position) > t.maxPositionBytes || len(stream) > t.maxPositionBytes {
		return ErrCheckpointLimit
	}
	s := t.streams[stream]
	if s == nil {
		if len(t.streams) >= t.maxStreams {
			return ErrCheckpointLimit
		}
		s = &ackStream{}
		// Do not retain a potentially large backing string for a short key.
		stream = strings.Clone(stream)
	}
	last := s.last
	for _, dep := range dependencies {
		if dep.owner != t || dep.index == 0 || dep.index > t.count {
			return ErrCheckpointDependency
		}
		if dep.index > last {
			last = dep.index
		}
	}
	c := &ackCandidate{position: append([]byte(nil), position...), index: last}
	s.last = last
	if s.probe == nil {
		s.probe = c
	} else {
		s.newer = c
	}
	t.streams[stream] = s
	return nil
}

// Confirm snapshots eligible candidates BEFORE GetUnsent. Errors never count
// as empty. Only that snapshot can advance: an empty observation cannot bless a
// candidate added during/after the read. The returned positions are consumed
// from the tracker; the caller must retain them until CAS succeeds (and discard
// them on a fence conflict). Shutdown must stop/join producers, Confirm and save
// within its deadline BEFORE closing the client and releasing its lease.
func (t *ACKTracker) Confirm() (map[string][]byte, error) {
	t.confirmMu.Lock()
	defer t.confirmMu.Unlock()
	t.mu.Lock()
	if t.broken {
		t.mu.Unlock()
		return nil, ErrCheckpointSequence
	}
	type snapshot struct {
		stream       string
		probe, newer *ackCandidate
	}
	eligible := make([]snapshot, 0, len(t.streams))
	for stream, s := range t.streams {
		if s.probe != nil {
			eligible = append(eligible, snapshot{stream, s.probe, s.newer})
		}
	}
	t.mu.Unlock()
	pending, err := t.client.GetUnsent()
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.broken {
		return nil, ErrCheckpointSequence
	}
	// Convert to nonwrapping local indexes only after checking against the
	// lifetime bound. A serialized in-flight Ship can contribute exactly one
	// extra pending tail sequence, but no candidate can yet depend on it. Do
	// not let that tail prevent confirmation of older, registered admissions.
	frontier := t.count + 1
	limit := t.count
	if t.inFlight && t.count > 0 {
		limit++
	}
	for i, msg := range pending {
		if msg == nil {
			return nil, ErrCheckpointSequence
		}
		index := msg.SeqNum - t.first
		if index >= limit || (i > 0 && index != frontier-1+uint64(i)) {
			return nil, ErrCheckpointSequence
		}
		if i == 0 {
			frontier = index + 1
		}
	}
	confirmed := make(map[string][]byte)
	for _, snap := range eligible {
		c := snap.probe
		if c.index >= frontier {
			continue
		}
		if snap.newer != nil && snap.newer.index < frontier {
			c = snap.newer
		}
		confirmed[snap.stream] = append([]byte(nil), c.position...)
		s := t.streams[snap.stream]
		if s.newer == c {
			s.probe, s.newer = nil, nil
		} else {
			// A newer candidate added during the read stays pending even
			// if this observation happened to cover its dependency too.
			s.probe, s.newer = s.newer, nil
		}
	}
	return confirmed, nil
}
