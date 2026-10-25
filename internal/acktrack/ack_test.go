package acktrack

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
)

// Model the SDK contract, not a socket-write/empty-byte-buffer approximation:
// admission retains messages until a cumulative ACK explicitly removes them.
type observerTestClient struct {
	mu                                           sync.Mutex
	next                                         uint64
	pending                                      []*protocol.DataMessage
	shipError, readError                         error
	beforeRead, afterRead, beforeShip, afterShip func()
}

func (c *observerTestClient) Ship(m *protocol.DataMessage, _ time.Duration) error {
	if c.beforeShip != nil {
		c.beforeShip()
	}
	c.mu.Lock()
	if c.shipError != nil {
		c.mu.Unlock()
		return c.shipError
	}
	m.SeqNum = c.next
	c.next++
	copy := *m
	c.pending = append(c.pending, &copy)
	c.mu.Unlock()
	if c.afterShip != nil {
		c.afterShip()
	}
	return nil
}

func (c *observerTestClient) GetUnsent() ([]*protocol.DataMessage, error) {
	if c.beforeRead != nil {
		c.beforeRead()
	}
	c.mu.Lock()
	pending := append([]*protocol.DataMessage(nil), c.pending...)
	err := c.readError
	c.mu.Unlock()
	if c.afterRead != nil {
		c.afterRead()
	}
	return pending, err
}

func (c *observerTestClient) ack(seq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, m := range c.pending {
		if m.SeqNum == seq {
			c.pending = c.pending[i+1:]
			return
		}
	}
}

func observerTracker(t *testing.T, c Client) *ACKTracker {
	t.Helper()
	tracker, err := NewACKTracker(c, 4, 128)
	require.NoError(t, err)
	return tracker
}

func observerShip(t *testing.T, tracker *ACKTracker) ACKDependency {
	t.Helper()
	dep, err := tracker.Ship(&protocol.DataMessage{TextPayload: "record"}, time.Second)
	require.NoError(t, err)
	return dep
}

func observerConfirm(t *testing.T, tracker *ACKTracker, want map[string]string) {
	t.Helper()
	got, err := tracker.Confirm()
	require.NoError(t, err)
	strings := make(map[string]string)
	for stream, position := range got {
		strings[stream] = string(position)
	}
	require.Equal(t, want, strings)
}

func TestACKTrackerWithheldAndPartialCumulativeACK(t *testing.T) {
	c := &observerTestClient{next: 1}
	tracker := observerTracker(t, c)
	a := observerShip(t, tracker)
	require.NoError(t, tracker.Candidate("stream", []byte("page1"), a))
	b := observerShip(t, tracker)
	require.NoError(t, tracker.Candidate("stream", []byte("page2"), b))
	observerConfirm(t, tracker, map[string]string{})
	c.ack(1)
	observerConfirm(t, tracker, map[string]string{"stream": "page1"})
	observerConfirm(t, tracker, map[string]string{})
	c.ack(2)
	observerConfirm(t, tracker, map[string]string{"stream": "page2"})
	observerConfirm(t, tracker, map[string]string{})
}

func TestACKTrackerContinuousTailBoundedAndProgress(t *testing.T) {
	c := &observerTestClient{next: 1}
	tracker := observerTracker(t, c)
	for batch := 0; batch < 100; batch++ {
		first := c.next
		for i := 0; i < 100; i++ {
			dep := observerShip(t, tracker)
			require.NoError(t, tracker.Candidate("stream", []byte(fmt.Sprint(c.next-1)), dep))
		}
		s := tracker.streams["stream"]
		require.NotNil(t, s.probe)
		require.NotNil(t, s.newer)
		require.Len(t, tracker.streams, 1)
		// Never empty the buffer: only the frozen probe gets an ACK.
		probeSeq := tracker.first + s.probe.index - 1
		c.ack(probeSeq)
		observerConfirm(t, tracker, map[string]string{"stream": fmt.Sprint(probeSeq)})
		require.NotEmpty(t, c.pending)
		require.GreaterOrEqual(t, tracker.streams["stream"].probe.index, first)
	}
}

func TestACKTrackerSnapshotExcludesNewCandidates(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint("afterRead=", after), func(t *testing.T) {
			c := &observerTestClient{next: 1}
			tracker := observerTracker(t, c)
			require.NoError(t, tracker.Candidate("stream", []byte("old empty page")))
			add := func() {
				// For afterRead=true this admission happens after an empty
				// SDK snapshot: that empty result must not bless the new page.
				dep := observerShip(t, tracker)
				require.NoError(t, tracker.Candidate("stream", []byte("new page"), dep))
				require.NoError(t, tracker.Candidate("new stream", []byte("new empty page")))
			}
			if after {
				c.afterRead = add
			} else {
				c.beforeRead = add
			}
			observerConfirm(t, tracker, map[string]string{"stream": "old empty page"})
			c.beforeRead, c.afterRead = nil, nil
			observerConfirm(t, tracker, map[string]string{"new stream": "new empty page"})
			c.ack(1)
			observerConfirm(t, tracker, map[string]string{"stream": "new page"})
		})
	}
}

func TestACKTrackerReadFailureIsNotEmpty(t *testing.T) {
	c := &observerTestClient{next: 1}
	tracker := observerTracker(t, c)
	dep := observerShip(t, tracker)
	require.NoError(t, tracker.Candidate("stream", []byte("page"), dep))
	c.ack(1)
	c.readError = errors.New("read unavailable")
	got, err := tracker.Confirm()
	require.ErrorIs(t, err, c.readError)
	require.Nil(t, got)
	c.readError = nil
	observerConfirm(t, tracker, map[string]string{"stream": "page"})
}

func TestACKTrackerWrapAndSequenceRejection(t *testing.T) {
	c := &observerTestClient{next: math.MaxUint64 - 1}
	tracker := observerTracker(t, c)
	a := observerShip(t, tracker)
	require.NoError(t, tracker.Candidate("stream", []byte("before wrap"), a))
	observerShip(t, tracker)
	b := observerShip(t, tracker) // sequence zero is a valid admission
	require.NoError(t, tracker.Candidate("stream", []byte("after wrap"), b))
	c.ack(math.MaxUint64 - 1)
	observerConfirm(t, tracker, map[string]string{"stream": "before wrap"})
	c.ack(math.MaxUint64)
	observerConfirm(t, tracker, map[string]string{})
	c.ack(0)
	observerConfirm(t, tracker, map[string]string{"stream": "after wrap"})

	for _, jump := range []uint64{0, 3, ackSequenceHalf, ackSequenceHalf + 1} {
		t.Run(fmt.Sprint("jump=", jump), func(t *testing.T) {
			c := &observerTestClient{next: 0}
			tracker := observerTracker(t, c)
			dep := observerShip(t, tracker)
			require.NoError(t, tracker.Candidate("stream", []byte("page"), dep))
			c.next = jump
			_, err := tracker.Ship(&protocol.DataMessage{}, time.Second)
			require.ErrorIs(t, err, ErrSequence)
			_, err = tracker.Confirm()
			require.ErrorIs(t, err, ErrSequence, "invalid ordering must latch closed")
		})
	}
}

func TestACKTrackerLifetimeBoundBeforeAdmission(t *testing.T) {
	c := &observerTestClient{next: 1}
	tracker := observerTracker(t, c)
	// Exercise the otherwise impractically large lifetime boundary directly.
	tracker.count = ackSequenceHalf - 1
	_, err := tracker.Ship(&protocol.DataMessage{}, time.Second)
	require.ErrorIs(t, err, ErrSequence)
	require.Empty(t, c.pending, "must reject before admitting an ambiguous sequence")
}

func TestACKTrackerRejectsInvalidPendingOrdering(t *testing.T) {
	for _, seqs := range [][]uint64{{1, 3}, {2, 1}, {1, 1}, {1, ackSequenceHalf + 1}, {0}, {4}} {
		t.Run(fmt.Sprint(seqs), func(t *testing.T) {
			c := &observerTestClient{next: 1}
			tracker := observerTracker(t, c)
			dep := observerShip(t, tracker)
			observerShip(t, tracker)
			observerShip(t, tracker)
			require.NoError(t, tracker.Candidate("stream", []byte("page"), dep))
			c.pending = nil
			for _, seq := range seqs {
				c.pending = append(c.pending, &protocol.DataMessage{SeqNum: seq})
			}
			_, err := tracker.Confirm()
			require.ErrorIs(t, err, ErrSequence)
			require.NotNil(t, tracker.streams["stream"].probe)
		})
	}
}

func TestACKTrackerPendingSuppressionAndEmptyPageDependency(t *testing.T) {
	c := &observerTestClient{next: 1}
	tracker := observerTracker(t, c)
	own := observerShip(t, tracker)
	suppressed := observerShip(t, tracker)
	latest, err := own.Include(suppressed)
	require.NoError(t, err)
	latest, err = latest.Include(own) // must retain MAX, not the last argument
	require.NoError(t, err)
	require.NoError(t, tracker.Candidate("stream", []byte("page with suppressed pending record"), latest))
	require.NoError(t, tracker.Candidate("stream", []byte("following empty page")))
	c.ack(1)
	observerConfirm(t, tracker, map[string]string{})
	c.ack(2)
	observerConfirm(t, tracker, map[string]string{"stream": "following empty page"})
}

func TestACKTrackerFailedAndInFlightAdmissionCannotSupplyDependency(t *testing.T) {
	c := &observerTestClient{next: 1, shipError: errors.New("admission failed")}
	tracker := observerTracker(t, c)
	dep, err := tracker.Ship(&protocol.DataMessage{SeqNum: 123}, time.Second)
	require.ErrorIs(t, err, c.shipError)
	require.ErrorIs(t, tracker.Candidate("stream", []byte("failed page"), dep), ErrDependency)
	_, err = (ACKDependency{}).Include(dep)
	require.ErrorIs(t, err, ErrDependency)
	observerConfirm(t, tracker, map[string]string{})

	// Source orchestration waits for admission before suppressing duplicates
	// or completing the page. Observation still works while Ship is blocked.
	c.shipError = nil
	started, release := make(chan struct{}), make(chan struct{})
	c.beforeShip = func() { close(started); <-release }
	type result struct {
		dep ACKDependency
		err error
	}
	done := make(chan result, 1)
	go func() {
		dep, err := tracker.Ship(&protocol.DataMessage{}, time.Second)
		done <- result{dep, err}
	}()
	<-started
	observerConfirm(t, tracker, map[string]string{})
	close(release)
	admitted := <-done
	require.NoError(t, admitted.err)
	require.NoError(t, tracker.Candidate("stream", []byte("retry completed"), admitted.dep))
	observerConfirm(t, tracker, map[string]string{})
	c.ack(1)
	observerConfirm(t, tracker, map[string]string{"stream": "retry completed"})
}

func TestACKTrackerUnregisteredTailDoesNotStarveFrozenProbe(t *testing.T) {
	c := &observerTestClient{next: 1}
	tracker := observerTracker(t, c)
	dep := observerShip(t, tracker)
	require.NoError(t, tracker.Candidate("stream", []byte("frozen probe"), dep))
	admitted, release := make(chan struct{}), make(chan struct{})
	c.afterShip = func() { close(admitted); <-release }
	done := make(chan error, 1)
	go func() {
		_, err := tracker.Ship(&protocol.DataMessage{}, time.Second)
		done <- err
	}()
	<-admitted // tail is in the SDK but Ship has not returned a dependency
	c.ack(1)
	observerConfirm(t, tracker, map[string]string{"stream": "frozen probe"})
	close(release)
	require.NoError(t, <-done)
}

// The generic tracker cannot infer a source page or partition's completeness.
// Exercise the required caller policy: stop the traversal at a failed send,
// submit no position for that page, and retry it before any later source page.
func TestACKTrackerCallerDoesNotAdvanceAcrossPartialFailedPage(t *testing.T) {
	c := &observerTestClient{next: 1}
	tracker := observerTracker(t, c)
	completePage := func(position string, records int) error {
		var dependency ACKDependency
		for i := 0; i < records; i++ {
			admitted, err := tracker.Ship(&protocol.DataMessage{}, time.Second)
			if err != nil {
				return err // freeze here; do not publish the incomplete page
			}
			dependency, err = dependency.Include(admitted)
			if err != nil {
				return err
			}
		}
		return tracker.Candidate("stream", []byte(position), dependency)
	}
	require.NoError(t, completePage("completed", 1))
	c.ack(1)
	observerConfirm(t, tracker, map[string]string{"stream": "completed"})
	calls := 0
	failure := errors.New("page admission failure")
	c.beforeShip = func() {
		calls++
		if calls == 2 {
			c.shipError = failure
		}
	}
	require.ErrorIs(t, completePage("incomplete", 3), failure)
	require.Equal(t, 2, calls, "stop traversal before the next record/position")
	c.ack(2) // even ACKing the admitted prefix cannot certify the failed page
	observerConfirm(t, tracker, map[string]string{})
	c.beforeShip, c.shipError = nil, nil
	require.NoError(t, completePage("retried complete page", 3))
	c.ack(4)
	observerConfirm(t, tracker, map[string]string{})
	c.ack(5)
	observerConfirm(t, tracker, map[string]string{"stream": "retried complete page"})
}

func TestACKTrackerOwnershipAndLimits(t *testing.T) {
	c := &observerTestClient{next: 1}
	tracker, err := NewACKTracker(c, 1, 5)
	require.NoError(t, err)
	for _, bounds := range [][2]int{{0, 5}, {1, 0}, {-1, 5}} {
		_, err := NewACKTracker(c, bounds[0], bounds[1])
		require.ErrorIs(t, err, ErrLimit)
	}
	_, err = NewACKTracker(nil, 1, 5)
	require.ErrorIs(t, err, ErrLimit)
	other := observerTracker(t, &observerTestClient{next: 1})
	foreign := observerShip(t, other)
	require.ErrorIs(t, tracker.Candidate("s", []byte("page"), foreign), ErrDependency)
	dep := observerShip(t, tracker)
	_, err = dep.Include(foreign)
	require.ErrorIs(t, err, ErrDependency)
	position := []byte("page1")
	require.NoError(t, tracker.Candidate("s", position, dep))
	position[0] = 'X'
	require.ErrorIs(t, tracker.Candidate("s", []byte("too big")), ErrLimit)
	require.ErrorIs(t, tracker.Candidate("other", []byte("page")), ErrLimit)
	c.ack(1)
	retained := tracker.streams["s"].probe
	got, err := tracker.Confirm()
	require.NoError(t, err)
	require.Equal(t, "page1", string(got["s"]))
	got["s"][0] = 'Y'
	require.Equal(t, "page1", string(retained.position), "returned state must not alias a candidate")
	require.Equal(t, "Xage1", string(position))
	// A new empty page still inherits the confirmed dependency, and returned
	// bytes do not alias another candidate or the input.
	require.NoError(t, tracker.Candidate("s", []byte("page2")))
	observerConfirm(t, tracker, map[string]string{"s": "page2"})
}

func TestACKTrackerConcurrentAdmissionsCandidatesAndObservations(t *testing.T) {
	c := &observerTestClient{next: 1}
	tracker := observerTracker(t, c)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(stream string) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				dep := observerShip(t, tracker)
				require.NoError(t, tracker.Candidate(stream, []byte(fmt.Sprint(i)), dep))
				c.mu.Lock()
				seq := c.next - 1
				c.mu.Unlock()
				c.ack(seq)
				_, err := tracker.Confirm()
				// Another admission may still be registering its sequence
				// after the SDK pending snapshot: this observation defers.
				require.True(t, err == nil || errors.Is(err, ErrSequence))
			}
		}(fmt.Sprint(worker))
	}
	wg.Wait()
	// Submit final empty pages only after producers join; they must all be
	// confirmable and there must be no forgotten pending dependency.
	for worker := 0; worker < 4; worker++ {
		require.NoError(t, tracker.Candidate(fmt.Sprint(worker), []byte("joined")))
	}
	observerConfirm(t, tracker, map[string]string{"0": "joined", "1": "joined", "2": "joined", "3": "joined"})
}

func TestACKTrackerCandidateRejectsForeignSuccessfulAdmission(t *testing.T) {
	first := observerTracker(t, &observerTestClient{next: 1})
	second := observerTracker(t, &observerTestClient{next: 1})
	own := observerShip(t, first)
	foreign := observerShip(t, second)
	require.NoError(t, first.Candidate("own", []byte("one"), own))
	require.ErrorIs(t, first.Candidate("foreign", []byte("two"), foreign), ErrDependency)
}
