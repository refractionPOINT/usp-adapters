package vendorerr

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for " + what)
	}
}

// Close waits for a callback that is running, drops what is still queued, and
// nothing is delivered afterwards.
func TestNotifierCloseWaitsForRunningCallbackAndDropsTheRest(t *testing.T) {
	var n Notifier
	var delivered, finished atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	deliver := func(error) {
		if delivered.Add(1) == 1 {
			close(started)
			<-release
		}
		finished.Add(1)
	}
	require.True(t, n.Notify(errors.New("first"), deliver))
	waitFor(t, started, "the first callback")
	require.True(t, n.Notify(errors.New("queued"), deliver))

	closed := make(chan struct{})
	go func() { n.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned while a callback was still running")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	waitFor(t, closed, "Close after the callback finished")
	require.Equal(t, int32(1), finished.Load())
	require.False(t, n.Notify(errors.New("late"), deliver), "a closed notifier refuses reports")
	n.Flush()
	require.Equal(t, int32(1), delivered.Load(), "queued and late reports are dropped")
}

// A callback that closes its own notifier (OnError calling the adapter's
// Close) waits out the bounded wait and then proceeds; it never hangs.
func TestNotifierCallbackCloseWaitsOnlyTheBound(t *testing.T) {
	var n Notifier
	returned := make(chan time.Duration, 1)
	require.True(t, n.Notify(errors.New("x"), func(error) {
		start := time.Now()
		n.Close()
		returned <- time.Since(start)
	}))
	select {
	case d := <-returned:
		require.GreaterOrEqual(t, d, NotifierCloseWait)
		require.Less(t, d, NotifierCloseWait+4*time.Second)
	case <-time.After(10 * time.Second):
		t.Fatal("Close from the callback never returned")
	}
}

// A callback that does not return must not make reports pile up: at most one
// callback runs and the queue is bounded; the overflow is refused, not hidden.
func TestNotifierBoundsPileUpWhileCallbackBlocks(t *testing.T) {
	var n Notifier
	var running, maxRunning, delivered atomic.Int32
	release := make(chan struct{})
	deliver := func(error) {
		cur := running.Add(1)
		for {
			m := maxRunning.Load()
			if cur <= m || maxRunning.CompareAndSwap(m, cur) {
				break
			}
		}
		<-release
		running.Add(-1)
		delivered.Add(1)
	}
	accepted := 0
	for i := range 100 {
		if n.Notify(fmt.Errorf("report %d", i), deliver) {
			accepted++
		}
	}
	require.LessOrEqual(t, accepted, notifyQueue+1)
	close(release)
	n.Flush()
	require.Equal(t, int32(1), maxRunning.Load())
	require.Equal(t, int32(accepted), delivered.Load(), "every accepted report is delivered")
	n.Close()
}

// An identical report already queued is accepted without being queued again, so
// a repeating error cannot fill the queue and starve a different report.
func TestNotifierCoalescesIdenticalQueuedReports(t *testing.T) {
	var n Notifier
	var mu sync.Mutex
	var got []string
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	deliver := func(err error) {
		once.Do(func() { close(started); <-release })
		mu.Lock()
		got = append(got, err.Error())
		mu.Unlock()
	}
	require.True(t, n.Notify(errors.New("blocker"), deliver))
	waitFor(t, started, "the blocking callback")
	for range 50 {
		require.True(t, n.Notify(errors.New("credentials rejected"), deliver))
	}
	require.True(t, n.Notify(errors.New("outage"), deliver), "spam did not use up the queue")
	close(release)
	n.Flush()
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"blocker", "credentials rejected", "outage"}, got)
	n.Close()
}

// A refused report is not lost for good: the caller sees false, keeps its
// one-shot flag unset, and delivers on a later attempt once the callback is free.
func TestNotifierRefusedReportCanBeSentAgainAfterCallbackIsReleased(t *testing.T) {
	var n Notifier
	var mu sync.Mutex
	var got []string
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	deliver := func(err error) {
		once.Do(func() { close(started); <-release })
		mu.Lock()
		got = append(got, err.Error())
		mu.Unlock()
	}
	require.True(t, n.Notify(errors.New("blocker"), deliver))
	waitFor(t, started, "the blocking callback")
	for i := range notifyQueue {
		require.True(t, n.Notify(fmt.Errorf("noise %d", i), deliver))
	}
	outage := errors.New("outage lasted a continuous retry interval")
	reported := n.Notify(outage, deliver)
	require.False(t, reported, "queue full: the outage report is refused")

	close(release)
	n.Flush()
	reported = n.Notify(outage, deliver) // the failure goes on, the caller tries again
	require.True(t, reported)
	n.Flush()
	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, got, outage.Error())
	n.Close()
}

// The callback receives the very error value that was queued, so a sentinel in
// its chain (embedders test errors.Is on it) survives asynchronous delivery.
func TestNotifierDeliversTheOriginalErrorValue(t *testing.T) {
	var n Notifier
	defer n.Close()
	sentinel := errors.New("sentinel")
	sent := fmt.Errorf("vendor text: %w", sentinel)
	got := make(chan error, 1)
	require.True(t, n.Notify(sent, func(err error) { got <- err }))
	select {
	case err := <-got:
		require.Same(t, sent, err)
		require.ErrorIs(t, err, sentinel)
	case <-time.After(5 * time.Second):
		t.Fatal("report was not delivered")
	}
}
