package vendorerr

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
	n.Notify(errors.New("first"), deliver)
	<-started
	n.Notify(errors.New("queued"), deliver)

	closed := make(chan struct{})
	go func() { n.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned while a callback was still running")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the callback finished")
	}
	require.Equal(t, int32(1), finished.Load())
	n.Notify(errors.New("late"), deliver)
	n.Flush()
	require.Equal(t, int32(1), delivered.Load(), "queued and late reports are dropped")
}

// A callback that closes its own notifier (OnError calling the adapter's
// Close) must not wait for itself.
func TestNotifierCallbackCanCloseItsOwnNotifier(t *testing.T) {
	var n Notifier
	returned := make(chan time.Duration, 1)
	n.Notify(errors.New("x"), func(error) {
		start := time.Now()
		n.Close()
		returned <- time.Since(start)
	})
	select {
	case d := <-returned:
		require.Less(t, d, NotifierCloseWait/2)
	case <-time.After(NotifierCloseWait):
		t.Fatal("Close from the callback waited on itself")
	}
}

// A callback that does not return must not make reports pile up: at most one
// callback runs and the queue is bounded.
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
	for range 100 {
		n.Notify(errors.New("x"), deliver)
	}
	close(release)
	n.Flush()
	require.Equal(t, int32(1), maxRunning.Load())
	require.LessOrEqual(t, delivered.Load(), int32(notifyQueue+1))
	n.Close()
}
