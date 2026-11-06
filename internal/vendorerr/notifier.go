package vendorerr

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const (
	// notifyQueue bounds the reports waiting for a slow callback; beyond it
	// new reports are dropped rather than piling up goroutines or memory.
	notifyQueue = 8
	// NotifierCloseWait is how long Notifier.Close waits for a callback that is
	// still running, so a callback that never returns cannot hang Close.
	NotifierCloseWait = 3 * time.Second
)

type notification struct {
	err     error
	deliver func(error)
}

// Notifier delivers error reports to an embedder callback (OnError) from one
// dedicated goroutine, so a callback can neither block nor deadlock the
// goroutines that produce the reports, even when it calls the adapter's Close.
// At most one callback runs at a time, reports queue up to a small bound and
// are then dropped, and Close drops what is still queued and waits (bounded)
// for a running callback, so no callback starts after Close and none is still
// running when Close returns unless it exceeds NotifierCloseWait.
//
// The zero value is ready to use.
type Notifier struct {
	mu      sync.Mutex
	cond    *sync.Cond
	started bool
	closed  bool
	pending int // queued or running reports, for Flush
	queue   chan notification
	quit    chan struct{}
	done    chan struct{}
	gid     uint64 // goroutine running the callbacks, set before the first one
}

// Notify queues err for deliver. A nil deliver or a closed notifier drops it.
func (n *Notifier) Notify(err error, deliver func(error)) {
	if deliver == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	if !n.started {
		n.started = true
		n.cond = sync.NewCond(&n.mu)
		n.queue = make(chan notification, notifyQueue)
		n.quit = make(chan struct{})
		n.done = make(chan struct{})
		go n.run()
	}
	select {
	case n.queue <- notification{err, deliver}:
		n.pending++
	default: // callback is not keeping up
	}
}

func (n *Notifier) run() {
	defer close(n.done)
	n.mu.Lock()
	n.gid = goroutineID()
	n.mu.Unlock()
	for {
		select {
		case item := <-n.queue:
			n.mu.Lock()
			closed := n.closed
			n.mu.Unlock()
			if !closed {
				item.deliver(item.err)
			}
			n.finish()
		case <-n.quit:
			for {
				select {
				case <-n.queue:
					n.finish()
				default:
					return
				}
			}
		}
	}
}

func (n *Notifier) finish() {
	n.mu.Lock()
	n.pending--
	n.cond.Broadcast()
	n.mu.Unlock()
}

// Flush waits until every report queued so far has been delivered. For tests.
func (n *Notifier) Flush() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for n.started && n.pending > 0 {
		n.cond.Wait()
	}
}

// Close stops delivery: queued reports are dropped, later ones ignored, and it
// waits up to NotifierCloseWait for a callback that is running. A callback
// calling Close (through the adapter's Close) is recognised and never waits on
// itself.
func (n *Notifier) Close() {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.closed = true
	started, gid, quit, done := n.started, n.gid, n.quit, n.done
	n.mu.Unlock()
	if !started {
		return
	}
	close(quit)
	if gid == goroutineID() {
		return
	}
	timer := time.NewTimer(NotifierCloseWait)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}

// goroutineID parses the current goroutine's id from its stack header. Only
// used to let a callback's own Close skip waiting for that callback.
func goroutineID() uint64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	if i := bytes.IndexByte(b, ' '); i > 0 {
		b = b[:i]
	}
	id, _ := strconv.ParseUint(string(b), 10, 64)
	return id
}
