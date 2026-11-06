package vendorerr

import (
	"sync"
	"time"
)

const (
	// notifyQueue bounds the reports waiting for a slow callback; beyond it
	// new reports are refused rather than piling up goroutines or memory.
	notifyQueue = 8
	// NotifierCloseWait is how long Notifier.Close waits for a callback that is
	// still running, so a callback that never returns, or that calls Close
	// itself, delays Close by at most this long.
	NotifierCloseWait = time.Second
)

type notification struct {
	err     error
	text    string
	deliver func(error)
}

// Notifier delivers error reports to an embedder callback (OnError) from one
// dedicated goroutine, so a callback can neither block nor deadlock the
// goroutines that produce the reports, even when it calls the adapter's Close.
// At most one callback runs at a time. Reports queue up to a small bound and
// are then refused; a report whose text equals one already queued is not queued
// twice. Close drops what is still queued and waits (bounded by
// NotifierCloseWait) for a running callback, so no callback starts after Close.
//
// The zero value is ready to use.
type Notifier struct {
	mu      sync.Mutex
	cond    *sync.Cond
	started bool
	closed  bool
	running bool
	queue   []notification
	done    chan struct{}
}

// Notify queues err for deliver and reports whether the report was accepted,
// meaning it is queued (or an identical one already is) and will be delivered
// unless Close comes first. A caller that sends a one-shot report must only
// consider it sent when Notify returns true, and try again later otherwise.
func (n *Notifier) Notify(err error, deliver func(error)) bool {
	if deliver == nil || err == nil {
		return false
	}
	text := err.Error()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return false
	}
	for _, q := range n.queue {
		if q.text == text {
			return true
		}
	}
	if len(n.queue) >= notifyQueue {
		return false // callback is not keeping up
	}
	if !n.started {
		n.started = true
		n.cond = sync.NewCond(&n.mu)
		n.done = make(chan struct{})
		go n.run()
	}
	n.queue = append(n.queue, notification{err, text, deliver})
	n.cond.Broadcast()
	return true
}

func (n *Notifier) run() {
	defer close(n.done)
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for len(n.queue) == 0 && !n.closed {
			n.cond.Wait()
		}
		if n.closed {
			n.queue = nil
			n.cond.Broadcast()
			return
		}
		item := n.queue[0]
		n.queue = n.queue[1:]
		n.running = true
		n.mu.Unlock()
		item.deliver(item.err)
		n.mu.Lock()
		n.running = false
		n.cond.Broadcast()
	}
}

// Flush waits until every report accepted so far has been delivered. For tests.
func (n *Notifier) Flush() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for n.started && (len(n.queue) > 0 || n.running) {
		n.cond.Wait()
	}
}

// Close stops delivery: queued reports are dropped, later ones refused, and it
// waits up to NotifierCloseWait for a callback that is running. A callback
// calling Close (through the adapter's Close) therefore waits out the bound.
func (n *Notifier) Close() {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.closed = true
	started, done := n.started, n.done
	if started {
		n.cond.Broadcast()
	}
	n.mu.Unlock()
	if !started {
		return
	}
	timer := time.NewTimer(NotifierCloseWait)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}
