package utils

import (
	"errors"
	"sync"
	"time"
)

type Deduper interface {
	CheckAndAdd(key string) (alreadyExists bool)
	Close()
}

// CheckingDeduper is a Deduper that can also report whether a key was already
// recorded without recording it. It lets a caller skip work that was already
// done, and record the key only once the work has succeeded, so that a failure
// leaves the work to be retried instead of marking it done.
type CheckingDeduper interface {
	Deduper
	Check(key string) (exists bool)
}

// DedupeKey identifies an item to deduplicate. Time is when the item was
// created at the source (zero if unknown). Callers must pass the same Time
// every time for the same Key; implementations may use it to group markers.
type DedupeKey struct {
	Key  string
	Time time.Time
}

// BatchDeduper is a CheckingDeduper that checks and records many keys per call.
type BatchDeduper interface {
	CheckingDeduper
	// CheckMany reports, for each key in order, whether it was already
	// recorded. It never records. On a backend error it fails open: the
	// affected keys report false.
	CheckMany(keys []DedupeKey) []bool
	// AddMany records the keys as done. Best effort.
	AddMany(keys []DedupeKey)
}

var _ BatchDeduper = (*localDeduper)(nil)

type localDeduper struct {
	window   time.Duration
	ttl      time.Duration
	nBuckets int
	nCurrent int

	stopEvt *Event
	m       sync.Mutex
	windows []map[string]struct{}
}

func NewLocalDeduper(window time.Duration, ttl time.Duration) (Deduper, error) {
	if window > ttl {
		return nil, errors.New("window is larger than ttl")
	}
	nBuckets := int(ttl / window)
	if nBuckets < 1 {
		nBuckets = 1
	}
	nCurrent := 0
	windows := make([]map[string]struct{}, nBuckets)
	windows[nCurrent] = make(map[string]struct{})

	d := &localDeduper{
		window:   window,
		ttl:      ttl,
		nBuckets: nBuckets,
		nCurrent: nCurrent,
		stopEvt:  NewEvent(),
		windows:  windows,
	}

	go func() {
		for !d.stopEvt.WaitFor(d.window) {
			d.m.Lock()
			d.nCurrent++
			if d.nCurrent >= d.nBuckets {
				d.nCurrent = 0
			}
			d.windows[d.nCurrent] = make(map[string]struct{})
			d.m.Unlock()
		}
	}()

	return d, nil
}

func (d *localDeduper) CheckAndAdd(key string) bool {
	d.m.Lock()
	defer d.m.Unlock()

	// Check all the buckets for the key.
	for i := 0; i < d.nBuckets; i++ {
		if len(d.windows[i]) == 0 {
			continue
		}
		if _, ok := d.windows[i][key]; ok {
			return true
		}
	}

	// Add the key to the current bucket.
	d.windows[d.nCurrent][key] = struct{}{}

	return false
}

func (d *localDeduper) Check(key string) bool {
	d.m.Lock()
	defer d.m.Unlock()

	for i := 0; i < d.nBuckets; i++ {
		if _, ok := d.windows[i][key]; ok {
			return true
		}
	}
	return false
}

// CheckMany implements BatchDeduper. The local deduper ignores key times.
func (d *localDeduper) CheckMany(keys []DedupeKey) []bool {
	d.m.Lock()
	defer d.m.Unlock()

	out := make([]bool, len(keys))
	for i, k := range keys {
		for _, w := range d.windows {
			if _, ok := w[k.Key]; ok {
				out[i] = true
				break
			}
		}
	}
	return out
}

// AddMany implements BatchDeduper. The local deduper ignores key times.
func (d *localDeduper) AddMany(keys []DedupeKey) {
	d.m.Lock()
	defer d.m.Unlock()

	for _, k := range keys {
		d.windows[d.nCurrent][k.Key] = struct{}{}
	}
}

func (d *localDeduper) Close() {
	d.stopEvt.Set()
}
