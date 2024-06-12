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

func (d *localDeduper) Close() {
	d.stopEvt.Set()
}
