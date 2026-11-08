package usp_ms_graph

import (
	"context"
	"errors"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/vendorerr"
)

const maxNoProgressInterval = time.Hour

var errRetryWindow = errors.New("Graph retry window exhausted")

// Only the reader goroutine accesses this state. A fetched page resets the
// failure clock only once all usable records on it have shipped. Replaying an
// already completed page without new shipments is not fresh forward progress.
type graphRetryHealth struct {
	windowStart    time.Time
	lastError      error
	completedPages map[string]bool
}

func (h *graphRetryHealth) note(now time.Time, err error) {
	if h.windowStart.IsZero() {
		h.windowStart = now
	}
	h.lastError = err
}
func (h *graphRetryHealth) pageComplete(now time.Time, key string, shippedNew bool) {
	if h.completedPages == nil {
		h.completedPages = map[string]bool{}
	}
	if !h.completedPages[key] || shippedNew {
		h.windowStart = now
		h.lastError = nil
	}
	h.completedPages[key] = true
}
func (h *graphRetryHealth) reset(now time.Time) { *h = graphRetryHealth{windowStart: now} }
func (h *graphRetryHealth) remaining(now time.Time) time.Duration {
	if h.windowStart.IsZero() {
		return maxNoProgressInterval
	}
	return maxNoProgressInterval - now.Sub(h.windowStart)
}
func (h *graphRetryHealth) escalation(now time.Time) error {
	if h.lastError == nil || h.windowStart.IsZero() || h.remaining(now) > 0 {
		return nil
	}
	// The host restarts a fatal instance from now, losing its in-memory cursor
	// and possibly leaving backlog behind. This backlog-loss tradeoff is accepted
	// after one hour without a fully shipped page making forward progress. A
	// recovering scan draining pages is allowed to continue beyond one hour.
	return vendorerr.Kind(errRetryWindow, "no Graph page progress for at least one hour; last error: "+h.lastError.Error(), nil)
}

type graphRuntime struct {
	now  func() time.Time
	wait func(context.Context, time.Duration) error
}

func defaultGraphRuntime() graphRuntime {
	return graphRuntime{now: time.Now, wait: func(ctx context.Context, delay time.Duration) error {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}}
}
