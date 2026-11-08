package usp_ms_graph

import (
	"context"
	"errors"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/vendorerr"
)

const maxUnsuccessfulPollInterval = time.Hour

var errRetryWindow = errors.New("Graph retry window exhausted")

// Only the reader goroutine accesses this state. Successful pages and shipments
// do not reset it: only a complete successful poll does.
type graphRetryHealth struct {
	firstFailure time.Time
	lastError    error
}

func (h *graphRetryHealth) note(now time.Time, err error) {
	if h.firstFailure.IsZero() {
		h.firstFailure = now
	}
	h.lastError = err
}
func (h *graphRetryHealth) reset() { *h = graphRetryHealth{} }
func (h *graphRetryHealth) remaining(now time.Time) time.Duration {
	if h.firstFailure.IsZero() {
		return maxUnsuccessfulPollInterval
	}
	return maxUnsuccessfulPollInterval - now.Sub(h.firstFailure)
}
func (h *graphRetryHealth) escalation(now time.Time) error {
	if h.firstFailure.IsZero() || h.remaining(now) > 0 {
		return nil
	}
	// The host restarts a fatal instance from now, losing its in-memory cursor
	// and possibly leaving backlog behind. This backlog-loss tradeoff is accepted
	// after one hour without a successful poll, rather than retrying forever.
	return vendorerr.Kind(errRetryWindow, "no successful Graph poll for at least one hour after a retryable failure; last error: "+h.lastError.Error(), nil)
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
