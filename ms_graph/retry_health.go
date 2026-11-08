package usp_ms_graph

import (
	"context"
	"errors"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/vendorerr"
)

const maxRequestFailureInterval = time.Hour

var errRetryWindow = errors.New("Graph request failure interval exceeded")

// Escalate only a failed request. Any successful Graph fetch resets this
// timestamp, even if its envelope is malformed or a later page remains stuck.
// Rotating continuation tokens and successful head responses can therefore
// keep a failing tail alive; this is an accepted limit, not replay detection.
func (a *MsGraphAdapter) requestFailure(err error) error {
	if errors.Is(err, errRetry) && a.runtime.now().Sub(a.lastSuccessfulFetch) > maxRequestFailureInterval {
		// A fatal hosted restart starts from now and can abandon older backlog.
		return vendorerr.Kind(errRetryWindow, "request failed more than one hour after the last successful Graph fetch: "+err.Error(), nil)
	}
	return err
}

// Complete fatal delivery before the sender returns and chStopped closes.
// A callback may synchronously Close this adapter, cancelling ctx and waiting
// for the sender, so cancellation must be able to break the Flush wait.
func (a *MsGraphAdapter) notifyFatal(err error) {
	if !a.notifier.Notify(err, a.conf.ClientOptions.OnError) {
		return
	}
	flushed := make(chan struct{})
	go func() { a.notifier.Flush(); close(flushed) }()
	select {
	case <-flushed:
	case <-a.ctx.Done():
	}
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
