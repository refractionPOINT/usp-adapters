package usp_pandadoc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	pollMaxCatchup    = 24 * time.Hour
	logPollMaxBackoff = 5 * time.Minute
	pollFailureLimit  = 30 * time.Minute
)

// pollState retains the unread range until a complete poll succeeds. It is
// owned by one polling goroutine; neither state nor dedupe survives a restart.
type pollState struct {
	completeEnd  time.Time
	overlap      time.Duration
	backoff      time.Duration
	failureSince time.Time
	lastFailure  error
}

func newPollState(now time.Time, overlap time.Duration) *pollState {
	return &pollState{completeEnd: now, overlap: overlap}
}

// window bounds catch-up memory and work. A caller must warn when expired is
// true, since records older than the returned start may remain unread.
func (p *pollState) window(now time.Time) (start, end time.Time, expired bool) {
	start = p.completeEnd.Add(-p.overlap)
	if floor := now.Add(-pollMaxCatchup); start.Before(floor) {
		start, expired = floor, true
	}
	return start, now, expired
}

func (p *pollState) complete(end time.Time) {
	p.completeEnd = end
	p.backoff = 0
	p.failureSince = time.Time{}
	p.lastFailure = nil
}

type logPollRetry struct {
	err        error
	retryAfter string
}

func (e *logPollRetry) Error() string { return e.err.Error() }
func (e *logPollRetry) Unwrap() error { return e.err }

// newPollRetry marks an error as recoverable without invoking a consumer's
// fatal error callback. Retry-After is retained for the polling loop to honor.
func newPollRetry(err error, retryAfter string) error {
	return &logPollRetry{err: err, retryAfter: retryAfter}
}

func pollRetryableStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

// retryDelay doubles fallback backoff from 30s to 5m. A longer server deadline
// always wins; capping Retry-After could send requests before the server permits.
func (p *pollState) retryDelay(err error, now time.Time) (time.Duration, bool) {
	var retry *logPollRetry
	if !errors.As(err, &retry) {
		return 0, false
	}
	if p.failureSince.IsZero() {
		p.failureSince = now
	}
	p.lastFailure = err
	if p.backoff == 0 {
		p.backoff = 30 * time.Second
	} else {
		p.backoff *= 2
	}
	if p.backoff > logPollMaxBackoff {
		p.backoff = logPollMaxBackoff
	}
	delay := p.backoff
	header := strings.TrimSpace(retry.retryAfter)
	if seconds, err := strconv.ParseInt(header, 10, 64); err == nil && seconds >= 0 {
		const maxDuration = time.Duration(1<<63 - 1)
		serverDelay := maxDuration
		if seconds <= int64(maxDuration/time.Second) {
			serverDelay = time.Duration(seconds) * time.Second
		}
		if serverDelay > delay {
			delay = serverDelay
		}
	} else if deadline, err := http.ParseTime(header); err == nil && deadline.Sub(now) > delay {
		delay = deadline.Sub(now)
	}
	return delay, true
}

// A failed poll attempt starts the continuous-failure clock. Partial pages
// cannot reset it; only complete() after an entirely successful poll does.
func (p *pollState) failureError(now time.Time) error {
	if !p.failureSince.IsZero() && !now.Before(p.failureSince.Add(pollFailureLimit)) {
		return fmt.Errorf("poll failed continuously for at least 30 minutes: %w", p.lastFailure)
	}
	return nil
}

// Wake to escalate at the deadline even if Retry-After forbids another request
// until later. The caller checks failureError before sending any request.
func (p *pollState) budgetDelay(delay time.Duration, now time.Time) time.Duration {
	if !p.failureSince.IsZero() {
		remaining := p.failureSince.Add(pollFailureLimit).Sub(now)
		if remaining < 0 {
			return 0
		}
		if delay > remaining {
			return remaining
		}
	}
	return delay
}

// USP interprets a zero shipping timeout as unlimited. Keep an expired
// budget strictly positive so a full buffer cannot prevent fatal escalation.
func (p *pollState) shipTimeout(delay time.Duration, now time.Time) time.Duration {
	if bounded := p.budgetDelay(delay, now); bounded > 0 {
		return bounded
	}
	return time.Nanosecond
}

func (p *pollState) requestContext(parent context.Context, now time.Time) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if !p.failureSince.IsZero() {
		return context.WithTimeout(parent, p.budgetDelay(pollFailureLimit, now))
	}
	return context.WithCancel(parent)
}
