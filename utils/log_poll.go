package utils

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	LogPollMaxCatchup = 24 * time.Hour
	logPollMaxBackoff = 5 * time.Minute
)

// LogPollState retains the unread range until a complete poll succeeds. It is
// owned by one polling goroutine; neither state nor dedupe survives a restart.
type LogPollState struct {
	completeEnd time.Time
	overlap     time.Duration
	backoff     time.Duration
}

func NewLogPollState(now time.Time, overlap time.Duration) *LogPollState {
	return &LogPollState{completeEnd: now, overlap: overlap}
}

// Window bounds catch-up memory and work. A caller must warn when expired is
// true, since records older than the returned start may remain unread.
func (p *LogPollState) Window(now time.Time) (start, end time.Time, expired bool) {
	start = p.completeEnd.Add(-p.overlap)
	if floor := now.Add(-LogPollMaxCatchup); start.Before(floor) {
		start, expired = floor, true
	}
	return start, now, expired
}

func (p *LogPollState) Complete(end time.Time) {
	p.completeEnd = end
	p.backoff = 0
}

type logPollRetry struct {
	err        error
	retryAfter string
}

func (e *logPollRetry) Error() string { return e.err.Error() }
func (e *logPollRetry) Unwrap() error { return e.err }

// NewLogPollRetry marks an error as recoverable without invoking a consumer's
// fatal error callback. Retry-After is retained for the polling loop to honor.
func NewLogPollRetry(err error, retryAfter string) error {
	return &logPollRetry{err: err, retryAfter: retryAfter}
}

func LogPollRetryableStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

// RetryDelay doubles fallback backoff from 30s to 5m. A longer server deadline
// always wins; capping Retry-After could send requests before the server permits.
func (p *LogPollState) RetryDelay(err error, now time.Time) (time.Duration, bool) {
	var retry *logPollRetry
	if !errors.As(err, &retry) {
		return 0, false
	}
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
