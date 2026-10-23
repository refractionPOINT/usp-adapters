package usp_mimecast

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type requestFailure struct {
	message string
	fatal   bool
	retryAt time.Time
}

func (e *requestFailure) Error() string { return e.message }

// Keep timers local to the sender and release them on shutdown, including
// long server retry delays. No sleeping timer goroutine survives cancellation.
func (a *MimecastAdapter) waitTimer(delay time.Duration) bool {
	if a.doStop.IsSet() {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-a.ctx.Done():
		return true
	case <-timer.C:
		return a.doStop.IsSet()
	}
}

func (a *MimecastAdapter) waitForRetry() bool {
	for {
		if a.stopOnFailureDeadline() {
			return false
		}
		delay := a.retryAt.Sub(a.now())
		if delay <= 0 {
			return true
		}
		if a.wait(a.boundByFailureDeadline(delay)) {
			return false
		}
	}
}

func (a *MimecastAdapter) waitForNextPoll() bool {
	if a.stopOnFailureDeadline() || a.wait(a.boundByFailureDeadline(a.pollInterval)) {
		return false
	}
	return !a.stopOnFailureDeadline()
}

func (a *MimecastAdapter) boundByFailureDeadline(delay time.Duration) time.Duration {
	if !a.failureSince.IsZero() {
		if remaining := a.failureSince.Add(maxContinuousFailure).Sub(a.now()); remaining < delay {
			if remaining < 0 {
				return 0
			}
			return remaining
		}
	}
	return delay
}

// Each request is also bounded, so a request started just before escalation
// cannot hold the sender past the deadline. Close cancels the same context.
func (a *MimecastAdapter) requestContext() (context.Context, context.CancelFunc) {
	if a.failureSince.IsZero() {
		return context.WithCancel(a.ctx)
	}
	return context.WithTimeout(a.ctx, a.failureSince.Add(maxContinuousFailure).Sub(a.now()))
}

func (a *MimecastAdapter) fatalPollFailure(err error) {
	if a.doStop.IsSet() {
		return
	}
	// Set the terminal state before invoking the callback: a consumer can
	// react immediately, and further polls must not repeat fatal callbacks.
	a.doStop.Set()
	a.cancel()
	a.conf.ClientOptions.OnError(err)
}

func (a *MimecastAdapter) stopOnFailureDeadline() bool {
	if a.doStop.IsSet() {
		return true
	}
	if a.failureSince.IsZero() || a.now().Before(a.failureSince.Add(maxContinuousFailure)) {
		return false
	}
	a.fatalPollFailure(fmt.Errorf("mimecast polling failed continuously for %s: %w", maxContinuousFailure, a.lastFailure))
	return true
}

// Mimecast's API 2.0 overview defines X-RateLimit-Reset as a relative delay
// in MILLISECONDS, not an epoch timestamp or seconds:
// https://developer.services.mimecast.com/api-overview (Rate Limiting).
// Retry-After is delta seconds or an HTTP-date (RFC 9110, section 10.2.3).
// If both are supplied, honor the later deadline.
func retryDeadline(headers http.Header, now time.Time, quotaExhausted bool) time.Time {
	var deadline time.Time
	addDelay := func(value string, unit time.Duration) {
		n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || n < 0 || n > int64((1<<63-1)/unit) {
			return
		}
		if t := now.Add(time.Duration(n) * unit); t.After(deadline) {
			deadline = t
		}
	}
	// Reset is present even with quota remaining. It is a required wait only
	// after throttling or an explicit exhausted-quota response.
	if quotaExhausted {
		addDelay(headers.Get("X-RateLimit-Reset"), time.Millisecond)
	}
	if value := headers.Get("Retry-After"); value != "" {
		if date, err := http.ParseTime(value); err == nil {
			if date.After(deadline) {
				deadline = date
			}
		} else {
			addDelay(value, time.Second)
		}
	}
	return deadline
}

func (a *MimecastAdapter) httpFailure(resp *http.Response, oauth bool) error {
	return &requestFailure{
		// Do not include response bodies: they may contain audit data or tokens.
		message: fmt.Sprintf("mimecast HTTP status %d (oauth=%v)", resp.StatusCode, oauth),
		fatal: resp.StatusCode >= 400 && resp.StatusCode < 500 &&
			resp.StatusCode != http.StatusRequestTimeout && resp.StatusCode != http.StatusTooManyRequests,
		retryAt: retryDeadline(resp.Header, a.now(), resp.StatusCode == http.StatusTooManyRequests || strings.TrimSpace(resp.Header.Get("X-RateLimit-Remaining")) == "0"),
	}
}

// Permanent failures can also appear in a 200 response's fail[] array.
func hasPermanentFailure(failures []FailureDetails) bool {
	for _, failure := range failures {
		for _, detail := range failure.Errors {
			if !detail.Retryable {
				return true
			}
			switch detail.Code {
			case "invalid_credentials", "invalid_access_token", "invalid_client_identifier", "invalid_client", "invalid_grant", "app_forbidden", "service_denied":
				return true
			}
		}
	}
	return false
}

// OnError can end a consumer's session. Permanent failures are fatal
// immediately; transient errors warn and retain state for at most 30 minutes.
// Only an entirely successful poll resets this continuous-failure clock.
func (a *MimecastAdapter) reportPollFailure(err error) {
	if a.doStop.IsSet() {
		return
	}
	var failure *requestFailure
	if errors.As(err, &failure) && failure.fatal {
		a.fatalPollFailure(err)
		return
	}
	a.noteTransientFailure(err)
	if a.stopOnFailureDeadline() {
		return
	}
	if a.retryBackoff == 0 {
		a.retryBackoff = defaultPollInterval
	} else {
		a.retryBackoff *= 2
		if a.retryBackoff > maxRetryBackoff {
			a.retryBackoff = maxRetryBackoff
		}
	}
	a.retryAt = a.now().Add(a.retryBackoff)
	if failure != nil && failure.retryAt.After(a.retryAt) {
		a.retryAt = failure.retryAt
	}
	a.conf.ClientOptions.OnWarning(fmt.Sprintf("%v; retrying in %s", err, a.retryAt.Sub(a.now()).Round(time.Second)))
}

func (a *MimecastAdapter) noteTransientFailure(err error) {
	a.lastFailure = err
	if a.failureSince.IsZero() {
		a.failureSince = a.now()
	}
}
