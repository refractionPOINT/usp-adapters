package usp_mimecast

import (
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

func (a *MimecastAdapter) waitForRetry() bool {
	for a.now().Before(a.retryAt) {
		if a.wait(a.retryAt.Sub(a.now())) {
			return false
		}
	}
	return !a.doStop.IsSet()
}

// Mimecast's API 2.0 overview defines X-RateLimit-Reset as a relative delay
// in MILLISECONDS, not an epoch timestamp or seconds:
// https://developer.services.mimecast.com/api-overview (Rate Limiting).
// Retry-After is delta seconds or an HTTP-date (RFC 9110, section 10.2.3).
// If both are supplied, honor the later deadline.
func retryDeadline(headers http.Header, now time.Time) time.Time {
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
	addDelay(headers.Get("X-RateLimit-Reset"), time.Millisecond)
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
		fatal: resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden ||
			(oauth && resp.StatusCode == http.StatusBadRequest),
		retryAt: retryDeadline(resp.Header, a.now()),
	}
}

// Authentication failures sometimes appear in a 200 response's fail[] array.
func hasAuthFailure(failures []FailureDetails) bool {
	for _, failure := range failures {
		for _, detail := range failure.Errors {
			switch detail.Code {
			case "invalid_credentials", "invalid_access_token", "invalid_client_identifier", "invalid_client", "invalid_grant", "app_forbidden", "service_denied":
				return true
			}
		}
	}
	return false
}

// OnError can end an adapter consumer's session. Reserve it for persistent
// authentication failures; recoverable errors must retain in-memory state.
func (a *MimecastAdapter) reportPollFailure(err error) {
	if a.doStop.IsSet() {
		return
	}
	var failure *requestFailure
	if errors.As(err, &failure) && failure.fatal {
		a.conf.ClientOptions.OnError(err)
		a.doStop.Set()
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
