package usp_sophos

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// logRecorder captures what the adapter reports through OnError and OnWarning.
// OnError is fatal to a cloud-hosted adapter, so which callback a failure goes
// to is the behavior under test.
type logRecorder struct {
	mu       sync.Mutex
	errors   []string
	warnings []string
}

func (r *logRecorder) errorList() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.errors...)
}

func (r *logRecorder) warningList() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.warnings...)
}

// recordLogs points conf's OnError/OnWarning at a new recorder.
func recordLogs(t *testing.T, conf *SophosConfig) *logRecorder {
	t.Helper()
	rec := &logRecorder{}
	conf.ClientOptions.OnError = func(err error) {
		t.Logf("ERR: %v", err)
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.errors = append(rec.errors, err.Error())
	}
	conf.ClientOptions.OnWarning = func(msg string) {
		t.Logf("WRN: %s", msg)
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.warnings = append(rec.warnings, msg)
	}
	return rec
}

// TestRateLimitedEventsAreNotFatal verifies a 429 from the events endpoint is
// a warning, not an error, and that the adapter recovers and ships the events
// once Sophos stops rate limiting.
func TestRateLimitedEventsAreNotFatal(t *testing.T) {
	conf := testConfig(t)
	rec := recordLogs(t, &conf)
	mock := newMockSophosCentral(t, conf.ClientId, conf.ClientSecret, conf.TenantId)
	mock.setEvents([]utils.Dict{
		realisticThreatEvent("11111111-0000-0000-0000-000000000001", time.Now().Add(-5*time.Second)),
		realisticThreatEvent("11111111-0000-0000-0000-000000000002", time.Now().Add(-4*time.Second)),
	})
	mock.failNextEvents(
		injectedFailure{status: http.StatusTooManyRequests},
		injectedFailure{status: http.StatusTooManyRequests},
		injectedFailure{status: http.StatusTooManyRequests},
	)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	_, chStopped, sink := startMockAdapter(t, server, conf)

	require.Eventually(t, func() bool { return sink.count() == 2 },
		10*time.Second, 20*time.Millisecond, "events should ship once the rate limiting stops")

	assert.Empty(t, rec.errorList(), "a 429 must not be reported as an error")
	warnings := rec.warningList()
	require.Len(t, warnings, 3, "each 429 should be reported as a warning")
	assert.Contains(t, warnings[0], "429")

	token, _, _, _ := mock.counts()
	assert.Equal(t, 1, token, "retries must reuse the cached JWT")

	select {
	case <-chStopped:
		t.Fatal("adapter stopped on a 429")
	default:
	}
}

// TestRateLimitBacksOff verifies the adapter waits longer between polls while
// Sophos keeps rate limiting it, and honors Retry-After.
func TestRateLimitBacksOff(t *testing.T) {
	conf := testConfig(t)
	conf.PollInterval = 50 * time.Millisecond
	rec := recordLogs(t, &conf)
	mock := newMockSophosCentral(t, conf.ClientId, conf.ClientSecret, conf.TenantId)
	mock.failNextEvents(
		injectedFailure{status: http.StatusTooManyRequests},
		injectedFailure{status: http.StatusTooManyRequests},
		injectedFailure{status: http.StatusTooManyRequests, retryAfter: "1"},
	)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	startMockAdapter(t, server, conf)

	require.Eventually(t, func() bool { return len(mock.eventsTimes()) >= 4 },
		10*time.Second, 20*time.Millisecond)

	times := mock.eventsTimes()
	// Without Retry-After the wait doubles per consecutive failure: 100ms
	// after the first 429, then 200ms after the second.
	assert.GreaterOrEqual(t, times[1].Sub(times[0]), 100*time.Millisecond)
	assert.GreaterOrEqual(t, times[2].Sub(times[1]), 200*time.Millisecond)
	// The third 429 asked for 1s.
	assert.GreaterOrEqual(t, times[3].Sub(times[2]), 1*time.Second)
	assert.Empty(t, rec.errorList())
}

// TestRateLimitedTokenIsNotFatal verifies a 429 from the OAuth endpoint is a
// warning and the events endpoint is not called without a token.
func TestRateLimitedTokenIsNotFatal(t *testing.T) {
	conf := testConfig(t)
	rec := recordLogs(t, &conf)
	mock := newMockSophosCentral(t, conf.ClientId, conf.ClientSecret, conf.TenantId)
	mock.setEvents([]utils.Dict{
		realisticThreatEvent("11111111-0000-0000-0000-000000000001", time.Now().Add(-5*time.Second)),
	})
	mock.failNextToken(injectedFailure{status: http.StatusTooManyRequests})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	_, _, sink := startMockAdapter(t, server, conf)

	require.Eventually(t, func() bool { return sink.count() == 1 },
		10*time.Second, 20*time.Millisecond)

	_, _, _, rejected := mock.counts()
	assert.Equal(t, 0, rejected, "no events request may be sent without a valid token")
	assert.Empty(t, rec.errorList())
	require.Len(t, rec.warningList(), 1)
	assert.Contains(t, rec.warningList()[0], "429")
}

// TestFirstPollRetryKeepsFromDate verifies that when the very first poll fails,
// the retry asks for the same from_date instead of a later one, so events are
// not skipped because of the backoff.
func TestFirstPollRetryKeepsFromDate(t *testing.T) {
	conf := testConfig(t)
	recordLogs(t, &conf)
	mock := newMockSophosCentral(t, conf.ClientId, conf.ClientSecret, conf.TenantId)
	// Created 29s ago: inside the initial now-30s window, but outside the
	// window a retry 2s later would compute if from_date were not pinned.
	mock.setEvents([]utils.Dict{
		realisticThreatEvent("11111111-0000-0000-0000-000000000001", time.Now().Add(-29*time.Second)),
	})
	mock.failNextEvents(injectedFailure{status: http.StatusTooManyRequests, retryAfter: "2"})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	_, _, sink := startMockAdapter(t, server, conf)

	require.Eventually(t, func() bool { return sink.count() == 1 },
		10*time.Second, 20*time.Millisecond, "the retry must use the original from_date")
}

// TestExpiredCachedTokenIsRenewed verifies that when Sophos rejects a cached
// token, the adapter fetches a new one without reporting an error.
func TestExpiredCachedTokenIsRenewed(t *testing.T) {
	conf := testConfig(t)
	rec := recordLogs(t, &conf)
	mock := newMockSophosCentral(t, conf.ClientId, conf.ClientSecret, conf.TenantId)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	_, _, sink := startMockAdapter(t, server, conf)

	require.Eventually(t, func() bool { _, _, cursor, _ := mock.counts(); return cursor >= 2 },
		5*time.Second, 20*time.Millisecond)

	mock.revokeTokens()
	mock.appendEvent(realisticThreatEvent("11111111-0000-0000-0000-000000000001", time.Now()))

	require.Eventually(t, func() bool { return sink.count() == 1 },
		10*time.Second, 20*time.Millisecond, "the adapter should recover with a new token")

	token, _, _, rejected := mock.counts()
	assert.Equal(t, 2, token, "exactly one new token should be fetched")
	assert.Equal(t, 1, rejected)
	assert.Empty(t, rec.errorList(), "a rejected cached token must not be reported as an error")
}

// TestTokenRenewedBeforeExpiry verifies the cached token is renewed based on
// the expires_in Sophos returns.
func TestTokenRenewedBeforeExpiry(t *testing.T) {
	conf := testConfig(t)
	rec := recordLogs(t, &conf)
	mock := newMockSophosCentral(t, conf.ClientId, conf.ClientSecret, conf.TenantId)
	// A 1s lifetime is cached for 500ms (the renewal margin is capped at half
	// the lifetime).
	mock.expiresIn = 1

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	startMockAdapter(t, server, conf)

	time.Sleep(1300 * time.Millisecond)

	token, fromDate, cursor, _ := mock.counts()
	polls := fromDate + cursor
	assert.GreaterOrEqual(t, token, 2, "the token should be renewed as it nears expiry")
	assert.LessOrEqual(t, token, 4)
	assert.Greater(t, polls, 3*token, "the token must be reused between renewals")
	assert.Empty(t, rec.errorList())
}

// TestPersistentTransientFailureEscalates verifies transient failures are
// escalated to an error once they persist past the grace period, so a
// permanently broken integration still surfaces.
func TestPersistentTransientFailureEscalates(t *testing.T) {
	conf := testConfig(t)
	conf.TransientErrorGracePeriod = 300 * time.Millisecond
	rec := recordLogs(t, &conf)
	mock := newMockSophosCentral(t, conf.ClientId, conf.ClientSecret, conf.TenantId)
	failures := make([]injectedFailure, 100)
	for i := range failures {
		failures[i] = injectedFailure{status: http.StatusServiceUnavailable}
	}
	mock.failNextEvents(failures...)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	startMockAdapter(t, server, conf)

	require.Eventually(t, func() bool { return len(rec.errorList()) >= 1 },
		10*time.Second, 20*time.Millisecond, "a failure outlasting the grace period must be reported as an error")
	assert.GreaterOrEqual(t, len(rec.warningList()), 2, "failures within the grace period are warnings")
	assert.Contains(t, rec.errorList()[0], "503")
}

func TestBackoff(t *testing.T) {
	a := &SophosAdapter{pollInterval: 30 * time.Second}

	assert.Equal(t, 60*time.Second, a.backoff(1, 0))
	assert.Equal(t, 120*time.Second, a.backoff(2, 0))
	assert.Equal(t, 240*time.Second, a.backoff(3, 0))
	assert.Equal(t, maxBackoff, a.backoff(4, 0))
	assert.Equal(t, maxBackoff, a.backoff(1000, 0), "must not overflow on long outages")

	assert.Equal(t, 7*time.Second, a.backoff(3, 7*time.Second), "Retry-After takes precedence")
	assert.Equal(t, maxRetryAfter, a.backoff(1, 48*time.Hour), "Retry-After is capped")
}

func TestParseRetryAfter(t *testing.T) {
	assert.Equal(t, 120*time.Second, parseRetryAfter("120"))
	assert.Equal(t, time.Duration(0), parseRetryAfter(""))
	assert.Equal(t, time.Duration(0), parseRetryAfter("-5"))
	assert.Equal(t, time.Duration(0), parseRetryAfter("soon"))

	d := parseRetryAfter(time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat))
	assert.Greater(t, d, 80*time.Second)
	assert.LessOrEqual(t, d, 90*time.Second)
	assert.Equal(t, time.Duration(0), parseRetryAfter(time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat)))
}
