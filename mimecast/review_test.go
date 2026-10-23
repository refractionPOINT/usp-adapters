package usp_mimecast

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthyMaximumOverlapDoesNotWarnOfMissingHistory(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	_, a := newIdleMock(t, func(c *MimecastConfig) {
		c.ClientOptions = opts
		c.OverlapPeriod = "24h"
	})
	clock := usePollClock(a)
	_, err := a.makeOneRequest()
	require.NoError(t, err)
	clock.current = clock.current.Add(defaultPollInterval)
	_, err = a.makeOneRequest()
	require.NoError(t, err)
	assert.NotContains(t, logs.joinedWarnings(), "older records may be missed")
}

func TestRecoveryDoesNotAttributePollingOutageToIndexLag(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	mock, a := newIdleMock(t, func(c *MimecastConfig) { c.ClientOptions = opts })
	clock := usePollClock(a)
	origin := clock.current
	_, err := a.makeOneRequest()
	require.NoError(t, err)
	clock.current = origin.Add(time.Minute)
	mock.setAuditStatus(http.StatusServiceUnavailable)
	_, err = a.makeOneRequest()
	require.Error(t, err)
	mock.addEvent(fakeLogonEvent("during-outage", origin.Add(90*time.Second)))
	mock.setAuditStatus(0)
	clock.current = origin.Add(27 * time.Minute)
	items, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.NotContains(t, logs.joinedWarnings(), "raise overlap_period")
	// A later poll can still diagnose a newly indexed, genuinely old record.
	mock.addEvent(fakeLogonEvent("late-after-recovery", origin.Add(2*time.Minute)))
	clock.current = clock.current.Add(defaultPollInterval)
	_, err = a.makeOneRequest()
	require.NoError(t, err)
	assert.Contains(t, logs.joinedWarnings(), "raise overlap_period")
}

func TestQuotaResetOnlyDelaysExhaustedQuota(t *testing.T) {
	_, a := newIdleMock(t)
	clock := usePollClock(a)
	for _, tc := range []struct {
		name      string
		status    int
		remaining string
		after     string
		want      time.Duration
	}{
		{"throttled", 429, "", "", 2 * time.Minute},
		{"explicitly exhausted", 503, "0", "", 2 * time.Minute},
		{"server error with quota", 503, "10", "", 0},
		{"expired authentication with quota", 401, "10", "", 0},
		{"unknown quota", 503, "", "", 0},
		{"retry-after still applies", 503, "10", "90", 90 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := make(http.Header)
			headers.Set("X-RateLimit-Reset", "120000")
			headers.Set("X-RateLimit-Remaining", tc.remaining)
			headers.Set("Retry-After", tc.after)
			for _, oauth := range []bool{false, true} {
				err := a.httpFailure(&http.Response{StatusCode: tc.status, Header: headers}, oauth)
				var failure *requestFailure
				require.ErrorAs(t, err, &failure)
				if tc.want == 0 {
					assert.True(t, failure.retryAt.IsZero())
				} else {
					assert.Equal(t, clock.current.Add(tc.want), failure.retryAt)
				}
			}
		})
	}
}
