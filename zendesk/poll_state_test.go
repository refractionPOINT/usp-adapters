package usp_zendesk

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogPollBoundedBackoff(t *testing.T) {
	now := time.Date(2024, 7, 15, 19, 0, 0, 0, time.UTC)
	state := newPollState(now, 30*time.Minute)
	err := newPollRetry(errors.New("temporary"), "")
	for _, want := range []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 5 * time.Minute, 5 * time.Minute} {
		delay, retryable := state.retryDelay(err, now)
		require.True(t, retryable)
		require.Equal(t, want, delay)
	}
	_, retryable := state.retryDelay(errors.New("permanent"), now)
	require.False(t, retryable)
	state.complete(now)
	delay, _ := state.retryDelay(err, now)
	require.Equal(t, 30*time.Second, delay, "complete success resets fallback backoff")
}

func TestLogPollRetryAfter(t *testing.T) {
	now := time.Date(2024, 7, 15, 19, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		header string
		want   time.Duration
	}{
		{"600", 10 * time.Minute},
		{now.Add(time.Hour).Format(http.TimeFormat), time.Hour},
		{"garbage", 30 * time.Second},
		{"-1", 30 * time.Second},
		{"0", 30 * time.Second},
		{now.Add(-time.Hour).Format(http.TimeFormat), 30 * time.Second},
		{"9223372036854775807", time.Duration(1<<63 - 1)},
	} {
		t.Run(tc.header, func(t *testing.T) {
			state := newPollState(now, 30*time.Minute)
			delay, retryable := state.retryDelay(newPollRetry(errors.New("temporary"), tc.header), now)
			require.True(t, retryable)
			require.Equal(t, tc.want, delay)
		})
	}
}
