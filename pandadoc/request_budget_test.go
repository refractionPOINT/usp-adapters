package usp_pandadoc

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Keep the dense workload and clock alignment fixed: 15000 distinct IDs
// spread over one second, ten minutes before the polling harness starts.
// It exceeds the 10000-row window ceiling and exercises recursive splitting
// through fetchEvents, rather than measuring the request helper in isolation.
func addRequestBudgetBurst(mock *mockPandaDoc, now time.Time) {
	burst := now.Add(-10 * time.Minute).Truncate(time.Second)
	for i := 0; i < 15000; i++ {
		mock.addLogs(auditLogEntry(fmt.Sprintf("burst-%05d", i), burst.Add(time.Duration(i%1000)*time.Millisecond)))
	}
}

func requireBurstDeliveredOnce(t *testing.T, h *pollHarness) {
	t.Helper()
	ids := shippedIDCounts(h.sink.snapshot())
	require.Len(t, ids, 15000)
	for id, count := range ids {
		require.Equal(t, 1, count, "ID %s", id)
	}
	require.Equal(t, 15000, h.sink.count())
}

func TestDenseBurstPollingRequestBudget(t *testing.T) {
	mock := newMockPandaDoc("test-api-key")
	h := newPollHarness(t, mock)
	addRequestBudgetBurst(mock, h.now)
	var perPoll []int
	last := 0
	recordPoll := func() {
		n := mock.requestCount()
		perPoll = append(perPoll, n-last)
		last = n
		requireBurstDeliveredOnce(t, h)
	}
	h.beforeWait = func(step int) {
		if step > 1 {
			recordPoll()
		}
	}
	h.run(4)
	recordPoll() // include the fourth actual poll, not the initial empty wait
	require.Empty(t, h.fatal)
	require.Empty(t, h.warnings)
	require.Len(t, perPoll, 4)
	for _, requests := range perPoll {
		// Bisection alignment changes the count (125-146 for these four
		// windows). Allow 14 requests above that maximum while still
		// catching the >1000 requests/poll from 100-record pages.
		require.LessOrEqual(t, requests, 160, "dense burst must fit comfortably below the List quota")
		require.Greater(t, requests, 10, "the burst must exercise subdivision")
	}
	for _, request := range mock.requestsSnapshot() {
		require.Equal(t, 1000, request.count)
		require.LessOrEqual(t, request.page*request.count, 10000)
	}
	require.Equal(t, []time.Duration{30 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second}, h.delays)
	t.Logf("requests per actual poll=%v; unique=15000 duplicates=0", perPoll)
}

// A temporarily exhausted/shared quota can interrupt a successful page walk.
// After Retry-After, the adapter restarts the window at page 1 and dedupes
// previously shipped pages; it does not retain a page cursor or partitions.
// Model recovery to the documented production List quota (2000/60s). Sandbox
// keys have only 10/60s: a persistent quota that low cannot finish this dense
// workload by restarting windows and will hit the existing 30m failure limit.
func TestDenseBurstQuotaBackoffAndWindowRestart(t *testing.T) {
	mock := newMockPandaDoc("test-api-key")
	h := newPollHarness(t, mock)
	addRequestBudgetBurst(mock, h.now)
	transport := h.a.httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	requests := 0
	var failedAt, retryAt time.Time
	var firstStart time.Time
	var recent []time.Time
	var perPoll []int
	last := 0
	h.a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
		requests++
		if requests == 1 {
			firstStart, _ = pollBounds(t, r)
		}
		if requests == 4 {
			failedAt = h.now
			retryAt = failedAt.Add(time.Minute)
			return pollResponse(http.StatusTooManyRequests, "60"), nil
		}
		if !retryAt.IsZero() {
			require.False(t, h.now.Before(retryAt), "no HTTP request is allowed before Retry-After")
			if requests == 5 {
				require.True(t, firstPage(r), "a retry restarts the window, without saved page progress")
				start, _ := pollBounds(t, r)
				require.Equal(t, firstStart, start, "a partial poll must not advance the complete checkpoint")
			}
		}
		// A sliding-minute quota, as documented by PandaDoc. All HTTP work
		// in a fake-clock polling attempt happens at that attempt's instant.
		kept := recent[:0]
		for _, at := range recent {
			if h.now.Sub(at) < time.Minute {
				kept = append(kept, at)
			}
		}
		recent = append(kept, h.now)
		require.LessOrEqual(t, len(recent), 2000, "recovered traffic must fit the production List quota")
		return transport.RoundTrip(r)
	})
	h.beforeWait = func(step int) {
		if step > 1 {
			perPoll = append(perPoll, requests-last)
			last = requests
		}
		if step == 2 {
			require.Equal(t, 3000, h.sink.count(), "ship all three successful pages before waiting")
			require.Equal(t, retryAt, h.now)
			require.Len(t, h.warnings, 1)
		}
		if step >= 3 {
			requireBurstDeliveredOnce(t, h)
		}
	}
	h.run(4)
	perPoll = append(perPoll, requests-last)
	require.Empty(t, h.fatal)
	require.Len(t, h.warnings, 1)
	requireBurstDeliveredOnce(t, h)
	require.Equal(t, []time.Duration{30 * time.Second, time.Minute, 30 * time.Second, 30 * time.Second}, h.delays,
		"complete recovery must restore the normal 30-second cadence")
	t.Logf("requests per actual attempt=%v; Retry-After=60s; unique=15000 duplicates=0", perPoll)
}
