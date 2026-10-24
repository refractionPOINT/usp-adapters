package usp_defender

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefenderOpenTailShipsPromptlyWithoutCommittingUnsettledWindow(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	alert := realisticAlert("tail-alert", base.Add(time.Second))
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := windowBounds(t, r)
		var items []map[string]interface{}
		if !base.Add(time.Second).Before(start) && base.Add(time.Second).Before(end) {
			items = append(items, alert)
		}
		sendPage(w, items, "")
	})
	sink := &captureSink{withheld: true}
	a, g := harness(t, server, sink)
	initial := g.confirmed.Load()
	var now atomic.Int64
	now.Store(base.Add(6 * time.Minute).UnixNano())
	a.runtime.now = func() time.Time { return time.Unix(0, now.Load()) }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.collect(ctx, g) }()
	defer func() { cancel(); <-done }()
	require.Eventually(t, func() bool { return sink.count() == 1 }, 250*time.Millisecond, time.Millisecond, "fresh creation-time alerts must ship before the first settled window")
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load(), "an open tail cannot advance the confirmed frontier")
	now.Store(base.Add(11 * time.Minute).UnixNano())
	// The settled scan sees the same pending ID and must inherit its ACK dependency.
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load())
	sink.ack(1)
	require.Eventually(t, func() bool {
		if a.confirm(g) != nil {
			return false
		}
		return g.confirmed.Load() >= 1
	}, time.Second, time.Millisecond)
	require.Equal(t, 1, sink.count(), "tail/window overlap must not re-emit a pending or confirmed ID")
}

func TestDefenderReplayFailureYieldsAndRetainsContinuationForFreshTraffic(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	var fresh atomic.Bool
	var failures atomic.Int32
	failed := make(chan struct{}, 1)
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := windowBounds(t, r)
		if start.Before(base) {
			if r.URL.Query().Get("$skiptoken") == "held" {
				failures.Add(1)
				w.WriteHeader(503)
				select {
				case failed <- struct{}{}:
				default:
				}
				return
			}
			next := *r.URL
			next.Scheme = "http"
			next.Host = r.Host
			q := next.Query()
			q.Set("$skiptoken", "held")
			next.RawQuery = q.Encode()
			sendPage(w, nil, next.String())
			return
		}
		var items []map[string]interface{}
		created := base.Add(19 * time.Minute)
		if fresh.Load() && !created.Before(start) && created.Before(end) {
			items = append(items, realisticAlert("fresh-during-replay-outage", created))
		}
		sendPage(w, items, "")
	})
	sink := &captureSink{}
	a, g := harness(t, server, sink)
	g.position.Coverage = base.Add(-10 * time.Minute).UnixNano()
	g.position.Replay = g.position.Coverage
	g.position.ReplayEnd = g.position.Boundary
	a.runtime.now = func() time.Time {
		if fresh.Load() {
			return base.Add(20 * time.Minute)
		}
		return base.Add(6 * time.Minute)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.collect(ctx, g) }()
	defer func() { cancel(); <-done }()
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not reach its failing continuation")
	}
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, int32(1), failures.Load(), "replay backoff must prevent a retry on each scheduler turn")
	fresh.Store(true)
	require.Eventually(t, func() bool { return sink.count() == 1 }, 250*time.Millisecond, time.Millisecond, "a replay-specific 503/backoff must yield to fresh/live traffic")
	cancel()
	<-done
	require.Equal(t, int32(1), failures.Load(), "replay backoff must survive scheduler turns")
	require.NotNil(t, g.replayScan)
	require.Contains(t, g.replayScan.next, "held")
	require.Equal(t, base.Add(-10*time.Minute).UnixNano(), g.replayScan.start)
	require.Equal(t, base.Add(-5*time.Minute).UnixNano(), g.replayScan.targetEnd)
}

func TestDefenderEverySuccessfulPageEndsContinuousFailureEpisode(t *testing.T) {
	var fail atomic.Bool
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(503)
			return
		}
		start, _ := windowBounds(t, r)
		sendPage(w, []map[string]interface{}{realisticAlert("recovered", start.Add(time.Second))}, "http://"+r.Host+alertsPath+"?$skiptoken=next")
	})
	a, g := harness(t, server, &captureSink{})
	var vendorErrors atomic.Int32
	a.conf.ClientOptions.OnError = func(error) { vendorErrors.Add(1) }
	requests := &graphRequests{failureStart: time.Now().Add(-61 * time.Minute), reported: true}
	page, err := a.requestPage(context.Background(), a.windowURL(g.position.Boundary, g.position.WindowEnd), false, requests)
	require.NoError(t, err)
	require.True(t, requests.failureStart.IsZero(), "a successful page ends the failure episode even when its interval has more pages")
	require.False(t, requests.reported)
	fail.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, _ = a.requestPage(ctx, page.Next, false, requests)
	require.Zero(t, vendorErrors.Load(), "a new short outage cannot inherit the old episode's age")
}

func TestDefenderOpenTailPagesAndSuppressedIDsCannotBypassWindowACK(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		index := 0
		if r.URL.Query().Get("$skiptoken") == "second" {
			index = 1
		}
		next := ""
		if index == 0 {
			u := *r.URL
			u.Scheme = "http"
			u.Host = r.Host
			q := u.Query()
			q.Set("$skiptoken", "second")
			u.RawQuery = q.Encode()
			next = u.String()
		}
		sendPage(w, []map[string]interface{}{realisticAlert(fmt.Sprint(index), base.Add(time.Duration(index+1)*time.Second))}, next)
	})
	sink := &captureSink{withheld: true}
	a, g := harness(t, server, sink)
	initial := g.confirmed.Load()
	start, end := g.position.Boundary, base.Add(6*time.Minute).UnixNano()
	_, err := a.scanPages(context.Background(), g, start, end, scanTail, 1, &graphRequests{})
	require.ErrorIs(t, err, errYield)
	sink.ack(1)
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load(), "one complete tail page is not a confirmed source window")
	_, err = a.scanPages(context.Background(), g, start, end, scanTail, 1, &graphRequests{})
	require.NoError(t, err)
	require.Equal(t, 2, sink.count())
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load(), "even draining the open tail cannot seal an unsettled window")
	scan(t, a, g)
	require.Equal(t, 2, sink.count(), "the settled scan must suppress existing pending tail IDs")
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load(), "suppression inherits the second message's unACKed dependency")
	sink.ack(2)
	require.NoError(t, a.confirm(g))
	require.Equal(t, g.serial, g.confirmed.Load(), "complete source marker awaits cumulative ACK")

	p := g.position
	require.Equal(t, base.Add(5*time.Minute).UnixNano(), p.Boundary)
}

func TestDefenderReplayRetryAfterAndThrottlePauseAllLanes(t *testing.T) {
	for _, code := range []int{503, 429} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var historical, foreground atomic.Int32
			server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("$skiptoken") == "history" {
					historical.Add(1)
					if code == 503 {
						w.Header().Set("Retry-After", "120")
					}
					w.WriteHeader(code)
					return
				}
				foreground.Add(1)
				sendPage(w, nil, "")
			})
			a, g := harness(t, server, &captureSink{})
			requests := &graphRequests{}
			live := a.windowURL(g.position.Boundary, g.position.WindowEnd)
			history := live + "&%24skiptoken=history"
			before := time.Now()
			_, err := a.requestPage(context.Background(), history, true, requests)
			require.ErrorIs(t, err, errYield)
			if code == 503 {
				require.False(t, requests.globalNext.Before(before.Add(120*time.Second)))
			} else {
				require.False(t, requests.globalNext.Before(before.Add(time.Second)))
			}
			_, err = a.requestPage(context.Background(), history, true, requests)
			require.ErrorIs(t, err, errYield)
			_, err = a.requestPage(context.Background(), live, false, requests)
			require.ErrorIs(t, err, errYield)
			require.Equal(t, int32(1), historical.Load())
			require.Zero(t, foreground.Load(), "vendor-global cooldown is shared by replay, live and open-tail requests")
			// Advance the stored deadline to model expiry without a real minutes-long wait.
			requests.globalNext = time.Now().Add(-time.Second)
			_, err = a.requestPage(context.Background(), live, false, requests)
			require.NoError(t, err)
			require.Equal(t, int32(1), foreground.Load())
			require.True(t, requests.failureStart.IsZero())
			require.True(t, time.Now().Before(requests.replayRetry), "fresh success must not remove the replay lane's pending backoff")
		})
	}
}

func TestDefenderPendingTailPagesAndHistoryDoNotStarveSettledLive(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	var tailPages, livePages, replayPages atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := windowBounds(t, r)
		lane := "live"
		if start.Before(base) {
			lane = "replay"
			replayPages.Add(1)
		} else if !start.Before(base.Add(10 * time.Minute)) {
			lane = "tail"
			tailPages.Add(1)
		} else {
			livePages.Add(1)
		}
		n := 0
		fmt.Sscan(r.URL.Query().Get("$skiptoken"), &n)
		u := *r.URL
		u.Scheme = "http"
		u.Host = r.Host
		q := u.Query()
		q.Set("$skiptoken", fmt.Sprint(n+1))
		u.RawQuery = q.Encode()
		sendPage(w, []map[string]interface{}{realisticAlert(fmt.Sprintf("%s-%d", lane, n), start.Add(time.Second))}, u.String())
	})
	a, g := harness(t, server, &captureSink{})
	g.position.Coverage = base.Add(-10 * time.Minute).UnixNano()
	g.position.Replay = g.position.Coverage
	g.position.ReplayEnd = g.position.Boundary
	a.runtime.now = func() time.Time { return base.Add(20 * time.Minute) }
	a.runtime.liveSpacing = 5 * time.Millisecond
	a.runtime.replaySpacing = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.collect(ctx, g) }()
	defer func() { cancel(); <-done }()
	require.Eventually(t, func() bool { return livePages.Load() >= 3 && tailPages.Load() >= 3 && replayPages.Load() >= 3 }, 300*time.Millisecond, time.Millisecond, "large open-tail and replay traversals must yield fairly to confirmed live progress")
	cancel()
	<-done
	require.NotNil(t, g.liveScan)
	require.NotNil(t, g.tailScan)
	require.NotNil(t, g.replayScan)
	require.Equal(t, base.UnixNano(), g.position.Boundary, "unfinished pages cannot advance the confirmed frontier")
}

func hasAlert(sink *captureSink, id string) bool {
	for _, message := range sink.snapshot() {
		if message.JsonPayload["id"] == id {
			return true
		}
	}
	return false
}
func tiedAlerts(at time.Time) []map[string]interface{} {
	items := make([]map[string]interface{}, 1000)
	for i := range items {
		items[i] = map[string]interface{}{"id": fmt.Sprintf("tie-%d", i), "createdDateTime": at.Format(time.RFC3339Nano)}
	}
	return items
}

func TestDefenderIncompleteTailRefreshesPastUnprovableTimestampTie(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	created := base.Add(19 * time.Minute)
	var now atomic.Int64
	now.Store(base.Add(6 * time.Minute).UnixNano())
	var stalls atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := windowBounds(t, r)
		var items []map[string]interface{}
		if !base.Before(start) && base.Before(end) {
			items = tiedAlerts(base)
		}
		if len(items) < 1000 && !created.Before(start) && created.Before(end) {
			items = append(items, realisticAlert("new-after-unprovable-tie", created))
		}
		sendPage(w, items, "")
	})
	sink := &captureSink{withheld: true}
	a, g := harness(t, server, sink)
	initial := g.confirmed.Load()
	a.runtime.now = func() time.Time { return time.Unix(0, now.Load()) }
	a.conf.ClientOptions.DebugLog = func(code string) {
		code = strings.TrimPrefix(code, "defender collection: ")
		if code == "interval_stalled" {
			stalls.Add(1)
			now.Store(base.Add(20 * time.Minute).UnixNano())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.collect(ctx, g) }()
	defer func() { cancel(); <-done }()
	require.Eventually(t, func() bool { return stalls.Load() > 0 }, 5*time.Second, time.Millisecond, "ordinary tail traversal must reach the unresolved timestamp tie")
	require.Eventually(t, func() bool { return hasAlert(sink, "new-after-unprovable-tie") }, 500*time.Millisecond, time.Millisecond, "a stale prompt traversal must not pin fresh creation-time collection")
	cancel()
	<-done
	sink.ack(^uint64(0))
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load(), "refreshing/ACKing the prompt tail cannot certify the unresolved source interval")
	require.Equal(t, base.UnixNano(), g.position.Boundary)
	require.Equal(t, base.Add(5*time.Minute).UnixNano(), g.position.WindowEnd)
	require.NotNil(t, g.liveScan)
	require.Equal(t, base.UnixNano(), g.liveScan.start)
	require.Equal(t, base.Add(5*time.Minute).UnixNano(), g.liveScan.targetEnd)
	require.Equal(t, 1001, sink.count(), "refresh must retain pending IDs from the abandoned prompt traversal")
}

func TestDefenderFailingTailContinuationRefreshesRepeatedlyWithoutResettingBackoff(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	var now, firstFailure, secondFailure, firstFresh, secondFresh atomic.Int64
	now.Store(base.Add(6 * time.Minute).UnixNano())
	failed := make(chan int64, 8)
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := windowBounds(t, r)
		if r.URL.Query().Get("$skiptoken") == "stuck" {
			if end.Equal(base.Add(6 * time.Minute)) {
				firstFailure.CompareAndSwap(0, time.Now().UnixNano())
			}
			if end.Equal(base.Add(20 * time.Minute)) {
				secondFailure.CompareAndSwap(0, time.Now().UnixNano())
			}
			w.WriteHeader(503)
			select {
			case failed <- end.UnixNano():
			default:
			}
			return
		}
		if !base.Before(start) && end.Sub(start) <= 5*time.Minute {
			sendPage(w, tiedAlerts(base), "")
			return
		}
		var items []map[string]interface{}
		for _, event := range []struct {
			id   string
			at   time.Time
			seen *atomic.Int64
		}{{"old-prompt", base.Add(time.Second), nil}, {"fresh-after-failed-tail", base.Add(19 * time.Minute), &firstFresh}, {"fresh-after-second-refresh", base.Add(33 * time.Minute), &secondFresh}} {
			if !event.at.Before(start) && event.at.Before(end) {
				items = append(items, realisticAlert(event.id, event.at))
				if event.seen != nil {
					event.seen.CompareAndSwap(0, time.Now().UnixNano())
				}
			}
		}
		u := *r.URL
		u.Scheme = "http"
		u.Host = r.Host
		q := u.Query()
		q.Set("$skiptoken", "stuck")
		u.RawQuery = q.Encode()
		sendPage(w, items, u.String())
	})
	sink := &captureSink{withheld: true}
	a, g := harness(t, server, sink)
	initial := g.confirmed.Load()
	a.runtime.now = func() time.Time { return time.Unix(0, now.Load()) }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.collect(ctx, g) }()
	defer func() { cancel(); <-done }()
	select {
	case end := <-failed:
		require.Equal(t, base.Add(6*time.Minute).UnixNano(), end)
	case <-time.After(time.Second):
		t.Fatal("tail did not reach its failing opaque continuation")
	}
	now.Store(base.Add(20 * time.Minute).UnixNano())
	require.Eventually(t, func() bool { return hasAlert(sink, "fresh-after-failed-tail") }, 3*time.Second, time.Millisecond, "persistent continuation failure must not pin the old prompt interval")
	select {
	case end := <-failed:
		require.Equal(t, base.Add(20*time.Minute).UnixNano(), end)
	case <-time.After(time.Second):
		t.Fatal("refreshed tail did not retain its own incomplete continuation")
	}
	now.Store(base.Add(34 * time.Minute).UnixNano())
	require.Eventually(t, func() bool { return hasAlert(sink, "fresh-after-second-refresh") }, 3*time.Second, time.Millisecond, "the refresh rule must continue to cover advancing time")
	cancel()
	<-done
	require.GreaterOrEqual(t, firstFresh.Load()-firstFailure.Load(), int64(time.Second), "refresh must preserve the foreground lane's vendor backoff")
	require.GreaterOrEqual(t, secondFresh.Load()-secondFailure.Load(), int64(time.Second))
	counts := make(map[string]int)
	for _, message := range sink.snapshot() {
		counts[message.JsonPayload["id"].(string)]++
	}
	require.Equal(t, 1, counts["old-prompt"])
	require.Equal(t, 1, counts["fresh-after-failed-tail"])
	require.Equal(t, 1, counts["fresh-after-second-refresh"])
	sink.ack(^uint64(0))
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load())
	require.Equal(t, base.UnixNano(), g.position.Boundary)
}

func TestDefenderTailRefreshKeepsVendorGlobalCooldown(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	var now atomic.Int64
	now.Store(base.Add(6 * time.Minute).UnixNano())
	var calls atomic.Int32
	retrySeen := make(chan struct{}, 1)
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Query().Get("$skiptoken") == "stuck" {
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(503)
			return
		}
		start, end := windowBounds(t, r)
		if start.Before(base.Add(10 * time.Minute)) {
			u := *r.URL
			u.Scheme = "http"
			u.Host = r.Host
			q := u.Query()
			q.Set("$skiptoken", "stuck")
			u.RawQuery = q.Encode()
			sendPage(w, []map[string]interface{}{realisticAlert("old-before-cooldown", base.Add(time.Second))}, u.String())
			return
		}
		sendPage(w, []map[string]interface{}{realisticAlert("blocked-by-global-cooldown", end.Add(-time.Second))}, "")
	})
	sink := &captureSink{}
	a, g := harness(t, server, sink)
	initial := g.confirmed.Load()
	a.runtime.now = func() time.Time { return time.Unix(0, now.Load()) }
	a.conf.ClientOptions.DebugLog = func(code string) {
		code = strings.TrimPrefix(code, "defender collection: ")
		if code == "vendor_retry" {
			now.Store(base.Add(20 * time.Minute).UnixNano())
			select {
			case retrySeen <- struct{}{}:
			default:
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.collect(ctx, g) }()
	defer func() { cancel(); <-done }()
	select {
	case <-retrySeen:
	case <-time.After(time.Second):
		t.Fatal("tail did not receive its global cooldown")
	}
	time.Sleep(30 * time.Millisecond)
	cancel()
	<-done
	require.NotNil(t, g.tailScan)
	require.Equal(t, base.Add(20*time.Minute).UnixNano(), g.tailScan.targetEnd, "the prompt interval must refresh even while HTTP remains deferred")
	require.Equal(t, int32(2), calls.Load(), "refresh must retain vendor-global Retry-After and class deadlines")
	require.False(t, hasAlert(sink, "blocked-by-global-cooldown"))
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load())
}
