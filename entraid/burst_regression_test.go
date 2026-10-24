package usp_entraid

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
)

// Production-cadence regression: a permanently malformed item at the live tip, AFTER live has
// advanced, while the ordinary five-minute settle lane keeps receiving valid
// pages at the production poll cadence. Production spacing/budgets, 30s
// windows and 30s pollInterval; the synthetic clock advances in 5s steps.
func TestEntraMalformedLiveTipEscalatesDespiteSettleSuccess(t *testing.T) {
	base := fixtureBaseTime()
	var clock atomic.Int64
	clock.Store(base.Add(10 * time.Minute).UnixNano())
	bad := base.Add(2 * time.Minute)
	var settleOK, liveBad atomic.Int64
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := bounds(t, r)
		var items []map[string]interface{}
		if !bad.Before(start) && bad.Before(end) {
			items = append(items, map[string]interface{}{"activityDateTime": bad.Format(time.RFC3339Nano)})
			liveBad.Add(1)
		} else {
			settleOK.Add(1)
		}
		for _, at := range []time.Time{base.Add(10 * time.Second), base.Add(70 * time.Second), base.Add(3 * time.Minute)} {
			if !at.Before(start) && at.Before(end) {
				items = append(items, item("valid-"+at.Format(time.RFC3339), at))
			}
		}
		sendPage(w, items, "")
	})
	sink := &captureSink{}
	a, g, _ := harness(t, server, sink)
	a.streams = a.streams[:1]
	stream := a.streams[0]
	g.position = entraPosition{stream.name: g.position[stream.name]}
	prod := defaultRuntime()
	a.runtime.window = prod.window
	a.runtime.liveSpacing, a.runtime.replaySpacing, a.runtime.globalSpacing = prod.liveSpacing, prod.replaySpacing, prod.globalSpacing
	a.runtime.liveBudget, a.runtime.replayBudget = prod.liveBudget, prod.replayBudget
	a.pollInterval = 30 * time.Second
	a.runtime.now = func() time.Time { return time.Unix(0, clock.Load()) }
	s := g.position[stream.name]
	s.WindowEnd = s.Boundary + int64(a.runtime.window)
	g.position[stream.name] = s
	var onErrors atomic.Int64
	a.conf.ClientOptions.OnError = func(error) { onErrors.Add(1) }
	requests := &graphRequests{}
	nextSettle, preferSettle := map[string]time.Time{}, map[string]bool{}
	ctx := context.Background()
	for clock.Load() < base.Add(3*time.Hour).UnixNano() {
		a.maintainHistory(g, false)
		if err := a.liveStep(ctx, g, stream, requests, nextSettle, preferSettle); err != nil {
			require.True(t, a.collectionFailure(ctx, err))
		}
		clock.Add(int64(5 * time.Second))
	}
	t.Logf("shipped=%d boundary=+%s settleOK=%d liveBad=%d onErrors=%d", sink.count(), time.Duration(g.position[stream.name].Boundary-base.UnixNano()), settleOK.Load(), liveBad.Load(), onErrors.Load())
	require.True(t, g.position[stream.name].Boundary > bad.UnixNano() || onErrors.Load() > 0,
		"a permanently invalid live page must not stall live collection for hours while staying invisible to OnError")
}

// Burst-pressure regression: a bounded burst. 1 record/ms for the first 6 minutes of source
// time (360000 records), then 1 record/s. Real multi-page HTTP, ACKing sink,
// concurrent confirm/save. After the burst has passed, the clock moves 1h and
// then advances within the unexpired horizon. Live collection must resume.
func TestEntraBurstHistoryPressureEventuallyResumes(t *testing.T) {
	base := fixtureBaseTime()
	burstEnd := base.Add(6 * time.Minute)
	var clock atomic.Int64
	clock.Store(base.Add(30 * time.Minute).UnixNano())
	next := func(at time.Time) time.Time {
		if at.Before(burstEnd) {
			return at.Add(time.Millisecond)
		}
		return at.Add(time.Second)
	}
	firstAt := func(start time.Time) time.Time {
		if start.Before(burstEnd) {
			f := start.Truncate(time.Millisecond)
			if f.Before(start) {
				f = f.Add(time.Millisecond)
			}
			return f
		}
		f := start.Truncate(time.Second)
		if f.Before(start) {
			f = f.Add(time.Second)
		}
		return f
	}
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := bounds(t, r)
		if visible := time.Unix(0, clock.Load()); end.After(visible) {
			end = visible
		}
		at := firstAt(start)
		skip, _ := strconv.Atoi(r.URL.Query().Get("$skiptoken"))
		for i := 0; i < skip && at.Before(end); i++ {
			at = next(at)
		}
		items := []map[string]interface{}{}
		for len(items) < 1000 && at.Before(end) {
			items = append(items, item(fmt.Sprintf("b-%d", at.UnixNano()), at))
			at = next(at)
		}
		link := ""
		if at.Before(end) {
			q := url.Values{"$filter": {r.URL.Query().Get("$filter")}, "$skiptoken": {strconv.Itoa(skip + len(items))}}
			link = "http://" + r.Host + r.URL.Path + "?" + q.Encode()
		}
		sendPage(w, items, link)
	})
	sink := &burstAdmissionSink{}
	a, g, store := harness(t, server, sink)
	a.streams = a.streams[:1]
	stream := a.streams[0]
	g.position = entraPosition{stream.name: g.position[stream.name]}
	a.runtime.window = defaultRuntime().window
	a.runtime.reconcileInterval = defaultRuntime().reconcileInterval
	a.runtime.now = func() time.Time { return time.Unix(0, clock.Load()) }
	s := g.position[stream.name]
	s.WindowEnd = s.Boundary + int64(a.runtime.window)
	g.position[stream.name] = s
	var pressure, onErrors atomic.Int64
	a.conf.ClientOptions.OnError = func(error) { onErrors.Add(1) }
	a.conf.CheckpointIssue = func(code string) {
		if code == "live_history_pressure" {
			pressure.Add(1)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a.collect(ctx, g) }()
	go func() {
		defer wg.Done()
		tk := time.NewTicker(5 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				_ = a.saveConfirmed(ctx, g)
			}
		}
	}()
	defer func() { cancel(); wg.Wait() }()
	saved := func() int64 {
		p, err := decodePosition(store.snapshot().Position)
		require.NoError(t, err)
		return p[stream.name].Boundary
	}
	require.Eventually(t, func() bool { return sink.count() >= liveIDLimit }, 45*time.Second, 10*time.Millisecond, "producer must reach the admission cap")
	b1 := saved()
	t.Logf("at cap: shipped=%d boundary=+%s pressure=%d ", sink.count(), time.Duration(b1-base.UnixNano()), pressure.Load())
	clock.Store(base.Add(90 * time.Minute).UnixNano())
	ok := waitForBurstProgress(func() bool { return saved() > burstEnd.Add(10*time.Minute).UnixNano() }, 20*time.Second)
	b2 := saved()
	cancel()
	wg.Wait()
	t.Logf("after +90m synthetic source time: shipped=%d boundary=+%s pressure=%d onErrors=%d dup=%d", sink.count(), time.Duration(b2-base.UnixNano()), pressure.Load(), onErrors.Load(), sink.dups())
	require.True(t, ok, "a completed burst must not wedge the settle tail until history expiry")
	require.Zero(t, onErrors.Load())
	require.Zero(t, sink.dups(), "reclaiming optional settle history must not re-emit delivered IDs")
}

type burstAdmissionSink struct {
	mu   sync.Mutex
	n    uint64
	seen map[string]struct{}
	dup  uint64
}

func (s *burstAdmissionSink) Ship(m *protocol.DataMessage, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		s.seen = map[string]struct{}{}
	}
	id := m.JsonPayload["id"].(string)
	if _, ok := s.seen[id]; ok {
		s.dup++
	}
	s.seen[id] = struct{}{}
	s.n++
	m.SeqNum = s.n
	return nil
}
func (s *burstAdmissionSink) GetUnsent() ([]*protocol.DataMessage, error) { return nil, nil }
func (s *burstAdmissionSink) Drain(time.Duration) error                   { return nil }
func (s *burstAdmissionSink) Close() ([]*protocol.DataMessage, error)     { return nil, nil }
func (s *burstAdmissionSink) count() uint64                               { s.mu.Lock(); defer s.mu.Unlock(); return s.n }
func (s *burstAdmissionSink) dups() uint64                                { s.mu.Lock(); defer s.mu.Unlock(); return s.dup }

func waitForBurstProgress(f func() bool, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return f()
}

func TestEntraSettlePressureFallsBackWithoutForgettingPendingOrIncomplete(t *testing.T) {
	base := fixtureBaseTime()
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	sink := &captureSink{withheld: true}
	a, g, _ := harness(t, server, sink)
	a.streams = a.streams[:1]
	stream := a.streams[0]
	s := g.position[stream.name]
	s.Boundary = base.Add(10 * time.Minute).UnixNano()
	s.WindowEnd = s.Boundary + int64(30*time.Second)
	s.Replay, s.ReplayEnd = s.Coverage, s.Boundary
	g.position = entraPosition{stream.name: s}
	now := base.Add(30 * time.Minute)
	a.runtime.now = func() time.Time { return now }
	for _, v := range []struct {
		id    string
		stamp time.Time
	}{{"old", base.Add(time.Minute)}, {"settle", base.Add(9 * time.Minute)}, {"unfinished", base.Add(10*time.Minute + time.Second)}} {
		require.NoError(t, a.shipItem(context.Background(), g, stream, item(v.id, v.stamp), s.Coverage, s.WindowEnd))
	}
	require.NoError(t, a.candidate(g))
	sink.ack(^uint64(0))
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.NoError(t, a.shipItem(context.Background(), g, stream, item("pending", base.Add(8*time.Minute)), s.Coverage, s.WindowEnd))
	require.NoError(t, a.candidate(g))
	digest := func(id string) [16]byte { return eventDigest(a.sourceFingerprint(), stream.name, id) }
	// Prefer ordinary history and preserve the settle overlap while space exists.
	a.reclaimExpiredHistory(g)
	require.NotContains(t, g.live, digest("old"))
	require.Contains(t, g.live, digest("settle"))
	require.GreaterOrEqual(t, g.position[stream.name].Replay, g.position[stream.name].Coverage, "clamp replay before publishing the reduced coverage")
	for _, saved := range g.recent[stream.name] {
		require.NotEqual(t, digest("old"), saved.digest, "removed IDs must leave the saved summary")
	}
	// Only the optional settle tail remains reclaimable; it must yield now.
	now = now.Add(2 * time.Second)
	a.reclaimExpiredHistory(g)
	require.NotContains(t, g.live, digest("settle"), "confirmed settle history must yield rather than form a permanent admission cycle")
	require.Contains(t, g.live, digest("pending"))
	require.Contains(t, g.live, digest("unfinished"))
	require.Equal(t, s.Boundary, g.position[stream.name].Boundary)
	require.LessOrEqual(t, g.position[stream.name].Coverage, s.Boundary)
	require.Greater(t, g.position[stream.name].Coverage, base.Add(9*time.Minute).UnixNano())
	require.Equal(t, s.Coverage, g.position[stream.name].Unverified)
	require.GreaterOrEqual(t, g.position[stream.name].Replay, g.position[stream.name].Coverage)
	for _, saved := range g.recent[stream.name] {
		require.NotEqual(t, digest("settle"), saved.digest)
	}
	// Direct admission is guarded too, even without scanLane's entry check.
	require.ErrorIs(t, a.shipItem(context.Background(), g, stream, item("late", base.Add(9*time.Minute+time.Second)), s.Coverage, s.WindowEnd), errPressure)
	sink.ack(^uint64(0))
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	a.maintainHistory(g, false)
	require.NotContains(t, g.live, digest("pending"), "ACKed IDs below Coverage are no longer settle protection")
	require.Contains(t, g.live, digest("unfinished"), "unfinished live admissions are mandatory even after ACK")
}

func TestEntraMalformedSettleEscalatesDespiteLiveSuccess(t *testing.T) {
	base := fixtureBaseTime()
	var clock atomic.Int64
	clock.Store(base.Add(10 * time.Minute).UnixNano())
	var late atomic.Bool
	var malformed, valid atomic.Int64
	stamp := base.Add(70 * time.Second)
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := bounds(t, r)
		if late.Load() && !stamp.Before(start) && stamp.Before(end) {
			malformed.Add(1)
			sendPage(w, []map[string]interface{}{{"activityDateTime": stamp.Format(time.RFC3339Nano)}}, "")
		} else {
			valid.Add(1)
			sendPage(w, nil, "")
		}
	})
	a, g, _ := harness(t, server, &captureSink{})
	a.streams = a.streams[:1]
	stream := a.streams[0]
	g.position = entraPosition{stream.name: g.position[stream.name]}
	prod := defaultRuntime()
	a.runtime.window = prod.window
	a.runtime.liveSpacing, a.runtime.replaySpacing, a.runtime.globalSpacing = prod.liveSpacing, prod.replaySpacing, prod.globalSpacing
	a.runtime.liveBudget, a.runtime.replayBudget = prod.liveBudget, prod.replayBudget
	a.pollInterval = 30 * time.Second
	a.runtime.now = func() time.Time { return time.Unix(0, clock.Load()) }
	s := g.position[stream.name]
	s.WindowEnd = s.Boundary + int64(a.runtime.window)
	g.position[stream.name] = s
	var callbacks atomic.Int64
	a.conf.ClientOptions.OnError = func(error) { callbacks.Add(1) }
	requests := &graphRequests{}
	nextSettle, preferSettle := map[string]time.Time{}, map[string]bool{}
	for clock.Load() < base.Add(3*time.Hour).UnixNano() {
		if g.position[stream.name].Boundary >= base.Add(3*time.Minute).UnixNano() {
			late.Store(true)
		}
		if err := a.liveStep(context.Background(), g, stream, requests, nextSettle, preferSettle); err != nil {
			require.True(t, a.collectionFailure(context.Background(), err))
		}
		clock.Add(int64(5 * time.Second))
	}
	require.Greater(t, valid.Load(), int64(100))
	require.Greater(t, malformed.Load(), int64(100))
	require.Greater(t, g.position[stream.name].Boundary, base.Add(time.Hour).UnixNano(), "healthy live collection remains available")
	require.Equal(t, int64(1), callbacks.Load(), "a fixed invalid settle traversal must escalate even while live succeeds")
}
