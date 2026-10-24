package usp_entraid

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEntraCollectionLocalIDsRemainDistinctAcrossRestore(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := bounds(t, r)
		p := item("local-id", start)
		p["collection"] = r.URL.Path
		sendPage(w, []map[string]interface{}{p}, "")
	})
	sink := &captureSink{}
	a, g, store := harness(t, server, sink)
	scan(t, a, g, a.streams[0])
	scan(t, a, g, a.streams[1])
	require.Equal(t, 2, sink.count(), "distinct collections must both deliver the same local ID")
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err := decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.NotEqual(t, p[a.streams[0].name].IDs[0].digest, p[a.streams[1].name].IDs[0].digest)
	restored := newGeneration(2, nil, p)
	start := a.startedAt
	end := start + int64(a.runtime.window)
	for _, stream := range a.streams[:2] {
		require.NoError(t, a.shipItem(context.Background(), restored, stream, item("local-id", time.Unix(0, start)), start, end))
	}
	require.Equal(t, 2, sink.count(), "restored collection-qualified IDs suppress only their own deliveries")
	require.NoError(t, a.shipItem(context.Background(), restored, a.streams[2], item("local-id", time.Unix(0, start)), start, end))
	require.Equal(t, 3, sink.count())
}

func TestEntraExpiredReplayPressureResumesHTTPProducer(t *testing.T) {
	base := fixtureBaseTime()
	boundary := base.Add(80 * time.Hour).UnixNano()
	var clock atomic.Int64
	clock.Store(boundary)
	var pressure atomic.Int32
	var onErrors atomic.Int32
	var liveRequests atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := bounds(t, r)
		if start.UnixNano() >= boundary {
			liveRequests.Add(1)
			sendPage(w, []map[string]interface{}{item("live-after-pressure", start)}, "")
			return
		}
		if start.After(base.Add(time.Hour)) {
			sendPage(w, nil, "")
			return
		}
		sendPage(w, []map[string]interface{}{item("unverified-history", start.Add(time.Second))}, "")
	})
	sink := &captureSink{withheld: true}
	a, g, store := harness(t, server, sink)
	a.streams = a.streams[:1]
	stream := a.streams[0]
	s := g.position[stream.name]
	s.Boundary = boundary
	s.WindowEnd = boundary + int64(30*time.Second)
	s.Coverage = base.UnixNano()
	s.Replay = base.UnixNano()
	s.ReplayEnd = base.Add(time.Hour).UnixNano()
	g.position = entraPosition{stream.name: s}
	a.runtime.window = 30 * time.Second
	a.runtime.now = func() time.Time { return time.Unix(0, clock.Load()) }
	require.NoError(t, a.shipItem(context.Background(), g, stream, item("pending-original", base.Add(10*time.Second)), s.Replay, s.ReplayEnd))
	var pendingKey [16]byte
	for key := range g.live {
		pendingKey = key
	}
	for i := 0; len(g.live) < liveIDLimit; i++ {
		var key [16]byte
		binary.BigEndian.PutUint64(key[:8], uint64(i))
		g.live[key] = liveID{expiry: base.Add(historyHorizon).Add(time.Duration(i) * 10 * time.Millisecond).UnixNano()}
	}
	require.NoError(t, a.candidate(g))
	prior := store.snapshot().Position
	a.conf.ClientOptions.OnError = func(error) { onErrors.Add(1) }
	a.conf.CheckpointIssue = func(code string) {
		if code == "live_history_pressure" {
			pressure.Add(1)
			clock.Store(base.Add(80*time.Hour + time.Minute).UnixNano())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.collect(ctx, g) }()
	defer func() { cancel(); <-done }()
	require.Eventually(t, func() bool { return pressure.Load() > 0 && liveRequests.Load() > 0 && sink.count() >= 2 }, 2*time.Second, time.Millisecond, "expired confirmed history must release capacity for an actual HTTP producer")
	cancel()
	<-done

	require.Contains(t, g.live, pendingKey, "an expired but unACKed admission must retain its dependency")
	require.Greater(t, g.position[stream.name].Coverage, s.Coverage)
	require.Equal(t, s.Coverage, g.position[stream.name].Unverified, "abandoned history must remain explicit")
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, prior, store.snapshot().Position, "pressure recovery cannot cross the original unACKed dependency")
	sink.ack(^uint64(0))
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	saved, err := decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, s.Coverage, saved[stream.name].Unverified)
	require.Less(t, len(g.live), liveIDLimit)
	require.Zero(t, onErrors.Load())
}

func TestEntraProductionCadencePreservesFiveMinuteSettle(t *testing.T) {
	base := fixtureBaseTime()
	var late atomic.Bool
	var requests atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := bounds(t, r)
		requests.Add(1)
		var events []map[string]interface{}
		for _, e := range []struct {
			id    string
			stamp time.Time
		}{{"late", base.Add(10 * time.Second)}, {"before-start", base.Add(-time.Second)}} {
			if late.Load() && !e.stamp.Before(start) && e.stamp.Before(end) {
				events = append(events, item(e.id, e.stamp))
			}
		}
		sendPage(w, events, "")
	})
	sink := &captureSink{}
	a, g, _ := harness(t, server, sink)
	a.streams = a.streams[:1]
	stream := a.streams[0]
	g.position = entraPosition{stream.name: g.position[stream.name]}
	a.runtime.window = defaultRuntime().window
	a.runtime.reconcileInterval = defaultRuntime().reconcileInterval
	a.runtime.now = func() time.Time { return base.Add(2 * time.Minute) }
	s := g.position[stream.name]
	s.WindowEnd = s.Boundary + int64(a.runtime.window)
	g.position[stream.name] = s
	scan(t, a, g, stream)
	late.Store(true)
	before := requests.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	a.collect(ctx, g)
	require.Greater(t, requests.Load(), before)
	require.Equal(t, 1, sink.count(), "ordinary live/settle polling must find one-minute-late records before bulk reconciliation")
	require.Equal(t, "late", sink.snapshot()[0].JsonPayload["id"])
}

func TestEntraStreamSuccessResetsBothLaneFailureEpisodes(t *testing.T) {
	var clock atomic.Int64
	base := fixtureBaseTime()
	clock.Store(base.UnixNano())
	var onErrors atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := bounds(t, r)
		if start.Before(base.Add(time.Hour)) {
			w.WriteHeader(503)
			return
		}
		sendPage(w, nil, "")
	})
	a, g, _ := harness(t, server, &captureSink{})
	a.runtime.now = func() time.Time { return time.Unix(0, clock.Load()) }
	a.conf.ClientOptions.OnError = func(error) { onErrors.Add(1) }
	stream := a.streams[0]
	requests := &graphRequests{}
	retry := func() error {
		_, err := a.scanWindow(context.Background(), g, stream, base.UnixNano(), base.Add(time.Minute).UnixNano(), true, requests)
		return err
	}
	require.ErrorIs(t, retry(), errRetry)
	clock.Store(base.Add(59 * time.Minute).UnixNano())
	_, err := a.scanWindow(context.Background(), g, stream, base.Add(time.Hour).UnixNano(), base.Add(2*time.Hour).UnixNano(), false, requests)
	require.NoError(t, err)
	clock.Store(base.Add(61 * time.Minute).UnixNano())
	require.ErrorIs(t, retry(), errRetry)
	require.Zero(t, onErrors.Load(), "same-stream success disproves a continuous hour of collection failure")
	deadline := g.scans[stream.name+"/replay"].failure.retryAt
	clock.Store(base.Add(61*time.Minute + 100*time.Millisecond).UnixNano())
	_, err = a.scanWindow(context.Background(), g, stream, base.Add(2*time.Hour).UnixNano(), base.Add(3*time.Hour).UnixNano(), false, requests)
	require.NoError(t, err)
	require.Equal(t, deadline, g.scans[stream.name+"/replay"].failure.retryAt)
	require.True(t, errors.Is(retry(), errYield), "success must not shorten another lane's backoff")
	clock.Store(base.Add(62 * time.Minute).UnixNano())
	require.ErrorIs(t, retry(), errRetry)
	require.Zero(t, onErrors.Load())
	clock.Store(base.Add(123 * time.Minute).UnixNano())
	_, err = a.scanWindow(context.Background(), g, a.streams[1], base.Add(time.Hour).UnixNano(), base.Add(2*time.Hour).UnixNano(), false, requests)
	require.NoError(t, err)
	require.ErrorIs(t, retry(), errRetry)
	require.Equal(t, int32(1), onErrors.Load(), "another collection cannot reset this stream's failure episode")
}

func TestEntraLiveAndSettleBackoffRemainIndependent(t *testing.T) {
	for _, blocked := range []string{"live", "settle"} {
		t.Run(blocked, func(t *testing.T) {
			base := fixtureBaseTime()
			var calls atomic.Int32
			server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
				start, _ := bounds(t, r)
				calls.Add(1)
				if blocked == "settle" {
					require.Equal(t, base.Add(time.Minute), start)
				} else {
					require.Equal(t, base, start)
				}
				sendPage(w, nil, "")
			})
			a, g, _ := harness(t, server, &captureSink{})
			stream := a.streams[0]
			s := g.position[stream.name]
			s.Boundary = base.Add(time.Minute).UnixNano()
			s.WindowEnd = s.Boundary + int64(30*time.Second)
			g.position[stream.name] = s
			a.runtime.now = func() time.Time { return base.Add(10 * time.Minute) }
			start, end := s.Boundary, s.WindowEnd
			if blocked == "settle" {
				start, end = s.Coverage, s.Boundary
			}
			work := a.newScan(stream, start, end, end)
			work.failure.retryAt = base.Add(time.Hour)
			g.scans[stream.name+"/"+blocked] = work
			err := a.liveStep(context.Background(), g, stream, &graphRequests{}, make(map[string]time.Time), map[string]bool{stream.name: blocked == "settle"})
			require.NoError(t, err, "one lane's retry deadline must not suppress the other ready lane")
			require.Equal(t, int32(1), calls.Load())
			require.Equal(t, base.Add(time.Hour), work.failure.retryAt)
		})
	}
}
