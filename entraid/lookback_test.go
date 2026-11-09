package usp_entraid

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/lookbacktest"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

func TestLookbackStartupLanesAndConfirmedIDs(t *testing.T) {
	base := fixtureBaseTime()
	stamp := base.Add(-3*time.Hour + 7*time.Second)
	var updated atomic.Bool
	vendor := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := bounds(t, r)
		records := []map[string]interface{}{}
		if !base.Add(time.Second).Before(start) && base.Add(time.Second).Before(end) {
			records = append(records, item("live", base.Add(time.Second)))
		}
		if !stamp.Before(start) && stamp.Before(end) {
			one := item("one", stamp)
			one["dismissed"] = updated.Load()
			records = append(records, one, item("two", stamp))
		}
		sendPage(w, records, "")
	})
	deduper, err := utils.NewLocalDeduper(time.Hour, 6*time.Hour)
	require.NoError(t, err)
	defer deduper.Close()
	gate := make(chan struct{})
	client, received := lookbacktest.Client(t, gate)
	a, _ := harness(t, vendor, client)
	a.streams = a.streams[:1]
	a.conf.Lookback = &utils.LookbackConfig{Window: 3 * time.Hour, Namespace: "record", Deduper: deduper.(utils.CheckingDeduper)}
	a.runtime.window = 30 * time.Second
	a.startedAt = base.UnixNano()
	a.runtime.now = func() time.Time { return base.Add(31 * time.Second) }
	g := newGeneration(1, a.initialPosition())
	stream := a.streams[0]
	s := g.position[stream.name]
	require.Equal(t, base.UnixNano(), s.Boundary)
	require.Equal(t, base.Add(30*time.Second).UnixNano(), s.WindowEnd)
	require.Equal(t, base.Add(-3*time.Hour).UnixNano(), s.Coverage)
	require.Equal(t, s.Coverage, s.Replay)
	require.Equal(t, s.Boundary, s.ReplayEnd)
	done := make(chan error, 1)
	go func() {
		done <- a.liveStep(context.Background(), g, stream, &graphRequests{}, map[string]time.Time{}, map[string]bool{})
	}()
	require.Equal(t, "live", lookbacktest.Receive(t, received).JsonPayload["id"])
	require.Empty(t, g.live, "unconfirmed receipts cannot populate local history")
	close(gate)
	require.NoError(t, <-done)
	run := func() {
		end, err := a.scanWindow(context.Background(), g, stream, s.Replay, s.Replay+int64(a.runtime.replayWindow), true, &graphRequests{})
		require.NoError(t, err)
		require.Equal(t, s.Replay+int64(15*time.Minute), end)
	}
	run()
	lookbacktest.Receive(t, received)
	lookbacktest.Receive(t, received)
	g = newGeneration(2, a.initialPosition())
	a.conf.ClientID = "rotated"
	run()
	for _, entry := range g.live {
		require.Zero(t, entry.serial)
		require.Equal(t, stamp.Add(historyHorizon).UnixNano(), entry.expiry)
	}
	outage := &lookbackMiss{}
	a.conf.Lookback.Deduper = outage
	updated.Store(true)
	run()
	require.Zero(t, outage.checks, "shared hits must become local confirmed ID history")
	require.Len(t, g.live, 2)
	select {
	case <-received:
		t.Fatal("existing ID redelivered")
	default:
	}
	// Local suppression is ID-only; a restart also preserves the shared ID marker.
	g = newGeneration(3, a.initialPosition())
	a.conf.Lookback.Deduper = deduper.(utils.CheckingDeduper)
	run()
	select {
	case <-received:
		t.Fatal("updated shared ID redelivered")
	default:
	}
	a.conf.Lookback.Namespace = "different-record"
	g = newGeneration(4, a.initialPosition())
	run()
	lookbacktest.Receive(t, received)
	lookbacktest.Receive(t, received)
	a.conf.Lookback.Deduper = outage
	g.live = make(map[[16]byte]liveID)
	fillHistory(g, stream, g.position[stream.name].Boundary)
	before := outage.checks
	// A new ID needs room; an existing ID never needs an extra slot.
	a.conf.TenantID = "different-source"
	_, err = a.scanWindow(context.Background(), g, stream, s.Replay, s.Replay+int64(a.runtime.replayWindow), true, &graphRequests{})
	require.ErrorIs(t, err, errPressure)
	require.Equal(t, before, outage.checks)
	require.Len(t, g.live, liveIDLimit)
	position := g.position[stream.name]
	position.Boundary = position.WindowEnd
	g.position[stream.name] = position
	g.maintenance = historyMaintenance{}
	_, err = a.scanWindow(context.Background(), g, stream, s.Replay, s.Replay+int64(a.runtime.replayWindow), true, &graphRequests{})
	require.ErrorIs(t, err, errPressure)
	require.Greater(t, g.position[stream.name].Coverage, s.Coverage)
	require.Equal(t, before, outage.checks)
}

type lookbackMiss struct{ checks int }

func (d *lookbackMiss) Check(string) bool     { d.checks++; return false }
func (*lookbackMiss) CheckAndAdd(string) bool { return false }
func (*lookbackMiss) Close()                  {}

func TestLookbackKeepsLateArrivalReconciliation(t *testing.T) {
	stamp := fixtureBaseTime().Add(-20 * time.Hour)
	vendor := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := bounds(t, r)
		items := []map[string]interface{}{}
		if !stamp.Before(start) && stamp.Before(end) {
			items = append(items, item("late", stamp))
		}
		sendPage(w, items, "")
	})
	client, received := lookbacktest.Client(t, nil)
	a, _ := harness(t, vendor, client)
	a.conf.Lookback = &utils.LookbackConfig{Window: 3 * time.Hour, Namespace: "record", Deduper: &lookbackMiss{}}
	a.runtime.reconcileInterval = time.Millisecond
	a.runtime.window = 30 * time.Second
	g := newGeneration(1, a.initialPosition())
	s := g.position[a.streams[0].name]
	s.Coverage = fixtureBaseTime().Add(-24 * time.Hour).UnixNano()
	s.Boundary = fixtureBaseTime().UnixNano()
	s.WindowEnd = a.runtime.now().Add(time.Hour).UnixNano()
	g.position = entraPosition{a.streams[0].name: s}
	a.streams = a.streams[:1]
	require.Equal(t, 15*time.Minute, a.runtime.replayWindow)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan struct{})
	go func() { defer close(done); a.collect(ctx, g) }()
	defer func() { cancel(); <-done }()
	require.Equal(t, "late", lookbacktest.Receive(t, received).JsonPayload["id"])
}
