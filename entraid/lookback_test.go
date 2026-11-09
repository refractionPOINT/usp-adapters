package usp_entraid

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/lookbacktest"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

func TestLookbackRestartGapRevisionAndStreamIdentity(t *testing.T) {
	stamp := fixtureBaseTime().Add(-time.Hour)
	var revision atomic.Int64
	revision.Store(9007199254740992)
	vendor := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := bounds(t, r)
		require.False(t, stamp.Before(start))
		require.True(t, stamp.Before(end))
		fmt.Fprintf(w, `{"value":[{"id":"one","activityDateTime":%q,"version":%d},{"id":"two","activityDateTime":%q}]}`, stamp.Format(time.RFC3339Nano), revision.Load(), stamp.Format(time.RFC3339Nano))
	})
	deduper, err := utils.NewLocalDeduper(time.Hour, 6*time.Hour)
	require.NoError(t, err)
	defer deduper.Close()
	gate := make(chan struct{})
	client, received := lookbacktest.Client(t, gate)
	a, _ := harness(t, vendor, client)
	a.conf.Lookback = &utils.LookbackConfig{Window: 3 * time.Hour, Namespace: "record", Deduper: deduper.(utils.CheckingDeduper)}
	a.runtime.window = 4 * time.Hour
	g := newGeneration(1, a.initialPosition())
	run := func() {
		s := g.position[a.streams[0].name]
		require.Equal(t, fixtureBaseTime().Add(-3*time.Hour).UnixNano(), s.Boundary)
		require.Equal(t, s.Boundary, s.Coverage)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		end, err := a.scanWindow(ctx, g, a.streams[0], s.Boundary, s.WindowEnd, false, &graphRequests{})
		require.NoError(t, err)
		require.Equal(t, s.WindowEnd, end)
	}
	done := make(chan struct{})
	go func() { defer close(done); run() }()
	require.IsType(t, float64(0), lookbacktest.Receive(t, received).JsonPayload["version"])
	lookbacktest.Receive(t, received)
	require.Empty(t, g.live, "unconfirmed receipts cannot populate local history")
	close(gate)
	<-done
	a.streams = a.streams[:1]
	a.conf.ClientID = "rotated"
	g = newGeneration(2, a.initialPosition())
	run()
	for _, entry := range g.live {
		require.Zero(t, entry.serial)
		require.Equal(t, stamp.Add(historyHorizon).UnixNano(), entry.expiry)
	}
	outage := &lookbackMiss{}
	a.conf.Lookback.Deduper = outage
	run()
	require.Zero(t, outage.checks, "shared hits must become local confirmed history")
	select {
	case <-received:
		t.Fatal("confirmed version replayed")
	default:
	}
	revision.Add(1) // Above float64's exact range; UseNumber must preserve the update.
	run()
	lookbacktest.Receive(t, received)
	select {
	case <-received:
		t.Fatal("unchanged sibling replayed")
	default:
	}
	require.Equal(t, 3, len(g.live), "revisions retained separately from entity counts")
	a.conf.Lookback.Namespace = "different-record"
	run()
	lookbacktest.Receive(t, received)
	lookbacktest.Receive(t, received)
	fillHistory(g, a.streams[0], g.position[a.streams[0].name].Boundary)
	revision.Add(1)
	s := g.position[a.streams[0].name]
	before := outage.checks
	_, err = a.scanWindow(context.Background(), g, a.streams[0], s.Boundary, s.WindowEnd, false, &graphRequests{})
	require.ErrorIs(t, err, errPressure)
	require.Equal(t, before, outage.checks, "capacity must be checked before shared lookup or admission")
	require.Len(t, g.live, liveIDLimit)
	s.Boundary = s.WindowEnd
	g.position[a.streams[0].name] = s
	g.maintenance = historyMaintenance{}
	_, err = a.scanWindow(context.Background(), g, a.streams[0], s.Coverage, s.WindowEnd, false, &graphRequests{})
	require.ErrorIs(t, err, errPressure)
	require.Greater(t, g.position[a.streams[0].name].Coverage, s.Coverage)
	require.Equal(t, before, outage.checks, "reclamation must not admit outside retained coverage")
}

type lookbackMiss struct{ checks int }

func (d *lookbackMiss) Check(string) bool     { d.checks++; return false }
func (*lookbackMiss) CheckAndAdd(string) bool { return false }
func (*lookbackMiss) Close()                  {}

func TestLookbackKeepsLateArrivalReconciliation(t *testing.T) {
	stamp := fixtureBaseTime().Add(-4 * time.Hour)
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
	g := newGeneration(1, a.initialPosition())
	s := g.position[a.streams[0].name]
	s.Coverage = fixtureBaseTime().Add(-6 * time.Hour).UnixNano()
	s.Boundary = fixtureBaseTime().UnixNano()
	s.WindowEnd = a.runtime.now().Add(time.Hour).UnixNano()
	g.position = entraPosition{a.streams[0].name: s}
	a.streams = a.streams[:1]
	a.runtime.replayWindow = 24 * time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan struct{})
	go func() { defer close(done); a.collect(ctx, g) }()
	defer func() { cancel(); <-done }()
	require.Equal(t, "late", lookbacktest.Receive(t, received).JsonPayload["id"])
}
