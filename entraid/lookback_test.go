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
	client, received := lookbacktest.Client(t, nil)
	a, _ := harness(t, vendor, client)
	a.conf.Lookback = &utils.LookbackConfig{Window: 3 * time.Hour, Namespace: "record", Deduper: deduper.(utils.CheckingDeduper)}
	a.runtime.window = 4 * time.Hour
	run := func() {
		g := newGeneration(1, a.initialPosition())
		s := g.position[a.streams[0].name]
		require.Equal(t, fixtureBaseTime().Add(-3*time.Hour).UnixNano(), s.Boundary)
		require.Equal(t, s.Boundary, s.Coverage)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		end, err := a.scanWindow(ctx, g, a.streams[0], s.Boundary, s.WindowEnd, false, &graphRequests{})
		require.NoError(t, err)
		require.Equal(t, s.WindowEnd, end)
	}
	run()
	require.IsType(t, float64(0), lookbacktest.Receive(t, received).JsonPayload["version"])
	lookbacktest.Receive(t, received)
	// Toggling unrelated collections and rotating credentials preserve markers.
	a.streams = a.streams[:1]
	a.conf.ClientID = "rotated"
	run()
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
	a.conf.Lookback.Namespace = "different-record"
	run()
	lookbacktest.Receive(t, received)
	lookbacktest.Receive(t, received)
}
