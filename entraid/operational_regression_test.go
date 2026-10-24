package usp_entraid

import (
	"context"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEntraOperationalStallsNeverBecomeVendorErrors(t *testing.T) {
	for _, kind := range []string{"full-tie", "page-cap", "capacity"} {
		t.Run(kind, func(t *testing.T) {
			base := fixtureBaseTime()
			var clock atomic.Int64
			clock.Store(base.UnixNano())
			var callbacks atomic.Int32
			server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
				if kind == "full-tie" {
					items := make([]map[string]interface{}, 1000)
					for i := range items {
						items[i] = item(strconv.Itoa(i), base)
					}
					sendPage(w, items, "")
				} else {
					sendPage(w, []map[string]interface{}{item("new", base)}, "")
				}
			})
			a, g, _ := harness(t, server, &captureSink{})
			stream := a.streams[0]
			a.runtime.now = func() time.Time { return time.Unix(0, clock.Load()) }
			a.conf.ClientOptions.OnError = func(error) { callbacks.Add(1) }
			end := base.Add(graphTimeQuantum).UnixNano()
			if kind == "capacity" {
				fillHistory(g, stream, base.Add(time.Second).UnixNano())
			}
			requests := &graphRequests{}
			for _, age := range []time.Duration{0, 2 * time.Hour, 4 * time.Hour} {
				clock.Store(base.Add(age).UnixNano())
				if kind == "page-cap" {
					work := a.newScan(stream, base.UnixNano(), end, end)
					for i := 0; i < 128; i++ {
						var key [32]byte
						key[0] = byte(i)
						work.links[key] = struct{}{}
					}
					g.scans[stream.name+"/live"] = work
				}
				_, err := a.scanWindow(context.Background(), g, stream, base.UnixNano(), end, false, requests)
				if kind == "capacity" {
					require.ErrorIs(t, err, errPressure)
				} else {
					require.ErrorIs(t, err, errIncomplete)
				}
				require.True(t, a.collectionFailure(context.Background(), err))
			}
			require.Zero(t, callbacks.Load(), "bounded memory and valid but unproven completeness are operational failures")
			require.Equal(t, base.UnixNano(), g.position[stream.name].Boundary)
		})
	}
}
