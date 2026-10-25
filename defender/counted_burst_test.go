package usp_defender

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Independent producer: burst straddles the settled-window boundary. Healthy
// replay reverses source order, repeats page seams, provides exact counts,
// and either refetches a malformed page or splits an ambiguous terminal page.
func TestDefenderCountedReorderedBurst(t *testing.T) {
	for _, variant := range []string{"counted-descending", "malformed-prefix", "split-counted-descending"} {
		t.Run(variant, func(t *testing.T) {
			b := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
			stamp := b.Add(4*time.Minute + 50*time.Second)
			now := b.Add(12 * time.Minute)
			healthy := false
			malformed := false
			seedGets := 0
			healthyGets := 0
			server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
				start, end := windowBounds(t, r)
				if healthy && start.After(b.Add(time.Hour)) {
					sendPage(w, []map[string]interface{}{pressureAlert("burst-reordered-new-tail", now.Add(-time.Minute))}, "")
					return
				}
				indices := make([]int, 0, liveIDLimit)
				for i := 0; i < liveIDLimit; i++ {
					created := stamp.Add(time.Duration(i) * time.Millisecond)
					if !created.Before(start) && created.Before(end) {
						indices = append(indices, i)
					}
				}
				offset := 0
				if r.URL.Query().Get("$skiptoken") != "" {
					_, err := fmt.Sscanf(r.URL.Query().Get("$skiptoken"), "burst-reordered-%d", &offset)
					require.NoError(t, err)
				}
				if healthy {
					healthyGets++
					for i, j := 0, len(indices)-1; i < j; i, j = i+1, j-1 {
						indices[i], indices[j] = indices[j], indices[i]
					}
				} else {
					seedGets++
				}
				items := []map[string]interface{}{}
				stop := offset + 999
				if stop > len(indices) {
					stop = len(indices)
				}
				if healthy && offset > 0 && offset <= len(indices) {
					i := indices[offset-1]
					items = append(items, pressureAlert(fmt.Sprint("burst-reordered-", i), stamp.Add(time.Duration(i)*time.Millisecond)))
				}
				for _, i := range indices[offset:stop] {
					items = append(items, pressureAlert(fmt.Sprint("burst-reordered-", i), stamp.Add(time.Duration(i)*time.Millisecond)))
				}
				next := ""
				if stop < len(indices) {
					u := *r.URL
					u.Scheme = "http"
					u.Host = r.Host
					q := u.Query()
					q.Set("$skiptoken", fmt.Sprint("burst-reordered-", stop))
					u.RawQuery = q.Encode()
					next = u.String()
				}
				if healthy && variant == "split-counted-descending" && len(indices) > 999 {
					// Exactly 1000 distinct records, no next link. Remaining source is real.
					i := indices[999]
					items = append(items, pressureAlert(fmt.Sprint("burst-reordered-", i), stamp.Add(time.Duration(i)*time.Millisecond)))
					next = ""
				}
				if healthy && variant == "malformed-prefix" && !malformed && len(items) > 0 {
					malformed = true
					items[len(items)-1] = map[string]interface{}{"id": "malformed-once"}
				}
				require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{"value": items, "@odata.nextLink": next, "@odata.count": len(indices)}))
			})
			sink := &captureSink{withheld: true}
			a, g := harness(t, server, sink)
			a.runtime.now = func() time.Time { return now }
			a.pollInterval = 30 * time.Second
			ctx := context.Background()
			requests := &graphRequests{}
			for turn := 0; turn < 100; turn++ {
				_, err := a.scanPages(ctx, g, b.Add(2*time.Minute).UnixNano(), now.UnixNano(), scanTail, 1, requests)
				if err == nil {
					break
				}
				require.ErrorIs(t, err, errYield)
			}
			require.Equal(t, 66, seedGets)
			require.Equal(t, liveIDLimit, sink.count())
			sink.ack(^uint64(0))
			require.NoError(t, a.confirm(g))
			require.Equal(t, uint64(liveIDLimit), g.acked.Load())
			healthy = true
			now = b.Add(80 * time.Hour)
			_, err := a.scanPages(ctx, g, now.Add(-10*time.Minute).UnixNano(), now.UnixNano(), scanTail, 1, requests)
			require.NoError(t, err)
			require.NotNil(t, g.liveProof)
			require.NotEmpty(t, g.liveProof.retained)
			// This new real admission remains unACKed throughout old-window recovery.
			for turn := 0; turn < 1000 && g.position.Boundary < b.Add(10*time.Minute).UnixNano(); turn++ {
				end, err := a.scanPages(ctx, g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
				if err == nil {
					require.NoError(t, a.complete(g, end, false))
				} else {
					require.True(t, errors.Is(err, errYield) || errors.Is(err, errPressure) || errors.Is(err, errIncomplete) || errors.Is(err, errPage), "%v", err)
				}
				require.NoError(t, a.confirm(g))
				require.Zero(t, g.confirmed.Load(), "unACKed tail still gates complete source markers")
				require.LessOrEqual(t, len(g.live), liveIDLimit)
				if g.liveProof != nil {
					require.Same(t, g.liveScan, g.liveProof)
					require.LessOrEqual(t, len(g.liveProof.ids)+len(g.liveProof.retained), liveIDLimit)
				}
				now = now.Add(12 * time.Minute)
			}
			require.GreaterOrEqual(t, g.position.Boundary, b.Add(10*time.Minute).UnixNano(), "finite counted source must recover")
			sink.ack(^uint64(0))
			require.NoError(t, a.confirm(g))
			require.Equal(t, g.serial, g.confirmed.Load(), "completed source markers require cumulative ACK")
			require.Equal(t, liveIDLimit+1, sink.count(), "no seam/split/refetch duplicate")
			unique := map[string]bool{}
			for _, m := range sink.snapshot() {
				id := m.JsonPayload["id"].(string)
				require.False(t, unique[id], "duplicate %s", id)
				unique[id] = true
			}
			t.Logf("admissions=%d distinct=%d healthyGets=%d completedOffset=%s", sink.count(), len(unique), healthyGets, time.Duration(g.position.Boundary-b.UnixNano()))
		})
	}
}
