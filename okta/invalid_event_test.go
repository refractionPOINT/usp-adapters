package usp_okta

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

func TestInvalidEventsDoNotStopCollection(t *testing.T) {
	for _, allInvalid := range []bool{false, true} {
		for _, warn := range []bool{false, true} {
			t.Run(fmt.Sprintf("all_invalid=%t/warning=%t", allInvalid, warn), func(t *testing.T) {
				var emptyPolls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					after := r.URL.Query().Get("after")
					next := "later"
					items := []utils.Dict{{"private": "fixture-payload"}, {"uuid": nil}, {"uuid": ""}, {"uuid": 42}, nil}
					if !allInvalid {
						items = append([]utils.Dict{{"uuid": "before"}}, items...)
						items = append(items, utils.Dict{"uuid": "after"}, utils.Dict{"uuid": "before"})
					}
					if after == "later" {
						items = []utils.Dict{{"uuid": "next-page"}}
						next = "empty"
					} else if after == "empty" {
						items = []utils.Dict{}
						next = "empty"
						emptyPolls.Add(1)
					}
					w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=%s>; rel="next"`, r.Host, logsURL, next))
					_ = json.NewEncoder(w).Encode(items)
				}))
				defer server.Close()
				var mu sync.Mutex
				var warnings []string
				opts := testClientOptions(t)
				opts.OnWarning = nil
				if warn {
					opts.OnWarning = func(s string) { mu.Lock(); warnings = append(warnings, s); mu.Unlock() }
				}
				sink := &captureSink{}
				a, stopped, err := newOktaAdapter(context.Background(), OktaConfig{URL: server.URL, ApiKey: "fixture", ClientOptions: opts, PollInterval: time.Millisecond}, sink)
				require.NoError(t, err)
				defer a.Close()
				require.Eventually(t, func() bool {
					select {
					case <-stopped:
						return true
					default:
					}
					return emptyPolls.Load() >= 2
				}, 3*time.Second, time.Millisecond)
				select {
				case <-stopped:
					t.Fatal("collector stopped at a malformed event")
				default:
				}
				require.NoError(t, a.Close())
				var ids []string
				for _, msg := range sink.snapshot() {
					ids = append(ids, msg.JsonPayload["uuid"].(string))
				}
				if allInvalid {
					require.Equal(t, []string{"next-page"}, ids)
				} else {
					require.Equal(t, []string{"before", "after", "next-page"}, ids)
				}
				mu.Lock()
				defer mu.Unlock()
				if warn {
					require.Equal(t, []string{"okta collection: skipped 5 event(s) with missing or invalid uuid"}, warnings)
				} else {
					require.Empty(t, warnings)
				}
			})
		}
	}
}
