package usp_bitwarden

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReviewStaleContinuationRestartsSameWindow(t *testing.T) {
	for _, initial := range []bool{false, true} {
		for _, repeat := range []bool{false, true} {
			t.Run(fmt.Sprintf("initial=%v/repeat=%v", initial, repeat), func(t *testing.T) {
				stop := utils.NewEvent()
				start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
				roots, failures, calls := 0, 0, 0
				var starts, ends []string
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls++
					if calls > 12 {
						stop.Set()
						w.WriteHeader(503)
						return
					}
					token := r.URL.Query().Get("continuationToken")
					if roots > 0 {
						assert.Equal(t, starts[0], r.URL.Query().Get("start"), "every continuation keeps the root time floor")
						assert.Equal(t, ends[0], r.URL.Query().Get("end"), "every continuation keeps the root time ceiling")
					}
					switch token {
					case "":
						roots++
						starts = append(starts, r.URL.Query().Get("start"))
						ends = append(ends, r.URL.Query().Get("end"))
						next := "stale"
						if roots > 1 {
							next = "fresh"
						}
						fmt.Fprintf(w, `{"data":[{"id":"first"}],"continuationToken":%q}`, next)
					case "stale":
						failures++
						if repeat {
							fmt.Fprint(w, `{"data":[],"continuationToken":"stale"}`)
						} else {
							w.WriteHeader(400)
						}
					case "fresh":
						fmt.Fprint(w, `{"data":[{"id":"recovered"}]}`)
						stop.Set()
					}
				}))
				defer srv.Close()
				a := &BitwardenAdapter{conf: BitwardenConfig{ClientOptions: uspclient.ClientOptions{DebugLog: func(string) {}, OnWarning: func(string) {}, OnError: func(error) {}}}, doStop: stop, uspClient: &captureSink{}, httpClient: srv.Client(), eventsBaseURL: srv.URL, accessToken: "test", tokenExpiry: time.Now().Add(time.Hour), lastStartDate: &start, pollInterval: time.Millisecond}
				if initial {
					a.lastStartDate = nil
				}
				a.wgSenders.Add(1)
				a.fetchEvents()
				require.Equal(t, 2, roots, "must restart the failed window")
				require.LessOrEqual(t, failures, 3)
				require.Equal(t, starts[0], starts[1])
				require.Equal(t, ends[0], ends[1])
				wantStart := start
				if initial {
					wantStart = time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -30)
				}
				require.Equal(t, wantStart.Format(time.RFC3339), starts[0])
				require.True(t, a.lastStartDate.After(start))
				msgs := a.uspClient.(*captureSink).snapshot()
				require.Equal(t, "recovered", msgs[len(msgs)-1].JsonPayload["id"])
			})
		}
	}
}
