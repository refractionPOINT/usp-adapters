package usp_bitwarden

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

func TestPagingFailureRetainsWindow(t *testing.T) {
	for _, firstWindow := range []bool{false, true} {
		t.Run(fmt.Sprint(firstWindow), func(t *testing.T) {
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			roots, pages := 0, 0
			var rootStart string
			a := &BitwardenAdapter{uspClient: &captureSink{}, doStop: utils.NewEvent(), accessToken: "test", tokenExpiry: time.Now().Add(time.Hour), pollInterval: time.Millisecond}
			a.conf.ClientOptions = uspclient.ClientOptions{DebugLog: func(string) {}, OnWarning: func(string) {}, OnError: func(error) {}}
			if !firstWindow {
				a.lastStartDate = &start
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("continuationToken") == "" {
					roots++
					rootStart = r.URL.Query().Get("start")
					fmt.Fprint(w, `{"data":[{"id":"first"}],"continuationToken":"next"}`)
				} else {
					pages++
					if pages == 1 {
						w.WriteHeader(503)
						return
					}
					if r.URL.Query().Get("continuationToken") == "next" {
						fmt.Fprint(w, `{"data":[{"id":"middle"}],"continuationToken":"last"}`)
						return
					}
					fmt.Fprint(w, `{"data":[{"id":"last"}]}`)
					a.doStop.Set()
				}
			}))
			defer srv.Close()
			a.httpClient = srv.Client()
			a.eventsBaseURL = srv.URL
			a.wgSenders.Add(1)
			done := make(chan struct{})
			go func() { a.fetchEvents(); close(done) }()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				a.doStop.Set()
				<-done
				t.Fatal("collector did not finish")
			}
			// A failed continuation is retried without restarting or advancing the window.
			if !firstWindow {
				require.Equal(t, start.Format(time.RFC3339), rootStart)
			} else {
				require.Equal(t, time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -30).Format(time.RFC3339), rootStart)
			}
			require.Equal(t, 3, pages)
			require.Equal(t, 3, a.uspClient.(*captureSink).count())
			require.Equal(t, 1, roots)
		})
	}
}
