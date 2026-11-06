package usp_entraid

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/vendorerr"
	"github.com/stretchr/testify/require"
)

// Graph answers that are retried (503) or invalid (a 200 that is not a page)
// must reach OnError once, with the vendor's text, after the failure interval.
func TestEntraPersistentVendorFailuresReachOnErrorWithVendorText(t *testing.T) {
	// A slow -race machine must not cut the body at the production deadline;
	// the server closes it, so this does not slow the test down.
	defer vendorerr.SetBodyTimeoutsForTest(10*time.Second, 10*time.Second)()
	for _, c := range []struct {
		name    string
		handler http.HandlerFunc
		want    []string
	}{
		{"unavailable", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(503)
			fmt.Fprint(w, `{"error":{"code":"ServiceNotAvailable","message":"Graph is down for maintenance"}}`)
		}, []string{"HTTP 503", "Graph is down for maintenance"}},
		{"invalid page", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"value":null}`)
		}, []string{"not a valid page"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			server := vendorServer(t, c.handler)
			a, _ := harness(t, server, &captureSink{})
			clock := fixtureBaseTime()
			a.runtime.now = func() time.Time { return clock }
			a.runtime.failureLimit = 10 * time.Minute
			var reports []string
			a.conf.ClientOptions.OnError = func(err error) { reports = append(reports, err.Error()) }
			state, failure, stream := &graphRequests{}, &requestFailure{}, a.streams[0]
			raw := a.windowURL(stream, clock.UnixNano(), clock.Add(time.Minute).UnixNano())
			request := func() {
				_, err := a.requestPage(context.Background(), stream, raw, false, state, failure)
				require.Error(t, err)
			}
			request()
			require.Empty(t, reports, "the first failure of an episode is a blip")
			clock = clock.Add(11 * time.Minute)
			request()
			require.Len(t, reports, 1)
			for _, want := range c.want {
				require.Contains(t, reports[0], want)
			}
			clock = clock.Add(11 * time.Minute)
			request()
			require.Len(t, reports, 1, "one report per episode")
		})
	}
}
