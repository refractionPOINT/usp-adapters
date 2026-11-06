package usp_defender

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A persistent non-retryable vendor answer (here a 404 with a JSON body) must
// reach OnError once, with the vendor's text, after the failure interval. A
// short blip stays quiet and a valid page re-arms the report.
func TestDefenderPersistentInvalidAnswerReachesOnErrorOnce(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(404)
			fmt.Fprint(w, `{"error":{"code":"ResourceNotFound","message":"Alerts collection moved","echo":"Bearer fixture-bearer"}}`)
			return
		}
		sendPage(w, nil, "")
	})
	a, g := harness(t, server, &captureSink{})
	a.runtime.failureLimit = 200 * time.Millisecond
	var reports []string
	a.conf.ClientOptions.OnError = func(err error) { reports = append(reports, err.Error()) }
	requests := &graphRequests{}
	raw := a.windowURL(g.position.Boundary, g.position.WindowEnd)
	request := func() error {
		_, err := a.requestPage(context.Background(), raw, false, requests)
		a.flushNotifications()
		return err
	}

	require.ErrorIs(t, request(), errPage)
	require.Empty(t, reports, "the first failure of an episode is a blip")
	time.Sleep(250 * time.Millisecond)
	require.ErrorIs(t, request(), errPage)
	require.Len(t, reports, 1)
	require.Contains(t, reports[0], "HTTP 404 Not Found")
	require.Contains(t, reports[0], "Alerts collection moved")
	require.NotContains(t, reports[0], "fixture-bearer")
	require.ErrorIs(t, request(), errPage)
	require.Len(t, reports, 1, "one report per episode")

	fail.Store(false)
	_, err := a.requestPage(context.Background(), raw, false, requests)
	require.NoError(t, err)
	fail.Store(true)
	require.ErrorIs(t, request(), errPage)
	require.Len(t, reports, 1, "a recovered episode starts over quietly")
	time.Sleep(250 * time.Millisecond)
	require.ErrorIs(t, request(), errPage)
	require.Len(t, reports, 2, "a new persistent failure reports again")
}

// A 200 answer that is not a valid alert page (here without a value array) is
// still a persistent vendor failure: it must report once after the interval,
// not retry silently forever.
func TestDefenderPersistentMalformedPageReachesOnErrorOnce(t *testing.T) {
	var malformed atomic.Bool
	malformed.Store(true)
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		if malformed.Load() {
			fmt.Fprint(w, `{"value":null}`)
			return
		}
		sendPage(w, nil, "")
	})
	a, g := harness(t, server, &captureSink{})
	a.runtime.failureLimit = 200 * time.Millisecond
	var reports []string
	a.conf.ClientOptions.OnError = func(err error) { reports = append(reports, err.Error()) }
	requests := &graphRequests{}
	raw := a.windowURL(g.position.Boundary, g.position.WindowEnd)
	request := func() error {
		_, err := a.requestPage(context.Background(), raw, false, requests)
		a.flushNotifications()
		return err
	}

	require.ErrorIs(t, request(), errPage)
	require.Empty(t, reports, "the first failure of an episode is a blip")
	time.Sleep(250 * time.Millisecond)
	require.ErrorIs(t, request(), errPage)
	require.Len(t, reports, 1)
	require.Contains(t, reports[0], "not a valid alert page")
	require.ErrorIs(t, request(), errPage)
	require.Len(t, reports, 1, "one report per episode")

	malformed.Store(false)
	_, err := a.requestPage(context.Background(), raw, false, requests)
	require.NoError(t, err)
	require.False(t, requests.reported, "a valid page re-arms the episode")
}
