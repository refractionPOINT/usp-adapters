package usp_defender

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// An alert item without a usable id or createdDateTime used to pass page
// validation (clearing the failure episode) and then fail again in the shipper
// on every refetch, so nothing was ever reported. It is now a page failure: it
// reports once after the interval, naming the offending item and field.
func TestDefenderPersistentMalformedItemReachesOnErrorOnce(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"value":[{"id":"bad-item","createdDateTime":"invalid-time"}]}`)
	})
	a, g := harness(t, server, &captureSink{})
	a.runtime.failureLimit = 200 * time.Millisecond
	var reports []string
	a.conf.ClientOptions.OnError = func(err error) { reports = append(reports, err.Error()) }
	requests := &graphRequests{}
	raw := a.windowURL(g.position.Boundary, g.position.WindowEnd)
	request := func() {
		_, err := a.requestPage(context.Background(), raw, false, requests)
		a.flushNotifications()
		require.ErrorIs(t, err, errPage)
	}

	request()
	require.Empty(t, reports, "the first failure of an episode is a blip")
	time.Sleep(250 * time.Millisecond)
	request()
	require.Len(t, reports, 1)
	require.Contains(t, reports[0], "bad-item")
	require.Contains(t, reports[0], "createdDateTime")
	require.Contains(t, reports[0], "invalid-time")
	request()
	require.Len(t, reports, 1, "one report per episode")
}

func TestDefenderInvalid200KeepsVendorText(t *testing.T) {
	for _, c := range []struct {
		name, body string
		want       []string
	}{
		{"json envelope", `{"error":{"code":"LicenseRequired","message":"Tenant license expired"}}`, []string{"HTTP 200 OK", "LicenseRequired", "Tenant license expired"}},
		{"html denial", `<html><body>Access denied by gateway policy</body></html>`, []string{"HTTP 200 OK", "Access denied by gateway policy"}},
		{"item without id", `{"value":[{"createdDateTime":"2026-01-02T03:04:05Z"}]}`, []string{"alert item 0 has no valid id", "HTTP 200 OK"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, c.body) })
			a, g := harness(t, server, &captureSink{})
			_, err := a.requestPage(context.Background(), a.windowURL(g.position.Boundary, g.position.WindowEnd), false, &graphRequests{})
			require.ErrorIs(t, err, errPage)
			for _, w := range c.want {
				require.Contains(t, err.Error(), w)
			}
		})
	}
}

// OnError may call Close. The persistent-failure report is delivered outside
// the worker Close joins, so this must return rather than deadlock, and nothing
// is delivered once Close has started.
func TestDefenderOnErrorCallingCloseDoesNotDeadlock(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		fmt.Fprint(w, `{"error":{"code":"ResourceNotFound","message":"Alerts collection moved"}}`)
	})
	a, g := harness(t, server, &captureSink{})
	a.runtime.failureLimit = 100 * time.Millisecond
	a.ctx, a.cancel = context.WithCancel(context.Background())
	closed := make(chan struct{})
	a.conf.ClientOptions.OnError = func(error) {
		_ = a.Close()
		close(closed)
	}
	raw := a.windowURL(g.position.Boundary, g.position.WindowEnd)
	requests := &graphRequests{}
	a.wgSenders.Add(1)
	go func() {
		defer a.wgSenders.Done()
		for a.ctx.Err() == nil {
			_, _ = a.requestPage(a.ctx, raw, false, requests)
			time.Sleep(20 * time.Millisecond)
		}
	}()
	select {
	case <-closed:
	case <-time.After(15 * time.Second):
		t.Fatal("Close called from OnError deadlocked against the worker")
	}
	var late []string
	a.conf.ClientOptions.OnError = func(err error) { late = append(late, err.Error()) }
	a.notify(fmt.Errorf("after close"))
	a.flushNotifications()
	require.Empty(t, late, "notifications are dropped once Close has started")
}
