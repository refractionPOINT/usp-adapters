package usp_okta

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/vendorerr"
	"github.com/stretchr/testify/require"
)

// A continuation link the adapter rejects is quoted in the error, so a
// credential riding in it (a query token or URL userinfo) must be redacted in
// the error text, in the debug log and in the OnError report.
func TestOktaRejectedContinuationLinkRedactsCredentials(t *testing.T) {
	for _, c := range []struct {
		name, link string
		hidden     []string
	}{
		{"query token", "http://%s" + logsURL + "?after=1&token=unknown-token-123456789", []string{"unknown-token-123456789"}},
		{"percent-encoded name", "http://%s" + logsURL + "?after=1&to%%6ben=unknown-token-123456789", []string{"unknown-token-123456789"}},
		{"userinfo", "http://alice:unknown-password-123@%s" + logsURL + "?after=1", []string{"alice", "unknown-password-123"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Link", fmt.Sprintf(`<`+c.link+`>; rel="next"`, r.Host))
				fmt.Fprint(w, `[]`)
			}))
			defer server.Close()
			a := directAdapter(t, server.URL)
			_, err := a.requestPage(context.Background(), server.URL+logsURL)
			require.ErrorIs(t, err, errInvalidPage)
			require.Contains(t, err.Error(), "rejected as a continuation")
			require.Contains(t, err.Error(), "[REDACTED]")
			for _, h := range c.hidden {
				require.NotContains(t, err.Error(), h)
			}

			// Through the collector: debug log and the persistent-failure OnError.
			local, g := localHarness(t, server.URL, &ackSink{})
			local.failureLimit = 100 * time.Millisecond
			var reports, debug reportLog
			local.conf.ClientOptions.OnError = reports.add
			local.conf.ClientOptions.DebugLog = func(m string) { debug.add(fmt.Errorf("%s", m)) }
			stop := runCollector(t, local, g)
			defer stop()
			require.Eventually(t, func() bool { return len(reports.all()) == 1 }, 10*time.Second, 10*time.Millisecond)
			for _, h := range c.hidden {
				for _, m := range append(reports.all(), debug.all()...) {
					require.NotContains(t, m, h)
				}
			}
		})
	}
}

// Okta's status and the body it managed to send survive a connection that drops
// mid-body: the status classifies the failure and the partial body is reported.
func TestOktaErrorBodyCutShortStillReportsStatusAndBody(t *testing.T) {
	defer vendorerr.SetBodyTimeoutsForTest(10*time.Second, 10*time.Second)()
	var status int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := `{"errorSummary":"License expired"}`
		w.Header().Set("Content-Length", fmt.Sprint(len(body)+100)) // promise more than is sent
		w.WriteHeader(status)
		fmt.Fprint(w, body)
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler) // drop the connection mid-body
	}))
	defer server.Close()
	a := directAdapter(t, server.URL)
	for code, kind := range map[int]error{403: errVendorCredential, 503: errRequestRetry, 404: errInvalidPage} {
		status = code
		_, err := a.requestPage(context.Background(), server.URL+logsURL)
		require.ErrorIs(t, err, kind)
		require.Contains(t, err.Error(), fmt.Sprintf("HTTP %d", code))
		require.Contains(t, err.Error(), "License expired")
	}
}

// A 200 that is not a log page keeps what Okta actually sent: status and body,
// for a JSON error envelope and for a non-JSON denial page alike.
func TestOktaInvalid200KeepsVendorText(t *testing.T) {
	for _, c := range []struct {
		name, body string
		want       []string
	}{
		{"json envelope", `{"error":{"code":"LicenseRequired","message":"Tenant license expired"}}`, []string{"HTTP 200 OK", "LicenseRequired", "Tenant license expired"}},
		{"html denial", `<html><body>Access denied by gateway policy</body></html>`, []string{"HTTP 200 OK", "Access denied by gateway policy"}},
		{"null page", `null`, []string{"not a log page", "HTTP 200 OK"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, c.body) }))
			defer server.Close()
			a := directAdapter(t, server.URL)
			_, err := a.requestPage(context.Background(), server.URL+logsURL)
			require.ErrorIs(t, err, errInvalidPage)
			for _, w := range c.want {
				require.Contains(t, err.Error(), w)
			}
		})
	}
}

// OnError may call Close. The persistent-failure report is delivered outside
// the collector Close joins, so this must return rather than deadlock, and
// nothing is delivered once Close has started.
func TestOktaOnErrorCallingCloseDoesNotDeadlock(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		fmt.Fprint(w, `{"errorSummary":"Not found: logs"}`)
	}))
	defer server.Close()
	a, g := localHarness(t, server.URL, &ackSink{})
	a.failureLimit = 100 * time.Millisecond
	a.ctx, a.cancel = context.WithCancel(context.Background())
	closed := make(chan struct{})
	a.conf.ClientOptions.OnError = func(error) {
		_ = a.Close()
		close(closed)
	}
	a.wgSenders.Add(1)
	go func() { defer a.wgSenders.Done(); a.collect(a.ctx, g) }()
	select {
	case <-closed:
	case <-time.After(15 * time.Second):
		t.Fatal("Close called from OnError deadlocked against the collector")
	}
	var late reportLog
	a.conf.ClientOptions.OnError = late.add
	a.notify(fmt.Errorf("after close"))
	a.flushNotifications()
	require.Empty(t, late.all(), "notifications are dropped once Close has started")
}

// Close waits for an OnError callback that is already running, so an embedder
// never sees OnError (or a test its t.Logf) after Close has returned.
func TestOktaCloseWaitsForRunningOnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()
	a, _ := localHarness(t, server.URL, &ackSink{})
	a.ctx, a.cancel = context.WithCancel(context.Background())
	started := make(chan struct{})
	var finished atomic.Bool
	a.conf.ClientOptions.OnError = func(error) {
		close(started)
		time.Sleep(200 * time.Millisecond)
		finished.Store(true)
	}
	a.notify(fmt.Errorf("vendor said no"))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("OnError was never called")
	}
	require.NoError(t, a.Close())
	require.True(t, finished.Load(), "OnError was still running after Close returned")
}

// A persistent-outage report refused because OnError is blocked and the queue
// is full is not marked as sent: once the callback is released and the failure
// goes on, the report is delivered.
func TestOktaPersistentFailureReportSurvivesFullNotifierQueue(t *testing.T) {
	a := directAdapter(t, "http://127.0.0.1:1")
	a.failureLimit = time.Minute
	var mu sync.Mutex
	var got []string
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	a.conf.ClientOptions.OnError = func(err error) {
		once.Do(func() { close(started); <-release })
		mu.Lock()
		got = append(got, err.Error())
		mu.Unlock()
	}
	require.True(t, a.notify(fmt.Errorf("blocker")))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("OnError was never called")
	}
	for i := 0; a.notify(fmt.Errorf("other report %d", i)); i++ {
		require.Less(t, i, 100, "the notifier queue is not bounded")
	}

	episode := &failureEpisode{since: time.Now().Add(-time.Hour)}
	a.retryFailure(episode, errors.New("HTTP 503 Service Unavailable: Okta is down"))
	require.False(t, episode.reported, "a refused report must not be marked as sent")

	close(release)
	a.flushNotifications()
	a.retryFailure(episode, errors.New("HTTP 503 Service Unavailable: Okta is down"))
	require.True(t, episode.reported)
	a.flushNotifications()
	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, got[len(got)-1], "Okta is down")
	a.notifier.Close()
}
