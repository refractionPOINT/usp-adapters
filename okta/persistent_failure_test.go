package usp_okta

import (
	"fmt"
	"github.com/refractionPOINT/usp-adapters/internal/vendorerr"
	"github.com/refractionPOINT/usp-adapters/utils"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type reportLog struct {
	mu   sync.Mutex
	msgs []string
	errs []error
}

func (r *reportLog) add(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, err.Error())
	r.errs = append(r.errs, err)
}
func (r *reportLog) errors() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]error(nil), r.errs...)
}
func (r *reportLog) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.msgs...)
}

func okPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=1>; rel="next"`, r.Host, logsURL))
	fmt.Fprint(w, `[]`)
}

// A persistent non-credential Okta failure (404 or 503 with a JSON body) used
// to retry forever with only a debug log line. It must reach OnError once, with
// Okta's own text, after the failure interval.
func TestOktaPersistentVendorFailureReachesOnErrorOnce(t *testing.T) {
	defer vendorerr.SetBodyTimeoutsForTest(10*time.Second, 10*time.Second)()
	for _, status := range []int{404, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(status)
				fmt.Fprintf(w, `{"errorCode":"E0000007","errorSummary":"Not found: Resource not found: logs (LogEvent)","echo":%q}`, r.Header.Get("Authorization"))
			}))
			defer server.Close()
			a, g := localHarness(t, server.URL, &ackSink{})
			a.failureLimit = 100 * time.Millisecond
			var reports reportLog
			a.conf.ClientOptions.OnError = reports.add
			stop := runCollector(t, a, g)
			defer stop()
			// Retries back off 1s, 2s, ...: the second failure is past the interval.
			require.Eventually(t, func() bool { return len(reports.all()) == 1 }, 10*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool { return requests.Load() >= 3 }, 10*time.Second, 10*time.Millisecond)
			got := reports.all()
			require.Len(t, got, 1, "later retries of the same episode stay quiet")
			require.Contains(t, got[0], fmt.Sprintf("HTTP %d", status))
			require.Contains(t, got[0], "Resource not found: logs (LogEvent)")
			require.NotContains(t, got[0], a.conf.ApiKey)
			require.ErrorIs(t, reports.errors()[0], utils.ErrPersistentVendorFailure, "the adapter keeps retrying, so embedders must not restart it")
		})
	}
}

// A blip that recovers before the interval ends never reaches OnError.
func TestOktaShortVendorBlipStaysQuiet(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(503)
			fmt.Fprint(w, `{"errorSummary":"temporarily unavailable"}`)
			return
		}
		okPage(w, r)
	}))
	defer server.Close()
	a, g := localHarness(t, server.URL, &ackSink{})
	a.failureLimit = 100 * time.Millisecond
	var reports reportLog
	a.conf.ClientOptions.OnError = reports.add
	stop := runCollector(t, a, g)
	defer stop()
	require.Eventually(t, func() bool { return requests.Load() >= 3 }, 10*time.Second, 10*time.Millisecond)
	require.Empty(t, reports.all())
}

// A successful page ends the episode: a later persistent failure reports again.
func TestOktaVendorFailureReportRearmsAfterSuccess(t *testing.T) {
	defer vendorerr.SetBodyTimeoutsForTest(10*time.Second, 10*time.Second)()
	var failing atomic.Bool
	var okSeen atomic.Int32
	failing.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failing.Load() {
			w.WriteHeader(404)
			fmt.Fprint(w, `{"errorSummary":"Not found: logs"}`)
			return
		}
		okSeen.Add(1)
		okPage(w, r)
	}))
	defer server.Close()
	a, g := localHarness(t, server.URL, &ackSink{})
	a.failureLimit = 100 * time.Millisecond
	var reports reportLog
	a.conf.ClientOptions.OnError = reports.add
	stop := runCollector(t, a, g)
	defer stop()
	require.Eventually(t, func() bool { return len(reports.all()) == 1 }, 10*time.Second, 10*time.Millisecond)
	failing.Store(false)
	require.Eventually(t, func() bool { return okSeen.Load() >= 1 }, 10*time.Second, 10*time.Millisecond)
	failing.Store(true)
	require.Eventually(t, func() bool { return len(reports.all()) == 2 }, 10*time.Second, 10*time.Millisecond)
}

// Only the "still retrying" report is advisory. A credential failure is
// reported as before; a failing continuation request (same cursor retried) is
// advisory like any other persistent failure.
func TestOktaAdvisoryMarkOnlyOnPersistentRetryReports(t *testing.T) {
	defer vendorerr.SetBodyTimeoutsForTest(10*time.Second, 10*time.Second)()
	t.Run("credential", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(401)
			fmt.Fprint(w, `{"errorSummary":"Invalid token provided"}`)
		}))
		defer server.Close()
		a, g := localHarness(t, server.URL, &ackSink{})
		a.failureLimit = 100 * time.Millisecond
		var reports reportLog
		a.conf.ClientOptions.OnError = reports.add
		stop := runCollector(t, a, g)
		defer stop()
		require.Eventually(t, func() bool { return len(reports.errors()) >= 1 }, 10*time.Second, 10*time.Millisecond)
		for _, err := range reports.errors() {
			require.Contains(t, err.Error(), "Invalid token provided")
			require.NotErrorIs(t, err, utils.ErrPersistentVendorFailure)
		}
	})
	t.Run("stalled continuation", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(400)
			fmt.Fprint(w, `{"errorCode":"E0000001","errorSummary":"Invalid cursor"}`)
		}))
		defer server.Close()
		a, g := localHarness(t, server.URL, &ackSink{})
		// Resume from an opaque cursor, so the 400 E0000001 is classified as a
		// rejected continuation rather than an invalid first page.
		g.position.Next += "&after=opaque-cursor-1"
		a.failureLimit = 100 * time.Millisecond
		var reports reportLog
		a.conf.ClientOptions.OnError = reports.add
		var stalled atomic.Bool
		a.conf.ClientOptions.DebugLog = func(s string) {
			if s == "okta collection: continuation_stalled" {
				stalled.Store(true)
			}
		}
		stop := runCollector(t, a, g)
		defer stop()
		require.Eventually(t, func() bool { return len(reports.errors()) >= 1 }, 10*time.Second, 10*time.Millisecond)
		require.True(t, stalled.Load(), "the failure must go through the stalled-continuation branch")
		err := reports.errors()[0]
		require.Contains(t, err.Error(), "Invalid cursor")
		require.ErrorIs(t, err, utils.ErrPersistentVendorFailure)
	})
}
