package usp_entraid

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A 200 that is not a valid page keeps what Graph actually sent: status and
// body, for a JSON error envelope and a non-JSON denial page alike, and an
// invalid item names its field.
func TestEntraInvalid200KeepsVendorText(t *testing.T) {
	for _, c := range []struct {
		name, body string
		want       []string
	}{
		{"json envelope", `{"error":{"code":"LicenseRequired","message":"Tenant license expired"}}`, []string{"HTTP 200 OK", "LicenseRequired", "Tenant license expired"}},
		{"html denial", `<html><body>Access denied by gateway policy</body></html>`, []string{"HTTP 200 OK", "Access denied by gateway policy"}},
		{"item with bad timestamp", `{"value":[{"id":"bad-item","activityDateTime":"invalid-time"}]}`, []string{"bad-item", "activityDateTime", "invalid-time"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, c.body) })
			a, _ := harness(t, server, &captureSink{})
			stream := a.streams[0] // risk_detections, timestamped by activityDateTime
			clock := fixtureBaseTime()
			raw := a.windowURL(stream, clock.UnixNano(), clock.Add(time.Minute).UnixNano())
			_, err := a.requestPage(context.Background(), stream, raw, false, &graphRequests{}, &requestFailure{})
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
func TestEntraOnErrorCallingCloseDoesNotDeadlock(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		fmt.Fprint(w, `{"error":{"code":"ServiceNotAvailable","message":"Graph is down for maintenance"}}`)
	})
	a, _ := harness(t, server, &captureSink{})
	a.runtime.failureLimit = 100 * time.Millisecond
	a.ctx, a.cancel = context.WithCancel(context.Background())
	closed := make(chan struct{})
	a.conf.ClientOptions.OnError = func(error) {
		_ = a.Close()
		close(closed)
	}
	stream := a.streams[0]
	clock := fixtureBaseTime()
	raw := a.windowURL(stream, clock.UnixNano(), clock.Add(time.Minute).UnixNano())
	state, failure := &graphRequests{}, &requestFailure{}
	a.wgSenders.Add(1)
	go func() {
		defer a.wgSenders.Done()
		for a.ctx.Err() == nil {
			_, _ = a.requestPage(a.ctx, stream, raw, false, state, failure)
			failure.retryAt = time.Time{} // retry immediately; only the episode age matters here
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

// A token endpoint that answers HTTP 200 without an access_token (an error
// envelope from a gateway or proxy) keeps its own words in the error and, once
// persistent, in the OnError report.
func TestEntraTokenResponseWithoutTokenKeepsVendorText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"error":"invalid_client","error_description":"Tenant disabled by gateway policy"}`)
	}))
	defer server.Close()
	a, _ := harness(t, server, &captureSink{})
	clock := fixtureBaseTime()
	a.runtime.now = func() time.Time { return clock }
	a.runtime.failureLimit = 10 * time.Minute
	var reports []string
	a.conf.ClientOptions.OnError = func(err error) { reports = append(reports, err.Error()) }
	state, failure, stream := &graphRequests{}, &requestFailure{}, a.streams[0]
	raw := a.windowURL(stream, clock.UnixNano(), clock.Add(time.Minute).UnixNano())
	request := func() error {
		_, err := a.requestPage(context.Background(), stream, raw, false, state, failure)
		a.flushNotifications()
		return err
	}
	err := request()
	require.ErrorIs(t, err, errPage)
	clock = clock.Add(11 * time.Minute)
	require.ErrorIs(t, request(), errPage)
	require.Len(t, reports, 1)
	for _, text := range []string{err.Error(), reports[0]} {
		for _, want := range []string{"no access_token", "HTTP 200 OK", "invalid_client", "Tenant disabled by gateway policy"} {
			require.Contains(t, text, want)
		}
	}
}

// A continuation link that carries a credential is rejected and quoted in the
// error; the credential must not survive in it even when the vendor's JSON
// writes the "&" as &.
func TestEntraRejectedContinuationLinkRedactsJSONEscapedCredential(t *testing.T) {
	var a *EntraIDAdapter
	clock := fixtureBaseTime()
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		stream := a.streams[0]
		sendPage(w, nil, a.windowURL(stream, clock.UnixNano(), clock.Add(time.Minute).UnixNano())+"&token=unknown-token-123456789")
	})
	a, _ = harness(t, server, &captureSink{})
	a.runtime.now = func() time.Time { return clock }
	a.runtime.failureLimit = 10 * time.Minute
	var reports []string
	a.conf.ClientOptions.OnError = func(err error) { reports = append(reports, err.Error()) }
	state, failure, stream := &graphRequests{}, &requestFailure{}, a.streams[0]
	raw := a.windowURL(stream, clock.UnixNano(), clock.Add(time.Minute).UnixNano())
	request := func() error {
		_, err := a.requestPage(context.Background(), stream, raw, false, state, failure)
		a.flushNotifications()
		return err
	}
	err := request()
	require.ErrorIs(t, err, errPage)
	clock = clock.Add(11 * time.Minute)
	require.ErrorIs(t, request(), errPage)
	require.Len(t, reports, 1)
	for _, text := range []string{err.Error(), reports[0]} {
		require.NotContains(t, text, "unknown-token-123456789")
		require.Contains(t, text, "[REDACTED]")
	}
}
