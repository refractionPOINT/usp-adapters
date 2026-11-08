package usp_ms_graph

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/vendorerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func manualAdapter(t *testing.T, conf MsGraphConfig, sink uspSink) *MsGraphAdapter {
	t.Helper()
	conf.PollInterval = time.Hour
	a, _, err := newMsGraphAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func TestMsGraphPermanentMalformedRecords(t *testing.T) {
	for _, mode := range []string{"success", "page", "shipping"} {
		t.Run(mode, func(t *testing.T) {
			failed := mode != "success"
			base := fixtureBase().Truncate(100 * time.Nanosecond)
			var last atomic.Int32
			_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/last" {
					if mode == "page" && last.Add(1) <= 3 {
						w.WriteHeader(502)
						return
					}
					page(w, []map[string]interface{}{graphSecurityAlert("c", graphTimestamp(base.Add(time.Minute)))}, "")
					return
				}
				// Persistent malformed items are mid-page, before a valid item and a
				// further valid page. Non-object items count as malformed records too.
				writeJSON(w, 200, map[string]interface{}{"value": []interface{}{
					graphSecurityAlert("a", graphTimestamp(base)),
					map[string]interface{}{"createdDateTime": graphTimestamp(base)},
					map[string]interface{}{"id": "bad", "createdDateTime": "invalid"},
					42, nil, map[string]interface{}{"id": "  ", "createdDateTime": graphTimestamp(base)},
					graphSecurityAlert("b", graphTimestamp(base)),
				}, "@odata.nextLink": "/last"})
			})
			var warnings []string
			conf.ClientOptions.OnWarning = func(s string) { warnings = append(warnings, s) }
			sink := &captureSink{}
			var targetSink uspSink = sink
			if mode == "shipping" {
				failing := &failOnceSink{}
				targetSink = failing
				sink = &failing.captureSink
			}
			a := manualAdapter(t, conf, targetSink)
			cursor := graphCursor{since: time.Now().UTC(), ids: map[string]bool{}}
			delivered := map[graphRecordKey]bool{}
			target := conf.GraphEndpoint + testResource
			next, err := a.poll(target, cursor, delivered)
			if failed {
				require.Error(t, err)
				assert.Equal(t, cursor, next, "page failure must not commit any progress")
				assert.Equal(t, []string{"skipped 5 malformed Graph record(s)"}, warnings, "defer must report even a failed poll")
				next, err = a.poll(target, cursor, delivered)
			}
			require.NoError(t, err)
			assert.Equal(t, base.Add(time.Minute).UTC(), next.since)
			assert.Equal(t, map[string]bool{"c": true}, next.ids)
			assert.Equal(t, 3, sink.count(), "valid records after bad records and on later pages must ship once")
			ids := map[string]int{}
			for _, msg := range sink.snapshot() {
				ids[msg.JsonPayload["id"].(string)]++
			}
			assert.Equal(t, map[string]int{"a": 1, "b": 1, "c": 1}, ids)
			count := 1
			if failed {
				count = 2
			}
			require.Len(t, warnings, count)
			for _, warning := range warnings {
				assert.Equal(t, "skipped 5 malformed Graph record(s)", warning)
			}
		})
	}
}

// Fatal callbacks are consumed by a supervisor that closes the instance, as a
// host does. A logging-only callback would conceal broken transient recovery.
func TestMsGraphHostedRecovery(t *testing.T) {
	for _, mode := range []string{"429", "500", "exhausted-502", "network", "read", "token-503", "shipping", "envelope"} {
		t.Run(mode, func(t *testing.T) {
			base := fixtureBase().Truncate(100 * time.Nanosecond)
			var attempts, tokenAttempts atomic.Int32
			token := newMockMsGraph()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/token") {
					if mode == "token-503" && tokenAttempts.Add(1) <= 3 {
						w.WriteHeader(503)
						return
					}
					token.serveToken(w, r)
					return
				}
				if r.URL.Path != "/last" {
					if mode == "network" {
						w.Header().Set("Connection", "close")
					}
					page(w, []map[string]interface{}{graphSecurityAlert("newest", graphTimestamp(base.Add(time.Minute)))}, "/last")
					return
				}
				n := attempts.Add(1)
				if n == 1 {
					switch mode {
					case "429":
						w.Header().Set("Retry-After", "1")
						w.WriteHeader(429)
						return
					case "envelope":
						writeJSON(w, 200, map[string]interface{}{"value": nil})
						return
					case "500":
						w.WriteHeader(500)
						return
					case "network":
						conn, _, err := w.(http.Hijacker).Hijack()
						if err == nil {
							_ = conn.Close()
						}
						return
					case "read":
						w.Header().Set("Content-Length", "10000")
						_, _ = io.WriteString(w, "{broken")
						return
					}
				}
				if mode == "exhausted-502" && n <= 3 {
					w.WriteHeader(502)
					return
				}
				page(w, []map[string]interface{}{graphSecurityAlert("older", graphTimestamp(base))}, "")
			}))
			defer server.Close()
			conf := testConfig(t, server)
			fatal := make(chan error, 1)
			var warnings, fatals atomic.Int32
			conf.ClientOptions.OnError = func(err error) {
				fatals.Add(1)
				select {
				case fatal <- err:
				default:
				}
			}
			conf.ClientOptions.OnWarning = func(string) { warnings.Add(1) }
			var sink uspSink
			capture := &captureSink{}
			if mode == "shipping" {
				failing := &failOnceSink{}
				sink = failing
				capture = &failing.captureSink
			} else {
				sink = capture
			}
			a, _, err := newMsGraphAdapter(context.Background(), conf, sink)
			require.NoError(t, err)
			stop, done := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				select {
				case <-fatal:
					_ = a.Close()
				case <-stop:
				}
			}()
			defer func() { close(stop); <-done; _ = a.Close() }()
			stableCount(t, capture, 2)
			ids := map[string]int{}
			for _, msg := range capture.snapshot() {
				ids[msg.JsonPayload["id"].(string)]++
			}
			assert.Equal(t, map[string]int{"newest": 1, "older": 1}, ids)
			assert.Zero(t, fatals.Load(), "transient failures must not kill the hosted instance")
			if mode == "shipping" || mode == "exhausted-502" || mode == "token-503" || mode == "envelope" {
				assert.Positive(t, warnings.Load(), "failed scans must warn")
			}
			select {
			case <-a.chStopped:
				t.Fatal("reader stopped during recovery")
			default:
			}
		})
	}
}

func TestMsGraphRetryAfterAndDiagnostics(t *testing.T) {
	defer vendorerr.SetBodyTimeoutsForTest(10*time.Second, 10*time.Second)()
	for _, endpoint := range []string{"graph", "token"} {
		t.Run(endpoint, func(t *testing.T) {
			var calls atomic.Int32
			var times []time.Time // request callbacks are sequential for this scan
			token := newMockMsGraph()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				isToken := strings.HasSuffix(r.URL.Path, "/token")
				if isToken && endpoint != "token" {
					token.serveToken(w, r)
					return
				}
				if !isToken && endpoint == "token" {
					page(w, []map[string]interface{}{}, "")
					return
				}
				times = append(times, time.Now())
				if calls.Add(1) == 1 {
					w.Header().Set("Retry-After", "2")
					w.WriteHeader(429)
					_, _ = io.WriteString(w, "temporary")
					return
				}
				if isToken {
					token.serveToken(w, r)
				} else {
					page(w, []map[string]interface{}{}, "")
				}
			}))
			defer server.Close()
			conf := testConfig(t, server)
			a := manualAdapter(t, conf, &captureSink{})
			_, err := a.poll(conf.GraphEndpoint+testResource, graphCursor{since: time.Now().UTC()}, map[graphRecordKey]bool{})
			require.NoError(t, err)
			require.Len(t, times, 2)
			assert.GreaterOrEqual(t, times[1].Sub(times[0]), 2*time.Second, "must honor Retry-After rather than the 1s default")
		})
	}
	for _, endpoint := range []string{"graph", "token"} {
		t.Run("diagnostic-"+endpoint, func(t *testing.T) {
			token := newMockMsGraph()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/token") && endpoint != "token" {
					token.serveToken(w, r)
					return
				}
				w.WriteHeader(403)
				_, _ = fmt.Fprintf(w, "AADSTS7000222 expired secret %s access_token=%s %s", testClientSecret, testAccessToken, strings.Repeat("tail ", 2000))
			}))
			defer server.Close()
			conf := testConfig(t, server)
			a := manualAdapter(t, conf, &captureSink{})
			_, err := a.poll(conf.GraphEndpoint+testResource, graphCursor{since: time.Now().UTC()}, map[graphRecordKey]bool{})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "403")
			assert.Contains(t, err.Error(), "AADSTS7000222")
			assert.NotContains(t, err.Error(), testClientSecret)
			assert.NotContains(t, err.Error(), testAccessToken)
			assert.Less(t, len(err.Error()), 3000)
		})
	}
}

func TestMsGraphRetryAfterSchedulingAndPrecision(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	var query string
	_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query().Get("$filter")
		page(w, []map[string]interface{}{}, "")
	})
	a := manualAdapter(t, conf, &captureSink{})
	stamp := now.Add(123456789 * time.Nanosecond)
	_, err := a.poll(conf.GraphEndpoint+testResource, graphCursor{since: stamp}, map[graphRecordKey]bool{})
	require.NoError(t, err)
	assert.Equal(t, "createdDateTime ge "+now.Add(123456700*time.Nanosecond).Format("2006-01-02T15:04:05.0000000Z"), query)
}

func TestMsGraphFinalRetryAfterSurvivesPollFailure(t *testing.T) {
	var firsts, lasts atomic.Int32
	var final, restarted time.Time
	token := newMockMsGraph()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/token") {
			token.serveToken(w, r)
			return
		}
		if r.URL.Path != "/last" {
			if firsts.Add(1) == 2 {
				restarted = time.Now()
			}
			page(w, []map[string]interface{}{}, "/last")
			return
		}
		if lasts.Add(1) <= 3 {
			if lasts.Load() == 3 {
				final = time.Now()
				w.Header().Set("Retry-After", "2")
			}
			w.WriteHeader(503)
			return
		}
		page(w, []map[string]interface{}{graphSecurityAlert("a", graphTimestamp(fixtureBase()))}, "")
	}))
	defer server.Close()
	conf := testConfig(t, server)
	var fatals atomic.Int32
	conf.ClientOptions.OnError = func(error) { fatals.Add(1) }
	sink, a, _ := startAdapter(t, conf)
	require.Eventually(t, func() bool { return sink.count() == 1 }, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, a.Close()) // joins all handlers before reading their timestamps
	assert.Zero(t, fatals.Load())
	assert.GreaterOrEqual(t, restarted.Sub(final), 2*time.Second)
}

func TestMsGraphHTTPDateRetryAfter(t *testing.T) {
	var calls atomic.Int32
	var deadline, retried time.Time
	_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			deadline = time.Now().UTC().Add(3 * time.Second).Truncate(time.Second)
			w.Header().Set("Retry-After", deadline.Format(http.TimeFormat))
			w.WriteHeader(503)
			return
		}
		retried = time.Now()
		page(w, []map[string]interface{}{}, "")
	})
	a := manualAdapter(t, conf, &captureSink{})
	_, err := a.poll(conf.GraphEndpoint+testResource, graphCursor{since: time.Now().UTC()}, map[graphRecordKey]bool{})
	require.NoError(t, err)
	assert.False(t, retried.Before(deadline), "HTTP-date Retry-After must not be shortened")
}

func TestMsGraphFatalCallbackCanClose(t *testing.T) {
	_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403) })
	ready := make(chan *MsGraphAdapter, 1)
	done := make(chan struct{})
	var fatals atomic.Int32
	conf.ClientOptions.OnError = func(error) {
		fatals.Add(1)
		a := <-ready
		_ = a.Close()
		close(done)
	}
	a, _, err := newMsGraphAdapter(context.Background(), conf, &captureSink{})
	require.NoError(t, err)
	ready <- a
	defer a.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fatal callback deadlocked while closing adapter")
	}
	assert.EqualValues(t, 1, fatals.Load())
}
