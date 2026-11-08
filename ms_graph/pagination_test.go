package usp_ms_graph

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// All responses and credentials are synthetic. The script controls only the
// Graph endpoint; the existing mock validates the token exchange.
func scriptedGraph(t *testing.T, graph http.HandlerFunc) (*httptest.Server, MsGraphConfig) {
	t.Helper()
	token := newMockMsGraph()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/"+testTenantID+"/oauth2/v2.0/token" {
			token.serveToken(w, r)
			return
		}
		graph(w, r)
	}))
	t.Cleanup(server.Close)
	return server, testConfig(t, server)
}

func page(w http.ResponseWriter, records []map[string]interface{}, next string) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"value": records, "@odata.nextLink": next})
}

func stableCount(t *testing.T, sink *captureSink, count int) {
	t.Helper()
	require.Eventually(t, func() bool { return sink.count() == count }, 10*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return sink.count() != count }, 200*time.Millisecond, 10*time.Millisecond)
}

func TestMsGraphEmptyPagesAndBoundaryIDs(t *testing.T) {
	base := fixtureBase().Truncate(time.Second)
	var scans atomic.Int32
	var pages atomic.Int32
	_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1.0/" + testResource:
			scans.Add(1)
			// An empty page is not the end of a collection.
			page(w, []map[string]interface{}{}, "/middle?opaque=a%2Bb")
		case "/middle":
			pages.Add(1)
			assert.Equal(t, "a+b", r.URL.Query().Get("opaque"))
			page(w, []map[string]interface{}{
				graphSecurityAlert("tie-a", base.Format(time.RFC3339Nano)),
				graphSecurityAlert("older", base.Add(-time.Minute).Format(time.RFC3339Nano)),
			}, "/last")
		case "/last":
			// Same instant expressed with an offset must share the boundary.
			records := []map[string]interface{}{graphSecurityAlert("tie-b", base.In(time.FixedZone("offset", 3600)).Format(time.RFC3339Nano))}
			if scans.Load() > 1 {
				records = append(records, graphSecurityAlert("tie-c", base.Format(time.RFC3339Nano)))
			}
			page(w, records, "")
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	})
	sink, _, _ := startAdapter(t, conf)
	stableCount(t, sink, 4)
	assert.GreaterOrEqual(t, pages.Load(), int32(2))
}

func TestMsGraphMergeQuery(t *testing.T) {
	stamp := graphTimestamp(fixtureBase())
	var requests atomic.Int32
	_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		assert.Equal(t, "7", r.URL.Query().Get("$top"))
		assert.Equal(t, "id,createdDateTime", r.URL.Query().Get("$select"))
		assert.Contains(t, r.URL.Query().Get("$filter"), "(severity eq 'high' or severity eq 'medium') and (createdDateTime ge ")
		page(w, []map[string]interface{}{graphSecurityAlert("a", stamp)}, "")
	})
	conf.URL += "?$top=7&$select=id%2CcreatedDateTime&$filter=severity%20eq%20'high'%20or%20severity%20eq%20'medium'"
	sink, _, _ := startAdapter(t, conf)
	stableCount(t, sink, 1)
	assert.Greater(t, requests.Load(), int32(1))
}

type failOnceSink struct {
	captureSink
	calls atomic.Int32
}

func (s *failOnceSink) Ship(m *protocol.DataMessage, d time.Duration) error {
	if s.calls.Add(1) == 2 {
		return errors.New("injected shipment failure")
	}
	return s.captureSink.Ship(m, d)
}

func TestMsGraphFailuresKeepProgress(t *testing.T) {
	for _, failure := range []string{"page", "shipping", "invalid-envelope"} {
		t.Run(failure, func(t *testing.T) {
			base := fixtureBase()
			newest := graphSecurityAlert("newest", graphTimestamp(base.Add(time.Minute)))
			older := graphSecurityAlert("older", graphTimestamp(base))
			var attempts atomic.Int32
			_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/last" {
					page(w, []map[string]interface{}{newest}, "/last")
					return
				}
				n := attempts.Add(1)
				if n == 1 {
					switch failure {
					case "page":
						w.WriteHeader(http.StatusBadGateway)
						return
					case "invalid-envelope":
						writeJSON(w, 200, map[string]interface{}{"value": nil})
						return
					}
				}
				page(w, []map[string]interface{}{older}, "")
			})
			if failure == "shipping" {
				sink := &failOnceSink{}
				a, _, err := newMsGraphAdapter(context.Background(), conf, sink)
				require.NoError(t, err)
				t.Cleanup(func() { _ = a.Close() })
				stableCount(t, &sink.captureSink, 2)
			} else {
				sink, _, _ := startAdapter(t, conf)
				stableCount(t, sink, 2)
			}
			assert.Greater(t, attempts.Load(), int32(1))
		})
	}
}

func TestMsGraphUnsafeContinuations(t *testing.T) {
	for _, mode := range []string{"cross-origin", "redirect", "token-redirect", "cycle"} {
		t.Run(mode, func(t *testing.T) {
			var leaked atomic.Int32
			other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1); w.WriteHeader(200) }))
			defer other.Close()
			var requests atomic.Int32
			_, conf := scriptedGraph(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				switch mode {
				case "redirect":
					http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
				case "cross-origin":
					page(w, []map[string]interface{}{}, other.URL)
				case "cycle":
					page(w, []map[string]interface{}{}, "/cycle")
				}
			})
			if mode == "token-redirect" {
				login := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
				}))
				defer login.Close()
				conf.LoginEndpoint = login.URL
			}
			var failures atomic.Int32
			conf.ClientOptions.OnError = func(error) { failures.Add(1) }
			if mode == "cycle" {
				conf.ClientOptions.OnWarning = func(string) { failures.Add(1) }
			}
			sink, _, _ := startAdapter(t, conf)
			require.Eventually(t, func() bool { return failures.Load() > 0 }, 5*time.Second, 10*time.Millisecond)
			assert.Zero(t, leaked.Load(), "no request, token or credentials may reach another origin")
			assert.Zero(t, sink.count())
			assert.Positive(t, requests.Load())
		})
	}
}

func TestMsGraphCloseCancelsRequests(t *testing.T) {
	for _, stage := range []string{"token", "page", "backoff"} {
		t.Run(stage, func(t *testing.T) {
			entered := make(chan struct{}, 1)
			release, unblock := context.WithCancel(context.Background())
			token := newMockMsGraph()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if stage != "token" && r.URL.Path == "/"+testTenantID+"/oauth2/v2.0/token" {
					token.serveToken(w, r)
					return
				}
				_, _ = io.Copy(io.Discard, r.Body)
				select {
				case entered <- struct{}{}:
				default:
				}
				if stage == "backoff" {
					w.WriteHeader(503)
					return
				}
				select {
				case <-r.Context().Done():
				case <-release.Done():
				}
			}))
			defer server.Close()
			defer unblock()
			a, _, err := newMsGraphAdapter(context.Background(), testConfig(t, server), &captureSink{})
			require.NoError(t, err)
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("request did not start")
			}
			stopped := make(chan struct{})
			go func() { _ = a.Close(); close(stopped) }()
			select {
			case <-stopped:
			case <-time.After(5 * time.Second):
				unblock()
				select {
				case <-stopped:
				case <-time.After(5 * time.Second):
					t.Fatal("Close remained stuck after releasing mock")
				}
				t.Fatal("Close did not cancel request/backoff")
			}
			// The old non-cancellable backoff finishes in three seconds, so
			// elapsed time alone cannot distinguish it with a generous limit.
			assert.ErrorIs(t, a.ctx.Err(), context.Canceled, "Close must cancel the reader context")
		})
	}
}

// A continuation can reject an expired token after earlier pages succeeded.
// Stop at the scan boundary so counts cannot be affected by a later poll.
func TestMsGraphRefreshTokenOnSamePage(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprintf("persistent-401=%t", persistent), func(t *testing.T) {
			var tokenCalls, firstCalls, lastCalls atomic.Int32
			var server *httptest.Server
			stamp := graphTimestamp(fixtureBase())
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/" + testTenantID + "/oauth2/v2.0/token":
					n := tokenCalls.Add(1)
					writeJSON(w, 200, map[string]interface{}{"access_token": fmt.Sprintf("fake-token-%d", n)})
				case "/v1.0/" + testResource:
					firstCalls.Add(1)
					page(w, []map[string]interface{}{graphSecurityAlert("first", stamp)}, server.URL+"/last?opaque=a%2Bb")
				case "/last":
					lastCalls.Add(1)
					assert.Equal(t, "opaque=a%2Bb", r.URL.RawQuery)
					if persistent || r.Header.Get("Authorization") == "Bearer fake-token-1" {
						w.WriteHeader(http.StatusUnauthorized)
						return
					}
					assert.Equal(t, "Bearer fake-token-2", r.Header.Get("Authorization"))
					page(w, []map[string]interface{}{graphSecurityAlert("last", stamp)}, "")
				default:
					t.Errorf("unexpected path: %s", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			conf := testConfig(t, server)
			conf.PollInterval = time.Hour // drive exactly one scan below
			sink := &captureSink{}
			a, _, err := newMsGraphAdapter(context.Background(), conf, sink)
			require.NoError(t, err)
			defer a.Close()
			_, err = a.poll(server.URL+"/v1.0/"+testResource, graphCursor{since: time.Now().UTC()}, map[graphRecordKey]bool{})
			if persistent {
				require.ErrorContains(t, err, "401")
				assert.Equal(t, 1, sink.count())
			} else {
				require.NoError(t, err)
				assert.Equal(t, 2, sink.count())
			}
			assert.EqualValues(t, 1, firstCalls.Load(), "refresh must not restart the first page")
			assert.EqualValues(t, 2, lastCalls.Load(), "one retry, including for persistent rejection")
			assert.EqualValues(t, 2, tokenCalls.Load(), "refresh must be bounded to one per page")
		})
	}
}
