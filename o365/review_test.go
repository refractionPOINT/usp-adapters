package usp_o365

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ProxyFromEnvironment caches its environment on first use. Run this test in
// a fresh process so other HTTP tests and the developer's proxy don't affect it.
func TestTransportHonorsProxyEnvironment(t *testing.T) {
	if os.Getenv("O365_PROXY_TEST_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestTransportHonorsProxyEnvironment$", "-test.timeout=15s", "-test.v")
		cmd.Env = append(os.Environ(), "O365_PROXY_TEST_CHILD=1")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		return
	}

	const ct = "Audit.Exchange"
	m := newMockO365(t)
	record := auditRecord(t, "eeeeeeee-0000-0000-0000-000000000001", "Send", "Exchange", "user@example.com", time.Now().Add(-time.Minute))
	m.addBlob(ct, contentBlob{id: "proxy-blob", created: time.Now().Add(-time.Minute), records: []string{record}})
	backend := httptest.NewTLSServer(m.handler())
	defer backend.Close()
	var connects atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != "manage.example.com:443" {
			http.Error(w, "unexpected proxy request", http.StatusBadRequest)
			return
		}
		upstream, err := net.Dial("tcp", backend.Listener.Addr().String())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		downstream, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			upstream.Close()
			return
		}
		defer downstream.Close()
		defer upstream.Close()
		connects.Add(1)
		_, _ = rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		_ = rw.Flush()
		go func() { _, _ = io.Copy(upstream, rw); upstream.Close() }()
		_, _ = io.Copy(downstream, upstream)
	}))
	defer proxy.Close()
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("https_proxy", "")
	t.Setenv("NO_PROXY", "bypass.example.com")
	t.Setenv("no_proxy", "")
	// Trust only the test server's TLS configuration in this isolated process.
	original := http.DefaultTransport
	defaults := original.(*http.Transport).Clone()
	defaults.TLSClientConfig = backend.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	http.DefaultTransport = defaults
	defer func() { http.DefaultTransport = original; defaults.CloseIdleConnections() }()
	m.mu.Lock()
	m.baseURL = "https://manage.example.com"
	m.mu.Unlock()
	conf := newTestConfig(t, m, ct)
	conf.PollInterval = time.Hour
	sink := &captureSink{}
	a, _, closeIt := startAdapter(t, conf, sink)
	defer closeIt()
	require.Eventually(t, func() bool { return sink.count() == 1 }, 5*time.Second, 5*time.Millisecond)
	assert.Equal(t, 1, sink.payloadCounts()[record], "token, registration and content must work through the HTTPS proxy")
	assert.Positive(t, connects.Load())
	require.NotNil(t, a.transport.Proxy)
	req, err := http.NewRequest(http.MethodGet, "https://bypass.example.com/", nil)
	require.NoError(t, err)
	bypass, err := a.transport.Proxy(req)
	require.NoError(t, err)
	assert.Nil(t, bypass, "NO_PROXY must bypass the configured proxy")
	assert.Equal(t, 90*time.Second, a.transport.IdleConnTimeout)
	assert.Equal(t, 10*time.Second, a.transport.TLSHandshakeTimeout)
	require.NotNil(t, a.transport.DialContext)
}

func TestCloseClosesUnderlyingIdleConnections(t *testing.T) {
	m := newMockO365(t)
	var mu sync.Mutex
	connections := map[net.Conn]http.ConnState{}
	server := httptest.NewUnstartedServer(m.handler())
	server.Config.ConnState = func(c net.Conn, state http.ConnState) {
		mu.Lock()
		defer mu.Unlock()
		if state == http.StateClosed {
			delete(connections, c)
		} else {
			connections[c] = state
		}
	}
	server.Start()
	defer server.Close()
	m.mu.Lock()
	m.baseURL = server.URL
	m.mu.Unlock()
	conf := newTestConfig(t, m, "Audit.Exchange")
	conf.PollInterval = time.Hour
	_, _, closeIt := startAdapter(t, conf, &captureSink{})
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, state := range connections {
			if state != http.StateIdle {
				return false
			}
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		return len(connections) > 0 && m.listRequests > 0
	}, 5*time.Second, 5*time.Millisecond)
	closeIt()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(connections) == 0
	}, time.Second, 5*time.Millisecond, "Close must reach the transport underneath OAuth2 and close idle sockets")
}

// A synchronous poll fixture makes failure-clock and cleanup assertions without
// racing the collector goroutine or sleeping through production time limits.
func pollFixture(t *testing.T, handler http.Handler) (*Office365Adapter, string) {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	conf := validConfig(t)
	fastRetries(&conf)
	conf.contentRetryLimit = 5 * time.Millisecond
	conf.retryBaseDelay = time.Millisecond
	conf.retryMaxDelay = time.Millisecond
	conf.failureEscalation = time.Minute
	dd := newSharedDeduper()
	conf.Deduper = dd
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client := s.Client()
	t.Cleanup(client.CloseIdleConnections)
	return &Office365Adapter{conf: conf, checker: dd, httpClient: client,
		uspClient: &captureSink{}, doStop: utils.NewEvent(), stopCtx: ctx, cancelStop: cancel}, s.URL
}

func TestNonTransientLeftBlobDoesNotKeepOutageClock(t *testing.T) {
	for _, kind := range []string{"404", "AF20050", "AF20051", "AF20052", "invalid json"} {
		t.Run(kind, func(t *testing.T) {
			var listHits atomic.Int32
			a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/list" {
					if listHits.Add(1) == 2 {
						w.WriteHeader(http.StatusTooManyRequests)
						return
					}
					writeJSON(w, http.StatusOK, []listItem{{ContentID: "left-blob", ContentURI: "http://" + r.Host + "/blob"}})
					return
				}
				switch kind {
				case "404":
					w.WriteHeader(http.StatusNotFound)
				case "invalid json":
					_, _ = w.Write([]byte(`[{`))
				default:
					writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": map[string]string{"code": kind}})
				}
			}))
			cb := &callbacks{}
			cb.install(t, &a.conf)
			fs := &failureState{since: time.Now().Add(-time.Hour)}
			// This is the same reset decision fetchEvents makes after each poll.
			if a.pollOnce(base+"/list", fs) {
				fs.reset()
			}
			assert.True(t, fs.since.IsZero(), "non-transient leftovers must let a completed poll clear an old outage")
			assert.True(t, a.pollOnce(base+"/list", fs))
			assert.Empty(t, cb.errors(), "an intermittent 429 after a completed poll must not inherit an old outage")
			assert.Equal(t, int32(3), listHits.Load(), "the next poll's 429 must be retried")
		})
	}
}

func TestListingSuccessDoesNotClearContentOutageClock(t *testing.T) {
	a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/list" {
			writeJSON(w, http.StatusOK, []listItem{{ContentID: "failing", ContentURI: "http://" + r.Host + "/blob"}})
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	cb := &callbacks{}
	cb.install(t, &a.conf)
	fs := &failureState{since: time.Now().Add(-time.Hour)}
	assert.False(t, a.pollOnce(base+"/list", fs), "exhausted transient retries must preserve the outage clock")
	assert.Len(t, cb.errors(), 1, "successful listing must not hide an existing continuous content outage")
}

func TestPersistentBlobEscalatesWhileHealthyContentFlows(t *testing.T) {
	for _, kind := range []string{"transient", "invalid json"} {
		t.Run(kind, func(t *testing.T) {
			const ct = "Audit.Exchange"
			m := newFaultMock(t)
			var recoverBlob atomic.Bool
			m.setFault(func(key string, hit int, w http.ResponseWriter, r *http.Request) bool {
				switch key {
				case "list":
					writeJSON(w, http.StatusOK, []listItem{
						{ContentID: "poison", ContentURI: "http://" + r.Host + "/api/v1.0/" + testTenantID + "/activity/feed/audit/poison"},
						{ContentID: fmt.Sprintf("healthy-%d", hit), ContentURI: "http://" + r.Host + "/api/v1.0/" + testTenantID + "/activity/feed/audit/healthy"},
					})
					return true
				case "poison":
					if !recoverBlob.Load() {
						if kind == "transient" {
							w.WriteHeader(http.StatusServiceUnavailable)
						} else {
							_, _ = w.Write([]byte(`[{`))
						}
						return true
					}
					fallthrough
				case "healthy":
					_, _ = fmt.Fprintf(w, `[{"Id":%q}]`, fmt.Sprintf("%s-%d", key, hit))
					return true
				}
				return false
			})
			conf := newTestConfig(t, m.mockO365, ct)
			fastRetries(&conf)
			conf.contentRetryLimit = 20 * time.Millisecond
			conf.failureEscalation = 300 * time.Millisecond
			cb := &callbacks{}
			cb.install(t, &conf)
			sink := &captureSink{}
			_, _, closeIt := startAdapter(t, conf, sink)
			require.Eventually(t, func() bool { return len(cb.errors()) > 0 }, 3*time.Second, 5*time.Millisecond,
				"one repeatedly unprocessed blob must escalate even while other blobs ship")
			assert.GreaterOrEqual(t, cb.errors()[0].Sub(m.hitTimes("poison")[0]), conf.failureEscalation)
			assert.GreaterOrEqual(t, sink.count(), 3, "healthy content must keep flowing across polls")
			time.Sleep(650 * time.Millisecond)
			assert.LessOrEqual(t, len(cb.errors()), 3, "reports must be bounded across polls")
			recoverBlob.Store(true)
			require.Eventually(t, func() bool {
				for payload := range sink.payloadCounts() {
					if strings.Contains(payload, "poison-") {
						return true
					}
				}
				return false
			}, time.Second, 5*time.Millisecond, "the blob must ship after recovery")
			n := len(cb.errors())
			time.Sleep(400 * time.Millisecond)
			closeIt()
			assert.Len(t, cb.errors(), n, "recovered blobs must stop generating errors")
		})
	}
}

func TestLeftBlobTrackingCleanupAndCapacity(t *testing.T) {
	var healthy atomic.Bool
	a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if healthy.Load() {
			_, _ = w.Write([]byte(`[{"Id":"recovered-event"}]`))
		} else {
			_, _ = w.Write([]byte(`[{`))
		}
	}))
	cb := &callbacks{}
	cb.install(t, &a.conf)
	fs := &failureState{}
	item := listItem{ContentID: "recovery", ContentURI: base}
	a.processItems([]listItem{item}, fs)
	require.Contains(t, fs.leftBlobs, "recovery")
	// Healthy content on another page must preserve the original first-left time.
	first := fs.leftBlobs["recovery"].since
	fs.reset()
	a.processItems([]listItem{item}, fs)
	assert.Equal(t, first, fs.leftBlobs["recovery"].since)
	healthy.Store(true)
	a.processItems([]listItem{item}, fs)
	assert.Empty(t, fs.leftBlobs, "successful retry must remove its tracking entry")

	// Another instance may complete a tracked blob in the shared deduper.
	fs.leftBlobs["recovery"] = leftBlob{since: first, expires: time.Now().Add(time.Hour)}
	a.processItems([]listItem{item}, fs)
	assert.Empty(t, fs.leftBlobs, "already-completed blobs must also be removed")

	now := time.Now()
	a.leaveBlob(listItem{ContentID: "window", ContentCreated: now.Add(-2 * time.Hour).Format(time.RFC3339)}, fs, true, fmt.Errorf("invalid json"))
	a.leaveBlob(listItem{ContentID: "retention", ContentExpiration: now.Add(10 * time.Minute).Format(time.RFC3339)}, fs, true, fmt.Errorf("invalid json"))
	assert.WithinDuration(t, now.Add(time.Hour), fs.leftBlobs["window"].expires, time.Second)
	assert.WithinDuration(t, now.Add(10*time.Minute), fs.leftBlobs["retention"].expires, time.Second)
	a.expireLeftBlobs(fs, now.Add(2*time.Hour))
	assert.Empty(t, fs.leftBlobs, "expired entries must be removed even if they are no longer listed")
	assert.Len(t, cb.errors(), 1, "overdue entries must be reported before expiry, with one aggregate error")

	// Use real timestamps again for capacity and its reporting interval.
	fs = &failureState{}
	cb = &callbacks{}
	cb.install(t, &a.conf)
	a.conf.ClientOptions.OnWarning = func(string) {}
	for i := 0; i < maxLeftBlobs+10; i++ {
		a.leaveBlob(listItem{ContentID: fmt.Sprintf("left-%d", i)}, fs, true, fmt.Errorf("invalid json"))
	}
	assert.Len(t, fs.leftBlobs, maxLeftBlobs, "failure bookkeeping must have a hard memory bound")
	assert.Len(t, cb.errors(), 1, "capacity exhaustion must be visible without flooding")
	assert.Contains(t, fs.leftBlobs, "left-0", "new failures must not evict an older blob before escalation")
}

func TestUnlistedLeftBlobExpiresAndReports(t *testing.T) {
	for _, age := range []time.Duration{2 * time.Minute, 5 * time.Second} {
		t.Run(age.String(), func(t *testing.T) {
			a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeJSON(w, http.StatusOK, []listItem{})
			}))
			cb := &callbacks{}
			cb.install(t, &a.conf)
			now := time.Now()
			fs := &failureState{leftBlobs: map[string]leftBlob{
				"aged-out": {since: now.Add(-age), expires: now.Add(-time.Second)},
			}}
			assert.True(t, a.pollOnce(base, fs))
			assert.Empty(t, fs.leftBlobs, "a blob no longer listed must not remain in memory")
			assert.Len(t, cb.errors(), 1, "an unresolved blob must be reported before it is forgotten, even if first seen near expiry")
		})
	}
}

func TestLegacyDeduperReportsUnretryableBlob(t *testing.T) {
	for _, kind := range []string{"transient", "gone", "invalid json"} {
		t.Run(kind, func(t *testing.T) {
			var hits atomic.Int32
			a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				switch kind {
				case "transient":
					w.WriteHeader(http.StatusServiceUnavailable)
				case "gone":
					w.WriteHeader(http.StatusNotFound)
				default:
					_, _ = w.Write([]byte(`[{`))
				}
			}))
			a.conf.Deduper = newLegacyDeduper()
			a.checker = nil
			var messages []string
			a.conf.ClientOptions.OnError = func(err error) { messages = append(messages, err.Error()) }
			var warnings []string
			a.conf.ClientOptions.OnWarning = func(msg string) { warnings = append(warnings, msg) }
			fs := &failureState{}
			items := []listItem{{ContentID: "legacy-blob", ContentURI: base}}
			a.processItems(items, fs)
			require.Len(t, messages, 1)
			assert.Contains(t, messages[0], "cannot be retried on the next poll")
			for _, warning := range warnings {
				assert.NotContains(t, warning, "leaving blob for the next poll")
			}
			n := hits.Load()
			a.processItems(items, fs)
			assert.Equal(t, n, hits.Load(), "the legacy deduper already recorded the blob")
			assert.Len(t, messages, 1, "a skipped blob must not flood errors on later polls")
		})
	}
}
