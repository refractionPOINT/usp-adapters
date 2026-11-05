package usp_prisma_saas

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// logCollector records every log callback so tests can assert on them (and on
// the absence of secrets).
type logCollector struct {
	mu       sync.Mutex
	debug    []string
	warnings []string
	errs     []string
}

func (l *logCollector) add(dst *[]string, s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	*dst = append(*dst, s)
}

func (l *logCollector) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append(append(append([]string{}, l.debug...), l.warnings...), l.errs...)
}

func (l *logCollector) errors() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string{}, l.errs...)
}

func (l *logCollector) warns() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string{}, l.warnings...)
}

func testClientOptions(t *testing.T, logs *logCollector) uspclient.ClientOptions {
	t.Helper()
	return uspclient.ClientOptions{
		Identity: uspclient.Identity{
			Oid:             "11111111-1111-1111-1111-111111111111",
			InstallationKey: "test-installation-key",
		},
		Platform:     "prisma_saas",
		TestSinkMode: true,
		DebugLog:     func(msg string) { logs.add(&logs.debug, msg); t.Logf("DBG: %s", msg) },
		OnWarning:    func(msg string) { logs.add(&logs.warnings, msg); t.Logf("WRN: %s", msg) },
		OnError:      func(err error) { logs.add(&logs.errs, err.Error()); t.Logf("ERR: %v", err) },
	}
}

// testConfig returns a config pointed at the mock with fast timings.
func testConfig(t *testing.T, srv *httptest.Server, logs *logCollector) PrismaSaaSConfig {
	t.Helper()
	return PrismaSaaSConfig{
		ClientOptions:      testClientOptions(t, logs),
		ClientID:           mockClientID,
		ClientSecret:       mockClientSecret,
		BaseURL:            srv.URL,
		MaxConcurrentPolls: 2,
		allowInsecureHTTP:  true,
		emptyWait:          10 * time.Millisecond,
		retryBaseDelay:     5 * time.Millisecond,
		retryMaxDelay:      50 * time.Millisecond,
		shutdownGrace:      3 * time.Second,
	}
}

// startAdapter runs an adapter against the mock and closes it at test end.
func startAdapter(t *testing.T, conf PrismaSaaSConfig) (*PrismaSaaSAdapter, *captureSink, chan struct{}) {
	t.Helper()
	sink := &captureSink{}
	a, chStopped, err := newPrismaSaaSAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a, sink, chStopped
}

// shippedNames returns the sorted item_name of every shipped message.
func shippedNames(sink *captureSink) []string {
	var names []string
	for _, m := range sink.snapshot() {
		n, _ := m.JsonPayload["item_name"].(string)
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func expectedNames(events []utils.Dict) []string {
	var names []string
	for _, e := range events {
		names = append(names, e["item_name"].(string))
	}
	sort.Strings(names)
	return names
}

func isStopped(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// --- configuration ----------------------------------------------------------

func validConfig() PrismaSaaSConfig {
	return PrismaSaaSConfig{
		ClientOptions: uspclient.ClientOptions{
			Identity: uspclient.Identity{Oid: "oid", InstallationKey: "ik"},
		},
		ClientID:     "id",
		ClientSecret: "secret",
	}
}

func TestConfigDefaults(t *testing.T) {
	c := validConfig()
	require.NoError(t, c.Validate())
	assert.Equal(t, "prisma_saas", c.ClientOptions.Platform, "platform defaults to prisma_saas")
	assert.Equal(t, "us", c.Region)
	assert.Equal(t, 4, c.MaxConcurrentPolls)
	assert.Equal(t, "https://api.aperture.paloaltonetworks.com", c.resolveBaseURL())

	c = validConfig()
	c.ClientOptions.Platform = "json"
	require.NoError(t, c.Validate())
	assert.Equal(t, "json", c.ClientOptions.Platform, "an explicit platform is kept")
}

func TestConfigRegions(t *testing.T) {
	want := map[string]string{
		"us":   "https://api.aperture.paloaltonetworks.com",
		"apac": "https://api.aperture-apac.paloaltonetworks.com",
		"eu":   "https://api.aperture-eu.paloaltonetworks.com",
		"uk":   "https://api.aperture-uk.paloaltonetworks.com",
		"in":   "https://api.in1.prisma-saas.paloaltonetworks.com",
		"au":   "https://api.au1.prisma-saas.paloaltonetworks.com",
		"uk2":  "https://api.uk2.prisma-saas.paloaltonetworks.com",
		"jp":   "https://api.jp1.prisma-saas.paloaltonetworks.com",
	}
	for region, host := range want {
		c := validConfig()
		c.Region = " " + strings.ToUpper(region) + " "
		require.NoError(t, c.Validate(), region)
		assert.Equal(t, region, c.Region)
		assert.Equal(t, host, c.resolveBaseURL(), region)
	}
	assert.Len(t, regionHosts, len(want), "every region is covered here")
}

func TestConfigRejects(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*PrismaSaaSConfig)
		wantErr string
	}{
		{"missing client_id", func(c *PrismaSaaSConfig) { c.ClientID = " " }, "missing client_id"},
		{"missing client_secret", func(c *PrismaSaaSConfig) { c.ClientSecret = "" }, "missing client_secret"},
		{"unknown region", func(c *PrismaSaaSConfig) { c.Region = "mars" }, "unknown region"},
		{"http base_url", func(c *PrismaSaaSConfig) { c.BaseURL = "http://api.example.com" }, "must use https"},
		{"schemeless base_url", func(c *PrismaSaaSConfig) { c.BaseURL = "api.example.com" }, ""},
		{"base_url with credentials", func(c *PrismaSaaSConfig) { c.BaseURL = "https://u:p@api.example.com" }, "must not contain credentials"},
		{"negative polls", func(c *PrismaSaaSConfig) { c.MaxConcurrentPolls = -1 }, "max_concurrent_polls"},
		{"too many polls", func(c *PrismaSaaSConfig) { c.MaxConcurrentPolls = 31 }, "max_concurrent_polls"},
		{"missing oid", func(c *PrismaSaaSConfig) { c.ClientOptions.Identity.Oid = "" }, "client_options"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			tt.mutate(&c)
			err := c.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestConfigBaseURLOverridesRegion(t *testing.T) {
	c := validConfig()
	c.Region = "eu"
	c.BaseURL = "https://api.new-region.example.com/"
	require.NoError(t, c.Validate())
	assert.Equal(t, "https://api.new-region.example.com", c.resolveBaseURL())
}

func TestConfigAcceptsConcurrencyBounds(t *testing.T) {
	for _, n := range []int{1, 30} {
		c := validConfig()
		c.MaxConcurrentPolls = n
		require.NoError(t, c.Validate())
		assert.Equal(t, n, c.MaxConcurrentPolls)
	}
}

func TestNewAdapterRejectsBadConfig(t *testing.T) {
	c := validConfig()
	c.ClientID = ""
	_, _, err := newPrismaSaaSAdapter(context.Background(), c, &captureSink{})
	require.Error(t, err)
}

// --- shipping ---------------------------------------------------------------

func TestBulkShipsEveryEventOnceVerbatim(t *testing.T) {
	mock := newMockPAN(t)
	events := mixedEvents(250) // two full bulk batches plus a partial one
	mock.enqueue(events...)
	srv := mock.start()
	logs := &logCollector{}

	_, sink, _ := startAdapter(t, testConfig(t, srv, logs))
	require.Eventually(t, func() bool { return sink.count() == 250 }, 10*time.Second, 10*time.Millisecond)

	assert.Equal(t, expectedNames(events), shippedNames(sink), "each event shipped exactly once")
	_, bulk, single, _, popped := mock.stats()
	assert.Equal(t, 250, popped)
	assert.Zero(t, single, "the bulk endpoint is preferred")
	assert.Positive(t, bulk)
	mock.mu.Lock()
	for _, sz := range mock.bulkSizes {
		assert.Equal(t, "100", sz)
	}
	mock.mu.Unlock()

	// Verbatim payloads, and the right event type / time for every log type.
	byName := map[string]map[string]interface{}{}
	for _, e := range events {
		byName[e["item_name"].(string)] = roundTrip(t, e)
	}
	wantTime := map[string]time.Time{
		"activity_monitoring": time.Date(2017, 2, 17, 0, 18, 58, 961e6, time.UTC),
		"incident":            time.Date(2020, 5, 8, 23, 50, 55, 0, time.UTC),
		"remediation":         time.Date(2017, 2, 17, 0, 18, 55, 581e6, time.UTC),
		"policy_violation":    time.Date(2017, 1, 6, 19, 4, 6, 0, time.UTC),
		"admin_audit":         time.Date(2017, 4, 6, 21, 35, 10, 25e6, time.UTC),
	}
	seenTypes := map[string]bool{}
	for _, m := range sink.snapshot() {
		name := m.JsonPayload["item_name"].(string)
		assert.Equal(t, byName[name], m.JsonPayload, "payload is verbatim")
		logType := m.JsonPayload["log_type"].(string)
		assert.Equal(t, logType, m.EventType)
		assert.Equal(t, uint64(wantTime[logType].UnixMilli()), m.TimestampMs, logType)
		seenTypes[logType] = true
	}
	assert.Len(t, seenTypes, 5, "all five log types shipped")
}

func TestBigIntegersSurviveVerbatim(t *testing.T) {
	mock := newMockPAN(t)
	mock.enqueue(fixtureIncident(0))
	srv := mock.start()
	_, sink, _ := startAdapter(t, testConfig(t, srv, &logCollector{}))
	require.Eventually(t, func() bool { return sink.count() == 1 }, 5*time.Second, 10*time.Millisecond)
	got := sink.snapshot()[0].JsonPayload["item_size"]
	assert.Equal(t, "9007199254740993", fmt.Sprint(got), "a >2^53 integer is not rounded through float64")
}

func TestMissingOrBadTimestampAndTypeFallBack(t *testing.T) {
	mock := newMockPAN(t)
	mock.enqueue(
		utils.Dict{"log_type": "incident", "item_name": "no-ts"},
		utils.Dict{"log_type": "incident", "item_name": "bad-ts", "timestamp": "yesterday"},
		utils.Dict{"item_name": "no-type", "timestamp": "2020-05-08T23:50:55Z"},
	)
	srv := mock.start()
	before := time.Now().UnixMilli()
	_, sink, _ := startAdapter(t, testConfig(t, srv, &logCollector{}))
	require.Eventually(t, func() bool { return sink.count() == 3 }, 5*time.Second, 10*time.Millisecond)
	after := time.Now().UnixMilli()

	for _, m := range sink.snapshot() {
		switch m.JsonPayload["item_name"] {
		case "no-ts", "bad-ts":
			assert.GreaterOrEqual(t, int64(m.TimestampMs), before)
			assert.LessOrEqual(t, int64(m.TimestampMs), after)
			assert.Equal(t, "incident", m.EventType)
		case "no-type":
			assert.Equal(t, "unknown", m.EventType)
			assert.Equal(t, uint64(time.Date(2020, 5, 8, 23, 50, 55, 0, time.UTC).UnixMilli()), m.TimestampMs)
		}
	}
}

func TestEmptyQueueKeepsPollingAndShipsLateEventOnce(t *testing.T) {
	mock := newMockPAN(t)
	srv := mock.start()
	_, sink, _ := startAdapter(t, testConfig(t, srv, &logCollector{}))

	require.Eventually(t, func() bool { _, bulk, _, _, _ := mock.stats(); return bulk >= 3 }, 5*time.Second, 5*time.Millisecond,
		"an empty queue (204) is polled again")
	assert.Zero(t, sink.count())

	mock.enqueue(fixtureActivity(1))
	require.Eventually(t, func() bool { return sink.count() == 1 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, sink.count(), "shipped exactly once")
}

func TestBulkMissingFallsBackToSingleEventEndpoint(t *testing.T) {
	mock := newMockPAN(t)
	mock.bulkMissing = true
	mock.longPoll = 20 * time.Millisecond
	events := mixedEvents(60)
	mock.enqueue(events...)
	srv := mock.start()
	logs := &logCollector{}

	conf := testConfig(t, srv, logs)
	conf.MaxConcurrentPolls = 4
	_, sink, _ := startAdapter(t, conf)
	require.Eventually(t, func() bool { return sink.count() == 60 }, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, expectedNames(events), shippedNames(sink), "each event exactly once through the fallback")

	// The fallback is permanent: the bulk endpoint is not retried.
	_, bulk, single, _, _ := mock.stats()
	assert.LessOrEqual(t, bulk, 4, "at most one probe per reader")
	assert.GreaterOrEqual(t, single, 60)
	time.Sleep(150 * time.Millisecond)
	_, bulkLater, _, _, _ := mock.stats()
	assert.Equal(t, bulk, bulkLater, "no further bulk requests once it returned 404")
	assert.Len(t, logs.warns(), 1, "the fallback is announced once")
	assert.Contains(t, logs.warns()[0], "log_events_bulk")
}

func TestSingleEventWorkersRunConcurrently(t *testing.T) {
	mock := newMockPAN(t)
	mock.bulkMissing = true
	mock.longPoll = 200 * time.Millisecond // each empty call holds like a real long-poll
	srv := mock.start()

	conf := testConfig(t, srv, &logCollector{})
	conf.MaxConcurrentPolls = 4
	_, sink, _ := startAdapter(t, conf)

	require.Eventually(t, func() bool {
		mock.mu.Lock()
		defer mock.mu.Unlock()
		return mock.inflight == 4
	}, 5*time.Second, 5*time.Millisecond, "all four long-poll workers hold a request open at once")

	events := mixedEvents(40)
	mock.enqueue(events...)
	require.Eventually(t, func() bool { return sink.count() == 40 }, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, expectedNames(events), shippedNames(sink))
	mock.mu.Lock()
	assert.Equal(t, 4, mock.maxInflight, "never more than max_concurrent_polls in flight")
	mock.mu.Unlock()
}

func TestSingleEndpointRepollsImmediatelyAfterRealTimeout(t *testing.T) {
	mock := newMockPAN(t)
	mock.bulkMissing = true
	mock.longPoll = 1100 * time.Millisecond // a genuine long-poll timeout, then 204
	srv := mock.start()

	conf := testConfig(t, srv, &logCollector{})
	conf.MaxConcurrentPolls = 1
	conf.emptyWait = time.Minute // any wait after a real timeout would blow the deadline
	startAdapter(t, conf)

	require.Eventually(t, func() bool {
		_, _, single, _, _ := mock.stats()
		return single >= 3 // singles chain back to back after each 204
	}, 10*time.Second, 20*time.Millisecond, "a 204 after the long-poll window is followed at once by a new request")
}

func TestUnparseableSuccessBodyIsSurvived(t *testing.T) {
	mock := newMockPAN(t)
	mock.garbageNext = 1
	mock.enqueue(fixtureActivity(1)) // lost with the garbage response
	srv := mock.start()
	logs := &logCollector{}
	_, sink, chStopped := startAdapter(t, testConfig(t, srv, logs))

	require.Eventually(t, func() bool { return len(logs.warns()) > 0 }, 5*time.Second, 5*time.Millisecond)
	mock.enqueue(fixtureActivity(2))
	require.Eventually(t, func() bool { return sink.count() == 1 }, 5*time.Second, 5*time.Millisecond)
	assert.False(t, isStopped(chStopped), "a garbled body does not stop the adapter")
	assert.Equal(t, "My File 2", sink.snapshot()[0].JsonPayload["item_name"])
}

// --- authentication ---------------------------------------------------------

func TestTokenRequestIsCachedAndShared(t *testing.T) {
	mock := newMockPAN(t)
	mock.enqueue(mixedEvents(500)...)
	srv := mock.start()
	conf := testConfig(t, srv, &logCollector{})
	conf.MaxConcurrentPolls = 4
	_, sink, _ := startAdapter(t, conf)

	require.Eventually(t, func() bool { return sink.count() == 500 }, 10*time.Second, 10*time.Millisecond)
	tokens, _, _, unauth, _ := mock.stats()
	assert.Equal(t, 1, tokens, "one token serves every reader and request")
	assert.Zero(t, unauth)
}

func TestTokenIsRefreshedBeforeExpiry(t *testing.T) {
	mock := newMockPAN(t)
	mock.tokenLifetime = 1 // short-lived: refreshed at half its lifetime
	srv := mock.start()
	c := newAPIClient(srv.URL, mockClientID, mockClientSecret, 1)
	defer c.Close()

	first, err := c.accessToken(context.Background())
	require.NoError(t, err)
	again, err := c.accessToken(context.Background())
	require.NoError(t, err)
	assert.Equal(t, first, again, "cached while valid")

	time.Sleep(700 * time.Millisecond)
	second, err := c.accessToken(context.Background())
	require.NoError(t, err)
	assert.NotEqual(t, first, second, "re-requested before expires_in elapsed")
	tokens, _, _, _, _ := mock.stats()
	assert.Equal(t, 2, tokens)
}

func TestTokenWithoutExpiryIsKept(t *testing.T) {
	mock := newMockPAN(t)
	mock.tokenLifetime = 0 // expires_in omitted: the token does not expire
	srv := mock.start()
	c := newAPIClient(srv.URL, mockClientID, mockClientSecret, 1)
	defer c.Close()
	for i := 0; i < 3; i++ {
		_, err := c.accessToken(context.Background())
		require.NoError(t, err)
	}
	tokens, _, _, _, _ := mock.stats()
	assert.Equal(t, 1, tokens)
}

func TestExpiredTokenRefreshedOn401WithoutLosingEvents(t *testing.T) {
	mock := newMockPAN(t)
	events := mixedEvents(300)
	mock.enqueue(events[:100]...)
	srv := mock.start()
	conf := testConfig(t, srv, &logCollector{})
	conf.MaxConcurrentPolls = 3
	_, sink, chStopped := startAdapter(t, conf)

	require.Eventually(t, func() bool { return sink.count() == 100 }, 10*time.Second, 10*time.Millisecond)
	mock.revokeTokens() // PAN expires the token early: the next events request is a 401
	mock.enqueue(events[100:]...)
	require.Eventually(t, func() bool { return sink.count() == 300 }, 10*time.Second, 10*time.Millisecond)

	assert.Equal(t, expectedNames(events), shippedNames(sink), "no event lost or duplicated across the refresh")
	tokens, _, _, unauth, _ := mock.stats()
	assert.Equal(t, 2, tokens, "exactly one refresh even with several readers seeing the 401")
	assert.Positive(t, unauth)
	assert.False(t, isStopped(chStopped))
}

func TestPersistent401StopsTheAdapter(t *testing.T) {
	mock := newMockPAN(t)
	mock.enqueue(fixtureActivity(1))
	mock.alwaysStatus = http.StatusUnauthorized // even a fresh token is refused
	srv := mock.start()
	logs := &logCollector{}
	_, sink, chStopped := startAdapter(t, testConfig(t, srv, logs))

	select {
	case <-chStopped:
	case <-time.After(10 * time.Second):
		t.Fatal("adapter did not stop on a persistent 401")
	}
	assert.Zero(t, sink.count())
	require.NotEmpty(t, logs.errors())
	assert.Contains(t, logs.errors()[0], "events request rejected")
	tokens, _, _, _, popped := mock.stats()
	assert.LessOrEqual(t, tokens, 4, "bounded refresh attempts, no loop")
	assert.Zero(t, popped)
}

func TestForbiddenStopsTheAdapter(t *testing.T) {
	mock := newMockPAN(t)
	mock.enqueue(fixtureActivity(1))
	mock.alwaysStatus = http.StatusForbidden
	srv := mock.start()
	logs := &logCollector{}
	_, sink, chStopped := startAdapter(t, testConfig(t, srv, logs))

	select {
	case <-chStopped:
	case <-time.After(10 * time.Second):
		t.Fatal("adapter did not stop on a 403")
	}
	assert.Zero(t, sink.count())
	require.NotEmpty(t, logs.errors())
	assert.Contains(t, logs.errors()[0], "Log File Access")
}

func TestTokenEndpointRejectionStopsTheAdapter(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			mock := newMockPAN(t)
			mock.tokenStatus = status
			srv := mock.start()
			logs := &logCollector{}
			_, sink, chStopped := startAdapter(t, testConfig(t, srv, logs))

			select {
			case <-chStopped:
			case <-time.After(10 * time.Second):
				t.Fatalf("adapter did not stop on token endpoint %d", status)
			}
			assert.Zero(t, sink.count())
			require.NotEmpty(t, logs.errors())
			assert.Contains(t, logs.errors()[0], "token request rejected")
			_, bulk, single, _, _ := mock.stats()
			assert.Zero(t, bulk+single, "no event request without a token")
		})
	}
}

func TestWrongSecretStopsTheAdapter(t *testing.T) {
	mock := newMockPAN(t)
	srv := mock.start()
	logs := &logCollector{}
	conf := testConfig(t, srv, logs)
	conf.ClientSecret = "not-the-secret"
	_, sink, chStopped := startAdapter(t, conf)

	select {
	case <-chStopped:
	case <-time.After(10 * time.Second):
		t.Fatal("adapter did not stop on bad credentials")
	}
	assert.Zero(t, sink.count())
	for _, line := range logs.all() {
		assert.NotContains(t, line, "not-the-secret")
	}
}

func TestSecretsAndTokensAreNeverLogged(t *testing.T) {
	mock := newMockPAN(t)
	mock.enqueue(mixedEvents(20)...)
	mock.setFaults(fault{status: http.StatusServiceUnavailable}, fault{status: http.StatusTooManyRequests})
	srv := mock.start()
	logs := &logCollector{}
	_, sink, _ := startAdapter(t, testConfig(t, srv, logs))
	require.Eventually(t, func() bool { return sink.count() == 20 }, 10*time.Second, 10*time.Millisecond)
	mock.revokeTokens()
	mock.enqueue(fixtureActivity(99))
	require.Eventually(t, func() bool { return sink.count() == 21 }, 10*time.Second, 10*time.Millisecond)

	lines := logs.all()
	require.NotEmpty(t, lines)
	for _, line := range lines {
		assert.NotContains(t, line, mockClientSecret)
		assert.NotContains(t, line, "tok-")
		assert.NotContains(t, line, "eyJhbGci")
	}
}

// --- error handling ---------------------------------------------------------

func TestRetryAfterIsHonored(t *testing.T) {
	mock := newMockPAN(t)
	mock.enqueue(fixtureActivity(1))
	mock.setFaults(fault{status: http.StatusTooManyRequests, retryAfter: "1"})
	srv := mock.start()
	conf := testConfig(t, srv, &logCollector{})
	conf.MaxConcurrentPolls = 1
	_, sink, _ := startAdapter(t, conf)

	require.Eventually(t, func() bool { return sink.count() == 1 }, 10*time.Second, 10*time.Millisecond)
	mock.mu.Lock()
	times := append([]time.Time{}, mock.requestTimes...)
	mock.mu.Unlock()
	require.GreaterOrEqual(t, len(times), 2)
	assert.GreaterOrEqual(t, times[1].Sub(times[0]), 950*time.Millisecond, "waited the server-requested second, not the 5ms backoff")
}

func TestTransientErrorsBackOffAndRecover(t *testing.T) {
	mock := newMockPAN(t)
	events := mixedEvents(10)
	mock.enqueue(events...)
	mock.setFaults(
		fault{status: http.StatusInternalServerError},
		fault{status: http.StatusBadGateway},
		fault{status: http.StatusServiceUnavailable},
		fault{status: http.StatusTooManyRequests},
	)
	srv := mock.start()
	logs := &logCollector{}
	conf := testConfig(t, srv, logs)
	conf.MaxConcurrentPolls = 1
	_, sink, chStopped := startAdapter(t, conf)

	require.Eventually(t, func() bool { return sink.count() == 10 }, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, expectedNames(events), shippedNames(sink))
	assert.False(t, isStopped(chStopped), "5xx and 429 never stop the adapter")
	assert.Len(t, logs.warns(), 4)
	assert.Empty(t, logs.errors())
}

func TestNetworkErrorsAreRetried(t *testing.T) {
	mock := newMockPAN(t)
	events := mixedEvents(5)
	mock.enqueue(events...)

	// A server the test owns that drops the connection without answering for
	// its first requests (token and event requests alike), then serves.
	const dropped = 4
	var seen atomic.Int32
	inner := mock.handler()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if seen.Add(1) <= dropped {
			conn, _, err := w.(http.Hijacker).Hijack()
			if assert.NoError(t, err) {
				_ = conn.Close()
			}
			return
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	logs := &logCollector{}
	conf := testConfig(t, srv, logs)
	conf.MaxConcurrentPolls = 1
	_, sink, chStopped := startAdapter(t, conf)

	require.Eventually(t, func() bool { return sink.count() == 5 }, 10*time.Second, 5*time.Millisecond)
	assert.Equal(t, expectedNames(events), shippedNames(sink))
	assert.GreaterOrEqual(t, int(seen.Load()), dropped+2, "the dropped requests were retried")
	assert.False(t, isStopped(chStopped), "connection failures are transient")
	assert.NotEmpty(t, logs.warns())
	assert.Empty(t, logs.errors())
}

func TestUnexpectedClientErrorStopsTheAdapter(t *testing.T) {
	mock := newMockPAN(t)
	mock.alwaysStatus = http.StatusBadRequest
	srv := mock.start()
	logs := &logCollector{}
	_, _, chStopped := startAdapter(t, testConfig(t, srv, logs))
	select {
	case <-chStopped:
	case <-time.After(10 * time.Second):
		t.Fatal("adapter did not stop on a permanent 4xx")
	}
	require.NotEmpty(t, logs.errors())
	assert.Contains(t, logs.errors()[0], "400")
}

func TestSingleEndpointMissingStopsTheAdapter(t *testing.T) {
	mock := newMockPAN(t)
	mock.bulkMissing = true
	mock.alwaysStatus = http.StatusNotFound // the single endpoint is gone too: wrong host
	srv := mock.start()
	logs := &logCollector{}
	_, _, chStopped := startAdapter(t, testConfig(t, srv, logs))
	select {
	case <-chStopped:
	case <-time.After(10 * time.Second):
		t.Fatal("adapter did not stop when no endpoint exists")
	}
	require.NotEmpty(t, logs.errors())
	assert.Contains(t, logs.errors()[0], "404")
}

// --- backpressure and shutdown ----------------------------------------------

func TestBufferFullNeverDropsAnEvent(t *testing.T) {
	mock := newMockPAN(t)
	events := mixedEvents(120)
	mock.enqueue(events...)
	srv := mock.start()
	logs := &logCollector{}

	sink := &captureSink{fullFor: 37}
	conf := testConfig(t, srv, logs)
	conf.MaxConcurrentPolls = 1
	a, _, err := newPrismaSaaSAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer a.Close()

	require.Eventually(t, func() bool { return sink.count() == 120 }, 10*time.Second, 5*time.Millisecond)
	assert.Equal(t, expectedNames(events), shippedNames(sink), "every event shipped exactly once despite ErrorBufferFull")
	assert.Equal(t, 120+37, sink.shipCalls(), "each rejected Ship was retried with the same event")
	assert.Contains(t, strings.Join(logs.warns(), "\n"), "falling behind")
}

func TestReadsPauseWhileTheBufferIsFull(t *testing.T) {
	mock := newMockPAN(t)
	events := mixedEvents(1000)
	mock.enqueue(events...)
	srv := mock.start()

	sink := &captureSink{}
	sink.blocked.Store(true)
	conf := testConfig(t, srv, &logCollector{})
	conf.MaxConcurrentPolls = 2
	a, _, err := newPrismaSaaSAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer a.Close()

	require.Eventually(t, func() bool { return mock.poppedCount() > 0 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	assert.LessOrEqual(t, mock.poppedCount(), 2*bulkSize,
		"with the buffer full no reader goes back to PAN: at most one batch per reader is off the queue")

	sink.blocked.Store(false)
	require.Eventually(t, func() bool { return sink.count() == 1000 }, 15*time.Second, 10*time.Millisecond)
	assert.Equal(t, expectedNames(events), shippedNames(sink))
}

func TestCloseShipsEverythingAlreadyRead(t *testing.T) {
	mock := newMockPAN(t)
	mock.enqueue(mixedEvents(3000)...)
	// Each events request waits for a token, so the queue cannot drain before
	// Close runs: four requests are released, the rest stay in flight.
	mock.gate = make(chan struct{}, 4)
	srv := mock.start()
	conf := testConfig(t, srv, &logCollector{})
	conf.MaxConcurrentPolls = 4
	sink := &captureSink{}
	a, chStopped, err := newPrismaSaaSAdapter(context.Background(), conf, sink)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		mock.gate <- struct{}{}
	}
	require.Eventually(t, func() bool { return sink.count() == 400 }, 10*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return mock.inflightNow() == 4 }, 5*time.Second, time.Millisecond,
		"all four readers are waiting on their next request")

	closed := make(chan error, 1)
	go func() { closed <- a.Close() }()
	require.Eventually(t, a.doStop.IsSet, 5*time.Second, time.Millisecond)
	close(mock.gate) // the in-flight requests now return their batches

	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
	require.NoError(t, a.Close(), "Close is idempotent")

	assert.Equal(t, 800, mock.poppedCount(), "the four in-flight reads completed")
	assert.Equal(t, mock.poppedCount(), sink.count(), "every event read from PAN was shipped before Close returned")
	assert.True(t, sink.drained.Load())
	assert.True(t, sink.closed.Load())
	select {
	case <-chStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("stopped channel not closed after Close")
	}
	names := shippedNames(sink)
	for i := 1; i < len(names); i++ {
		assert.NotEqual(t, names[i-1], names[i], "no duplicate")
	}
}

// A read that completes after Close began, and after the point where the USP
// client would have stopped accepting messages had the seal been armed at the
// start of Close, must still be shipped.
func TestCloseShipsAReadThatReturnsLate(t *testing.T) {
	mock := newMockPAN(t)
	mock.bulkMissing = true
	mock.longPoll = 5 * time.Second
	srv := mock.start()
	conf := testConfig(t, srv, &logCollector{})
	conf.MaxConcurrentPolls = 1
	conf.producerStopGrace = 100 * time.Millisecond
	conf.shutdownGrace = 10 * time.Second
	sink := &captureSink{}
	a, _, err := newPrismaSaaSAdapter(context.Background(), conf, sink)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return mock.singleCount() >= 1 && mock.inflightNow() == 1 },
		5*time.Second, time.Millisecond, "the long-poll itself is open")

	closed := make(chan error, 1)
	go func() { closed <- a.Close() }()
	require.Eventually(t, a.doStop.IsSet, 5*time.Second, time.Millisecond)

	// The server holds the request well past producerStopGrace and answers
	// late, as a real long-poll can. Waiting here models that slow server.
	time.Sleep(4 * conf.producerStopGrace)
	require.Zero(t, sink.count())
	require.False(t, sink.drainBegan.Load(), "the USP client is not sealed while a read is in flight")
	mock.enqueue(fixtureActivity(7))

	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
	require.Equal(t, 1, sink.count(), "the late event was shipped, not dropped")
	assert.Equal(t, 1, mock.poppedCount())
}

// Readers stuck in Ship (a full buffer) are still released: once no read is in
// flight the USP client is sealed after producerStopGrace.
func TestCloseReleasesReadersStuckInShip(t *testing.T) {
	mock := newMockPAN(t)
	mock.enqueue(mixedEvents(300)...)
	srv := mock.start()
	conf := testConfig(t, srv, &logCollector{})
	conf.MaxConcurrentPolls = 2
	conf.producerStopGrace = 150 * time.Millisecond
	sink := &captureSink{}
	sink.blocked.Store(true)
	a, chStopped, err := newPrismaSaaSAdapter(context.Background(), conf, sink)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return sink.shipCalls() > 0 }, 5*time.Second, time.Millisecond)
	start := time.Now()
	require.NoError(t, a.Close())
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.True(t, isStopped(chStopped))
	assert.Zero(t, sink.count())
}

func TestCloseDoesNotHangOnAStuckRequest(t *testing.T) {
	mock := newMockPAN(t)
	mock.holdForever = true
	srv := mock.start()
	conf := testConfig(t, srv, &logCollector{})
	conf.MaxConcurrentPolls = 2
	conf.shutdownGrace = 200 * time.Millisecond
	sink := &captureSink{}
	a, chStopped, err := newPrismaSaaSAdapter(context.Background(), conf, sink)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return mock.inflightNow() == 2 }, 5*time.Second, 2*time.Millisecond)

	start := time.Now()
	require.NoError(t, a.Close())
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.True(t, isStopped(chStopped))
}

func TestParentContextCancellationStopsTheAdapter(t *testing.T) {
	mock := newMockPAN(t)
	srv := mock.start()
	conf := testConfig(t, srv, &logCollector{})
	ctx, cancel := context.WithCancel(context.Background())
	a, chStopped, err := newPrismaSaaSAdapter(ctx, conf, &captureSink{})
	require.NoError(t, err)
	defer a.Close()

	cancel()
	select {
	case <-chStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("readers did not exit after the parent context was cancelled")
	}
	require.NoError(t, a.Close())
}

func TestShipFailureStopsTheAdapter(t *testing.T) {
	mock := newMockPAN(t)
	mock.enqueue(mixedEvents(10)...)
	srv := mock.start()
	logs := &logCollector{}
	conf := testConfig(t, srv, logs)
	sink := &captureSink{}
	sink.failErr.Store(errors.New("boom"))
	a, chStopped, err := newPrismaSaaSAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer a.Close()

	select {
	case <-chStopped:
	case <-time.After(10 * time.Second):
		t.Fatal("adapter did not stop on a fatal Ship error")
	}
	require.NotEmpty(t, logs.errors())
	assert.Contains(t, logs.errors()[0], "Ship()")
}

// --- response parsing -------------------------------------------------------

func TestParseEvents(t *testing.T) {
	t.Run("bulk envelope", func(t *testing.T) {
		b, err := parseEvents([]byte(`{"events":[{"log_type":"incident","n":1},{"log_type":"admin_audit","n":2}]}`), true)
		require.NoError(t, err)
		require.Len(t, b.events, 2)
		assert.Equal(t, "admin_audit", b.events[1]["log_type"])
	})
	t.Run("bulk bare array", func(t *testing.T) {
		b, err := parseEvents([]byte(`[{"log_type":"incident"}]`), true)
		require.NoError(t, err)
		assert.Len(t, b.events, 1)
	})
	t.Run("bulk empty envelope", func(t *testing.T) {
		b, err := parseEvents([]byte(`{"events":[]}`), true)
		require.NoError(t, err)
		assert.Empty(t, b.events)
	})
	t.Run("single event object", func(t *testing.T) {
		b, err := parseEvents([]byte(`{"log_type":"activity_monitoring","target_name":null,"events":["kept as a field"]}`), false)
		require.NoError(t, err)
		require.Len(t, b.events, 1, "a single-event body is one event, never unwrapped")
		assert.Contains(t, b.events[0], "events")
	})
	t.Run("blank body", func(t *testing.T) {
		b, err := parseEvents([]byte("  \n"), false)
		require.NoError(t, err)
		assert.Empty(t, b.events)
	})
	t.Run("non-object elements are counted", func(t *testing.T) {
		b, err := parseEvents([]byte(`{"events":[{"a":1},7,null,{}, "x"]}`), true)
		require.NoError(t, err)
		assert.Len(t, b.events, 1)
		assert.Equal(t, 4, b.skipped)
	})
	t.Run("garbage", func(t *testing.T) {
		_, err := parseEvents([]byte(`<html>`), true)
		assert.Error(t, err)
		_, err = parseEvents([]byte(`{"events":`), true)
		assert.Error(t, err)
	})
}

func TestParseRetryAfter(t *testing.T) {
	assert.Equal(t, 3*time.Second, parseRetryAfter("3"))
	assert.Equal(t, 1500*time.Millisecond, parseRetryAfter("1.5"))
	assert.Zero(t, parseRetryAfter(""))
	assert.Zero(t, parseRetryAfter("-4"))
	assert.Zero(t, parseRetryAfter("soon"))
	d := parseRetryAfter(time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat))
	assert.Greater(t, d, 25*time.Second)
	assert.LessOrEqual(t, d, 30*time.Second)
	assert.Zero(t, parseRetryAfter(time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat)))
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	a := &PrismaSaaSAdapter{conf: PrismaSaaSConfig{retryBaseDelay: time.Second, retryMaxDelay: 30 * time.Second}}
	prev := time.Duration(0)
	for n := 0; n < 5; n++ {
		d := a.backoff(n, 0)
		assert.GreaterOrEqual(t, d, time.Second<<uint(n))
		assert.Less(t, d, (time.Second<<uint(n))*11/10+time.Millisecond)
		assert.Greater(t, d, prev)
		prev = d
	}
	assert.LessOrEqual(t, a.backoff(50, 0), 33*time.Second, "capped (plus jitter)")
	assert.GreaterOrEqual(t, a.backoff(0, 20*time.Second), 20*time.Second, "Retry-After wins over a shorter backoff")
	assert.Equal(t, maxRetryAfter, a.backoff(0, time.Hour), "an absurd Retry-After is capped")
}

func TestIsTransientError(t *testing.T) {
	assert.True(t, isTransientError(&HTTPError{StatusCode: 500}))
	assert.True(t, isTransientError(&HTTPError{StatusCode: 503}))
	assert.True(t, isTransientError(&HTTPError{StatusCode: 429}))
	assert.True(t, isTransientError(fmt.Errorf("failed to execute request %q: dial tcp: refused", "/x")))
	assert.True(t, isTransientError(&parseError{URL: "/x", Err: errors.New("bad")}))
	assert.False(t, isTransientError(&HTTPError{StatusCode: 404}))
	assert.False(t, isTransientError(&HTTPError{StatusCode: 400}))
	assert.False(t, isTransientError(&AuthError{Err: &HTTPError{StatusCode: 500}}), "auth errors are never retried")
	assert.False(t, isTransientError(context.Canceled))
	assert.False(t, isTransientError(nil))
}
