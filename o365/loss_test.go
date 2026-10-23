package usp_o365

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests check that a content blob is never lost: whatever fails or
// stops mid-way (a vendor error, a Ship error, Close), every event is shipped
// once the API answers again, by the same instance or by the next one sharing
// its deduper (as a hosted deployment does), and that transient failures are
// retried with backoff before they are reported as errors.

// --- shared dedupers -------------------------------------------------------------

// legacyDeduper is a Deduper shared across adapter instances (Close is a
// no-op) that can only check-and-record, like a key-value store queried with
// SETNX.
type legacyDeduper struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func newLegacyDeduper() *legacyDeduper { return &legacyDeduper{seen: map[string]struct{}{}} }

func (d *legacyDeduper) CheckAndAdd(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[key]; ok {
		return true
	}
	d.seen[key] = struct{}{}
	return false
}

func (d *legacyDeduper) Close() {}

// sharedDeduper is a shared Deduper that can also check a key without
// recording it (utils.CheckingDeduper).
type sharedDeduper struct {
	*legacyDeduper
}

func newSharedDeduper() *sharedDeduper { return &sharedDeduper{newLegacyDeduper()} }

func (d *sharedDeduper) Check(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[key]
	return ok
}

// --- fault injection -------------------------------------------------------------

// faultMock serves the mock Management API, and lets a test answer any
// request itself first: fault gets the request's key ("token", "start",
// "list" or the blob's content ID) and how many times that key was requested,
// and returns true when it wrote the response.
type faultMock struct {
	*mockO365

	fmu   sync.Mutex
	hits  map[string]int
	times map[string][]time.Time
	fault func(key string, hit int, w http.ResponseWriter, r *http.Request) bool
}

func newFaultMock(t *testing.T) *faultMock {
	t.Helper()
	m := &mockO365{
		t:                 t,
		clientID:          testClientID,
		clientSecret:      testClientSecret,
		blobs:             map[string][]contentBlob{},
		subscriptionStart: map[string]int{},
	}
	fm := &faultMock{mockO365: m, hits: map[string]int{}, times: map[string][]time.Time{}}
	inner := m.handler()
	apiRoot := "/api/v1.0/" + testTenantID + "/activity/feed/"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var key string
		switch {
		case r.URL.Path == "/oauth2/token":
			key = "token"
		case r.URL.Path == apiRoot+"subscriptions/start":
			key = "start"
		case r.URL.Path == apiRoot+"subscriptions/content":
			key = "list"
		default:
			key = strings.TrimPrefix(r.URL.Path, apiRoot+"audit/")
		}
		fm.fmu.Lock()
		fm.hits[key]++
		n := fm.hits[key]
		fm.times[key] = append(fm.times[key], time.Now())
		f := fm.fault
		fm.fmu.Unlock()
		if f != nil && f(key, n, w, r) {
			return
		}
		inner.ServeHTTP(w, r)
	}))
	m.mu.Lock()
	m.baseURL = srv.URL
	m.mu.Unlock()
	t.Cleanup(srv.Close)
	return fm
}

func (fm *faultMock) setFault(f func(key string, hit int, w http.ResponseWriter, r *http.Request) bool) {
	fm.fmu.Lock()
	defer fm.fmu.Unlock()
	fm.fault = f
}

func (fm *faultMock) hit(key string) int {
	fm.fmu.Lock()
	defer fm.fmu.Unlock()
	return fm.hits[key]
}

func (fm *faultMock) hitTimes(key string) []time.Time {
	fm.fmu.Lock()
	defer fm.fmu.Unlock()
	return append([]time.Time(nil), fm.times[key]...)
}

// addBlobs adds n blobs of one record each, "blob-1" to "blob-n", and returns
// each blob's record.
func addBlobs(t *testing.T, m *faultMock, ct string, n int) map[string]string {
	t.Helper()
	recs := map[string]string{}
	created := time.Now().UTC().Add(-10 * time.Minute)
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("blob-%d", i)
		rec := auditRecord(t, fmt.Sprintf("cccccccc-0000-0000-0000-%012d", i), "Send", "Exchange", "user@example.com", created)
		m.addBlob(ct, contentBlob{id: id, created: created, records: []string{rec}})
		recs[id] = rec
	}
	return recs
}

// --- callbacks and sinks ---------------------------------------------------------

// callbacks records OnError and OnWarning calls.
type callbacks struct {
	mu    sync.Mutex
	errs  []time.Time
	warns int
}

func (c *callbacks) install(t *testing.T, conf *Office365Config) {
	conf.ClientOptions.OnError = func(err error) {
		t.Logf("ERR: %.200s", err.Error())
		c.mu.Lock()
		defer c.mu.Unlock()
		c.errs = append(c.errs, time.Now())
	}
	conf.ClientOptions.OnWarning = func(msg string) {
		t.Logf("WRN: %.200s", msg)
		c.mu.Lock()
		defer c.mu.Unlock()
		c.warns++
	}
}

func (c *callbacks) errors() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.errs...)
}

func (c *callbacks) warnings() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.warns
}

// failingSink fails the failOn-th Ship call, once.
type failingSink struct {
	captureSink
	fmu    sync.Mutex
	calls  int
	failOn int
}

func (s *failingSink) Ship(m *protocol.DataMessage, d time.Duration) error {
	s.fmu.Lock()
	s.calls++
	fail := s.calls == s.failOn
	s.fmu.Unlock()
	if fail {
		return errors.New("connection to LimaCharlie lost")
	}
	return s.captureSink.Ship(m, d)
}

// fastRetries shortens the retry timing so tests run in milliseconds.
func fastRetries(conf *Office365Config) {
	conf.retryBaseDelay = 20 * time.Millisecond
	conf.retryMaxDelay = 50 * time.Millisecond
	conf.failureEscalation = time.Hour
}

// startAdapter starts an adapter and closes it when the test ends, unless the
// test closed it already.
func startAdapter(t *testing.T, conf Office365Config, sink uspSink) (*Office365Adapter, chan struct{}, func()) {
	t.Helper()
	a, chStopped, err := newOffice365Adapter(context.Background(), conf, sink)
	require.NoError(t, err)
	var once sync.Once
	closeIt := func() { once.Do(func() { a.Close() }) }
	t.Cleanup(closeIt)
	return a, chStopped, closeIt
}

// lostRecords returns the records shipped by none of the sinks, and fails the
// test if any record shipped more than once.
func lostRecords(t *testing.T, recs map[string]string, sinks ...*captureSink) []string {
	t.Helper()
	var lost []string
	for id, rec := range recs {
		n := 0
		for _, s := range sinks {
			n += s.payloadCounts()[rec]
		}
		if n == 0 {
			lost = append(lost, id)
		}
		assert.LessOrEqual(t, n, 1, "%s shipped %d times", id, n)
	}
	return lost
}

// --- tests -----------------------------------------------------------------------

// A transient 503 on a blob is retried after a backoff, and the blob ships.
func TestTransientContentErrorIsRetried(t *testing.T) {
	const ct = "Audit.Exchange"
	m := newFaultMock(t)
	recs := addBlobs(t, m, ct, 1)
	m.setFault(func(key string, hit int, w http.ResponseWriter, _ *http.Request) bool {
		if key == "blob-1" && hit == 1 {
			writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
				"error": map[string]string{"code": "AF50000", "message": "An internal error occurred. Retry the request."},
			})
			return true
		}
		return false
	})

	cb := &callbacks{}
	sink := &captureSink{}
	conf := newTestConfig(t, m.mockO365, ct)
	conf.PollInterval = time.Hour // one poll: the retry must happen within it
	fastRetries(&conf)
	cb.install(t, &conf)
	startAdapter(t, conf, sink)

	require.Eventually(t, func() bool { return sink.payloadCounts()[recs["blob-1"]] == 1 },
		5*time.Second, 10*time.Millisecond, "the blob must ship after the 503")
	assert.Equal(t, 2, m.hit("blob-1"))
	assert.Empty(t, cb.errors(), "a single 503 must not be reported as an error")
	assert.GreaterOrEqual(t, cb.warnings(), 1, "the retry must be reported as a warning")
}

// Close while a page is being downloaded: nothing the instance didn't ship is
// recorded as done, so the next instance sharing the deduper ships all of it,
// and nothing twice.
func TestCloseMidPageLosesNothing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		deduper func() interface{}
		maxLost int
	}{
		// A deduper that can check without recording: nothing is lost.
		{name: "checking deduper", deduper: func() interface{} { return newSharedDeduper() }},
		// A check-and-record-only deduper: only the blob being downloaded
		// when Close lands can be lost; the rest of the page isn't touched.
		{name: "legacy deduper", deduper: func() interface{} { return newLegacyDeduper() }, maxLost: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const ct = "Audit.Exchange"
			m := newFaultMock(t)
			recs := addBlobs(t, m, ct, 5)
			blob2Started := make(chan struct{})
			m.setFault(func(key string, hit int, w http.ResponseWriter, r *http.Request) bool {
				if key != "blob-2" || hit != 1 {
					return false
				}
				// A slow download, still running when the instance stops.
				close(blob2Started)
				<-r.Context().Done()
				return true
			})

			dd := tc.deduper()
			sink1 := &captureSink{}
			conf1 := newTestConfig(t, m.mockO365, ct)
			conf1.Deduper = dd.(interface {
				CheckAndAdd(string) bool
				Close()
			})
			conf1.PollInterval = time.Hour
			fastRetries(&conf1)
			_, _, close1 := startAdapter(t, conf1, sink1)

			select {
			case <-blob2Started:
			case <-time.After(5 * time.Second):
				t.Fatal("blob-2 was never requested")
			}
			closed := make(chan struct{})
			go func() { close1(); close(closed) }()
			select {
			case <-closed:
			case <-time.After(10 * time.Second):
				t.Fatal("Close did not interrupt the in-flight download")
			}
			for _, id := range []string{"blob-3", "blob-4", "blob-5"} {
				assert.Equal(t, 0, m.hit(id), "%s must not be touched after Close", id)
			}

			sink2 := &captureSink{}
			conf2 := newTestConfig(t, m.mockO365, ct)
			conf2.Deduper = conf1.Deduper
			conf2.PollInterval = time.Hour
			fastRetries(&conf2)
			startAdapter(t, conf2, sink2)

			require.Eventually(t, func() bool {
				return sink2.payloadCounts()[recs["blob-5"]] == 1
			}, 5*time.Second, 10*time.Millisecond, "the next instance must ship the rest of the page")
			lost := lostRecords(t, recs, sink1, sink2)
			assert.LessOrEqual(t, len(lost), tc.maxLost, "lost blobs: %v", lost)
			for _, id := range lost {
				assert.Equal(t, "blob-2", id, "only the in-flight blob may be lost")
			}
		})
	}
}

// blockingSink signals each Ship and waits for release before accepting it.
type blockingSink struct {
	captureSink
	shipped chan struct{}
	release chan struct{}
}

func (s *blockingSink) Ship(m *protocol.DataMessage, d time.Duration) error {
	s.shipped <- struct{}{}
	<-s.release
	return s.captureSink.Ship(m, d)
}

// A stop that lands between two blobs doesn't touch the next blob, even with
// a deduper that records a key when it checks it.
func TestStopBetweenBlobsMarksNothing(t *testing.T) {
	const ct = "Audit.Exchange"
	m := newFaultMock(t)
	recs := addBlobs(t, m, ct, 3)

	dd := newLegacyDeduper()
	sink1 := &blockingSink{shipped: make(chan struct{}, 1), release: make(chan struct{})}
	conf1 := newTestConfig(t, m.mockO365, ct)
	conf1.Deduper = dd
	conf1.PollInterval = time.Hour
	fastRetries(&conf1)
	a1, _, close1 := startAdapter(t, conf1, sink1)

	select {
	case <-sink1.shipped:
	case <-time.After(5 * time.Second):
		t.Fatal("blob-1 was never shipped")
	}
	// Stop the instance while blob-1's last event is being shipped.
	closed := make(chan struct{})
	go func() { close1(); close(closed) }()
	require.Eventually(t, a1.doStop.IsSet, 5*time.Second, 5*time.Millisecond)
	close(sink1.release)
	<-closed
	assert.Equal(t, 0, m.hit("blob-2"), "blob-2 must not be requested after the stop")

	sink2 := &captureSink{}
	conf2 := newTestConfig(t, m.mockO365, ct)
	conf2.Deduper = dd
	conf2.PollInterval = time.Hour
	fastRetries(&conf2)
	startAdapter(t, conf2, sink2)

	require.Eventually(t, func() bool { return sink2.count() == 2 },
		5*time.Second, 10*time.Millisecond, "the next instance must ship blob-2 and blob-3")
	assert.Empty(t, lostRecords(t, recs, &sink1.captureSink, sink2))
}

// A Ship failure stops the instance without recording the unshipped events or
// their blob as done: the next instance sharing the deduper ships the rest,
// and doesn't re-ship what was shipped.
func TestFailedShipIsRetriedByNextInstance(t *testing.T) {
	const ct = "Audit.Exchange"
	m := newFaultMock(t)
	created := time.Now().UTC().Add(-10 * time.Minute)
	recs := map[string]string{}
	var records []string
	for i := 1; i <= 3; i++ {
		rec := auditRecord(t, fmt.Sprintf("dddddddd-0000-0000-0000-%012d", i), "Send", "Exchange", "user@example.com", created)
		recs[fmt.Sprintf("record-%d", i)] = rec
		records = append(records, rec)
	}
	m.addBlob(ct, contentBlob{id: "blob-1", created: created, records: records})

	dd := newSharedDeduper()
	cb := &callbacks{}
	sink1 := &failingSink{failOn: 2}
	conf1 := newTestConfig(t, m.mockO365, ct)
	conf1.Deduper = dd
	conf1.PollInterval = time.Hour
	fastRetries(&conf1)
	cb.install(t, &conf1)
	_, chStopped, close1 := startAdapter(t, conf1, sink1)

	select {
	case <-chStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the adapter must stop after a Ship failure")
	}
	assert.Len(t, cb.errors(), 1, "the Ship failure must be reported")
	close1()

	sink2 := &captureSink{}
	conf2 := newTestConfig(t, m.mockO365, ct)
	conf2.Deduper = dd
	conf2.PollInterval = time.Hour
	fastRetries(&conf2)
	startAdapter(t, conf2, sink2)

	require.Eventually(t, func() bool { return sink2.count() == 2 },
		5*time.Second, 10*time.Millisecond, "the next instance must ship the two unshipped records")
	require.Never(t, func() bool { return sink2.count() != 2 },
		200*time.Millisecond, 20*time.Millisecond)
	assert.Empty(t, lostRecords(t, recs, &sink1.captureSink, sink2))
}

// Throttling on the listing is retried with a growing delay, not in a tight
// loop, and isn't reported as an error before the escalation time.
func TestListThrottlingBacksOff(t *testing.T) {
	const ct = "Audit.Exchange"
	m := newFaultMock(t)
	addBlobs(t, m, ct, 1)
	m.setFault(func(key string, _ int, w http.ResponseWriter, _ *http.Request) bool {
		if key != "list" {
			return false
		}
		writeJSON(w, http.StatusTooManyRequests, map[string]interface{}{
			"error": map[string]string{"code": "AF429", "message": "Too many requests."},
		})
		return true
	})

	cb := &callbacks{}
	conf := newTestConfig(t, m.mockO365, ct)
	conf.retryBaseDelay = 50 * time.Millisecond
	conf.retryMaxDelay = 200 * time.Millisecond
	conf.failureEscalation = time.Hour
	cb.install(t, &conf)
	_, _, closeIt := startAdapter(t, conf, &captureSink{})

	time.Sleep(1500 * time.Millisecond)
	closeIt()

	// Waits of 25-50, 50-100, then 100-200 ms: at most about 12 requests in
	// 1.5 s. A loop that retries at once makes hundreds.
	n := m.hit("list")
	assert.GreaterOrEqual(t, n, 3, "the listing must be retried")
	assert.LessOrEqual(t, n, 16, "the listing was retried in a tight loop")
	assert.Empty(t, cb.errors(), "throttling must not be reported as an error before the escalation time")
}

// A Retry-After on a 429 is honoured.
func TestRetryAfterIsHonoured(t *testing.T) {
	const ct = "Audit.Exchange"
	m := newFaultMock(t)
	recs := addBlobs(t, m, ct, 1)
	m.setFault(func(key string, hit int, w http.ResponseWriter, _ *http.Request) bool {
		if key == "blob-1" && hit == 1 {
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusTooManyRequests, map[string]interface{}{
				"error": map[string]string{"code": "AF429", "message": "Too many requests."},
			})
			return true
		}
		return false
	})

	cb := &callbacks{}
	sink := &captureSink{}
	conf := newTestConfig(t, m.mockO365, ct)
	conf.PollInterval = time.Hour
	fastRetries(&conf)
	cb.install(t, &conf)
	startAdapter(t, conf, sink)

	require.Eventually(t, func() bool { return sink.payloadCounts()[recs["blob-1"]] == 1 },
		5*time.Second, 10*time.Millisecond)
	times := m.hitTimes("blob-1")
	require.Len(t, times, 2)
	assert.GreaterOrEqual(t, times[1].Sub(times[0]), 950*time.Millisecond, "the retry came before Retry-After")
	assert.Empty(t, cb.errors())
}

// A failure that lasts is reported with OnError once the escalation time has
// passed, not before; the adapter keeps retrying and ships the blob when the
// API recovers.
func TestSustainedFailureEscalates(t *testing.T) {
	const ct = "Audit.Exchange"
	m := newFaultMock(t)
	recs := addBlobs(t, m, ct, 1)
	var mu sync.Mutex
	failing := true
	m.setFault(func(key string, _ int, w http.ResponseWriter, _ *http.Request) bool {
		mu.Lock()
		defer mu.Unlock()
		if key == "blob-1" && failing {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"error": map[string]string{"code": "AF50000", "message": "An internal error occurred. Retry the request."},
			})
			return true
		}
		return false
	})

	cb := &callbacks{}
	sink := &captureSink{}
	conf := newTestConfig(t, m.mockO365, ct)
	conf.PollInterval = time.Hour
	fastRetries(&conf)
	conf.failureEscalation = 400 * time.Millisecond
	cb.install(t, &conf)
	startAdapter(t, conf, sink)

	require.Eventually(t, func() bool { return len(cb.errors()) >= 1 },
		5*time.Second, 10*time.Millisecond, "a failure that lasts must be reported with OnError")
	first := m.hitTimes("blob-1")[0]
	assert.GreaterOrEqual(t, cb.errors()[0].Sub(first), 400*time.Millisecond, "OnError came before the escalation time")
	assert.GreaterOrEqual(t, cb.warnings(), 3, "failures before the escalation time are warnings")

	mu.Lock()
	failing = false
	mu.Unlock()
	require.Eventually(t, func() bool { return sink.payloadCounts()[recs["blob-1"]] == 1 },
		5*time.Second, 10*time.Millisecond, "the blob must ship once the API recovers")
	assert.LessOrEqual(t, len(cb.errors()), 2, "OnError must not be raised on every retry")
}

// A non-transient error (here a 403 on the listing) is reported at once and
// waits for the next poll instead of being retried in a loop.
func TestPermanentErrorIsNotRetried(t *testing.T) {
	const ct = "Audit.Exchange"
	m := newFaultMock(t)
	addBlobs(t, m, ct, 1)
	m.setFault(func(key string, _ int, w http.ResponseWriter, _ *http.Request) bool {
		if key != "list" {
			return false
		}
		writeJSON(w, http.StatusForbidden, map[string]interface{}{
			"error": map[string]string{"code": "AF10001", "message": "The permission set does not include ActivityFeed.Read."},
		})
		return true
	})

	cb := &callbacks{}
	conf := newTestConfig(t, m.mockO365, ct)
	conf.PollInterval = time.Hour
	fastRetries(&conf)
	cb.install(t, &conf)
	startAdapter(t, conf, &captureSink{})

	require.Eventually(t, func() bool { return len(cb.errors()) == 1 },
		5*time.Second, 10*time.Millisecond, "a 403 must be reported at once")
	require.Never(t, func() bool { return m.hit("list") != 1 },
		500*time.Millisecond, 20*time.Millisecond, "a 403 must not be retried before the next poll")
	assert.Len(t, cb.errors(), 1)
}

// Credentials the token endpoint refuses are reported at once, not retried
// as a transient failure.
func TestRefusedCredentialsAreNotRetried(t *testing.T) {
	const ct = "Audit.Exchange"
	m := newFaultMock(t)
	addBlobs(t, m, ct, 1)
	var mu sync.Mutex
	refuse := false
	m.setFault(func(key string, _ int, w http.ResponseWriter, _ *http.Request) bool {
		mu.Lock()
		defer mu.Unlock()
		switch key {
		case "token":
			if refuse {
				writeJSON(w, http.StatusUnauthorized, map[string]string{
					"error":             "invalid_client",
					"error_description": "AADSTS7000222: The provided client secret keys are expired.",
				})
				return true
			}
			// A token that is already about to expire, so the next
			// request needs a new one.
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"token_type":   "Bearer",
				"expires_in":   1,
				"access_token": testAccessToken,
				"resource":     "https://manage.office.com",
			})
			return true
		case "start":
			// The secret expires right after the subscription is started.
			refuse = true
		}
		return false
	})

	cb := &callbacks{}
	conf := newTestConfig(t, m.mockO365, ct)
	conf.PollInterval = time.Hour
	fastRetries(&conf)
	cb.install(t, &conf)
	startAdapter(t, conf, &captureSink{})

	require.Eventually(t, func() bool { return len(cb.errors()) == 1 },
		5*time.Second, 10*time.Millisecond, "refused credentials must be reported at once")
	require.Never(t, func() bool { return m.hit("token") != 2 },
		300*time.Millisecond, 20*time.Millisecond, "refused credentials must not be retried before the next poll")
	assert.Equal(t, 0, m.hit("list"))
}

// A blob the API says doesn't exist is skipped with a warning, the rest of the
// page ships, and the blob is tried again on the next poll.
func TestMissingBlobIsTriedAgain(t *testing.T) {
	const ct = "Audit.Exchange"
	m := newFaultMock(t)
	recs := addBlobs(t, m, ct, 2)
	m.setFault(func(key string, hit int, w http.ResponseWriter, _ *http.Request) bool {
		if key == "blob-1" && hit == 1 {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"error": map[string]string{"code": "AF20050", "message": "The specified content does not exist."},
			})
			return true
		}
		return false
	})

	cb := &callbacks{}
	sink := &captureSink{}
	conf := newTestConfig(t, m.mockO365, ct)
	fastRetries(&conf)
	cb.install(t, &conf)
	startAdapter(t, conf, sink)

	require.Eventually(t, func() bool {
		c := sink.payloadCounts()
		return c[recs["blob-1"]] == 1 && c[recs["blob-2"]] == 1
	}, 5*time.Second, 10*time.Millisecond, "both blobs must ship")
	assert.Equal(t, 2, m.hit("blob-1"))
	assert.Empty(t, cb.errors())
	assert.GreaterOrEqual(t, cb.warnings(), 1)
}
