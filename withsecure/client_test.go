package usp_withsecure

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClientOptions returns ClientOptions wired for a sink (no real LimaCharlie
// connection) with the logging callbacks pointed at the test log.
func testClientOptions(t *testing.T) uspclient.ClientOptions {
	t.Helper()
	return uspclient.ClientOptions{
		Identity: uspclient.Identity{
			Oid:             "11111111-1111-1111-1111-111111111111",
			InstallationKey: "test-installation-key",
		},
		Platform:     "json",
		TestSinkMode: true,
		DebugLog:     func(msg string) { t.Logf("DBG: %s", msg) },
		OnWarning:    func(msg string) { t.Logf("WRN: %s", msg) },
		OnError:      func(err error) { t.Logf("ERR: %v", err) },
	}
}

// testConfig returns a config pointed at the mock, collecting only the streams
// a test asks for. Polling is fast so tests finish quickly.
func testConfig(t *testing.T, baseURL string, feeds ...string) WithSecureConfig {
	t.Helper()
	off := false
	conf := WithSecureConfig{
		ClientOptions: testClientOptions(t),
		ClientID:      "test-client-id",
		ClientSecret:  "test-client-secret",
		BaseURL:       baseURL,
		PollInterval:  50 * time.Millisecond,
		Lookback:      1 * time.Hour,
		// Everything off, then switch on what the test wants.
		CollectSecurityEvents: &off,
		CollectIncidents:      &off,
		CollectDetections:     &off,
		CollectAuditLogs:      &off,
	}
	on := true
	for _, f := range feeds {
		switch f {
		case feedSecurityEvents:
			conf.CollectSecurityEvents = &on
		case feedIncidents:
			conf.CollectIncidents = &on
		case feedDetections:
			conf.CollectDetections = &on
		case feedAuditLogs:
			conf.CollectAuditLogs = &on
		default:
			t.Fatalf("unknown feed %q", f)
		}
	}
	return conf
}

// startAdapter runs the adapter against a capture sink and returns both.
func startAdapter(t *testing.T, conf WithSecureConfig) (*WithSecureAdapter, *captureSink) {
	t.Helper()
	sink := &captureSink{}
	a, _, err := newWithSecureAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a, sink
}

// waitForCount waits until the sink has at least n messages.
func waitForCount(t *testing.T, sink *captureSink, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return sink.count() >= n }, 5*time.Second, 10*time.Millisecond,
		"expected at least %d shipped messages, got %d", n, sink.count())
}

// countingDeduper wraps a Deduper and records, per key, how many times the key
// was reported as new (CheckAndAdd returned false). The adapter ships a record
// exactly when its key is new, so a count above 1 means a record was shipped
// more than once -- a dedup failure.
type countingDeduper struct {
	inner utils.Deduper
	mu    sync.Mutex
	new   map[string]int
}

func newCountingDeduper(t *testing.T) *countingDeduper {
	t.Helper()
	inner, err := utils.NewLocalDeduper(1*time.Hour, 24*time.Hour)
	require.NoError(t, err)
	return &countingDeduper{inner: inner, new: map[string]int{}}
}

func (d *countingDeduper) CheckAndAdd(key string) bool {
	seen := d.inner.CheckAndAdd(key)
	if !seen {
		d.mu.Lock()
		d.new[key]++
		d.mu.Unlock()
	}
	return seen
}

func (d *countingDeduper) Close() { d.inner.Close() }

func (d *countingDeduper) maxNewCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	max := 0
	for _, n := range d.new {
		if n > max {
			max = n
		}
	}
	return max
}

// Config validation
// ============================================================================

func TestValidate(t *testing.T) {
	base := func() WithSecureConfig {
		return WithSecureConfig{
			ClientOptions: testClientOptions(t),
			ClientID:      "id",
			ClientSecret:  "secret",
		}
	}

	t.Run("defaults are filled", func(t *testing.T) {
		c := base()
		require.NoError(t, c.Validate())
		assert.Equal(t, defaultBaseURL, c.BaseURL)
		assert.Equal(t, defaultPollInterval, c.PollInterval)
		assert.Equal(t, defaultLookback, c.Lookback)
		assert.Equal(t, defaultDedupeTTL, c.DedupeTTL)
		assert.Equal(t, defaultMaxPages, c.MaxPages)
		assert.Equal(t, defaultUserAgent, c.UserAgent)
		assert.Equal(t, defaultMaxRetryAttempts, c.MaxRetryAttempts)
	})

	t.Run("credentials required", func(t *testing.T) {
		c := base()
		c.ClientID = ""
		assert.ErrorContains(t, c.Validate(), "client_id")

		c = base()
		c.ClientSecret = "  "
		assert.ErrorContains(t, c.Validate(), "client_secret")
	})

	t.Run("base url is normalized", func(t *testing.T) {
		c := base()
		c.BaseURL = "https://api.connect-stg.fsapi.com/"
		require.NoError(t, c.Validate())
		assert.Equal(t, "https://api.connect-stg.fsapi.com", c.BaseURL)
	})

	// Detections are fetched per incident -- there is no organization-wide
	// detections endpoint -- so the combination is rejected rather than
	// silently collecting nothing.
	t.Run("detections require incidents", func(t *testing.T) {
		c := base()
		off := false
		c.CollectIncidents = &off
		assert.ErrorContains(t, c.Validate(), "collect_detections requires collect_incidents")
	})

	t.Run("at least one stream", func(t *testing.T) {
		c := base()
		off := false
		c.CollectSecurityEvents = &off
		c.CollectIncidents = &off
		c.CollectDetections = &off
		c.CollectAuditLogs = &off
		assert.ErrorContains(t, c.Validate(), "every stream is disabled")
	})

	// Audit logs default OFF; the other streams default ON.
	t.Run("stream defaults", func(t *testing.T) {
		c := base()
		require.NoError(t, c.Validate())
		assert.True(t, enabled(c.CollectSecurityEvents, true))
		assert.True(t, enabled(c.CollectIncidents, true))
		assert.True(t, enabled(c.CollectDetections, true))
		assert.False(t, enabled(c.CollectAuditLogs, false))
	})
}

// Authentication
// ============================================================================

// The token endpoint takes the credentials in an HTTP Basic header and only
// grant_type/scope in the body; the documented behavior is to reject a payload
// carrying anything else, so the mock enforces it and this test pins it.
func TestAuth_TokenRequestShape(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)
	m.addSecurityEvents(realisticSecurityEvent("evt-1", time.Now().Add(-time.Minute)))

	_, sink := startAdapter(t, testConfig(t, srv.URL, feedSecurityEvents))
	waitForCount(t, sink, 1)

	assert.Equal(t, 1, m.tokenRequestCount(), "the token should be minted once and cached")
	reqs := m.requestsFor(pathSecurityEvents)
	require.NotEmpty(t, reqs)
	assert.Equal(t, "Bearer ws-token-1", reqs[0].Auth)
	assert.Equal(t, defaultUserAgent, reqs[0].UserAgent,
		"the API rejects requests without a User-Agent")
}

// A bad credential stops the adapter: retrying cannot fix it, and silently
// collecting nothing would be worse than failing visibly.
func TestAuth_BadCredentialsStopAdapter(t *testing.T) {
	m := newMockElements()
	m.rejectAuth = true
	srv := m.start(t)
	m.addSecurityEvents(realisticSecurityEvent("evt-1", time.Now().Add(-time.Minute)))

	conf := testConfig(t, srv.URL, feedSecurityEvents)
	sink := &captureSink{}
	a, chRunning, err := newWithSecureAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })

	select {
	case <-chRunning:
	case <-time.After(5 * time.Second):
		t.Fatal("adapter did not stop on an authentication failure")
	}
	assert.Zero(t, sink.count(), "nothing should ship when authentication fails")
}

// A 401 mid-poll is far more likely to be an expiry race than a revoked
// credential, so the request is replayed once with a freshly minted token.
func TestAuth_RefreshesTokenOn401(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)
	m.addSecurityEvents(realisticSecurityEvent("evt-1", time.Now().Add(-time.Minute)))

	_, sink := startAdapter(t, testConfig(t, srv.URL, feedSecurityEvents))
	waitForCount(t, sink, 1)

	// Force the next data request to 401.
	m.setFailNext(401)
	m.addSecurityEvents(realisticSecurityEvent("evt-2", time.Now()))

	waitForCount(t, sink, 2)
	assert.GreaterOrEqual(t, m.tokenRequestCount(), 2, "a 401 should mint a fresh token")
}

// Security events
// ============================================================================

func TestSecurityEvents_ShipVerbatim(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)
	ts := time.Now().Add(-5 * time.Minute).Truncate(time.Millisecond)
	want := realisticSecurityEvent("evt-1", ts)
	m.addSecurityEvents(want)

	_, sink := startAdapter(t, testConfig(t, srv.URL, feedSecurityEvents))
	waitForCount(t, sink, 1)

	msgs := sink.snapshot()
	require.Len(t, msgs, 1)
	assert.Equal(t, feedSecurityEvents, msgs[0].EventType)
	assert.Equal(t, uint64(ts.UTC().UnixMilli()), msgs[0].TimestampMs,
		"the event time should come from persistenceTimestamp, not the wall clock")

	got := utils.Dict(msgs[0].JsonPayload)
	assertVerbatim(t, want, got)

	// Large integers must survive the round-trip without float coercion.
	details, ok := got["details"].(map[string]interface{})
	require.True(t, ok)
	assert.EqualValues(t, uint64(9007199254740993), toUint64(t, details["fileSize"]),
		"a large integer lost precision")
}

// The query must carry a time bound (the API rejects an unbounded one) and
// exclusiveStart (without it the boundary event is re-read on every poll).
func TestSecurityEvents_QueryShape(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)
	m.addSecurityEvents(realisticSecurityEvent("evt-1", time.Now().Add(-time.Minute)))

	conf := testConfig(t, srv.URL, feedSecurityEvents)
	conf.OrganizationID = "org-uuid"
	conf.Engines = []string{"deepGuard", "firewall"}
	conf.Severities = []string{"critical", "warning"}
	_, sink := startAdapter(t, conf)
	waitForCount(t, sink, 1)

	reqs := m.requestsFor(pathSecurityEvents)
	require.NotEmpty(t, reqs)
	r := reqs[0]

	// A form-encoded POST, not a GET or a JSON body.
	assert.Equal(t, "POST", r.Method)
	require.NotNil(t, r.Form, "security events must be queried with a form-encoded body")

	p := r.params()
	assert.NotEmpty(t, p.Get("persistenceTimestampStart"), "the API rejects a query with no time bound")
	assert.Equal(t, "true", p.Get("exclusiveStart"))
	assert.Equal(t, "asc", p.Get("order"))
	assert.Equal(t, "org-uuid", p.Get("organizationId"))
	assert.ElementsMatch(t, []string{"deepGuard", "firewall"}, p["engine"],
		"array filters are sent as repeated keys")
	assert.ElementsMatch(t, []string{"critical", "warning"}, p["severity"])
}

// Re-polling an unchanged data set must not re-ship anything.
func TestSecurityEvents_RepollDoesNotDuplicate(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)
	for i := 0; i < 3; i++ {
		m.addSecurityEvents(realisticSecurityEvent(
			fmt.Sprintf("evt-%c", 'a'+i), time.Now().Add(-time.Duration(10-i)*time.Minute)))
	}

	dedup := newCountingDeduper(t)
	conf := testConfig(t, srv.URL, feedSecurityEvents)
	conf.Deduper = dedup
	_, sink := startAdapter(t, conf)
	waitForCount(t, sink, 3)

	// Let several more polls run over the same data.
	time.Sleep(400 * time.Millisecond)

	assert.Equal(t, 3, sink.count(), "a re-poll re-shipped events")
	assert.Equal(t, 1, dedup.maxNewCount(), "an event was treated as new more than once")
	assert.Greater(t, m.requestCount(), 3, "the test did not actually re-poll")
}

// An event appearing mid-run ships exactly once.
func TestSecurityEvents_NewEventShipsOnce(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)
	m.addSecurityEvents(realisticSecurityEvent("evt-1", time.Now().Add(-10*time.Minute)))

	dedup := newCountingDeduper(t)
	conf := testConfig(t, srv.URL, feedSecurityEvents)
	conf.Deduper = dedup
	_, sink := startAdapter(t, conf)
	waitForCount(t, sink, 1)

	m.addSecurityEvents(realisticSecurityEvent("evt-2", time.Now()))
	waitForCount(t, sink, 2)
	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, 2, sink.count())
	assert.Equal(t, 1, dedup.maxNewCount())
	ids := []string{}
	for _, d := range sink.byEventType(feedSecurityEvents) {
		ids = append(ids, d.FindOneString("id"))
	}
	assert.ElementsMatch(t, []string{"evt-1", "evt-2"}, ids)
}

// A multi-page result set is walked to the end, each record shipped once.
func TestSecurityEvents_MultiPage(t *testing.T) {
	m := newMockElements()
	m.pageSize = 3 // force pagination regardless of the requested limit
	srv := m.start(t)

	base := time.Now().Add(-time.Hour).Add(time.Minute)
	const total = 10
	for i := 0; i < total; i++ {
		m.addSecurityEvents(realisticSecurityEvent(
			fmt.Sprintf("evt-%02d", i), base.Add(time.Duration(i)*time.Second)))
	}

	dedup := newCountingDeduper(t)
	conf := testConfig(t, srv.URL, feedSecurityEvents)
	conf.Deduper = dedup
	_, sink := startAdapter(t, conf)
	waitForCount(t, sink, total)
	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, total, sink.count(), "a paginated result set was not fully consumed")
	assert.Equal(t, 1, dedup.maxNewCount())

	// The anchor must have been used: more than one page request per poll.
	var anchored int
	for _, r := range m.requestsFor(pathSecurityEvents) {
		if r.params().Get("anchor") != "" {
			anchored++
		}
	}
	assert.Greater(t, anchored, 0, "the nextAnchor cursor was never followed")
}

// The page size is clamped to each endpoint's documented ceiling; requesting
// more is rejected by the real API.
func TestPageSizeIsClamped(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)
	m.addSecurityEvents(realisticSecurityEvent("evt-1", time.Now().Add(-time.Minute)))
	inc := realisticIncident("inc-1", time.Now().Add(-time.Hour), time.Now().Add(-time.Minute))
	m.addIncidents(inc)

	conf := testConfig(t, srv.URL, feedSecurityEvents, feedIncidents)
	conf.PageSize = 10000
	_, sink := startAdapter(t, conf)
	waitForCount(t, sink, 2)

	evReqs := m.requestsFor(pathSecurityEvents)
	require.NotEmpty(t, evReqs)
	assert.Equal(t, strconv.Itoa(maxSecurityEventPageSize), evReqs[0].params().Get("limit"))

	incReqs := m.requestsFor(pathIncidents)
	require.NotEmpty(t, incReqs)
	assert.Equal(t, strconv.Itoa(maxIncidentPageSize), incReqs[0].params().Get("limit"),
		"the incidents endpoint caps at a lower page size than security events")
}

// Incidents and detections
// ============================================================================

func TestIncidents_ShipWithDetections(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)

	created := time.Now().Add(-30 * time.Minute).Truncate(time.Millisecond)
	updated := time.Now().Add(-5 * time.Minute).Truncate(time.Millisecond)
	inc := realisticIncident("inc-1", created, updated)
	det := realisticDetection("det-1", "inc-1", created)
	m.addIncidents(inc)
	m.setDetections("inc-1", det)

	_, sink := startAdapter(t, testConfig(t, srv.URL, feedIncidents, feedDetections))
	waitForCount(t, sink, 2)

	incidents := sink.byEventType(feedIncidents)
	require.Len(t, incidents, 1)
	assertVerbatim(t, inc, incidents[0])

	detections := sink.byEventType(feedDetections)
	require.Len(t, detections, 1)
	assertVerbatim(t, det, detections[0])

	// Event times come from the records, not the wall clock.
	for _, msg := range sink.snapshot() {
		switch msg.EventType {
		case feedIncidents:
			assert.Equal(t, uint64(updated.UTC().UnixMilli()), msg.TimestampMs)
		case feedDetections:
			assert.Equal(t, uint64(created.UTC().UnixMilli()), msg.TimestampMs)
		}
	}
}

// A BCD is a living object: when it evolves, the new version is shipped again
// (it is new information), but an unchanged one is not.
func TestIncidents_ReshippedOnlyWhenUpdated(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)

	created := time.Now().Add(-30 * time.Minute)
	updated := time.Now().Add(-10 * time.Minute)
	m.addIncidents(realisticIncident("inc-1", created, updated))

	dedup := newCountingDeduper(t)
	conf := testConfig(t, srv.URL, feedIncidents)
	conf.Deduper = dedup
	_, sink := startAdapter(t, conf)
	waitForCount(t, sink, 1)

	// Several polls over the unchanged incident must not duplicate it.
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, 1, sink.count(), "an unchanged incident was re-shipped")

	// Now the incident evolves: new status, new updatedTimestamp.
	evolved := realisticIncident("inc-1", created, time.Now())
	evolved["status"] = "inProgress"
	evolved["riskLevel"] = "high"
	m.updateIncident("inc-1", evolved)

	waitForCount(t, sink, 2)
	time.Sleep(200 * time.Millisecond)

	incidents := sink.byEventType(feedIncidents)
	require.Len(t, incidents, 2, "the updated incident version should ship")
	assert.Equal(t, "new", incidents[0].FindOneString("status"))
	assert.Equal(t, "inProgress", incidents[1].FindOneString("status"))
	assert.Equal(t, 1, dedup.maxNewCount())
}

// A detection is stable, so re-visiting an incident that changed must not
// re-ship the detections already collected -- only the new one.
func TestDetections_NotDuplicatedWhenIncidentUpdates(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)

	created := time.Now().Add(-30 * time.Minute)
	m.addIncidents(realisticIncident("inc-1", created, time.Now().Add(-10*time.Minute)))
	m.setDetections("inc-1", realisticDetection("det-1", "inc-1", created))

	dedup := newCountingDeduper(t)
	conf := testConfig(t, srv.URL, feedIncidents, feedDetections)
	conf.Deduper = dedup
	_, sink := startAdapter(t, conf)
	waitForCount(t, sink, 2)

	// The incident gains a second detection and is re-stamped.
	m.setDetections("inc-1",
		realisticDetection("det-1", "inc-1", created),
		realisticDetection("det-2", "inc-1", time.Now()))
	evolved := realisticIncident("inc-1", created, time.Now())
	m.updateIncident("inc-1", evolved)

	waitForCount(t, sink, 4) // 2 incident versions + 2 detections
	time.Sleep(300 * time.Millisecond)

	detections := sink.byEventType(feedDetections)
	ids := []string{}
	for _, d := range detections {
		ids = append(ids, d.FindOneString("detectionId"))
	}
	assert.ElementsMatch(t, []string{"det-1", "det-2"}, ids,
		"the already-collected detection was shipped again")
	assert.Equal(t, 1, dedup.maxNewCount())
}

// Archived BCDs are filtered out by default (the API documents that skipping
// them is also faster).
func TestIncidents_ArchivedFilteredByDefault(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)

	created := time.Now().Add(-30 * time.Minute)
	live := realisticIncident("inc-live", created, time.Now().Add(-time.Minute))
	archived := realisticIncident("inc-archived", created, time.Now().Add(-time.Minute))
	archived["archived"] = true
	m.addIncidents(live, archived)

	_, sink := startAdapter(t, testConfig(t, srv.URL, feedIncidents))
	waitForCount(t, sink, 1)
	time.Sleep(300 * time.Millisecond)

	incidents := sink.byEventType(feedIncidents)
	require.Len(t, incidents, 1)
	assert.Equal(t, "inc-live", incidents[0].FindOneString("incidentId"))

	reqs := m.requestsFor(pathIncidents)
	require.NotEmpty(t, reqs)
	assert.Equal(t, "false", reqs[0].params().Get("archived"))
}

func TestIncidents_ArchivedIncludedWhenAsked(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)

	created := time.Now().Add(-30 * time.Minute)
	archived := realisticIncident("inc-archived", created, time.Now().Add(-time.Minute))
	archived["archived"] = true
	m.addIncidents(archived)

	conf := testConfig(t, srv.URL, feedIncidents)
	conf.IncludeArchivedIncidents = true
	_, sink := startAdapter(t, conf)
	waitForCount(t, sink, 1)

	reqs := m.requestsFor(pathIncidents)
	require.NotEmpty(t, reqs)
	assert.Empty(t, reqs[0].params().Get("archived"))
}

// Detections are fetched per incident, so the incident id must be on the query.
func TestDetections_QueryShape(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)

	created := time.Now().Add(-30 * time.Minute)
	m.addIncidents(realisticIncident("inc-1", created, time.Now().Add(-time.Minute)))
	m.setDetections("inc-1", realisticDetection("det-1", "inc-1", created))

	_, sink := startAdapter(t, testConfig(t, srv.URL, feedIncidents, feedDetections))
	waitForCount(t, sink, 2)

	reqs := m.requestsFor(pathDetections)
	require.NotEmpty(t, reqs)
	assert.Equal(t, "inc-1", reqs[0].params().Get("incidentId"))
}

// With detections off, only incidents are collected.
func TestDetections_DisabledSkipsTheEndpoint(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)

	created := time.Now().Add(-30 * time.Minute)
	m.addIncidents(realisticIncident("inc-1", created, time.Now().Add(-time.Minute)))
	m.setDetections("inc-1", realisticDetection("det-1", "inc-1", created))

	_, sink := startAdapter(t, testConfig(t, srv.URL, feedIncidents))
	waitForCount(t, sink, 1)
	time.Sleep(300 * time.Millisecond)

	assert.Empty(t, sink.byEventType(feedDetections))
	assert.Empty(t, m.requestsFor(pathDetections), "the detections endpoint should not be called")
}

// Audit logs
// ============================================================================

func TestAuditLogs_ShipVerbatim(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)
	ts := time.Now().Add(-2 * time.Minute).Truncate(time.Millisecond)
	want := realisticAuditLog("audit-1", ts)
	m.addAuditLogs(want)

	_, sink := startAdapter(t, testConfig(t, srv.URL, feedAuditLogs))
	waitForCount(t, sink, 1)

	msgs := sink.snapshot()
	require.Len(t, msgs, 1)
	assert.Equal(t, feedAuditLogs, msgs[0].EventType)
	assert.Equal(t, uint64(ts.UTC().UnixMilli()), msgs[0].TimestampMs)

	got := utils.Dict(msgs[0].JsonPayload)
	assertVerbatim(t, want, got)

	reqs := m.requestsFor(pathAuditLogs)
	require.NotEmpty(t, reqs)
	assert.Equal(t, "true", reqs[0].params().Get("exclusiveStart"))
	assert.NotEmpty(t, reqs[0].params().Get("serverTimestampStart"))
}

// The API rejects an audit-log query spanning more than 30 days, so a longer
// lookback must be capped rather than passed through.
func TestAuditLogs_LookbackCappedAtThirtyDays(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)
	m.addAuditLogs(realisticAuditLog("audit-1", time.Now().Add(-time.Minute)))

	conf := testConfig(t, srv.URL, feedAuditLogs)
	conf.Lookback = 365 * 24 * time.Hour
	_, sink := startAdapter(t, conf)
	waitForCount(t, sink, 1)

	reqs := m.requestsFor(pathAuditLogs)
	require.NotEmpty(t, reqs)
	start, err := time.Parse(cursorLayout, reqs[0].params().Get("serverTimestampStart"))
	require.NoError(t, err)
	age := time.Since(start)
	assert.LessOrEqual(t, age, maxAuditLogWindow+time.Minute,
		"an audit-log query older than 30 days is rejected by the API")
}

// Multiple streams
// ============================================================================

func TestMultipleStreams_TaggedCorrectly(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)

	created := time.Now().Add(-30 * time.Minute)
	m.addSecurityEvents(realisticSecurityEvent("evt-1", time.Now().Add(-time.Minute)))
	m.addIncidents(realisticIncident("inc-1", created, time.Now().Add(-time.Minute)))
	m.setDetections("inc-1", realisticDetection("det-1", "inc-1", created))
	m.addAuditLogs(realisticAuditLog("audit-1", time.Now().Add(-time.Minute)))

	_, sink := startAdapter(t, testConfig(t, srv.URL,
		feedSecurityEvents, feedIncidents, feedDetections, feedAuditLogs))
	waitForCount(t, sink, 4)
	time.Sleep(300 * time.Millisecond)

	assert.Len(t, sink.byEventType(feedSecurityEvents), 1)
	assert.Len(t, sink.byEventType(feedIncidents), 1)
	assert.Len(t, sink.byEventType(feedDetections), 1)
	assert.Len(t, sink.byEventType(feedAuditLogs), 1)
}

// Error handling
// ============================================================================

// A source-side failure must not stop the adapter: the cloud-sensor host treats
// an adapter error as fatal to the instance, and restarting cannot fix a
// problem on WithSecure's side. The poll is skipped and retried.
func TestSourceErrorDoesNotStopAdapter(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)
	m.addSecurityEvents(realisticSecurityEvent("evt-1", time.Now().Add(-time.Minute)))

	conf := testConfig(t, srv.URL, feedSecurityEvents)
	conf.RetryBaseDelay = 10 * time.Millisecond
	conf.MaxRetryDelay = 20 * time.Millisecond
	sink := &captureSink{}
	a, chRunning, err := newWithSecureAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })

	// A 500 on the first poll; the adapter should retry and recover.
	m.setFailNext(500)
	waitForCount(t, sink, 1)

	select {
	case <-chRunning:
		t.Fatal("the adapter stopped on a source-side error")
	default:
	}
}

// A transient failure is retried; a 429 counts as transient because the API
// rate-limits these endpoints at 300 requests/minute.
func TestIsTransientError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"500", &HTTPError{StatusCode: 500}, true},
		{"503", &HTTPError{StatusCode: 503}, true},
		{"429 rate limit", &HTTPError{StatusCode: 429}, true},
		{"400", &HTTPError{StatusCode: 400}, false},
		{"401", &HTTPError{StatusCode: 401}, false},
		{"403", &HTTPError{StatusCode: 403}, false},
		{"404", &HTTPError{StatusCode: 404}, false},
		{"context canceled", context.Canceled, false},
		{"nil", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isTransientError(tc.err))
		})
	}
}

func TestIsAuthError(t *testing.T) {
	assert.True(t, isAuthError(&HTTPError{StatusCode: 401}))
	assert.False(t, isAuthError(&HTTPError{StatusCode: 403}),
		"a 403 is a scope problem, not a credential problem a restart could fix")
	assert.False(t, isAuthError(&HTTPError{StatusCode: 500}))
	assert.True(t, isAuthError(&tokenError{statusCode: 401, code: "invalid_client", fatal: true}))
	assert.False(t, isAuthError(&tokenError{statusCode: 503, code: "token_request_failed"}))
}

// Envelope parsing
// ============================================================================

func TestExtractPage(t *testing.T) {
	t.Run("items and anchor", func(t *testing.T) {
		items, next, err := extractPage([]byte(`{"items":[{"id":"a"},{"id":"b"}],"nextAnchor":"cursor-1"}`))
		require.NoError(t, err)
		require.Len(t, items, 2)
		assert.Equal(t, "cursor-1", next)
	})

	t.Run("last page has no anchor", func(t *testing.T) {
		items, next, err := extractPage([]byte(`{"items":[{"id":"a"}]}`))
		require.NoError(t, err)
		require.Len(t, items, 1)
		assert.Empty(t, next)
	})

	t.Run("empty result set", func(t *testing.T) {
		items, next, err := extractPage([]byte(`{"items":[]}`))
		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, next)
	})

	t.Run("empty body", func(t *testing.T) {
		items, next, err := extractPage(nil)
		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, next)
	})

	t.Run("malformed", func(t *testing.T) {
		_, _, err := extractPage([]byte(`not json`))
		assert.Error(t, err)
	})

	// One anomalous element must not block the rest of the page.
	t.Run("skips non-object records", func(t *testing.T) {
		items, _, err := extractPage([]byte(`{"items":[{"id":"a"},null,42,{},{"id":"b"}]}`))
		require.NoError(t, err)
		require.Len(t, items, 2)
		assert.Equal(t, "a", items[0].FindOneString("id"))
		assert.Equal(t, "b", items[1].FindOneString("id"))
	})
}

func TestParseTimestamp(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"2026-07-30T09:31:03.292Z", "2026-07-30T09:31:03.292Z"},
		{"2026-07-30T09:31:03Z", "2026-07-30T09:31:03Z"},
		{"2026-07-30T09:31:03", "2026-07-30T09:31:03Z"},
		{"2026-07-30 09:31:03", "2026-07-30T09:31:03Z"},
	} {
		got, ok := parseTimestamp(tc.in)
		require.True(t, ok, "failed to parse %q", tc.in)
		assert.Equal(t, tc.want, got.Format(time.RFC3339Nano))
	}

	_, ok := parseTimestamp("")
	assert.False(t, ok)
	_, ok = parseTimestamp("not a timestamp")
	assert.False(t, ok)
}

// A record missing its id field still deduplicates, via a content hash.
func TestRecordID(t *testing.T) {
	assert.Equal(t, "evt-1", recordID(utils.Dict{"id": "evt-1"}, "id"))
	// Numeric ids are accepted too.
	assert.Equal(t, "42", recordID(utils.Dict{"id": 42}, "id"))

	hashed := recordID(utils.Dict{"other": "value"}, "id")
	assert.True(t, len(hashed) > 7 && hashed[:7] == "sha256:", "expected a content hash, got %q", hashed)
	assert.Equal(t, hashed, recordID(utils.Dict{"other": "value"}, "id"), "the hash must be stable")
	assert.NotEqual(t, hashed, recordID(utils.Dict{"other": "different"}, "id"))
}

func TestClampPageSize(t *testing.T) {
	assert.Equal(t, 50, clamp(50, 200))
	assert.Equal(t, 200, clamp(500, 200))
	assert.Equal(t, 200, clamp(0, 200))
	assert.Equal(t, 200, clamp(-1, 200))
	assert.Equal(t, 200, clamp(200, 200))
}

func TestAddAll(t *testing.T) {
	v := url.Values{}
	addAll(v, "engine", []string{"deepGuard", "  ", "firewall", ""})
	assert.Equal(t, []string{"deepGuard", "firewall"}, v["engine"],
		"blank values must be dropped rather than sent as empty filters")
}

// Lifecycle
// ============================================================================

func TestClose_IsIdempotent(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)
	m.addSecurityEvents(realisticSecurityEvent("evt-1", time.Now().Add(-time.Minute)))

	sink := &captureSink{}
	a, _, err := newWithSecureAdapter(context.Background(), testConfig(t, srv.URL, feedSecurityEvents), sink)
	require.NoError(t, err)
	waitForCount(t, sink, 1)

	assert.NoError(t, a.Close())
	assert.NoError(t, a.Close(), "Close must be idempotent")
}

func TestClose_StopsPolling(t *testing.T) {
	m := newMockElements()
	srv := m.start(t)
	m.addSecurityEvents(realisticSecurityEvent("evt-1", time.Now().Add(-time.Minute)))

	sink := &captureSink{}
	a, _, err := newWithSecureAdapter(context.Background(), testConfig(t, srv.URL, feedSecurityEvents), sink)
	require.NoError(t, err)
	waitForCount(t, sink, 1)

	require.NoError(t, a.Close())
	before := m.requestCount()
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, before, m.requestCount(), "polling continued after Close")
}

// Helpers
// ============================================================================

// toUint64 reads a decoded JSON number that must retain full integer precision.
func toUint64(t *testing.T, v interface{}) uint64 {
	t.Helper()
	switch n := v.(type) {
	case uint64:
		return n
	case int64:
		return uint64(n)
	case int:
		return uint64(n)
	case float64:
		return uint64(n)
	case json.Number:
		u, err := n.Int64()
		require.NoError(t, err)
		return uint64(u)
	}
	t.Fatalf("unexpected numeric type %T", v)
	return 0
}
