package usp_sublime

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock of the Sublime
// Security platform API (GET /v0/audit-log/events), capturing the exact
// messages it ships so their content -- timestamp, event type and verbatim
// payload -- can be asserted.

// --- in-memory USP sink -----------------------------------------------------

// captureSink is an in-memory uspSink that records every shipped message.
type captureSink struct {
	mu       sync.Mutex
	messages []*protocol.DataMessage
}

func (s *captureSink) Ship(m *protocol.DataMessage, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, m)
	return nil
}

func (s *captureSink) Drain(time.Duration) error               { return nil }
func (s *captureSink) Close() ([]*protocol.DataMessage, error) { return nil, nil }

func (s *captureSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.messages)
}

func (s *captureSink) snapshot() []*protocol.DataMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*protocol.DataMessage, len(s.messages))
	copy(out, s.messages)
	return out
}

// --- mock Sublime platform API -----------------------------------------------

// mockSublime is an in-memory stand-in for the Sublime Security platform API's
// audit log listing (https://docs.sublime.security/reference/listeventsinauditlog).
// It honours the contract the adapter relies on: a GET on /v0/audit-log/events
// authenticated with a Bearer API key, offset/limit query pagination (limit
// capped at 500 by the real API), and an object envelope
// {"events": [...], "count": N, "total": M} around the page (count is the
// number of results on the current page, total the number available).
//
// Each of these behaviours was checked against the live API and holds:
//   - `Authorization: Bearer <key>` on https://platform.sublime.security
//     returns 200; an invalid key returns 401 with an
//     {"error":{"type":"unauthorized",...}} body.
//   - `created_at[gte]` is applied SERVER-SIDE and is INCLUSIVE, with
//     microsecond resolution; `offset`/`limit` are applied AFTER it, which is
//     why this mock filters before slicing.
//   - `created_at[lt]` is the documented EXCLUSIVE upper bound, also applied
//     server-side. (`created_at[lte]` and `created_at[gt]` are not supported
//     and are silently ignored.)
//   - The filter is accepted with literal `[`/`]` and a percent-encoded value
//     -- exactly the form makeOneRequest builds.
//   - An unknown parameter name is SILENTLY IGNORED (a misspelled filter
//     degrades to a full scan rather than erroring), so the name must stay
//     exactly `created_at[gte]`.
//   - limit=500 is accepted; limit=501 is rejected with 400.
//   - Events are returned newest-first (created_at DESCENDING), and no sort
//     parameter changes that. The mock sorts the same way, so pagination
//     bugs that depend on the ordering are reproduced here.
//   - An empty window returns {"events":[],"count":0,"total":0} -- `[]`, not
//     null.
type mockSublime struct {
	mu     sync.Mutex
	apiKey string
	events []utils.Dict
	// times caches each event's parsed created_at (the zero time when it
	// does not parse), keyed by index into events. Both are kept sorted
	// newest-first, the order the live API serves.
	times []time.Time

	requests      int
	authFailures  int
	maxOffsetSeen int
	lastMethod    string
	lastPath      string
	lastAccept    string
	lastLimit     int
	lastGTE       string
	sawGTE        bool
	lastLT        string
	sawLT         bool

	// fail, when set, is consulted for every authenticated request; returning
	// true makes that request fail with a 502 -- a transient upstream error.
	fail func(r *http.Request) bool
}

func newMockSublime(apiKey string) *mockSublime {
	return &mockSublime{apiKey: apiKey}
}

func (m *mockSublime) setEvents(events []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = nil
	m.times = nil
	for _, e := range events {
		m.events = append(m.events, e)
		m.times = append(m.times, parseCreatedAt(e))
	}
	m.sortNewestFirst()
}

func (m *mockSublime) sortNewestFirst() {
	idx := make([]int, len(m.events))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(i, j int) bool { return m.times[idx[i]].After(m.times[idx[j]]) })
	events := make([]utils.Dict, len(idx))
	times := make([]time.Time, len(idx))
	for i, k := range idx {
		events[i], times[i] = m.events[k], m.times[k]
	}
	m.events, m.times = events, times
}

func parseCreatedAt(e utils.Dict) time.Time {
	at, err := time.Parse(time.RFC3339Nano, fmt.Sprint(e["created_at"]))
	if err != nil {
		return time.Time{}
	}
	return at
}

// appendEvent adds a newly-occurred event to the audit log.
func (m *mockSublime) appendEvent(event utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
	m.times = append(m.times, parseCreatedAt(event))
	m.sortNewestFirst()
}

func (m *mockSublime) setFail(fail func(r *http.Request) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail = fail
}

func (m *mockSublime) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func (m *mockSublime) authFailureCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authFailures
}

func (m *mockSublime) sawTimeFilter() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sawGTE
}

func (m *mockSublime) maxOffset() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxOffsetSeen
}

func (m *mockSublime) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()

		m.requests++
		m.lastMethod = r.Method
		m.lastPath = r.URL.Path
		m.lastAccept = r.Header.Get("Accept")

		if r.Method != http.MethodGet {
			http.Error(w, `{"message":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != logsPath {
			http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
			return
		}
		// The platform API authenticates with a Bearer API key.
		if r.Header.Get("Authorization") != "Bearer "+m.apiKey {
			m.authFailures++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"invalid or missing API key"}`))
			return
		}
		if m.fail != nil && m.fail(r) {
			http.Error(w, `{"message":"bad gateway"}`, http.StatusBadGateway)
			return
		}

		limit := pageLimit
		if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 {
			limit = v
		}
		offset := 0
		if v, err := strconv.Atoi(r.URL.Query().Get("offset")); err == nil && v >= 0 {
			offset = v
		}
		m.lastLimit = limit
		if offset > m.maxOffsetSeen {
			m.maxOffsetSeen = offset
		}

		// Honour the created_at[gte] server-side filter the real API supports
		// (inclusive lower bound, ISO 8601 UTC). The adapter relies on this to
		// avoid re-scanning the whole audit log every poll, so the mock must
		// apply it before offset/limit -- exactly as the real API does.
		m.lastGTE = r.URL.Query().Get("created_at[gte]")
		var gte time.Time
		if m.lastGTE != "" {
			m.sawGTE = true
			if parsed, err := time.Parse(time.RFC3339Nano, m.lastGTE); err == nil {
				gte = parsed
			}
		}

		m.lastLT = r.URL.Query().Get("created_at[lt]")
		var lt time.Time
		if m.lastLT != "" {
			m.sawLT = true
			if parsed, err := time.Parse(time.RFC3339Nano, m.lastLT); err == nil {
				lt = parsed
			}
		}

		// m.events is already newest-first, like the live API.
		filtered := []utils.Dict{}
		for i, e := range m.events {
			createdAt := m.times[i]
			if createdAt.IsZero() {
				continue
			}
			if !gte.IsZero() && createdAt.Before(gte) { // created_at >= gte
				continue
			}
			if !lt.IsZero() && !createdAt.Before(lt) { // created_at < lt
				continue
			}
			filtered = append(filtered, e)
		}

		page := []utils.Dict{}
		if offset < len(filtered) {
			end := offset + limit
			if end > len(filtered) {
				end = len(filtered)
			}
			page = filtered[offset:end]
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"events": page,
			"count":  len(page),
			"total":  len(filtered),
		})
	}
}

// --- realistic event fixtures -------------------------------------------------

// realisticAuditEvent returns an event shaped like a real Sublime Security
// audit log entry: id/type/created_at plus the created_by user object and the
// data.request details of the action recorded. All identifiers are fake.
//
// The field set was verified against live API responses from
// GET /v0/audit-log/events: the top-level keys are exactly
// {id, type, created_at, created_by, data} (the documented `additional_data` is
// absent from real request-derived events), data.request carries exactly
// {id, path, method, query, body, authentication_method, ip, user_agent}, and
// `ip` is a bare address with no CIDR suffix.
func realisticAuditEvent(id, eventType, createdAt string) utils.Dict {
	return utils.Dict{
		"id":         id,
		"type":       eventType,
		"created_at": createdAt,
		"created_by": utils.Dict{
			"id":                      "11111111-1111-1111-1111-111111111111",
			"email_address":           "analyst@example.com",
			"first_name":              "Alex",
			"last_name":               "Analyst",
			"phone_number":            nil,
			"role":                    "admin",
			"active":                  true,
			"is_enrolled":             true,
			"access_restricted":       false,
			"google_oauth_user_id":    "",
			"microsoft_oauth_user_id": "",
			"created_at":              "2026-01-01T00:00:00Z",
			"updated_at":              "2026-01-02T00:00:00Z",
		},
		"data": utils.Dict{
			"request": utils.Dict{
				"id":                    "22222222-2222-2222-2222-222222222222",
				"method":                "POST",
				"path":                  "/v1/messages/groups/33333333-3333-3333-3333-333333333333/trash",
				"user_agent":            "Mozilla/5.0 (X11; Linux x86_64) Example/1.0",
				"ip":                    "203.0.113.10",
				"authentication_method": "user_session",
				"query":                 utils.Dict{},
				"body":                  "",
			},
			"message": utils.Dict{
				"id":          "44444444-4444-4444-4444-444444444444",
				"external_id": "55555555-5555-5555-5555-555555555555",
			},
		},
	}
}

// futureTS returns an RFC3339Nano timestamp d into the future. The adapter
// only ships events created after it started, and startMockAdapter runs the
// adapter's clock ahead of wall time once it has started, so future-dated
// fixtures land inside the polling window deterministically.
func futureTS(d time.Duration) string {
	return time.Now().Add(d).UTC().Format(time.RFC3339Nano)
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func shippedIDs(msgs []*protocol.DataMessage) map[string]int {
	out := map[string]int{}
	for _, m := range msgs {
		id, _ := m.JsonPayload["id"].(string)
		out[id]++
	}
	return out
}

// startMockAdapter wires the adapter to the mock server with the capture sink
// and a fast poll interval.
func startMockAdapter(t *testing.T, serverURL, apiKey string, sink uspSink) (*SublimeAdapter, chan struct{}) {
	t.Helper()
	conf := SublimeConfig{
		ClientOptions: testClientOptions(t),
		ApiKey:        apiKey,
		BaseURL:       serverURL,
		PollInterval:  25 * time.Millisecond,
	}
	clock := &testClock{}
	adapter, chStopped, err := newSublimeAdapter(t.Context(), conf, sink, clock.Now)
	require.NoError(t, err)
	// The adapter captured its start time at construction; from here on its
	// clock runs ahead so that futureTS fixtures are inside the poll window.
	clock.Advance(fixtureHorizon)
	return adapter, chStopped
}

// fixtureHorizon is how far ahead of wall time the adapter's clock runs after
// start: futureTS fixtures must stay below it.
const fixtureHorizon = 3 * time.Hour

// testClock is wall time plus an adjustable offset.
type testClock struct {
	mu     sync.Mutex
	offset time.Duration
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Add(c.offset)
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offset += d
}

// --- tests --------------------------------------------------------------------

// TestMockAuditLogEndToEnd drives the adapter against the mock API and asserts
// the exact messages shipped: ingestion-time TimestampMs, empty EventType (the
// adapter does not tag audit events), and the payload preserved verbatim. It
// also pins the request contract: GET on /v0/audit-log/events with the Bearer
// API key, Accept: application/json and limit=500.
func TestMockAuditLogEndToEnd(t *testing.T) {
	const apiKey = "sublime-test-api-key-000000000000"

	mock := newMockSublime(apiKey)
	want := []utils.Dict{
		realisticAuditEvent("aaaaaaaa-1111-1111-1111-111111111111", "message_group.quarantine", futureTS(1*time.Hour)),
		realisticAuditEvent("aaaaaaaa-2222-2222-2222-222222222222", "message.view_contents", futureTS(1*time.Hour+1*time.Minute)),
		realisticAuditEvent("aaaaaaaa-3333-3333-3333-333333333333", "message.access_justification", futureTS(1*time.Hour+2*time.Minute)),
	}
	mock.setEvents(want)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	before := uint64(time.Now().UnixMilli())
	adapter, _ := startMockAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond, "expected all 3 audit events to ship")
	after := uint64(time.Now().UnixMilli())

	// Wait for at least one more full poll, then verify nothing re-ships.
	polled := mock.requestCount()
	require.Eventually(t, func() bool { return mock.requestCount() >= polled+2 },
		5*time.Second, 10*time.Millisecond, "expected further polls to happen")
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 25*time.Millisecond, "events were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		// The adapter ships audit events without an EventType tag.
		assert.Equal(t, "", msg.EventType)
		// The adapter stamps ingestion time, not the event's created_at.
		assert.GreaterOrEqual(t, msg.TimestampMs, before, "TimestampMs should be ingestion time")
		assert.LessOrEqual(t, msg.TimestampMs, after, "TimestampMs should be ingestion time")
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "event %s was not shipped", id)
		// The payload is shipped verbatim -- nested objects included.
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Sublime audit event")
	}

	// Request contract.
	mock.mu.Lock()
	defer mock.mu.Unlock()
	assert.Equal(t, http.MethodGet, mock.lastMethod)
	assert.Equal(t, logsPath, mock.lastPath)
	assert.Equal(t, "application/json", mock.lastAccept)
	assert.Equal(t, pageLimit, mock.lastLimit, "the adapter should request the full page limit")
	assert.Zero(t, mock.authFailures, "the Bearer API key must be sent on every request")
}

// TestMockNewEventMidRunShipsOnce verifies an audit event that appears while
// the adapter is running ships exactly once, and already-shipped events are
// never re-sent.
func TestMockNewEventMidRunShipsOnce(t *testing.T) {
	const apiKey = "sublime-test-api-key-000000000000"

	mock := newMockSublime(apiKey)
	mock.setEvents([]utils.Dict{
		realisticAuditEvent("bbbbbbbb-1111-1111-1111-111111111111", "message_group.trash", futureTS(1*time.Hour)),
		realisticAuditEvent("bbbbbbbb-2222-2222-2222-222222222222", "message.flagged", futureTS(1*time.Hour+1*time.Minute)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 10*time.Millisecond)

	// A new event occurs mid-run: it is created "now" on the adapter's clock,
	// later than everything already seen.
	mock.appendEvent(realisticAuditEvent(
		"bbbbbbbb-3333-3333-3333-333333333333", "message_group.quarantine", futureTS(fixtureHorizon)))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond, "the new event should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 25*time.Millisecond)

	assert.Equal(t, map[string]int{
		"bbbbbbbb-1111-1111-1111-111111111111": 1,
		"bbbbbbbb-2222-2222-2222-222222222222": 1,
		"bbbbbbbb-3333-3333-3333-333333333333": 1,
	}, shippedIDs(sink.snapshot()), "every event must ship exactly once")
}

// TestMockPaginationFullDataset verifies a dataset larger than one page
// (pageLimit=500) is walked completely via offset pagination and every event
// ships exactly once.
func TestMockPaginationFullDataset(t *testing.T) {
	const apiKey = "sublime-test-api-key-000000000000"
	const total = 750 // 500 + 250 => two pages

	mock := newMockSublime(apiKey)
	events := make([]utils.Dict, total)
	for i := 0; i < total; i++ {
		events[i] = realisticAuditEvent(
			fmt.Sprintf("cccccccc-0000-0000-0000-%012d", i),
			"message.view_contents",
			futureTS(time.Hour+time.Duration(i)*time.Millisecond))
	}
	mock.setEvents(events)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 25*time.Millisecond, "all paginated events should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 25*time.Millisecond)

	ids := shippedIDs(sink.snapshot())
	assert.Len(t, ids, total, "every distinct event should ship")
	for id, n := range ids {
		assert.Equalf(t, 1, n, "event %s shipped %d times", id, n)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	assert.GreaterOrEqual(t, mock.maxOffsetSeen, pageLimit,
		"the adapter should have requested the second page (offset=%d)", pageLimit)
}

// TestMockEventsBeforeStartDoNotShip pins the adapter's startup window: it only
// ships audit events whose created_at is after the adapter started; the
// pre-existing backlog is not replayed.
func TestMockEventsBeforeStartDoNotShip(t *testing.T) {
	const apiKey = "sublime-test-api-key-000000000000"

	mock := newMockSublime(apiKey)
	mock.setEvents([]utils.Dict{
		realisticAuditEvent("dddddddd-1111-1111-1111-111111111111", "message.view_contents", time.Now().Add(-2*time.Hour).UTC().Format(time.RFC3339Nano)),
		realisticAuditEvent("dddddddd-2222-2222-2222-222222222222", "message_group.trash", time.Now().Add(-1*time.Hour).UTC().Format(time.RFC3339Nano)),
		realisticAuditEvent("dddddddd-3333-3333-3333-333333333333", "message_group.quarantine", futureTS(1*time.Hour)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 10*time.Millisecond, "the post-start event should ship")
	// Let several more polls happen; the historical events must stay unshipped.
	polled := mock.requestCount()
	require.Eventually(t, func() bool { return mock.requestCount() >= polled+2 },
		5*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return sink.count() != 1 },
		300*time.Millisecond, 25*time.Millisecond, "historical events must not be replayed")

	assert.Equal(t, map[string]int{"dddddddd-3333-3333-3333-333333333333": 1},
		shippedIDs(sink.snapshot()))
}

// TestMockHighVolumeUsesTimeFilter is the regression test for the reported
// `context deadline exceeded` on busy tenants. A large backlog of events that
// predate the adapter's start must NOT be walked page by page: the adapter
// constrains each poll with created_at[gte], so the server returns an empty
// window and pagination never advances past offset 0. Before the fix the
// adapter re-scanned the entire backlog (offset 0, 500, ... hundreds of pages)
// on every poll, which is what eventually blew the HTTP timeout.
func TestMockHighVolumeUsesTimeFilter(t *testing.T) {
	const apiKey = "sublime-test-api-key-000000000000"
	const backlog = 100000 // far more than one page; all created before start

	mock := newMockSublime(apiKey)
	events := make([]utils.Dict, backlog)
	base := time.Now().Add(-2 * time.Hour)
	for i := 0; i < backlog; i++ {
		events[i] = realisticAuditEvent(
			fmt.Sprintf("ffffffff-0000-0000-0000-%012d", i),
			"message.view_contents",
			base.Add(time.Duration(i)*time.Millisecond).UTC().Format(time.RFC3339Nano))
	}
	mock.setEvents(events)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	// Let several full polls happen against the large backlog.
	require.Eventually(t, func() bool { return mock.requestCount() >= 4 },
		5*time.Second, 10*time.Millisecond, "the adapter should keep polling")

	// The historical backlog is before the start watermark, so nothing ships...
	require.Never(t, func() bool { return sink.count() != 0 },
		300*time.Millisecond, 25*time.Millisecond, "pre-start backlog must not ship")

	// ...and, crucially, the adapter never deep-paginates the backlog: with the
	// created_at[gte] filter the recent window is empty, so every poll is a
	// single offset=0 request.
	assert.True(t, mock.sawTimeFilter(), "the adapter must send a created_at[gte] filter")
	assert.Equal(t, 0, mock.maxOffset(),
		"the adapter must not walk the backlog by offset; it should stay at offset 0")
}

// TestBaseURLDefaultBackfilled is the regression test for the reported
// `unsupported protocol scheme ""`. The production runner builds the adapter
// without calling Validate(), so the constructor itself must backfill the
// North America default base URL when none is configured.
func TestBaseURLDefaultBackfilled(t *testing.T) {
	conf := SublimeConfig{
		ClientOptions: testClientOptions(t),
		ApiKey:        "sublime-test-api-key-000000000000",
		// BaseURL intentionally left empty, as in the failing customer config.
	}
	sink := &captureSink{}
	adapter, _, err := newSublimeAdapter(t.Context(), conf, sink, nil)
	require.NoError(t, err)
	// Close immediately: the poll loop waits PollInterval before its first
	// request, so no network call is made against the real default host.
	defer adapter.Close()

	assert.Equal(t, defaultBaseURL, adapter.conf.BaseURL,
		"the constructor must backfill the default base URL when none is set")
}

// TestMockBadAPIKeyShipsNothing pins the adapter's behavior on auth failure:
// a 401 from the API is reported via OnError and nothing ships, but the
// adapter does not stop -- it keeps polling on its interval.
func TestMockBadAPIKeyShipsNothing(t *testing.T) {
	mock := newMockSublime("the-correct-api-key")
	mock.setEvents([]utils.Dict{
		realisticAuditEvent("eeeeeeee-1111-1111-1111-111111111111", "message.flagged", futureTS(1*time.Hour)),
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	var mu sync.Mutex
	errorCount := 0
	opts := testClientOptions(t)
	opts.OnError = func(err error) {
		t.Logf("ERR: %v", err)
		mu.Lock()
		errorCount++
		mu.Unlock()
	}

	sink := &captureSink{}
	conf := SublimeConfig{
		ClientOptions: opts,
		ApiKey:        "the-wrong-api-key",
		BaseURL:       server.URL,
		PollInterval:  25 * time.Millisecond,
	}
	adapter, chStopped, err := newSublimeAdapter(t.Context(), conf, sink, nil)
	require.NoError(t, err)
	defer adapter.Close()

	// The adapter keeps polling despite the 401s (it does not treat auth
	// failure as fatal) ...
	require.Eventually(t, func() bool { return mock.authFailureCount() >= 3 },
		5*time.Second, 10*time.Millisecond, "the adapter should keep polling on auth failure")
	select {
	case <-chStopped:
		t.Fatal("the adapter is not expected to stop on auth failure")
	default:
	}

	// ... reports the failures, and ships nothing.
	mu.Lock()
	assert.GreaterOrEqual(t, errorCount, 1, "auth failures must be reported via OnError")
	mu.Unlock()
	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
}

// shippedInOrder reports whether the shipped events are in chronological
// created_at order.
func shippedInOrder(t *testing.T, msgs []*protocol.DataMessage) bool {
	t.Helper()
	var prev time.Time
	for _, m := range msgs {
		at, err := time.Parse(time.RFC3339Nano, fmt.Sprint(m.JsonPayload["created_at"]))
		require.NoError(t, err)
		if at.Before(prev) {
			return false
		}
		prev = at
	}
	return true
}

// TestMockTransientPageFailureLosesNothing is the regression test for events
// being dropped when one page of a multi-page poll fails. Previously the pages
// read before the failure were discarded while the watermark still advanced
// past them, so with the API's newest-first ordering a single transient error
// lost the whole poll. Now the window is only committed once read completely,
// and the next poll retries it.
func TestMockTransientPageFailureLosesNothing(t *testing.T) {
	const apiKey = "sublime-test-api-key-000000000000"
	const total = 1200 // three pages

	mock := newMockSublime(apiKey)
	events := make([]utils.Dict, total)
	for i := 0; i < total; i++ {
		events[i] = realisticAuditEvent(
			fmt.Sprintf("12121212-0000-0000-0000-%012d", i),
			"message.view_contents",
			futureTS(time.Hour+time.Duration(i)*time.Millisecond))
	}
	mock.setEvents(events)

	// The second page fails exactly once.
	var failed int32
	var failMu sync.Mutex
	mock.setFail(func(r *http.Request) bool {
		failMu.Lock()
		defer failMu.Unlock()
		if failed == 0 && r.URL.Query().Get("offset") == strconv.Itoa(pageLimit) {
			failed++
			return true
		}
		return false
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 25*time.Millisecond, "every event must ship despite the failed page")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 25*time.Millisecond)

	failMu.Lock()
	assert.Equal(t, int32(1), failed, "the injected failure must have happened")
	failMu.Unlock()
	msgs := sink.snapshot()
	for id, n := range shippedIDs(msgs) {
		assert.Equalf(t, 1, n, "event %s shipped %d times", id, n)
	}
	assert.True(t, shippedInOrder(t, msgs), "events must ship oldest-first")
}

// TestMockOversizedWindowIsSplit covers a window holding more events than one
// window may paginate (maxPagesPerWindow pages). Previously pagination stopped
// at the cap and the watermark jumped to the newest event, skipping everything
// older that had not been read. Now the window is halved until it fits, and
// every event ships exactly once, in order.
func TestMockOversizedWindowIsSplit(t *testing.T) {
	const apiKey = "sublime-test-api-key-000000000000"
	total := maxPagesPerWindow*pageLimit + 5000

	mock := newMockSublime(apiKey)
	events := make([]utils.Dict, total)
	for i := 0; i < total; i++ {
		events[i] = realisticAuditEvent(
			fmt.Sprintf("34343434-0000-0000-0000-%012d", i),
			"message.view_contents",
			futureTS(time.Hour+time.Duration(i)*10*time.Millisecond))
	}
	mock.setEvents(events)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, apiKey, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		60*time.Second, 50*time.Millisecond, "every event of the oversized window must ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 25*time.Millisecond)

	msgs := sink.snapshot()
	ids := shippedIDs(msgs)
	assert.Len(t, ids, total)
	for id, n := range ids {
		if n != 1 {
			t.Fatalf("event %s shipped %d times", id, n)
		}
	}
	assert.True(t, shippedInOrder(t, msgs), "events must ship oldest-first")

	mock.mu.Lock()
	defer mock.mu.Unlock()
	assert.True(t, mock.sawLT, "the adapter must bound every window with created_at[lt]")
	assert.Less(t, mock.maxOffsetSeen, maxPagesPerWindow*pageLimit,
		"no window may be paginated past the per-window cap")
}
