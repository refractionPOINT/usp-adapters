package usp_cortex_xsoar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock Cortex XSOAR API that
// reproduces the real contract: API-key auth (standard and advanced), the
// /incidents/search filter/sort/pagination, and the {"data":[...],"total":N}
// envelope. It captures the exact messages shipped so their content -- event
// type, timestamp and verbatim payload -- can be asserted.

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

// --- mock Cortex XSOAR API --------------------------------------------------

var modifiedClauseRe = regexp.MustCompile(`modified:>="([^"]+)"`)

// mockXSOAR is an in-memory stand-in for the Cortex XSOAR REST API. It honours
// the /incidents/search contract: authenticates the way the real API does,
// filters the dataset by the modified-time clause in the query, sorts ascending
// on modified, paginates by 0-based page/size, and returns {"data":[...],"total":N}.
type mockXSOAR struct {
	mu        sync.Mutex
	apiKey    string
	apiKeyID  string
	advanced  bool
	incidents []utils.Dict
	requests  int
	lastPath  string
	lastAuth  string // x-xdr-auth-id of the most recent request
}

func newMockXSOAR(apiKey string) *mockXSOAR {
	return &mockXSOAR{apiKey: apiKey}
}

func (m *mockXSOAR) setIncidents(incs []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incidents = incs
}

// appendIncident adds (or, when an incident with the same id exists, replaces) an
// incident, as the real API would when an incident is created or updated.
func (m *mockXSOAR) appendIncident(inc utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := inc.FindOneString("id")
	for i := range m.incidents {
		if m.incidents[i].FindOneString("id") == id {
			m.incidents[i] = inc
			return
		}
	}
	m.incidents = append(m.incidents, inc)
}

func (m *mockXSOAR) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func (m *mockXSOAR) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.requests++
		m.lastPath = r.URL.Path
		m.lastAuth = r.Header.Get("x-xdr-auth-id")
		m.mu.Unlock()

		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/incidents/search") {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if !m.authOK(r) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"Authorization failed"}`))
			return
		}

		body := decodeRequestBody(t, r)
		filter, _ := body["filter"].(map[string]interface{})
		query, _ := filter["query"].(string)
		page := 0
		if v, ok := filter["page"].(float64); ok {
			page = int(v)
		}
		size := defaultPageSize
		if v, ok := filter["size"].(float64); ok && v > 0 {
			size = int(v)
		}

		matched := m.matching(query)

		var pageItems []utils.Dict
		if start := page * size; start < len(matched) {
			end := start + size
			if end > len(matched) {
				end = len(matched)
			}
			pageItems = matched[start:end]
		}
		if pageItems == nil {
			pageItems = []utils.Dict{}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data":  pageItems,
			"total": len(matched),
		})
	}
}

// authOK reproduces XSOAR's authentication checks for the configured key type.
func (m *mockXSOAR) authOK(r *http.Request) bool {
	if m.advanced {
		ts := r.Header.Get("x-xdr-timestamp")
		nonce := r.Header.Get("x-xdr-nonce")
		authID := r.Header.Get("x-xdr-auth-id")
		got := r.Header.Get("Authorization")
		if ts == "" || nonce == "" || authID != m.apiKeyID {
			return false
		}
		return got == signAdvanced(m.apiKey, nonce, ts)
	}
	return r.Header.Get("Authorization") == m.apiKey
}

// matching returns the incidents satisfying the query's modified clause, sorted
// ascending by modified -- exactly what the adapter requests.
func (m *mockXSOAR) matching(query string) []utils.Dict {
	m.mu.Lock()
	defer m.mu.Unlock()

	var since time.Time
	if mm := modifiedClauseRe.FindStringSubmatch(query); mm != nil {
		since, _ = parseTimestamp(mm[1])
	}

	out := make([]utils.Dict, 0, len(m.incidents))
	for _, inc := range m.incidents {
		mt, ok := incidentModified(inc)
		if !ok {
			continue
		}
		if !since.IsZero() && mt.Before(since) {
			continue
		}
		out = append(out, inc)
	}
	sort.SliceStable(out, func(i, j int) bool {
		ti, _ := incidentModified(out[i])
		tj, _ := incidentModified(out[j])
		return ti.Before(tj)
	})
	return out
}

// --- realistic fixture ------------------------------------------------------

// realisticIncident returns a record shaped like a real Cortex XSOAR incident:
// the documented top-level fields with numeric severity/status, RFC3339 times
// (fractional seconds and a Go zero-time for an unset "closed"), a labels array,
// the capitalized CustomFields/ShardID keys, and a large integer autime to prove
// integer precision is preserved.
func realisticIncident(id, modified string) utils.Dict {
	return utils.Dict{
		"id":              id,
		"version":         7,
		"name":            "Suspicious login - case " + id,
		"type":            "Phishing",
		"severity":        3,
		"status":          1,
		"owner":           "analyst1",
		"created":         "2026-05-21T13:45:30.162034Z",
		"modified":        modified,
		"occurred":        "2026-05-21T13:45:30.162034Z",
		"closed":          "0001-01-01T00:00:00Z",
		"category":        "",
		"labels":          []interface{}{utils.Dict{"type": "Email/from", "value": "attacker@evil.example"}},
		"CustomFields":    utils.Dict{"sourceip": "203.0.113.10", "verdict": "malicious"},
		"rawJSON":         "",
		"sourceBrand":     "Mail Listener v2",
		"sourceInstance":  "EWS_O365_instance",
		"investigationId": id,
		"playbookId":      "Phishing Investigation - Generic v2",
		"autime":          1601389784162034000,
		"ShardID":         0,
	}
}

// payload views a shipped message's JSON payload as a utils.Dict so the same
// field helpers used in the adapter can be used in assertions.
func payload(msg *protocol.DataMessage) utils.Dict {
	return utils.Dict(msg.JsonPayload)
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// decodeRequestBody decodes a JSON request body. It runs inside httptest handler
// goroutines, so it must not use require (whose FailNow/Goexit is only valid on
// the test goroutine); assert reports failures safely from any goroutine.
func decodeRequestBody(t *testing.T, r *http.Request) map[string]interface{} {
	t.Helper()
	m := map[string]interface{}{}
	body, err := io.ReadAll(r.Body)
	if !assert.NoError(t, err) {
		return m
	}
	assert.NoError(t, json.Unmarshal(body, &m))
	return m
}

// baseConf returns a config pointed at the mock server, with a long lookback so
// fixtures dated in 2026 are inside the first poll's window, and a fast poll.
func baseConf(t *testing.T, serverURL, apiKey string) XSOARConfig {
	return XSOARConfig{
		ClientOptions:   testClientOptions(t),
		URL:             serverURL,
		APIKey:          apiKey,
		InitialLookback: 3650 * 24 * time.Hour,
		PollInterval:    40 * time.Millisecond,
	}
}

// --- tests ------------------------------------------------------------------

// TestMockSearchIncidentsEndToEnd drives the adapter against the mock API and
// asserts the exact events shipped: event type, timestamp parsed from "modified",
// and the payload preserved verbatim (nested objects and arrays included).
func TestMockSearchIncidentsEndToEnd(t *testing.T) {
	const apiKey = "xsoar-api-key-xyz"

	mock := newMockXSOAR(apiKey)
	want := []utils.Dict{
		realisticIncident("101", "2026-05-21T13:45:30.162034Z"),
		realisticIncident("102", "2026-05-21T13:50:05.001Z"),
		realisticIncident("103", "2026-05-21T14:02:11.9Z"),
	}
	mock.setIncidents(want)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newXSOARAdapter(ctx, baseConf(t, server.URL, apiKey), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 incidents to ship")
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 30*time.Millisecond, "incidents were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		assert.Equal(t, "incident", msg.EventType)
		require.NotNil(t, msg.JsonPayload)
		byID[payload(msg).FindOneString("id")] = msg
	}
	require.Len(t, byID, 3)

	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "incident %s was not shipped", id)

		// The event time is parsed from the incident's modified field.
		ts, ok := parseTimestamp(src["modified"].(string))
		require.True(t, ok)
		assert.Equal(t, uint64(ts.UnixMilli()), msg.TimestampMs,
			"event time should come from the incident's modified field")

		// The payload is shipped verbatim -- nested objects and arrays included.
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original XSOAR incident")
	}
}

// TestMockNewIncidentShippedOnce verifies an incident that appears mid-run is
// shipped exactly once, and already-shipped incidents are never re-sent.
func TestMockNewIncidentShippedOnce(t *testing.T) {
	const apiKey = "k"

	mock := newMockXSOAR(apiKey)
	mock.setIncidents([]utils.Dict{
		realisticIncident("a", "2026-05-21T10:00:00Z"),
		realisticIncident("b", "2026-05-21T10:01:00Z"),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newXSOARAdapter(ctx, baseConf(t, server.URL, apiKey), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)

	// A new incident appears, with a modified time after the current cursor.
	mock.appendIncident(realisticIncident("c", "2026-05-21T10:05:00Z"))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "the new incident should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 30*time.Millisecond)

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[payload(msg).FindOneString("id")]++
	}
	assert.Equal(t, map[string]int{"a": 1, "b": 1, "c": 1}, shippedPerID,
		"every incident must ship exactly once")
}

// TestMockUpdatedIncidentReshipped verifies that when an incident is updated (its
// modified time advances) the new version is re-collected, while the unchanged
// version is not re-shipped. Deduplication is on id+modified.
func TestMockUpdatedIncidentReshipped(t *testing.T) {
	const apiKey = "k"

	mock := newMockXSOAR(apiKey)
	mock.setIncidents([]utils.Dict{
		realisticIncident("evt-1", "2026-05-21T10:00:00Z"),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newXSOARAdapter(ctx, baseConf(t, server.URL, apiKey), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond)

	// The same incident is updated: same id, newer modified time.
	updated := realisticIncident("evt-1", "2026-05-21T10:07:30Z")
	updated["status"] = 2 // closed
	mock.appendIncident(updated)

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "the updated version should re-ship")
	require.Never(t, func() bool { return sink.count() > 2 },
		300*time.Millisecond, 30*time.Millisecond, "only the two distinct versions should ship")

	versions := sink.snapshot()
	require.Len(t, versions, 2)
	// Both carry the same id; the second reflects the update.
	for _, msg := range versions {
		assert.Equal(t, "evt-1", payload(msg).FindOneString("id"))
	}
	statuses := map[uint64]bool{}
	for _, msg := range versions {
		if v, ok := payload(msg).GetInt("status"); ok {
			statuses[v] = true
		}
	}
	assert.True(t, statuses[1] && statuses[2], "should have shipped both the open and the closed version, got %v", statuses)
}

// TestMockPaginationFullDataset verifies a dataset larger than one page is walked
// completely and every incident is shipped exactly once.
func TestMockPaginationFullDataset(t *testing.T) {
	const apiKey = "k"
	const total = 250

	mock := newMockXSOAR(apiKey)
	records := make([]utils.Dict, total)
	for i := 0; i < total; i++ {
		records[i] = realisticIncident(
			fmt.Sprintf("inc-%03d", i),
			time.Date(2026, 5, 21, 0, 0, 0, i*int(time.Millisecond), time.UTC).Format(time.RFC3339Nano))
	}
	mock.setIncidents(records)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conf := baseConf(t, server.URL, apiKey)
	conf.PageSize = 100 // 250 incidents => 3 pages
	conf.PollInterval = 50 * time.Millisecond
	adapter, _, err := newXSOARAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 25*time.Millisecond, "all paginated incidents should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 30*time.Millisecond)

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		ids[payload(msg).FindOneString("id")] = true
	}
	assert.Len(t, ids, total, "every distinct incident should be shipped once")
}

// TestMockCursorWindowExcludesOld verifies the modified-time cursor scopes the
// first poll: an incident older than initial_lookback is not collected, a recent
// one is.
func TestMockCursorWindowExcludesOld(t *testing.T) {
	const apiKey = "k"

	mock := newMockXSOAR(apiKey)
	now := time.Now().UTC()
	mock.setIncidents([]utils.Dict{
		realisticIncident("old", now.Add(-48*time.Hour).Format(time.RFC3339)),
		realisticIncident("recent", now.Add(-10*time.Minute).Format(time.RFC3339)),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := baseConf(t, server.URL, apiKey)
	conf.InitialLookback = 1 * time.Hour // old (-48h) is outside, recent (-10m) is inside
	adapter, _, err := newXSOARAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond, "only the recent incident should ship")
	require.Never(t, func() bool { return sink.count() != 1 },
		300*time.Millisecond, 30*time.Millisecond)

	assert.Equal(t, "recent", payload(sink.snapshot()[0]).FindOneString("id"))
}

// TestMockAdvancedKeyAuth verifies the advanced (signed) API key scheme: the
// adapter sends the x-xdr-* headers and a valid SHA-256 signature the mock
// accepts, and incidents flow.
func TestMockAdvancedKeyAuth(t *testing.T) {
	const apiKey = "advanced-secret"
	const keyID = "42"

	mock := newMockXSOAR(apiKey)
	mock.advanced = true
	mock.apiKeyID = keyID
	mock.setIncidents([]utils.Dict{
		realisticIncident("x", "2026-05-21T10:00:00Z"),
		realisticIncident("y", "2026-05-21T10:01:00Z"),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := baseConf(t, server.URL, apiKey)
	conf.APIKeyID = keyID
	conf.Advanced = true
	adapter, _, err := newXSOARAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "advanced-key requests should authenticate and ship")
}

// TestMockXSOAR8PathAndAuthID verifies that for api_version 8 the adapter targets
// the /xsoar/public/v1 prefix and sends the x-xdr-auth-id header.
func TestMockXSOAR8PathAndAuthID(t *testing.T) {
	const apiKey = "k8"
	const keyID = "99"

	mock := newMockXSOAR(apiKey)
	mock.setIncidents([]utils.Dict{realisticIncident("v8", "2026-05-21T10:00:00Z")})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := baseConf(t, server.URL, apiKey)
	conf.APIVersion = "8"
	conf.APIKeyID = keyID
	adapter, _, err := newXSOARAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond)

	mock.mu.Lock()
	defer mock.mu.Unlock()
	assert.Equal(t, "/xsoar/public/v1/incidents/search", mock.lastPath,
		"XSOAR 8 requests must target the /xsoar/public/v1 prefix")
	assert.Equal(t, keyID, mock.lastAuth, "XSOAR 8 requests must carry x-xdr-auth-id")
}

// TestMockPaginationNotTruncatedByMalformedRecord verifies that a malformed
// (non-object) record on an otherwise-full page does not make the page look
// short and end the walk early: pagination continues and every valid incident on
// later pages still ships. Regression test for paginating on the raw page length.
func TestMockPaginationNotTruncatedByMalformedRecord(t *testing.T) {
	const apiKey = "k"
	// page_size 3. Page 0 carries 3 raw records but one is junk (2 kept); without
	// raw-length pagination the 2 kept would look like a short page and stop the
	// walk, dropping pages 1+.
	inc := func(id, modified string) string { return mustJSON(t, realisticIncident(id, modified)) }
	pages := map[int]string{
		0: fmt.Sprintf(`{"data":[%s,"junk-not-an-object",%s],"total":5}`,
			inc("a", "2026-05-21T10:00:00Z"), inc("b", "2026-05-21T10:01:00Z")),
		1: fmt.Sprintf(`{"data":[%s,%s,%s],"total":5}`,
			inc("c", "2026-05-21T10:02:00Z"), inc("d", "2026-05-21T10:03:00Z"), inc("e", "2026-05-21T10:04:00Z")),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != apiKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		body := decodeRequestBody(t, r)
		filter, _ := body["filter"].(map[string]interface{})
		page := 0
		if v, ok := filter["page"].(float64); ok {
			page = int(v)
		}
		w.Header().Set("Content-Type", "application/json")
		if resp, ok := pages[page]; ok {
			_, _ = io.WriteString(w, resp)
			return
		}
		_, _ = io.WriteString(w, `{"data":[],"total":5}`)
	}))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := baseConf(t, server.URL, apiKey)
	conf.PageSize = 3
	adapter, _, err := newXSOARAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// All 5 valid incidents (across both pages) must ship; the junk record on the
	// full first page must not have ended the walk.
	require.Eventually(t, func() bool { return sink.count() == 5 },
		5*time.Second, 20*time.Millisecond, "all valid incidents across pages should ship despite a malformed record")
	require.Never(t, func() bool { return sink.count() != 5 },
		300*time.Millisecond, 30*time.Millisecond)

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		ids[payload(msg).FindOneString("id")] = true
	}
	assert.Equal(t, map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true}, ids)
}

// TestMockFutureDatedIncidentDoesNotStallCursor verifies the cursor is capped at
// "now": a single incident with a far-future "modified" must not jump the cursor
// ahead and starve collection of normal incidents that arrive afterward.
func TestMockFutureDatedIncidentDoesNotStallCursor(t *testing.T) {
	const apiKey = "k"

	mock := newMockXSOAR(apiKey)
	// A bad/clock-skewed incident dated a year in the future.
	mock.setIncidents([]utils.Dict{
		realisticIncident("future", time.Now().UTC().Add(365*24*time.Hour).Format(time.RFC3339)),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newXSOARAdapter(ctx, baseConf(t, server.URL, apiKey), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond, "the future-dated incident ships once")

	// A normal incident arrives shortly after, dated slightly ahead of now (but
	// far before the future one). With the cursor capped at now it is collected;
	// without the cap the cursor would sit a year ahead and this would be skipped.
	mock.appendIncident(realisticIncident("normal", time.Now().UTC().Add(10*time.Minute).Format(time.RFC3339)))

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "a normal incident must still be collected after a future-dated one")
	require.Never(t, func() bool { return sink.count() > 2 },
		300*time.Millisecond, 30*time.Millisecond)

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		ids[payload(msg).FindOneString("id")] = true
	}
	assert.Equal(t, map[string]bool{"future": true, "normal": true}, ids)
}

// TestMockRejectsBadKey verifies the adapter warns, ships nothing, and does NOT
// stop when the API rejects the key. A rejected key is a source-side problem a
// restart cannot fix, so the adapter must stay up and retry (an operator fixing
// the key should see it recover on its own) rather than letting the cloud-sensor
// host tear it down and eventually disable it.
func TestMockRejectsBadKey(t *testing.T) {
	mock := newMockXSOAR("correct-key")
	mock.setIncidents([]utils.Dict{realisticIncident("a", "2026-05-21T10:00:00Z")})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var warnings, fatalErrors atomic.Int32
	opts := testClientOptions(t)
	opts.OnWarning = func(msg string) { warnings.Add(1); t.Logf("WRN: %s", msg) }
	opts.OnError = func(err error) { fatalErrors.Add(1); t.Logf("ERR: %v", err) }

	conf := baseConf(t, server.URL, "wrong-key")
	conf.ClientOptions = opts
	conf.PollInterval = 50 * time.Millisecond
	adapter, chStopped, err := newXSOARAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return warnings.Load() > 0 },
		3*time.Second, 20*time.Millisecond, "expected a warning when the key is rejected")

	select {
	case <-chStopped:
		t.Fatal("adapter must not stop when the API rejects the key")
	case <-time.After(300 * time.Millisecond):
	}
	assert.False(t, adapter.doStop.IsSet(), "doStop must not be set on a rejected key")
	assert.Equal(t, int32(0), fatalErrors.Load(), "a rejected key must warn, not fatally error")
	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
	assert.GreaterOrEqual(t, mock.requestCount(), 1, "the adapter should have called the API")
}
