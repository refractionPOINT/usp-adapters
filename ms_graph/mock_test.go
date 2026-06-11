package usp_ms_graph

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock of the two
// Microsoft endpoints it talks to:
//
//   - the Azure AD OAuth2 token endpoint
//     (login.microsoftonline.com/<tenant>/oauth2/v2.0/token), which it hits
//     with a client_credentials form exchange before every poll, and
//   - a Microsoft Graph resource endpoint (graph.microsoft.com/v1.0/<url>),
//     which it polls with a Bearer token and an OData
//     `$filter=createdDateTime ge <since>` query, expecting the standard
//     Graph envelope {"@odata.context": ..., "value": [...]}.
//
// All fixture identifiers are deliberately fake (example.com, all-1s UUIDs,
// made-up tokens).

const (
	testTenantID     = "11111111-1111-1111-1111-111111111111"
	testClientID     = "22222222-2222-2222-2222-222222222222"
	testClientSecret = "fake-test-client-secret-0001"
	testAccessToken  = "fake-test-access-token-0001"
	testResource     = "security/alerts_v2"
)

// --- in-memory USP sink -------------------------------------------------------

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

// --- mock Microsoft endpoints ---------------------------------------------------

// graphRecord pairs a record's payload with its parsed createdDateTime so the
// mock can evaluate the adapter's `createdDateTime ge <since>` filter.
type graphRecord struct {
	t       time.Time
	payload map[string]interface{}
}

// mockMsGraph is an in-memory stand-in for the Azure AD token endpoint and a
// Microsoft Graph list endpoint. The token endpoint validates the
// client_credentials exchange; the Graph endpoint validates the Bearer token,
// honours the adapter's createdDateTime filter (inclusive `ge`, like OData),
// returns records in ascending createdDateTime order inside the standard
// {"value": [...]} envelope, and advertises @odata.nextLink when pageSize
// truncates the result set.
type mockMsGraph struct {
	mu      sync.Mutex
	baseURL string // set once the httptest server is up; used for nextLink

	records  []graphRecord // kept in ascending createdDateTime order
	pageSize int           // 0 = unlimited

	graphFailStatus    int // when non-zero, the graph endpoint fails with this
	graphFailRemaining int // how many failures to serve; < 0 = forever

	tokenRequests     int
	graphRequests     int
	skipTokenRequests int
	lastTokenForm     url.Values
	lastGraphPath     string
	lastGraphFilter   string
}

func newMockMsGraph() *mockMsGraph {
	return &mockMsGraph{}
}

// start serves the mock and returns the httptest server (closed via t.Cleanup).
func (m *mockMsGraph) start(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(m)
	t.Cleanup(server.Close)
	m.mu.Lock()
	m.baseURL = server.URL
	m.mu.Unlock()
	return server
}

// addRecord appends a record. Records must be added in ascending
// createdDateTime order, matching how tests model a growing event stream.
func (m *mockMsGraph) addRecord(payload map[string]interface{}) {
	created, err := time.Parse(time.RFC3339Nano, payload["createdDateTime"].(string))
	if err != nil {
		panic(fmt.Sprintf("fixture createdDateTime unparseable: %v", err))
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, graphRecord{t: created, payload: payload})
}

// failGraph makes the Graph endpoint answer `status` for the next `times`
// requests (times < 0 = every request).
func (m *mockMsGraph) failGraph(status int, times int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.graphFailStatus = status
	m.graphFailRemaining = times
}

func (m *mockMsGraph) counts() (tokenReqs, graphReqs, skipTokenReqs int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenRequests, m.graphRequests, m.skipTokenRequests
}

func (m *mockMsGraph) tokenForm() url.Values {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastTokenForm
}

func (m *mockMsGraph) graphPathAndFilter() (string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastGraphPath, m.lastGraphFilter
}

func (m *mockMsGraph) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/"+testTenantID+"/oauth2/v2.0/token":
		m.serveToken(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1.0/"):
		m.serveGraph(w, r)
	default:
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"error": map[string]interface{}{"code": "BadRequest", "message": "unknown path " + r.URL.Path},
		})
	}
}

// serveToken implements the Azure AD client_credentials token exchange.
func (m *mockMsGraph) serveToken(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.tokenRequests++
	m.mu.Unlock()

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/x-www-form-urlencoded") {
		writeJSON(w, http.StatusBadRequest, aadError("AADSTS900144", "the request body must be form-urlencoded"))
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, aadError("AADSTS900144", "unreadable body"))
		return
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, aadError("AADSTS900144", "unparseable form body"))
		return
	}
	m.mu.Lock()
	m.lastTokenForm = form
	m.mu.Unlock()

	if form.Get("grant_type") != "client_credentials" {
		writeJSON(w, http.StatusBadRequest, aadError("AADSTS70003", "unsupported grant type"))
		return
	}
	if form.Get("scope") != scope {
		writeJSON(w, http.StatusBadRequest, aadError("AADSTS70011", "invalid scope"))
		return
	}
	if form.Get("client_id") != testClientID {
		writeJSON(w, http.StatusBadRequest, aadError("AADSTS700016", "application not found in the directory"))
		return
	}
	if form.Get("client_secret") != testClientSecret {
		// Like the real endpoint, a bad secret yields a 401 with no
		// access_token -- which is all the adapter's fetchToken looks at.
		writeJSON(w, http.StatusUnauthorized, aadError("AADSTS7000215", "invalid client secret provided"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"token_type":     "Bearer",
		"expires_in":     3599,
		"ext_expires_in": 3599,
		"access_token":   testAccessToken,
	})
}

// serveGraph implements a Graph v1.0 list endpoint with an OData
// createdDateTime filter and the standard value/@odata.nextLink envelope.
func (m *mockMsGraph) serveGraph(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.graphRequests++
	m.lastGraphPath = r.URL.Path
	if r.URL.Query().Get("$skiptoken") != "" {
		m.skipTokenRequests++
	}
	failStatus := 0
	if m.graphFailStatus != 0 && m.graphFailRemaining != 0 {
		failStatus = m.graphFailStatus
		if m.graphFailRemaining > 0 {
			m.graphFailRemaining--
		}
	}
	m.mu.Unlock()

	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, graphError("BadRequest", "method not allowed"))
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+testAccessToken {
		writeJSON(w, http.StatusUnauthorized, graphError("InvalidAuthenticationToken", "access token is empty or invalid"))
		return
	}
	if failStatus != 0 {
		writeJSON(w, failStatus, graphError("ServiceUnavailable", "injected failure"))
		return
	}

	const filterPrefix = "createdDateTime ge "
	filter := r.URL.Query().Get("$filter")
	m.mu.Lock()
	m.lastGraphFilter = filter
	m.mu.Unlock()
	if !strings.HasPrefix(filter, filterPrefix) {
		writeJSON(w, http.StatusBadRequest, graphError("BadRequest", "unsupported $filter: "+filter))
		return
	}
	since, err := time.Parse(time.RFC3339Nano, strings.TrimPrefix(filter, filterPrefix))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, graphError("BadRequest", "unparseable filter timestamp: "+err.Error()))
		return
	}

	m.mu.Lock()
	var matching []map[string]interface{}
	for _, rec := range m.records {
		// OData `ge` is inclusive.
		if !rec.t.Before(since) {
			matching = append(matching, rec.payload)
		}
	}
	pageSize := m.pageSize
	baseURL := m.baseURL
	m.mu.Unlock()

	page := matching
	truncated := false
	if pageSize > 0 && len(matching) > pageSize {
		page = matching[:pageSize]
		truncated = true
	}
	if page == nil {
		page = []map[string]interface{}{}
	}

	envelope := map[string]interface{}{
		"@odata.context": "https://graph.microsoft.com/v1.0/$metadata#" + strings.TrimPrefix(r.URL.Path, "/v1.0/"),
		"value":          page,
	}
	if truncated {
		envelope["@odata.nextLink"] = baseURL + r.URL.Path + "?$skiptoken=fakeskiptoken1111"
	}
	writeJSON(w, http.StatusOK, envelope)
}

func writeJSON(w http.ResponseWriter, status int, body map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func aadError(code, description string) map[string]interface{} {
	return map[string]interface{}{
		"error":             "invalid_client",
		"error_description": fmt.Sprintf("%s: %s", code, description),
		"error_codes":       []interface{}{code},
	}
}

func graphError(code, message string) map[string]interface{} {
	return map[string]interface{}{
		"error": map[string]interface{}{"code": code, "message": message},
	}
}

// --- fixtures -----------------------------------------------------------------

// graphTimestamp renders a time the way Graph does: UTC with 7 fractional
// digits and a Z suffix.
func graphTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.0000000Z")
}

// fixtureBase returns a time safely later than the adapter's startup `since`
// filter. The adapter formats its initial `since` from the local clock with a
// literal Z suffix, so on a machine east of UTC that boundary can sit up to 14
// hours in the future; +24h clears it on any timezone.
func fixtureBase() time.Time {
	return time.Now().UTC().Add(24 * time.Hour)
}

// graphSecurityAlert is shaped like a real Graph security alerts_v2 record:
// the documented top-level fields plus nested objects and arrays, with clearly
// fake identifiers.
func graphSecurityAlert(id string, createdDateTime string) map[string]interface{} {
	return map[string]interface{}{
		"id":                 id,
		"providerAlertId":    "11111111-1111-1111-1111-111111111111",
		"incidentId":         "1111",
		"status":             "new",
		"severity":           "high",
		"classification":     nil,
		"determination":      nil,
		"serviceSource":      "microsoftDefenderForEndpoint",
		"detectionSource":    "antivirus",
		"detectorId":         "11111111-1111-1111-1111-111111111111",
		"tenantId":           testTenantID,
		"title":              "Suspicious process injection observed",
		"description":        "A process injected code into another process.",
		"category":           "DefenseEvasion",
		"createdDateTime":    createdDateTime,
		"lastUpdateDateTime": createdDateTime,
		"mitreTechniques":    []interface{}{"T1055", "T1055.001"},
		"vendorInformation": map[string]interface{}{
			"provider": "Microsoft 365 Defender",
			"vendor":   "Microsoft",
		},
		"evidence": []interface{}{
			map[string]interface{}{
				"@odata.type":     "#microsoft.graph.security.processEvidence",
				"processId":       4242,
				"imageFile":       map[string]interface{}{"fileName": "evil.exe", "filePath": `C:\Users\jdoe\Downloads`},
				"userAccount":     map[string]interface{}{"accountName": "jdoe", "userPrincipalName": "jdoe@example.com"},
				"detectionStatus": "detected",
			},
		},
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// --- helpers ------------------------------------------------------------------

// testConfig builds an adapter config pointed at the mock server.
func testConfig(t *testing.T, server *httptest.Server) MsGraphConfig {
	t.Helper()
	return MsGraphConfig{
		ClientOptions: testClientOptions(t),
		TenantID:      testTenantID,
		ClientID:      testClientID,
		ClientSecret:  testClientSecret,
		URL:           testResource,
		LoginEndpoint: server.URL,
		GraphEndpoint: server.URL + "/v1.0/",
		PollInterval:  50 * time.Millisecond,
	}
}

// startAdapter starts the adapter with a capture sink and registers cleanup.
func startAdapter(t *testing.T, conf MsGraphConfig) (*captureSink, *MsGraphAdapter, chan struct{}) {
	t.Helper()
	sink := &captureSink{}
	adapter, chStopped, err := newMsGraphAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = adapter.Close() })
	return sink, adapter, chStopped
}

// --- tests ----------------------------------------------------------------------

// TestMsGraphEndToEnd drives the adapter against the mock endpoints and pins
// the full contract: the client_credentials token exchange, the Bearer-auth'd
// GET with a createdDateTime filter against the configured resource (leading
// "/" stripped), every record shipped verbatim exactly once, the adapter's
// ingestion-time TimestampMs and empty EventType, and no re-shipping on
// subsequent polls.
func TestMsGraphEndToEnd(t *testing.T) {
	mock := newMockMsGraph()
	base := fixtureBase()
	want := []map[string]interface{}{
		graphSecurityAlert("alert-0001", graphTimestamp(base.Add(1*time.Minute))),
		graphSecurityAlert("alert-0002", graphTimestamp(base.Add(2*time.Minute))),
		graphSecurityAlert("alert-0003", graphTimestamp(base.Add(3*time.Minute))),
	}
	for _, rec := range want {
		mock.addRecord(rec)
	}
	server := mock.start(t)

	conf := testConfig(t, server)
	conf.URL = "/" + testResource // leading slash must be stripped

	before := uint64(time.Now().UnixMilli())
	sink, _, chStopped := startAdapter(t, conf)

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 alerts to ship")
	after := uint64(time.Now().UnixMilli())

	// Re-polling must not re-ship: the boundary record keeps coming back
	// (inclusive `ge` filter) but is suppressed via the last-event-id check.
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 30*time.Millisecond, "records were re-shipped on a later poll")

	select {
	case <-chStopped:
		t.Fatal("adapter stopped unexpectedly")
	default:
	}

	// The token exchange carried the full client_credentials contract (the
	// mock rejects anything else, but pin the form fields explicitly too).
	form := mock.tokenForm()
	require.NotNil(t, form)
	assert.Equal(t, "client_credentials", form.Get("grant_type"))
	assert.Equal(t, scope, form.Get("scope"))
	assert.Equal(t, testClientID, form.Get("client_id"))
	assert.Equal(t, testClientSecret, form.Get("client_secret"))

	// The Graph request targeted the configured resource with a
	// createdDateTime filter.
	path, filter := mock.graphPathAndFilter()
	assert.Equal(t, "/v1.0/"+testResource, path)
	assert.True(t, strings.HasPrefix(filter, "createdDateTime ge "), "unexpected $filter: %q", filter)

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		// The generic Graph adapter does not tag an event type and stamps
		// ingestion time rather than the record's createdDateTime -- pin
		// that behavior.
		assert.Empty(t, msg.EventType)
		assert.GreaterOrEqual(t, msg.TimestampMs, before, "TimestampMs should be ingestion time")
		assert.LessOrEqual(t, msg.TimestampMs, after, "TimestampMs should be ingestion time")
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	// Payloads ship verbatim -- nested objects, arrays and nulls included.
	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "record %s was not shipped", id)
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Graph record")
	}
}

// TestMsGraphMidRunRecordShipsOnce verifies a record appearing while the
// adapter is running is picked up by the advancing createdDateTime filter and
// shipped exactly once.
func TestMsGraphMidRunRecordShipsOnce(t *testing.T) {
	mock := newMockMsGraph()
	base := fixtureBase()
	mock.addRecord(graphSecurityAlert("alert-a", graphTimestamp(base.Add(1*time.Minute))))
	mock.addRecord(graphSecurityAlert("alert-b", graphTimestamp(base.Add(2*time.Minute))))
	server := mock.start(t)

	sink, _, _ := startAdapter(t, testConfig(t, server))

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)

	// A new alert lands after a few polls have already gone by.
	mock.addRecord(graphSecurityAlert("alert-c", graphTimestamp(base.Add(5*time.Minute))))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "the new alert should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 30*time.Millisecond)

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["id"].(string)]++
	}
	assert.Equal(t, map[string]int{"alert-a": 1, "alert-b": 1, "alert-c": 1}, shippedPerID,
		"every alert must ship exactly once")
}

// TestMsGraphPagedSetFullyConsumed verifies behavior when Graph truncates a
// result set and advertises @odata.nextLink: the adapter does not follow the
// nextLink (pinning current behavior), but still consumes the full set across
// successive polls because each shipped batch advances the createdDateTime
// filter -- and every record ships exactly once.
func TestMsGraphPagedSetFullyConsumed(t *testing.T) {
	mock := newMockMsGraph()
	mock.pageSize = 2
	base := fixtureBase()
	const total = 5
	for i := 1; i <= total; i++ {
		mock.addRecord(graphSecurityAlert(
			fmt.Sprintf("alert-%04d", i),
			graphTimestamp(base.Add(time.Duration(i)*time.Minute))))
	}
	server := mock.start(t)

	sink, _, _ := startAdapter(t, testConfig(t, server))

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 25*time.Millisecond, "the full paginated set should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 30*time.Millisecond)

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["id"].(string)]++
	}
	require.Len(t, shippedPerID, total)
	for id, n := range shippedPerID {
		assert.Equalf(t, 1, n, "record %s shipped %d times", id, n)
	}

	// The adapter never follows @odata.nextLink; it relies on the advancing
	// time filter instead.
	_, _, skipTokenReqs := mock.counts()
	assert.Equal(t, 0, skipTokenReqs, "the adapter is not expected to follow @odata.nextLink")
}

// TestMsGraphBadClientSecretShipsNothing pins the adapter's behavior on a
// failed credential exchange: fetchToken is retried 3 times, an error is
// surfaced, nothing ships, the Graph endpoint is never called -- and the
// adapter keeps running (it does not stop on auth failure).
func TestMsGraphBadClientSecretShipsNothing(t *testing.T) {
	mock := newMockMsGraph()
	base := fixtureBase()
	mock.addRecord(graphSecurityAlert("alert-a", graphTimestamp(base.Add(1*time.Minute))))
	server := mock.start(t)

	var tokenFailures atomic.Int32
	conf := testConfig(t, server)
	conf.ClientSecret = "wrong-secret"
	conf.ClientOptions.OnError = func(err error) {
		t.Logf("ERR: %v", err)
		if strings.Contains(err.Error(), "error fetching token after 3 attempts") {
			tokenFailures.Add(1)
		}
	}

	sink, _, chStopped := startAdapter(t, conf)

	// The retry path sleeps 1s + 2s inside the adapter before giving up.
	require.Eventually(t, func() bool { return tokenFailures.Load() >= 1 },
		15*time.Second, 50*time.Millisecond, "expected the token failure to be reported")

	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
	tokenReqs, graphReqs, _ := mock.counts()
	assert.GreaterOrEqual(t, tokenReqs, 3, "the token exchange should be retried 3 times")
	assert.Equal(t, 0, graphReqs, "Graph must not be called without a token")

	select {
	case <-chStopped:
		t.Fatal("the adapter is not expected to stop on an auth failure; it keeps polling")
	default:
	}
}

// TestMsGraphRejectedTokenKeepsPolling pins the adapter's behavior when Graph
// itself rejects the Bearer token (e.g. missing API permissions): the error is
// surfaced without retry, nothing ships, and the adapter keeps polling on its
// interval rather than stopping.
func TestMsGraphRejectedTokenKeepsPolling(t *testing.T) {
	mock := newMockMsGraph()
	base := fixtureBase()
	mock.addRecord(graphSecurityAlert("alert-a", graphTimestamp(base.Add(1*time.Minute))))
	mock.failGraph(http.StatusUnauthorized, -1)
	server := mock.start(t)

	var graphErrors atomic.Int32
	conf := testConfig(t, server)
	conf.ClientOptions.OnError = func(err error) {
		t.Logf("ERR: %v", err)
		if strings.Contains(err.Error(), "verify permissions") {
			graphErrors.Add(1)
		}
	}

	sink, _, chStopped := startAdapter(t, conf)

	// At least two poll cycles must report the error -- proof the adapter
	// keeps polling instead of stopping or retrying a non-retryable status.
	require.Eventually(t, func() bool { return graphErrors.Load() >= 2 },
		5*time.Second, 20*time.Millisecond, "expected repeated Graph auth errors across polls")

	assert.Equal(t, 0, sink.count(), "nothing should ship when Graph rejects the token")
	select {
	case <-chStopped:
		t.Fatal("the adapter is not expected to stop on a Graph 401; it keeps polling")
	default:
	}
}

// TestMsGraphTransient503Retried verifies a 503 from Graph is retried within
// the same poll and the records ship once the endpoint recovers.
func TestMsGraphTransient503Retried(t *testing.T) {
	mock := newMockMsGraph()
	base := fixtureBase()
	want := graphSecurityAlert("alert-a", graphTimestamp(base.Add(1*time.Minute)))
	mock.addRecord(want)
	mock.failGraph(http.StatusServiceUnavailable, 2)
	server := mock.start(t)

	sink, _, chStopped := startAdapter(t, testConfig(t, server))

	// The retry path sleeps 1s + 2s inside the adapter before the third try.
	require.Eventually(t, func() bool { return sink.count() == 1 },
		15*time.Second, 50*time.Millisecond, "the record should ship after the 503s clear")

	_, graphReqs, _ := mock.counts()
	assert.GreaterOrEqual(t, graphReqs, 3, "two 503s then a success")
	assert.JSONEq(t, mustJSON(t, want), mustJSON(t, sink.snapshot()[0].JsonPayload))

	select {
	case <-chStopped:
		t.Fatal("adapter should survive transient 503s")
	default:
	}
}
