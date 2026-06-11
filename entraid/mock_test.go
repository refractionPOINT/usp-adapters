package usp_entraid

// This file exercises the adapter end-to-end against a mock of the two
// Microsoft endpoints it talks to:
//
//   - the Microsoft identity platform token endpoint
//     (POST /<tenant>/oauth2/v2.0/token, OAuth2 client_credentials), and
//   - the Microsoft Graph Identity Protection risk detections endpoint
//     (GET /v1.0/identityProtection/riskDetections?$filter=activityDateTime ge <ts>).
//
// The mock validates the credential exchange and the bearer token, honours the
// adapter's $filter on activityDateTime (inclusive "ge", as OData defines it),
// orders results by activityDateTime ascending and can truncate responses to a
// page size, advertising the Graph "@odata.nextLink" continuation the real API
// returns. Note the adapter does not follow @odata.nextLink: it drains large
// result sets across successive polls by advancing its $filter to the last
// detection's activityDateTime, which is what the pagination test exercises.
//
// All fixture data is fake: example.com principals, all-1s UUIDs,
// documentation-range IPs (203.0.113.0/24) and made-up tokens.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTenantID     = "11111111-1111-1111-1111-111111111111"
	testClientID     = "11111111-1111-1111-1111-222222222222"
	testClientSecret = "fake-client-secret-for-tests"
	testAccessToken  = "fake-access-token-issued-by-mock"

	// graphTimeLayout matches the shape of Microsoft Graph datetime fields
	// (UTC, fractional seconds, Z suffix). It is also the layout the adapter
	// uses for its initial "since" value.
	graphTimeLayout = "2006-01-02T15:04:05.000000Z"

	// testPollInterval keeps the e2e tests fast; production default is 30s.
	testPollInterval = 50 * time.Millisecond
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

// --- test client options ------------------------------------------------------

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

// errorRecorder collects OnError messages so tests can assert on the adapter's
// error reporting. Safe for use from the adapter's goroutines.
type errorRecorder struct {
	mu     sync.Mutex
	errors []string
}

func (e *errorRecorder) record(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.errors = append(e.errors, err.Error())
}

func (e *errorRecorder) anyContains(substr string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range e.errors {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

// --- mock Microsoft (login.microsoftonline.com + graph.microsoft.com) ---------

// mockMicrosoft serves both the identity platform token endpoint and the Graph
// riskDetections endpoint from a single httptest server; tests point both of
// the adapter's endpoint overrides at it.
type mockMicrosoft struct {
	mu sync.Mutex

	tenantID     string
	clientID     string
	clientSecret string
	accessToken  string

	// detections is the in-memory Graph dataset.
	detections []map[string]interface{}

	// pageSize, when > 0, caps how many detections a single response carries;
	// truncated responses include an @odata.nextLink, like the real Graph API.
	pageSize int

	// revokeGraphAccess makes the Graph endpoint reject every bearer token
	// (e.g. the app registration lost its IdentityRiskEvent.Read.All grant).
	revokeGraphAccess bool

	tokenRequests int
	graphRequests int

	// lastTokenForm is the most recent decoded body of a token request, kept
	// so tests can assert the exact client_credentials exchange.
	lastTokenForm url.Values

	serverURL string
}

func newMockMicrosoft() *mockMicrosoft {
	return &mockMicrosoft{
		tenantID:     testTenantID,
		clientID:     testClientID,
		clientSecret: testClientSecret,
		accessToken:  testAccessToken,
	}
}

func (m *mockMicrosoft) start(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(m.handler(t))
	t.Cleanup(server.Close)
	m.mu.Lock()
	m.serverURL = server.URL
	m.mu.Unlock()
	return server
}

func (m *mockMicrosoft) addDetection(d map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.detections = append(m.detections, d)
}

func (m *mockMicrosoft) tokenRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenRequests
}

func (m *mockMicrosoft) graphRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.graphRequests
}

func (m *mockMicrosoft) lastTokenRequest() url.Values {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastTokenForm
}

func (m *mockMicrosoft) handler(t *testing.T) http.HandlerFunc {
	tokenPath := "/" + testTenantID + "/oauth2/v2.0/token"
	graphPath := "/v1.0/identityProtection/riskDetections"

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case tokenPath:
			m.handleToken(w, r)
		case graphPath:
			m.handleRiskDetections(w, r)
		default:
			writeGraphError(w, http.StatusNotFound, "ResourceNotFound",
				fmt.Sprintf("Resource not found for the segment %q.", r.URL.Path))
		}
	}
}

// handleToken implements the OAuth2 client_credentials exchange of the
// Microsoft identity platform v2.0 endpoint.
func (m *mockMicrosoft) handleToken(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.tokenRequests++
	m.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error":             "invalid_request",
			"error_description": "AADSTS900561: The endpoint only accepts POST requests.",
		})
		return
	}
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error":             "invalid_request",
			"error_description": "AADSTS900144: The request body must be form-urlencoded.",
		})
		return
	}

	m.mu.Lock()
	m.lastTokenForm = r.PostForm
	clientID, clientSecret := m.clientID, m.clientSecret
	token := m.accessToken
	m.mu.Unlock()

	if r.PostForm.Get("grant_type") != "client_credentials" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error":             "unsupported_grant_type",
			"error_description": "AADSTS70003: The app requested an unsupported grant type.",
		})
		return
	}
	if r.PostForm.Get("client_id") != clientID || r.PostForm.Get("client_secret") != clientSecret {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error":             "invalid_client",
			"error_description": "AADSTS7000215: Invalid client secret provided. Ensure the secret being sent in the request is the client secret value, not the client secret ID.",
			"error_codes":       []int{7000215},
			"timestamp":         time.Now().UTC().Format("2006-01-02 15:04:05Z"),
			"trace_id":          "11111111-1111-1111-1111-111111111111",
			"correlation_id":    "11111111-1111-1111-1111-111111111111",
		})
		return
	}
	if r.PostForm.Get("scope") != "https://graph.microsoft.com/.default" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error":             "invalid_scope",
			"error_description": "AADSTS70011: The provided value for the input parameter 'scope' is not valid.",
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"token_type":     "Bearer",
		"expires_in":     3599,
		"ext_expires_in": 3599,
		"access_token":   token,
	})
}

// handleRiskDetections implements GET /v1.0/identityProtection/riskDetections
// with the $filter the adapter sends: "activityDateTime ge <timestamp>".
func (m *mockMicrosoft) handleRiskDetections(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.graphRequests++
	token := m.accessToken
	revoked := m.revokeGraphAccess
	pageSize := m.pageSize
	detections := make([]map[string]interface{}, len(m.detections))
	copy(detections, m.detections)
	serverURL := m.serverURL
	m.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		writeGraphError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed.")
		return
	}
	if revoked || r.Header.Get("Authorization") != "Bearer "+token {
		writeGraphError(w, http.StatusUnauthorized, "InvalidAuthenticationToken",
			"Access token validation failure. Invalid audience.")
		return
	}

	since, ok := parseActivityDateTimeFilter(r.URL.Query().Get("$filter"))
	if !ok {
		writeGraphError(w, http.StatusBadRequest, "BadRequest", "Invalid $filter clause.")
		return
	}

	// "ge" is inclusive, and Graph returns risk detections ordered by
	// activityDateTime ascending when filtered this way.
	matched := make([]map[string]interface{}, 0, len(detections))
	for _, d := range detections {
		at, err := time.Parse(time.RFC3339, d["activityDateTime"].(string))
		if err != nil {
			continue
		}
		if !at.Before(since) {
			matched = append(matched, d)
		}
	}
	sort.SliceStable(matched, func(i, j int) bool {
		ti, _ := time.Parse(time.RFC3339, matched[i]["activityDateTime"].(string))
		tj, _ := time.Parse(time.RFC3339, matched[j]["activityDateTime"].(string))
		return ti.Before(tj)
	})

	envelope := map[string]interface{}{
		"@odata.context": "https://graph.microsoft.com/v1.0/$metadata#riskDetections",
	}
	if pageSize > 0 && len(matched) > pageSize {
		matched = matched[:pageSize]
		envelope["@odata.nextLink"] = serverURL +
			"/v1.0/identityProtection/riskDetections?$skiptoken=fake-skip-token-1111"
	}
	envelope["value"] = matched

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(envelope)
}

// parseActivityDateTimeFilter extracts the timestamp from a
// "activityDateTime ge <ts>" OData filter. The adapter has a quirk where a
// retried request re-appends "?$filter=..." to an URL that already carries one,
// so anything from a stray "?" onward is ignored.
func parseActivityDateTimeFilter(filter string) (time.Time, bool) {
	if i := strings.Index(filter, "?"); i >= 0 {
		filter = filter[:i]
	}
	const prefix = "activityDateTime ge "
	if !strings.HasPrefix(filter, prefix) {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339, strings.TrimSpace(strings.TrimPrefix(filter, prefix)))
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

// writeGraphError writes a Microsoft Graph error envelope.
func writeGraphError(w http.ResponseWriter, status int, code, message string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{
			"code":    code,
			"message": message,
			"innerError": map[string]interface{}{
				"date":              time.Now().UTC().Format(time.RFC3339),
				"request-id":        "11111111-1111-1111-1111-111111111111",
				"client-request-id": "11111111-1111-1111-1111-111111111111",
			},
		},
	})
}

// --- fixtures -------------------------------------------------------------------

// fixtureBaseTime returns a base time for fixture activityDateTime values. The
// adapter's initial "since" is the local wall-clock rendered with a Z suffix,
// so fixtures sit a full day in the future to be on the late side of that
// filter in any timezone.
func fixtureBaseTime() time.Time {
	return time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
}

// realisticRiskDetection builds a record shaped like a Microsoft Graph
// identityProtection riskDetection resource. All identifying values are fake.
func realisticRiskDetection(seq int, riskEventType, upn, activityDateTime string) map[string]interface{} {
	return map[string]interface{}{
		"id":                  fmt.Sprintf("%060d%04d", 0, seq),
		"requestId":           "11111111-1111-1111-1111-111111111111",
		"correlationId":       "11111111-1111-1111-1111-111111111111",
		"riskEventType":       riskEventType,
		"riskState":           "atRisk",
		"riskLevel":           "medium",
		"riskDetail":          "none",
		"source":              "IdentityProtection",
		"detectionTimingType": "realtime",
		"activity":            "signin",
		"tokenIssuerType":     "AzureAD",
		"ipAddress":           fmt.Sprintf("203.0.113.%d", seq%250+1),
		"activityDateTime":    activityDateTime,
		"detectedDateTime":    activityDateTime,
		"lastUpdatedDateTime": activityDateTime,
		"userId":              "11111111-1111-1111-1111-111111111111",
		"userDisplayName":     "Jane Doe",
		"userPrincipalName":   upn,
		"additionalInfo":      `[{"Key":"riskReasons","Value":["Anonymous IP address"]}]`,
		"location": map[string]interface{}{
			"city":            "Springfield",
			"state":           "Illinois",
			"countryOrRegion": "US",
			"geoCoordinates": map[string]interface{}{
				"latitude":  39.78,
				"longitude": -89.65,
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

// testConfig returns an adapter config pointed at the mock server.
func testConfig(t *testing.T, serverURL string) EntraIDConfig {
	t.Helper()
	return EntraIDConfig{
		ClientOptions: testClientOptions(t),
		TenantID:      testTenantID,
		ClientID:      testClientID,
		ClientSecret:  testClientSecret,
		LoginEndpoint: serverURL,
		GraphEndpoint: serverURL,
		PollInterval:  testPollInterval,
	}
}

// --- tests ----------------------------------------------------------------------

// TestRiskDetectionsEndToEnd drives the adapter against the mock Microsoft
// endpoints and asserts the exact events shipped: every detection arrives with
// its payload verbatim (nested location object included), no EventType (the
// adapter does not tag one) and an ingestion-time TimestampMs. It then verifies
// that subsequent polls do not re-ship anything.
func TestRiskDetectionsEndToEnd(t *testing.T) {
	mock := newMockMicrosoft()
	base := fixtureBaseTime()
	want := []map[string]interface{}{
		realisticRiskDetection(1, "anonymizedIPAddress", "jdoe@example.com", base.Format(graphTimeLayout)),
		realisticRiskDetection(2, "unfamiliarFeatures", "asmith@example.com", base.Add(1*time.Minute).Format(graphTimeLayout)),
		realisticRiskDetection(3, "unlikelyTravel", "bjones@example.com", base.Add(2*time.Minute).Format(graphTimeLayout)),
	}
	for _, d := range want {
		mock.addDetection(d)
	}
	server := mock.start(t)

	sink := &captureSink{}
	startMs := uint64(time.Now().UnixMilli())

	adapter, chStopped, err := newEntraIDAdapter(context.Background(), testConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		10*time.Second, 20*time.Millisecond, "expected all 3 risk detections to ship")

	// Re-polling must not re-ship: the count stays at 3 across further polls.
	require.Never(t, func() bool { return sink.count() != 3 },
		400*time.Millisecond, 30*time.Millisecond, "detections were re-shipped on a later poll")

	select {
	case <-chStopped:
		t.Fatal("adapter stopped unexpectedly")
	default:
	}

	endMs := uint64(time.Now().UnixMilli())
	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		// The adapter does not set an EventType for risk detections.
		assert.Empty(t, msg.EventType)
		// TimestampMs is the ingestion time, not the detection time.
		assert.GreaterOrEqual(t, msg.TimestampMs, startMs)
		assert.LessOrEqual(t, msg.TimestampMs, endMs)
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "detection %s was not shipped", id)
		// The payload is shipped verbatim -- nested objects included.
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Graph riskDetection")
	}

	// The mock validated the credential exchange and bearer token on every
	// request; verify the exact client_credentials form the adapter sent.
	form := mock.lastTokenRequest()
	require.NotNil(t, form)
	assert.Equal(t, "client_credentials", form.Get("grant_type"))
	assert.Equal(t, testClientID, form.Get("client_id"))
	assert.Equal(t, testClientSecret, form.Get("client_secret"))
	assert.Equal(t, "https://graph.microsoft.com/.default", form.Get("scope"))
}

// TestMidRunDetectionShipsOnce verifies a detection appearing while the adapter
// is running ships exactly once, and the already-shipped ones never re-ship.
func TestMidRunDetectionShipsOnce(t *testing.T) {
	mock := newMockMicrosoft()
	base := fixtureBaseTime()
	mock.addDetection(realisticRiskDetection(1, "anonymizedIPAddress", "jdoe@example.com", base.Format(graphTimeLayout)))
	mock.addDetection(realisticRiskDetection(2, "passwordSpray", "asmith@example.com", base.Add(1*time.Minute).Format(graphTimeLayout)))
	server := mock.start(t)

	sink := &captureSink{}
	adapter, _, err := newEntraIDAdapter(context.Background(), testConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		10*time.Second, 20*time.Millisecond)

	// A new risk detection occurs mid-run, later than everything shipped so far.
	mock.addDetection(realisticRiskDetection(3, "unlikelyTravel", "bjones@example.com", base.Add(5*time.Minute).Format(graphTimeLayout)))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		10*time.Second, 20*time.Millisecond, "the new detection should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		400*time.Millisecond, 30*time.Millisecond, "a detection was shipped more than once")

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["id"].(string)]++
	}
	require.Len(t, shippedPerID, 3)
	for id, n := range shippedPerID {
		assert.Equal(t, 1, n, "detection %q must ship exactly once", id)
	}
}

// TestPaginatedResultSetFullyConsumed verifies a result set larger than one
// Graph response is fully consumed. The mock truncates each response to a page
// and advertises @odata.nextLink like the real API; the adapter does not follow
// the link but drains the set across polls by advancing its activityDateTime
// filter, and every detection must ship exactly once.
func TestPaginatedResultSetFullyConsumed(t *testing.T) {
	const total = 5

	mock := newMockMicrosoft()
	mock.pageSize = 2
	base := fixtureBaseTime()
	for i := 1; i <= total; i++ {
		mock.addDetection(realisticRiskDetection(
			i, "unfamiliarFeatures", fmt.Sprintf("user%d@example.com", i),
			base.Add(time.Duration(i)*time.Minute).Format(graphTimeLayout)))
	}
	server := mock.start(t)

	sink := &captureSink{}
	adapter, _, err := newEntraIDAdapter(context.Background(), testConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 20*time.Millisecond, "all paginated detections should ship")
	require.Never(t, func() bool { return sink.count() != total },
		400*time.Millisecond, 30*time.Millisecond, "detections were re-shipped")

	// Draining a truncated result set takes multiple Graph requests.
	assert.GreaterOrEqual(t, mock.graphRequestCount(), 3,
		"consuming the set should take several polls when responses are truncated")

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["id"].(string)]++
	}
	require.Len(t, shippedPerID, total)
	for id, n := range shippedPerID {
		assert.Equal(t, 1, n, "detection %q must ship exactly once", id)
	}
}

// TestBadClientSecretShipsNothing verifies that when the token endpoint rejects
// the credentials, nothing ships, the failure is reported through OnError, the
// Graph endpoint is never reached, and -- per the adapter's error handling --
// the adapter stays alive and keeps retrying on its poll interval.
func TestBadClientSecretShipsNothing(t *testing.T) {
	mock := newMockMicrosoft()
	base := fixtureBaseTime()
	mock.addDetection(realisticRiskDetection(1, "anonymizedIPAddress", "jdoe@example.com", base.Format(graphTimeLayout)))
	server := mock.start(t)

	rec := &errorRecorder{}
	conf := testConfig(t, server.URL)
	conf.ClientSecret = "wrong-secret"
	baseOnError := conf.ClientOptions.OnError
	conf.ClientOptions.OnError = func(err error) {
		rec.record(err)
		baseOnError(err)
	}

	sink := &captureSink{}
	adapter, chStopped, err := newEntraIDAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// The adapter keeps polling: it reports the token failure and tries again
	// on the next interval rather than stopping.
	require.Eventually(t, func() bool { return mock.tokenRequestCount() >= 2 },
		10*time.Second, 20*time.Millisecond, "the adapter should retry the token exchange on later polls")
	require.Eventually(t, func() bool { return rec.anyContains("error fetching token") },
		10*time.Second, 20*time.Millisecond, "the token failure should be reported via OnError")

	select {
	case <-chStopped:
		t.Fatal("adapter should keep running after a failed token exchange")
	default:
	}
	assert.Equal(t, 0, sink.count(), "nothing should ship when the credential exchange fails")
	assert.Equal(t, 0, mock.graphRequestCount(), "the Graph API must not be called without a token")
}

// TestGraphRejectsTokenShipsNothing verifies that when Graph rejects the bearer
// token (e.g. missing IdentityRiskEvent.Read.All), the adapter retries the call
// up to 3 times per poll, reports the failure, ships nothing, and stays alive.
func TestGraphRejectsTokenShipsNothing(t *testing.T) {
	mock := newMockMicrosoft()
	mock.revokeGraphAccess = true
	base := fixtureBaseTime()
	mock.addDetection(realisticRiskDetection(1, "anonymizedIPAddress", "jdoe@example.com", base.Format(graphTimeLayout)))
	server := mock.start(t)

	rec := &errorRecorder{}
	conf := testConfig(t, server.URL)
	baseOnError := conf.ClientOptions.OnError
	conf.ClientOptions.OnError = func(err error) {
		rec.record(err)
		baseOnError(err)
	}

	sink := &captureSink{}
	adapter, chStopped, err := newEntraIDAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// Each poll retries the Graph call up to 3 times before giving up.
	require.Eventually(t, func() bool { return mock.graphRequestCount() >= 3 },
		10*time.Second, 20*time.Millisecond, "the adapter should retry the Graph call within a poll")
	require.Eventually(t, func() bool { return rec.anyContains("error response from Microsoft API") },
		10*time.Second, 20*time.Millisecond, "the Graph rejection should be reported via OnError")

	select {
	case <-chStopped:
		t.Fatal("adapter should keep running after a Graph authorization failure")
	default:
	}
	assert.Equal(t, 0, sink.count(), "nothing should ship when Graph rejects the token")
}
