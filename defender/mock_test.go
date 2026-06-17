package usp_defender

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
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock of the two
// Microsoft endpoints it talks to -- the identity platform token endpoint and
// the MS Graph security alerts_v2 endpoint -- capturing the exact messages it
// ships so their content can be asserted.

// graphTimestampLayout matches how MS Graph renders alerts_v2 timestamps
// (seven fractional digits, Z suffix). The trailing Z is a literal in this
// layout, mirroring how the adapter itself renders its `since` watermark.
const graphTimestampLayout = "2006-01-02T15:04:05.0000000Z"

const (
	mockTenantID     = "11111111-1111-1111-1111-111111111111"
	mockClientID     = "22222222-2222-2222-2222-222222222222"
	mockClientSecret = "fake-client-secret-for-tests-only"
	mockAccessToken  = "fake-access-token-0000000000000001"

	tokenPath  = "/" + mockTenantID + "/oauth2/v2.0/token"
	alertsPath = "/v1.0/security/alerts_v2"
)

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

// --- mock Microsoft endpoints -------------------------------------------------

// mockMicrosoft is an in-memory stand-in for the two Microsoft endpoints the
// adapter uses:
//
//   - POST /<tenant>/oauth2/v2.0/token -- the identity platform
//     client_credentials flow. It validates client_id/client_secret/
//     grant_type/scope exactly as the adapter sends them and returns an
//     access_token.
//   - GET /v1.0/security/alerts_v2 -- the MS Graph security alerts endpoint.
//     It validates the bearer token, honors the adapter's
//     `$filter=createdDateTime ge <ts>` query parameter against an in-memory
//     dataset, and returns the Graph envelope ({"@odata.context", "value",
//     and "@odata.nextLink" when the page is truncated}).
//
// Ordering caveat: the mock serves alerts in ascending createdDateTime order
// because the adapter's watermark logic (take the createdDateTime of the last
// item processed as the next `since`) only makes forward progress under that
// ordering. The official docs do NOT promise it: the List alerts_v2 page says
// "The most recent alerts are displayed at the top of the list" (newest
// first) and $orderby is not among the supported query parameters ($count,
// $filter, $skip, $top). These tests pin the adapter's behavior, not a Graph
// ordering guarantee.
type mockMicrosoft struct {
	mu sync.Mutex

	alerts   []map[string]interface{} // ascending createdDateTime
	pageSize int                      // 0 = no truncation

	tokenRequests int
	alertRequests int
	nextLinks     int

	lastTokenForm  url.Values
	lastAuthHeader string
	lastFilter     string
}

func newMockMicrosoft() *mockMicrosoft {
	return &mockMicrosoft{}
}

// appendAlert appends an alert at the end of the dataset (newest last),
// keeping the dataset in the ascending createdDateTime order the adapter's
// watermark logic depends on (see the ordering caveat on mockMicrosoft).
func (m *mockMicrosoft) appendAlert(alert map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alerts = append(m.alerts, alert)
}

func (m *mockMicrosoft) tokenRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenRequests
}

func (m *mockMicrosoft) alertRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.alertRequests
}

func (m *mockMicrosoft) nextLinkCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nextLinks
}

func (m *mockMicrosoft) lastSeenTokenForm() url.Values {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastTokenForm
}

func (m *mockMicrosoft) lastSeenAuthHeader() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastAuthHeader
}

func (m *mockMicrosoft) lastSeenFilter() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastFilter
}

func (m *mockMicrosoft) handler(t *testing.T) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(tokenPath, func(w http.ResponseWriter, r *http.Request) { m.handleToken(t, w, r) })
	mux.HandleFunc(alertsPath, func(w http.ResponseWriter, r *http.Request) { m.handleAlerts(t, w, r) })
	return mux
}

// handleToken implements the client_credentials token grant. Note: handlers
// run on httptest goroutines, so they use assert (never require).
func (m *mockMicrosoft) handleToken(t *testing.T, w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.tokenRequests++
	m.mu.Unlock()

	if !assert.Equal(t, http.MethodPost, r.Method, "token endpoint expects POST") {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	assert.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))

	body, err := io.ReadAll(r.Body)
	if !assert.NoError(t, err) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	form, err := url.ParseQuery(string(body))
	if !assert.NoError(t, err, "token request body must be form-encoded") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	m.lastTokenForm = form
	m.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")

	// Documented AAD error responses carry error/error_description/error_codes
	// plus timestamp, trace_id, correlation_id and (when available) error_uri.
	// All ids here are clearly fake; the adapter only cares that no
	// access_token is present.
	if form.Get("grant_type") != "client_credentials" ||
		form.Get("scope") != "https://graph.microsoft.com/.default" ||
		form.Get("client_id") != mockClientID {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error":             "invalid_request",
			"error_description": "AADSTS900144: The request body must contain the following parameter: 'client_id'.\r\nTrace ID: 00000000-0000-0000-0000-000000000001\r\nCorrelation ID: 00000000-0000-0000-0000-000000000002\r\nTimestamp: 2024-01-01 00:00:00Z",
			"error_codes":       []int{900144},
			"timestamp":         "2024-01-01 00:00:00Z",
			"trace_id":          "00000000-0000-0000-0000-000000000001",
			"correlation_id":    "00000000-0000-0000-0000-000000000002",
			"error_uri":         "https://login.microsoftonline.com/error?code=900144",
		})
		return
	}
	if form.Get("client_secret") != mockClientSecret {
		// What AAD returns for a bad secret: error "invalid_client" with
		// AADSTS7000215 ("Invalid client secret is provided" per the AADSTS
		// error code reference). AAD serves invalid_client over HTTP 401,
		// which RFC 6749 section 5.2 permits.
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error":             "invalid_client",
			"error_description": "AADSTS7000215: Invalid client secret provided. Ensure the secret being sent in the request is the client secret value, not the client secret ID, for a secret added to app '" + mockClientID + "'.\r\nTrace ID: 00000000-0000-0000-0000-000000000001\r\nCorrelation ID: 00000000-0000-0000-0000-000000000002\r\nTimestamp: 2024-01-01 00:00:00Z",
			"error_codes":       []int{7000215},
			"timestamp":         "2024-01-01 00:00:00Z",
			"trace_id":          "00000000-0000-0000-0000-000000000001",
			"correlation_id":    "00000000-0000-0000-0000-000000000002",
			"error_uri":         "https://login.microsoftonline.com/error?code=7000215",
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"token_type":     "Bearer",
		"expires_in":     3599,
		"ext_expires_in": 3599,
		"access_token":   mockAccessToken,
	})
}

// handleAlerts implements GET /v1.0/security/alerts_v2 with the
// `$filter=createdDateTime ge <ts>` filter the adapter sends.
func (m *mockMicrosoft) handleAlerts(t *testing.T, w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.alertRequests++
	m.lastAuthHeader = r.Header.Get("Authorization")
	m.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")

	if !assert.Equal(t, http.MethodGet, r.Method, "alerts endpoint expects GET") {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+mockAccessToken {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{
				"code":    "InvalidAuthenticationToken",
				"message": "Access token is empty or invalid.",
			},
		})
		return
	}

	filter := r.URL.Query().Get("$filter")
	m.mu.Lock()
	m.lastFilter = filter
	m.mu.Unlock()

	const prefix = "createdDateTime ge "
	if !assert.True(t, strings.HasPrefix(filter, prefix), "unexpected $filter: %q", filter) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{"code": "BadRequest", "message": "Invalid filter clause."},
		})
		return
	}
	since, err := time.Parse(time.RFC3339Nano, strings.TrimPrefix(filter, prefix))
	if !assert.NoError(t, err, "unparseable $filter timestamp in %q", filter) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{"code": "BadRequest", "message": "Invalid filter clause."},
		})
		return
	}

	m.mu.Lock()
	var matching []map[string]interface{}
	for _, alert := range m.alerts {
		created, perr := time.Parse(time.RFC3339Nano, alert["createdDateTime"].(string))
		if !assert.NoError(t, perr) {
			continue
		}
		if !created.Before(since) {
			matching = append(matching, alert)
		}
	}
	pageSize := m.pageSize
	m.mu.Unlock()

	envelope := map[string]interface{}{
		"@odata.context": "https://graph.microsoft.com/v1.0/$metadata#security/alerts_v2",
	}
	if pageSize > 0 && len(matching) > pageSize {
		matching = matching[:pageSize]
		envelope["@odata.nextLink"] = fmt.Sprintf(
			"https://graph.microsoft.com/v1.0/security/alerts_v2?$filter=%s&$skiptoken=fakeSkipToken%d",
			url.QueryEscape(filter), pageSize)
		m.mu.Lock()
		m.nextLinks++
		m.mu.Unlock()
	}
	if matching == nil {
		matching = []map[string]interface{}{}
	}
	envelope["value"] = matching

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(envelope)
}

// --- realistic alert fixtures -------------------------------------------------

// realisticAlert returns an alert shaped like a real MS Graph security
// alerts_v2 record (microsoft.graph.security.alert): the documented top-level
// fields plus an evidence array mixing deviceEvidence / fileEvidence /
// processEvidence, nulls, ints, arrays and nested objects. All identifiers
// are clearly fake.
func realisticAlert(id string, createdDateTime time.Time) map[string]interface{} {
	created := createdDateTime.Format(graphTimestampLayout)
	return map[string]interface{}{
		"@odata.type":        "#microsoft.graph.security.alert",
		"id":                 id,
		"providerAlertId":    strings.TrimPrefix(id, "da"),
		"incidentId":         "10001",
		"status":             "new",
		"severity":           "medium",
		"classification":     nil,
		"determination":      nil,
		"serviceSource":      "microsoftDefenderForEndpoint",
		"detectionSource":    "antivirus",
		"detectorId":         "33333333-3333-3333-3333-333333333333",
		"tenantId":           mockTenantID,
		"title":              "Suspicious execution of hidden file",
		"description":        "A hidden file has been launched. This activity could indicate a malicious attempt to evade detection.",
		"recommendedActions": "Collect artifacts and determine scope. Review the machine timeline for suspicious activities.",
		"category":           "DefenseEvasion",
		"assignedTo":         nil,
		"alertWebUrl":        "https://security.microsoft.com/alerts/" + id + "?tid=" + mockTenantID,
		"incidentWebUrl":     "https://security.microsoft.com/incidents/10001?tid=" + mockTenantID,
		"actorDisplayName":   nil,
		"threatDisplayName":  nil,
		"threatFamilyName":   nil,
		"mitreTechniques":    []interface{}{"T1564.001"},
		"createdDateTime":    created,
		"lastUpdateDateTime": created,
		"resolvedDateTime":   nil,
		"firstActivityDateTime": createdDateTime.Add(-2 * time.Minute).
			Format(graphTimestampLayout),
		"lastActivityDateTime": created,
		"comments":             []interface{}{},
		"systemTags":           []interface{}{},
		"alertPolicyId":        nil,
		"evidence": []interface{}{
			map[string]interface{}{
				"@odata.type":       "#microsoft.graph.security.deviceEvidence",
				"createdDateTime":   created,
				"verdict":           "suspicious",
				"remediationStatus": "none",
				"roles":             []interface{}{},
				"tags":              []interface{}{"Test Machine"},
				"firstSeenDateTime": "2024-01-01T08:00:00.0000000Z",
				"mdeDeviceId":       "1111111111111111111111111111111111111111",
				"azureAdDeviceId":   nil,
				"deviceDnsName":     "test-host-1.example.com",
				"hostName":          "test-host-1",
				"osPlatform":        "Windows11",
				"osBuild":           22631,
				"version":           "23H2",
				"healthStatus":      "active",
				"riskScore":         "medium",
				"onboardingStatus":  "onboarded",
				"defenderAvStatus":  "updated",
				"loggedOnUsers": []interface{}{
					map[string]interface{}{
						"accountName": "jdoe",
						"domainName":  "EXAMPLE",
					},
				},
			},
			map[string]interface{}{
				"@odata.type":       "#microsoft.graph.security.fileEvidence",
				"createdDateTime":   created,
				"verdict":           "suspicious",
				"remediationStatus": "none",
				"roles":             []interface{}{},
				"tags":              []interface{}{},
				"detectionStatus":   "detected",
				"mdeDeviceId":       "1111111111111111111111111111111111111111",
				"fileDetails": map[string]interface{}{
					"sha1":          "1111111111111111111111111111111111111111",
					"sha256":        "1111111111111111111111111111111111111111111111111111111111111111",
					"fileName":      "hidden_payload.exe",
					"filePath":      `C:\Users\jdoe\AppData\Local\Temp`,
					"fileSize":      433152,
					"filePublisher": "Example Software Inc.",
					"signer":        nil,
					"issuer":        nil,
				},
			},
			map[string]interface{}{
				"@odata.type":             "#microsoft.graph.security.processEvidence",
				"createdDateTime":         created,
				"verdict":                 "suspicious",
				"remediationStatus":       "none",
				"roles":                   []interface{}{},
				"tags":                    []interface{}{},
				"processId":               4780,
				"parentProcessId":         668,
				"processCommandLine":      `"hidden_payload.exe" -enc SQBFAFgA`,
				"detectionStatus":         "detected",
				"mdeDeviceId":             "1111111111111111111111111111111111111111",
				"processCreationDateTime": created,
				"parentProcessCreationDateTime": createdDateTime.Add(-10 * time.Minute).
					Format(graphTimestampLayout),
				"imageFile": map[string]interface{}{
					"fileName": "hidden_payload.exe",
					"filePath": `C:\Users\jdoe\AppData\Local\Temp`,
					"fileSize": 433152,
				},
				"userAccount": map[string]interface{}{
					"accountName":       "jdoe",
					"domainName":        "EXAMPLE",
					"userSid":           "S-1-5-21-1111111111-1111111111-1111111111-1001",
					"azureAdUserId":     nil,
					"userPrincipalName": "jdoe@example.com",
				},
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

// startMockAdapter spins up the adapter wired to the mock server and the
// capture sink, with a short poll interval so tests run fast.
func startMockAdapter(t *testing.T, serverURL string, sink uspSink, clientSecret string) (*DefenderAdapter, chan struct{}) {
	t.Helper()
	conf := DefenderConfig{
		ClientOptions: testClientOptions(t),
		TenantID:      mockTenantID,
		ClientID:      mockClientID,
		ClientSecret:  clientSecret,
		TokenURL:      serverURL + tokenPath,
		AlertsURL:     serverURL + alertsPath,
		PollInterval:  30 * time.Millisecond,
	}
	require.NoError(t, conf.Validate())
	adapter, chStopped, err := newDefenderAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	return adapter, chStopped
}

// --- tests ------------------------------------------------------------------

// TestMockAlertsEndToEnd drives the adapter against the mock Microsoft
// endpoints and asserts the exact events shipped: every alert ships exactly
// once with its payload verbatim (nested evidence included), an
// ingestion-time TimestampMs and no EventType (the adapter sets none); the
// mock observed a valid client_credentials token request and a bearer-token'd
// Graph call carrying the createdDateTime $filter.
func TestMockAlertsEndToEnd(t *testing.T) {
	testStart := time.Now()

	// The adapter only collects alerts created after it starts (its watermark
	// is initialized to "now"), so fixtures carry future createdDateTimes.
	base := time.Now().Add(1 * time.Hour)
	want := []map[string]interface{}{
		realisticAlert("da637551227677560813_-1111111111", base),
		realisticAlert("da637551227677560814_-1111111112", base.Add(1*time.Second)),
		realisticAlert("da637551227677560815_-1111111113", base.Add(2*time.Second)),
	}

	mock := newMockMicrosoft()
	for _, alert := range want {
		mock.appendAlert(alert)
	}
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, sink, mockClientSecret)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond, "expected all 3 alerts to ship")

	// Re-polling must not re-ship: the count stays at 3.
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 20*time.Millisecond, "alerts were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		// The adapter ships alerts without an EventType.
		assert.Equal(t, "", msg.EventType)
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg

		// TimestampMs is ingestion time: between test start and now.
		assert.GreaterOrEqual(t, msg.TimestampMs, uint64(testStart.UnixMilli()))
		assert.LessOrEqual(t, msg.TimestampMs, uint64(time.Now().UnixMilli()))
	}
	require.Len(t, byID, 3, "each alert must ship exactly once")

	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "alert %s was not shipped", id)
		// The payload is shipped verbatim -- nested evidence objects, arrays,
		// nulls and ints included.
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Graph alert")
	}

	// The mock saw the credential flow and the Graph query the adapter sends.
	form := mock.lastSeenTokenForm()
	require.NotNil(t, form)
	assert.Equal(t, mockClientID, form.Get("client_id"))
	assert.Equal(t, mockClientSecret, form.Get("client_secret"))
	assert.Equal(t, "client_credentials", form.Get("grant_type"))
	assert.Equal(t, "https://graph.microsoft.com/.default", form.Get("scope"))
	assert.Equal(t, "Bearer "+mockAccessToken, mock.lastSeenAuthHeader())
	assert.True(t, strings.HasPrefix(mock.lastSeenFilter(), "createdDateTime ge "),
		"Graph call must filter on createdDateTime, got %q", mock.lastSeenFilter())
	// A token is fetched for every poll of the alerts endpoint.
	assert.GreaterOrEqual(t, mock.tokenRequestCount(), 1)
	assert.GreaterOrEqual(t, mock.alertRequestCount(), 1)
}

// TestMockNewAlertMidRunShipsOnce verifies an alert appearing while the
// adapter is running is picked up by a later poll and shipped exactly once,
// without re-shipping the alerts that came before it.
func TestMockNewAlertMidRunShipsOnce(t *testing.T) {
	base := time.Now().Add(1 * time.Hour)

	mock := newMockMicrosoft()
	mock.appendAlert(realisticAlert("da637551227677560813_-2222222221", base))
	mock.appendAlert(realisticAlert("da637551227677560814_-2222222222", base.Add(1*time.Second)))
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, sink, mockClientSecret)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 10*time.Millisecond)

	// A new alert is created while the adapter is running.
	mock.appendAlert(realisticAlert("da637551227677560815_-2222222223", base.Add(2*time.Second)))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 10*time.Millisecond, "the new alert should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 20*time.Millisecond)

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["id"].(string)]++
	}
	assert.Equal(t, map[string]int{
		"da637551227677560813_-2222222221": 1,
		"da637551227677560814_-2222222222": 1,
		"da637551227677560815_-2222222223": 1,
	}, shippedPerID, "every alert must ship exactly once")
}

// TestMockPaginatedDatasetFullyConsumed verifies a result set larger than one
// Graph page is fully collected. The adapter does not follow @odata.nextLink;
// it makes progress because each poll advances the createdDateTime watermark
// to the last alert it saw, so successive polls drain the remainder. The mock
// truncates to a page size and emits @odata.nextLink exactly like Graph does;
// every alert must still ship exactly once.
func TestMockPaginatedDatasetFullyConsumed(t *testing.T) {
	const total = 12
	base := time.Now().Add(1 * time.Hour)

	mock := newMockMicrosoft()
	mock.pageSize = 5
	wantIDs := make([]string, 0, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("da637551227677560813_-33333333%02d", i)
		wantIDs = append(wantIDs, id)
		mock.appendAlert(realisticAlert(id, base.Add(time.Duration(i)*time.Second)))
	}
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, sink, mockClientSecret)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 10*time.Millisecond, "the full paginated dataset should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 20*time.Millisecond, "no alert may ship twice")

	shipped := map[string]int{}
	for _, msg := range sink.snapshot() {
		shipped[msg.JsonPayload["id"].(string)]++
	}
	for _, id := range wantIDs {
		assert.Equal(t, 1, shipped[id], "alert %s must ship exactly once", id)
	}
	assert.GreaterOrEqual(t, mock.nextLinkCount(), 1,
		"the mock must actually have truncated (served @odata.nextLink) for this test to be meaningful")
	assert.GreaterOrEqual(t, mock.alertRequestCount(), 3,
		"consuming 12 alerts at page size 5 requires at least 3 polls")
}

// TestMockBadCredentialsShipNothing verifies the adapter's error handling for
// rejected credentials: the token endpoint rejects the client secret, the
// adapter retries the token fetch 3 times then reports an error for the poll,
// nothing is ever shipped, the Graph endpoint is never called, and -- per the
// adapter's design -- it keeps running (it does not stop itself on auth
// failure; it retries on the next poll).
func TestMockBadCredentialsShipNothing(t *testing.T) {
	mock := newMockMicrosoft()
	mock.appendAlert(realisticAlert("da637551227677560813_-4444444441", time.Now().Add(1*time.Hour)))
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	var errMu sync.Mutex
	var errs []string

	sink := &captureSink{}
	conf := DefenderConfig{
		ClientOptions: testClientOptions(t),
		TenantID:      mockTenantID,
		ClientID:      mockClientID,
		ClientSecret:  "wrong-secret",
		TokenURL:      server.URL + tokenPath,
		AlertsURL:     server.URL + alertsPath,
		PollInterval:  30 * time.Millisecond,
	}
	conf.ClientOptions.OnError = func(err error) {
		t.Logf("ERR: %v", err)
		errMu.Lock()
		errs = append(errs, err.Error())
		errMu.Unlock()
	}
	adapter, chStopped, err := newDefenderAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// The token fetch retries 3 times (with 1s+2s backoff) before reporting
	// the poll as failed.
	require.Eventually(t, func() bool {
		errMu.Lock()
		defer errMu.Unlock()
		for _, e := range errs {
			if strings.Contains(e, "error fetching token after 3 attempts") {
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "the failed token fetch must be reported")

	assert.Equal(t, 0, sink.count(), "nothing may ship when credentials are rejected")
	assert.Equal(t, 0, mock.alertRequestCount(), "the Graph endpoint must not be called without a token")
	assert.GreaterOrEqual(t, mock.tokenRequestCount(), 3, "the token fetch is retried 3 times")

	// The adapter does not stop itself on bad credentials -- it keeps polling.
	select {
	case <-chStopped:
		t.Fatal("adapter stopped on bad credentials; it is designed to keep retrying")
	default:
	}
}

// TestMockNoAlertsShipsNothing verifies that an empty alerts_v2 result set
// polls cleanly: requests are made, nothing ships, and no errors are raised.
func TestMockNoAlertsShipsNothing(t *testing.T) {
	mock := newMockMicrosoft()
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	var errMu sync.Mutex
	var errs []string

	sink := &captureSink{}
	conf := DefenderConfig{
		ClientOptions: testClientOptions(t),
		TenantID:      mockTenantID,
		ClientID:      mockClientID,
		ClientSecret:  mockClientSecret,
		TokenURL:      server.URL + tokenPath,
		AlertsURL:     server.URL + alertsPath,
		PollInterval:  30 * time.Millisecond,
	}
	conf.ClientOptions.OnError = func(err error) {
		t.Logf("ERR: %v", err)
		errMu.Lock()
		errs = append(errs, err.Error())
		errMu.Unlock()
	}
	adapter, _, err := newDefenderAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return mock.alertRequestCount() >= 2 },
		5*time.Second, 10*time.Millisecond, "the adapter should keep polling")

	assert.Equal(t, 0, sink.count(), "an empty result set ships nothing")
	errMu.Lock()
	defer errMu.Unlock()
	assert.Empty(t, errs, "an empty result set is not an error")
}
