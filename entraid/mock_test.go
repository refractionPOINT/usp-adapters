package usp_entraid

// This file exercises the adapter end-to-end against a mock of the Microsoft
// endpoints it talks to:
//
//   - the Microsoft identity platform token endpoint
//     (POST /<tenant>/oauth2/v2.0/token, OAuth2 client_credentials), and
//   - the Microsoft Graph collections the adapter's streams poll:
//     /v1.0/identityProtection/riskDetections (filtered on activityDateTime),
//     /v1.0/auditLogs/signIns (filtered on createdDateTime) and
//     /v1.0/auditLogs/directoryAudits (filtered on activityDateTime).
//
// The mock validates the credential exchange and the bearer token, honours the
// adapter's $filter (inclusive "ge", as OData defines it), orders results by
// the collection's timestamp field ascending and can truncate responses to a
// page size, advertising the Graph "@odata.nextLink" continuation (with a
// working $skiptoken) the real API returns.
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
	"strconv"
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

	// expectScope is the OAuth2 scope the token endpoint accepts; anything
	// else is rejected with AADSTS70011, like a real tenant handed a scope
	// belonging to a different national cloud deployment.
	expectScope string

	// detections, signIns and audits are the in-memory Graph datasets, one
	// per collection the adapter can poll.
	detections []map[string]interface{}
	signIns    []map[string]interface{}
	audits     []map[string]interface{}

	// pageSize, when > 0, caps how many detections a single response carries;
	// truncated responses include an @odata.nextLink, like the real Graph API
	// (the documented default page size for this endpoint is 20 objects, and
	// the maximum with $top is 500; tests use a tiny page to force paging).
	pageSize int

	// revokeGraphAccess makes the Graph endpoint reject every bearer token
	// (e.g. the app registration lost its IdentityRiskEvent.Read.All grant).
	revokeGraphAccess bool

	// descendingDefault serves results newest-first when the request carries
	// no $orderby, mirroring how Graph commonly returns the auditLogs
	// collections. An explicit $orderby always wins.
	descendingDefault bool

	// lastQueryByPath records the most recent raw query string per Graph
	// path, so tests can assert on the filter/orderby the adapter sent.
	lastQueryByPath map[string]string

	tokenRequests int
	graphRequests int

	// lastTokenForm is the most recent decoded body of a token request, kept
	// so tests can assert the exact client_credentials exchange.
	lastTokenForm url.Values

	serverURL string
}

func newMockMicrosoft() *mockMicrosoft {
	return &mockMicrosoft{
		tenantID:        testTenantID,
		clientID:        testClientID,
		clientSecret:    testClientSecret,
		accessToken:     testAccessToken,
		expectScope:     "https://graph.microsoft.com/.default",
		lastQueryByPath: map[string]string{},
	}
}

func (m *mockMicrosoft) lastQuery(path string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastQueryByPath[path]
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

func (m *mockMicrosoft) addSignIn(d map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.signIns = append(m.signIns, d)
}

func (m *mockMicrosoft) addAudit(d map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.audits = append(m.audits, d)
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

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case tokenPath:
			m.handleToken(w, r)
		case "/v1.0/identityProtection/riskDetections":
			m.handleCollection(w, r, func() []map[string]interface{} { return m.detections }, "activityDateTime")
		case "/v1.0/auditLogs/signIns":
			m.handleCollection(w, r, func() []map[string]interface{} { return m.signIns }, "createdDateTime")
		case "/v1.0/auditLogs/directoryAudits":
			m.handleCollection(w, r, func() []map[string]interface{} { return m.audits }, "activityDateTime")
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
	expectScope := m.expectScope
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
	if r.PostForm.Get("scope") != expectScope {
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

// handleCollection implements a GET on a Graph collection with the $filter the
// adapter sends: "<tsField> ge <timestamp>". Truncated responses advertise an
// @odata.nextLink whose $skiptoken encodes the offset into the matched set,
// like the real API's server-driven paging.
func (m *mockMicrosoft) handleCollection(w http.ResponseWriter, r *http.Request, dataset func() []map[string]interface{}, tsField string) {
	m.mu.Lock()
	m.graphRequests++
	m.lastQueryByPath[r.URL.Path] = r.URL.RawQuery
	token := m.accessToken
	revoked := m.revokeGraphAccess
	pageSize := m.pageSize
	descendingDefault := m.descendingDefault
	items := make([]map[string]interface{}, len(dataset()))
	copy(items, dataset())
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

	since, ok := parseTimestampFilter(r.URL.Query().Get("$filter"), tsField)
	if !ok {
		writeGraphError(w, http.StatusBadRequest, "BadRequest", "Invalid $filter clause.")
		return
	}

	// "ge" is inclusive (greater than or equal, as OData defines it). The
	// mock returns matches ordered by the timestamp field ascending: the
	// official docs make no ordering guarantee for these endpoints, but
	// keeping them ordered makes the tests deterministic.
	matched := make([]map[string]interface{}, 0, len(items))
	for _, d := range items {
		at, err := time.Parse(time.RFC3339, d[tsField].(string))
		if err != nil {
			continue
		}
		if !at.Before(since) {
			matched = append(matched, d)
		}
	}
	// Ordering: an explicit $orderby ("<field> asc" or "<field> desc") is
	// honoured; without one the mock defaults to ascending unless the test
	// opted into descendingDefault (Graph's common newest-first behavior).
	descending := descendingDefault
	if orderBy := r.URL.Query().Get("$orderby"); orderBy != "" {
		if orderBy != tsField+" asc" && orderBy != tsField+" desc" {
			writeGraphError(w, http.StatusBadRequest, "BadRequest", "Unsupported $orderby: "+orderBy)
			return
		}
		descending = strings.HasSuffix(orderBy, " desc")
	}
	sort.SliceStable(matched, func(i, j int) bool {
		ti, _ := time.Parse(time.RFC3339, matched[i][tsField].(string))
		tj, _ := time.Parse(time.RFC3339, matched[j][tsField].(string))
		if descending {
			return tj.Before(ti)
		}
		return ti.Before(tj)
	})

	offset := 0
	if skip := r.URL.Query().Get("$skiptoken"); skip != "" {
		n, err := strconv.Atoi(strings.TrimPrefix(skip, "offset-"))
		if err != nil || n < 0 || n > len(matched) {
			writeGraphError(w, http.StatusBadRequest, "BadRequest", "Invalid $skiptoken.")
			return
		}
		offset = n
	}
	matched = matched[offset:]

	envelope := map[string]interface{}{
		"@odata.context": "https://graph.microsoft.com/v1.0/$metadata" + r.URL.Path,
	}
	if pageSize > 0 && len(matched) > pageSize {
		matched = matched[:pageSize]
		// Like the real API, the nextLink carries the query parameters of the
		// original request plus a $skiptoken (see
		// https://learn.microsoft.com/en-us/graph/paging).
		q := url.Values{}
		q.Set("$filter", r.URL.Query().Get("$filter"))
		if orderBy := r.URL.Query().Get("$orderby"); orderBy != "" {
			q.Set("$orderby", orderBy)
		}
		q.Set("$skiptoken", fmt.Sprintf("offset-%d", offset+pageSize))
		envelope["@odata.nextLink"] = serverURL + r.URL.Path + "?" + q.Encode()
	}
	envelope["value"] = matched

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(envelope)
}

// parseTimestampFilter extracts the timestamp from a "<tsField> ge <ts>" OData
// filter.
func parseTimestampFilter(filter string, tsField string) (time.Time, bool) {
	prefix := tsField + " ge "
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
		"source":              "activeDirectory",
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
		// additionalInfo is a JSON-formatted *string*, not a nested object;
		// this is the example value from the official riskDetection docs.
		"additionalInfo": `[{"Key":"userAgent","Value":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36"}]`,
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

// realisticSignIn builds a record shaped like a Microsoft Graph auditLogs
// signIn resource. All identifying values are fake.
func realisticSignIn(seq int, upn, createdDateTime string) map[string]interface{} {
	return map[string]interface{}{
		"id":                      fmt.Sprintf("%056d%04d-sgn", 0, seq),
		"createdDateTime":         createdDateTime,
		"userDisplayName":         "Jane Doe",
		"userPrincipalName":       upn,
		"userId":                  "11111111-1111-1111-1111-111111111111",
		"appId":                   "11111111-1111-1111-1111-333333333333",
		"appDisplayName":          "Office 365 Exchange Online",
		"ipAddress":               fmt.Sprintf("203.0.113.%d", seq%250+1),
		"clientAppUsed":           "Browser",
		"correlationId":           "11111111-1111-1111-1111-111111111111",
		"conditionalAccessStatus": "success",
		"isInteractive":           true,
		"riskDetail":              "none",
		"riskLevelAggregated":     "none",
		"riskLevelDuringSignIn":   "none",
		"riskState":               "none",
		"status": map[string]interface{}{
			"errorCode":         0,
			"failureReason":     "Other.",
			"additionalDetails": nil,
		},
		"deviceDetail": map[string]interface{}{
			"deviceId":        "",
			"displayName":     "",
			"operatingSystem": "Windows 10",
			"browser":         "Edge 138.0.0",
		},
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

// realisticDirectoryAudit builds a record shaped like a Microsoft Graph
// auditLogs directoryAudit resource. All identifying values are fake.
func realisticDirectoryAudit(seq int, activityDisplayName, activityDateTime string) map[string]interface{} {
	return map[string]interface{}{
		"id":                  fmt.Sprintf("Directory_%04d", seq),
		"category":            "UserManagement",
		"correlationId":       "11111111-1111-1111-1111-111111111111",
		"result":              "success",
		"resultReason":        "",
		"activityDisplayName": activityDisplayName,
		"activityDateTime":    activityDateTime,
		"loggedByService":     "Core Directory",
		"operationType":       "Add",
		"initiatedBy": map[string]interface{}{
			"user": map[string]interface{}{
				"id":                "11111111-1111-1111-1111-111111111111",
				"displayName":       nil,
				"userPrincipalName": "admin@example.com",
			},
		},
		"targetResources": []interface{}{
			map[string]interface{}{
				"id":                "11111111-1111-1111-1111-444444444444",
				"displayName":       nil,
				"type":              "User",
				"userPrincipalName": "new.user@example.com",
				"modifiedProperties": []interface{}{
					map[string]interface{}{
						"displayName": "AccountEnabled",
						"oldValue":    nil,
						"newValue":    "[true]",
					},
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

// TestGCCHighScope drives the adapter against a mock that only accepts the GCC
// High scope, standing in for a US Government L4 tenant: the token exchange
// there rejects the commercial scope with AADSTS70011. The hosts still point at
// the mock (the real graph.microsoft.us is not reachable from tests), so what
// is under test is that endpoint=gcc-high-gov moves the OAuth2 scope -- the
// half a login_endpoint/graph_endpoint override cannot reach.
func TestGCCHighScope(t *testing.T) {
	base := fixtureBaseTime()
	detection := realisticRiskDetection(1, "anonymizedIPAddress", "jdoe@example.com", base.Format(graphTimeLayout))

	t.Run("endpoint gcc-high-gov authenticates and ships", func(t *testing.T) {
		mock := newMockMicrosoft()
		mock.expectScope = "https://graph.microsoft.us/.default"
		mock.addDetection(detection)
		server := mock.start(t)

		conf := testConfig(t, server.URL)
		conf.Endpoint = "gcc-high-gov"

		sink := &captureSink{}
		adapter, _, err := newEntraIDAdapter(context.Background(), conf, sink)
		require.NoError(t, err)
		defer adapter.Close()

		require.Eventually(t, func() bool { return sink.count() == 1 },
			10*time.Second, 20*time.Millisecond, "expected the detection to ship against the GCC High endpoint")

		form := mock.lastTokenRequest()
		require.NotNil(t, form)
		assert.Equal(t, "https://graph.microsoft.us/.default", form.Get("scope"))
	})

	t.Run("commercial default is rejected by a GCC High tenant", func(t *testing.T) {
		mock := newMockMicrosoft()
		mock.expectScope = "https://graph.microsoft.us/.default"
		mock.addDetection(detection)
		server := mock.start(t)

		// Same config, but without endpoint: only the hosts are overridden, so
		// the adapter still asks for the commercial scope and never gets a
		// token. Nothing ships.
		sink := &captureSink{}
		adapter, _, err := newEntraIDAdapter(context.Background(), testConfig(t, server.URL), sink)
		require.NoError(t, err)
		defer adapter.Close()

		require.Never(t, func() bool { return sink.count() != 0 },
			500*time.Millisecond, 25*time.Millisecond, "no event can ship without a valid token")

		form := mock.lastTokenRequest()
		require.NotNil(t, form)
		assert.Equal(t, "https://graph.microsoft.com/.default", form.Get("scope"))
	})
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
// and advertises @odata.nextLink like the real API; the adapter follows the
// continuations within a poll, and every detection must ship exactly once.
func TestPaginatedResultSetFullyConsumed(t *testing.T) {
	const total = 5

	mock := newMockMicrosoft()
	mock.pageSize = 2
	// risk_detections sends no $orderby (historical request shape), so serve
	// newest-first to prove draining does not depend on response order.
	mock.descendingDefault = true
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

	// Draining a truncated result set takes multiple Graph requests
	// (@odata.nextLink continuations within the poll).
	assert.GreaterOrEqual(t, mock.graphRequestCount(), 3,
		"consuming the set should take several Graph requests when responses are truncated")

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

// TestSignInsStreamEndToEnd verifies the sign_ins stream polls
// auditLogs/signIns (filtered on createdDateTime), ships every sign-in
// verbatim exactly once, and does not touch the risk detections dataset.
func TestSignInsStreamEndToEnd(t *testing.T) {
	mock := newMockMicrosoft()
	// Graph commonly returns auditLogs collections newest-first; the adapter
	// counters with an explicit $orderby, which the mock honours over this.
	mock.descendingDefault = true
	base := fixtureBaseTime()
	// A risk detection that must NOT ship: the stream selection excludes it.
	mock.addDetection(realisticRiskDetection(1, "anonymizedIPAddress", "jdoe@example.com", base.Format(graphTimeLayout)))
	want := []map[string]interface{}{
		realisticSignIn(1, "jdoe@example.com", base.Format(graphTimeLayout)),
		realisticSignIn(2, "asmith@example.com", base.Add(1*time.Minute).Format(graphTimeLayout)),
	}
	for _, s := range want {
		mock.addSignIn(s)
	}
	server := mock.start(t)

	conf := testConfig(t, server.URL)
	conf.Streams = "sign_ins"

	sink := &captureSink{}
	adapter, _, err := newEntraIDAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		10*time.Second, 20*time.Millisecond, "expected both sign-ins to ship")
	require.Never(t, func() bool { return sink.count() != 2 },
		400*time.Millisecond, 30*time.Millisecond, "sign-ins were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		id, _ := msg.JsonPayload["id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 2)
	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "sign-in %s was not shipped", id)
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Graph signIn")
	}

	// The adapter must request a deterministic oldest-first ordering so
	// truncated result sets drain correctly across polls.
	query, err := url.ParseQuery(mock.lastQuery("/v1.0/auditLogs/signIns"))
	require.NoError(t, err)
	assert.Equal(t, "createdDateTime asc", query.Get("$orderby"))
	assert.Contains(t, query.Get("$filter"), "createdDateTime ge ")
}

// TestAllStreamsShipConcurrently verifies that configuring all three streams
// ships risk detections, sign-ins and directory audits, each exactly once.
func TestAllStreamsShipConcurrently(t *testing.T) {
	mock := newMockMicrosoft()
	base := fixtureBaseTime()
	mock.addDetection(realisticRiskDetection(1, "anonymizedIPAddress", "jdoe@example.com", base.Format(graphTimeLayout)))
	mock.addSignIn(realisticSignIn(2, "asmith@example.com", base.Add(1*time.Minute).Format(graphTimeLayout)))
	mock.addAudit(realisticDirectoryAudit(3, "Add user", base.Add(2*time.Minute).Format(graphTimeLayout)))
	server := mock.start(t)

	conf := testConfig(t, server.URL)
	conf.Streams = "risk_detections, sign_ins,audit_logs"

	sink := &captureSink{}
	adapter, _, err := newEntraIDAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		10*time.Second, 20*time.Millisecond, "expected one event from each stream")
	require.Never(t, func() bool { return sink.count() != 3 },
		400*time.Millisecond, 30*time.Millisecond, "events were re-shipped on a later poll")

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["id"].(string)]++
	}
	require.Len(t, shippedPerID, 3)
	for id, n := range shippedPerID {
		assert.Equal(t, 1, n, "event %q must ship exactly once", id)
	}
}

// TestSameTimestampEventsShipOnce verifies the cursor dedup handles many
// events sharing one timestamp: with an inclusive "ge" filter they are
// refetched every poll and must still ship exactly once. Sign-ins commonly
// carry second-resolution timestamps, making this the norm, not the edge.
func TestSameTimestampEventsShipOnce(t *testing.T) {
	const total = 5

	mock := newMockMicrosoft()
	base := fixtureBaseTime()
	ts := base.Format(graphTimeLayout)
	for i := 1; i <= total; i++ {
		mock.addSignIn(realisticSignIn(i, fmt.Sprintf("user%d@example.com", i), ts))
	}
	server := mock.start(t)

	conf := testConfig(t, server.URL)
	conf.Streams = "sign_ins"

	sink := &captureSink{}
	adapter, _, err := newEntraIDAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 20*time.Millisecond, "all same-timestamp sign-ins should ship")
	require.Never(t, func() bool { return sink.count() != total },
		600*time.Millisecond, 30*time.Millisecond, "same-timestamp sign-ins were re-shipped")

	// A new sign-in at the same timestamp must still ship exactly once.
	mock.addSignIn(realisticSignIn(total+1, "late@example.com", ts))
	require.Eventually(t, func() bool { return sink.count() == total+1 },
		10*time.Second, 20*time.Millisecond, "the late same-timestamp sign-in should ship")
	require.Never(t, func() bool { return sink.count() != total+1 },
		400*time.Millisecond, 30*time.Millisecond, "sign-ins were re-shipped after the late arrival")

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["id"].(string)]++
	}
	require.Len(t, shippedPerID, total+1)
	for id, n := range shippedPerID {
		assert.Equal(t, 1, n, "sign-in %q must ship exactly once", id)
	}
}

// TestLateArrivalWithinLookbackShips verifies an event that surfaces in the
// API with a timestamp OLDER than events already shipped (Graph ingestion
// delay) still ships: the poll window looks back behind the newest timestamp
// seen instead of only moving forward. A forward-only cursor would exclude
// such an event forever.
func TestLateArrivalWithinLookbackShips(t *testing.T) {
	mock := newMockMicrosoft()
	base := fixtureBaseTime()
	mock.addSignIn(realisticSignIn(1, "jdoe@example.com", base.Format(graphTimeLayout)))
	mock.addSignIn(realisticSignIn(2, "asmith@example.com", base.Add(2*time.Minute).Format(graphTimeLayout)))
	server := mock.start(t)

	conf := testConfig(t, server.URL)
	conf.Streams = "sign_ins"

	sink := &captureSink{}
	adapter, _, err := newEntraIDAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		10*time.Second, 20*time.Millisecond, "initial sign-ins should ship")

	// A sign-in surfaces late: its createdDateTime sits BETWEEN the two
	// already-shipped events, i.e. behind the newest timestamp seen.
	mock.addSignIn(realisticSignIn(3, "late@example.com", base.Add(1*time.Minute).Format(graphTimeLayout)))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		10*time.Second, 20*time.Millisecond, "the late-arriving sign-in should ship")
	require.Never(t, func() bool { return sink.count() != 3 },
		400*time.Millisecond, 30*time.Millisecond, "sign-ins were re-shipped after the late arrival")

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["id"].(string)]++
	}
	require.Len(t, shippedPerID, 3)
	for id, n := range shippedPerID {
		assert.Equal(t, 1, n, "sign-in %q must ship exactly once", id)
	}
}

// TestPaginatedSameTimestampSetFullyConsumed verifies a same-timestamp result
// set larger than one page is fully consumed within a poll via
// @odata.nextLink: the timestamp cursor alone cannot make progress here, so
// only the continuation following drains it.
func TestPaginatedSameTimestampSetFullyConsumed(t *testing.T) {
	const total = 5

	mock := newMockMicrosoft()
	mock.pageSize = 2
	base := fixtureBaseTime()
	ts := base.Format(graphTimeLayout)
	for i := 1; i <= total; i++ {
		mock.addSignIn(realisticSignIn(i, fmt.Sprintf("user%d@example.com", i), ts))
	}
	server := mock.start(t)

	conf := testConfig(t, server.URL)
	conf.Streams = "sign_ins"

	sink := &captureSink{}
	adapter, _, err := newEntraIDAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 20*time.Millisecond, "all paginated same-timestamp sign-ins should ship")
	require.Never(t, func() bool { return sink.count() != total },
		400*time.Millisecond, 30*time.Millisecond, "sign-ins were re-shipped")
}

// TestDirectoryAuditsStreamEndToEnd verifies the audit_logs stream polls
// auditLogs/directoryAudits (filtered on activityDateTime) and ships the
// audit events verbatim.
func TestDirectoryAuditsStreamEndToEnd(t *testing.T) {
	mock := newMockMicrosoft()
	base := fixtureBaseTime()
	want := realisticDirectoryAudit(1, "Add user", base.Format(graphTimeLayout))
	mock.addAudit(want)
	server := mock.start(t)

	conf := testConfig(t, server.URL)
	conf.Streams = "audit_logs"

	sink := &captureSink{}
	adapter, _, err := newEntraIDAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		10*time.Second, 20*time.Millisecond, "expected the directory audit to ship")
	require.Never(t, func() bool { return sink.count() != 1 },
		400*time.Millisecond, 30*time.Millisecond, "the directory audit was re-shipped")

	msg := sink.snapshot()[0]
	assert.JSONEq(t, mustJSON(t, want), mustJSON(t, msg.JsonPayload),
		"shipped payload must match the original Graph directoryAudit")
}
