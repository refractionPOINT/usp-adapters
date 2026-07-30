package usp_withsecure

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
)

// This file exercises the adapter end-to-end against a mock WithSecure Elements
// API, capturing the exact messages it ships so their content -- event type,
// timestamp and verbatim payload -- can be asserted.
//
// The mock is deliberately strict about what the real API is strict about:
// Basic-auth-only token requests whose body carries nothing beyond
// grant_type/scope, the mandatory User-Agent header, anchor pagination over the
// {"items":[...],"nextAnchor":"..."} envelope, exclusiveStart semantics on the
// timestamp cursor, and the flat {"message","code","transactionId"} error
// object. A test that passes here would not have failed in production for a
// reason the mock quietly tolerated.

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

// byEventType returns the shipped payloads of one event type, in order.
func (s *captureSink) byEventType(eventType string) []utils.Dict {
	out := []utils.Dict{}
	for _, m := range s.snapshot() {
		if m.EventType != eventType {
			continue
		}
		out = append(out, utils.Dict(m.JsonPayload))
	}
	return out
}

// --- mock WithSecure Elements API -------------------------------------------

// mockElements is an in-memory stand-in for the Elements API.
type mockElements struct {
	mu sync.Mutex

	clientID     string
	clientSecret string

	// Datasets, each sorted ascending by its cursor timestamp field.
	securityEvents []utils.Dict
	incidents      []utils.Dict
	auditLogs      []utils.Dict
	// detections is keyed by incidentId.
	detections map[string][]utils.Dict

	// pageSize forces the mock to paginate at this size regardless of the
	// requested limit, so multi-page walks are easy to provoke.
	pageSize int

	// tokenSeq makes every minted token distinct, so a test can tell a reused
	// cached token from a freshly minted one.
	tokenSeq int
	// rejectAuth makes the token endpoint reject the credentials.
	rejectAuth bool
	// failNextWith makes the next data request fail once with this status.
	// Accessed atomically: tests set it while the handler goroutines read it.
	failNextWith int32

	// Request counters and the recorded query of each data request.
	tokenRequests int
	requests      []recordedRequest
}

type recordedRequest struct {
	Method    string
	Path      string
	Query     url.Values
	Form      url.Values
	Auth      string
	UserAgent string
}

// params returns the request's effective parameters: the query string for a
// GET, the form body for a POST.
func (r recordedRequest) params() url.Values {
	if r.Form != nil {
		return r.Form
	}
	return r.Query
}

func newMockElements() *mockElements {
	return &mockElements{
		clientID:     "test-client-id",
		clientSecret: "test-client-secret",
		detections:   map[string][]utils.Dict{},
		pageSize:     100,
	}
}

func (m *mockElements) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(srv.Close)
	return srv
}

func (m *mockElements) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	// The real API rejects any request without a User-Agent, token endpoint
	// included.
	if r.Header.Get("User-Agent") == "" {
		writeAPIError(w, http.StatusBadRequest, "User-Agent header is required.", 400001)
		return
	}

	if strings.HasSuffix(r.URL.Path, tokenPath) {
		m.handleToken(w, r, body)
		return
	}

	var form url.Values
	if r.Method == http.MethodPost {
		form, _ = url.ParseQuery(string(body))
	}

	m.mu.Lock()
	m.requests = append(m.requests, recordedRequest{
		Method: r.Method, Path: strings.TrimPrefix(r.URL.Path, "/"),
		Query: r.URL.Query(), Form: form,
		Auth: r.Header.Get("Authorization"), UserAgent: r.Header.Get("User-Agent"),
	})
	m.mu.Unlock()

	if status := atomic.SwapInt32(&m.failNextWith, 0); status != 0 {
		writeAPIError(w, int(status), "forced failure", 500000)
		return
	}

	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		writeAPIError(w, http.StatusUnauthorized, "Unauthorized.", 401000)
		return
	}

	params := r.URL.Query()
	if form != nil {
		params = form
	}

	switch strings.TrimPrefix(r.URL.Path, "/") {
	case pathSecurityEvents:
		m.serveCursorPage(w, params, m.snapshotEvents(), "persistenceTimestamp", "persistenceTimestampStart")
	case pathIncidents:
		m.serveIncidents(w, params)
	case pathAuditLogs:
		m.serveCursorPage(w, params, m.snapshotAudit(), "serverTimestamp", "serverTimestampStart")
	case pathDetections:
		m.serveDetections(w, params)
	default:
		writeAPIError(w, http.StatusNotFound, "no mock route for "+r.URL.Path, 404000)
	}
}

func (m *mockElements) handleToken(w http.ResponseWriter, r *http.Request, body []byte) {
	form, _ := url.ParseQuery(string(body))
	user, pass, hasBasic := r.BasicAuth()

	m.mu.Lock()
	m.tokenRequests++
	m.tokenSeq++
	seq := m.tokenSeq
	reject := m.rejectAuth
	wantID, wantSecret := m.clientID, m.clientSecret
	m.mu.Unlock()

	// The credentials must arrive as HTTP Basic, not in the body.
	if !hasBasic {
		writeJSON(w, http.StatusUnauthorized,
			`{"error":"invalid_client","error_description":"Client authentication failed"}`)
		return
	}
	// The documented rejection: anything beyond grant_type/scope in the payload.
	for k := range form {
		if k != "grant_type" && k != "scope" {
			writeJSON(w, http.StatusBadRequest,
				`{"error":"invalid_request","error_description":"unexpected parameter `+k+` in payload"}`)
			return
		}
	}
	if form.Get("grant_type") != "client_credentials" {
		writeJSON(w, http.StatusBadRequest,
			`{"error":"unsupported_grant_type","error_description":"bad grant_type"}`)
		return
	}
	if reject || user != wantID || pass != wantSecret {
		writeJSON(w, http.StatusUnauthorized,
			`{"error":"invalid_client","error_description":"Client authentication failed"}`)
		return
	}

	writeJSON(w, http.StatusOK, fmt.Sprintf(
		`{"token_type":"Bearer","expires_in":1797,"access_token":"ws-token-%d"}`, seq))
}

// serveCursorPage implements the shared shape of the timestamp-cursor
// endpoints: filter by a start bound (exclusive when exclusiveStart=true),
// order, then page with an anchor.
func (m *mockElements) serveCursorPage(w http.ResponseWriter, params url.Values, dataset []utils.Dict, tsField, startParam string) {
	start := params.Get(startParam)
	exclusive := params.Get("exclusiveStart") == "true"

	filtered := make([]utils.Dict, 0, len(dataset))
	for _, d := range dataset {
		ts := d.FindOneString(tsField)
		if start != "" {
			if exclusive && ts <= start {
				continue
			}
			if !exclusive && ts < start {
				continue
			}
		}
		filtered = append(filtered, d)
	}
	sortByField(filtered, tsField, params.Get("order") != "desc")
	m.writePage(w, filtered, params)
}

func (m *mockElements) serveIncidents(w http.ResponseWriter, params url.Values) {
	dataset := m.snapshotIncidents()
	if params.Get("archived") == "false" {
		kept := make([]utils.Dict, 0, len(dataset))
		for _, d := range dataset {
			if archived, _ := d["archived"].(bool); !archived {
				kept = append(kept, d)
			}
		}
		dataset = kept
	}
	m.serveCursorPage(w, params, dataset, "updatedTimestamp", "updatedTimestampStart")
}

func (m *mockElements) serveDetections(w http.ResponseWriter, params url.Values) {
	incidentID := params.Get("incidentId")
	if incidentID == "" {
		writeAPIError(w, http.StatusBadRequest, "incidentId is required", 400000)
		return
	}
	m.mu.Lock()
	items := append([]utils.Dict(nil), m.detections[incidentID]...)
	m.mu.Unlock()
	m.writePage(w, items, params)
}

// writePage slices the result set the way the API does: an opaque anchor
// pointing at the next offset, omitted entirely on the last page.
func (m *mockElements) writePage(w http.ResponseWriter, items []utils.Dict, params url.Values) {
	m.mu.Lock()
	pageSize := m.pageSize
	m.mu.Unlock()

	if limit, err := strconv.Atoi(params.Get("limit")); err == nil && limit > 0 && limit < pageSize {
		pageSize = limit
	}

	offset := 0
	if a := params.Get("anchor"); a != "" && a != "no-value" {
		// The anchor is opaque to the client; the mock encodes an offset in it.
		if n, err := strconv.Atoi(strings.TrimPrefix(a, "offset:")); err == nil {
			offset = n
		}
	}
	if offset > len(items) {
		offset = len(items)
	}
	end := offset + pageSize
	if end > len(items) {
		end = len(items)
	}

	page := map[string]interface{}{"items": items[offset:end]}
	if end < len(items) {
		page["nextAnchor"] = "offset:" + strconv.Itoa(end)
	}
	body, err := json.Marshal(page)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error(), 500000)
		return
	}
	writeJSON(w, http.StatusOK, string(body))
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write([]byte(body)) //nolint:errcheck
}

// writeAPIError emits the flat Elements error envelope, which carries a numeric
// code rather than the nested {"error":{...}} shape most vendors use.
func writeAPIError(w http.ResponseWriter, status int, message string, code int) {
	writeJSON(w, status, fmt.Sprintf(
		`{"message":%q,"code":%d,"transactionId":"0000-mock"}`, message, code))
}

func sortByField(items []utils.Dict, field string, ascending bool) {
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i].FindOneString(field), items[j].FindOneString(field)
		if ascending {
			return a < b
		}
		return a > b
	})
}

// --- dataset helpers --------------------------------------------------------

func (m *mockElements) snapshotEvents() []utils.Dict {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]utils.Dict(nil), m.securityEvents...)
}

func (m *mockElements) snapshotIncidents() []utils.Dict {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]utils.Dict(nil), m.incidents...)
}

func (m *mockElements) snapshotAudit() []utils.Dict {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]utils.Dict(nil), m.auditLogs...)
}

func (m *mockElements) addSecurityEvents(items ...utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.securityEvents = append(m.securityEvents, items...)
}

func (m *mockElements) addIncidents(items ...utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incidents = append(m.incidents, items...)
}

func (m *mockElements) addAuditLogs(items ...utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.auditLogs = append(m.auditLogs, items...)
}

func (m *mockElements) setDetections(incidentID string, items ...utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.detections[incidentID] = items
}

// updateIncident replaces an incident, simulating a BCD that evolved.
func (m *mockElements) updateIncident(incidentID string, updated utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, inc := range m.incidents {
		if inc.FindOneString("incidentId") == incidentID {
			m.incidents[i] = updated
			return
		}
	}
	m.incidents = append(m.incidents, updated)
}

// setFailNext makes the next data request fail once with the given status.
func (m *mockElements) setFailNext(status int32) {
	atomic.StoreInt32(&m.failNextWith, status)
}

func (m *mockElements) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

func (m *mockElements) recordedRequests() []recordedRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]recordedRequest(nil), m.requests...)
}

func (m *mockElements) tokenRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenRequests
}

// requestsFor returns the recorded requests against one API path.
func (m *mockElements) requestsFor(path string) []recordedRequest {
	out := []recordedRequest{}
	for _, r := range m.recordedRequests() {
		if r.Path == path {
			out = append(out, r)
		}
	}
	return out
}

// --- realistic fixtures -----------------------------------------------------

// elementsTime renders a timestamp the way the Elements API does: RFC 3339 UTC
// with millisecond precision and a Z suffix.
func elementsTime(ts time.Time) string {
	return ts.UTC().Format("2006-01-02T15:04:05.000Z")
}

// realisticSecurityEvent is shaped like a real EPP security event, following the
// example published in the Elements API specification: nested organization /
// device / target objects, a free-form details map, and mixed scalar types.
func realisticSecurityEvent(id string, ts time.Time) utils.Dict {
	return utils.Dict{
		"id":                   id,
		"action":               "blocked",
		"engine":               "deepGuard",
		"severity":             "warning",
		"serverTimestamp":      elementsTime(ts),
		"persistenceTimestamp": elementsTime(ts),
		"clientTimestamp":      elementsTime(ts.Add(-2 * time.Second)),
		"eventTransactionId":   "0000-187cf62797634fef",
		"acknowledged":         false,
		"message":              "DeepGuard blocked a harmful application",
		"description":          "DeepGuard event",
		"organization": map[string]interface{}{
			"id":   "222298dc-3785-4d2d-a957-f7399c2cd084",
			"name": "example-org",
		},
		"device": map[string]interface{}{
			"id":     "71cceb9f-2728-4ac4-8e63-402cbbf76e18",
			"name":   "DESKTOP-3D64DAK",
			"labels": []interface{}{"finance"},
		},
		"target": map[string]interface{}{
			"id":   "71cceb9f-2728-4ac4-8e63-402cbbf76e18",
			"name": "DESKTOP-3D64DAK",
		},
		"userName": "EXAMPLE\\jdoe",
		"details": map[string]interface{}{
			"path":           "C:\\Users\\jdoe\\AppData\\Local\\Temp\\evil.exe",
			"alertType":      "deepguard.harmful.blocked",
			"throttledCount": "0",
			"profileId":      "12444818",
			"hostIpAddress":  "10.1.2.3/24",
			// A large integer, to prove payloads round-trip without float
			// coercion losing precision.
			"fileSize": 9007199254740993,
		},
	}
}

// realisticIncident is shaped like a real Broad Context Detection.
func realisticIncident(id string, created, updated time.Time) utils.Dict {
	return utils.Dict{
		"incidentId":               id,
		"incidentPublicId":         "3599-4F9929A4",
		"organizationId":           "222298dc-3785-4d2d-a957-f7399c2cd084",
		"name":                     "Incident on DESKTOP-3D64DAK",
		"status":                   "new",
		"severity":                 "high",
		"riskLevel":                "severe",
		"riskScore":                74.86341234,
		"resolution":               "unconfirmed",
		"archived":                 false,
		"createdTimestamp":         elementsTime(created),
		"updatedTimestamp":         elementsTime(updated),
		"initialReceivedTimestamp": elementsTime(created),
		"categories":               []interface{}{"CREDENTIAL_THEFT"},
		"sources":                  []interface{}{"endpoint"},
	}
}

// realisticDetection is shaped like a real detection under a BCD.
func realisticDetection(detectionID, incidentID string, ts time.Time) utils.Dict {
	return utils.Dict{
		"detectionId":              detectionID,
		"incidentId":               incidentID,
		"deviceId":                 "3a8f06e1-c3e8-4933-b617-e23597111644",
		"name":                     "suspiciousPowershellCommand",
		"detectionClass":           "PROCESS",
		"severity":                 "medium",
		"riskLevel":                "medium",
		"exePath":                  "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe",
		"exeName":                  "powershell.exe",
		"exeHash":                  "c8f21ef51fa2f2033a9ca7c0cc0412c25065e2ba",
		"cmdl":                     "powershell -enc SQBFAFgA",
		"pid":                      1234,
		"username":                 "EXAMPLE\\jdoe",
		"privileges":               "NormalPrivileges",
		"createdTimestamp":         elementsTime(ts),
		"initialReceivedTimestamp": elementsTime(ts),
		"activityContext": []interface{}{
			map[string]interface{}{"type": "elevation", "description": "Elevation of privileges"},
		},
	}
}

// realisticAuditLog is shaped like a real Elements audit entry.
func realisticAuditLog(id string, ts time.Time) utils.Dict {
	return utils.Dict{
		"id":              id,
		"serverTimestamp": elementsTime(ts),
		"transactionId":   "0000-abcdef123456",
		"namespace":       "devices",
		"action":          "isolateFromNetwork",
		"username":        "admin@example.test",
		"description":     "Device isolated from network",
		"organization": map[string]interface{}{
			"id":      "222298dc-3785-4d2d-a957-f7399c2cd084",
			"name":    "example-org",
			"orgPath": "example-org",
		},
		"target": map[string]interface{}{
			"id":   "3a8f06e1-c3e8-4933-b617-e23597111644",
			"name": "DESKTOP-3D64DAK",
		},
	}
}

// --- assertions -------------------------------------------------------------

// assertVerbatim checks that a shipped payload is byte-for-byte the record the
// API served. Adapters must not reshape payloads -- LimaCharlie maps fields in
// the cloud.
func assertVerbatim(t *testing.T, want, got utils.Dict) {
	t.Helper()
	wantJSON, err := json.Marshal(want)
	if !assert.NoError(t, err) {
		return
	}
	gotJSON, err := json.Marshal(got)
	if !assert.NoError(t, err) {
		return
	}
	assert.JSONEq(t, string(wantJSON), string(gotJSON), "payload was not shipped verbatim")
}
