package usp_cylance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock of the Cylance
// (BlackBerry Protect) API, capturing the exact messages it ships so their
// content -- payload, ObjectSource/ObjectType tagging and timestamp -- can be
// asserted without live credentials.
//
// The mock reproduces the API surface the adapter uses:
//
//   - POST /auth/v2/token        -- exchanges an HS256 application JWT (signed
//     with the app secret, sub=app id, tid=tenant id) for an access token.
//   - GET  /detections/v2        -- paginated list, ?page&page_size&start,
//     RFC3339 "ReceivedTime", envelope {page_items, page_number, total_pages,
//     page_size, total_number_of_items}.
//   - GET  /detections/v2/{id}/details -- single-object detail.
//   - GET  /threats/v2 and /threats/v2/{sha256} -- same envelope, but
//     "last_found" uses the zone-less "2006-01-02T15:04:05" format.
//   - GET  /memoryprotection/v2 and /memoryprotection/v2/{id} -- as threats,
//     keyed by "device_image_file_event_id" / "created".
//
// Time-filter fidelity: the real detections API documents the ?start query
// parameter, so the mock honors it there. The real threats and memory
// protection list APIs only document ?page and ?page_size -- the ?start_time
// parameter the adapter sends is an unknown parameter the real server ignores,
// so the mock ignores it too (the adapter filters by time client-side).
//
// All list/detail endpoints require "Authorization: Bearer <issued token>".

// legacyTimeFormat is the zone-less timestamp format Cylance uses on the
// threats and memory protection APIs.
const legacyTimeFormat = "2006-01-02T15:04:05"

// Clearly-fake identifiers for fixtures (public repo: no real values).
const (
	testTenantID  = "11111111-1111-1111-1111-111111111111"
	testAppID     = "22222222-2222-2222-2222-222222222222"
	testAppSecret = "33333333-3333-3333-3333-333333333333"
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

// --- mock Cylance API ---------------------------------------------------------

type mockCylance struct {
	t *testing.T

	appID     string
	tenantID  string
	appSecret string

	mu          sync.Mutex
	validTokens map[string]bool
	authCount   int
	dataCount   int
	listPages   map[string]map[int]bool // list path -> page numbers requested

	detections       []utils.Dict
	detectionDetails map[string]utils.Dict
	threats          []utils.Dict
	threatDetails    map[string]utils.Dict
	memProtects      []utils.Dict
	memProtectInfo   map[string]utils.Dict
}

func newMockCylance(t *testing.T) *mockCylance {
	return &mockCylance{
		t:                t,
		appID:            testAppID,
		tenantID:         testTenantID,
		appSecret:        testAppSecret,
		validTokens:      map[string]bool{},
		listPages:        map[string]map[int]bool{},
		detectionDetails: map[string]utils.Dict{},
		threatDetails:    map[string]utils.Dict{},
		memProtectInfo:   map[string]utils.Dict{},
	}
}

// addDetection registers a detection list item and its detail document.
func (m *mockCylance) addDetection(id string, received time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.detections = append(m.detections, fixtureDetection(id, received))
	m.detectionDetails[id] = fixtureDetectionDetail(id, received)
}

// addDetectionWithoutDetail registers a detection whose detail lookup 404s.
func (m *mockCylance) addDetectionWithoutDetail(id string, received time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.detections = append(m.detections, fixtureDetection(id, received))
}

func (m *mockCylance) addThreat(sha256 string, lastFound time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.threats = append(m.threats, fixtureThreat(sha256, lastFound))
	m.threatDetails[sha256] = fixtureThreatDetail(sha256, lastFound)
}

func (m *mockCylance) addMemProtect(id string, created time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.memProtects = append(m.memProtects, fixtureMemProtect(id, created))
	m.memProtectInfo[id] = fixtureMemProtectDetail(id, created)
}

// revokeTokens invalidates every access token issued so far, as the real API
// does when a token expires: the next data call gets a 401.
func (m *mockCylance) revokeTokens() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.validTokens = map[string]bool{}
}

func (m *mockCylance) authCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authCount
}

func (m *mockCylance) dataCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dataCount
}

func (m *mockCylance) pageRequested(listPath string, page int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.listPages[listPath][page]
}

func (m *mockCylance) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		if path == authEndpoint {
			if r.Method != http.MethodPost {
				http.Error(w, `{"message":"method not allowed"}`, http.StatusMethodNotAllowed)
				return
			}
			m.handleAuth(w, r)
			return
		}

		// Every other endpoint requires a previously issued bearer token.
		if !m.checkBearer(w, r) {
			return
		}
		m.mu.Lock()
		m.dataCount++
		m.mu.Unlock()

		if r.Method != http.MethodGet {
			http.Error(w, `{"message":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		switch {
		case path == detectionsEndpoint:
			m.serveList(w, r, m.snapshotList(&m.detections), "ReceivedTime", time.RFC3339, "start")
		case strings.HasPrefix(path, detectionsEndpoint+"/") && strings.HasSuffix(path, "/details"):
			id := strings.TrimSuffix(strings.TrimPrefix(path, detectionsEndpoint+"/"), "/details")
			m.serveDetail(w, m.detectionDetails, id)
		case path == threatsEndpoint:
			// The real threats API has no time filter; start_time is ignored.
			m.serveList(w, r, m.snapshotList(&m.threats), "last_found", legacyTimeFormat, "")
		case strings.HasPrefix(path, threatsEndpoint+"/"):
			m.serveDetail(w, m.threatDetails, strings.TrimPrefix(path, threatsEndpoint+"/"))
		case path == memoryProtectionEndpoint:
			// Same: the real memory protection API has no time filter.
			m.serveList(w, r, m.snapshotList(&m.memProtects), "created", legacyTimeFormat, "")
		case strings.HasPrefix(path, memoryProtectionEndpoint+"/"):
			m.serveDetail(w, m.memProtectInfo, strings.TrimPrefix(path, memoryProtectionEndpoint+"/"))
		default:
			http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
		}
	}
}

// handleAuth implements POST /auth/v2/token: it validates the caller's signed
// application JWT exactly as the real service does (HS256 signature with the
// app secret, sub == application id, tid == tenant id, iss == cylance.com) and
// returns {"access_token": <JWT with a 30 minute exp>}.
func (m *mockCylance) handleAuth(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.authCount++
	m.mu.Unlock()

	var body struct {
		AuthToken string `json:"auth_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.AuthToken == "" {
		http.Error(w, `{"message":"malformed request body"}`, http.StatusBadRequest)
		return
	}

	token, err := jwt.Parse(body.AuthToken, func(tok *jwt.Token) (interface{}, error) {
		if _, ok := tok.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method %v", tok.Header["alg"])
		}
		return []byte(m.appSecret), nil
	})
	if err != nil || !token.Valid {
		http.Error(w, `{"message":"invalid application token"}`, http.StatusUnauthorized)
		return
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok ||
		claims["sub"] != m.appID ||
		claims["tid"] != m.tenantID ||
		claims["iss"] != "http://cylance.com" {
		http.Error(w, `{"message":"application not found"}`, http.StatusUnauthorized)
		return
	}

	access := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"exp": time.Now().UTC().Add(30 * time.Minute).Unix(),
		"iat": time.Now().UTC().Unix(),
		"iss": "http://cylance.com",
		"sub": m.appID,
		"tid": m.tenantID,
		"jti": uuid.New().String(),
	})
	signed, err := access.SignedString([]byte(m.appSecret))
	if !assert.NoError(m.t, err) {
		http.Error(w, `{"message":"internal error"}`, http.StatusInternalServerError)
		return
	}

	m.mu.Lock()
	m.validTokens[signed] = true
	m.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"access_token": signed})
}

func (m *mockCylance) checkBearer(w http.ResponseWriter, r *http.Request) bool {
	auth := r.Header.Get("Authorization")
	tok := strings.TrimPrefix(auth, "Bearer ")
	m.mu.Lock()
	valid := tok != auth && m.validTokens[tok]
	m.mu.Unlock()
	if !valid {
		http.Error(w, `{"message":"Unauthorized"}`, http.StatusUnauthorized)
		return false
	}
	return true
}

func (m *mockCylance) snapshotList(list *[]utils.Dict) []utils.Dict {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]utils.Dict, len(*list))
	copy(out, *list)
	return out
}

// serveList implements the paginated list endpoints: it filters records by the
// caller's start-time query parameter (when the real endpoint supports one --
// startParam is empty for endpoints that don't), slices the requested page and
// wraps it in the Cylance page envelope.
func (m *mockCylance) serveList(w http.ResponseWriter, r *http.Request, records []utils.Dict, timeField, timeFormat, startParam string) {
	q := r.URL.Query()
	page := intParam(q.Get("page"), 1)
	pageSize := intParam(q.Get("page_size"), 10)

	m.mu.Lock()
	if m.listPages[r.URL.Path] == nil {
		m.listPages[r.URL.Path] = map[int]bool{}
	}
	m.listPages[r.URL.Path][page] = true
	m.mu.Unlock()

	filtered := records
	if startStr := q.Get(startParam); startParam != "" && startStr != "" {
		start, err := time.Parse(timeFormat, startStr)
		if !assert.NoError(m.t, err, "adapter sent an unparseable %s", startParam) {
			http.Error(w, `{"message":"invalid time filter"}`, http.StatusBadRequest)
			return
		}
		filtered = nil
		for _, rec := range records {
			ts, err := time.Parse(timeFormat, rec[timeField].(string))
			if !assert.NoError(m.t, err) {
				continue
			}
			if !ts.Before(start) {
				filtered = append(filtered, rec)
			}
		}
	}

	totalPages := (len(filtered) + pageSize - 1) / pageSize
	var pageItems []utils.Dict
	if start := (page - 1) * pageSize; start < len(filtered) {
		end := start + pageSize
		if end > len(filtered) {
			end = len(filtered)
		}
		pageItems = filtered[start:end]
	}
	if pageItems == nil {
		pageItems = []utils.Dict{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"page_items":            pageItems,
		"page_number":           page,
		"page_size":             pageSize,
		"total_pages":           totalPages,
		"total_number_of_items": len(filtered),
	})
}

func (m *mockCylance) serveDetail(w http.ResponseWriter, details map[string]utils.Dict, id string) {
	m.mu.Lock()
	detail, ok := details[id]
	m.mu.Unlock()
	if !ok {
		http.Error(w, `{"message":"resource not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(detail)
}

func intParam(s string, def int) int {
	v, err := strconv.Atoi(s)
	if err != nil || v < 1 {
		return def
	}
	return v
}

// --- realistic fixtures -------------------------------------------------------

// fixtureDetection is shaped like a CylanceOPTICS detection list item
// (GET /detections/v2 page_items).
func fixtureDetection(id string, received time.Time) utils.Dict {
	return utils.Dict{
		"Id":                   id,
		"TenantId":             testTenantID,
		"DetectionDescription": "Powershell Download And Execute",
		"OccurrenceTime":       received.Add(-3 * time.Second).UTC().Format(time.RFC3339),
		"ReceivedTime":         received.UTC().Format(time.RFC3339),
		"Severity":             "High",
		"Status":               "New",
		"PhoneticId":           "AlphaBravoCharlie",
		"Device": utils.Dict{
			"CylanceId": "44444444-4444-4444-4444-444444444444",
			"Name":      "WIN-EXAMPLE-01",
		},
	}
}

// fixtureDetectionDetail is the full document returned by
// GET /detections/v2/{id}/details.
func fixtureDetectionDetail(id string, received time.Time) utils.Dict {
	d := fixtureDetection(id, received)
	d["ActivationTime"] = received.Add(-5 * time.Second).UTC().Format(time.RFC3339)
	d["DetectionRule"] = utils.Dict{
		"Id":       "55555555-5555-5555-5555-555555555555",
		"Name":     "Powershell Download And Execute",
		"Category": "Execution",
	}
	d["AssociatedIocs"] = []interface{}{
		utils.Dict{
			"Type":  "File",
			"Value": `C:\Users\jdoe\AppData\Local\Temp\dropper.ps1`,
		},
	}
	d["Tactics"] = []interface{}{"Execution", "Defense Evasion"}
	d["Comments"] = []interface{}{}
	return d
}

// fixtureThreat is shaped like a real GET /threats/v2 page item (field names
// from the BlackBerry Protect API docs).
func fixtureThreat(sha256 string, lastFound time.Time) utils.Dict {
	return utils.Dict{
		"name":               "bad_installer.exe",
		"sha256":             sha256,
		"md5":                "11111111111111111111111111111111",
		"cylance_score":      -0.92,
		"av_industry":        nil,
		"classification":     "Malware",
		"sub_classification": "Trojan",
		"global_quarantined": false,
		"safelisted":         false,
		"file_size":          38912,
		"unique_to_cylance":  false,
		"last_found":         lastFound.UTC().Format(legacyTimeFormat),
	}
}

// fixtureThreatDetail is the document returned by GET /threats/v2/{sha256}.
func fixtureThreatDetail(sha256 string, lastFound time.Time) utils.Dict {
	d := fixtureThreat(sha256, lastFound)
	d["cert_publisher"] = ""
	d["cert_issuer"] = ""
	d["cert_timestamp"] = "0001-01-01T00:00:00"
	d["signed"] = false
	d["auto_run"] = false
	d["running"] = false
	d["detected_by"] = "FileWatcher"
	return d
}

// fixtureMemProtect is shaped like a GET /memoryprotection/v2 page item.
func fixtureMemProtect(id string, created time.Time) utils.Dict {
	return utils.Dict{
		"device_image_file_event_id": id,
		"tenant_id":                  testTenantID,
		"device_id":                  "66666666-6666-6666-6666-666666666666",
		"image_path":                 `C:\Users\jdoe\AppData\Local\Temp\injector.exe`,
		"action":                     "Terminate",
		"violation_type":             "LsassRead",
		"process_id":                 4242,
		"user_name":                  `EXAMPLE\jdoe`,
		"created":                    created.UTC().Format(legacyTimeFormat),
	}
}

// fixtureMemProtectDetail is the document returned by
// GET /memoryprotection/v2/{id}.
func fixtureMemProtectDetail(id string, created time.Time) utils.Dict {
	d := fixtureMemProtect(id, created)
	d["device_name"] = "WIN-EXAMPLE-02"
	return d
}

// expectedShipped is what the adapter ships for a detail document: the payload
// verbatim plus the ObjectSource/ObjectType meta fields it injects.
func expectedShipped(detail utils.Dict, objectType string) utils.Dict {
	out := utils.Dict{}
	for k, v := range detail {
		out[k] = v
	}
	out["ObjectSource"] = "Cylance"
	out["ObjectType"] = objectType
	return out
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// startMockAdapter spins up the adapter against a mock server with a capture
// sink and a fast poll interval.
func startMockAdapter(t *testing.T, mock *mockCylance, conf CylanceConfig) (*CylanceAdapter, chan struct{}, *captureSink, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(mock.handler())

	if conf.TenantID == "" {
		conf.TenantID = testTenantID
	}
	if conf.AppID == "" {
		conf.AppID = testAppID
	}
	if conf.AppSecret == "" {
		conf.AppSecret = testAppSecret
	}
	conf.LoggingBaseURL = server.URL
	if conf.PollInterval == 0 {
		conf.PollInterval = 50 * time.Millisecond
	}

	sink := &captureSink{}
	adapter, chStopped, err := newCylanceAdapter(context.Background(), conf, sink)
	if err != nil {
		server.Close()
		t.Fatalf("newCylanceAdapter: %v", err)
	}
	t.Cleanup(server.Close)
	t.Cleanup(func() { _ = adapter.Close() })
	return adapter, chStopped, sink, server
}

// --- tests --------------------------------------------------------------------

// TestMockDetectionsEndToEnd drives the adapter against the mock API and
// asserts the exact events shipped: the detection *detail* document verbatim,
// tagged ObjectSource=Cylance / ObjectType=Detection, with the adapter's
// ship-time TimestampMs and no EventType (the adapter relies on the platform
// default). It then verifies re-polling does not re-ship.
func TestMockDetectionsEndToEnd(t *testing.T) {
	mock := newMockCylance(t)
	// Timestamps must fall inside the adapter's initial 60-minute lookback.
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	ids := []string{
		"77777777-7777-7777-7777-777777777701",
		"77777777-7777-7777-7777-777777777702",
		"77777777-7777-7777-7777-777777777703",
	}
	for i, id := range ids {
		mock.addDetection(id, base.Add(time.Duration(i)*time.Minute))
	}

	testStart := time.Now()
	opts, _ := testClientOptions(t)
	_, _, sink, _ := startMockAdapter(t, mock, CylanceConfig{ClientOptions: opts})

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 detections to ship")

	// Re-polling must not re-ship: the count stays at 3 across several polls.
	require.Never(t, func() bool { return sink.count() != 3 },
		400*time.Millisecond, 30*time.Millisecond, "detections were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		require.NotNil(t, msg.JsonPayload)
		assert.Empty(t, msg.EventType, "the adapter does not set EventType")
		assert.GreaterOrEqual(t, msg.TimestampMs, uint64(testStart.UnixMilli()),
			"TimestampMs is the ship time")
		assert.LessOrEqual(t, msg.TimestampMs, uint64(time.Now().UnixMilli()))
		id, _ := msg.JsonPayload["Id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3, "each detection ships exactly once")

	for i, id := range ids {
		msg := byID[id]
		require.NotNil(t, msg, "detection %s was not shipped", id)
		want := expectedShipped(fixtureDetectionDetail(id, base.Add(time.Duration(i)*time.Minute)), "Detection")
		assert.JSONEq(t, mustJSON(t, want), mustJSON(t, msg.JsonPayload),
			"shipped payload must be the detail document verbatim plus ObjectSource/ObjectType")
	}
}

// TestMockAllThreeAPIs verifies the adapter polls all three Cylance APIs
// (detections, threats, memory protection), ships each detail document once,
// and tags each with the correct ObjectType. Threat and memory protection
// payloads are also checked verbatim, including their zone-less timestamps.
func TestMockAllThreeAPIs(t *testing.T) {
	mock := newMockCylance(t)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)

	detID := "77777777-7777-7777-7777-777777777711"
	sha := "1111111111111111111111111111111111111111111111111111111111111111"
	memID := "88888888-8888-8888-8888-888888888801"
	mock.addDetection(detID, base)
	mock.addThreat(sha, base.Add(1*time.Minute))
	mock.addMemProtect(memID, base.Add(2*time.Minute))

	opts, _ := testClientOptions(t)
	_, _, sink, _ := startMockAdapter(t, mock, CylanceConfig{ClientOptions: opts})

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected one event from each API")
	require.Never(t, func() bool { return sink.count() != 3 },
		400*time.Millisecond, 30*time.Millisecond, "events were re-shipped on a later poll")

	byType := map[string]utils.Dict{}
	for _, msg := range sink.snapshot() {
		objType, _ := msg.JsonPayload["ObjectType"].(string)
		require.NotEmpty(t, objType)
		assert.Equal(t, "Cylance", msg.JsonPayload["ObjectSource"])
		byType[objType] = msg.JsonPayload
	}
	require.Len(t, byType, 3)

	assert.JSONEq(t,
		mustJSON(t, expectedShipped(fixtureDetectionDetail(detID, base), "Detection")),
		mustJSON(t, byType["Detection"]))
	assert.JSONEq(t,
		mustJSON(t, expectedShipped(fixtureThreatDetail(sha, base.Add(1*time.Minute)), "Threat")),
		mustJSON(t, byType["Threat"]))
	assert.JSONEq(t,
		mustJSON(t, expectedShipped(fixtureMemProtectDetail(memID, base.Add(2*time.Minute)), "Memory Protection")),
		mustJSON(t, byType["Memory Protection"]))
}

// TestMockNewDetectionMidRunShipsOnce verifies an event that appears while the
// adapter is running is picked up on a later poll and shipped exactly once,
// without re-shipping earlier events.
func TestMockNewDetectionMidRunShipsOnce(t *testing.T) {
	mock := newMockCylance(t)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	idA := "77777777-7777-7777-7777-777777777721"
	idB := "77777777-7777-7777-7777-777777777722"
	idC := "77777777-7777-7777-7777-777777777723"
	mock.addDetection(idA, base)
	mock.addDetection(idB, base.Add(1*time.Minute))

	opts, _ := testClientOptions(t)
	_, _, sink, _ := startMockAdapter(t, mock, CylanceConfig{ClientOptions: opts})

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)

	// A new detection arrives mid-run, strictly newer than everything shipped.
	mock.addDetection(idC, time.Now().UTC().Truncate(time.Second).Add(-1*time.Minute))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "the new detection should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		400*time.Millisecond, 30*time.Millisecond)

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["Id"].(string)]++
	}
	assert.Equal(t, map[string]int{idA: 1, idB: 1, idC: 1}, shippedPerID,
		"every detection must ship exactly once")
}

// TestMockPaginationFullDataset verifies a dataset larger than one page
// (page_size is a fixed 100) is walked to the last page and every record's
// detail document is shipped exactly once. It uses the threats API: its real
// server has no time filter, so the multi-page walk sees a stable dataset.
//
// (The detections API cannot fully consume a multi-page poll: the adapter
// advances the start filter to the newest record seen while also incrementing
// the page number, so against a server that honors ?start -- as the real
// detections API does -- later pages of the shrunken result set skip records.
// That is existing adapter behavior, faithfully reproduced by this mock, not a
// mock artifact.)
func TestMockPaginationFullDataset(t *testing.T) {
	const total = 250 // 3 pages at the adapter's fixed page_size of 100

	mock := newMockCylance(t)
	base := time.Now().UTC().Truncate(time.Second).Add(-30 * time.Minute)
	for i := 0; i < total; i++ {
		mock.addThreat(fmt.Sprintf("%064d", i), base.Add(time.Duration(i)*time.Second))
	}

	opts, _ := testClientOptions(t)
	_, _, sink, _ := startMockAdapter(t, mock, CylanceConfig{ClientOptions: opts})

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 25*time.Millisecond, "all paginated threats should ship")
	require.Never(t, func() bool { return sink.count() != total },
		400*time.Millisecond, 30*time.Millisecond)

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		assert.Equal(t, "Threat", msg.JsonPayload["ObjectType"])
		ids[msg.JsonPayload["sha256"].(string)] = true
	}
	assert.Len(t, ids, total, "every distinct threat should ship exactly once")

	// The walk must have covered every page and stopped at total_pages.
	for page := 1; page <= 3; page++ {
		assert.True(t, mock.pageRequested(threatsEndpoint, page), "page %d should be requested", page)
	}
	assert.False(t, mock.pageRequested(threatsEndpoint, 4), "pagination must stop at total_pages")
}

// TestMockBadCredentialsShipNothing verifies that when the token endpoint
// rejects the application JWT (wrong app secret), the adapter ships nothing
// and never reaches a data endpoint, and that it surfaces the auth failure.
//
// The adapter's full shutdown on bad credentials only happens after
// consecutiveAuthFailsLimit failed refreshes with 60s sleeps between them
// (~10+ minutes by design), so this test asserts the observable fast part:
// the auth error is reported and no data ships.
func TestMockBadCredentialsShipNothing(t *testing.T) {
	mock := newMockCylance(t)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	mock.addDetection("77777777-7777-7777-7777-777777777731", base)

	opts, log := testClientOptions(t)
	_, _, sink, _ := startMockAdapter(t, mock, CylanceConfig{
		ClientOptions: opts,
		AppSecret:     "99999999-9999-9999-9999-999999999999", // wrong secret
	})

	require.Eventually(t, func() bool {
		return mock.authCalls() >= 1 && log.hasErrorContaining("cylance auth api non-200")
	}, 5*time.Second, 20*time.Millisecond, "the auth failure should be attempted and reported")

	// Nothing ships and no data endpoint is ever reached.
	require.Never(t, func() bool { return sink.count() != 0 || mock.dataCalls() != 0 },
		400*time.Millisecond, 30*time.Millisecond, "nothing must ship with bad credentials")
}

// TestMockTokenRevokedReauthenticates verifies the adapter transparently
// re-authenticates when a data endpoint answers 401 (revoked/expired token)
// and then resumes shipping new events.
func TestMockTokenRevokedReauthenticates(t *testing.T) {
	mock := newMockCylance(t)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	idA := "77777777-7777-7777-7777-777777777741"
	idB := "77777777-7777-7777-7777-777777777742"
	mock.addDetection(idA, base)

	opts, _ := testClientOptions(t)
	_, _, sink, _ := startMockAdapter(t, mock, CylanceConfig{ClientOptions: opts})

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond)
	authedOnce := mock.authCalls()
	require.GreaterOrEqual(t, authedOnce, 1)

	// The token dies server-side; a newer detection appears.
	mock.revokeTokens()
	mock.addDetection(idB, time.Now().UTC().Truncate(time.Second).Add(-1*time.Minute))

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "the adapter should re-authenticate and ship the new detection")
	assert.Greater(t, mock.authCalls(), authedOnce, "a second token exchange must have happened")

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["Id"].(string)]++
	}
	assert.Equal(t, map[string]int{idA: 1, idB: 1}, shippedPerID)
}

// TestMockMissingDetailDoesNotBlockOthers verifies that a list item whose
// detail lookup fails (404) is reported and skipped without blocking other
// events from shipping.
func TestMockMissingDetailDoesNotBlockOthers(t *testing.T) {
	mock := newMockCylance(t)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	idOK := "77777777-7777-7777-7777-777777777751"
	idGone := "77777777-7777-7777-7777-777777777752"
	mock.addDetection(idOK, base)
	mock.addDetectionWithoutDetail(idGone, base.Add(1*time.Minute))

	opts, log := testClientOptions(t)
	_, _, sink, _ := startMockAdapter(t, mock, CylanceConfig{ClientOptions: opts})

	require.Eventually(t, func() bool {
		return sink.count() == 1 && log.hasErrorContaining("details fetch failed")
	}, 5*time.Second, 20*time.Millisecond, "the healthy detection ships, the broken one is reported")
	require.Never(t, func() bool { return sink.count() > 1 },
		400*time.Millisecond, 30*time.Millisecond)

	assert.Equal(t, idOK, sink.snapshot()[0].JsonPayload["Id"])
}
