package usp_o365

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock of the Office 365
// Management Activity API (token endpoint, subscription start, content listing
// with NextPageUri pagination, and content blobs), capturing the exact messages
// shipped so their content can be asserted.

// All fixture identifiers are deliberately fake (example.com users, all-same
// digit UUIDs, made-up tokens).
const (
	testTenantID     = "11111111-1111-1111-1111-111111111111"
	testPublisherID  = "22222222-2222-2222-2222-222222222222"
	testClientID     = "33333333-3333-3333-3333-333333333333"
	testClientSecret = "fake-client-secret-for-tests"
	testAccessToken  = "fake-access-token-1111111111"
	testDomain       = "tenant.example.com"

	// aadTimeLayout is the (zone-less) layout the adapter renders startTime /
	// endTime in, and the layout the real Management Activity API uses for
	// CreationTime.
	aadTimeLayout = "2006-01-02T15:04:05"
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

// payloadCounts returns, per shipped TextPayload, how many times it shipped.
func (s *captureSink) payloadCounts() map[string]int {
	out := map[string]int{}
	for _, m := range s.snapshot() {
		out[m.TextPayload]++
	}
	return out
}

// --- realistic record fixtures ------------------------------------------------

// auditRecord returns a compact-JSON Management Activity audit record with the
// documented common-schema fields (Id, RecordType, CreationTime, Operation,
// OrganizationId, UserType, UserKey, Workload, ResultStatus, ObjectId, UserId,
// ClientIP) plus an ExtendedProperties array. The returned string is the exact
// byte sequence the mock serves inside a content blob, so tests can assert the
// adapter ships it verbatim.
func auditRecord(t *testing.T, id, operation, workload, userID string, creationTime time.Time) string {
	t.Helper()
	rec := map[string]interface{}{
		"Id":             id,
		"RecordType":     15,
		"CreationTime":   creationTime.UTC().Format(aadTimeLayout),
		"Operation":      operation,
		"OrganizationId": testTenantID,
		"UserType":       0,
		"UserKey":        userID,
		"Workload":       workload,
		"ResultStatus":   "Succeeded",
		"ObjectId":       "Unknown",
		"UserId":         userID,
		"ClientIP":       "203.0.113.42",
		"ExtendedProperties": []map[string]interface{}{
			{"Name": "UserAgent", "Value": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
			{"Name": "RequestType", "Value": "OAuth2:Authorize"},
		},
	}
	b, err := json.Marshal(rec)
	require.NoError(t, err)
	return string(b)
}

// --- mock Office 365 Management Activity API ----------------------------------

// contentBlob is one unit of available content: a listItem in the content
// listing pointing at a blob URI that serves an array of audit records.
type contentBlob struct {
	id      string
	created time.Time
	records []string // pre-marshaled JSON objects, served verbatim
}

// mockO365 reproduces the parts of the AAD token endpoint and the Office 365
// Management Activity API the adapter relies on:
//
//   - POST /oauth2/token: OAuth2 client-credentials grant. Accepts the
//     credentials either as HTTP basic auth or as form values (the oauth2
//     library auto-detects the style), requires grant_type=client_credentials
//     and the manage.office.com resource, and returns a bearer token.
//   - POST /api/v1.0/{tenant}/activity/feed/subscriptions/start: requires the
//     bearer token and a contentType / PublisherIdentifier query.
//   - GET /api/v1.0/{tenant}/activity/feed/subscriptions/content: lists the
//     blobs of a content type whose contentCreated falls inside the request's
//     [startTime, endTime] window, paginating via the NextPageUri response
//     header when listPageSize is set.
//   - GET /api/v1.0/{tenant}/activity/feed/audit/{contentId}: serves the
//     blob's records as a JSON array.
type mockO365 struct {
	t *testing.T

	mu      sync.Mutex
	baseURL string

	clientID     string
	clientSecret string

	listPageSize int // 0 = everything in one page

	alreadyEnabled bool // subscriptions/start answers 400 "already enabled"

	blobs map[string][]contentBlob // contentType -> available blobs

	tokenRequests     int
	subscriptionStart map[string]int // contentType -> times started
	listRequests      int
	pagedListRequests int             // list requests that carried a nextPage token
	listWindows       []time.Duration // endTime-startTime per (unpaged) list request
}

func newMockO365(t *testing.T) *mockO365 {
	t.Helper()
	m := &mockO365{
		t:                 t,
		clientID:          testClientID,
		clientSecret:      testClientSecret,
		blobs:             map[string][]contentBlob{},
		subscriptionStart: map[string]int{},
	}
	server := httptest.NewServer(m.handler())
	m.mu.Lock()
	m.baseURL = server.URL
	m.mu.Unlock()
	t.Cleanup(server.Close)
	return m
}

func (m *mockO365) base() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.baseURL
}

// endpoint returns the Management API root to put in the adapter config; the
// adapter appends "{tenant}/activity/feed/..." to it, so it ends with "/".
func (m *mockO365) endpoint() string { return m.base() + "/api/v1.0/" }

// tokenURL returns the OAuth2 token endpoint to put in the adapter config.
func (m *mockO365) tokenURL() string { return m.base() + "/oauth2/token" }

func (m *mockO365) addBlob(contentType string, blob contentBlob) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[contentType] = append(m.blobs[contentType], blob)
}

func (m *mockO365) tokenRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenRequests
}

func (m *mockO365) startCount(contentType string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.subscriptionStart[contentType]
}

func (m *mockO365) pagedListCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pagedListRequests
}

func (m *mockO365) windows() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]time.Duration, len(m.listWindows))
	copy(out, m.listWindows)
	return out
}

func (m *mockO365) handler() http.Handler {
	apiRoot := "/api/v1.0/" + testTenantID
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth2/token":
			m.handleToken(w, r)
		case r.URL.Path == apiRoot+"/activity/feed/subscriptions/start":
			m.handleStart(w, r)
		case r.URL.Path == apiRoot+"/activity/feed/subscriptions/content":
			m.handleList(w, r)
		case strings.HasPrefix(r.URL.Path, apiRoot+"/activity/feed/audit/"):
			m.handleBlob(w, r, strings.TrimPrefix(r.URL.Path, apiRoot+"/activity/feed/audit/"))
		default:
			http.NotFound(w, r)
		}
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// handleToken implements the AAD v1.0 client-credentials token endpoint.
func (m *mockO365) handleToken(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.tokenRequests++
	m.mu.Unlock()

	if !assert.Equal(m.t, http.MethodPost, r.Method, "token endpoint expects POST") {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "invalid_request"})
		return
	}
	if err := r.ParseForm(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}

	if r.PostFormValue("grant_type") != "client_credentials" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":             "unsupported_grant_type",
			"error_description": "AADSTS70003: The app requested an unsupported grant type.",
		})
		return
	}

	// The oauth2 library auto-detects the auth style: it may send the client
	// credentials as (query-escaped) HTTP basic auth or as form values.
	id, secret := r.PostFormValue("client_id"), r.PostFormValue("client_secret")
	if u, p, ok := r.BasicAuth(); ok {
		id, _ = url.QueryUnescape(u)
		secret, _ = url.QueryUnescape(p)
	}
	if id != m.clientID || secret != m.clientSecret {
		writeJSON(w, http.StatusUnauthorized, map[string]string{
			"error":             "invalid_client",
			"error_description": "AADSTS7000215: Invalid client secret provided.",
		})
		return
	}

	// The v1.0 endpoint requires the resource the token is for; the adapter
	// must request the Management API resource or the real AAD would mint a
	// token the Management API rejects.
	if res := r.PostFormValue("resource"); !assert.Equal(m.t, "https://manage.office.com", res, "token request must carry the manage.office.com resource") {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":             "invalid_resource",
			"error_description": "AADSTS500011: The resource principal was not found.",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"token_type":   "Bearer",
		"expires_in":   3599,
		"access_token": testAccessToken,
		"resource":     "https://manage.office.com",
	})
}

func (m *mockO365) authorized(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Authorization") == "Bearer "+testAccessToken {
		return true
	}
	writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
		"error": map[string]string{"code": "AF10001", "message": "The permission set on the request does not allow access."},
	})
	return false
}

func (m *mockO365) handleStart(w http.ResponseWriter, r *http.Request) {
	if !assert.Equal(m.t, http.MethodPost, r.Method, "subscriptions/start expects POST") {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "invalid_request"})
		return
	}
	if !m.authorized(w, r) {
		return
	}
	q := r.URL.Query()
	ct := q.Get("contentType")
	if !assert.NotEmpty(m.t, ct, "subscriptions/start must carry contentType") ||
		!assert.Equal(m.t, testPublisherID, q.Get("PublisherIdentifier"), "subscriptions/start must carry PublisherIdentifier") {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": map[string]string{"code": "AF20055", "message": "Invalid parameters."},
		})
		return
	}

	m.mu.Lock()
	m.subscriptionStart[ct]++
	alreadyEnabled := m.alreadyEnabled
	m.mu.Unlock()

	if alreadyEnabled {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": map[string]string{"code": "AF20024", "message": "The subscription is already enabled. No property change."},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"contentType": ct,
		"status":      "enabled",
		"webhook":     nil,
	})
}

func (m *mockO365) handleList(w http.ResponseWriter, r *http.Request) {
	if !assert.Equal(m.t, http.MethodGet, r.Method, "subscriptions/content expects GET") {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "invalid_request"})
		return
	}
	if !m.authorized(w, r) {
		return
	}
	q := r.URL.Query()
	ct := q.Get("contentType")
	if !assert.NotEmpty(m.t, ct, "subscriptions/content must carry contentType") ||
		!assert.Equal(m.t, testPublisherID, q.Get("PublisherIdentifier"), "subscriptions/content must carry PublisherIdentifier") {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": map[string]string{"code": "AF20055", "message": "Invalid parameters."},
		})
		return
	}

	start, errS := time.Parse(aadTimeLayout, q.Get("startTime"))
	end, errE := time.Parse(aadTimeLayout, q.Get("endTime"))
	if !assert.NoError(m.t, errS, "startTime must be yyyy-MM-ddTHH:mm:ss") ||
		!assert.NoError(m.t, errE, "endTime must be yyyy-MM-ddTHH:mm:ss") {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": map[string]string{"code": "AF20055", "message": "Invalid startTime/endTime."},
		})
		return
	}

	page := 0
	if v := q.Get("nextPage"); v != "" {
		page, _ = strconv.Atoi(v)
	}

	m.mu.Lock()
	m.listRequests++
	if page > 0 {
		m.pagedListRequests++
	} else {
		m.listWindows = append(m.listWindows, end.Sub(start))
	}
	var inWindow []contentBlob
	for _, b := range m.blobs[ct] {
		created := b.created.UTC()
		if !created.Before(start) && !created.After(end) {
			inWindow = append(inWindow, b)
		}
	}
	pageSize := m.listPageSize
	base := m.baseURL
	m.mu.Unlock()

	if pageSize <= 0 {
		pageSize = len(inWindow) + 1
	}
	lo := page * pageSize
	hi := lo + pageSize
	if lo > len(inWindow) {
		lo = len(inWindow)
	}
	if hi > len(inWindow) {
		hi = len(inWindow)
	}

	// More content available: hand back a NextPageUri header pointing at the
	// next page, the way the real API paginates.
	if hi < len(inWindow) {
		nq := r.URL.Query()
		nq.Set("nextPage", strconv.Itoa(page+1))
		w.Header().Set("NextPageUri", base+r.URL.Path+"?"+nq.Encode())
	}

	items := make([]map[string]string, 0, hi-lo)
	for _, b := range inWindow[lo:hi] {
		items = append(items, map[string]string{
			"contentType":       ct,
			"contentId":         b.id,
			"contentUri":        base + "/api/v1.0/" + testTenantID + "/activity/feed/audit/" + b.id,
			"contentCreated":    b.created.UTC().Format("2006-01-02T15:04:05.000Z"),
			"contentExpiration": b.created.UTC().Add(7 * 24 * time.Hour).Format("2006-01-02T15:04:05.000Z"),
		})
	}
	writeJSON(w, http.StatusOK, items)
}

func (m *mockO365) handleBlob(w http.ResponseWriter, r *http.Request, id string) {
	if !m.authorized(w, r) {
		return
	}
	m.mu.Lock()
	var records []string
	found := false
	for _, blobs := range m.blobs {
		for _, b := range blobs {
			if b.id == id {
				records = b.records
				found = true
			}
		}
	}
	m.mu.Unlock()

	if !found {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"error": map[string]string{"code": "AF20051", "message": "Content not found."},
		})
		return
	}

	// Serve the records verbatim so tests can assert byte-for-byte payloads.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("[" + strings.Join(records, ",") + "]"))
}

// --- test plumbing --------------------------------------------------------------

// newTestConfig returns an adapter config pointed at the mock, with a fast
// poll interval.
func newTestConfig(t *testing.T, m *mockO365, contentTypes string) Office365Config {
	t.Helper()
	return Office365Config{
		ClientOptions: testClientOptions(t),
		Domain:        testDomain,
		TenantID:      testTenantID,
		PublisherID:   testPublisherID,
		ClientID:      testClientID,
		ClientSecret:  testClientSecret,
		Endpoint:      m.endpoint(),
		TokenURL:      m.tokenURL(),
		ContentTypes:  contentTypes,
		PollInterval:  50 * time.Millisecond,
	}
}

// --- tests ----------------------------------------------------------------------

// TestMockEndToEnd drives the adapter against the mock API and asserts the
// exact events shipped: the OAuth2 token is acquired, the subscription is
// started exactly once, every audit record ships verbatim exactly once with a
// plausible ship-time timestamp, and re-polling does not re-ship.
func TestMockEndToEnd(t *testing.T) {
	const ct = "Audit.AzureActiveDirectory"
	m := newMockO365(t)

	created := time.Now().UTC().Add(-10 * time.Minute)
	want := []string{
		auditRecord(t, "aaaaaaaa-1111-1111-1111-111111111111", "UserLoggedIn", "AzureActiveDirectory", "jdoe@example.com", created),
		auditRecord(t, "aaaaaaaa-2222-2222-2222-222222222222", "UserLoginFailed", "AzureActiveDirectory", "asmith@example.com", created),
		auditRecord(t, "aaaaaaaa-3333-3333-3333-333333333333", "Add user.", "AzureActiveDirectory", "admin@example.com", created),
	}
	m.addBlob(ct, contentBlob{id: "blob-0001", created: created, records: want[:2]})
	m.addBlob(ct, contentBlob{id: "blob-0002", created: created, records: want[2:]})

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	before := uint64(time.Now().UnixMilli())
	adapter, chStopped, err := newOffice365Adapter(ctx, newTestConfig(t, m, ct), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 audit records to ship")

	// Re-polling (poll interval is 50ms, so several polls happen here) must
	// not re-ship: the count stays at 3.
	require.Never(t, func() bool { return sink.count() != 3 },
		400*time.Millisecond, 30*time.Millisecond, "records were re-shipped on a later poll")
	after := uint64(time.Now().UnixMilli())

	select {
	case <-chStopped:
		t.Fatal("adapter stopped unexpectedly")
	default:
	}

	// Each record shipped verbatim, exactly once.
	counts := sink.payloadCounts()
	require.Len(t, counts, 3)
	for _, rec := range want {
		assert.Equal(t, 1, counts[rec], "record must ship verbatim exactly once: %s", rec)
	}

	// The adapter stamps events with the ship time and sets no event type
	// (the platform mapping handles typing for office365).
	for _, msg := range sink.snapshot() {
		assert.Empty(t, msg.EventType)
		assert.Empty(t, msg.JsonPayload, "o365 ships TextPayload, not JsonPayload")
		assert.GreaterOrEqual(t, msg.TimestampMs, before)
		assert.LessOrEqual(t, msg.TimestampMs, after)
	}

	assert.GreaterOrEqual(t, m.tokenRequestCount(), 1, "the OAuth2 token endpoint must have been used")
	assert.Equal(t, 1, m.startCount(ct), "the subscription must be started exactly once")
}

// TestMockMidRunRecordShipsOnce verifies new content appearing between polls is
// picked up on a later poll and shipped exactly once.
func TestMockMidRunRecordShipsOnce(t *testing.T) {
	const ct = "Audit.Exchange"
	m := newMockO365(t)

	first := auditRecord(t, "bbbbbbbb-1111-1111-1111-111111111111", "MailItemsAccessed", "Exchange", "jdoe@example.com", time.Now().UTC().Add(-15*time.Minute))
	m.addBlob(ct, contentBlob{id: "blob-first", created: time.Now().UTC().Add(-15 * time.Minute), records: []string{first}})

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newOffice365Adapter(ctx, newTestConfig(t, m, ct), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond, "the initial record should ship")

	// New content becomes available mid-run.
	second := auditRecord(t, "bbbbbbbb-2222-2222-2222-222222222222", "Send", "Exchange", "asmith@example.com", time.Now().UTC())
	m.addBlob(ct, contentBlob{id: "blob-second", created: time.Now().UTC(), records: []string{second}})

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "the mid-run record should ship")
	require.Never(t, func() bool { return sink.count() > 2 },
		400*time.Millisecond, 30*time.Millisecond, "no record may ship twice")

	counts := sink.payloadCounts()
	assert.Equal(t, 1, counts[first])
	assert.Equal(t, 1, counts[second])
}

// TestMockPaginationNextPageUri verifies a listing spread over several pages
// (via the NextPageUri response header) is fully consumed within a single poll.
func TestMockPaginationNextPageUri(t *testing.T) {
	const ct = "Audit.SharePoint"
	const total = 5
	m := newMockO365(t)
	m.listPageSize = 2 // 5 blobs => 3 pages

	created := time.Now().UTC().Add(-20 * time.Minute)
	want := make([]string, 0, total)
	for i := 0; i < total; i++ {
		rec := auditRecord(t,
			fmt.Sprintf("cccccccc-%04d-1111-1111-111111111111", i),
			"FileAccessed", "SharePoint", "jdoe@example.com", created)
		want = append(want, rec)
		m.addBlob(ct, contentBlob{id: fmt.Sprintf("blob-%04d", i), created: created, records: []string{rec}})
	}

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := newTestConfig(t, m, ct)
	// Only the very first poll may run during the test: every record arriving
	// proves the NextPageUri chain was walked to the end within one poll.
	conf.PollInterval = 1 * time.Hour
	adapter, _, err := newOffice365Adapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		5*time.Second, 20*time.Millisecond, "all paginated records should ship in the first poll")
	require.Never(t, func() bool { return sink.count() != total },
		400*time.Millisecond, 30*time.Millisecond)

	assert.GreaterOrEqual(t, m.pagedListCount(), 2, "pages 2 and 3 must be fetched via NextPageUri")
	counts := sink.payloadCounts()
	require.Len(t, counts, total)
	for _, rec := range want {
		assert.Equal(t, 1, counts[rec], "record must ship exactly once: %s", rec)
	}
}

// TestMockMultipleContentTypes verifies each configured content type gets its
// own subscription and collector, and every record of every type ships once.
func TestMockMultipleContentTypes(t *testing.T) {
	m := newMockO365(t)
	created := time.Now().UTC().Add(-5 * time.Minute)

	aad := []string{
		auditRecord(t, "dddddddd-1111-1111-1111-111111111111", "UserLoggedIn", "AzureActiveDirectory", "jdoe@example.com", created),
		auditRecord(t, "dddddddd-2222-2222-2222-222222222222", "Update user.", "AzureActiveDirectory", "admin@example.com", created),
	}
	exch := []string{
		auditRecord(t, "eeeeeeee-1111-1111-1111-111111111111", "MailItemsAccessed", "Exchange", "asmith@example.com", created),
		auditRecord(t, "eeeeeeee-2222-2222-2222-222222222222", "New-InboxRule", "Exchange", "asmith@example.com", created),
	}
	m.addBlob("Audit.AzureActiveDirectory", contentBlob{id: "blob-aad", created: created, records: aad})
	m.addBlob("Audit.Exchange", contentBlob{id: "blob-exch", created: created, records: exch})

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newOffice365Adapter(ctx,
		newTestConfig(t, m, "Audit.AzureActiveDirectory, Audit.Exchange"), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 4 },
		5*time.Second, 20*time.Millisecond, "records of both content types should ship")
	require.Never(t, func() bool { return sink.count() != 4 },
		400*time.Millisecond, 30*time.Millisecond)

	assert.Equal(t, 1, m.startCount("Audit.AzureActiveDirectory"))
	assert.Equal(t, 1, m.startCount("Audit.Exchange"))

	counts := sink.payloadCounts()
	require.Len(t, counts, 4)
	for _, rec := range append(append([]string{}, aad...), exch...) {
		assert.Equal(t, 1, counts[rec])
	}
}

// TestMockDefaultContentTypes verifies an empty content_types config falls back
// to the five standard Management Activity content types, each subscribed.
func TestMockDefaultContentTypes(t *testing.T) {
	m := newMockO365(t)
	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newOffice365Adapter(ctx, newTestConfig(t, m, ""), sink)
	require.NoError(t, err)
	defer adapter.Close()

	for _, ct := range []string{
		"Audit.AzureActiveDirectory", "Audit.Exchange", "Audit.SharePoint", "Audit.General", "DLP.All",
	} {
		assert.Equal(t, 1, m.startCount(ct), "default content type %s must be subscribed", ct)
	}
	assert.Equal(t, 0, sink.count(), "no content available, nothing should ship")
}

// TestMockSubscriptionAlreadyEnabled verifies the "subscription already
// enabled" 400 from subscriptions/start is treated as success and collection
// proceeds.
func TestMockSubscriptionAlreadyEnabled(t *testing.T) {
	const ct = "Audit.General"
	m := newMockO365(t)
	m.alreadyEnabled = true

	created := time.Now().UTC().Add(-5 * time.Minute)
	rec := auditRecord(t, "ffffffff-1111-1111-1111-111111111111", "AlertTriggered", "SecurityComplianceCenter", "admin@example.com", created)
	m.addBlob(ct, contentBlob{id: "blob-already", created: created, records: []string{rec}})

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newOffice365Adapter(ctx, newTestConfig(t, m, ct), sink)
	require.NoError(t, err, "an already-enabled subscription is not an error")
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond, "collection should proceed past 'already enabled'")
	assert.Equal(t, rec, sink.snapshot()[0].TextPayload)
}

// TestMockBadCredentials verifies the adapter constructor fails -- and nothing
// ships -- when AAD rejects the client credentials.
func TestMockBadCredentials(t *testing.T) {
	const ct = "Audit.AzureActiveDirectory"
	m := newMockO365(t)
	created := time.Now().UTC().Add(-5 * time.Minute)
	m.addBlob(ct, contentBlob{id: "blob-x", created: created, records: []string{
		auditRecord(t, "99999999-1111-1111-1111-111111111111", "UserLoggedIn", "AzureActiveDirectory", "jdoe@example.com", created),
	}})

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := newTestConfig(t, m, ct)
	conf.ClientSecret = "wrong-secret"
	adapter, chStopped, err := newOffice365Adapter(ctx, conf, sink)
	require.Error(t, err, "bad credentials must fail the constructor")
	assert.Nil(t, adapter)
	assert.Nil(t, chStopped)
	assert.GreaterOrEqual(t, m.tokenRequestCount(), 1, "the token endpoint must have been tried")
	assert.Equal(t, 0, sink.count(), "nothing may ship when authentication fails")
}

// TestMockTimeWindowHonored verifies polls request a 3-hour lookback window and
// that only content inside the window is collected.
func TestMockTimeWindowHonored(t *testing.T) {
	const ct = "Audit.SharePoint"
	m := newMockO365(t)

	oldRec := auditRecord(t, "11110000-1111-1111-1111-111111111111", "FileDeleted", "SharePoint", "jdoe@example.com", time.Now().UTC().Add(-5*time.Hour))
	newRec := auditRecord(t, "22220000-2222-2222-2222-222222222222", "FileAccessed", "SharePoint", "asmith@example.com", time.Now().UTC().Add(-10*time.Minute))
	m.addBlob(ct, contentBlob{id: "blob-old", created: time.Now().UTC().Add(-5 * time.Hour), records: []string{oldRec}})
	m.addBlob(ct, contentBlob{id: "blob-new", created: time.Now().UTC().Add(-10 * time.Minute), records: []string{newRec}})

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newOffice365Adapter(ctx, newTestConfig(t, m, ct), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond, "the in-window record should ship")
	require.Never(t, func() bool { return sink.count() != 1 },
		400*time.Millisecond, 30*time.Millisecond, "the out-of-window record must not ship")
	assert.Equal(t, newRec, sink.snapshot()[0].TextPayload)

	// Every (unpaged) listing must have asked for exactly a 3-hour window.
	windows := m.windows()
	require.NotEmpty(t, windows)
	for _, w := range windows {
		assert.Equal(t, 3*time.Hour, w, "list requests must use a 3h lookback window")
	}
}

// TestMockDuplicateEventIDAcrossBlobs verifies per-event Id deduplication: the
// Management Activity API makes no uniqueness guarantee, so the same record
// appearing in two different content blobs must ship only once.
func TestMockDuplicateEventIDAcrossBlobs(t *testing.T) {
	const ct = "Audit.General"
	m := newMockO365(t)

	created := time.Now().UTC().Add(-5 * time.Minute)
	dup := auditRecord(t, "55550000-1111-1111-1111-111111111111", "AlertEntityGenerated", "SecurityComplianceCenter", "admin@example.com", created)
	other := auditRecord(t, "55550000-2222-2222-2222-222222222222", "AlertTriggered", "SecurityComplianceCenter", "admin@example.com", created)
	m.addBlob(ct, contentBlob{id: "blob-a", created: created, records: []string{dup, other}})
	m.addBlob(ct, contentBlob{id: "blob-b", created: created, records: []string{dup}})

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newOffice365Adapter(ctx, newTestConfig(t, m, ct), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "two distinct records should ship")
	require.Never(t, func() bool { return sink.count() != 2 },
		400*time.Millisecond, 30*time.Millisecond, "the duplicated Id must not ship twice")

	counts := sink.payloadCounts()
	assert.Equal(t, 1, counts[dup], "duplicate Id across blobs ships once")
	assert.Equal(t, 1, counts[other])
}

// TestMockStartTimeFirstPoll verifies a configured start_time widens the very
// first poll's window so older content is collected once.
func TestMockStartTimeFirstPoll(t *testing.T) {
	const ct = "Audit.Exchange"
	m := newMockO365(t)

	oldRec := auditRecord(t, "33330000-1111-1111-1111-111111111111", "HardDelete", "Exchange", "jdoe@example.com", time.Now().UTC().Add(-5*time.Hour))
	newRec := auditRecord(t, "44440000-2222-2222-2222-222222222222", "Send", "Exchange", "asmith@example.com", time.Now().UTC().Add(-10*time.Minute))
	m.addBlob(ct, contentBlob{id: "blob-old", created: time.Now().UTC().Add(-5 * time.Hour), records: []string{oldRec}})
	m.addBlob(ct, contentBlob{id: "blob-new", created: time.Now().UTC().Add(-10 * time.Minute), records: []string{newRec}})

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := newTestConfig(t, m, ct)
	conf.StartTime = time.Now().UTC().Add(-6 * time.Hour).Format(aadTimeLayout)
	adapter, _, err := newOffice365Adapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "start_time should pull in the older record too")
	require.Never(t, func() bool { return sink.count() != 2 },
		400*time.Millisecond, 30*time.Millisecond, "nothing may ship twice after the first poll")

	counts := sink.payloadCounts()
	assert.Equal(t, 1, counts[oldRec])
	assert.Equal(t, 1, counts[newRec])
}
