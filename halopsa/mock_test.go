package usp_halopsa

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
)

// mockHaloPSA emulates the bits of the real HaloPSA OAuth2 + REST API the
// adapter relies on: the /auth/token endpoint and any number of resource
// endpoints under /api/<resource>. Response shapes mirror what HaloPSA's
// /api/Tickets, /api/Actions, and /api/Agent endpoints actually return,
// including the {"<resource>": [...]} envelope and the {"results": [...]}
// shape used by /api/Agent.
type mockHaloPSA struct {
	server *httptest.Server

	mu sync.Mutex

	// resource state: name -> records, newest-first (highest id first).
	resources map[string][]utils.Dict
	// dataField per resource (e.g. "tickets", "actions", "results").
	dataFields map[string]string

	// auth state
	clientID      string
	clientSecret  string
	tokens        map[string]time.Time
	tokenTTL      time.Duration
	requireScope  string
	authCalls     atomic.Int32
	expectedGrant string

	// observed request inspection
	lastQuery map[string]string

	// fault injection
	failNextTokenFetches atomic.Int32
	failNextAPICalls     atomic.Int32
}

func newMockHaloPSA(t *testing.T) *mockHaloPSA {
	t.Helper()
	m := &mockHaloPSA{
		clientID:      "test-client",
		clientSecret:  "test-secret",
		resources:     map[string][]utils.Dict{},
		dataFields:    map[string]string{},
		tokens:        map[string]time.Time{},
		tokenTTL:      2 * time.Hour,
		requireScope:  "all",
		expectedGrant: "client_credentials",
		lastQuery:     map[string]string{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/token", m.handleToken)
	mux.HandleFunc("/api/", m.handleAPI)
	m.server = httptest.NewServer(mux)
	t.Cleanup(m.server.Close)
	return m
}

func (m *mockHaloPSA) URL() string { return m.server.URL }

func (m *mockHaloPSA) setResource(name, dataField string, records []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resources[name] = records
	if dataField != "" {
		m.dataFields[name] = dataField
	}
}

func (m *mockHaloPSA) prependRecord(name string, rec utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resources[name] = append([]utils.Dict{rec}, m.resources[name]...)
}

func (m *mockHaloPSA) handleToken(w http.ResponseWriter, r *http.Request) {
	m.authCalls.Add(1)
	if n := m.failNextTokenFetches.Add(-1); n >= 0 {
		http.Error(w, `{"error":"server_error"}`, http.StatusInternalServerError)
		return
	}
	// Restore to zero — Add(-1) goes negative when no failures pending.
	m.failNextTokenFetches.Store(0)

	_ = r.ParseForm()
	if r.Form.Get("grant_type") != m.expectedGrant {
		http.Error(w, `{"error":"unsupported_grant_type"}`, http.StatusBadRequest)
		return
	}
	if r.Form.Get("client_id") != m.clientID || r.Form.Get("client_secret") != m.clientSecret {
		http.Error(w, `{"error":"invalid_client","error_description":"The specified client credentials are invalid."}`, http.StatusUnauthorized)
		return
	}
	if m.requireScope != "" && r.Form.Get("scope") != m.requireScope {
		http.Error(w, `{"error":"invalid_scope"}`, http.StatusBadRequest)
		return
	}

	tok := fmt.Sprintf("tok-%d", time.Now().UnixNano())
	m.mu.Lock()
	m.tokens[tok] = time.Now().Add(m.tokenTTL)
	m.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"access_token": tok,
		"token_type":   "Bearer",
		"expires_in":   int(m.tokenTTL.Seconds()),
		"scope":        m.requireScope,
	})
}

func (m *mockHaloPSA) handleAPI(w http.ResponseWriter, r *http.Request) {
	if n := m.failNextAPICalls.Add(-1); n >= 0 {
		http.Error(w, `{"error":"backend_unavailable"}`, http.StatusBadGateway)
		return
	}
	m.failNextAPICalls.Store(0)

	auth := r.Header.Get("Authorization")
	if len(auth) < len("Bearer ") || auth[:len("Bearer ")] != "Bearer " {
		http.Error(w, "missing bearer", http.StatusUnauthorized)
		return
	}
	tok := auth[len("Bearer "):]
	m.mu.Lock()
	exp, ok := m.tokens[tok]
	m.mu.Unlock()
	if !ok || time.Now().After(exp) {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	// Capture the query for assertions.
	m.mu.Lock()
	m.lastQuery = map[string]string{}
	for k := range r.URL.Query() {
		m.lastQuery[k] = r.URL.Query().Get(k)
	}
	m.mu.Unlock()

	// Strip "/api/" prefix.
	resource := r.URL.Path[len("/api/"):]
	m.mu.Lock()
	records, known := m.resources[resource]
	dataField := m.dataFields[resource]
	m.mu.Unlock()
	if !known {
		http.NotFound(w, r)
		return
	}
	if dataField == "" {
		dataField = lowerFirstChar(resource)
	}

	// Honour orderdesc=true: records are stored newest-first.
	// (Real Halo also exposes orderdesc=false, but the adapter only requests desc.)

	pageNo, _ := strconv.Atoi(r.URL.Query().Get("page_no"))
	if pageNo < 1 {
		pageNo = 1
	}
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	if pageSize <= 0 {
		pageSize = 50
	}

	total := len(records)
	start := (pageNo - 1) * pageSize
	end := start + pageSize
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}
	page := records[start:end]

	w.Header().Set("Content-Type", "application/json")
	if r.URL.Query().Get("envelope_style") == "bare_array" {
		_ = json.NewEncoder(w).Encode(page)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"page_no":      pageNo,
		"page_size":    pageSize,
		"record_count": total,
		dataField:      page,
	})
}

func lowerFirstChar(s string) string {
	if s == "" {
		return s
	}
	b := []byte(s)
	if b[0] >= 'A' && b[0] <= 'Z' {
		b[0] += 'a' - 'A'
	}
	return string(b)
}

// newTestAdapter builds a HaloPSAAdapter pointing at the mock, with sane
// no-op log callbacks so the adapter doesn't nil-panic in tests.
func newTestAdapter(t *testing.T, m *mockHaloPSA, endpoint string) *HaloPSAAdapter {
	t.Helper()
	logs := func(string) {}
	logE := func(err error) { t.Logf("adapter error: %v", err) }
	a := &HaloPSAAdapter{
		conf: HaloPSAConfig{
			ClientOptions: uspclient.ClientOptions{
				DebugLog:  logs,
				OnError:   logE,
				OnWarning: logs,
			},
			InstanceURL:  m.URL(),
			ClientID:     m.clientID,
			ClientSecret: m.clientSecret,
			Scope:        m.requireScope,
			IDField:      defaultIDField,
			Endpoint:     endpoint,
			PageSize:     5,
			PollInterval: defaultPollInterval,
		},
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
		httpClient: &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{Dial: (&net.Dialer{Timeout: 2 * time.Second}).Dial},
		},
	}
	return a
}

// rec is a tiny helper to build a record carrying a numeric id and arbitrary
// extra fields, matching the shape of real HaloPSA records.
func rec(id int, extras ...any) utils.Dict {
	d := utils.Dict{"id": uint64(id)}
	for i := 0; i+1 < len(extras); i += 2 {
		k, _ := extras[i].(string)
		d[k] = extras[i+1]
	}
	return d
}

// newestFirst returns records ordered newest-first (highest id first).
func newestFirst(start, count int) []utils.Dict {
	out := make([]utils.Dict, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, rec(start-i, "summary", fmt.Sprintf("ticket-%d", start-i)))
	}
	return out
}

func TestMock_TokenFetchAndCaching(t *testing.T) {
	m := newMockHaloPSA(t)
	a := newTestAdapter(t, m, "Tickets")

	tok1, err := a.getToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok1 == "" {
		t.Fatal("empty token")
	}
	if got := m.authCalls.Load(); got != 1 {
		t.Fatalf("authCalls = %d, want 1", got)
	}

	// Second call reuses the cached token; no extra auth hit.
	tok2, _ := a.getToken()
	if tok2 != tok1 {
		t.Fatal("expected cached token reuse")
	}
	if got := m.authCalls.Load(); got != 1 {
		t.Fatalf("authCalls after cached read = %d, want 1", got)
	}

	// Force expiry → next call must re-authenticate.
	a.tokenExpiry = time.Now().Add(-time.Second)
	tok3, _ := a.getToken()
	if tok3 == tok1 {
		t.Fatal("expected new token after expiry")
	}
	if got := m.authCalls.Load(); got != 2 {
		t.Fatalf("authCalls after refresh = %d, want 2", got)
	}
}

func TestMock_TokenFetchFailsOnBadCreds(t *testing.T) {
	m := newMockHaloPSA(t)
	a := newTestAdapter(t, m, "Tickets")
	a.conf.ClientSecret = "wrong"

	_, err := a.getToken()
	if err == nil {
		t.Fatal("expected error on bad creds")
	}
}

func TestMock_RequestPage_TicketsEnvelope(t *testing.T) {
	m := newMockHaloPSA(t)
	m.setResource("Tickets", "tickets", newestFirst(110, 5))
	a := newTestAdapter(t, m, "Tickets")

	tok, _ := a.getToken()
	items, count, err := a.requestPage(tok, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Fatalf("record_count=%d, want 5", count)
	}
	want := []uint64{110, 109, 108, 107, 106}
	if got := idsOf(items); !reflect.DeepEqual(got, want) {
		t.Fatalf("ids=%v, want %v", got, want)
	}

	// Adapter must have sent the expected query parameters.
	expectedQuery := map[string]string{
		"pageinate":  "true",
		"page_size":  "5",
		"page_no":    "1",
		"order":      "id",
		"orderdesc":  "true",
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range expectedQuery {
		if got := m.lastQuery[k]; got != v {
			t.Fatalf("query %q=%q, want %q", k, got, v)
		}
	}
}

func TestMock_RequestPage_AgentResultsEnvelope(t *testing.T) {
	// Real-world quirk: the /api/Agent endpoint returns its records under a
	// "results" key rather than "agent". The auto-detect must still pick it up.
	m := newMockHaloPSA(t)
	m.setResource("Agent", "results", []utils.Dict{
		rec(14, "name", "Maxime Lamothe-Brassard"),
		rec(3, "name", "Bruce Wayne"),
		rec(1, "name", "Unassigned"),
	})
	a := newTestAdapter(t, m, "Agent")

	tok, _ := a.getToken()
	items, count, err := a.requestPage(tok, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("record_count=%d, want 3", count)
	}
	if got := idsOf(items); !reflect.DeepEqual(got, []uint64{14, 3, 1}) {
		t.Fatalf("ids=%v, want [14 3 1]", got)
	}
}

func TestMock_RequestPage_ExplicitDataField(t *testing.T) {
	m := newMockHaloPSA(t)
	m.setResource("Custom", "weird_name", []utils.Dict{rec(1), rec(2)})
	a := newTestAdapter(t, m, "Custom")
	a.conf.DataField = "weird_name"

	tok, _ := a.getToken()
	items, _, err := a.requestPage(tok, 1)
	if err != nil || len(items) != 2 {
		t.Fatalf("items=%d err=%v", len(items), err)
	}
}

func TestMock_RequestPage_401ClearsToken(t *testing.T) {
	m := newMockHaloPSA(t)
	m.setResource("Tickets", "tickets", newestFirst(100, 3))
	a := newTestAdapter(t, m, "Tickets")

	_, _ = a.getToken()
	// Sabotage the cached token. The adapter must clear it on 401 so the
	// next poll fetches a fresh one (real-world recovery from token rotation).
	a.accessToken = "obviously-invalid"
	a.tokenExpiry = time.Now().Add(time.Hour)

	_, _, err := a.requestPage(a.accessToken, 1)
	if err == nil {
		t.Fatal("expected error from 401")
	}
	if a.accessToken != "" {
		t.Fatalf("accessToken should have been cleared, got %q", a.accessToken)
	}
}

func TestMock_RequestPage_404Errors(t *testing.T) {
	m := newMockHaloPSA(t)
	a := newTestAdapter(t, m, "DoesNotExist")
	tok, _ := a.getToken()
	if _, _, err := a.requestPage(tok, 1); err == nil {
		t.Fatal("expected error on unknown endpoint")
	}
}

func TestMock_ExtraParamsForwarded(t *testing.T) {
	m := newMockHaloPSA(t)
	m.setResource("Tickets", "tickets", newestFirst(100, 1))
	a := newTestAdapter(t, m, "Tickets")
	a.conf.ExtraParams = map[string]interface{}{
		"open_only":  true,
		"client_id":  42,
		"agent_name": "alice",
	}

	tok, _ := a.getToken()
	if _, _, err := a.requestPage(tok, 1); err != nil {
		t.Fatal(err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for k, want := range map[string]string{
		"open_only":  "true",
		"client_id":  "42",
		"agent_name": "alice",
	} {
		if got := m.lastQuery[k]; got != want {
			t.Fatalf("extra param %q forwarded as %q, want %q", k, got, want)
		}
	}
}

func TestMock_PollLifecycle(t *testing.T) {
	m := newMockHaloPSA(t)
	m.setResource("Tickets", "tickets", newestFirst(105, 5)) // 105,104,103,102,101
	a := newTestAdapter(t, m, "Tickets")

	// 1) Baseline poll establishes the cursor at the newest id and ships nothing.
	items, err := a.poll()
	if err != nil {
		t.Fatal(err)
	}
	if items != nil {
		t.Fatalf("baseline should ship nothing, got %d items", len(items))
	}
	if a.lastMaxID != 105 || !a.initialized {
		t.Fatalf("baseline cursor=%d initialized=%v, want cursor=105 initialized=true", a.lastMaxID, a.initialized)
	}

	// 2) No-op poll: nothing new, cursor unchanged.
	items, err = a.poll()
	if err != nil || len(items) != 0 || a.lastMaxID != 105 {
		t.Fatalf("noop poll: items=%d cursor=%d err=%v", len(items), a.lastMaxID, err)
	}

	// 3) A new ticket arrives.
	m.prependRecord("Tickets", rec(106, "summary", "new!"))
	items, err = a.poll()
	if err != nil {
		t.Fatal(err)
	}
	if got := idsOf(items); !reflect.DeepEqual(got, []uint64{106}) {
		t.Fatalf("incremental poll ids=%v, want [106]", got)
	}
	if a.lastMaxID != 106 {
		t.Fatalf("cursor after new record = %d, want 106", a.lastMaxID)
	}
}

func TestMock_CatchUpAcrossMultiplePages(t *testing.T) {
	// Simulate the adapter recovering from downtime: while it was offline,
	// many new records were added. Cursor advances correctly across pages.
	m := newMockHaloPSA(t)
	m.setResource("Tickets", "tickets", newestFirst(120, 20)) // 120..101
	a := newTestAdapter(t, m, "Tickets")
	a.conf.PageSize = 5

	// Force an initial state as if cursor was at 105 (the adapter "knew"
	// about 105 before downtime).
	a.initialized = true
	a.lastMaxID = 105

	items, err := a.poll()
	if err != nil {
		t.Fatal(err)
	}
	want := []uint64{106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120}
	if got := idsOf(items); !reflect.DeepEqual(got, want) {
		t.Fatalf("catch-up ids=%v\nwant %v", got, want)
	}
	if a.lastMaxID != 120 {
		t.Fatalf("cursor after catch-up = %d, want 120", a.lastMaxID)
	}
}

func TestMock_TransientAPIErrorPreservesCursor(t *testing.T) {
	m := newMockHaloPSA(t)
	m.setResource("Tickets", "tickets", newestFirst(110, 5))
	a := newTestAdapter(t, m, "Tickets")

	// Baseline.
	if _, err := a.poll(); err != nil {
		t.Fatal(err)
	}
	cursorBefore := a.lastMaxID

	// Inject a transient API error on the next poll.
	m.failNextAPICalls.Store(1)
	_, err := a.poll()
	if err == nil {
		t.Fatal("expected transient error")
	}
	if a.lastMaxID != cursorBefore {
		t.Fatalf("cursor moved on error: before=%d after=%d", cursorBefore, a.lastMaxID)
	}

	// Recovery: subsequent poll succeeds with cursor preserved.
	m.prependRecord("Tickets", rec(111))
	items, err := a.poll()
	if err != nil {
		t.Fatal(err)
	}
	if got := idsOf(items); !reflect.DeepEqual(got, []uint64{111}) {
		t.Fatalf("recovery poll ids=%v, want [111]", got)
	}
}

func TestMock_BareJSONArrayEndpoint(t *testing.T) {
	// Some HaloPSA list endpoints return a bare array instead of an envelope.
	// The adapter advertises this is supported; verify it works end-to-end.
	m := newMockHaloPSA(t)
	m.setResource("Tickets", "tickets", newestFirst(103, 3))
	a := newTestAdapter(t, m, "Tickets")
	// Trigger the mock's bare-array mode for every page request.
	a.conf.ExtraParams = map[string]interface{}{"envelope_style": "bare_array"}

	tok, _ := a.getToken()
	items, count, err := a.requestPage(tok, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("count=%d, want 3", count)
	}
	if got := idsOf(items); !reflect.DeepEqual(got, []uint64{103, 102, 101}) {
		t.Fatalf("ids=%v, want [103 102 101]", got)
	}
}

func TestMock_PageBeyondEndReturnsEmpty(t *testing.T) {
	// Confirms that fetchPage stops paginating when a page is empty, which is
	// the same shape the live API returns past the last page.
	m := newMockHaloPSA(t)
	m.setResource("Tickets", "tickets", newestFirst(102, 2)) // only 102, 101
	a := newTestAdapter(t, m, "Tickets")
	a.conf.PageSize = 2

	a.initialized = true
	a.lastMaxID = 50

	items, err := a.poll()
	if err != nil {
		t.Fatal(err)
	}
	if got := idsOf(items); !reflect.DeepEqual(got, []uint64{101, 102}) {
		t.Fatalf("ids=%v, want [101 102]", got)
	}
	if a.lastMaxID != 102 {
		t.Fatalf("cursor=%d, want 102", a.lastMaxID)
	}
}

