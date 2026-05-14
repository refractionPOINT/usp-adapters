package usp_harmony

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
)

// ----- Pure helpers ------------------------------------------------------------------------

func TestRestoreLifecycleState(t *testing.T) {
	cases := []struct {
		name     string
		payload  utils.Dict
		expected string
	}{
		{
			name:     "pending when neither flag is set (string form)",
			payload:  utils.Dict{"isRestoreRequested": "true", "isRestored": "false", "isRestoreDeclined": "false"},
			expected: "pending",
		},
		{
			name:     "restored (string form, as docs show)",
			payload:  utils.Dict{"isRestoreRequested": "true", "isRestored": "true", "isRestoreDeclined": "false"},
			expected: "restored",
		},
		{
			name:     "restored (bool form, as live gateway emits)",
			payload:  utils.Dict{"isRestoreRequested": true, "isRestored": true, "isRestoreDeclined": false},
			expected: "restored",
		},
		{
			name:     "declined (bool form)",
			payload:  utils.Dict{"isRestoreRequested": true, "isRestored": false, "isRestoreDeclined": true},
			expected: "declined",
		},
		{
			name:     "case-insensitive match on string form",
			payload:  utils.Dict{"isRestored": "True"},
			expected: "restored",
		},
		{
			name:     "restored wins when both decision flags are oddly set",
			payload:  utils.Dict{"isRestored": true, "isRestoreDeclined": true},
			expected: "restored",
		},
		{
			name:     "missing payload defaults to pending",
			payload:  nil,
			expected: "pending",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := utils.Dict{"entityPayload": c.payload}
			got := restoreLifecycleState(rec)
			if got != c.expected {
				t.Fatalf("expected %q, got %q", c.expected, got)
			}
		})
	}
}

func TestRestoreDedupKey(t *testing.T) {
	t.Run("no entityId returns empty key so caller skips it", func(t *testing.T) {
		if got := restoreDedupKey(utils.Dict{}); got != "" {
			t.Fatalf("expected empty key, got %q", got)
		}
	})

	t.Run("entityUpdated drives the key", func(t *testing.T) {
		a := utils.Dict{"entityInfo": utils.Dict{"entityId": "abc", "entityUpdated": "2026-05-12T10:00:00Z"}}
		b := utils.Dict{"entityInfo": utils.Dict{"entityId": "abc", "entityUpdated": "2026-05-12T10:00:00Z"}}
		c := utils.Dict{"entityInfo": utils.Dict{"entityId": "abc", "entityUpdated": "2026-05-12T11:00:00Z"}}
		if restoreDedupKey(a) != restoreDedupKey(b) {
			t.Fatalf("same entityUpdated should produce same key")
		}
		if restoreDedupKey(a) == restoreDedupKey(c) {
			t.Fatalf("different entityUpdated should produce different keys (transition)")
		}
	})

	t.Run("falls back to flag fingerprint when entityUpdated is absent", func(t *testing.T) {
		// Mix string and bool forms — the fingerprint must change between
		// states regardless of which encoding the gateway returns.
		pending := utils.Dict{
			"entityInfo":    utils.Dict{"entityId": "abc"},
			"entityPayload": utils.Dict{"isRestoreRequested": true, "isRestored": false, "isRestoreDeclined": false},
		}
		restored := utils.Dict{
			"entityInfo":    utils.Dict{"entityId": "abc"},
			"entityPayload": utils.Dict{"isRestoreRequested": true, "isRestored": true, "isRestoreDeclined": false},
		}
		if restoreDedupKey(pending) == restoreDedupKey(restored) {
			t.Fatalf("state transition should change the dedup key even without entityUpdated")
		}
	})
}

func TestExtractRecordTime(t *testing.T) {
	cases := []struct {
		name string
		rec  utils.Dict
		want string // RFC3339Nano; empty string means zero time
	}{
		{"rfc3339 no fractional", utils.Dict{"time": "2026-05-12T19:45:42Z"}, "2026-05-12T19:45:42Z"},
		{"rfc3339nano fractional", utils.Dict{"time": "2026-05-12T19:45:42.017013Z"}, "2026-05-12T19:45:42.017013Z"},
		{"eventTime fallback", utils.Dict{"eventTime": "2026-05-12T19:45:42Z"}, "2026-05-12T19:45:42Z"},
		{"timestamp fallback", utils.Dict{"timestamp": "2026-05-12T19:45:42Z"}, "2026-05-12T19:45:42Z"},
		{"missing all", utils.Dict{}, ""},
		{"nil record", nil, ""},
		{"garbage value", utils.Dict{"time": "not-a-time"}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := extractRecordTime(c.rec)
			if c.want == "" {
				if !got.IsZero() {
					t.Fatalf("expected zero time, got %s", got)
				}
				return
			}
			if got.IsZero() {
				t.Fatalf("expected %s, got zero time", c.want)
			}
			if got.UTC().Format(time.RFC3339Nano) != c.want {
				t.Fatalf("expected %s, got %s", c.want, got.UTC().Format(time.RFC3339Nano))
			}
		})
	}
}

func TestNewRequestIDFormat(t *testing.T) {
	uuidLike := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	seen := map[string]struct{}{}
	for i := 0; i < 1000; i++ {
		id := newRequestID()
		if !uuidLike.MatchString(id) {
			t.Fatalf("id %q does not match UUID-shape pattern", id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id within batch: %q", id)
		}
		seen[id] = struct{}{}
	}
}

// ----- Validate ----------------------------------------------------------------------------

// validClientOptions returns a ClientOptions populated with the minimum
// fields required to satisfy uspclient validation while keeping the client
// in TestSinkMode (no network). The three callback hooks are non-nil because
// the adapter calls DebugLog / OnError / OnWarning unconditionally.
func validClientOptions() uspclient.ClientOptions {
	return uspclient.ClientOptions{
		Identity: uspclient.Identity{
			Oid:             "00000000-0000-0000-0000-000000000001",
			InstallationKey: "00000000-0000-0000-0000-000000000002",
		},
		Platform:      "json",
		SensorSeedKey: "harmony-test",
		TestSinkMode:  true,
		DebugLog:      func(string) {},
		OnError:       func(error) {},
		OnWarning:     func(string) {},
	}
}

func TestValidate(t *testing.T) {
	t.Run("missing credentials", func(t *testing.T) {
		c := HarmonyConfig{ClientOptions: validClientOptions(), Events: EventsConfig{Enabled: true}}
		if err := c.Validate(); err == nil {
			t.Fatalf("expected error for missing client_id")
		}
		c.ClientID = "x"
		if err := c.Validate(); err == nil {
			t.Fatalf("expected error for missing access_key")
		}
	})

	t.Run("no source enabled is rejected", func(t *testing.T) {
		c := HarmonyConfig{ClientOptions: validClientOptions(), ClientID: "x", AccessKey: "y"}
		if err := c.Validate(); err == nil {
			t.Fatalf("expected error when neither source is enabled")
		}
	})

	t.Run("defaults are filled in for events", func(t *testing.T) {
		c := HarmonyConfig{
			ClientOptions: validClientOptions(),
			ClientID:      "x", AccessKey: "y",
			Events: EventsConfig{Enabled: true},
		}
		if err := c.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.URL != defaultBaseURL {
			t.Fatalf("expected default URL, got %q", c.URL)
		}
		if len(c.Events.CloudServices) != len(defaultEventsCloudServices) {
			t.Fatalf("expected default cloud services, got %v", c.Events.CloudServices)
		}
		// Make sure none of the defaults use the "and" spelling — the gateway
		// rejects that and it's easy for someone to copy the wrong form.
		for _, svc := range c.Events.CloudServices {
			if strings.Contains(svc, " and ") {
				t.Fatalf("default cloud service %q uses 'and' instead of '&'", svc)
			}
		}
		if c.Events.PollInterval != defaultEventsPollInterval {
			t.Fatalf("expected default poll interval, got %s", c.Events.PollInterval)
		}
	})

	t.Run("gateway minimum 10 is enforced for page_limit and limit", func(t *testing.T) {
		c := HarmonyConfig{
			ClientOptions: validClientOptions(),
			ClientID:      "x", AccessKey: "y",
			Events: EventsConfig{Enabled: true, PageLimit: 5, Limit: 1},
		}
		if err := c.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.Events.PageLimit < 10 || c.Events.Limit < 10 {
			t.Fatalf("page_limit/limit should be clamped to 10, got %d/%d", c.Events.PageLimit, c.Events.Limit)
		}
	})

	t.Run("trailing slash on URL is normalized", func(t *testing.T) {
		c := HarmonyConfig{
			ClientOptions: validClientOptions(),
			ClientID:      "x", AccessKey: "y",
			URL:    "https://example.com/",
			Events: EventsConfig{Enabled: true},
		}
		if err := c.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.URL != "https://example.com" {
			t.Fatalf("expected trailing slash trimmed, got %q", c.URL)
		}
	})

	t.Run("unsupported saas value is rejected", func(t *testing.T) {
		c := HarmonyConfig{
			ClientOptions: validClientOptions(),
			ClientID:      "x", AccessKey: "y",
			RestoreRequests: RestoreRequestsConfig{Enabled: true, Saas: []string{"slack"}},
		}
		if err := c.Validate(); err == nil {
			t.Fatalf("expected error for unsupported saas")
		}
	})

	t.Run("restore_requests defaults fill in", func(t *testing.T) {
		c := HarmonyConfig{
			ClientOptions: validClientOptions(),
			ClientID:      "x", AccessKey: "y",
			RestoreRequests: RestoreRequestsConfig{Enabled: true},
		}
		if err := c.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.RestoreRequests.PollInterval != defaultRestorePollInterval {
			t.Fatalf("expected default poll interval, got %s", c.RestoreRequests.PollInterval)
		}
		if c.RestoreRequests.Lookback != defaultRestoreLookback {
			t.Fatalf("expected default lookback, got %s", c.RestoreRequests.Lookback)
		}
		if len(c.RestoreRequests.Saas) != len(defaultRestoreSaas) {
			t.Fatalf("expected default saas list")
		}
	})
}

// ----- HTTP integration with httptest ------------------------------------------------------

// fakeGateway is a stand-in for the Infinity Portal gateway: /auth/external,
// the laas-logs-api submit/poll/retrieve sequence, and the HEC search endpoint.
type fakeGateway struct {
	mu sync.Mutex

	authCalls       int32
	submitCalls     int32
	statusCalls     int32
	retrieveCalls   int32
	searchCalls     int32
	rejectFirstAuth bool // simulate one 401 then accept

	currentToken string

	// Records returned by laas retrieve, optionally split into multiple pages.
	eventPages [][]utils.Dict

	// Records returned by the HEC search; can be swapped at runtime to simulate transitions.
	hecRecords []utils.Dict
}

func (f *fakeGateway) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == authPath:
			f.serveAuth(w, r)
		case r.URL.Path == eventsQueryPath && r.Method == http.MethodPost:
			f.serveEventsSubmit(w, r)
		case r.URL.Path == eventsRetrievePath && r.Method == http.MethodPost:
			f.serveEventsRetrieve(w, r)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, eventsQueryPath+"/"):
			f.serveEventsStatus(w, r)
		case r.URL.Path == hecSearchEntityPath:
			f.serveHECSearch(w, r)
		default:
			http.NotFound(w, r)
		}
	}
}

func (f *fakeGateway) serveAuth(w http.ResponseWriter, r *http.Request) {
	n := atomic.AddInt32(&f.authCalls, 1)
	f.mu.Lock()
	f.currentToken = fmt.Sprintf("test-token-%d", n)
	token := f.currentToken
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, utils.Dict{
		"success": true,
		"data": utils.Dict{
			"token":     token,
			"expiresIn": 1800,
		},
	})
}

func (f *fakeGateway) checkAuth(w http.ResponseWriter, r *http.Request) bool {
	got := r.Header.Get("Authorization")
	f.mu.Lock()
	expected := "Bearer " + f.currentToken
	reject := f.rejectFirstAuth
	if reject {
		f.rejectFirstAuth = false
	}
	f.mu.Unlock()
	if reject || got != expected {
		writeJSON(w, http.StatusUnauthorized, utils.Dict{"message": "Authentication required"})
		return false
	}
	return true
}

func (f *fakeGateway) serveEventsSubmit(w http.ResponseWriter, r *http.Request) {
	if !f.checkAuth(w, r) {
		return
	}
	atomic.AddInt32(&f.submitCalls, 1)
	writeJSON(w, http.StatusOK, utils.Dict{"success": true, "data": utils.Dict{"taskId": "task-1"}})
}

func (f *fakeGateway) serveEventsStatus(w http.ResponseWriter, r *http.Request) {
	if !f.checkAuth(w, r) {
		return
	}
	atomic.AddInt32(&f.statusCalls, 1)
	tokens := []string{}
	for i := range f.eventPages {
		tokens = append(tokens, fmt.Sprintf("page-%d", i))
	}
	if len(tokens) == 0 {
		writeJSON(w, http.StatusOK, utils.Dict{"data": utils.Dict{"state": "Done"}})
		return
	}
	writeJSON(w, http.StatusOK, utils.Dict{"data": utils.Dict{"state": "Ready", "pageTokens": tokens}})
}

func (f *fakeGateway) serveEventsRetrieve(w http.ResponseWriter, r *http.Request) {
	if !f.checkAuth(w, r) {
		return
	}
	atomic.AddInt32(&f.retrieveCalls, 1)
	var body utils.Dict
	_ = json.NewDecoder(r.Body).Decode(&body)
	pt, _ := body.GetString("pageToken")
	var idx int
	if _, err := fmt.Sscanf(pt, "page-%d", &idx); err != nil || idx < 0 || idx >= len(f.eventPages) {
		idx = 0
	}
	next := "NULL"
	if idx+1 < len(f.eventPages) {
		next = fmt.Sprintf("page-%d", idx+1)
	}
	writeJSON(w, http.StatusOK, utils.Dict{"data": utils.Dict{
		"records":       dictListToInterface(f.eventPages[idx]),
		"nextPageToken": next,
	}})
}

func (f *fakeGateway) serveHECSearch(w http.ResponseWriter, r *http.Request) {
	if !f.checkAuth(w, r) {
		return
	}
	atomic.AddInt32(&f.searchCalls, 1)
	f.mu.Lock()
	records := append([]utils.Dict{}, f.hecRecords...)
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, utils.Dict{
		"responseEnvelope": utils.Dict{"recordsNumber": len(records), "scrollId": ""},
		"responseData":     dictListToInterface(records),
	})
}

func writeJSON(w http.ResponseWriter, status int, body utils.Dict) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func dictListToInterface(in []utils.Dict) []interface{} {
	out := make([]interface{}, len(in))
	for i, v := range in {
		out[i] = map[string]interface{}(v)
	}
	return out
}

func TestEventsFlowWithMockServer(t *testing.T) {
	fake := &fakeGateway{
		eventPages: [][]utils.Dict{
			{
				{"id": "a", "time": "2026-05-12T10:00:00Z", "subject": "first"},
				{"id": "b", "time": "2026-05-12T10:01:00Z", "subject": "second"},
			},
			{
				{"id": "c", "time": "2026-05-12T10:02:00Z", "subject": "third"},
			},
		},
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	conf := HarmonyConfig{
		ClientOptions: validClientOptions(),
		ClientID:      "client", AccessKey: "secret",
		URL: srv.URL,
		Events: EventsConfig{
			Enabled:       true,
			CloudServices: []string{"Harmony Endpoint"},
			PollInterval:  10 * time.Millisecond,
		},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	waitUntil(t, 3*time.Second, func() bool {
		return atomic.LoadInt32(&fake.retrieveCalls) >= 2
	}, "expected both pages to be retrieved")

	if got := atomic.LoadInt32(&fake.authCalls); got == 0 {
		t.Fatalf("expected auth to be called, got 0")
	}
	if got := atomic.LoadInt32(&fake.submitCalls); got == 0 {
		t.Fatalf("expected submit to be called, got 0")
	}
}

func TestEventsReAuthOn401(t *testing.T) {
	fake := &fakeGateway{
		rejectFirstAuth: true,
		eventPages:      [][]utils.Dict{{{"id": "a", "time": "2026-05-12T10:00:00Z"}}},
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	conf := HarmonyConfig{
		ClientOptions: validClientOptions(),
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Events: EventsConfig{Enabled: true, CloudServices: []string{"Harmony Endpoint"}, PollInterval: 10 * time.Millisecond},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	waitUntil(t, 3*time.Second, func() bool {
		return atomic.LoadInt32(&fake.retrieveCalls) >= 1 && atomic.LoadInt32(&fake.authCalls) >= 2
	}, "expected re-auth after 401 followed by a successful retrieve")
}

// recordingDeduper wraps an inner deduper and notifies on each admitted key.
// We use it to observe which restore-request entities the adapter ships.
type recordingDeduper struct {
	mu       sync.Mutex
	admitted []string
	seen     map[string]struct{}
}

func newRecordingDeduper() *recordingDeduper {
	return &recordingDeduper{seen: map[string]struct{}{}}
}

func (d *recordingDeduper) CheckAndAdd(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[key]; ok {
		return true
	}
	d.seen[key] = struct{}{}
	d.admitted = append(d.admitted, key)
	return false
}

func (d *recordingDeduper) Close() {}

func (d *recordingDeduper) admittedKeys() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.admitted...)
}

// TestRestoreRequestsDedupAndTransition verifies the lifecycle:
//   - On first poll, a pending request is admitted to dedup (and therefore shipped).
//   - On subsequent polls with no change, the same entity is suppressed.
//   - When the gateway bumps entityUpdated to reflect an admin decision, a new
//     dedup key admits the entity again, capturing the transition.
func TestRestoreRequestsDedupAndTransition(t *testing.T) {
	fake := &fakeGateway{}
	// Use the bool form here because that's what the live gateway returns —
	// the test would have passed against either form before the bool fix,
	// but using booleans pins the assertion to the wire shape we'll actually
	// see in production.
	fake.hecRecords = []utils.Dict{
		{
			"entityInfo":    utils.Dict{"entityId": "e1", "entityUpdated": "2026-05-12T10:00:00Z"},
			"entityPayload": utils.Dict{"isRestoreRequested": true, "isRestored": false, "isRestoreDeclined": false},
		},
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	dedup := newRecordingDeduper()

	conf := HarmonyConfig{
		ClientOptions: validClientOptions(),
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		RestoreRequests: RestoreRequestsConfig{
			Enabled:      true,
			Saas:         []string{"office365_emails"},
			PollInterval: 30 * time.Millisecond,
			Lookback:     1 * time.Hour,
			Deduper:      dedup,
		},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	// Wait until the first admission lands.
	waitUntil(t, 2*time.Second, func() bool {
		return len(dedup.admittedKeys()) >= 1
	}, "expected the first poll to admit the pending entity")

	pendingKey := dedup.admittedKeys()[0]
	if !strings.Contains(pendingKey, "e1") || !strings.Contains(pendingKey, "2026-05-12T10:00:00Z") {
		t.Fatalf("unexpected pending dedup key: %q", pendingKey)
	}

	// Let a few more polls happen — count must stay at 1 since the record hasn't changed.
	time.Sleep(200 * time.Millisecond)
	if got := len(dedup.admittedKeys()); got != 1 {
		t.Fatalf("expected dedup to suppress repeats; admitted=%d keys=%v", got, dedup.admittedKeys())
	}
	if got := atomic.LoadInt32(&fake.searchCalls); got < 2 {
		t.Fatalf("expected multiple search polls; got %d", got)
	}

	// Now flip the record to "restored" with a fresh entityUpdated.
	fake.mu.Lock()
	fake.hecRecords[0] = utils.Dict{
		"entityInfo":    utils.Dict{"entityId": "e1", "entityUpdated": "2026-05-12T11:00:00Z"},
		"entityPayload": utils.Dict{"isRestoreRequested": true, "isRestored": true, "isRestoreDeclined": false},
	}
	fake.mu.Unlock()

	waitUntil(t, 2*time.Second, func() bool {
		return len(dedup.admittedKeys()) >= 2
	}, "expected the transition to produce a second admission")

	keys := dedup.admittedKeys()
	transitionKey := keys[1]
	if !strings.Contains(transitionKey, "2026-05-12T11:00:00Z") {
		t.Fatalf("expected transition key to include new entityUpdated, got %q", transitionKey)
	}
}

// TestRestoreRequestsIncludeResolvedIssuesExtraQueries asserts that toggling
// IncludeResolved makes the adapter run three filtered queries per poll
// (isRestoreRequested, isRestored, isRestoreDeclined). Without the toggle it
// should only issue one.
func TestRestoreRequestsIncludeResolvedIssuesExtraQueries(t *testing.T) {
	mk := func(includeResolved bool) (queriesPerPoll int) {
		fake := &fakeGateway{}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()

		conf := HarmonyConfig{
			ClientOptions: validClientOptions(),
			ClientID:      "c", AccessKey: "s", URL: srv.URL,
			RestoreRequests: RestoreRequestsConfig{
				Enabled:         true,
				Saas:            []string{"office365_emails"},
				PollInterval:    1 * time.Hour, // single poll
				Lookback:        1 * time.Hour,
				IncludeResolved: includeResolved,
				Deduper:         newRecordingDeduper(),
			},
		}
		adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
		if err != nil {
			t.Fatalf("NewHarmonyAdapter: %v", err)
		}
		// Allow the initial-poll burst to complete.
		time.Sleep(300 * time.Millisecond)
		adapter.Close()
		return int(atomic.LoadInt32(&fake.searchCalls))
	}

	if got := mk(false); got != 1 {
		t.Fatalf("expected 1 search call when IncludeResolved=false, got %d", got)
	}
	if got := mk(true); got != 3 {
		t.Fatalf("expected 3 search calls when IncludeResolved=true, got %d", got)
	}
}

// waitUntil polls cond until it returns true or the timeout elapses. Fails
// the test with msg on timeout. Used everywhere we'd otherwise need a manual
// loop + select; keeps the test bodies focused on assertions.
func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timed out after %s: %s", timeout, msg)
}
