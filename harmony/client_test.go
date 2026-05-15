package usp_harmony

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
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

	// cancelEventsErrors is the errors[] payload returned alongside a
	// Canceled status. The gateway emits strings today; cancelEventsErrorsAny
	// lets a test stick non-string values in there to exercise the JSON
	// marshal fallback path.
	cancelEventsErrors    []string
	cancelEventsErrorsAny []interface{}

	// cancelEventsTimes controls how many status polls return Canceled
	// before the gateway reverts to normal Ready/Done behavior. Negative
	// values mean "always cancel" (used to model a not-provisioned cloud
	// service that never recovers).
	cancelEventsTimes int

	// retrieve503Times controls how many retrieve calls return a transient
	// 503 before succeeding — models the intermittent gateway slowness the
	// adapter must absorb via bounded retry. Negative = always 503.
	retrieve503Times int

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

	f.mu.Lock()
	shouldCancel := f.cancelEventsTimes != 0
	if f.cancelEventsTimes > 0 {
		f.cancelEventsTimes--
	}
	stringErrs := append([]string(nil), f.cancelEventsErrors...)
	anyErrs := append([]interface{}(nil), f.cancelEventsErrorsAny...)
	f.mu.Unlock()

	if shouldCancel {
		errs := make([]interface{}, 0, len(stringErrs)+len(anyErrs))
		for _, s := range stringErrs {
			errs = append(errs, s)
		}
		errs = append(errs, anyErrs...)
		writeJSON(w, http.StatusOK, utils.Dict{"data": utils.Dict{"state": "Canceled", "errors": errs}})
		return
	}

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

	f.mu.Lock()
	transient := f.retrieve503Times != 0
	if f.retrieve503Times > 0 {
		f.retrieve503Times--
	}
	f.mu.Unlock()
	if transient {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"success":false,"message":"temporarily unavailable"}`))
		return
	}

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

// TestEventsCanceledIsSoftFailure asserts that a logs_query task ending in
// state "Canceled" is treated as a per-service soft failure: OnWarning fires
// with the gateway's error detail, OnError stays silent, and the worker
// keeps polling instead of either bubbling the error up or wedging.
//
// This is the failure mode the gateway returns when a cloud service is
// listed in cloud_services but isn't actually provisioned for the tenant
// (e.g. "Harmony Connect" on a tenant that only has Email/Endpoint/Mobile).
// Bubbling it as OnError would spam the operator's error stream every
// poll_interval for every non-provisioned product in the default list.
func TestEventsCanceledIsSoftFailure(t *testing.T) {
	fake := &fakeGateway{
		cancelEventsErrors: []string{"Couldn't process request"},
		cancelEventsTimes:  -1, // persistently canceled, never recovers
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	var warnings []string
	var errors []string
	var mu sync.Mutex
	opts := validClientOptions()
	opts.OnWarning = func(msg string) {
		mu.Lock()
		warnings = append(warnings, msg)
		mu.Unlock()
	}
	opts.OnError = func(err error) {
		mu.Lock()
		errors = append(errors, err.Error())
		mu.Unlock()
	}

	conf := HarmonyConfig{
		ClientOptions: opts,
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Events: EventsConfig{Enabled: true, CloudServices: []string{"Harmony Connect"}, PollInterval: 10 * time.Millisecond},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	// Wait until we've seen at least two cancellations — proves the worker
	// keeps polling instead of wedging on the first one.
	waitUntil(t, 3*time.Second, func() bool {
		return atomic.LoadInt32(&fake.statusCalls) >= 2
	}, "expected the worker to keep polling after a Canceled status")

	mu.Lock()
	defer mu.Unlock()
	if len(errors) != 0 {
		t.Fatalf("Canceled should not surface as OnError; got %d errors: %v", len(errors), errors)
	}
	if len(warnings) == 0 {
		t.Fatalf("expected at least one OnWarning for the Canceled status")
	}
	if !strings.Contains(warnings[0], "Harmony Connect") {
		t.Fatalf("warning should name the offending service; got %q", warnings[0])
	}
	if !strings.Contains(warnings[0], "Couldn't process request") {
		t.Fatalf("warning should include the gateway's error detail; got %q", warnings[0])
	}
}

// TestEventsCanceledTransientRecovery covers the recovery case: the gateway
// cancels exactly one task and then resumes normal Ready/Done responses. The
// worker must surface a single OnWarning, no OnError, and eventually issue a
// retrieve once the gateway recovers — proving the cursor wasn't wedged and
// that the soft-fail path lets normal traffic resume on the next poll.
func TestEventsCanceledTransientRecovery(t *testing.T) {
	fake := &fakeGateway{
		cancelEventsErrors: []string{"Transient gateway hiccup"},
		cancelEventsTimes:  1, // cancel exactly once, then recover
		eventPages: [][]utils.Dict{
			{{"id": "post-recovery", "time": "2026-05-14T22:00:00Z"}},
		},
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	var warnings, errs []string
	var mu sync.Mutex
	opts := validClientOptions()
	opts.OnWarning = func(msg string) {
		mu.Lock()
		warnings = append(warnings, msg)
		mu.Unlock()
	}
	opts.OnError = func(err error) {
		mu.Lock()
		errs = append(errs, err.Error())
		mu.Unlock()
	}

	conf := HarmonyConfig{
		ClientOptions: opts,
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Events: EventsConfig{Enabled: true, CloudServices: []string{"Harmony Endpoint"}, PollInterval: 10 * time.Millisecond},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	// A successful retrieve only happens after the gateway recovers, so this
	// is the cleanest proof of "post-cancellation traffic flows again".
	waitUntil(t, 3*time.Second, func() bool {
		return atomic.LoadInt32(&fake.retrieveCalls) >= 1
	}, "expected a retrieve to succeed after the transient cancel")

	mu.Lock()
	defer mu.Unlock()
	if len(errs) != 0 {
		t.Fatalf("transient Canceled should not surface as OnError; got %d errors: %v", len(errs), errs)
	}
	if len(warnings) == 0 {
		t.Fatalf("expected exactly one OnWarning for the transient cancel")
	}
}

// TestEventsCanceledMarshalsStructuredErrorDetail pins the JSON-marshal
// fallback for non-string errors[] entries. Today Check Point returns
// strings; if it ever switches to objects we want the warning to still
// carry the detail rather than silently dropping it.
func TestEventsCanceledMarshalsStructuredErrorDetail(t *testing.T) {
	fake := &fakeGateway{
		cancelEventsErrorsAny: []interface{}{
			map[string]interface{}{"code": 5042, "message": "Service not provisioned"},
		},
		cancelEventsTimes: -1,
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	var warnings []string
	var mu sync.Mutex
	opts := validClientOptions()
	opts.OnWarning = func(msg string) {
		mu.Lock()
		warnings = append(warnings, msg)
		mu.Unlock()
	}
	opts.OnError = func(error) {}

	conf := HarmonyConfig{
		ClientOptions: opts,
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Events: EventsConfig{Enabled: true, CloudServices: []string{"Harmony Endpoint"}, PollInterval: 10 * time.Millisecond},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	waitUntil(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(warnings) >= 1
	}, "expected a warning for the canceled task")

	mu.Lock()
	defer mu.Unlock()
	// Both fields from the object should appear somewhere in the warning,
	// proving the marshal fallback ran and kept the detail.
	if !strings.Contains(warnings[0], "5042") || !strings.Contains(warnings[0], "Service not provisioned") {
		t.Fatalf("warning should carry the structured error detail via JSON marshal; got %q", warnings[0])
	}
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

func TestTransientClassification(t *testing.T) {
	if !isTransientStatus(502) || !isTransientStatus(503) || !isTransientStatus(504) {
		t.Fatalf("502/503/504 must be transient")
	}
	for _, s := range []int{200, 400, 401, 403, 404, 429, 500} {
		if isTransientStatus(s) {
			t.Fatalf("status %d must not be classified transient", s)
		}
	}
	transient := []error{
		context.DeadlineExceeded,
		fmt.Errorf(`Post "https://x/y": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`),
		fmt.Errorf("read tcp 1.2.3.4:5->6.7.8.9:443: connection reset by peer"),
		fmt.Errorf("net/http: TLS handshake timeout"),
		&net.DNSError{IsTimeout: true},
	}
	for _, e := range transient {
		if !isTransientErr(e) {
			t.Fatalf("expected transient: %v", e)
		}
	}
	notTransient := []error{
		nil,
		fmt.Errorf("x509: certificate signed by unknown authority"),
		fmt.Errorf("malformed URL"),
	}
	for _, e := range notTransient {
		if isTransientErr(e) {
			t.Fatalf("expected NOT transient: %v", e)
		}
	}
}

// withFastBackoff swaps the package retry backoff for tiny durations so the
// retry tests don't sleep for seconds, restoring it on cleanup.
func withFastBackoff(t *testing.T) {
	t.Helper()
	orig := requestRetryBackoff
	requestRetryBackoff = []time.Duration{5 * time.Millisecond}
	t.Cleanup(func() { requestRetryBackoff = orig })
}

// TestEventsRetrieveTransientRetryRecovers models the reported failure: the
// gateway 503s the retrieve a couple of times, then succeeds. The window must
// complete with records shipped, and nothing should reach OnError — the blip
// is absorbed inside one query cycle so events aren't re-shipped.
func TestEventsRetrieveTransientRetryRecovers(t *testing.T) {
	withFastBackoff(t)
	fake := &fakeGateway{
		eventPages:       [][]utils.Dict{{{"id": "e1", "time": "2026-05-15T10:00:00Z"}}},
		retrieve503Times: 2, // fail twice, succeed on the 3rd attempt
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	var errs []string
	var mu sync.Mutex
	opts := validClientOptions()
	opts.OnError = func(err error) {
		mu.Lock()
		errs = append(errs, err.Error())
		mu.Unlock()
	}

	conf := HarmonyConfig{
		ClientOptions: opts,
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Events: EventsConfig{Enabled: true, CloudServices: []string{"Harmony Browse"}, PollInterval: 10 * time.Millisecond},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	// >=3 retrieve calls proves the 2 transient 503s were retried and the
	// 3rd succeeded within a single query cycle.
	waitUntil(t, 3*time.Second, func() bool {
		return atomic.LoadInt32(&fake.retrieveCalls) >= 3
	}, "expected retrieve to be retried past the transient 503s")

	mu.Lock()
	defer mu.Unlock()
	if len(errs) != 0 {
		t.Fatalf("transient 503s must not surface as OnError; got: %v", errs)
	}
}

// TestEventsRetrieveTransientExhaustionSurfacesError asserts that a gateway
// that never recovers eventually surfaces a single OnError (the bounded
// retry gives up rather than spinning forever).
func TestEventsRetrieveTransientExhaustionSurfacesError(t *testing.T) {
	withFastBackoff(t)
	fake := &fakeGateway{
		eventPages:       [][]utils.Dict{{{"id": "e1", "time": "2026-05-15T10:00:00Z"}}},
		retrieve503Times: -1, // always 503
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	var errs []string
	var mu sync.Mutex
	opts := validClientOptions()
	opts.OnError = func(err error) {
		mu.Lock()
		errs = append(errs, err.Error())
		mu.Unlock()
	}

	conf := HarmonyConfig{
		ClientOptions: opts,
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Events: EventsConfig{Enabled: true, CloudServices: []string{"Harmony Browse"}, PollInterval: 10 * time.Millisecond},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	waitUntil(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(errs) >= 1
	}, "expected exhausted retry budget to surface an OnError")

	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(errs[0], "exhausted") {
		t.Fatalf("error should indicate retry exhaustion; got %q", errs[0])
	}
	// Each failed window makes maxRequestAttempts retrieve calls.
	if got := atomic.LoadInt32(&fake.retrieveCalls); got < int32(maxRequestAttempts) {
		t.Fatalf("expected >= %d retrieve attempts before giving up, got %d", maxRequestAttempts, got)
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
