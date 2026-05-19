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

func TestEmailDedupKey(t *testing.T) {
	t.Run("no entityId returns empty key so caller skips it", func(t *testing.T) {
		if got := emailDedupKey(utils.Dict{}); got != "" {
			t.Fatalf("expected empty key, got %q", got)
		}
	})

	t.Run("entityUpdated drives the key", func(t *testing.T) {
		a := utils.Dict{"entityInfo": utils.Dict{"entityId": "abc", "entityUpdated": "2026-05-12T10:00:00Z"}}
		b := utils.Dict{"entityInfo": utils.Dict{"entityId": "abc", "entityUpdated": "2026-05-12T10:00:00Z"}}
		c := utils.Dict{"entityInfo": utils.Dict{"entityId": "abc", "entityUpdated": "2026-05-12T11:00:00Z"}}
		if emailDedupKey(a) != emailDedupKey(b) {
			t.Fatalf("same entityUpdated should produce same key")
		}
		if emailDedupKey(a) == emailDedupKey(c) {
			t.Fatalf("different entityUpdated should produce different keys (state change)")
		}
	})

	t.Run("falls back to entityCreated when entityUpdated is absent", func(t *testing.T) {
		noUpdated := utils.Dict{"entityInfo": utils.Dict{"entityId": "abc", "entityCreated": "2026-05-12T09:00:00Z"}}
		withUpdated := utils.Dict{"entityInfo": utils.Dict{"entityId": "abc", "entityUpdated": "2026-05-12T10:00:00Z"}}
		got := emailDedupKey(noUpdated)
		if got == "" || !strings.Contains(got, "2026-05-12T09:00:00Z") {
			t.Fatalf("expected key to fall back to entityCreated, got %q", got)
		}
		if emailDedupKey(noUpdated) == emailDedupKey(withUpdated) {
			t.Fatalf("entityCreated fallback must not collide with an entityUpdated key")
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
			Emails: EmailsConfig{Enabled: true, Saas: []string{"slack"}},
		}
		if err := c.Validate(); err == nil {
			t.Fatalf("expected error for unsupported saas")
		}
	})

	t.Run("emails defaults fill in", func(t *testing.T) {
		c := HarmonyConfig{
			ClientOptions: validClientOptions(),
			ClientID:      "x", AccessKey: "y",
			Emails: EmailsConfig{Enabled: true},
		}
		if err := c.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.Emails.PollInterval != defaultEmailsPollInterval {
			t.Fatalf("expected default poll interval, got %s", c.Emails.PollInterval)
		}
		if c.Emails.Lookback != defaultEmailsLookback {
			t.Fatalf("expected default lookback, got %s", c.Emails.Lookback)
		}
		if len(c.Emails.Saas) != len(defaultEmailsSaas) {
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

	// auth503Times controls how many /auth/external calls return a
	// transient 503 before succeeding — models the same gateway slowness
	// hitting the auth endpoint specifically. Negative = always 503.
	auth503Times int

	// Records returned by the HEC search; can be swapped at runtime to simulate transitions.
	hecRecords []utils.Dict

	// Number of entityExtendedFilter entries on the most recent HEC search
	// request, and whether a search request has been observed. The emails
	// feed must send zero (unfiltered).
	lastExtFilterLen int
	sawSearchRequest bool

	// Full entityExtendedFilter list and entityFilter object from the most
	// recent HEC search request, so a test can assert the exact restore
	// query shape (predicates, and that no endDate/saasEntity is sent).
	lastExtFilter    []interface{}
	lastEntityFilter utils.Dict

	// hecPageSize > 0 makes serveHECSearch model HEC's stable-handle scroll:
	// records are returned in pages of this size, the scrollId is the same
	// on every page, and an empty page marks the end. 0 keeps the legacy
	// single-page behavior. hecScrollOffset is the server-side cursor.
	hecPageSize     int
	hecScrollOffset int
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
	transient := f.auth503Times != 0
	if f.auth503Times > 0 {
		f.auth503Times--
	}
	f.mu.Unlock()
	if transient {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"success":false,"message":"temporarily unavailable"}`))
		return
	}

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

	extLen := 0
	reqScroll := ""
	var extFilter []interface{}
	var entFilter utils.Dict
	var reqBody utils.Dict
	if json.NewDecoder(r.Body).Decode(&reqBody) == nil {
		rd, _ := reqBody.GetDict("requestData")
		if ef, ok := rd["entityExtendedFilter"].([]interface{}); ok {
			extLen = len(ef)
			extFilter = ef
		}
		entFilter, _ = rd.GetDict("entityFilter")
		reqScroll, _ = rd.GetString("scrollId")
	}

	f.mu.Lock()
	f.lastExtFilterLen = extLen
	f.lastExtFilter = extFilter
	f.lastEntityFilter = entFilter
	f.sawSearchRequest = true
	total := append([]utils.Dict{}, f.hecRecords...)

	if f.hecPageSize <= 0 {
		// Legacy: whole result set in a single page.
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, utils.Dict{
			"responseEnvelope": utils.Dict{"recordsNumber": len(total), "scrollId": ""},
			"responseData":     dictListToInterface(total),
		})
		return
	}

	// HEC stable-handle scroll: a fresh request (empty scrollId) starts a
	// new session; the handle returned is the same on every page; the end
	// is signalled by an empty page.
	if reqScroll == "" {
		f.hecScrollOffset = 0
	}
	off := f.hecScrollOffset
	if off > len(total) {
		off = len(total)
	}
	end := off + f.hecPageSize
	if end > len(total) {
		end = len(total)
	}
	page := total[off:end]
	f.hecScrollOffset = end
	f.mu.Unlock()

	writeJSON(w, http.StatusOK, utils.Dict{
		"responseEnvelope": utils.Dict{"recordsNumber": len(total), "scrollId": "hec-scroll"},
		"responseData":     dictListToInterface(page),
	})
}

// lastSearchExtFilter reports the entityExtendedFilter length on the most
// recent HEC search request and whether one was seen at all.
func (f *fakeGateway) lastSearchExtFilter() (length int, seen bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastExtFilterLen, f.sawSearchRequest
}

// lastSearchQuery returns copies of the entityExtendedFilter list and
// entityFilter object from the most recent HEC search request.
func (f *fakeGateway) lastSearchQuery() ([]interface{}, utils.Dict) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]interface{}{}, f.lastExtFilter...), f.lastEntityFilter
}

// extFilterHas reports whether the extended-filter list contains a predicate
// with the given saasAttrName and saasAttrOp.
func extFilterHas(ext []interface{}, name, op string) bool {
	for _, e := range ext {
		m, ok := e.(map[string]interface{})
		if !ok {
			continue
		}
		if m["saasAttrName"] == name && m["saasAttrOp"] == op {
			return true
		}
	}
	return false
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
// We use it to observe which email entities the adapter ships.
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

// TestEmailsDedupAndStateChange verifies the feed semantics:
//   - On first poll, an email entity is admitted to dedup (and therefore shipped).
//   - On subsequent polls with no change, the same entity is suppressed.
//   - When the gateway bumps entityUpdated to reflect a state change, a new
//     dedup key admits the entity again, capturing the change.
func TestEmailsDedupAndStateChange(t *testing.T) {
	fake := &fakeGateway{}
	fake.hecRecords = []utils.Dict{
		{
			"entityInfo":    utils.Dict{"entityId": "e1", "entityUpdated": "2026-05-12T10:00:00Z"},
			"entityPayload": utils.Dict{"direction": "incoming"},
		},
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	dedup := newRecordingDeduper()

	conf := HarmonyConfig{
		ClientOptions: validClientOptions(),
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Emails: EmailsConfig{
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
	}, "expected the first poll to admit the entity")

	firstKey := dedup.admittedKeys()[0]
	if !strings.Contains(firstKey, "e1") || !strings.Contains(firstKey, "2026-05-12T10:00:00Z") {
		t.Fatalf("unexpected dedup key: %q", firstKey)
	}

	// Let a few more polls happen — count must stay at 1 since the record hasn't changed.
	time.Sleep(200 * time.Millisecond)
	if got := len(dedup.admittedKeys()); got != 1 {
		t.Fatalf("expected dedup to suppress repeats; admitted=%d keys=%v", got, dedup.admittedKeys())
	}
	if got := atomic.LoadInt32(&fake.searchCalls); got < 2 {
		t.Fatalf("expected multiple search polls; got %d", got)
	}

	// Now advance the entity's state with a fresh entityUpdated.
	fake.mu.Lock()
	fake.hecRecords[0] = utils.Dict{
		"entityInfo":    utils.Dict{"entityId": "e1", "entityUpdated": "2026-05-12T11:00:00Z"},
		"entityPayload": utils.Dict{"direction": "incoming", "isQuarantined": true},
	}
	fake.mu.Unlock()

	waitUntil(t, 2*time.Second, func() bool {
		return len(dedup.admittedKeys()) >= 2
	}, "expected the state change to produce a second admission")

	keys := dedup.admittedKeys()
	changeKey := keys[1]
	if !strings.Contains(changeKey, "2026-05-12T11:00:00Z") {
		t.Fatalf("expected changed key to include new entityUpdated, got %q", changeKey)
	}
}

// TestEmailsUnfilteredSingleQueryPerPoll asserts the emails source issues
// exactly one query per poll and sends no server-side filter, so the full
// email-entity feed comes through and triage happens downstream.
func TestEmailsUnfilteredSingleQueryPerPoll(t *testing.T) {
	fake := &fakeGateway{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	conf := HarmonyConfig{
		ClientOptions: validClientOptions(),
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Emails: EmailsConfig{
			Enabled:      true,
			Saas:         []string{"office365_emails"},
			PollInterval: 1 * time.Hour, // single poll
			Lookback:     1 * time.Hour,
			Deduper:      newRecordingDeduper(),
		},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	// Allow the initial poll to complete.
	time.Sleep(300 * time.Millisecond)
	adapter.Close()

	if got := atomic.LoadInt32(&fake.searchCalls); got != 1 {
		t.Fatalf("expected exactly 1 search call per poll, got %d", got)
	}
	extLen, seen := fake.lastSearchExtFilter()
	if !seen {
		t.Fatalf("expected the emails source to issue a search request")
	}
	if extLen != 0 {
		t.Fatalf("emails feed must send no extended filter; got %d filter entries", extLen)
	}
}

// TestEmailsScrollDrainsAllPages is the regression test for the HEC
// stable-handle scroll bug: the endpoint returns the *same* scrollId on
// every page, so terminating on an unchanged scrollId stops after page 1
// and silently drops the rest of the window. The adapter must keep
// re-sending the handle until it gets an empty page. With 7 records paged
// 2 at a time that is 4 non-empty pages + 1 empty terminator = 5 search
// calls, and all 7 entities must be shipped.
func TestEmailsScrollDrainsAllPages(t *testing.T) {
	fake := &fakeGateway{hecPageSize: 2}
	for i := 1; i <= 7; i++ {
		fake.hecRecords = append(fake.hecRecords, utils.Dict{
			"entityInfo": utils.Dict{
				"entityId":      fmt.Sprintf("e%d", i),
				"entityUpdated": fmt.Sprintf("2026-05-15T10:00:%02dZ", i),
			},
			"entityPayload": utils.Dict{"direction": "incoming"},
		})
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	dedup := newRecordingDeduper()
	conf := HarmonyConfig{
		ClientOptions: validClientOptions(),
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Emails: EmailsConfig{
			Enabled:      true,
			Saas:         []string{"office365_emails"},
			PollInterval: 1 * time.Hour, // single poll
			Lookback:     1 * time.Hour,
			Deduper:      dedup,
		},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	// All 7 must be shipped — the buggy termination would yield only the
	// first page (2).
	waitUntil(t, 3*time.Second, func() bool {
		return len(dedup.admittedKeys()) >= 7
	}, "expected every page to be drained (all 7 entities shipped)")

	if got := len(dedup.admittedKeys()); got != 7 {
		t.Fatalf("expected exactly 7 entities shipped, got %d: %v", got, dedup.admittedKeys())
	}
	// 4 pages of records (2,2,2,1) + 1 empty terminator page.
	waitUntil(t, 2*time.Second, func() bool {
		return atomic.LoadInt32(&fake.searchCalls) >= 5
	}, "expected the scroll to continue past page 1 until an empty page")
	if got := atomic.LoadInt32(&fake.searchCalls); got != 5 {
		t.Fatalf("expected exactly 5 search calls (4 pages + empty terminator), got %d", got)
	}
}

func TestEntitiesValidate(t *testing.T) {
	base := func(q EntityQuery) *HarmonyConfig {
		return &HarmonyConfig{
			ClientOptions: validClientOptions(),
			ClientID:      "x", AccessKey: "y",
			Entities: EntitiesConfig{Enabled: true, Queries: []EntityQuery{q}},
		}
	}

	t.Run("window-mode defaults", func(t *testing.T) {
		c := base(EntityQuery{Name: "q1"})
		if err := c.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		q := c.Entities.Queries[0]
		if q.PollInterval != defaultEntitiesPollInterval {
			t.Fatalf("poll interval default; got %s", q.PollInterval)
		}
		if q.Lookback != defaultEntitiesWindowLookback {
			t.Fatalf("window-mode lookback default; got %s", q.Lookback)
		}
		if len(q.Saas) != len(defaultEmailsSaas) {
			t.Fatalf("saas default")
		}
	})

	t.Run("cursor-mode defaults differ", func(t *testing.T) {
		c := base(EntityQuery{Name: "q1", CursorField: "entityPayload.restoreRequestTime"})
		if err := c.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.Entities.Queries[0].Lookback != defaultEntitiesCursorLookback {
			t.Fatalf("cursor-mode lookback default (15d); got %s", c.Entities.Queries[0].Lookback)
		}
		if c.Entities.Queries[0].InitialLookback != defaultEntitiesInitialLookback {
			t.Fatalf("cursor-mode initial lookback default; got %s", c.Entities.Queries[0].InitialLookback)
		}
	})

	t.Run("enabled with no queries is rejected", func(t *testing.T) {
		c := HarmonyConfig{
			ClientOptions: validClientOptions(),
			ClientID:      "x", AccessKey: "y",
			Entities: EntitiesConfig{Enabled: true},
		}
		if err := c.Validate(); err == nil {
			t.Fatalf("expected error when entities is enabled but has no queries")
		}
	})

	t.Run("missing name is rejected", func(t *testing.T) {
		if err := base(EntityQuery{}).Validate(); err == nil {
			t.Fatalf("expected error for missing name")
		}
	})

	t.Run("duplicate names are rejected", func(t *testing.T) {
		c := HarmonyConfig{
			ClientOptions: validClientOptions(),
			ClientID:      "x", AccessKey: "y",
			Entities: EntitiesConfig{Enabled: true, Queries: []EntityQuery{
				{Name: "dup"}, {Name: "dup"},
			}},
		}
		if err := c.Validate(); err == nil {
			t.Fatalf("expected error for duplicate query names")
		}
	})

	t.Run("unsupported saas is rejected", func(t *testing.T) {
		if err := base(EntityQuery{Name: "q1", Saas: []string{"slack"}}).Validate(); err == nil {
			t.Fatalf("expected error for unsupported saas")
		}
	})

	t.Run("unknown filter op is rejected", func(t *testing.T) {
		err := base(EntityQuery{Name: "q1", Filter: []EntityPredicate{
			{Attr: "entityPayload.subject", Op: "matches", Value: "x"},
		}}).Validate()
		if err == nil {
			t.Fatalf("expected error for unknown op")
		}
	})

	t.Run("filter missing attr is rejected", func(t *testing.T) {
		err := base(EntityQuery{Name: "q1", Filter: []EntityPredicate{
			{Op: "is", Value: "x"},
		}}).Validate()
		if err == nil {
			t.Fatalf("expected error for missing attr")
		}
	})

	t.Run("cursor_field must be entityInfo or entityPayload prefixed", func(t *testing.T) {
		if err := base(EntityQuery{Name: "q1", CursorField: "restoreRequestTime"}).Validate(); err == nil {
			t.Fatalf("expected error for bare cursor_field")
		}
		if err := base(EntityQuery{Name: "q1", CursorField: "foo.bar"}).Validate(); err == nil {
			t.Fatalf("expected error for unknown section in cursor_field")
		}
		if err := base(EntityQuery{Name: "q1", CursorField: "entityPayload.restoreRequestTime"}).Validate(); err != nil {
			t.Fatalf("entityPayload.* cursor_field should be accepted; got %v", err)
		}
		if err := base(EntityQuery{Name: "q1", CursorField: "entityInfo.entityUpdated"}).Validate(); err != nil {
			t.Fatalf("entityInfo.* cursor_field should be accepted; got %v", err)
		}
	})

	t.Run("source still required when entities is disabled", func(t *testing.T) {
		c := HarmonyConfig{
			ClientOptions: validClientOptions(),
			ClientID:      "x", AccessKey: "y",
			Entities: EntitiesConfig{Enabled: false},
		}
		if err := c.Validate(); err == nil {
			t.Fatalf("expected error when no source is enabled")
		}
	})
}

// TestEntitiesCursorModeRestorePreset expresses the original reported bug
// ("quarantined-email restore requests never come in") as the generic
// source's cursor-mode preset, and pins the behavior the fix relies on:
//
//   - The query is server-side filtered: the configured predicate is sent
//     as-is, AND the adapter auto-injects a "{CursorField} greaterThan
//     {cursor}" predicate so cursor mode actually advances.
//   - entityFilter sends only saas + a wide startDate — no endDate, no
//     saasEntity. That is what lets an old quarantined email's restore
//     request surface at all (the Emails feed's received-time window
//     never returns it).
//   - A "split" master record is skipped, so the same event isn't emitted
//     twice.
//   - Dedup suppresses unchanged repeats, and a lifecycle advance
//     (entityUpdated bump) re-emits — under the query's name namespace.
//
// All records are synthetic — no real tenant data.
func TestEntitiesCursorModeRestorePreset(t *testing.T) {
	fake := &fakeGateway{}
	fake.hecRecords = []utils.Dict{
		{
			"entityInfo":    utils.Dict{"entityId": "rr-1", "entityCreated": "2026-01-01T09:00:00Z", "entityUpdated": "2026-01-02T10:05:00Z"},
			"entityPayload": utils.Dict{"isRestoreRequested": "true", "restoreRequestTime": "2026-01-02T10:04:30Z", "subject": "synthetic restore request"},
		},
		{
			"entityInfo":    utils.Dict{"entityId": "rr-split-master", "entityUpdated": "2026-01-02T10:06:00Z"},
			"entityPayload": utils.Dict{"isRestoreRequested": "true", "restoreRequestTime": "2026-01-02T10:05:30Z", "emailSplit": "split", "subject": "synthetic split master"},
		},
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	dedup := newRecordingDeduper()
	conf := HarmonyConfig{
		ClientOptions: validClientOptions(),
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Entities: EntitiesConfig{
			Enabled: true,
			Queries: []EntityQuery{{
				Name: "restore_requests",
				Saas: []string{"office365_emails"},
				Filter: []EntityPredicate{
					{Attr: "entityPayload.isRestoreRequested", Op: "is", Value: "true"},
				},
				CursorField:     "entityPayload.restoreRequestTime",
				Lookback:        15 * 24 * time.Hour,
				InitialLookback: 1 * time.Hour,
				PollInterval:    20 * time.Millisecond,
				Deduper:         dedup,
			}},
		},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	waitUntil(t, 2*time.Second, func() bool {
		return len(dedup.admittedKeys()) >= 1
	}, "expected the restore request to be admitted/shipped")

	time.Sleep(150 * time.Millisecond)
	keys := dedup.admittedKeys()
	if len(keys) != 1 {
		t.Fatalf("expected exactly 1 admitted key (split skipped, repeats deduped); got %d: %v", len(keys), keys)
	}
	if !strings.HasPrefix(keys[0], "restore_requests:") {
		t.Fatalf("admitted key should be namespaced by query name; got %q", keys[0])
	}
	if !strings.Contains(keys[0], "rr-1") {
		t.Fatalf("admitted key should reference the real entity; got %q", keys[0])
	}
	for _, k := range keys {
		if strings.Contains(k, "rr-split-master") {
			t.Fatalf("split master must never be admitted; got %q", k)
		}
	}

	ext, entFilter := fake.lastSearchQuery()
	// Configured predicate passes through verbatim.
	if !extFilterHas(ext, "entityPayload.isRestoreRequested", "is") {
		t.Fatalf("expected configured isRestoreRequested predicate; got %v", ext)
	}
	// Cursor predicate is injected automatically — this is what makes
	// cursor mode actually advance and not re-ship.
	if !extFilterHas(ext, "entityPayload.restoreRequestTime", "greaterThan") {
		t.Fatalf("expected auto-injected cursor predicate; got %v", ext)
	}
	if entFilter["saas"] != "office365_emails" {
		t.Fatalf("entityFilter.saas should be set; got %v", entFilter)
	}
	if _, ok := entFilter["startDate"]; !ok {
		t.Fatalf("entityFilter.startDate should be set; got %v", entFilter)
	}
	if _, ok := entFilter["endDate"]; ok {
		t.Fatalf("cursor mode must NOT send endDate (it would re-impose the received-time window); got %v", entFilter)
	}
	if _, ok := entFilter["saasEntity"]; ok {
		t.Fatalf("cursor mode must NOT send saasEntity; got %v", entFilter)
	}

	// Lifecycle advance: gateway bumps entityUpdated when the request is
	// declined/restored. A new dedup key admits it again.
	fake.mu.Lock()
	fake.hecRecords[0] = utils.Dict{
		"entityInfo":    utils.Dict{"entityId": "rr-1", "entityCreated": "2026-01-01T09:00:00Z", "entityUpdated": "2026-01-02T11:00:00Z"},
		"entityPayload": utils.Dict{"isRestoreRequested": "true", "isRestoreDeclined": "true", "restoreRequestTime": "2026-01-02T10:04:30Z", "subject": "synthetic restore request"},
	}
	fake.mu.Unlock()

	waitUntil(t, 2*time.Second, func() bool {
		return len(dedup.admittedKeys()) >= 2
	}, "expected the lifecycle advance (entityUpdated bump) to re-emit")
	keys = dedup.admittedKeys()
	if !strings.Contains(keys[1], "2026-01-02T11:00:00Z") {
		t.Fatalf("re-emitted key should reflect the new entityUpdated; got %q", keys[1])
	}
}

// TestEntitiesWindowMode covers the "spam/content/recipient filter on a
// received-time window" scenario: no cursor_field, just a server-side
// predicate. entityFilter must carry startDate+endDate+saasEntity (like
// the Emails feed), the configured predicate must pass through, and no
// cursor predicate must be injected.
func TestEntitiesWindowMode(t *testing.T) {
	fake := &fakeGateway{}
	fake.hecRecords = []utils.Dict{
		{
			"entityInfo":    utils.Dict{"entityId": "ent-1", "entityUpdated": "2026-01-02T10:00:00Z"},
			"entityPayload": utils.Dict{"subject": "synthetic match"},
		},
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	dedup := newRecordingDeduper()
	conf := HarmonyConfig{
		ClientOptions: validClientOptions(),
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Entities: EntitiesConfig{
			Enabled: true,
			Queries: []EntityQuery{{
				Name: "subject_watch",
				Saas: []string{"office365_emails"},
				Filter: []EntityPredicate{
					{Attr: "entityPayload.subject", Op: "contains", Value: "synthetic"},
				},
				PollInterval: 20 * time.Millisecond,
				Lookback:     1 * time.Hour,
				Deduper:      dedup,
			}},
		},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	waitUntil(t, 2*time.Second, func() bool {
		return len(dedup.admittedKeys()) >= 1
	}, "expected the matching entity to be admitted")

	// Repeats must dedup, and no cursor advancement happens in window mode.
	time.Sleep(150 * time.Millisecond)
	if got := len(dedup.admittedKeys()); got != 1 {
		t.Fatalf("expected dedup to suppress repeats in window mode; admitted=%d", got)
	}

	ext, entFilter := fake.lastSearchQuery()
	if !extFilterHas(ext, "entityPayload.subject", "contains") {
		t.Fatalf("expected configured subject predicate; got %v", ext)
	}
	// Critically: NO cursor predicate is injected in window mode.
	for _, e := range ext {
		m, _ := e.(map[string]interface{})
		if m["saasAttrOp"] == "greaterThan" {
			t.Fatalf("window mode must not inject a cursor predicate; got %v", ext)
		}
	}
	if _, ok := entFilter["startDate"]; !ok {
		t.Fatalf("window mode must send startDate; got %v", entFilter)
	}
	if _, ok := entFilter["endDate"]; !ok {
		t.Fatalf("window mode must send endDate; got %v", entFilter)
	}
	if entFilter["saasEntity"] != "office365_emails_email" {
		t.Fatalf("window mode must scope saasEntity from the saas map; got %v", entFilter["saasEntity"])
	}
}

// TestEntitiesScrollDrainsAllPages mirrors TestEmailsScrollDrainsAllPages
// for the entities source: the stable-handle scroll must keep re-sending
// the handle until an empty terminator page, not stop on an unchanged
// scrollId. With 5 records paged 2 at a time that is 3 non-empty pages +
// 1 empty terminator = 4 search calls, and all 5 entities shipped.
func TestEntitiesScrollDrainsAllPages(t *testing.T) {
	fake := &fakeGateway{hecPageSize: 2}
	for i := 1; i <= 5; i++ {
		fake.hecRecords = append(fake.hecRecords, utils.Dict{
			"entityInfo": utils.Dict{
				"entityId":      fmt.Sprintf("ent-%d", i),
				"entityUpdated": fmt.Sprintf("2026-01-02T10:00:%02dZ", i),
			},
			"entityPayload": utils.Dict{"subject": "synthetic"},
		})
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	dedup := newRecordingDeduper()
	conf := HarmonyConfig{
		ClientOptions: validClientOptions(),
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Entities: EntitiesConfig{
			Enabled: true,
			Queries: []EntityQuery{{
				Name:         "drain",
				Saas:         []string{"office365_emails"},
				PollInterval: 1 * time.Hour, // single poll
				Lookback:     1 * time.Hour,
				Deduper:      dedup,
			}},
		},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	waitUntil(t, 3*time.Second, func() bool {
		return len(dedup.admittedKeys()) >= 5
	}, "expected the scroll to drain every page (all 5 entities shipped)")

	if got := len(dedup.admittedKeys()); got != 5 {
		t.Fatalf("expected exactly 5 entities shipped, got %d: %v", got, dedup.admittedKeys())
	}
	waitUntil(t, 2*time.Second, func() bool {
		return atomic.LoadInt32(&fake.searchCalls) >= 4
	}, "expected 4 search calls (3 pages + empty terminator)")
}

// TestEntitiesCursorAdvancesWithSubsecondPrecision pins two related
// correctness properties of cursor mode:
//
//   - Across polls, the injected cursor predicate value moves forward to
//     reflect the newest CursorField value shipped (not the initial
//     InitialLookback floor). A regression that failed to advance the
//     cursor would re-ship everything every poll.
//   - The advanced cursor value preserves the gateway's sub-second
//     precision. Truncating to whole seconds (using RFC3339 instead of
//     RFC3339Nano) would skip later events that fall in the same whole
//     second as the cursor — a silent drop that is exactly the bug class
//     this source exists to avoid.
func TestEntitiesCursorAdvancesWithSubsecondPrecision(t *testing.T) {
	fake := &fakeGateway{}
	// Record's restoreRequestTime is recent (30 min ago) and carries
	// sub-second precision so we can prove that precision survives.
	recordTime := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Microsecond)
	recordTimeStr := recordTime.Format(time.RFC3339Nano)
	if !strings.Contains(recordTimeStr, ".") {
		// Truncate landed exactly on a whole second — pick a sub-second
		// offset so the precision assertion remains meaningful.
		recordTime = recordTime.Add(123 * time.Microsecond)
		recordTimeStr = recordTime.Format(time.RFC3339Nano)
	}
	fake.hecRecords = []utils.Dict{{
		"entityInfo":    utils.Dict{"entityId": "ent-1", "entityCreated": "2026-01-01T00:00:00Z", "entityUpdated": recordTimeStr},
		"entityPayload": utils.Dict{"isRestoreRequested": "true", "restoreRequestTime": recordTimeStr, "subject": "synthetic"},
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	dedup := newRecordingDeduper()
	initialCursorFloor := time.Now().UTC().Add(-1 * time.Hour)
	conf := HarmonyConfig{
		ClientOptions: validClientOptions(),
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Entities: EntitiesConfig{
			Enabled: true,
			Queries: []EntityQuery{{
				Name: "restore_requests",
				Saas: []string{"office365_emails"},
				Filter: []EntityPredicate{
					{Attr: "entityPayload.isRestoreRequested", Op: "is", Value: "true"},
				},
				CursorField:     "entityPayload.restoreRequestTime",
				Lookback:        15 * 24 * time.Hour,
				InitialLookback: 1 * time.Hour,
				PollInterval:    20 * time.Millisecond,
				Deduper:         dedup,
			}},
		},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	// Wait for the record to be shipped (proves poll 1 ran end-to-end).
	waitUntil(t, 2*time.Second, func() bool {
		return len(dedup.admittedKeys()) >= 1
	}, "expected the record to be admitted on the first poll")

	// Wait for at least one *subsequent* poll request after admission, so
	// lastSearchQuery reflects a request built with the advanced cursor.
	callsAtAdmission := atomic.LoadInt32(&fake.searchCalls)
	waitUntil(t, 2*time.Second, func() bool {
		return atomic.LoadInt32(&fake.searchCalls) > callsAtAdmission
	}, "expected a follow-up poll after admission")

	ext, _ := fake.lastSearchQuery()
	var cursorValue string
	for _, e := range ext {
		m, _ := e.(map[string]interface{})
		if m["saasAttrName"] == "entityPayload.restoreRequestTime" && m["saasAttrOp"] == "greaterThan" {
			cursorValue, _ = m["saasAttrValue"].(string)
		}
	}
	if cursorValue == "" {
		t.Fatalf("cursor predicate not found in follow-up poll request; got ext=%v", ext)
	}
	parsed, perr := time.Parse(time.RFC3339Nano, cursorValue)
	if perr != nil {
		t.Fatalf("cursor predicate value should parse as RFC3339Nano; got %q (%v)", cursorValue, perr)
	}
	// Cursor must have advanced strictly past the initial floor — otherwise
	// the worker would re-ship every poll.
	if !parsed.After(initialCursorFloor) {
		t.Fatalf("cursor did not advance: predicate value %s is not after initial floor %s", parsed, initialCursorFloor)
	}
	// And it must equal the record's restoreRequestTime: confirms both the
	// "newest value seen" semantics and that sub-second precision survives.
	if !parsed.Equal(recordTime) {
		t.Fatalf("cursor predicate value should equal the record's restoreRequestTime (sub-second preserved); want %s, got %s", recordTime.Format(time.RFC3339Nano), parsed.Format(time.RFC3339Nano))
	}
}

// TestEntitiesMultipleQueriesIndependent is the central guarantee of the
// generic design: two queries run side-by-side, each maintains its own
// dedup state, and each entity is shipped per query with the query's own
// _lc_harmony_query annotation (here observed via the namespaced dedup
// key). Cross-query interference would defeat the "add a new scenario by
// adding a config entry" promise.
func TestEntitiesMultipleQueriesIndependent(t *testing.T) {
	fake := &fakeGateway{}
	fake.hecRecords = []utils.Dict{{
		"entityInfo":    utils.Dict{"entityId": "ent-1", "entityUpdated": "2026-01-02T10:00:00Z"},
		"entityPayload": utils.Dict{"subject": "synthetic"},
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	d1, d2 := newRecordingDeduper(), newRecordingDeduper()
	conf := HarmonyConfig{
		ClientOptions: validClientOptions(),
		ClientID:      "c", AccessKey: "s", URL: srv.URL,
		Entities: EntitiesConfig{
			Enabled: true,
			Queries: []EntityQuery{
				{Name: "q1", Saas: []string{"office365_emails"}, PollInterval: 20 * time.Millisecond, Lookback: 1 * time.Hour, Deduper: d1},
				{Name: "q2", Saas: []string{"office365_emails"}, PollInterval: 20 * time.Millisecond, Lookback: 1 * time.Hour, Deduper: d2},
			},
		},
	}
	adapter, _, err := NewHarmonyAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewHarmonyAdapter: %v", err)
	}
	defer adapter.Close()

	waitUntil(t, 2*time.Second, func() bool {
		return len(d1.admittedKeys()) >= 1 && len(d2.admittedKeys()) >= 1
	}, "expected both queries to independently admit the entity")

	// A few extra polls must not cause cross-query re-admission: each
	// query's deduper handles its own keys, unaffected by the other.
	time.Sleep(150 * time.Millisecond)
	if got := len(d1.admittedKeys()); got != 1 {
		t.Fatalf("q1: expected exactly 1 admitted key, got %d: %v", got, d1.admittedKeys())
	}
	if got := len(d2.admittedKeys()); got != 1 {
		t.Fatalf("q2: expected exactly 1 admitted key, got %d: %v", got, d2.admittedKeys())
	}
	if !strings.HasPrefix(d1.admittedKeys()[0], "q1:") {
		t.Fatalf("q1 key should be namespaced; got %q", d1.admittedKeys()[0])
	}
	if !strings.HasPrefix(d2.admittedKeys()[0], "q2:") {
		t.Fatalf("q2 key should be namespaced; got %q", d2.admittedKeys()[0])
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

// TestAuthTransientRetryRecovers pins the gap-fix: a transient blip on
// /auth/external (the call every data request depends on) must be absorbed
// by the same bounded retry, not surface as OnError + a re-run window.
func TestAuthTransientRetryRecovers(t *testing.T) {
	withFastBackoff(t)
	fake := &fakeGateway{
		eventPages:   [][]utils.Dict{{{"id": "e1", "time": "2026-05-15T10:00:00Z"}}},
		auth503Times: 2, // auth fails twice, succeeds on the 3rd attempt
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

	// A successful retrieve can only happen after auth recovered and a
	// token was issued — proves the auth blip was retried, not surfaced.
	waitUntil(t, 3*time.Second, func() bool {
		return atomic.LoadInt32(&fake.retrieveCalls) >= 1
	}, "expected auth to recover after transient 503s and data to flow")

	mu.Lock()
	defer mu.Unlock()
	if len(errs) != 0 {
		t.Fatalf("transient auth 503s must not surface as OnError; got: %v", errs)
	}
	if got := atomic.LoadInt32(&fake.authCalls); got < 3 {
		t.Fatalf("expected auth to be retried past the 2 transient 503s, got %d calls", got)
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
