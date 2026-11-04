package usp_symantec

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
)

func testClientOptions() uspclient.ClientOptions {
	return uspclient.ClientOptions{
		Identity: uspclient.Identity{
			Oid:             "c1ffedc0-ffee-4a1e-b1a5-abc123def456",
			InstallationKey: "e9a3bcdf-efa2-47ae-b6df-579a02f3a54d",
		},
		Platform:      "json",
		SensorSeedKey: "test-symantec",
	}
}

func TestValidateDefaults(t *testing.T) {
	c := SymantecConfig{
		ClientOptions:    testClientOptions(),
		OAuthCredentials: "abc123",
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate(): %v", err)
	}
	if c.URL != defaultURL {
		t.Errorf("url = %q, expected %q", c.URL, defaultURL)
	}
	if c.EventTypes != feedEventSearch {
		t.Errorf("event_types = %q, expected %q", c.EventTypes, feedEventSearch)
	}
	if c.FeatureName != "All" {
		t.Errorf("feature_name = %q, expected All", c.FeatureName)
	}
	if c.PageLimit != defaultPageLimit {
		t.Errorf("page_limit = %d, expected %d", c.PageLimit, defaultPageLimit)
	}
	if c.PollIntervalSec != defaultPollIntervalSec {
		t.Errorf("poll_interval_sec = %d, expected %d", c.PollIntervalSec, defaultPollIntervalSec)
	}
}

func TestValidateRejectsBadConfigs(t *testing.T) {
	for name, c := range map[string]SymantecConfig{
		"no credentials": {
			ClientOptions: testClientOptions(),
		},
		"client id without secret": {
			ClientOptions: testClientOptions(),
			ClientID:      "id-only",
		},
		"unknown event type": {
			ClientOptions:    testClientOptions(),
			OAuthCredentials: "abc123",
			EventTypes:       "devices",
		},
		"page limit too large": {
			ClientOptions:    testClientOptions(),
			OAuthCredentials: "abc123",
			PageLimit:        5000,
		},
		"negative page limit": {
			ClientOptions:    testClientOptions(),
			OAuthCredentials: "abc123",
			PageLimit:        -1,
		},
		"bad start time": {
			ClientOptions:    testClientOptions(),
			OAuthCredentials: "abc123",
			StartTime:        "2026-09-22 00:00:00",
		},
	} {
		if err := c.Validate(); err == nil {
			t.Errorf("%s: expected an error, got none", name)
		}
	}
}

func TestBuildFeeds(t *testing.T) {
	c := SymantecConfig{
		ClientOptions:    testClientOptions(),
		OAuthCredentials: "abc123",
		// Repeated entries should collapse to one collector.
		EventTypes: "incidents, event_search , incidents",
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate(): %v", err)
	}
	feeds, err := c.buildFeeds()
	if err != nil {
		t.Fatalf("buildFeeds(): %v", err)
	}
	if len(feeds) != 2 {
		t.Fatalf("got %d feeds, expected 2", len(feeds))
	}
	expected := map[string]string{
		feedIncidents:   incidentsPath,
		feedEventSearch: eventSearchPath,
	}
	for _, f := range feeds {
		if expected[f.name] != f.path {
			t.Errorf("feed %s has path %s, expected %s", f.name, f.path, expected[f.name])
		}
		delete(expected, f.name)
	}
	for name := range expected {
		t.Errorf("missing feed: %s", name)
	}
}

func TestAuthHeader(t *testing.T) {
	// The console blob is already encoded and only needs the scheme.
	c := SymantecConfig{OAuthCredentials: "c2VjcmV0"}
	if got := c.authHeader(); got != "Basic c2VjcmV0" {
		t.Errorf("authHeader() = %q, expected %q", got, "Basic c2VjcmV0")
	}

	// A value that already carries the scheme is passed through untouched.
	c = SymantecConfig{OAuthCredentials: "Basic c2VjcmV0"}
	if got := c.authHeader(); got != "Basic c2VjcmV0" {
		t.Errorf("authHeader() = %q, expected it unchanged", got)
	}

	// An ID and secret are combined and encoded here.
	c = SymantecConfig{ClientID: "id", ClientSecret: "secret"}
	expected := "Basic " + base64.StdEncoding.EncodeToString([]byte("id:secret"))
	if got := c.authHeader(); got != expected {
		t.Errorf("authHeader() = %q, expected %q", got, expected)
	}
}

func TestRequestBodyPerFeed(t *testing.T) {
	a := &SymantecAdapter{conf: SymantecConfig{
		FeatureName:   "Malware Protection",
		Query:         "type_id:8001",
		IncludeEvents: true,
		PageLimit:     250,
	}}
	since := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	until := time.Date(2026, 9, 22, 11, 0, 0, 0, time.UTC)

	// Event Search requires product and feature_name on every call.
	body := a.requestBody(feed{name: feedEventSearch}, since, until, 0)
	if body["product"] != productSAEP {
		t.Errorf("product = %v, expected %s", body["product"], productSAEP)
	}
	if body["feature_name"] != "Malware Protection" {
		t.Errorf("feature_name = %v", body["feature_name"])
	}
	if body["query"] != "type_id:8001" {
		t.Errorf("query = %v", body["query"])
	}
	if body["start_date"] != "2026-09-22T10:00:00.000Z" {
		t.Errorf("start_date = %v, expected Symantec's ISO 8601 form", body["start_date"])
	}
	if body["limit"] != 250 {
		t.Errorf("limit = %v, expected 250", body["limit"])
	}
	// The first page must not carry a next index.
	if _, ok := body["next"]; ok {
		t.Error("next should be absent on the first page")
	}

	body = a.requestBody(feed{name: feedEventSearch}, since, until, 100)
	if body["next"] != 100 {
		t.Errorf("next = %v, expected 100", body["next"])
	}

	body = a.requestBody(feed{name: feedIncidents}, since, until, 0)
	if body["include_events"] != true {
		t.Errorf("include_events = %v, expected true", body["include_events"])
	}
	if _, ok := body["product"]; ok {
		t.Error("incidents must not carry the event-search product field")
	}

	body = a.requestBody(feed{name: feedIncidentEvents}, since, until, 0)
	if body["incident_associate"] != true {
		t.Errorf("incident_associate = %v, expected true", body["incident_associate"])
	}
}

func TestParseResponseEnvelopes(t *testing.T) {
	// Each endpoint names its array differently.
	for name, body := range map[string]string{
		"events":    `{"events":[{"uuid":"a"},{"uuid":"b"}],"next":200,"total":2}`,
		"incidents": `{"incidents":[{"incident_uid":"a"},{"incident_uid":"b"}],"next":200}`,
		"result":    `{"result":[{"id":"a"},{"id":"b"}],"next":"200"}`,
	} {
		items, next, err := parseResponse([]byte(body))
		if err != nil {
			t.Fatalf("%s: parseResponse(): %v", name, err)
		}
		if len(items) != 2 {
			t.Errorf("%s: got %d items, expected 2", name, len(items))
		}
		if next != 200 {
			t.Errorf("%s: next = %d, expected 200 (string or number)", name, next)
		}
	}

	items, next, err := parseResponse([]byte(`{"events":[],"total":0}`))
	if err != nil {
		t.Fatalf("parseResponse(): %v", err)
	}
	if len(items) != 0 || next != 0 {
		t.Errorf("empty response gave %d items, next %d", len(items), next)
	}

	if _, _, err := parseResponse([]byte(`not json`)); err == nil {
		t.Error("expected an error on malformed json")
	}
}

func TestRecordIDAndTime(t *testing.T) {
	if got := recordID(utils.Dict{"uuid": "abc"}); got != "abc" {
		t.Errorf("recordID() = %q", got)
	}
	if got := recordID(utils.Dict{"incident_uid": "xyz"}); got != "xyz" {
		t.Errorf("recordID() = %q", got)
	}
	// A JSON number must not render in exponent form.
	if got := recordID(utils.Dict{"id": float64(1700000000)}); got != "1700000000" {
		t.Errorf("recordID() = %q", got)
	}
	if got := recordID(utils.Dict{"nothing": 1}); got != "" {
		t.Errorf("recordID() = %q, expected empty", got)
	}

	fallback := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	expected := time.Date(2026, 9, 22, 10, 30, 0, 0, time.UTC)

	if got := recordTime(utils.Dict{"time": "2026-09-22T10:30:00.000Z"}, fallback); !got.Equal(expected) {
		t.Errorf("recordTime(ses format) = %v, expected %v", got, expected)
	}
	if got := recordTime(utils.Dict{"created": "2026-09-22T10:30:00Z"}, fallback); !got.Equal(expected) {
		t.Errorf("recordTime(rfc3339) = %v, expected %v", got, expected)
	}
	if got := recordTime(utils.Dict{"time": float64(expected.UnixMilli())}, fallback); !got.Equal(expected) {
		t.Errorf("recordTime(epoch ms) = %v, expected %v", got, expected)
	}
	if got := recordTime(utils.Dict{"unrecognised": "x"}, fallback); !got.Equal(fallback) {
		t.Errorf("recordTime(unknown) = %v, expected the fallback", got)
	}
}

// The token is fetched once and reused across requests until it nears expiry.
func TestTokenIsReused(t *testing.T) {
	var mu sync.Mutex
	tokenCalls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == tokenPath {
			mu.Lock()
			tokenCalls++
			mu.Unlock()
			fmt.Fprint(w, `{"access_token":"tok-1","token_type":"Bearer","expires_in":3600}`)
			return
		}
		fmt.Fprint(w, `{"events":[]}`)
	}))
	defer srv.Close()

	a := newTestAdapter(t, srv.URL)
	defer a.dedupe.Close()

	for i := 0; i < 3; i++ {
		if _, err := a.getToken(); err != nil {
			t.Fatalf("getToken(): %v", err)
		}
	}
	if tokenCalls != 1 {
		t.Errorf("token endpoint called %d times, expected 1", tokenCalls)
	}

	// A token near expiry is renewed rather than reused.
	a.mToken.Lock()
	a.tokenExpiry = time.Now().Add(tokenRenewMargin / 2)
	a.mToken.Unlock()
	if _, err := a.getToken(); err != nil {
		t.Fatalf("getToken(): %v", err)
	}
	if tokenCalls != 2 {
		t.Errorf("token endpoint called %d times, expected a renewal", tokenCalls)
	}
}

// A 401 mid-collection means the token died early; the adapter re-authenticates
// once and carries on rather than dropping the cycle.
func TestReauthenticatesOn401(t *testing.T) {
	var mu sync.Mutex
	tokenCalls := 0
	dataCalls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == tokenPath {
			tokenCalls++
			fmt.Fprintf(w, `{"access_token":"tok-%d","expires_in":3600}`, tokenCalls)
			return
		}
		dataCalls++
		if dataCalls == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"error":"token expired"}`)
			return
		}
		fmt.Fprint(w, `{"events":[{"uuid":"a","time":"2026-09-22T10:30:00.000Z"}]}`)
	}))
	defer srv.Close()

	a := newTestAdapter(t, srv.URL)
	defer a.dedupe.Close()

	_, n := a.fetchWindow(feed{name: feedEventSearch, path: eventSearchPath}, time.Now().UTC().Add(-time.Hour))

	if n != 1 {
		t.Errorf("shipped %d records, expected 1 after re-authenticating", n)
	}
	if tokenCalls != 2 {
		t.Errorf("token endpoint called %d times, expected 2", tokenCalls)
	}
}

// Paging follows the next index until the API stops advancing it.
func TestPaginationFollowsNextIndex(t *testing.T) {
	var mu sync.Mutex
	var seenNext []interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == tokenPath {
			fmt.Fprint(w, `{"access_token":"tok","expires_in":3600}`)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		json.Unmarshal(body, &req)

		mu.Lock()
		seenNext = append(seenNext, req["next"])
		page := len(seenNext)
		mu.Unlock()

		if page < 3 {
			fmt.Fprintf(w, `{"events":[{"uuid":"p%d","time":"2026-09-22T10:0%d:00.000Z"}],"next":%d}`, page, page, page*100)
			return
		}
		// Final page: no next index.
		fmt.Fprintf(w, `{"events":[{"uuid":"p%d","time":"2026-09-22T10:0%d:00.000Z"}]}`, page, page)
	}))
	defer srv.Close()

	a := newTestAdapter(t, srv.URL)
	defer a.dedupe.Close()

	_, n := a.fetchWindow(feed{name: feedEventSearch, path: eventSearchPath}, time.Now().UTC().Add(-time.Hour))

	if n != 3 {
		t.Errorf("shipped %d records, expected 3 across three pages", n)
	}
	if len(seenNext) != 3 {
		t.Fatalf("made %d requests, expected 3", len(seenNext))
	}
	if seenNext[0] != nil {
		t.Errorf("first request sent next=%v, expected it absent", seenNext[0])
	}
	if seenNext[1] != float64(100) || seenNext[2] != float64(200) {
		t.Errorf("next indexes were %v, expected 100 then 200", seenNext[1:])
	}
}

// Symantec rejects an incident start_date older than 30 days, so the adapter
// has to clamp it rather than let the request fail.
func TestIncidentStartIsClampedTo30Days(t *testing.T) {
	var mu sync.Mutex
	var startDates []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == tokenPath {
			fmt.Fprint(w, `{"access_token":"tok","expires_in":3600}`)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		json.Unmarshal(body, &req)
		mu.Lock()
		startDates = append(startDates, fmt.Sprintf("%v", req["start_date"]))
		mu.Unlock()
		fmt.Fprint(w, `{"incidents":[]}`)
	}))
	defer srv.Close()

	a := newTestAdapter(t, srv.URL)
	defer a.dedupe.Close()

	ancient := time.Now().UTC().Add(-90 * 24 * time.Hour)
	a.fetchWindow(feed{name: feedIncidents, path: incidentsPath}, ancient)

	if len(startDates) != 1 {
		t.Fatalf("made %d requests, expected 1", len(startDates))
	}
	sent, err := time.Parse(sesTimeFormat, startDates[0])
	if err != nil {
		t.Fatalf("start_date %q did not parse: %v", startDates[0], err)
	}
	if sent.Before(time.Now().UTC().Add(-maxIncidentLookback - time.Minute)) {
		t.Errorf("start_date %v was not clamped to the 30 day limit", sent)
	}

	// Event Search has no such limit, so its window is left alone.
	startDates = nil
	a.fetchWindow(feed{name: feedEventSearch, path: eventSearchPath}, ancient)
	sent, err = time.Parse(sesTimeFormat, startDates[0])
	if err != nil {
		t.Fatalf("start_date %q did not parse: %v", startDates[0], err)
	}
	if sent.After(time.Now().UTC().Add(-60 * 24 * time.Hour)) {
		t.Errorf("event_search start_date %v should not have been clamped", sent)
	}
}

// Repeats within the overlap window must not be shipped twice.
func TestDuplicatesAreDropped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == tokenPath {
			fmt.Fprint(w, `{"access_token":"tok","expires_in":3600}`)
			return
		}
		fmt.Fprint(w, `{"events":[{"uuid":"same","time":"2026-09-22T10:30:00.000Z"}]}`)
	}))
	defer srv.Close()

	a := newTestAdapter(t, srv.URL)
	defer a.dedupe.Close()

	f := feed{name: feedEventSearch, path: eventSearchPath}
	since := time.Now().UTC().Add(-time.Hour)

	if _, n := a.fetchWindow(f, since); n != 1 {
		t.Errorf("first cycle shipped %d, expected 1", n)
	}
	if _, n := a.fetchWindow(f, since); n != 0 {
		t.Errorf("second cycle shipped %d, expected the repeat to be dropped", n)
	}
}

func newTestAdapter(t *testing.T, baseURL string) *SymantecAdapter {
	t.Helper()
	dd, err := utils.NewLocalDeduper(time.Minute, time.Hour)
	if err != nil {
		t.Fatalf("NewLocalDeduper(): %v", err)
	}

	opts := testClientOptions()
	// tool.go installs these before building any adapter; adapters call them
	// unguarded, so the tests have to supply them too.
	opts.DebugLog = func(string) {}
	opts.OnWarning = func(string) {}
	opts.OnError = func(error) {}
	opts.TestSinkMode = true

	uc, err := uspclient.NewClient(context.Background(), opts)
	if err != nil {
		t.Fatalf("uspclient.NewClient(): %v", err)
	}

	return &SymantecAdapter{
		conf: SymantecConfig{
			ClientOptions:    opts,
			OAuthCredentials: "abc123",
			URL:              baseURL,
			FeatureName:      "All",
			PageLimit:        defaultPageLimit,
			PollIntervalSec:  defaultPollIntervalSec,
		},
		ctx:        context.Background(),
		doStop:     utils.NewEvent(),
		dedupe:     dd,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		uspClient:  uc,
	}
}
