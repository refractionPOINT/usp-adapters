package usp_okta

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock Okta System Log
// API (GET /api/v1/logs), capturing the exact messages it ships so their
// content -- timestamp and verbatim payload -- can be asserted.
//
// Mock fidelity notes:
//   - Authentication is the SSWS API token scheme: `Authorization: SSWS <token>`;
//     a bad token gets Okta's E0000011 error body with HTTP 401.
//   - The mock honours the `since`/`until` query parameters the adapter sends
//     (both RFC3339): only events whose `published` falls inside the inclusive
//     window are returned, exactly like the real System Log bounded query.
//   - The response is a bare JSON array of System Log events. The mock also
//     sets a `Link: rel="self"` header like the real API. It does NOT emulate
//     Link rel="next" pagination: the real API caps a response (default 100,
//     max 1000 events) and exposes the rest through an `after` cursor in a
//     Link header -- but the adapter performs exactly one request per poll and
//     never follows Link headers, so the mock returns the full window in a
//     single response, matching how the adapter consumes the API.

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

// --- mock Okta System Log API ---------------------------------------------------

// mockOktaSystemLog is an in-memory stand-in for the Okta System Log API.
type mockOktaSystemLog struct {
	mu     sync.Mutex
	token  string
	events []utils.Dict

	requests   int
	lastMethod string
	lastPath   string
	lastAuth   string
	lastSince  string
	lastUntil  string
}

func newMockOktaSystemLog(token string) *mockOktaSystemLog {
	return &mockOktaSystemLog{token: token}
}

// addEvent appends an event to the log, as the real API would when a new
// System Log event is recorded.
func (m *mockOktaSystemLog) addEvent(event utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
}

func (m *mockOktaSystemLog) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func (m *mockOktaSystemLog) lastRequest() (method, path, auth, since, until string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastMethod, m.lastPath, m.lastAuth, m.lastSince, m.lastUntil
}

func oktaError(w http.ResponseWriter, status int, code, summary string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"errorCode":    code,
		"errorSummary": summary,
		"errorLink":    code,
		"errorId":      "oae1111111111111111111111",
		"errorCauses":  []interface{}{},
	})
}

func (m *mockOktaSystemLog) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()

		m.mu.Lock()
		m.requests++
		m.lastMethod = r.Method
		m.lastPath = r.URL.Path
		m.lastAuth = r.Header.Get("Authorization")
		m.lastSince = q.Get("since")
		m.lastUntil = q.Get("until")
		token := m.token
		events := append([]utils.Dict(nil), m.events...)
		m.mu.Unlock()

		if r.Method != http.MethodGet {
			oktaError(w, http.StatusMethodNotAllowed, "E0000022", "The endpoint does not support the provided HTTP method")
			return
		}
		if r.URL.Path != logsURL {
			// E0000007 is "Not found: {0}" in Okta's error code reference.
			oktaError(w, http.StatusNotFound, "E0000007", fmt.Sprintf("Not found: %s", r.URL.Path))
			return
		}
		if r.Header.Get("Authorization") != "SSWS "+token {
			oktaError(w, http.StatusUnauthorized, "E0000011", "Invalid token provided")
			return
		}

		// A malformed timestamp gets Okta's documented validation error:
		// HTTP 400 with errorCode E0000001 ("Api validation failed").
		since, err := time.Parse(time.RFC3339, q.Get("since"))
		if err != nil {
			oktaError(w, http.StatusBadRequest, "E0000001", "Api validation failed: 'since': The date format in your query is not recognized. Please enter dates using ISO8601 string format.")
			return
		}
		until, err := time.Parse(time.RFC3339, q.Get("until"))
		if err != nil {
			oktaError(w, http.StatusBadRequest, "E0000001", "Api validation failed: 'until': The date format in your query is not recognized. Please enter dates using ISO8601 string format.")
			return
		}

		// A bounded query returns the events whose `published` falls inside
		// the inclusive [since, until] window. Bounded requests are
		// "guaranteed to be in order according to the published field"
		// (default sortOrder is ASCENDING), so the mock sorts the page.
		page := []utils.Dict{}
		for _, e := range events {
			published, perr := time.Parse(time.RFC3339, fmt.Sprintf("%v", e["published"]))
			if perr != nil {
				continue
			}
			if published.Before(since) || published.After(until) {
				continue
			}
			page = append(page, e)
		}
		sort.SliceStable(page, func(i, j int) bool {
			return fmt.Sprintf("%v", page[i]["published"]) < fmt.Sprintf("%v", page[j]["published"])
		})

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s?since=%s&until=%s>; rel=\"self\"", r.Host, logsURL, q.Get("since"), q.Get("until")))
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(page)
	}
}

// --- realistic fixtures ---------------------------------------------------------

// systemLogEvent returns a record shaped like a real Okta System Log event:
// uuid, published, eventType plus the actor/client/request/target/outcome/
// transaction/debugContext structures the real API emits (nested objects,
// arrays, nulls and mixed scalar types).
func systemLogEvent(uuid, eventType string, published time.Time) utils.Dict {
	return utils.Dict{
		"uuid": uuid,
		// The real API emits `published` at millisecond precision, e.g.
		// "2017-09-31T22:23:07.777Z" in the official LogEvent example.
		"published":      published.UTC().Format("2006-01-02T15:04:05.000Z"),
		"eventType":      eventType,
		"version":        "0",
		"severity":       "INFO",
		"displayMessage": "User login to Okta",
		"actor": utils.Dict{
			"id":          "00u1111111111111111",
			"type":        "User",
			"alternateId": "jdoe@example.com",
			"displayName": "Jane Doe",
			"detailEntry": nil,
		},
		"client": utils.Dict{
			"userAgent": utils.Dict{
				"rawUserAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
				"os":           "Mac OS X",
				"browser":      "CHROME",
			},
			"zone":      "null",
			"device":    "Computer",
			"id":        nil,
			"ipAddress": "198.51.100.23",
			"geographicalContext": utils.Dict{
				"city":       "Springfield",
				"state":      "Illinois",
				"country":    "United States",
				"postalCode": "62701",
				"geolocation": utils.Dict{
					"lat": 39.781,
					"lon": -89.644,
				},
			},
		},
		"outcome": utils.Dict{
			"result": "SUCCESS",
			"reason": nil,
		},
		"target": []interface{}{
			utils.Dict{
				"id":          "0oa1111111111111111",
				"type":        "AppInstance",
				"alternateId": "Example App",
				"displayName": "Example App",
				"detailEntry": nil,
			},
		},
		"transaction": utils.Dict{
			"type":   "WEB",
			"id":     "AaAaAaAaAaAaAaAaAaAa11",
			"detail": utils.Dict{},
		},
		"authenticationContext": utils.Dict{
			"authenticationStep": 0,
			"externalSessionId":  "102Aa11aAaAaAaAaAaAaAaAa1",
		},
		"securityContext": utils.Dict{
			"asNumber": 64496,
			"asOrg":    "example isp",
			"isp":      "example isp inc",
			"domain":   "example.com",
			"isProxy":  false,
		},
		"debugContext": utils.Dict{
			"debugData": utils.Dict{
				"requestId":       "req1111111111111111111",
				"requestUri":      "/api/v1/authn",
				"threatSuspected": "false",
			},
		},
		"legacyEventType": "core.user_auth.login_success",
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// startMockAdapter builds an adapter wired to the mock server through the
// injected sink, with a short poll interval so tests run fast.
func startMockAdapter(t *testing.T, serverURL, token string, sink uspSink) *OktaAdapter {
	t.Helper()
	conf := OktaConfig{
		ClientOptions: testClientOptions(t),
		ApiKey:        token,
		URL:           serverURL,
		PollInterval:  25 * time.Millisecond,
	}
	require.NoError(t, conf.Validate())
	adapter, _, err := newOktaAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	return adapter
}

// --- tests ----------------------------------------------------------------------

// TestMockSystemLogEndToEnd drives the adapter against the mock API and asserts
// the exact events shipped: every event arrives exactly once with its payload
// preserved verbatim (nested objects, arrays and nulls included), and re-polling
// the same window does not re-ship.
//
// It also pins two intentional behaviors of this adapter: events are stamped
// with ingestion time (TimestampMs is "now", not the event's `published`), and
// EventType is left empty (the platform's parsing config tags them).
func TestMockSystemLogEndToEnd(t *testing.T) {
	const token = "00aBcDeFgHiJkLmNoPqRsTuVwXyZ1111111111111"

	mock := newMockOktaSystemLog(token)
	// `published` must be >= the adapter's start time: the adapter clamps the
	// `since` bound of its poll window to its own start, and the mock honours
	// that bound the way the real API does. A small future offset keeps the
	// events inside the first polls' windows regardless of second-truncation.
	publishBase := time.Now().UTC().Add(2 * time.Second)
	want := []utils.Dict{
		systemLogEvent("11111111-1111-1111-1111-111111111111", "user.session.start", publishBase),
		systemLogEvent("22222222-2222-2222-2222-222222222222", "user.authentication.sso", publishBase),
		systemLogEvent("33333333-3333-3333-3333-333333333333", "policy.lifecycle.update", publishBase),
	}
	for _, e := range want {
		mock.addEvent(e)
	}

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	before := uint64(time.Now().UnixMilli())
	adapter := startMockAdapter(t, server.URL, token, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		10*time.Second, 20*time.Millisecond, "expected all 3 System Log events to ship")
	after := uint64(time.Now().UnixMilli())

	// Re-polling the (overlapping) window must not re-ship: the count stays at 3.
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 30*time.Millisecond, "events were re-shipped on a later poll")

	byUUID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		// The adapter does not tag an event type; LimaCharlie's platform
		// config does the parsing.
		assert.Empty(t, msg.EventType)
		// The adapter stamps ingestion time, not the event's `published`.
		assert.GreaterOrEqual(t, msg.TimestampMs, before)
		assert.LessOrEqual(t, msg.TimestampMs, after)

		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["uuid"].(string)
		require.NotEmpty(t, id)
		byUUID[id] = msg
	}
	require.Len(t, byUUID, 3)

	for _, src := range want {
		id := src["uuid"].(string)
		msg := byUUID[id]
		require.NotNil(t, msg, "event %s was not shipped", id)
		// The payload is shipped verbatim -- nested objects, arrays and nulls
		// included.
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original System Log event")
	}
}

// TestMockRequestContract verifies the request the adapter sends matches the
// System Log API contract: GET /api/v1/logs with an `Authorization: SSWS <token>`
// header and RFC3339 `since`/`until` bounds where since <= until and since is
// clamped to no earlier than the adapter's start.
func TestMockRequestContract(t *testing.T) {
	const token = "contract-test-token"

	mock := newMockOktaSystemLog(token)
	server := httptest.NewServer(mock.handler())
	defer server.Close()

	testStart := time.Now()
	sink := &captureSink{}
	adapter := startMockAdapter(t, server.URL, token, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return mock.requestCount() >= 1 },
		5*time.Second, 10*time.Millisecond, "adapter never called the API")

	method, path, auth, sinceStr, untilStr := mock.lastRequest()
	assert.Equal(t, http.MethodGet, method)
	assert.Equal(t, "/api/v1/logs", path)
	assert.Equal(t, "SSWS "+token, auth)

	since, err := time.Parse(time.RFC3339, sinceStr)
	require.NoError(t, err, "since must be RFC3339")
	until, err := time.Parse(time.RFC3339, untilStr)
	require.NoError(t, err, "until must be RFC3339")

	assert.False(t, until.Before(since), "since must be <= until")
	// On a fresh adapter the 30-minute overlap window is clamped to the
	// adapter's start time (RFC3339 truncates to the second, hence the slack).
	assert.False(t, since.Before(testStart.Add(-2*time.Second).UTC()),
		"since %v must be clamped to the adapter start (~%v), not 30 minutes back", since, testStart.UTC())
	assert.False(t, until.After(time.Now().UTC().Add(2*time.Second)), "until must be ~now")
}

// TestMockEventMidRunShipsExactlyOnce verifies an event that appears while the
// adapter is running is shipped exactly once, and already-shipped events are
// never re-sent.
func TestMockEventMidRunShipsExactlyOnce(t *testing.T) {
	const token = "tok-mid-run"

	mock := newMockOktaSystemLog(token)
	publishBase := time.Now().UTC().Add(2 * time.Second)
	mock.addEvent(systemLogEvent("aaaaaaaa-1111-1111-1111-111111111111", "user.session.start", publishBase))
	mock.addEvent(systemLogEvent("bbbbbbbb-1111-1111-1111-111111111111", "user.session.end", publishBase))

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter := startMockAdapter(t, server.URL, token, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		10*time.Second, 20*time.Millisecond, "initial events should ship")

	// A new event is recorded while the adapter is running.
	mock.addEvent(systemLogEvent("cccccccc-1111-1111-1111-111111111111", "user.account.lock",
		time.Now().UTC().Add(2*time.Second)))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		10*time.Second, 20*time.Millisecond, "the new event should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 30*time.Millisecond, "no event may ship twice")

	shippedPerUUID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerUUID[msg.JsonPayload["uuid"].(string)]++
	}
	assert.Equal(t, map[string]int{
		"aaaaaaaa-1111-1111-1111-111111111111": 1,
		"bbbbbbbb-1111-1111-1111-111111111111": 1,
		"cccccccc-1111-1111-1111-111111111111": 1,
	}, shippedPerUUID, "every event must ship exactly once")
}

// TestMockLargeBatchShipsOnce verifies a large window of events is fully
// consumed in one poll and each event ships exactly once across re-polls.
//
// Note on pagination fidelity: the real System Log API caps a response (100 by
// default, 1000 max) and exposes the remainder through Link rel="next"
// after-cursors. This adapter performs a single request per poll and does not
// follow Link headers, so the mock -- mirroring how the adapter consumes the
// API -- returns the entire window in one response. This test therefore covers
// "large dataset fully consumed" as far as the adapter's design allows.
func TestMockLargeBatchShipsOnce(t *testing.T) {
	const token = "tok-large-batch"
	const total = 250

	mock := newMockOktaSystemLog(token)
	publishBase := time.Now().UTC().Add(2 * time.Second)
	for i := 0; i < total; i++ {
		mock.addEvent(systemLogEvent(
			fmt.Sprintf("dddddddd-1111-1111-1111-%012d", i),
			"user.session.start",
			publishBase))
	}

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter := startMockAdapter(t, server.URL, token, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		15*time.Second, 25*time.Millisecond, "all events should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 30*time.Millisecond, "no duplicates across re-polls")

	uuids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		uuids[msg.JsonPayload["uuid"].(string)] = true
	}
	assert.Len(t, uuids, total, "every distinct event should be shipped once")
}

// TestMockBadTokenShipsNothing pins the adapter's behavior on an auth failure:
// the API rejects the token with 401/E0000011, the adapter reports the error
// and ships nothing -- and (unlike adapters that abort on 401) it keeps
// polling rather than stopping.
func TestMockBadTokenShipsNothing(t *testing.T) {
	mock := newMockOktaSystemLog("the-real-token")
	mock.addEvent(systemLogEvent("eeeeeeee-1111-1111-1111-111111111111", "user.session.start",
		time.Now().UTC().Add(2*time.Second)))

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	var mu sync.Mutex
	var apiErrors int
	opts := testClientOptions(t)
	opts.OnError = func(err error) {
		t.Logf("ERR: %v", err)
		mu.Lock()
		apiErrors++
		mu.Unlock()
	}

	sink := &captureSink{}
	conf := OktaConfig{
		ClientOptions: opts,
		ApiKey:        "wrong-token",
		URL:           server.URL,
		PollInterval:  25 * time.Millisecond,
	}
	require.NoError(t, conf.Validate())
	adapter, chStopped, err := newOktaAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// Let several polls fail authentication.
	require.Eventually(t, func() bool { return mock.requestCount() >= 3 },
		5*time.Second, 10*time.Millisecond, "adapter should keep polling on auth failure")

	assert.Equal(t, 0, sink.count(), "nothing may ship when authentication fails")
	mu.Lock()
	assert.GreaterOrEqual(t, apiErrors, 1, "the auth failure must be reported via OnError")
	mu.Unlock()

	select {
	case <-chStopped:
		t.Fatal("the adapter does not stop on a 401; it reports the error and retries")
	default:
		// Expected: still running.
	}
}
