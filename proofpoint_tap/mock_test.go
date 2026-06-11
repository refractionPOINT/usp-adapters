package usp_proofpoint_tap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock Proofpoint TAP
// SIEM API (GET /v2/siem/all), capturing the exact messages it ships so their
// content -- the injected eventType, ingestion timestamp and verbatim payload
// -- can be asserted.
//
// The mock reproduces the API as the adapter uses it: it authenticates the
// Basic principal/secret pair, requires format=json, parses the
// interval=<ISO8601>/<ISO8601> window parameter, filters the in-memory event
// set by that window, and answers with the documented envelope
// {"queryEndTime": ..., "messagesDelivered": [...], "messagesBlocked": [...],
// "clicksPermitted": [...], "clicksBlocked": [...]}.

// Clearly fake TAP service credentials.
const (
	testPrincipal = "11111111-1111-1111-1111-111111111111"
	testSecret    = "fake-tap-secret-1111111111111111"
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

// --- mock Proofpoint TAP SIEM API ---------------------------------------------

// TAP SIEM envelope keys; also used as the mock's event categories.
const (
	catMessagesDelivered = "messagesDelivered"
	catMessagesBlocked   = "messagesBlocked"
	catClicksPermitted   = "clicksPermitted"
	catClicksBlocked     = "clicksBlocked"
)

// tapEvent is one record in the mock's dataset: the envelope array it belongs
// to, the SIEM time used for interval filtering, and the payload served.
type tapEvent struct {
	category string
	ts       time.Time
	payload  utils.Dict
}

// mockProofpointTap is an in-memory stand-in for the TAP SIEM /v2/siem/all
// endpoint.
type mockProofpointTap struct {
	mu        sync.Mutex
	principal string
	secret    string
	events    []tapEvent

	requests          int
	authFailures      int
	lastFormat        string    // format query param of the last authenticated request
	lastIntervalStart time.Time // parsed interval start of the last authenticated request
	lastIntervalEnd   time.Time // parsed interval end of the last authenticated request
}

func newMockProofpointTap(principal, secret string) *mockProofpointTap {
	return &mockProofpointTap{principal: principal, secret: secret}
}

func (m *mockProofpointTap) addEvent(category string, ts time.Time, payload utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, tapEvent{category: category, ts: ts, payload: payload})
}

func (m *mockProofpointTap) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func (m *mockProofpointTap) authFailureCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authFailures
}

func (m *mockProofpointTap) lastRequest() (string, time.Time, time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastFormat, m.lastIntervalStart, m.lastIntervalEnd
}

func (m *mockProofpointTap) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.requests++

		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		// TAP authenticates SIEM API calls with HTTP Basic auth carrying the
		// service principal and secret.
		principal, secret, ok := r.BasicAuth()
		if !ok || principal != m.principal || secret != m.secret {
			m.authFailures++
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("Credentials did not pass authentication"))
			return
		}

		q := r.URL.Query()
		m.lastFormat = q.Get("format")
		if m.lastFormat != "json" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("The request was missing a mandatory request parameter"))
			return
		}

		// interval=<ISO8601 start>/<ISO8601 end>, at most one hour wide.
		parts := strings.SplitN(q.Get("interval"), "/", 2)
		if len(parts) != 2 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("The interval parameter is malformed"))
			return
		}
		start, err := time.Parse(time.RFC3339, parts[0])
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("The interval start is malformed"))
			return
		}
		end, err := time.Parse(time.RFC3339, parts[1])
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("The interval end is malformed"))
			return
		}
		if end.Sub(start) > time.Hour {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("The requested interval is too wide"))
			return
		}
		m.lastIntervalStart, m.lastIntervalEnd = start, end

		// Select the events inside the requested window, per category. The
		// real service always emits all four arrays for /siem/all.
		resp := map[string]interface{}{
			"queryEndTime":       end.UTC().Format(time.RFC3339),
			catMessagesDelivered: []utils.Dict{},
			catMessagesBlocked:   []utils.Dict{},
			catClicksPermitted:   []utils.Dict{},
			catClicksBlocked:     []utils.Dict{},
		}
		for _, e := range m.events {
			if e.ts.Before(start) || e.ts.After(end) {
				continue
			}
			resp[e.category] = append(resp[e.category].([]utils.Dict), e.payload)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// --- realistic record fixtures ------------------------------------------------

// realisticMessageEvent returns a record shaped like a real TAP SIEM message
// event (messagesDelivered/messagesBlocked): GUID, RFC3339 messageTime, the
// nested messageParts and threatsInfoMap arrays, scores, and sender/recipient
// addressing -- all with clearly fake values.
func realisticMessageEvent(guid string, messageTime time.Time) utils.Dict {
	ts := messageTime.UTC().Format(time.RFC3339)
	return utils.Dict{
		"GUID":                guid,
		"QID":                 "r2FNwRHF004109",
		"ccAddresses":         []interface{}{"finance@example.com"},
		"clusterId":           "example_hosted",
		"completelyRewritten": "true",
		"fromAddress":         "badguy@attacker.example.net",
		"headerFrom":          "\"A. Badguy\" <badguy@attacker.example.net>",
		"headerReplyTo":       nil,
		"impostorScore":       0,
		"malwareScore":        100,
		"messageID":           "20260611100000.11111.mail@attacker.example.net",
		"messageParts": []interface{}{
			utils.Dict{
				"contentType":   "application/pdf",
				"disposition":   "attached",
				"filename":      "invoice.pdf",
				"md5":           "11111111111111111111111111111111",
				"oContentType":  "application/pdf",
				"sandboxStatus": "threat",
				"sha256":        "1111111111111111111111111111111111111111111111111111111111111111",
			},
		},
		"messageSize":      18874,
		"messageTime":      ts,
		"modulesRun":       []interface{}{"pdr", "sandbox", "spam", "urldefense"},
		"phishScore":       46,
		"policyRoutes":     []interface{}{"default_inbound", "executives"},
		"quarantineFolder": "Attachment Defense",
		"quarantineRule":   "module.sandbox.threat",
		"recipient":        []interface{}{"jdoe@example.com"},
		"replyToAddress":   nil,
		"sender":           "badguy@attacker.example.net",
		"senderIP":         "192.0.2.255",
		"spamScore":        4,
		"subject":          "Please find a totally legitimate invoice attached",
		"threatsInfoMap": []interface{}{
			utils.Dict{
				"campaignId":     "11111111-1111-1111-1111-111111111111",
				"classification": "MALWARE",
				"threat":         "1111111111111111111111111111111111111111111111111111111111111111",
				"threatId":       "1111111111111111111111111111111111111111111111111111111111111111",
				"threatStatus":   "active",
				"threatTime":     ts,
				"threatType":     "ATTACHMENT",
				"threatUrl":      "https://threatinsight.proofpoint.com/11111111-1111-1111-1111-111111111111/threat/email/1111",
			},
		},
		"toAddresses": []interface{}{"jdoe@example.com"},
		"xmailer":     "Spambot v2.5",
	}
}

// realisticClickEvent returns a record shaped like a real TAP SIEM click event
// (clicksPermitted/clicksBlocked): the click-specific id field the adapter
// dedupes on, RFC3339 clickTime, the clicked URL and threat metadata.
func realisticClickEvent(id string, clickTime time.Time) utils.Dict {
	ts := clickTime.UTC().Format(time.RFC3339)
	return utils.Dict{
		"GUID":           "click-guid-" + id,
		"campaignId":     "11111111-1111-1111-1111-111111111111",
		"classification": "MALWARE",
		"clickIP":        "192.0.2.10",
		"clickTime":      ts,
		"id":             id,
		"messageID":      "20260611100000.22222.mail@attacker.example.net",
		"recipient":      "jdoe@example.com",
		"sender":         "badguy@attacker.example.net",
		"senderIP":       "192.0.2.255",
		"threatID":       "2222222222222222222222222222222222222222222222222222222222222222",
		"threatStatus":   "active",
		"threatTime":     ts,
		"threatURL":      "https://threatinsight.proofpoint.com/11111111-1111-1111-1111-111111111111/threat/email/2222",
		"url":            "http://badsite.example.net/malicious",
		"userAgent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
	}
}

// withEventType returns a copy of payload with the eventType key the adapter
// injects before shipping.
func withEventType(payload utils.Dict, eventType string) utils.Dict {
	out := utils.Dict{}
	for k, v := range payload {
		out[k] = v
	}
	out["eventType"] = eventType
	return out
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// startMockAdapter wires an adapter to the mock server with a fast poll
// interval and an injected capture sink.
func startMockAdapter(t *testing.T, serverURL, principal, secret string, sink uspSink) (*ProofpointTapAdapter, chan struct{}) {
	t.Helper()
	conf := ProofpointTapConfig{
		ClientOptions: testClientOptions(t),
		Principal:     principal,
		Secret:        secret,
		URL:           serverURL,
		PollInterval:  25 * time.Millisecond,
	}
	adapter, chStopped, err := newProofpointTapAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	return adapter, chStopped
}

// --- tests ----------------------------------------------------------------------

// Note on fixture timestamps: the adapter's first poll queries the interval
// [adapterStart-2min, now], and every later poll [queryEndTime-2min, now], so
// fixtures stamped "just now" are inside every window of a fast-polling test.

// TestMockAllCategoriesEndToEnd drives the adapter against the mock API and
// asserts the exact events shipped from each of the four SIEM categories:
// payload preserved verbatim (nested threatsInfoMap/messageParts included)
// apart from the injected eventType key, no DataMessage.EventType tagging,
// ingestion-time TimestampMs, the format/interval/Basic-auth request contract,
// and that re-polling overlapping windows does not re-ship.
func TestMockAllCategoriesEndToEnd(t *testing.T) {
	mock := newMockProofpointTap(testPrincipal, testSecret)
	now := time.Now()

	delivered1 := realisticMessageEvent("11111111-1111-1111-1111-aaaaaaaaaaaa", now.Add(-30*time.Second))
	delivered2 := realisticMessageEvent("11111111-1111-1111-1111-bbbbbbbbbbbb", now.Add(-20*time.Second))
	blockedMsg := realisticMessageEvent("11111111-1111-1111-1111-cccccccccccc", now.Add(-25*time.Second))
	permittedClick := realisticClickEvent("11111111-1111-1111-1111-dddddddddddd", now.Add(-15*time.Second))
	blockedClick := realisticClickEvent("11111111-1111-1111-1111-eeeeeeeeeeee", now.Add(-10*time.Second))

	mock.addEvent(catMessagesDelivered, now.Add(-30*time.Second), delivered1)
	mock.addEvent(catMessagesDelivered, now.Add(-20*time.Second), delivered2)
	mock.addEvent(catMessagesBlocked, now.Add(-25*time.Second), blockedMsg)
	mock.addEvent(catClicksPermitted, now.Add(-15*time.Second), permittedClick)
	mock.addEvent(catClicksBlocked, now.Add(-10*time.Second), blockedClick)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	testStartMs := uint64(time.Now().UnixMilli())
	adapter, _ := startMockAdapter(t, server.URL, testPrincipal, testSecret, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 5 },
		8*time.Second, 20*time.Millisecond, "expected all 5 SIEM events to ship")

	// Consecutive polls re-query overlapping windows; the dedupe maps must
	// prevent any re-shipping: the count stays at 5.
	require.Never(t, func() bool { return sink.count() != 5 },
		300*time.Millisecond, 30*time.Millisecond, "records were re-shipped on a later poll")

	doneMs := uint64(time.Now().UnixMilli())

	byKey := map[string]*protocol.DataMessage{}
	perType := map[string]int{}
	for _, msg := range sink.snapshot() {
		// The adapter tags the event category inside the payload (eventType
		// key), not on DataMessage.EventType.
		assert.Empty(t, msg.EventType)
		// The adapter stamps ingestion time, not the record's messageTime.
		assert.GreaterOrEqual(t, msg.TimestampMs, testStartMs)
		assert.LessOrEqual(t, msg.TimestampMs, doneMs)
		require.NotNil(t, msg.JsonPayload)

		et, _ := msg.JsonPayload["eventType"].(string)
		require.NotEmpty(t, et, "every shipped payload must carry an injected eventType")
		perType[et]++

		// Messages are keyed by GUID, clicks by id.
		if strings.HasPrefix(et, "message_") {
			byKey[msg.JsonPayload["GUID"].(string)] = msg
		} else {
			byKey[msg.JsonPayload["id"].(string)] = msg
		}
	}
	assert.Equal(t, map[string]int{
		"message_delivered": 2,
		"message_blocked":   1,
		"click_permitted":   1,
		"click_blocked":     1,
	}, perType, "each SIEM category must be tagged with its eventType")

	// The payload is shipped verbatim -- nested messageParts/threatsInfoMap
	// included -- with only the eventType key added.
	for _, tc := range []struct {
		key       string
		src       utils.Dict
		eventType string
	}{
		{delivered1["GUID"].(string), delivered1, "message_delivered"},
		{delivered2["GUID"].(string), delivered2, "message_delivered"},
		{blockedMsg["GUID"].(string), blockedMsg, "message_blocked"},
		{permittedClick["id"].(string), permittedClick, "click_permitted"},
		{blockedClick["id"].(string), blockedClick, "click_blocked"},
	} {
		msg := byKey[tc.key]
		require.NotNil(t, msg, "record %s was not shipped", tc.key)
		assert.JSONEq(t, mustJSON(t, withEventType(tc.src, tc.eventType)), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original TAP record plus eventType")
	}

	// Request contract: format=json and an interval of two RFC3339 timestamps,
	// starting 2 minutes before the poll cursor and ending around now.
	format, start, end := mock.lastRequest()
	assert.Equal(t, "json", format)
	assert.False(t, end.Before(start), "interval end must not precede its start")
	assert.LessOrEqual(t, end.Sub(start), time.Hour, "the adapter must never request more than an hour")
	assert.WithinDuration(t, time.Now(), end, 5*time.Second, "interval end should be the poll time")
	assert.WithinDuration(t, time.Now().Add(-2*time.Minute), start, 5*time.Second,
		"interval start should trail the cursor by the 2 minute overlap")
}

// TestMockNewEventMidRunShipsOnce verifies an event appearing while the
// adapter is running is shipped exactly once, and already-shipped events are
// never re-sent even though every poll re-queries an overlapping window.
func TestMockNewEventMidRunShipsOnce(t *testing.T) {
	mock := newMockProofpointTap(testPrincipal, testSecret)
	now := time.Now()
	first := realisticMessageEvent("11111111-1111-1111-1111-000000000001", now.Add(-30*time.Second))
	second := realisticClickEvent("11111111-1111-1111-1111-000000000002", now.Add(-20*time.Second))
	mock.addEvent(catMessagesDelivered, now.Add(-30*time.Second), first)
	mock.addEvent(catClicksBlocked, now.Add(-20*time.Second), second)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	adapter, _ := startMockAdapter(t, server.URL, testPrincipal, testSecret, sink)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		8*time.Second, 20*time.Millisecond)

	// A new blocked message lands mid-run.
	third := realisticMessageEvent("11111111-1111-1111-1111-000000000003", time.Now())
	mock.addEvent(catMessagesBlocked, time.Now(), third)

	require.Eventually(t, func() bool { return sink.count() == 3 },
		8*time.Second, 20*time.Millisecond, "the new event should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 30*time.Millisecond)

	shippedPerKey := map[string]int{}
	for _, msg := range sink.snapshot() {
		et, _ := msg.JsonPayload["eventType"].(string)
		if strings.HasPrefix(et, "message_") {
			shippedPerKey[msg.JsonPayload["GUID"].(string)]++
		} else {
			shippedPerKey[msg.JsonPayload["id"].(string)]++
		}
	}
	assert.Equal(t, map[string]int{
		"11111111-1111-1111-1111-000000000001": 1,
		"11111111-1111-1111-1111-000000000002": 1,
		"11111111-1111-1111-1111-000000000003": 1,
	}, shippedPerKey, "every event must ship exactly once")
}

// TestMockRejectsBadCredentialsShipsNothing verifies bad credentials ship
// nothing. The adapter's behavior on a 401 is to report the error and keep
// polling with an unchanged cursor (it does not stop), so the test also pins
// that: repeated rejected requests, no shutdown.
func TestMockRejectsBadCredentialsShipsNothing(t *testing.T) {
	mock := newMockProofpointTap(testPrincipal, testSecret)
	mock.addEvent(catMessagesDelivered, time.Now(),
		realisticMessageEvent("11111111-1111-1111-1111-404040404040", time.Now()))

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	var apiErrors atomic.Int32
	opts := testClientOptions(t)
	prevOnError := opts.OnError
	opts.OnError = func(err error) {
		apiErrors.Add(1)
		prevOnError(err)
	}

	sink := &captureSink{}
	conf := ProofpointTapConfig{
		ClientOptions: opts,
		Principal:     testPrincipal,
		Secret:        "wrong-secret-00000000000000000000",
		URL:           server.URL,
		PollInterval:  25 * time.Millisecond,
	}
	adapter, chStopped, err := newProofpointTapAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// The adapter keeps polling (and keeps being rejected) rather than stopping.
	require.Eventually(t, func() bool { return mock.authFailureCount() >= 2 },
		5*time.Second, 20*time.Millisecond, "the adapter should keep retrying on 401")
	require.Eventually(t, func() bool { return apiErrors.Load() >= 1 },
		5*time.Second, 20*time.Millisecond, "the 401 must be surfaced via OnError")

	select {
	case <-chStopped:
		t.Fatal("the adapter stopped on a 401; its documented behavior is to keep polling")
	default:
	}

	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
	assert.GreaterOrEqual(t, mock.requestCount(), 2)
}

// TestMockHourCapOnBacklog verifies that when the cursor is far in the past
// the adapter requests at most a one-hour interval (59 minutes plus the
// 2-minute overlap trim) and advances its cursor to the returned queryEndTime,
// rather than asking the API for an over-wide window it would reject.
func TestMockHourCapOnBacklog(t *testing.T) {
	mock := newMockProofpointTap(testPrincipal, testSecret)

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	sink := &captureSink{}
	conf := ProofpointTapConfig{
		ClientOptions: testClientOptions(t),
		Principal:     testPrincipal,
		Secret:        testSecret,
		URL:           server.URL,
		PollInterval:  time.Hour, // the background poller idles for the whole test
	}
	adapter, _, err := newProofpointTapAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	since := time.Now().Add(-3 * time.Hour)
	items, newSince, reqErr := adapter.makeOneRequest(since)
	require.NoError(t, reqErr)
	assert.Empty(t, items, "no events exist in the requested backlog window")

	_, start, end := mock.lastRequest()
	assert.Equal(t, 59*time.Minute, end.Sub(start),
		"a backlogged cursor must be queried as a capped 59 minute interval")
	assert.WithinDuration(t, since.Add(-2*time.Minute), start, 2*time.Second,
		"the capped interval still starts at cursor-2min")

	// The cursor advances to the response's queryEndTime (the interval end),
	// so the adapter walks the backlog forward an hour at a time.
	assert.WithinDuration(t, end, newSince, 2*time.Second)
}
