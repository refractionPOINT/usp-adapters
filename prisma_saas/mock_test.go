package usp_prisma_saas

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
)

// This file reproduces the SaaS Security API closely enough to run the adapter
// end to end without credentials. The shapes come from the vendor docs
// (token + log_events: 200 with one event, 204 after the long-poll timeout,
// 401 invalid_token on expiry) and from the Cortex XSOAR SaaS Security Event
// Collector (log_events_bulk: GET ?size=N answering {"events":[...]} with at
// most 100 events, 204 when the queue is empty; reads pop the queue).

const (
	mockClientID     = "acme"
	mockClientSecret = "acme-test-secret-value"
)

// --- in-memory USP sink -----------------------------------------------------

// captureSink is an in-memory uspSink recording every shipped message. Ship can
// be made to report a full buffer, by count (fullFor) or while blocked is set.
type captureSink struct {
	mu       sync.Mutex
	messages []*protocol.DataMessage
	ships    int // Ship calls, including rejected ones

	fullFor int          // the next N Ship calls return ErrorBufferFull
	blocked atomic.Bool  // while true, Ship returns ErrorBufferFull
	failErr atomic.Value // when set (error), Ship returns it

	drained    atomic.Bool
	closed     atomic.Bool
	drainBegan atomic.Bool // set by BeginDrain/Drain: Ship then returns ErrorClosed
}

func (s *captureSink) Ship(m *protocol.DataMessage, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ships++
	// Like *uspclient.Client: once draining begins, nothing more is accepted.
	if s.drainBegan.Load() {
		return uspclient.ErrorClosed
	}
	if v := s.failErr.Load(); v != nil {
		return v.(error)
	}
	if s.blocked.Load() {
		// Mimic Ship waiting for room before giving up.
		s.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		s.mu.Lock()
		if s.drainBegan.Load() {
			return uspclient.ErrorClosed
		}
		return uspclient.ErrorBufferFull
	}
	if s.fullFor > 0 {
		s.fullFor--
		return uspclient.ErrorBufferFull
	}
	s.messages = append(s.messages, m)
	return nil
}

// BeginDrain mirrors (*uspclient.Client).BeginDrain: it stops admission and
// wakes blocked Ship calls.
func (s *captureSink) BeginDrain() { s.drainBegan.Store(true) }

func (s *captureSink) Drain(time.Duration) error {
	s.drainBegan.Store(true)
	s.drained.Store(true)
	return nil
}

func (s *captureSink) Close() ([]*protocol.DataMessage, error) {
	s.closed.Store(true)
	return nil, nil
}

func (s *captureSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.messages)
}

func (s *captureSink) shipCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ships
}

func (s *captureSink) snapshot() []*protocol.DataMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*protocol.DataMessage, len(s.messages))
	copy(out, s.messages)
	return out
}

// --- mock SaaS Security API -------------------------------------------------

type mockPAN struct {
	t *testing.T

	mu     sync.Mutex
	queue  []utils.Dict // pending events, consumed on read
	popped int          // events removed from the queue (i.e. read)

	// Behaviour switches.
	bulkMissing   bool          // log_events_bulk answers 404
	tokenLifetime int           // expires_in in token responses (0 = omit)
	longPoll      time.Duration // how long log_events / empty bulk hold before 204
	holdForever   bool          // events requests never answer (until the client gives up)
	alwaysStatus  int           // events requests always answer this status
	tokenStatus   int           // token requests answer this status (e.g. 400 invalid_client)
	faults        []fault       // consumed in order by events requests
	garbageNext   int           // next N events 200s carry an unparseable body
	gate          chan struct{} // when set, each events request waits for a token (or a closed gate)

	// Token state.
	nextToken int
	valid     map[string]bool

	// Counters.
	tokenRequests  int
	bulkRequests   int
	singleRequests int
	unauthorized   int
	inflight       int
	maxInflight    int
	bulkSizes      []string
	requestTimes   []time.Time // events requests
}

type fault struct {
	status     int
	retryAfter string
}

func newMockPAN(t *testing.T) *mockPAN {
	return &mockPAN{t: t, tokenLifetime: 7199, valid: map[string]bool{}}
}

func (m *mockPAN) start() *httptest.Server {
	srv := httptest.NewServer(m.handler())
	m.t.Cleanup(srv.Close)
	return srv
}

func (m *mockPAN) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth/token", m.handleToken)
	mux.HandleFunc("/api/v1/log_events_bulk", func(w http.ResponseWriter, r *http.Request) { m.handleEvents(w, r, true) })
	mux.HandleFunc("/api/v1/log_events", func(w http.ResponseWriter, r *http.Request) { m.handleEvents(w, r, false) })
	return mux
}

func (m *mockPAN) enqueue(events ...utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queue = append(m.queue, events...)
}

func (m *mockPAN) setFaults(f ...fault) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.faults = f
}

// revokeTokens makes every issued token answer 401 invalid_token from now on.
func (m *mockPAN) revokeTokens() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.valid = map[string]bool{}
}

func (m *mockPAN) stats() (tokens, bulk, single, unauth, popped int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenRequests, m.bulkRequests, m.singleRequests, m.unauthorized, m.popped
}

func (m *mockPAN) singleCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.singleRequests
}

func (m *mockPAN) inflightNow() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.inflight
}

func (m *mockPAN) poppedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.popped
}

func (m *mockPAN) handleToken(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.tokenRequests++
	status := m.tokenStatus
	m.mu.Unlock()

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Basic auth carries base64(client_id:client_secret), per the docs.
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte(mockClientID+":"+mockClientSecret))
	if r.Header.Get("Authorization") != want {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized","resolution":"Please submit valid credentials.","error_description":"Invalid credentials"}`))
		return
	}
	if !assert.NoError(m.t, r.ParseForm()) {
		return
	}
	assert.Equal(m.t, "client_credentials", r.PostForm.Get("grant_type"))
	assert.Equal(m.t, "api_access", r.PostForm.Get("scope"))
	assert.Equal(m.t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))

	if status != 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":"invalid_client","error_description":"Bad client credentials"}`))
		return
	}

	m.mu.Lock()
	m.nextToken++
	tok := fmt.Sprintf("tok-%d-eyJhbGciOiJIUzI1NiJ9", m.nextToken)
	m.valid[tok] = true
	lifetime := m.tokenLifetime
	m.mu.Unlock()

	resp := map[string]interface{}{
		"access_token": tok,
		"token_type":   "bearer",
		"scope":        "api_access",
		"tenant":       "test tenant",
		"jti":          "d66abfad-8c3c-40d1-8c1f-91cc0e1931f1",
	}
	if lifetime > 0 {
		resp["expires_in"] = lifetime
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (m *mockPAN) handleEvents(w http.ResponseWriter, r *http.Request, bulk bool) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	m.mu.Lock()
	if bulk {
		m.bulkRequests++
		m.bulkSizes = append(m.bulkSizes, r.URL.Query().Get("size"))
	} else {
		m.singleRequests++
	}
	m.requestTimes = append(m.requestTimes, time.Now())
	m.inflight++
	if m.inflight > m.maxInflight {
		m.maxInflight = m.inflight
	}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.inflight--
		m.mu.Unlock()
	}()

	// The bulk endpoint is unknown to some tenants.
	m.mu.Lock()
	missing := bulk && m.bulkMissing
	m.mu.Unlock()
	if missing {
		http.NotFound(w, r)
		return
	}

	// Auth is checked before anything is consumed from the queue.
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	m.mu.Lock()
	ok := tok != "" && m.valid[tok]
	if !ok {
		m.unauthorized++
	}
	always := m.alwaysStatus
	m.mu.Unlock()
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_token","resolution":"Please retrieve a new token.","error_description":"Authentication token was invalid."}`))
		return
	}
	if always != 0 {
		w.WriteHeader(always)
		return
	}

	m.mu.Lock()
	if len(m.faults) > 0 {
		f := m.faults[0]
		m.faults = m.faults[1:]
		m.mu.Unlock()
		if f.retryAfter != "" {
			w.Header().Set("Retry-After", f.retryAfter)
		}
		w.WriteHeader(f.status)
		return
	}
	hold := m.holdForever
	gate := m.gate
	m.mu.Unlock()
	if hold {
		<-r.Context().Done()
		return
	}
	if gate != nil {
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
	}

	// Consume. An empty single-event queue is a long-poll: wait for data until
	// the server-side timeout, then 204.
	deadline := time.Now().Add(m.longPoll)
	for {
		m.mu.Lock()
		if len(m.queue) > 0 {
			n := 1
			if bulk {
				n = 100
				if sz, err := strconv.Atoi(r.URL.Query().Get("size")); err == nil && sz < n {
					n = sz
				}
			}
			if n > len(m.queue) {
				n = len(m.queue)
			}
			batch := m.queue[:n]
			m.queue = m.queue[n:]
			m.popped += n
			garbage := m.garbageNext > 0
			if garbage {
				m.garbageNext--
			}
			m.mu.Unlock()

			w.Header().Set("Content-Type", "application/json;charset=UTF-8")
			if garbage {
				_, _ = w.Write([]byte(`<html>not json</html>`))
				return
			}
			var body interface{} = batch[0]
			if bulk {
				body = map[string]interface{}{"events": batch}
			}
			assert.NoError(m.t, json.NewEncoder(w).Encode(body))
			return
		}
		m.mu.Unlock()

		// Queue empty: 204 once the (possibly zero) long-poll window is over.
		if !time.Now().Before(deadline) {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// --- fixtures ---------------------------------------------------------------

// Shapes follow the documented examples for each log_type.
func fixtureActivity(n int) utils.Dict {
	return utils.Dict{
		"log_type": "activity_monitoring", "item_type": "File", "item_name": fmt.Sprintf("My File %d", n),
		"user": "John Smith", "source_ip": "10.10.10.10", "location": "Somewhere, USA", "action": "delete",
		"target_name": nil, "target_type": nil, "serial": "mySerial", "cloud_app_instance": "My Cloud App",
		"timestamp": "2017-02-17T00:18:58.961Z",
	}
}

func fixtureIncident(n int) utils.Dict {
	return utils.Dict{
		"log_type": "incident", "severity": 1.0, "item_type": "File", "item_name": fmt.Sprintf("helloworld%d.java", n),
		"asset_id": "5e9e38823cedb43cb015b460", "item_owner": "Admin User", "container_name": nil,
		"item_creator": "Admin User", "exposure": "COMPANY", "occurrences_by_rule": nil,
		"item_owner_email": "owner@emaildomain.com", "item_creator_email": "owner@emaildomain.com",
		"serial": nil, "cloud_app_instance": "Office 365 8", "timestamp": "2020-05-08T23:50:55Z",
		"incident_id": "5eb5ed492021c32b37588a6c", "policy_rule_name": "java", "incident_category": nil,
		"incident_owner": nil, "collaborators": "", "datetime_edited": "2020-05-08T23:50:55Z",
		"item_cloud_url":   "https://www.sharepoint.com/sites/site/Shared%20Documents/foldername/helloworld.java",
		"item_owner_group": "O365_1_all", "item_sha256": "4953946b0bbcd10d872d09561bf0f0988e186b625e4af65c64691adf5af279d4",
		"item_size": 9007199254740993, "item_verdict": "not available",
	}
}

func fixtureRemediation(n int) utils.Dict {
	return utils.Dict{
		"log_type": "remediation", "item_type": "File", "item_name": fmt.Sprintf("My File %d", n),
		"asset_id": "ce7c9ed11e6f4891ae73c1601af7f741", "item_owner": "John Smith", "item_creator": "John Smith",
		"container_name": "test-container", "action_taken": "quarantine", "action_taken_by": "John Smith",
		"serial": "mySerial", "cloud_app_instance": "My Cloud App", "timestamp": "2017-02-17T00:18:55.581Z",
		"incident_id": "9610efdcd8a74a259bf031843eac0309", "policy_rule_name": "PCI Policy",
		"item_owner_email": "owner@email-domain.com", "item_creator_email": "owner@email-domain.com",
	}
}

func fixturePolicyViolation(n int) utils.Dict {
	return utils.Dict{
		"log_type": "policy_violation", "severity": 3.0, "item_type": "File", "item_name": fmt.Sprintf("My File %d", n),
		"item_owner": "John Smith", "item_creator": "John Smith", "action_taken": "download", "action_taken_by": "John Smith",
		"asset_id": "ce7c9ed11e6f4891ae73c1601af7f741", "serial": "serial", "cloud_app_instance": "My Cloud App",
		"timestamp": "2017-01-06T19:04:06Z", "policy_rule_name": "Policy Rule",
		"incident_id": "9610efdcd8a74a259bf031843eac0309", "item_owner_email": nil, "item_creator_email": nil,
	}
}

func fixtureAdminAudit(n int) utils.Dict {
	return utils.Dict{
		"log_type": "admin_audit", "admin_id": "admin id", "admin_role": "admin role", "ip": "ip address",
		"event_type": "event type", "item_type": "File", "item_name": fmt.Sprintf("My File %d", n), "field": "field",
		"action": "action", "resource_value_old": "old val", "resource_value_new": "new val",
		"timestamp": "2017-04-06T21:35:10.025Z", "serial": "mySerial",
	}
}

// mixedEvents returns n events cycling through every log type, each with a
// unique item_name suffix so exactly-once can be asserted.
func mixedEvents(n int) []utils.Dict {
	makers := []func(int) utils.Dict{fixtureActivity, fixtureIncident, fixtureRemediation, fixturePolicyViolation, fixtureAdminAudit}
	out := make([]utils.Dict, n)
	for i := range out {
		out[i] = makers[i%len(makers)](i)
	}
	return out
}

// roundTrip passes a fixture through JSON the way the wire does, so it can be
// compared with what the adapter ships.
func roundTrip(t *testing.T, d utils.Dict) map[string]interface{} {
	t.Helper()
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	m, err := utils.UnmarshalCleanJSON(string(b))
	if err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	return m
}
