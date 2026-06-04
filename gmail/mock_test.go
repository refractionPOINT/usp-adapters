package usp_gmail

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"context"

	"github.com/golang-jwt/jwt/v4"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock Gmail API -- the
// OAuth token endpoint, users.messages.list and users.messages.get -- and
// captures the exact messages it ships so their content (event type, timestamp
// from internalDate, and verbatim payload) can be asserted. Both authentication
// modes (refresh token and service-account JWT-bearer) are covered.

// --- in-memory USP sink -----------------------------------------------------

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

// --- mock Gmail API ---------------------------------------------------------

const (
	mockClientID     = "test-client-id"
	mockClientSecret = "test-client-secret"
	mockRefreshToken = "test-refresh-token"
	mockSubject      = "user@example.test"
)

// mockGmail stands in for the Gmail OAuth + REST surface. It mints
// counter-numbered access tokens (so re-mints are observable), validates the
// bearer token on data requests, filters messages.list by the after: bound in
// the query, paginates, and serves full messages on get.
type mockGmail struct {
	mu sync.Mutex

	// messages is the mailbox, keyed by message id.
	messages map[string]utils.Dict
	order    []string // insertion order, for stable listing

	// getNotFound marks ids that appear in listings but whose get returns 404,
	// simulating a message deleted between the list and the get.
	getNotFound map[string]bool

	tokensIssued int

	// rejectTokensBefore makes data endpoints answer 401 for any access token
	// numbered at or below this value, simulating an expired access token and
	// forcing the adapter to refresh. Any issued token numbered above it is
	// accepted, so multiple mailboxes minting tokens concurrently do not
	// invalidate each other.
	rejectTokensBefore int

	// impersonatedSubjects counts, per impersonated subject, the service-account
	// assertions the token endpoint minted. It lets multi-mailbox tests assert
	// every expected mailbox was delegated.
	impersonatedSubjects map[string]int

	tokenRequests int
	listRequests  int
	getRequests   int

	// failTokenStatus, when non-zero, makes the token endpoint return that
	// status instead of issuing a token (e.g. 400 invalid_grant).
	failTokenStatus int

	// saPublicKey, when set, makes the token endpoint accept the jwt-bearer
	// grant and verify the assertion signature against it.
	saPublicKey *rsa.PublicKey

	// lastAssertionClaims captures the claims of the most recent service-account
	// assertion, for delegation assertions in tests.
	lastAssertionClaims jwt.MapClaims

	// --- BEC capability state (settings, profile, history) -----------------

	filters             []utils.Dict
	forwardingAddresses []utils.Dict
	autoForwarding      utils.Dict
	sendAs              []utils.Dict
	delegates           []utils.Dict
	imap                utils.Dict
	pop                 utils.Dict
	vacation            utils.Dict

	// profileHistoryID is what users.getProfile reports as the current historyId
	// (the baseline cursor for history collection).
	profileHistoryID string
	// history is the change log; the list handler returns records whose id is
	// greater than the requested startHistoryId.
	history []utils.Dict

	// statusOverride, keyed by capability path suffix (e.g. "settings/delegates",
	// "history"), forces that endpoint to answer the given HTTP status, to
	// simulate a feature unavailable for the account type or an expired cursor.
	statusOverride map[string]int

	// per-capability request counters, for asserting cadence.
	capabilityRequests map[string]int

	// --- Admin SDK Directory API (mailbox discovery) -----------------------

	// directoryUsers is what users.list returns, across pages of directoryPageSize.
	directoryUsers []discoveredUser
	// directoryPageSize, when > 0, forces users.list to paginate at this size.
	directoryPageSize int
	directoryStatus   int // when non-zero, users.list answers this HTTP status
	directoryRequests int
}

func newMockGmail() *mockGmail {
	return &mockGmail{
		messages:             map[string]utils.Dict{},
		getNotFound:          map[string]bool{},
		statusOverride:       map[string]int{},
		capabilityRequests:   map[string]int{},
		impersonatedSubjects: map[string]int{},
	}
}

// markGetNotFound makes get(id) answer 404 while the id stays listable.
func (m *mockGmail) markGetNotFound(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getNotFound[id] = true
}

func (m *mockGmail) addMessage(msg utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := msg.FindOneString("id")
	if _, ok := m.messages[id]; !ok {
		m.order = append(m.order, id)
	}
	m.messages[id] = msg
}

func (m *mockGmail) counts() (tokens, list, get int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenRequests, m.listRequests, m.getRequests
}

func (m *mockGmail) handler(t *testing.T) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/token", m.tokenHandler(t))
	mux.HandleFunc("/gmail/v1/users/me/messages", m.listHandler(t))
	mux.HandleFunc("/gmail/v1/users/me/messages/", m.getHandler(t))

	// BEC capability endpoints.
	mux.HandleFunc("/gmail/v1/users/me/settings/filters", m.capListHandler("settings/filters", "filter", func() []utils.Dict { return m.filters }))
	mux.HandleFunc("/gmail/v1/users/me/settings/forwardingAddresses", m.capListHandler("settings/forwardingAddresses", "forwardingAddresses", func() []utils.Dict { return m.forwardingAddresses }))
	mux.HandleFunc("/gmail/v1/users/me/settings/sendAs", m.capListHandler("settings/sendAs", "sendAs", func() []utils.Dict { return m.sendAs }))
	mux.HandleFunc("/gmail/v1/users/me/settings/delegates", m.capListHandler("settings/delegates", "delegates", func() []utils.Dict { return m.delegates }))
	mux.HandleFunc("/gmail/v1/users/me/settings/autoForwarding", m.capObjectHandler("settings/autoForwarding", func() utils.Dict { return m.autoForwarding }))
	mux.HandleFunc("/gmail/v1/users/me/settings/imap", m.capObjectHandler("settings/imap", func() utils.Dict { return m.imap }))
	mux.HandleFunc("/gmail/v1/users/me/settings/pop", m.capObjectHandler("settings/pop", func() utils.Dict { return m.pop }))
	mux.HandleFunc("/gmail/v1/users/me/settings/vacation", m.capObjectHandler("settings/vacation", func() utils.Dict { return m.vacation }))
	mux.HandleFunc("/gmail/v1/users/me/profile", m.profileHandler())
	mux.HandleFunc("/gmail/v1/users/me/history", m.historyHandler())

	// Admin SDK Directory API (mailbox discovery).
	mux.HandleFunc("/admin/directory/v1/users", m.directoryUsersHandler())
	return mux
}

func (m *mockGmail) tokenHandler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.tokenRequests++
		failStatus := m.failTokenStatus
		m.mu.Unlock()

		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !assert.NoError(t, r.ParseForm()) {
			return
		}

		switch r.Form.Get("grant_type") {
		case "refresh_token":
			if r.Form.Get("client_id") != mockClientID ||
				r.Form.Get("client_secret") != mockClientSecret ||
				r.Form.Get("refresh_token") != mockRefreshToken {
				writeJSON(w, http.StatusBadRequest, `{"error":"invalid_grant"}`)
				return
			}
		case jwtBearerGrantType:
			claims, ok := m.verifyAssertion(t, r.Form.Get("assertion"))
			if !ok {
				writeJSON(w, http.StatusBadRequest, `{"error":"invalid_grant","error_description":"assertion failed verification"}`)
				return
			}
			if sub, _ := claims["sub"].(string); sub != "" {
				m.mu.Lock()
				m.impersonatedSubjects[sub]++
				m.mu.Unlock()
			}
		default:
			writeJSON(w, http.StatusBadRequest, `{"error":"unsupported_grant_type"}`)
			return
		}

		if failStatus != 0 {
			writeJSON(w, failStatus, `{"error":"invalid_grant"}`)
			return
		}

		m.mu.Lock()
		m.tokensIssued++
		token := fmt.Sprintf("AT-%d", m.tokensIssued)
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, fmt.Sprintf(
			`{"access_token":%q,"token_type":"Bearer","expires_in":3600,"scope":%q}`,
			token, gmailReadonlyScope))
	}
}

// verifyAssertion validates a service-account JWT assertion's signature and
// records its claims, returning the parsed claims. Caller must not hold m.mu.
func (m *mockGmail) verifyAssertion(t *testing.T, assertion string) (jwt.MapClaims, bool) {
	m.mu.Lock()
	pub := m.saPublicKey
	m.mu.Unlock()
	if pub == nil || assertion == "" {
		return nil, false
	}
	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(assertion, claims, func(tok *jwt.Token) (interface{}, error) {
		if _, ok := tok.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method %v", tok.Header["alg"])
		}
		return pub, nil
	})
	if err != nil {
		t.Logf("assertion verification failed: %v", err)
		return nil, false
	}
	m.mu.Lock()
	m.lastAssertionClaims = claims
	m.mu.Unlock()
	return claims, true
}

func (m *mockGmail) listHandler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.listRequests++
		m.mu.Unlock()

		if !m.authorize(w, r) {
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		q := r.URL.Query()
		after := parseAfter(q.Get("q"))
		maxResults, _ := strconv.Atoi(q.Get("maxResults"))
		if maxResults <= 0 {
			maxResults = 100
		}
		offset := 0
		if pt := q.Get("pageToken"); pt != "" {
			offset, _ = strconv.Atoi(pt)
		}
		includeSpamTrash := q.Get("includeSpamTrash") == "true"
		labelFilter := q["labelIds"]

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(m.buildList(after, offset, maxResults, includeSpamTrash, labelFilter))
	}
}

func (m *mockGmail) getHandler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.getRequests++
		m.mu.Unlock()

		if !m.authorize(w, r) {
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/gmail/v1/users/me/messages/")

		m.mu.Lock()
		msg, ok := m.messages[id]
		if m.getNotFound[id] {
			ok = false
		}
		m.mu.Unlock()
		if !ok {
			writeJSON(w, http.StatusNotFound,
				`{"error":{"code":404,"message":"Requested entity was not found.","errors":[{"reason":"notFound"}],"status":"NOT_FOUND"}}`)
			return
		}
		b, _ := json.Marshal(msg)
		writeJSON(w, http.StatusOK, string(b))
	}
}

// authorize validates the bearer token, writing a 401 and returning false on
// failure. Any token the endpoint issued (AT-n with 1<=n<=tokensIssued) is
// accepted, except those at or below rejectTokensBefore -- so concurrent
// mailboxes minting their own tokens never invalidate each other.
func (m *mockGmail) authorize(w http.ResponseWriter, r *http.Request) bool {
	auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	m.mu.Lock()
	ok := m.tokenAccepted(auth)
	m.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusUnauthorized,
			`{"error":{"code":401,"message":"Invalid Credentials","errors":[{"reason":"authError"}],"status":"UNAUTHENTICATED"}}`)
		return false
	}
	return true
}

// tokenAccepted reports whether the given access token is a currently-valid
// issued token. Caller holds m.mu.
func (m *mockGmail) tokenAccepted(token string) bool {
	var n int
	if _, err := fmt.Sscanf(token, "AT-%d", &n); err != nil {
		return false
	}
	return n >= 1 && n <= m.tokensIssued && n > m.rejectTokensBefore
}

// buildList renders a messages.list response: ids whose internalDate is at or
// after `after` (seconds), respecting the label/spam-trash filters, sorted by
// internalDate then id, paginated from `offset` by `maxResults`.
func (m *mockGmail) buildList(after int64, offset, maxResults int, includeSpamTrash bool, labelFilter []string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	var matched []utils.Dict
	for _, id := range m.order {
		msg := m.messages[id]
		ms := internalDateMs(msg)
		if ms/1000 < after {
			continue
		}
		labels := messageLabels(msg)
		if !includeSpamTrash && (labels["SPAM"] || labels["TRASH"]) {
			continue
		}
		if !hasAllLabels(labels, labelFilter) {
			continue
		}
		matched = append(matched, msg)
	}
	sort.SliceStable(matched, func(i, j int) bool {
		mi, mj := internalDateMs(matched[i]), internalDateMs(matched[j])
		if mi != mj {
			return mi < mj
		}
		return matched[i].FindOneString("id") < matched[j].FindOneString("id")
	})

	total := len(matched)
	end := offset + maxResults
	if end > total {
		end = total
	}
	page := matched
	if offset < total {
		page = matched[offset:end]
	} else {
		page = nil
	}

	resp := map[string]interface{}{"resultSizeEstimate": total}
	if len(page) > 0 {
		refs := make([]map[string]string, 0, len(page))
		for _, msg := range page {
			refs = append(refs, map[string]string{
				"id":       msg.FindOneString("id"),
				"threadId": msg.FindOneString("threadId"),
			})
		}
		resp["messages"] = refs
	}
	if end < total {
		resp["nextPageToken"] = strconv.Itoa(end)
	}
	b, _ := json.Marshal(resp)
	return b
}

// --- helpers ----------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

// parseAfter extracts the epoch-seconds value of an "after:" operator from a
// Gmail query, or 0 if absent.
func parseAfter(q string) int64 {
	for _, tok := range strings.Fields(q) {
		if v, ok := strings.CutPrefix(tok, "after:"); ok {
			n, _ := strconv.ParseInt(v, 10, 64)
			return n
		}
	}
	return 0
}

func internalDateMs(msg utils.Dict) int64 {
	n, _ := strconv.ParseInt(msg.FindOneString("internalDate"), 10, 64)
	return n
}

func messageLabels(msg utils.Dict) map[string]bool {
	out := map[string]bool{}
	if raw, ok := msg["labelIds"]; ok {
		if list, ok := raw.([]interface{}); ok {
			for _, l := range list {
				if s, ok := l.(string); ok {
					out[s] = true
				}
			}
		}
	}
	return out
}

func hasAllLabels(have map[string]bool, want []string) bool {
	for _, l := range want {
		if !have[l] {
			return false
		}
	}
	return true
}

// recentMs renders an internalDate (epoch ms, as Gmail returns it) for a time
// `ago` before now, so fixtures land inside the adapter's collection window.
func recentMs(ago time.Duration) string {
	return strconv.FormatInt(time.Now().Add(-ago).UnixMilli(), 10)
}

// realisticMessage builds a full-format Gmail message resource with the nested
// payload/headers/body shape the real API returns.
func realisticMessage(id, threadID, internalDate, from, subject string) utils.Dict {
	body := base64.URLEncoding.EncodeToString([]byte("Hello, this is the body of " + subject))
	return utils.Dict{
		"id":           id,
		"threadId":     threadID,
		"labelIds":     []interface{}{"INBOX", "UNREAD"},
		"snippet":      "Hello, this is the body of " + subject,
		"historyId":    "987654",
		"internalDate": internalDate,
		"sizeEstimate": 1024,
		"payload": utils.Dict{
			"partId":   "",
			"mimeType": "text/plain",
			"filename": "",
			"headers": []interface{}{
				utils.Dict{"name": "From", "value": from},
				utils.Dict{"name": "To", "value": mockSubject},
				utils.Dict{"name": "Subject", "value": subject},
				utils.Dict{"name": "Date", "value": "Wed, 13 Sep 2023 15:42:47 +0000"},
				utils.Dict{"name": "Message-ID", "value": "<" + id + "@mail.example.test>"},
			},
			"body": utils.Dict{"size": 256, "data": body},
		},
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// refreshConfig is a minimal valid refresh-token config pointed at the mock.
func refreshConfig(t *testing.T, baseURL string) GmailConfig {
	t.Helper()
	return GmailConfig{
		ClientOptions:   testClientOptions(t),
		ClientID:        mockClientID,
		ClientSecret:    mockClientSecret,
		RefreshToken:    mockRefreshToken,
		BaseURL:         baseURL,
		TokenURL:        baseURL + "/token",
		InitialLookback: 24 * time.Hour,
		PollInterval:    40 * time.Millisecond,
	}
}

// generateServiceAccount builds a throwaway RSA key and a service-account JSON
// key embedding it, returning the JSON and the matching public key.
func generateServiceAccount(t *testing.T) (saJSON string, pub *rsa.PublicKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})

	sa := map[string]string{
		"type":           "service_account",
		"project_id":     "test-project",
		"private_key_id": "kid-123",
		"private_key":    string(pemBytes),
		"client_email":   "collector@test-project.iam.gserviceaccount.com",
		"client_id":      "1234567890",
		"token_uri":      "https://oauth2.googleapis.com/token",
	}
	b, err := json.Marshal(sa)
	require.NoError(t, err)
	return string(b), &key.PublicKey
}

// --- tests ------------------------------------------------------------------

// TestMockEndToEndRefreshToken drives the adapter against the mock API via the
// refresh-token flow and asserts the exact events shipped: event type
// "gmail_message", timestamp from internalDate, and the message payload
// preserved verbatim (nested payload/headers/body included).
func TestMockEndToEndRefreshToken(t *testing.T) {
	mock := newMockGmail()
	msgs := []utils.Dict{
		realisticMessage("m1", "t1", recentMs(50*time.Minute), "alice@partner.test", "Invoice #1"),
		realisticMessage("m2", "t2", recentMs(40*time.Minute), "bob@partner.test", "Re: meeting"),
		realisticMessage("m3", "t3", recentMs(30*time.Minute), "carol@partner.test", "Welcome"),
	}
	for _, m := range msgs {
		mock.addMessage(m)
	}

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newGmailAdapter(ctx, refreshConfig(t, server.URL), staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 messages to ship")
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 30*time.Millisecond, "messages were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		assert.Equal(t, eventTypeMessage, msg.EventType)
		require.NotNil(t, msg.JsonPayload)
		byID[utils.Dict(msg.JsonPayload).FindOneString("id")] = msg
	}

	for _, want := range msgs {
		shipped := byID[want.FindOneString("id")]
		require.NotNil(t, shipped, "message %s not shipped", want.FindOneString("id"))

		ms, perr := strconv.ParseInt(want.FindOneString("internalDate"), 10, 64)
		require.NoError(t, perr)
		assert.Equal(t, uint64(ms), shipped.TimestampMs, "event time must come from internalDate")
		assert.JSONEq(t, mustJSON(t, want), mustJSON(t, shipped.JsonPayload),
			"shipped payload must match the original Gmail message verbatim")
	}
}

// TestMockEndToEndServiceAccount drives the adapter via the service-account
// JWT-bearer flow and asserts collection works and the signed assertion carried
// the delegation claims (subject, scope, issuer).
func TestMockEndToEndServiceAccount(t *testing.T) {
	saJSON, pub := generateServiceAccount(t)

	mock := newMockGmail()
	mock.saPublicKey = pub
	mock.addMessage(realisticMessage("s1", "t1", recentMs(20*time.Minute), "dave@partner.test", "Quarterly report"))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := GmailConfig{
		ClientOptions:             testClientOptions(t),
		ServiceAccountCredentials: saJSON,
		Subject:                   mockSubject,
		BaseURL:                   server.URL,
		TokenURL:                  server.URL + "/token",
		InitialLookback:           24 * time.Hour,
		PollInterval:              40 * time.Millisecond,
	}
	adapter, _, err := newGmailAdapter(ctx, conf, staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond, "the message should ship via the service-account flow")

	mock.mu.Lock()
	claims := mock.lastAssertionClaims
	mock.mu.Unlock()
	require.NotNil(t, claims, "the token endpoint should have received a signed assertion")
	assert.Equal(t, mockSubject, claims["sub"], "assertion must impersonate the configured subject")
	assert.Equal(t, gmailReadonlyScope, claims["scope"], "assertion must request the configured scope")
	assert.Equal(t, "collector@test-project.iam.gserviceaccount.com", claims["iss"])
}

// TestMockNewMessageShippedOnce verifies a message that arrives mid-run is
// shipped exactly once, and already-shipped messages are never re-sent across
// overlapping polls.
func TestMockNewMessageShippedOnce(t *testing.T) {
	mock := newMockGmail()
	mock.addMessage(realisticMessage("m1", "t1", recentMs(50*time.Minute), "alice@partner.test", "First"))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newGmailAdapter(ctx, refreshConfig(t, server.URL), staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond)

	// A new email arrives.
	mock.addMessage(realisticMessage("m2", "t2", recentMs(1*time.Second), "bob@partner.test", "Second"))

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "the new message should ship")
	require.Never(t, func() bool { return sink.count() > 2 },
		300*time.Millisecond, 30*time.Millisecond)

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[utils.Dict(msg.JsonPayload).FindOneString("id")]++
	}
	assert.Equal(t, map[string]int{"m1": 1, "m2": 1}, shippedPerID, "every message must ship exactly once")
}

// TestMockMultiPollNoReship verifies the rolling-window cursor advances and the
// deduper keeps each message to exactly one shipment across many overlapping
// polls.
func TestMockMultiPollNoReship(t *testing.T) {
	mock := newMockGmail()
	mock.addMessage(realisticMessage("m1", "t1", recentMs(10*time.Minute), "alice@partner.test", "A"))
	mock.addMessage(realisticMessage("m2", "t2", recentMs(5*time.Minute), "bob@partner.test", "B"))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A generous overlap forces every poll to re-list both messages.
	conf := refreshConfig(t, server.URL)
	conf.Overlap = time.Hour
	adapter, _, err := newGmailAdapter(ctx, conf, staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)
	require.Never(t, func() bool { return sink.count() != 2 },
		500*time.Millisecond, 40*time.Millisecond, "overlapping polls must not re-ship")

	_, listReqs, _ := mock.counts()
	assert.GreaterOrEqual(t, listReqs, 3, "several polls should have run")
}

// TestMockTokenRefreshOn401 verifies the adapter transparently re-mints its
// access token when the API rejects it with a 401, then succeeds.
func TestMockTokenRefreshOn401(t *testing.T) {
	mock := newMockGmail()
	mock.rejectTokensBefore = 1 // reject AT-1 on data calls, forcing a refresh to AT-2
	mock.addMessage(realisticMessage("m1", "t1", recentMs(10*time.Minute), "alice@partner.test", "Hi"))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, chStopped, err := newGmailAdapter(ctx, refreshConfig(t, server.URL), staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond, "the message should ship after a token refresh")

	select {
	case <-chStopped:
		t.Fatal("adapter stopped on a recoverable 401 instead of refreshing")
	default:
	}

	tokens, _, _ := mock.counts()
	assert.GreaterOrEqual(t, tokens, 2, "expected at least an initial mint plus one refresh")
}

// TestMockBadCredentialsStops verifies the adapter stops, and ships nothing,
// when the token endpoint permanently rejects the credentials.
func TestMockBadCredentialsStops(t *testing.T) {
	mock := newMockGmail()
	mock.failTokenStatus = http.StatusBadRequest // invalid_grant
	mock.addMessage(realisticMessage("m1", "t1", recentMs(10*time.Minute), "alice@partner.test", "Hi"))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, chStopped, err := newGmailAdapter(ctx, refreshConfig(t, server.URL), staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	select {
	case <-chStopped:
		// Expected: dead credentials stop the adapter.
	case <-time.After(3 * time.Second):
		t.Fatal("adapter should stop when the credentials are rejected")
	}
	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
}

// TestMockPagination verifies the adapter walks every page of a multi-page
// listing and ships all messages exactly once.
func TestMockPagination(t *testing.T) {
	mock := newMockGmail()
	const n = 5
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("m%d", i)
		mock.addMessage(realisticMessage(id, "t"+id, recentMs(time.Duration(n-i)*time.Minute),
			"sender@partner.test", "Subject "+id))
	}

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := refreshConfig(t, server.URL)
	conf.MaxResults = 2 // force three pages (2 + 2 + 1)
	adapter, _, err := newGmailAdapter(ctx, conf, staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == n },
		5*time.Second, 20*time.Millisecond, "all paginated messages should ship")
	require.Never(t, func() bool { return sink.count() != n },
		300*time.Millisecond, 30*time.Millisecond)

	ids := map[string]int{}
	for _, msg := range sink.snapshot() {
		ids[utils.Dict(msg.JsonPayload).FindOneString("id")]++
	}
	require.Len(t, ids, n)
	for id, c := range ids {
		assert.Equal(t, 1, c, "message %s shipped more than once", id)
	}
}

// TestMockMissingMessageSkipped verifies that a message id returned by list but
// missing on get (deleted in between) is skipped without stopping the adapter,
// and other messages still ship.
func TestMockMissingMessageSkipped(t *testing.T) {
	mock := newMockGmail()
	// "ghost" is returned by the listing but 404s on get -- the message was
	// deleted between the list and the fetch. "real" is a normal message.
	mock.addMessage(realisticMessage("ghost", "tg", recentMs(9*time.Minute), "x@partner.test", "Ghost"))
	mock.addMessage(realisticMessage("real", "tr", recentMs(8*time.Minute), "y@partner.test", "Real"))
	mock.markGetNotFound("ghost")

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, chStopped, err := newGmailAdapter(ctx, refreshConfig(t, server.URL), staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond, "the real message should still ship despite the 404")

	select {
	case <-chStopped:
		t.Fatal("a 404 on one message must not stop the adapter")
	default:
	}
	assert.Equal(t, "real", utils.Dict(sink.snapshot()[0].JsonPayload).FindOneString("id"))
}
