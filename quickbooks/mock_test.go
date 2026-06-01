package usp_quickbooks

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock QuickBooks Online
// API -- the OAuth token endpoint and the ChangeDataCapture endpoint -- and
// captures the exact messages it ships so their content (entity type as event
// type, timestamp from MetaData.LastUpdatedTime, and verbatim payload) can be
// asserted.

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

// --- mock QuickBooks Online API ---------------------------------------------

const (
	mockClientID     = "test-client-id"
	mockClientSecret = "test-client-secret"
	mockRefreshToken = "test-refresh-token"
	mockRealmID      = "1234567890"
)

// mockQuickBooks stands in for the QBO OAuth + CDC surface. It mints
// counter-numbered access tokens (so re-mints are observable), validates the
// bearer token on CDC, and serves per-entity datasets filtered by changedSince.
type mockQuickBooks struct {
	mu sync.Mutex

	datasets map[string][]utils.Dict // entity type -> records

	tokensIssued int
	activeToken  string

	// rejectTokensBefore makes the CDC endpoint answer 401 for any access token
	// numbered at or below this value, simulating an expired access token and
	// forcing the adapter to refresh.
	rejectTokensBefore int

	tokenRequests int
	cdcRequests   int

	// failTokenStatus, when non-zero, makes the token endpoint return that
	// status instead of issuing a token (e.g. 400 invalid_grant).
	failTokenStatus int
}

func newMockQuickBooks() *mockQuickBooks {
	return &mockQuickBooks{datasets: map[string][]utils.Dict{}}
}

func (m *mockQuickBooks) setDataset(entityType string, records []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.datasets[entityType] = records
}

// appendRecord adds a record to an entity's dataset, as the API would when a
// new change occurs.
func (m *mockQuickBooks) appendRecord(entityType string, record utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.datasets[entityType] = append(m.datasets[entityType], record)
}

func (m *mockQuickBooks) counts() (tokens, cdc int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenRequests, m.cdcRequests
}

func (m *mockQuickBooks) handler(t *testing.T) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth2/v1/tokens/bearer", func(w http.ResponseWriter, r *http.Request) {
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
		// Validate HTTP Basic client credentials.
		wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(mockClientID+":"+mockClientSecret))
		if r.Header.Get("Authorization") != wantAuth {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
			return
		}
		if r.Form.Get("grant_type") != "refresh_token" || r.Form.Get("refresh_token") != mockRefreshToken {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
			return
		}
		if failStatus != 0 {
			w.WriteHeader(failStatus)
			_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
			return
		}

		m.mu.Lock()
		m.tokensIssued++
		m.activeToken = fmt.Sprintf("AT-%d", m.tokensIssued)
		token := m.activeToken
		m.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(
			`{"access_token":%q,"refresh_token":%q,"token_type":"bearer","expires_in":3600,"x_refresh_token_expires_in":8726400}`,
			token, mockRefreshToken)))
	})

	mux.HandleFunc("/v3/company/"+mockRealmID+"/cdc", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.cdcRequests++
		m.mu.Unlock()

		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		m.mu.Lock()
		active := m.activeToken
		reject := m.tokenRejected(auth)
		m.mu.Unlock()
		if auth == "" || auth != active || reject {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"Fault":{"Error":[{"Message":"AuthenticationFailed"}],"type":"AUTHENTICATION"}}`))
			return
		}

		changedSince, _ := time.Parse(time.RFC3339, r.URL.Query().Get("changedSince"))
		entities := strings.Split(r.URL.Query().Get("entities"), ",")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(m.buildCDC(entities, changedSince))
	})
	return mux
}

// tokenRejected reports whether the given access token should be answered with
// a 401. Caller holds m.mu.
func (m *mockQuickBooks) tokenRejected(token string) bool {
	if m.rejectTokensBefore == 0 {
		return false
	}
	var n int
	if _, err := fmt.Sscanf(token, "AT-%d", &n); err != nil {
		return false
	}
	return n <= m.rejectTokensBefore
}

// buildCDC renders a CDCResponse for the requested entities, including only
// records whose LastUpdatedTime is at or after changedSince.
func (m *mockQuickBooks) buildCDC(entities []string, changedSince time.Time) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	var blocks []map[string]interface{}
	sortedEntities := append([]string(nil), entities...)
	sort.Strings(sortedEntities)
	for _, e := range sortedEntities {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		var matched []utils.Dict
		for _, rec := range m.datasets[e] {
			lut := rec.FindOneString("MetaData/LastUpdatedTime")
			ts, err := time.Parse(time.RFC3339, lut)
			if err != nil || ts.Before(changedSince) {
				continue
			}
			matched = append(matched, rec)
		}
		if len(matched) == 0 {
			continue
		}
		blocks = append(blocks, map[string]interface{}{
			e:               matched,
			"startPosition": 1,
			"maxResults":    len(matched),
		})
	}
	if len(blocks) == 0 {
		blocks = []map[string]interface{}{{}}
	}

	env := map[string]interface{}{
		"CDCResponse": []map[string]interface{}{{"QueryResponse": blocks}},
		"time":        "2026-05-12T14:10:00Z",
	}
	b, _ := json.Marshal(env)
	return b
}

// --- realistic fixtures -----------------------------------------------------

// realisticCustomer returns a record shaped like a real QBO Customer, with the
// nested MetaData and a mix of scalar types.
func realisticCustomer(id, lut string) utils.Dict {
	return utils.Dict{
		"Id":               id,
		"SyncToken":        "0",
		"DisplayName":      "Amy's Bird Sanctuary " + id,
		"Active":           true,
		"Balance":          239.0,
		"PrimaryEmailAddr": utils.Dict{"Address": "amy@example.test"},
		"MetaData": utils.Dict{
			"CreateTime":      "2024-12-06T16:48:43-08:00",
			"LastUpdatedTime": lut,
		},
	}
}

// realisticInvoice returns a record shaped like a real QBO Invoice with nested
// Line items.
func realisticInvoice(id, lut string) utils.Dict {
	return utils.Dict{
		"Id":          id,
		"SyncToken":   "2",
		"DocNumber":   "10" + id,
		"TotalAmt":    362.07,
		"CustomerRef": utils.Dict{"value": "1", "name": "Amy's Bird Sanctuary"},
		"Line": []interface{}{
			utils.Dict{"Id": "1", "Amount": 150.0, "DetailType": "SalesItemLineDetail"},
			utils.Dict{"Id": "2", "Amount": 212.07, "DetailType": "SalesItemLineDetail"},
		},
		"MetaData": utils.Dict{
			"CreateTime":      "2024-12-06T16:48:43-08:00",
			"LastUpdatedTime": lut,
		},
	}
}

// deletedInvoice returns a record shaped like a CDC-reported deletion.
func deletedInvoice(id, lut string) utils.Dict {
	return utils.Dict{
		"Id":        id,
		"SyncToken": "3",
		"status":    "Deleted",
		"MetaData":  utils.Dict{"LastUpdatedTime": lut},
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func mockConfig(t *testing.T, baseURL string) QuickBooksConfig {
	t.Helper()
	return QuickBooksConfig{
		ClientOptions: testClientOptions(t),
		ClientID:      mockClientID,
		ClientSecret:  mockClientSecret,
		RefreshToken:  mockRefreshToken,
		RealmID:       mockRealmID,
		BaseURL:       baseURL,
		TokenURL:      baseURL + "/oauth2/v1/tokens/bearer",
		// Reach back far enough that the seeded fixtures (dated in the recent
		// past) fall inside the very first poll's window.
		InitialLookback: 24 * time.Hour,
		PollInterval:    40 * time.Millisecond,
		Entities:        []string{"Customer", "Invoice"},
	}
}

// recentTime renders a timestamp `ago` before now in RFC3339, so fixtures land
// inside the adapter's change window regardless of when the test runs.
func recentTime(ago time.Duration) string {
	return time.Now().Add(-ago).UTC().Format(time.RFC3339)
}

// --- tests ------------------------------------------------------------------

// TestMockCDCEndToEnd drives the adapter against the mock API and asserts the
// exact events shipped: event type set to the entity type, timestamp parsed
// from MetaData.LastUpdatedTime, and the payload preserved verbatim (nested
// objects and arrays included).
func TestMockCDCEndToEnd(t *testing.T) {
	mock := newMockQuickBooks()
	customers := []utils.Dict{
		realisticCustomer("1", recentTime(50*time.Minute)),
		realisticCustomer("2", recentTime(40*time.Minute)),
	}
	invoices := []utils.Dict{
		realisticInvoice("130", recentTime(30*time.Minute)),
	}
	mock.setDataset("Customer", customers)
	mock.setDataset("Invoice", invoices)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newQuickBooksAdapter(ctx, mockConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 changed entities to ship")
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 30*time.Millisecond, "changes were re-shipped on a later poll")

	byType := map[string]int{}
	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		byType[msg.EventType]++
		require.NotNil(t, msg.JsonPayload)
		byID[msg.EventType+"/"+utils.Dict(msg.JsonPayload).FindOneString("Id")] = msg
	}
	assert.Equal(t, map[string]int{"Customer": 2, "Invoice": 1}, byType)

	// Every fixture shipped verbatim, with its event time taken from MetaData.
	for _, want := range append(append([]utils.Dict{}, customers...), invoices...) {
		entityType := "Customer"
		if _, ok := want["DocNumber"]; ok {
			entityType = "Invoice"
		}
		msg := byID[entityType+"/"+want.FindOneString("Id")]
		require.NotNil(t, msg, "record %s/%s not shipped", entityType, want.FindOneString("Id"))

		ts, perr := time.Parse(time.RFC3339, want.FindOneString("MetaData/LastUpdatedTime"))
		require.NoError(t, perr)
		assert.Equal(t, uint64(ts.UnixMilli()), msg.TimestampMs,
			"event time must come from MetaData.LastUpdatedTime")
		assert.JSONEq(t, mustJSON(t, want), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original QBO entity")
	}
}

// TestMockNewChangeShippedOnce verifies a change that appears mid-run is shipped
// exactly once, and already-shipped changes are never re-sent.
func TestMockNewChangeShippedOnce(t *testing.T) {
	mock := newMockQuickBooks()
	mock.setDataset("Customer", []utils.Dict{
		realisticCustomer("1", recentTime(50*time.Minute)),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newQuickBooksAdapter(ctx, mockConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond)

	// A new change appears: a brand-new invoice.
	mock.appendRecord("Invoice", realisticInvoice("200", recentTime(1*time.Second)))

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "the new change should ship")
	require.Never(t, func() bool { return sink.count() > 2 },
		300*time.Millisecond, 30*time.Millisecond)

	shippedPerKey := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerKey[msg.EventType+"/"+utils.Dict(msg.JsonPayload).FindOneString("Id")]++
	}
	assert.Equal(t, map[string]int{"Customer/1": 1, "Invoice/200": 1}, shippedPerKey,
		"every change must ship exactly once")
}

// TestMockUpdatedEntityReship verifies that when an entity changes again (a new
// MetaData.LastUpdatedTime), the new revision is shipped as a fresh event even
// though the Id is unchanged.
func TestMockUpdatedEntityReship(t *testing.T) {
	mock := newMockQuickBooks()
	mock.setDataset("Customer", []utils.Dict{
		realisticCustomer("7", recentTime(50*time.Minute)),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newQuickBooksAdapter(ctx, mockConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond)

	// Same Id, newer LastUpdatedTime: the customer was edited again.
	mock.setDataset("Customer", []utils.Dict{
		realisticCustomer("7", recentTime(1*time.Second)),
	})

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "a new revision of the same entity should ship")

	ids := map[string]int{}
	for _, msg := range sink.snapshot() {
		ids[utils.Dict(msg.JsonPayload).FindOneString("Id")]++
	}
	assert.Equal(t, 2, ids["7"], "both revisions of customer 7 should ship")
}

// TestMockDeletedEntity verifies a CDC-reported deletion (status="Deleted") is
// collected and shipped, tagged with its entity type.
func TestMockDeletedEntity(t *testing.T) {
	mock := newMockQuickBooks()
	mock.setDataset("Invoice", []utils.Dict{
		deletedInvoice("404", recentTime(10*time.Minute)),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newQuickBooksAdapter(ctx, mockConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond, "the deletion should ship")

	msg := sink.snapshot()[0]
	assert.Equal(t, "Invoice", msg.EventType)
	assert.Equal(t, "Deleted", utils.Dict(msg.JsonPayload).FindOneString("status"))
	assert.Equal(t, "404", utils.Dict(msg.JsonPayload).FindOneString("Id"))
}

// TestMockAccessTokenRefreshOn401 verifies the adapter transparently re-mints
// its access token when the API rejects it with a 401, then succeeds.
func TestMockAccessTokenRefreshOn401(t *testing.T) {
	mock := newMockQuickBooks()
	// Reject the first issued token (AT-1) on CDC, forcing a refresh to AT-2.
	mock.rejectTokensBefore = 1
	mock.setDataset("Customer", []utils.Dict{
		realisticCustomer("1", recentTime(10*time.Minute)),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, chStopped, err := newQuickBooksAdapter(ctx, mockConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond, "the change should ship after a token refresh")

	// The adapter must not have stopped: a 401 is recoverable via refresh.
	select {
	case <-chStopped:
		t.Fatal("adapter stopped on a recoverable 401 instead of refreshing")
	default:
	}

	tokens, _ := mock.counts()
	assert.GreaterOrEqual(t, tokens, 2, "expected at least an initial mint plus one refresh")
}

// TestMockBadCredentialsStops verifies the adapter stops, and ships nothing,
// when the token endpoint permanently rejects the refresh token.
func TestMockBadCredentialsStops(t *testing.T) {
	mock := newMockQuickBooks()
	mock.failTokenStatus = http.StatusBadRequest // invalid_grant
	mock.setDataset("Customer", []utils.Dict{
		realisticCustomer("1", recentTime(10*time.Minute)),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, chStopped, err := newQuickBooksAdapter(ctx, mockConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	select {
	case <-chStopped:
		// Expected: a dead refresh token stops the adapter.
	case <-time.After(3 * time.Second):
		t.Fatal("adapter should stop when the refresh token is rejected")
	}
	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
	tokens, _ := mock.counts()
	assert.GreaterOrEqual(t, tokens, 1, "the adapter should have attempted a token refresh")
}

// TestMockMultiPollNoGap verifies the rolling-window cursor advances and the
// deduper keeps each change to exactly one shipment across many polls.
func TestMockMultiPollNoGap(t *testing.T) {
	mock := newMockQuickBooks()
	mock.setDataset("Customer", []utils.Dict{
		realisticCustomer("1", recentTime(30*time.Minute)),
		realisticCustomer("2", recentTime(20*time.Minute)),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newQuickBooksAdapter(ctx, mockConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)

	// Let several more polls run; the overlap re-fetches these records every
	// poll, but the deduper must keep the total at 2.
	require.Never(t, func() bool { return sink.count() != 2 },
		500*time.Millisecond, 40*time.Millisecond, "overlapping polls must not re-ship")

	_, cdc := mock.counts()
	assert.GreaterOrEqual(t, cdc, 3, "several polls should have run")
}
