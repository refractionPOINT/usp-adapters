package usp_wiz

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock of the Wiz API:
// the OAuth client-credentials token endpoint (auth.app.wiz.io/oauth/token in
// production) and the GraphQL endpoint, capturing the exact messages shipped
// so their content can be asserted.

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

// --- mock Wiz API -------------------------------------------------------------

// mockWiz is an in-memory stand-in for the Wiz API. It serves two endpoints:
//
//   - POST /oauth/token: the OAuth2 client-credentials exchange the real
//     auth.app.wiz.io/oauth/token performs (form-encoded, audience=wiz-api),
//     returning a bearer token.
//   - POST /graphql: the GraphQL endpoint (api.<region>.app.wiz.io/graphql in
//     production). It validates the bearer token, parses the query/variables
//     the adapter sends, and answers from an in-memory issue set, honoring
//     the filterBy.<timeField>.after time filter the way the real API filters
//     issues (strictly newer than "after", newest first).
type mockWiz struct {
	mu sync.Mutex

	clientID     string
	clientSecret string
	expiresIn    float64

	tokenSerial int    // bumps on every successful token issue
	lastToken   string // the most recently issued bearer token

	issues []map[string]interface{}

	graphqlError bool // when true, /graphql returns a GraphQL errors envelope

	tokenRequests   int
	graphqlRequests int
	lastQuery       string
	lastVariables   map[string]interface{}
}

func newMockWiz(clientID, clientSecret string) *mockWiz {
	return &mockWiz{
		clientID:     clientID,
		clientSecret: clientSecret,
		expiresIn:    3600,
	}
}

func (m *mockWiz) setIssues(issues []map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.issues = issues
}

func (m *mockWiz) addIssue(issue map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.issues = append(m.issues, issue)
}

func (m *mockWiz) setGraphQLError(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.graphqlError = v
}

func (m *mockWiz) tokenRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenRequests
}

func (m *mockWiz) graphqlRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.graphqlRequests
}

func (m *mockWiz) tokenIssueCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenSerial
}

func (m *mockWiz) capturedRequest() (string, map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastQuery, m.lastVariables
}

func (m *mockWiz) server(t *testing.T) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth/token", m.handleToken(t))
	mux.HandleFunc("/graphql", m.handleGraphQL(t))
	return httptest.NewServer(mux)
}

// handleToken implements the OAuth2 client-credentials exchange the way
// auth.app.wiz.io does: a form-encoded POST carrying grant_type, client_id,
// client_secret and audience=wiz-api, answered with an access_token JSON.
func (m *mockWiz) handleToken(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.tokenRequests++
		m.mu.Unlock()

		// Handlers run on server goroutines: use assert (not require).
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		form, err := url.ParseQuery(string(body))
		if !assert.NoError(t, err) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		assert.Equal(t, "client_credentials", form.Get("grant_type"),
			"the token exchange must be a client-credentials grant")
		assert.Equal(t, "wiz-api", form.Get("audience"),
			"the Wiz token exchange requires audience=wiz-api")

		if form.Get("client_id") != m.clientID || form.Get("client_secret") != m.clientSecret {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"access_denied","error_description":"Unauthorized"}`))
			return
		}

		m.mu.Lock()
		m.tokenSerial++
		m.lastToken = fmt.Sprintf("wiz-test-access-token-%04d", m.tokenSerial)
		token := m.lastToken
		expiresIn := m.expiresIn
		m.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": token,
			"token_type":   "Bearer",
			"expires_in":   expiresIn,
		})
	}
}

// handleGraphQL validates the bearer token and answers the adapter's query
// from the in-memory issue set, honoring the filterBy.<timeField>.after
// filter: only issues with a createdAt strictly newer than "after" are
// returned, newest first, inside the standard GraphQL data envelope.
func (m *mockWiz) handleGraphQL(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.graphqlRequests++
		expectedAuth := "Bearer " + m.lastToken
		hasToken := m.lastToken != ""
		gqlError := m.graphqlError
		m.mu.Unlock()

		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		if !hasToken || r.Header.Get("Authorization") != expectedAuth {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"errors":[{"message":"Unauthorized","extensions":{"code":"UNAUTHENTICATED"}}]}`))
			return
		}

		var reqBody struct {
			Query     string                 `json:"query"`
			Variables map[string]interface{} `json:"variables"`
		}
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&reqBody)) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastQuery = reqBody.Query
		m.lastVariables = reqBody.Variables
		m.mu.Unlock()

		if gqlError {
			// A GraphQL-level failure: HTTP 200 with an errors envelope and no
			// data, exactly how GraphQL APIs report resolver errors.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"errors":[{"message":"internal server error","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`))
			return
		}

		after := extractAfter(reqBody.Variables)
		var afterTime time.Time
		if after != "" {
			parsed, err := time.Parse(time.RFC3339, after)
			if !assert.NoError(t, err, "the after filter must be an RFC3339 timestamp") {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			afterTime = parsed
		}

		m.mu.Lock()
		var nodes []map[string]interface{}
		for _, issue := range m.issues {
			createdAt, _ := issue["createdAt"].(string)
			ts, err := time.Parse(time.RFC3339, createdAt)
			if !assert.NoError(t, err, "fixture createdAt must be RFC3339") {
				continue
			}
			if after != "" && !ts.After(afterTime) {
				continue
			}
			nodes = append(nodes, issue)
		}
		m.mu.Unlock()

		// The adapter's incremental polling relies on a newest-first ordering
		// (it takes the first node's timestamp as the next "after" watermark).
		sort.SliceStable(nodes, func(i, j int) bool {
			ti, _ := time.Parse(time.RFC3339, nodes[i]["createdAt"].(string))
			tj, _ := time.Parse(time.RFC3339, nodes[j]["createdAt"].(string))
			return ti.After(tj)
		})
		if nodes == nil {
			nodes = []map[string]interface{}{}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"issues": map[string]interface{}{
					"nodes": nodes,
					"pageInfo": map[string]interface{}{
						"hasNextPage": false,
						"endCursor":   nil,
					},
					"totalCount": len(nodes),
				},
			},
		})
	}
}

// extractAfter digs variables.filterBy.createdAt.after out of the adapter's
// GraphQL variables.
func extractAfter(variables map[string]interface{}) string {
	filterBy, _ := variables["filterBy"].(map[string]interface{})
	createdAt, _ := filterBy["createdAt"].(map[string]interface{})
	after, _ := createdAt["after"].(string)
	return after
}

// --- realistic fixtures -------------------------------------------------------

// testIssuesQuery is shaped like the issues query Wiz documents for pulling
// Issues over GraphQL. The adapter treats the query as an opaque string; the
// mock captures it so tests can assert it is sent verbatim.
const testIssuesQuery = `query IssuesTable($filterBy: IssueFilters, $first: Int, $after: String, $orderBy: IssueOrder) {
  issues: issuesV2(filterBy: $filterBy, first: $first, after: $after, orderBy: $orderBy) {
    nodes {
      id
      createdAt
      updatedAt
      status
      severity
      type
      sourceRule { id name }
      entitySnapshot { id type name cloudPlatform region subscriptionExternalId }
      projects { id name slug }
    }
    pageInfo { hasNextPage endCursor }
    totalCount
  }
}`

// realisticWizIssue returns a record shaped like a real Wiz Issue node:
// nested sourceRule/entitySnapshot objects, a projects array and mixed scalar
// types. All identifiers are clearly fake.
func realisticWizIssue(id, createdAt string) map[string]interface{} {
	return map[string]interface{}{
		"id":        id,
		"createdAt": createdAt,
		"updatedAt": createdAt,
		"status":    "OPEN",
		"severity":  "CRITICAL",
		"type":      "TOXIC_COMBINATION",
		"sourceRule": map[string]interface{}{
			"id":   "11111111-1111-1111-1111-111111111111",
			"name": "Publicly exposed VM with a highly privileged IAM role",
		},
		"entitySnapshot": map[string]interface{}{
			"id":                     "22222222-2222-2222-2222-222222222222",
			"type":                   "VIRTUAL_MACHINE",
			"name":                   "prod-web-01.example.com",
			"cloudPlatform":          "AWS",
			"region":                 "us-east-1",
			"subscriptionExternalId": "111111111111",
			"tags": map[string]interface{}{
				"env":  "production",
				"team": "platform",
			},
		},
		"projects": []interface{}{
			map[string]interface{}{
				"id":   "33333333-3333-3333-3333-333333333333",
				"name": "Example Project",
				"slug": "example-project",
			},
		},
		"notes": []interface{}{},
	}
}

// issueTime renders a fixture timestamp n minutes in the past. Fixtures use
// recent timestamps because the adapter's first poll filters to the last 24h.
func issueTime(minutesAgo int) string {
	return time.Now().UTC().Add(-time.Duration(minutesAgo) * time.Minute).Format(time.RFC3339)
}

const (
	testClientID     = "test-client-id"
	testClientSecret = "test-client-secret"
)

// testWizConfig returns a config pointed at the mock server, polling fast so
// tests stay quick.
func testWizConfig(t *testing.T, serverURL string) WizConfig {
	t.Helper()
	return WizConfig{
		ClientOptions: testClientOptions(t),
		ClientID:      testClientID,
		ClientSecret:  testClientSecret,
		URL:           serverURL + "/graphql",
		TokenURL:      serverURL + "/oauth/token",
		Query:         testIssuesQuery,
		Variables: map[string]interface{}{
			"first": 100,
			"filterBy": map[string]interface{}{
				"status": []interface{}{"OPEN", "IN_PROGRESS"},
			},
		},
		TimeField:    "createdAt",
		DataPath:     []string{"data", "issues", "nodes"},
		IDField:      "id",
		PollInterval: 25 * time.Millisecond,
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// --- tests ----------------------------------------------------------------------

// TestMockIssuesEndToEnd drives the adapter against the mock Wiz API and
// asserts the exact events shipped: verbatim payloads (nested objects and
// arrays included), the adapter's TimestampMs/EventType conventions, no
// re-shipping on later polls, and a cached token reused across polls.
func TestMockIssuesEndToEnd(t *testing.T) {
	mock := newMockWiz(testClientID, testClientSecret)
	want := []map[string]interface{}{
		realisticWizIssue("44444444-4444-4444-4444-000000000001", issueTime(30)),
		realisticWizIssue("44444444-4444-4444-4444-000000000002", issueTime(20)),
		realisticWizIssue("44444444-4444-4444-4444-000000000003", issueTime(10)),
	}
	mock.setIssues(want)

	server := mock.server(t)
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	startMs := uint64(time.Now().UnixMilli())
	adapter, _, err := newWizAdapter(ctx, testWizConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 issues to ship")

	// Re-polling must not re-ship: the time filter advances past the newest
	// shipped issue, so the count stays at 3 across many further polls.
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 30*time.Millisecond, "issues were re-shipped on a later poll")

	endMs := uint64(time.Now().UnixMilli())
	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		// The adapter does not set an EventType and stamps events with the
		// ship time (not the record's createdAt) -- pin both behaviors.
		assert.Empty(t, msg.EventType, "the wiz adapter does not tag an event type")
		assert.GreaterOrEqual(t, msg.TimestampMs, startMs)
		assert.LessOrEqual(t, msg.TimestampMs, endMs)

		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	// Payloads are shipped verbatim -- nested objects and arrays included.
	for _, src := range want {
		id := src["id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "issue %s was not shipped", id)
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original Wiz issue")
	}

	// The bearer token is cached and reused: with expires_in=3600 every poll
	// after the first reuses the same token.
	assert.Equal(t, 1, mock.tokenRequestCount(), "the token must be fetched once and cached")
	assert.GreaterOrEqual(t, mock.graphqlRequestCount(), 2, "multiple polls should have run")
}

// TestMockNewIssueMidRunShipsOnce verifies an issue appearing mid-run is
// picked up by the advancing time filter and shipped exactly once, without
// re-shipping anything already sent.
func TestMockNewIssueMidRunShipsOnce(t *testing.T) {
	mock := newMockWiz(testClientID, testClientSecret)
	mock.setIssues([]map[string]interface{}{
		realisticWizIssue("44444444-4444-4444-4444-00000000000a", issueTime(30)),
		realisticWizIssue("44444444-4444-4444-4444-00000000000b", issueTime(20)),
	})

	server := mock.server(t)
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newWizAdapter(ctx, testWizConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)

	// A new issue appears, strictly newer than everything shipped so far.
	mock.addIssue(realisticWizIssue("44444444-4444-4444-4444-00000000000c", issueTime(0)))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "the new issue should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 30*time.Millisecond)

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["id"].(string)]++
	}
	assert.Equal(t, map[string]int{
		"44444444-4444-4444-4444-00000000000a": 1,
		"44444444-4444-4444-4444-00000000000b": 1,
		"44444444-4444-4444-4444-00000000000c": 1,
	}, shippedPerID, "every issue must ship exactly once")
}

// TestMockGraphQLRequestShape verifies the request the adapter sends: the
// configured query verbatim, the configured variables preserved, and the
// filterBy.<time_field>.after watermark injected and advanced to the newest
// shipped issue's timestamp on later polls.
func TestMockGraphQLRequestShape(t *testing.T) {
	newestCreatedAt := issueTime(5)
	mock := newMockWiz(testClientID, testClientSecret)
	mock.setIssues([]map[string]interface{}{
		realisticWizIssue("44444444-4444-4444-4444-000000000001", issueTime(15)),
		realisticWizIssue("44444444-4444-4444-4444-000000000002", newestCreatedAt),
	})

	server := mock.server(t)
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newWizAdapter(ctx, testWizConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)

	// Wait for at least one post-shipment poll, whose "after" must be the
	// newest shipped issue's createdAt.
	require.Eventually(t, func() bool {
		_, variables := mock.capturedRequest()
		return extractAfter(variables) == newestCreatedAt
	}, 5*time.Second, 20*time.Millisecond, "the after watermark should advance to the newest issue")

	query, variables := mock.capturedRequest()
	assert.Equal(t, testIssuesQuery, query, "the configured GraphQL query must be sent verbatim")
	assert.Equal(t, float64(100), variables["first"], "configured variables must be preserved")

	filterBy, ok := variables["filterBy"].(map[string]interface{})
	require.True(t, ok, "filterBy must be an object")
	assert.Equal(t, []interface{}{"OPEN", "IN_PROGRESS"}, filterBy["status"],
		"configured filterBy entries must be preserved alongside the injected time filter")
}

// TestMockTokenRefreshOnExpiry verifies an expiring token is re-fetched and
// the new bearer token is used on subsequent GraphQL calls (the mock rejects
// any token but the most recently issued one).
func TestMockTokenRefreshOnExpiry(t *testing.T) {
	mock := newMockWiz(testClientID, testClientSecret)
	// expires_in=1s is within the adapter's one-minute refresh margin, so
	// every poll must fetch a fresh token.
	mock.expiresIn = 1
	mock.setIssues([]map[string]interface{}{
		realisticWizIssue("44444444-4444-4444-4444-000000000001", issueTime(10)),
	})

	server := mock.server(t)
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newWizAdapter(ctx, testWizConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 20*time.Millisecond, "the issue should ship")
	require.Eventually(t, func() bool { return mock.tokenIssueCount() >= 3 },
		5*time.Second, 20*time.Millisecond, "an expiring token must be re-fetched on later polls")

	// Polling kept working across token rotations (the mock 401s any stale
	// token, which would surface as re-ship failures or stalled polls).
	require.Never(t, func() bool { return sink.count() != 1 },
		300*time.Millisecond, 30*time.Millisecond)
}

// TestMockBadCredentialsShipsNothing pins the adapter's behavior on an OAuth
// failure: nothing ships, the GraphQL endpoint is never reached, and the
// adapter keeps retrying (it does not stop itself on auth errors).
func TestMockBadCredentialsShipsNothing(t *testing.T) {
	mock := newMockWiz(testClientID, "the-real-secret")
	mock.setIssues([]map[string]interface{}{
		realisticWizIssue("44444444-4444-4444-4444-000000000001", issueTime(10)),
	})

	server := mock.server(t)
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := testWizConfig(t, server.URL)
	conf.ClientSecret = "wrong-secret"
	adapter, chStopped, err := newWizAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return mock.tokenRequestCount() >= 3 },
		5*time.Second, 20*time.Millisecond, "the adapter should keep retrying the token exchange")

	assert.Equal(t, 0, sink.count(), "nothing must ship when authentication fails")
	assert.Equal(t, 0, mock.graphqlRequestCount(), "GraphQL must not be queried without a token")

	select {
	case <-chStopped:
		t.Fatal("the wiz adapter keeps polling on auth failure; it must not stop")
	default:
	}
}

// TestMockGraphQLErrorThenRecovery verifies a GraphQL errors envelope (HTTP
// 200, no data) ships nothing and does not kill the polling loop: once the
// API recovers, the pending issues ship.
func TestMockGraphQLErrorThenRecovery(t *testing.T) {
	mock := newMockWiz(testClientID, testClientSecret)
	mock.setGraphQLError(true)
	mock.setIssues([]map[string]interface{}{
		realisticWizIssue("44444444-4444-4444-4444-000000000001", issueTime(20)),
		realisticWizIssue("44444444-4444-4444-4444-000000000002", issueTime(10)),
	})

	server := mock.server(t)
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, chStopped, err := newWizAdapter(ctx, testWizConfig(t, server.URL), sink)
	require.NoError(t, err)
	defer adapter.Close()

	// While the API errors, polls happen but nothing ships and the adapter
	// stays alive.
	require.Eventually(t, func() bool { return mock.graphqlRequestCount() >= 3 },
		5*time.Second, 20*time.Millisecond)
	assert.Equal(t, 0, sink.count(), "nothing must ship while the API returns errors")
	select {
	case <-chStopped:
		t.Fatal("a GraphQL error must not stop the adapter")
	default:
	}

	// The API recovers; both issues ship exactly once.
	mock.setGraphQLError(false)
	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond, "issues should ship once the API recovers")
	require.Never(t, func() bool { return sink.count() != 2 },
		300*time.Millisecond, 30*time.Millisecond)
}
