package usp_mimecast

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Bound pathological mocks independently of the production pagination guard.
// A removed guard must fail an assertion, rather than the package timeout.
func boundAuditRequests(a *MimecastAdapter, limit int) *atomic.Int32 {
	calls := &atomic.Int32{}
	inner := a.httpClient.Transport
	a.httpClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/api/audit/get-audit-events" && calls.Add(1) > int32(limit) {
			return nil, errors.New("mock audit request budget exhausted")
		}
		return inner.RoundTrip(r)
	})
	return calls
}

func TestOAuthLifetimeControlsProactiveRefresh(t *testing.T) {
	for _, tc := range []struct {
		name      string
		expires   int
		cachedFor time.Duration
	}{
		{"advertised 1800 seconds", 1800, 29 * time.Minute},
		{"advertised 120 seconds", 120, time.Minute},
		{"missing expiry uses conservative fallback", 0, 4 * time.Minute},
		{"short lifetime is not cached", 30, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock, a := newIdleMock(t)
			mock.setTokenExpiresIn(tc.expires)
			clock := usePollClock(a)
			origin := clock.current
			_, err := a.makeOneRequest()
			require.NoError(t, err)
			require.Equal(t, 1, mock.tokenCount())
			if tc.cachedFor > 0 {
				clock.current = origin.Add(tc.cachedFor - time.Second)
				_, err = a.makeOneRequest()
				require.NoError(t, err)
				assert.Equal(t, 1, mock.tokenCount(), "reuse the token until its safe lifetime expires")
			}
			beforeAudit := mock.auditCount()
			// Expire the server's copy exactly at the local refresh deadline. A late
			// refresh can recover via 401, but its EXTRA audit attempt is a defect.
			mock.rotateToken()
			clock.current = origin.Add(tc.cachedFor)
			_, err = a.makeOneRequest()
			require.NoError(t, err)
			assert.Equal(t, 2, mock.tokenCount(), "refresh at local expiry, including the safety margin")
			assert.Equal(t, beforeAudit+1, mock.auditCount(), "refresh proactively before sending an expired bearer token")
		})
	}
}

func TestEmptyOAuthTokenNeverReachesAuditEndpoint(t *testing.T) {
	mock := newMockMimecast("cid", "secret")
	handler := mock.handler(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			_, _ = io.WriteString(w, `{"access_token":"","expires_in":1800}`)
			return
		}
		handler(w, r)
	}))
	defer server.Close()
	opts, logs := recordingClientOptions(t)
	a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
	_, err := a.makeOneRequest()
	require.ErrorContains(t, err, "no access_token")
	assert.Zero(t, mock.auditCount(), "never send an empty bearer token")
	assert.Empty(t, logs.joinedErrors(), "an incomplete OAuth response must retain the session for retry")
	assert.Contains(t, logs.joinedWarnings(), "no access_token")
}

type interruptedOAuthBody struct {
	data   []byte
	err    error
	closed atomic.Bool
}

func (b *interruptedOAuthBody) Read(p []byte) (int, error) {
	n := copy(p, b.data)
	b.data = b.data[n:]
	if len(b.data) == 0 {
		return n, b.err
	}
	return n, nil
}
func (b *interruptedOAuthBody) Close() error { b.closed.Store(true); return nil }

func TestOAuthReadFailureRejectsEvenCompleteJSON(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	mock, a := newIdleMock(t, func(c *MimecastConfig) { c.ClientOptions = opts })
	usePollClock(a)
	readErr := errors.New("mock OAuth response interrupted")
	body := &interruptedOAuthBody{data: []byte(`{"access_token":"` + mockAccessToken + `","expires_in":1800}`), err: readErr}
	inner := a.httpClient.Transport
	a.httpClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/oauth/token" {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, Request: r}, nil
		}
		return inner.RoundTrip(r)
	})
	_, err := a.makeOneRequest()
	require.ErrorIs(t, err, readErr, "valid JSON bytes do not make a failed read successful")
	assert.Zero(t, mock.auditCount())
	assert.True(t, body.closed.Load())
	assert.Empty(t, a.token, "do not cache an incomplete token exchange")
	assert.Empty(t, logs.joinedErrors())
	assert.Contains(t, logs.joinedWarnings(), readErr.Error())
}

func TestExpiredTokenRefreshesOnPageTwo(t *testing.T) {
	mock := newMockMimecast("cid", "secret")
	handler := mock.handler(t)
	var calls atomic.Int32
	// The handler's request order is serial; record into a channel to keep
	// observations independent of HTTP goroutine scheduling.
	type request struct{ page, auth string }
	requests := make(chan request, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/audit/get-audit-events" {
			raw, err := io.ReadAll(r.Body)
			if !assert.NoError(t, err) {
				w.WriteHeader(500)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var req auditRequestBody
			if !assert.NoError(t, json.Unmarshal(raw, &req)) {
				w.WriteHeader(500)
				return
			}
			requests <- request{page: req.Meta.Pagination.PageToken, auth: r.Header.Get("Authorization")}
			handler(w, r)
			if calls.Add(1) == 1 {
				mock.rotateToken()
			}
			return
		}
		handler(w, r)
	}))
	defer server.Close()
	a := newIdleAdapter(t, server.URL, &captureSink{})
	clock := usePollClock(a)
	for i := 0; i < 120; i++ {
		mock.addEvent(fakeLogonEvent(fmt.Sprintf("event-%03d", i), clock.current.Add(-time.Minute)))
	}
	items, err := a.makeOneRequest()
	require.NoError(t, err)
	assert.Len(t, items, 120)
	assert.Len(t, dedupeStrings(itemIDs(items)), 120)
	assert.Equal(t, 2, mock.tokenCount(), "one refresh after page one must serve the rest of this poll")
	assert.Equal(t, 3, mock.auditCount())
	require.Len(t, requests, 3)
	first, stale, refreshed := <-requests, <-requests, <-requests
	assert.Equal(t, "", first.page)
	assert.Equal(t, "offset-100", stale.page)
	assert.Equal(t, stale.page, refreshed.page, "retry the failed page, not page one")
	assert.Equal(t, first.auth, stale.auth)
	assert.NotEqual(t, stale.auth, refreshed.auth)
}

type observedResponseBody struct {
	io.ReadCloser
	closed atomic.Bool
}

func (b *observedResponseBody) Close() error { b.closed.Store(true); return b.ReadCloser.Close() }

func TestEachResponseBodyClosesBeforeNextRequest(t *testing.T) {
	for _, failSecond := range []bool{false, true} {
		t.Run(fmt.Sprintf("second page fails=%v", failSecond), func(t *testing.T) {
			mock, a := newIdleMock(t)
			clock := usePollClock(a)
			for i := 0; i < 120; i++ {
				mock.addEvent(fakeLogonEvent(fmt.Sprintf("event-%03d", i), clock.current.Add(-time.Minute)))
			}
			if failSecond {
				mock.failNextAuditRequests(0, 503)
			}
			inner := a.httpClient.Transport
			var bodies []*observedResponseBody
			a.httpClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if len(bodies) > 0 && !bodies[len(bodies)-1].closed.Load() {
					return nil, errors.New("previous response body is still open before next request")
				}
				response, err := inner.RoundTrip(r)
				if err != nil {
					return nil, err
				}
				body := &observedResponseBody{ReadCloser: response.Body}
				response.Body = body
				bodies = append(bodies, body)
				return response, nil
			})
			items, err := a.makeOneRequest()
			if failSecond {
				require.Error(t, err)
				assert.Len(t, items, 100)
			} else {
				require.NoError(t, err)
				assert.Len(t, items, 120)
			}
			assert.Equal(t, 2, mock.auditCount(), "body lifetime must not block the next page")
			require.Len(t, bodies, 3, "token and two audit responses")
			for _, body := range bodies {
				assert.True(t, body.closed.Load(), "close successful and failing responses promptly")
			}
		})
	}
}

func TestDocumentedTimestampReachesLagAccounting(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	mock, a := newIdleMock(t, func(c *MimecastConfig) { c.ClientOptions = opts })
	clock := usePollClock(a)
	_, err := a.makeOneRequest() // A successful empty baseline is not lag evidence.
	require.NoError(t, err)
	event := fakeLogonEvent("documented-timestamp", clock.current.Add(-27*time.Minute))
	// Fixture rendering/filtering uses the documented contract independently
	// of production. A broken parser must reach the real adapter, not alter
	// which records the mock server returns.
	require.True(t, strings.HasSuffix(event["eventTime"].(string), "+0000"))
	mock.addEvent(event)
	items, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, event["eventTime"], items[0]["eventTime"])
	assert.Contains(t, logs.joinedWarnings(), "27m0s after their eventTime", "neither zero time nor discovery time can stand in for the vendor timestamp")
	assert.NotContains(t, logs.joinedWarnings(), "unparseable")
	items, err = a.makeOneRequest()
	require.NoError(t, err)
	assert.Empty(t, items, "timestamp parsing must not compromise discovery-based dedupe")
}

func TestPollCountsDescribeEveryWindow(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	mock, a := newIdleMock(t, func(c *MimecastConfig) { c.ClientOptions = opts })
	clock := usePollClock(a)
	for i := 0; i < 120; i++ {
		mock.addEvent(fakeLogonEvent(fmt.Sprintf("event-%03d", i), clock.current.Add(-time.Minute)))
	}
	_, err := a.makeOneRequest()
	require.NoError(t, err)
	_, err = a.makeOneRequest()
	require.NoError(t, err)
	debug := logs.joinedDebug()
	assert.Contains(t, debug, ": 2 pages, 120 records, 120 new, 120 tracked")
	assert.Contains(t, debug, ": 2 pages, 120 records, 0 new, 120 tracked")
}

func TestHTTPDiagnosticsExcludeResponseBody(t *testing.T) {
	const marker = "mock-response-body-must-not-be-logged"
	mock := newMockMimecast("cid", "secret")
	handler := mock.handler(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			handler(w, r)
			return
		}
		w.WriteHeader(503)
		_, _ = io.WriteString(w, marker)
	}))
	defer server.Close()
	opts, logs := recordingClientOptions(t)
	a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })
	_, err := a.makeOneRequest()
	require.Error(t, err)
	diagnostic := err.Error() + logs.joinedErrors() + logs.joinedWarnings() + logs.joinedDebug()
	assert.Contains(t, diagnostic, "HTTP status 503")
	assert.NotContains(t, diagnostic, marker)
	assert.NotContains(t, diagnostic, "REQUEST:")
	assert.NotContains(t, diagnostic, "RESPONSE:")
}
