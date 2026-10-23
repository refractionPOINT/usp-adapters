package usp_zendesk

import (
	"context"
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

type pageRoundTripper func(*http.Request) (*http.Response, error)

func (f pageRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type brokenPageBody struct{}

func (brokenPageBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (brokenPageBody) Close() error             { return nil }

func twoPageAPI(t *testing.T) http.Handler {
	mock := newMockZendesk(mockEmail, mockToken)
	base := time.Now().Add(-time.Minute)
	for i := 0; i < 150; i++ {
		mock.addLog(auditLogFixture(fmt.Sprintf("%d", 10000+i), base, "update"))
	}
	return mock.handler(t)
}

// Successful pages must be returned even when fetching a later page fails.
// Retrying the same window must deliver every ID once across both results.
func TestPaginationPreservesPartialResults(t *testing.T) {
	for _, fault := range []string{"status", "decode", "network", "read", "request", "shutdown"} {
		t.Run(fault, func(t *testing.T) {
			server := httptest.NewServer(twoPageAPI(t))
			defer server.Close()
			a := newIdleAdapter(t, server.URL, &captureSink{})
			transport := a.httpClient.Transport
			failed := false
			a.httpClient.Transport = pageRoundTripper(func(r *http.Request) (*http.Response, error) {
				if fault == "request" && !failed {
					resp, err := transport.RoundTrip(r)
					if err != nil {
						return nil, err
					}
					var body map[string]interface{}
					decodeErr := json.NewDecoder(resp.Body).Decode(&body)
					resp.Body.Close()
					if decodeErr != nil {
						return nil, decodeErr
					}
					body["links"].(map[string]interface{})["next"] = ":invalid-next-url"
					data, marshalErr := json.Marshal(body)
					if marshalErr != nil {
						return nil, marshalErr
					}
					resp.Body = io.NopCloser(strings.NewReader(string(data)))
					failed = true
					return resp, nil
				}
				if fault == "shutdown" && !failed {
					resp, err := transport.RoundTrip(r)
					failed = true
					a.doStop.Set()
					return resp, err
				}
				if r.URL.Query().Get("page[after]") != "" && !failed {
					failed = true
					if fault == "network" {
						return nil, errors.New("transient connection failure")
					}
					resp := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{")), Header: make(http.Header)}
					if fault == "read" {
						resp.Body = brokenPageBody{}
					}
					if fault == "status" {
						resp.StatusCode = http.StatusServiceUnavailable
						resp.Status = "503 Service Unavailable"
					}
					return resp, nil
				}
				return transport.RoundTrip(r)
			})
			since := time.Now().Add(-24 * time.Hour)
			first, _, err := a.makeOneRequest(since)
			require.Error(t, err)
			require.True(t, failed)
			require.Len(t, first, 100, "page 1 must survive the later failure")
			if fault == "shutdown" {
				require.ErrorIs(t, err, context.Canceled)
			}
			if fault == "shutdown" || fault == "request" {
				a.doStop.Clear()
			}
			second, _, err := a.makeOneRequest(since)
			require.NoError(t, err)
			require.Len(t, second, 50, "only the remaining page should be new on retry")
			counts := map[string]int{}
			for _, item := range append(first, second...) {
				counts[recordID(item["id"])]++
			}
			require.Len(t, counts, 150)
			for id, count := range counts {
				assert.Equal(t, 1, count, "ID %s", id)
			}
		})
	}
}

// Exercise the real poll loop: page 1 must ship before a failing page can
// recover, and must also drain when Close interrupts a paginated poll.
func TestFetchEventsShipsPartialResults(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprintf("shutdown=%t", shutdown), func(t *testing.T) {
			handler := twoPageAPI(t)
			var retryAllowed atomic.Bool
			pageStarted := make(chan struct{}, 1)
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("page[after]") != "" && !retryAllowed.Load() {
					if shutdown {
						pageStarted <- struct{}{}
						<-release
					}
					http.Error(w, "transient failure", http.StatusServiceUnavailable)
					return
				}
				handler.ServeHTTP(w, r)
			}))
			defer server.Close()
			sink := &captureSink{}
			a, _ := startMockAdapter(t, server.URL, sink)
			if shutdown {
				select {
				case <-pageStarted:
				case <-time.After(3 * time.Second):
					t.Fatal("second page was not requested")
				}
				closed := make(chan error, 1)
				go func() { closed <- a.Close() }()
				require.Eventually(t, a.doStop.IsSet, time.Second, time.Millisecond)
				close(release)
				select {
				case err := <-closed:
					require.NoError(t, err)
				case <-time.After(3 * time.Second):
					t.Fatal("Close did not drain partial results")
				}
				require.Equal(t, 100, sink.count())
				return
			}
			defer a.Close()
			require.Eventually(t, func() bool { return sink.count() == 100 }, 3*time.Second, 5*time.Millisecond,
				"successful page must ship while page 2 is still failing")
			retryAllowed.Store(true)
			require.Eventually(t, func() bool { return sink.count() == 150 }, 3*time.Second, 5*time.Millisecond)
			counts := map[string]int{}
			for _, msg := range sink.snapshot() {
				item := msg.JsonPayload
				counts[recordID(item["id"])]++
			}
			require.Len(t, counts, 150)
			for id, count := range counts {
				assert.Equal(t, 1, count, "ID %s", id)
			}
		})
	}
}
