package usp_sentinelone

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/stretchr/testify/require"
)

func TestPagingContinuationSurvivesExhaustedRetries(t *testing.T) {
	var mu sync.Mutex
	var first url.Values
	var changed bool
	var continuationCalls int
	const stamp = "2026-01-01T00:00:01Z"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		q := r.URL.Query()
		if first == nil {
			first = q
		}
		if q.Get("cursor") == "" {
			if continuationCalls >= 3 {
				json.NewEncoder(w).Encode(map[string]interface{}{"data": []interface{}{}})
				return
			}
			if continuationCalls <= 2 && q.Encode() != first.Encode() {
				changed = true
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data":       []map[string]interface{}{{"id": "first", "createdAt": stamp}},
				"pagination": map[string]interface{}{"nextCursor": "tail"},
			})
			return
		}
		continuationCalls++
		for _, key := range []string{"createdAt__gte", "createdAt__gt", "createdAt__lte"} {
			if q.Get(key) != first.Get(key) {
				changed = true
			}
		}
		if continuationCalls <= 2 {
			w.WriteHeader(503)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data":       []map[string]interface{}{{"id": "tail", "createdAt": stamp}},
			"pagination": map[string]interface{}{"nextCursor": nil},
		})
	}))
	defer server.Close()
	sink := &memorySink{}
	a, _, err := newSentinelOneAdapter(context.Background(), SentinelOneConfig{
		Domain: server.URL, APIKey: "test-only", URLs: "/web/api/v2.1/activities",
		StartTime: "2026-01-01T00:00:00Z", TimeBetweenRequests: time.Millisecond,
		MaxRetryAttempts: 2, RetryBaseDelay: time.Millisecond, MaxRetryDelay: time.Millisecond,
		ClientOptions: uspclient.ClientOptions{TestSinkMode: true, OnError: func(error) {}, OnWarning: func(string) {}, DebugLog: func(string) {}},
	}, sink)
	require.NoError(t, err)
	defer a.Close()
	require.Eventually(t, func() bool {
		for _, m := range sink.byEventType("activities") {
			if m.JsonPayload["id"] == "tail" {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond)
	a.doStop.Set()
	a.wgSenders.Wait()
	mu.Lock()
	defer mu.Unlock()
	require.False(t, changed, "failed continuation must retain the original query bounds")
	require.GreaterOrEqual(t, continuationCalls, 3)
}

func TestPagingMalformedTimeDoesNotAdvanceBoundary(t *testing.T) {
	const stamp = "2026-01-01T00:00:01Z"
	var mu sync.Mutex
	var nextFloor string
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			json.NewEncoder(w).Encode(map[string]interface{}{"data": []map[string]interface{}{{"id": "valid", "createdAt": stamp}, {"id": "bad-time"}}})
			return
		}
		nextFloor = r.URL.Query().Get("createdAt__gt")
		json.NewEncoder(w).Encode(map[string]interface{}{"data": []interface{}{}})
	}))
	defer server.Close()
	sink := &memorySink{}
	a, _, err := newSentinelOneAdapter(context.Background(), SentinelOneConfig{
		Domain: server.URL, APIKey: "test-only", URLs: "/web/api/v2.1/activities", StartTime: "2026-01-01T00:00:00Z", TimeBetweenRequests: time.Millisecond,
		ClientOptions: uspclient.ClientOptions{TestSinkMode: true, OnError: func(error) {}, OnWarning: func(string) {}, DebugLog: func(string) {}},
	}, sink)
	require.NoError(t, err)
	defer a.Close()
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls >= 2 }, 5*time.Second, time.Millisecond)
	a.doStop.Set()
	a.wgSenders.Wait()
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, stamp, nextFloor, "local fallback clock must not advance the source window")
}
