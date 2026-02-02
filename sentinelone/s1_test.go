package usp_sentinelone

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAdapterRetriesOnTransientErrors verifies that the adapter retries
// when it encounters transient API errors (500, 502, 503, etc.) instead of
// terminating permanently.
// This test verifies the fix for GitHub issue #3990.
func TestAdapterRetriesOnTransientErrors(t *testing.T) {
	var requestCount atomic.Int32

	// Mock server that fails twice with 500, then succeeds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := requestCount.Add(1)
		if count <= 2 {
			// First two requests: transient errors
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"errors":[{"code":5000010,"message":"Server could not process the request"}]}`))
			return
		}
		// Third request: success with empty data
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data":       []map[string]interface{}{},
			"nextCursor": nil,
		})
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conf := SentinelOneConfig{
		Domain:              server.URL,
		APIKey:              "test-api-key",
		URLs:                "/web/api/v2.1/activities",
		TimeBetweenRequests: 100 * time.Millisecond,
		RetryBaseDelay:      50 * time.Millisecond, // Short delay for testing
		MaxRetryAttempts:    3,
		ClientOptions: uspclient.ClientOptions{
			TestSinkMode: true,
			OnError: func(err error) {
				t.Logf("ERROR: %v", err)
			},
			OnWarning: func(msg string) {
				t.Logf("WARNING: %s", msg)
			},
			DebugLog: func(msg string) {
				t.Logf("DEBUG: %s", msg)
			},
		},
	}

	adapter, chStopped, err := NewSentinelOneAdapter(ctx, conf)
	require.NoError(t, err, "Failed to create adapter")

	// Give the adapter time to make requests and retry
	time.Sleep(500 * time.Millisecond)

	// Check if adapter is still running (expected after fix)
	select {
	case <-chStopped:
		t.Fatal("Adapter stopped unexpectedly - it should retry on transient errors")
	default:
		// Adapter is still running - correct behavior
	}

	finalRequestCount := requestCount.Load()

	// After the fix, we expect at least 3 requests (2 failures + 1 success)
	assert.GreaterOrEqual(t, finalRequestCount, int32(3),
		"Expected at least 3 requests (2 retries + 1 success), got %d", finalRequestCount)

	// Verify adapter is still running (doStop should NOT be set)
	assert.False(t, adapter.doStop.IsSet(),
		"doStop should NOT be set - adapter should continue running after transient errors")

	adapter.Close()
}

// TestAdapterStopsOnPermanentErrors verifies that the adapter stops
// when it encounters permanent errors (401, 403, 404).
func TestAdapterStopsOnPermanentErrors(t *testing.T) {
	testCases := []struct {
		name       string
		statusCode int
	}{
		{"401_Unauthorized", 401},
		{"403_Forbidden", 403},
		{"404_NotFound", 404},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var requestCount atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestCount.Add(1)
				w.WriteHeader(tc.statusCode)
				w.Write([]byte(`{"error": "permanent error"}`))
			}))
			defer server.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conf := SentinelOneConfig{
				Domain:              server.URL,
				APIKey:              "test-api-key",
				URLs:                "/web/api/v2.1/activities",
				TimeBetweenRequests: 100 * time.Millisecond,
				RetryBaseDelay:      50 * time.Millisecond,
				MaxRetryAttempts:    3,
				ClientOptions: uspclient.ClientOptions{
					TestSinkMode: true,
					OnError:      func(err error) {},
					DebugLog:     func(msg string) {},
				},
			}

			adapter, chStopped, err := NewSentinelOneAdapter(ctx, conf)
			require.NoError(t, err)

			// Wait for adapter to stop
			select {
			case <-chStopped:
				// Adapter stopped - correct behavior for permanent errors
			case <-time.After(2 * time.Second):
				t.Fatalf("Adapter should have stopped on permanent error %d", tc.statusCode)
			}

			// Should have made exactly 1 request (no retries for permanent errors)
			assert.Equal(t, int32(1), requestCount.Load(),
				"Should make exactly 1 request for permanent error %d (no retries)", tc.statusCode)

			// doStop should be set
			assert.True(t, adapter.doStop.IsSet(),
				"doStop should be set for permanent error %d", tc.statusCode)

			adapter.Close()
		})
	}
}

// TestTransientErrorsAreRetried verifies that all transient HTTP status codes
// trigger retry behavior with proper delays.
func TestTransientErrorsAreRetried(t *testing.T) {
	testCases := []struct {
		name       string
		statusCode int
	}{
		{"500_InternalServerError", 500},
		{"502_BadGateway", 502},
		{"503_ServiceUnavailable", 503},
		{"504_GatewayTimeout", 504},
		{"429_TooManyRequests", 429},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Use two endpoints: one that fails with transient error, one that always succeeds
			// Compare their request counts to verify retry delays are being applied
			var failingEndpointRequests atomic.Int32
			var successEndpointRequests atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/web/api/v2.1/activities":
					// This endpoint fails once with transient error, then succeeds
					count := failingEndpointRequests.Add(1)
					if count == 1 {
						w.WriteHeader(tc.statusCode)
						w.Write([]byte(`{"error": "transient error"}`))
						return
					}
					w.WriteHeader(http.StatusOK)
					json.NewEncoder(w).Encode(map[string]interface{}{
						"data":       []map[string]interface{}{},
						"nextCursor": nil,
					})
				case "/web/api/v2.1/threats":
					// Reference endpoint - always succeeds immediately
					successEndpointRequests.Add(1)
					w.WriteHeader(http.StatusOK)
					json.NewEncoder(w).Encode(map[string]interface{}{
						"data":       []map[string]interface{}{},
						"nextCursor": nil,
					})
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conf := SentinelOneConfig{
				Domain:              server.URL,
				APIKey:              "test-api-key",
				URLs:                "/web/api/v2.1/activities,/web/api/v2.1/threats",
				TimeBetweenRequests: 50 * time.Millisecond,
				RetryBaseDelay:      100 * time.Millisecond, // Significant delay to show difference
				MaxRetryAttempts:    3,
				ClientOptions: uspclient.ClientOptions{
					TestSinkMode: true,
					OnError:      func(err error) {},
					OnWarning:    func(msg string) {},
					DebugLog:     func(msg string) {},
				},
			}

			adapter, chStopped, err := NewSentinelOneAdapter(ctx, conf)
			require.NoError(t, err)

			// Let adapter run - enough time for retry + several successful cycles
			time.Sleep(500 * time.Millisecond)

			// Adapter should still be running
			select {
			case <-chStopped:
				t.Fatalf("Adapter stopped on transient error %d - should have retried", tc.statusCode)
			default:
				// Still running - correct
			}

			failing := failingEndpointRequests.Load()
			success := successEndpointRequests.Load()

			t.Logf("Status %d: failing endpoint=%d, success endpoint=%d requests", tc.statusCode, failing, success)

			// The failing endpoint should have made at least 2 requests (1 fail + 1 success after retry)
			assert.GreaterOrEqual(t, failing, int32(2),
				"Failing endpoint should have retried on %d error", tc.statusCode)

			// Key ratio assertion: success endpoint should have more requests than failing endpoint
			// because the failing endpoint spent time in retry delay (100ms) on first request
			// In 500ms with 50ms between requests:
			// - Success: ~10 requests (500ms / 50ms)
			// - Failing: fewer because first cycle had 100ms retry delay
			assert.Greater(t, success, failing,
				"Success endpoint should have more requests than failing endpoint (retry delay effect)")

			// doStop should NOT be set
			assert.False(t, adapter.doStop.IsSet(),
				"doStop should NOT be set after transient error %d", tc.statusCode)

			adapter.Close()
		})
	}
}

// TestEndpointsContinueIndependently verifies that one endpoint's transient
// error doesn't stop other endpoints (fix for shared doStop issue).
func TestEndpointsContinueIndependently(t *testing.T) {
	var activitiesRequests atomic.Int32
	var alertsRequests atomic.Int32
	var threatsRequests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		switch path {
		case "/web/api/v2.1/activities":
			activitiesRequests.Add(1)
			// This endpoint always returns 500
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "transient error"}`))
		case "/web/api/v2.1/cloud-detection/alerts":
			alertsRequests.Add(1)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data":       []map[string]interface{}{},
				"nextCursor": nil,
			})
		case "/web/api/v2.1/threats":
			threatsRequests.Add(1)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data":       []map[string]interface{}{},
				"nextCursor": nil,
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := SentinelOneConfig{
		Domain:              server.URL,
		APIKey:              "test-api-key",
		URLs:                "", // Empty means default: activities, alerts, threats
		TimeBetweenRequests: 50 * time.Millisecond,
		RetryBaseDelay:      50 * time.Millisecond,
		MaxRetryAttempts:    3,
		ClientOptions: uspclient.ClientOptions{
			TestSinkMode: true,
			OnError:      func(err error) { t.Logf("ERROR: %v", err) },
			OnWarning:    func(msg string) { t.Logf("WARNING: %s", msg) },
			DebugLog:     func(msg string) {},
		},
	}

	adapter, chStopped, err := NewSentinelOneAdapter(ctx, conf)
	require.NoError(t, err)

	// Let the adapter run for a bit
	time.Sleep(1 * time.Second)

	// Adapter should still be running
	select {
	case <-chStopped:
		t.Fatal("Adapter stopped - other endpoints should continue despite activities endpoint failures")
	default:
		// Still running - correct
	}

	activities := activitiesRequests.Load()
	alerts := alertsRequests.Load()
	threats := threatsRequests.Load()

	t.Logf("Request counts - activities: %d, alerts: %d, threats: %d", activities, alerts, threats)

	// Key insight: alerts/threats should make MORE requests than activities because:
	// - Activities spends time in retry delays (50ms after attempt 1, 100ms after attempt 2)
	// - Total retry delay per cycle: 150ms (no delay after final attempt)
	// - Alerts/threats complete immediately and only wait TimeBetweenRequests (50ms)
	//
	// Expected in 1 second:
	// - Activities: ~9 requests (3 attempts × ~3 cycles, with ~200ms per cycle including delays)
	// - Alerts/Threats: ~20 requests (1000ms / 50ms between requests)
	//
	// If retry delays weren't working, activities would make as many requests as alerts.

	// Activities should have made at least 3 requests (one full retry cycle)
	assert.GreaterOrEqual(t, activities, int32(3),
		"Activities endpoint should have made at least one full retry cycle (3 attempts)")

	// Critical assertion: alerts/threats must make MORE requests than activities
	// This proves that activities is spending time in retry delays
	assert.Greater(t, alerts, activities,
		"Alerts should make more requests than activities (activities is delayed by retries)")
	assert.Greater(t, threats, activities,
		"Threats should make more requests than activities (activities is delayed by retries)")

	// doStop should NOT be set (transient errors don't stop the adapter)
	assert.False(t, adapter.doStop.IsSet(),
		"doStop should NOT be set - transient errors should not stop the adapter")

	adapter.Close()
}

// TestIsTransientError verifies the error classification logic.
func TestIsTransientError(t *testing.T) {
	testCases := []struct {
		name        string
		err         error
		isTransient bool
	}{
		// Transient HTTP errors (using HTTPError type)
		{"500 error", &HTTPError{StatusCode: 500, URL: "http://test", Body: "error"}, true},
		{"502 error", &HTTPError{StatusCode: 502, URL: "http://test", Body: "error"}, true},
		{"503 error", &HTTPError{StatusCode: 503, URL: "http://test", Body: "error"}, true},
		{"504 error", &HTTPError{StatusCode: 504, URL: "http://test", Body: "error"}, true},
		{"429 error", &HTTPError{StatusCode: 429, URL: "http://test", Body: "error"}, true},

		// Network error (string-based)
		{"network error", errors.New(`failed to execute request "http://test": connection refused`), true},

		// Permanent HTTP errors (using HTTPError type)
		{"401 error", &HTTPError{StatusCode: 401, URL: "http://test", Body: "unauthorized"}, false},
		{"403 error", &HTTPError{StatusCode: 403, URL: "http://test", Body: "forbidden"}, false},
		{"404 error", &HTTPError{StatusCode: 404, URL: "http://test", Body: "not found"}, false},
		{"400 error", &HTTPError{StatusCode: 400, URL: "http://test", Body: "bad request"}, false},

		// Edge cases
		{"nil error", nil, false},
		{"unknown error", errors.New("some random error"), false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isTransientError(tc.err)
			assert.Equal(t, tc.isTransient, result,
				"isTransientError(%v) = %v, want %v", tc.err, result, tc.isTransient)
		})
	}
}

// TestHTTPError verifies the HTTPError type behavior.
func TestHTTPError(t *testing.T) {
	t.Run("Error method format", func(t *testing.T) {
		err := &HTTPError{
			StatusCode: 500,
			URL:        "http://example.com/api",
			Body:       "internal server error",
		}
		expected := `unexpected status code 500 for "http://example.com/api": internal server error`
		assert.Equal(t, expected, err.Error())
	})

	t.Run("errors.As extraction", func(t *testing.T) {
		originalErr := &HTTPError{StatusCode: 503, URL: "http://test", Body: "unavailable"}
		wrappedErr := fmt.Errorf("GetFromAPI(): %w", originalErr)

		var httpErr *HTTPError
		assert.True(t, errors.As(wrappedErr, &httpErr), "should extract HTTPError from wrapped error")
		assert.Equal(t, 503, httpErr.StatusCode)
	})

	t.Run("implements error interface", func(t *testing.T) {
		var err error = &HTTPError{StatusCode: 404, URL: "http://test", Body: "not found"}
		assert.Contains(t, err.Error(), "404")
	})
}

// TestRetryExhaustion verifies that after exhausting all retries, the adapter
// continues (doesn't stop) but reports the error, with proper retry delays.
func TestRetryExhaustion(t *testing.T) {
	// Use two endpoints: one that always fails (exhausts retries), one that always succeeds
	// Compare request counts to verify retry delays are being applied
	var failingEndpointRequests atomic.Int32
	var successEndpointRequests atomic.Int32
	var mu sync.Mutex
	var capturedErrors []error

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/web/api/v2.1/activities":
			// Always fails - will exhaust retries
			failingEndpointRequests.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "always failing"}`))
		case "/web/api/v2.1/threats":
			// Reference endpoint - always succeeds
			successEndpointRequests.Add(1)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data":       []map[string]interface{}{},
				"nextCursor": nil,
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := SentinelOneConfig{
		Domain:              server.URL,
		APIKey:              "test-api-key",
		URLs:                "/web/api/v2.1/activities,/web/api/v2.1/threats",
		TimeBetweenRequests: 50 * time.Millisecond,
		RetryBaseDelay:      50 * time.Millisecond,
		MaxRetryAttempts:    3,
		ClientOptions: uspclient.ClientOptions{
			TestSinkMode: true,
			OnError: func(err error) {
				mu.Lock()
				capturedErrors = append(capturedErrors, err)
				mu.Unlock()
			},
			OnWarning: func(msg string) {},
			DebugLog:  func(msg string) {},
		},
	}

	adapter, chStopped, err := NewSentinelOneAdapter(ctx, conf)
	require.NoError(t, err)

	// Let adapter run for enough time to exhaust retries multiple times
	time.Sleep(1 * time.Second)

	// Adapter should still be running (not stopped by exhausted retries)
	select {
	case <-chStopped:
		t.Fatal("Adapter stopped after exhausting retries - should continue and try again later")
	default:
		// Still running - correct
	}

	failing := failingEndpointRequests.Load()
	success := successEndpointRequests.Load()

	t.Logf("Request counts - failing: %d, success: %d", failing, success)

	// Failing endpoint should have made multiple retry attempts
	// Each cycle: 3 attempts + delays (50+100=150ms, no delay after final attempt) + TimeBetweenRequests (50ms) ≈ 200ms
	// In 1 second: ~5 cycles × 3 attempts = ~15 requests
	assert.GreaterOrEqual(t, failing, int32(3),
		"Should have made at least one full retry cycle (3 attempts)")

	// Key ratio assertion: success endpoint should make MORE requests than failing endpoint
	// Success: ~20 requests (1000ms / 50ms between requests)
	// Failing: ~9 requests (delayed by retries)
	assert.Greater(t, success, failing,
		"Success endpoint should have more requests than failing endpoint (retry delays slow down failing endpoint)")

	// Should have received error callbacks about exhausted retries
	mu.Lock()
	hasExhaustionError := false
	for _, e := range capturedErrors {
		if e != nil && len(e.Error()) > 0 {
			hasExhaustionError = true
			break
		}
	}
	mu.Unlock()
	assert.True(t, hasExhaustionError, "Should have received error callback after exhausting retries")

	// doStop should NOT be set - adapter continues after exhausting retries
	assert.False(t, adapter.doStop.IsSet(),
		"doStop should NOT be set after exhausting retries - adapter should continue")

	adapter.Close()
}
