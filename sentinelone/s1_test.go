package usp_sentinelone

import (
	"context"
	"encoding/json"
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

// TestAdapterStopsPermanentlyOnTransientError demonstrates the bug described in
// GitHub issue #3990: The SentinelOne adapter terminates permanently when it
// encounters transient API errors (e.g., 500 status codes from the server).
//
// This test verifies the CURRENT (buggy) behavior where a single 500 error
// causes the entire adapter to stop. Once the fix is implemented, this test
// should be updated to verify that the adapter retries on transient errors.
func TestAdapterStopsPermanentlyOnTransientError(t *testing.T) {
	var requestCount atomic.Int32
	var mu sync.Mutex
	var receivedErrors []error

	// Create a mock SentinelOne API server that returns 500 on the first request,
	// then would return success on subsequent requests (if there were any)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := requestCount.Add(1)
		if count == 1 {
			// First request: simulate transient server error (like SentinelOne error 5000010)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"errors":[{"code":5000010,"message":"Server could not process the request"}]}`))
			return
		}
		// Subsequent requests would succeed - but due to the bug, we never get here
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data":       []map[string]interface{}{},
			"nextCursor": nil,
		})
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := SentinelOneConfig{
		Domain:              server.URL,
		APIKey:              "test-api-key",
		URLs:                "/web/api/v2.1/activities", // Single endpoint for simpler testing
		TimeBetweenRequests: 100 * time.Millisecond,
		ClientOptions: uspclient.ClientOptions{
			TestSinkMode: true, // Use test sink to avoid needing real USP connection
			OnError: func(err error) {
				mu.Lock()
				receivedErrors = append(receivedErrors, err)
				mu.Unlock()
			},
			DebugLog: func(msg string) {
				t.Logf("DEBUG: %s", msg)
			},
		},
	}

	adapter, chStopped, err := NewSentinelOneAdapter(ctx, conf)
	require.NoError(t, err, "Failed to create adapter")
	require.NotNil(t, adapter, "Adapter should not be nil")

	// Wait for the adapter to stop (due to the bug, it should stop after the first 500 error)
	select {
	case <-chStopped:
		// Adapter stopped - this is the buggy behavior we're testing
		t.Log("Adapter stopped after encountering error")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for adapter to stop - unexpected behavior")
	}

	// Verify that only one request was made (bug: adapter didn't retry)
	finalRequestCount := requestCount.Load()
	assert.Equal(t, int32(1), finalRequestCount,
		"BUG CONFIRMED: Adapter made only %d request(s) and stopped permanently on transient 500 error. "+
			"Expected behavior would be to retry.", finalRequestCount)

	// Verify that doStop was set (bug: adapter terminated permanently)
	assert.True(t, adapter.doStop.IsSet(),
		"BUG CONFIRMED: doStop flag is set, meaning the adapter terminated permanently")

	// Verify an error was received
	mu.Lock()
	errorCount := len(receivedErrors)
	mu.Unlock()
	assert.Greater(t, errorCount, 0, "Should have received at least one error callback")

	// Clean up
	adapter.Close()
}

// TestAdapterShouldRetryOnTransientErrors is the test that SHOULD pass once
// the bug is fixed. Currently, this test will FAIL because the adapter
// doesn't implement retry logic.
//
// The fix should implement exponential backoff retry for transient errors:
// - 500, 502, 503, 504 status codes
// - Network timeouts
// - Connection refused (temporary)
func TestAdapterShouldRetryOnTransientErrors(t *testing.T) {
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
		ClientOptions: uspclient.ClientOptions{
			TestSinkMode: true,
			OnError: func(err error) {
				t.Logf("ERROR: %v", err)
			},
			DebugLog: func(msg string) {
				t.Logf("DEBUG: %s", msg)
			},
		},
	}

	adapter, chStopped, err := NewSentinelOneAdapter(ctx, conf)
	require.NoError(t, err, "Failed to create adapter")

	// Give the adapter time to make requests and potentially retry
	time.Sleep(3 * time.Second)

	// Check if adapter is still running (expected behavior after fix)
	select {
	case <-chStopped:
		finalCount := requestCount.Load()
		t.Fatalf("FAILED: Adapter stopped prematurely after %d request(s). "+
			"With retry logic, it should have retried and succeeded on the 3rd attempt.", finalCount)
	default:
		// Adapter is still running - this is the expected behavior after the fix
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

// TestTransientVsPermanentErrors documents which HTTP status codes should be
// considered transient (retry-worthy) vs. permanent errors.
// This test verifies current behavior and documents expected behavior after fix.
func TestTransientVsPermanentErrors(t *testing.T) {
	testCases := []struct {
		name            string
		statusCode      int
		isTransient     bool // true = should retry (after fix), false = permanent error
	}{
		// Transient errors - should retry after fix
		{"500_InternalServerError", 500, true},
		{"502_BadGateway", 502, true},
		{"503_ServiceUnavailable", 503, true},
		{"504_GatewayTimeout", 504, true},
		{"429_TooManyRequests", 429, true},
		// Permanent errors - should stop adapter (correct behavior)
		{"401_Unauthorized", 401, false},
		{"403_Forbidden", 403, false},
		{"404_NotFound", 404, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var requestCount atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count := requestCount.Add(1)
				if count == 1 {
					w.WriteHeader(tc.statusCode)
					w.Write([]byte(`{"error": "test error"}`))
					return
				}
				// Subsequent requests succeed
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"data":       []map[string]interface{}{},
					"nextCursor": nil,
				})
			}))
			defer server.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conf := SentinelOneConfig{
				Domain:              server.URL,
				APIKey:              "test-api-key",
				URLs:                "/web/api/v2.1/activities",
				TimeBetweenRequests: 50 * time.Millisecond,
				ClientOptions: uspclient.ClientOptions{
					TestSinkMode: true,
					OnError:      func(err error) {},
					DebugLog:     func(msg string) {},
				},
			}

			adapter, chStopped, err := NewSentinelOneAdapter(ctx, conf)
			require.NoError(t, err)

			// Wait to see if adapter retries or stops
			time.Sleep(500 * time.Millisecond)

			var adapterStopped bool
			select {
			case <-chStopped:
				adapterStopped = true
			default:
				adapterStopped = false
			}

			finalCount := requestCount.Load()

			// Current behavior: ALL errors stop the adapter (bug for transient errors)
			assert.True(t, adapterStopped,
				"Current behavior: adapter stops on status %d", tc.statusCode)
			assert.Equal(t, int32(1), finalCount,
				"Current behavior: only 1 request made before stopping on status %d", tc.statusCode)

			// Document what SHOULD happen after the fix:
			if tc.isTransient {
				t.Logf("BUG: Status %d is transient and SHOULD be retried, but adapter stopped after %d request(s)",
					tc.statusCode, finalCount)
			} else {
				t.Logf("CORRECT: Status %d is permanent error, adapter correctly stopped after %d request(s)",
					tc.statusCode, finalCount)
			}

			adapter.Close()
		})
	}
}

// TestAllEndpointsStopOnSingleError verifies the bug where one endpoint's
// error stops ALL endpoints (they share the same doStop event).
func TestAllEndpointsStopOnSingleError(t *testing.T) {
	var activitiesRequests atomic.Int32
	var alertsRequests atomic.Int32
	var threatsRequests atomic.Int32

	// Use a channel to ensure activities endpoint fails first
	activitiesFailed := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		switch path {
		case "/web/api/v2.1/activities":
			activitiesRequests.Add(1)
			// This endpoint returns 500 - it will kill ALL other endpoints due to shared doStop
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "transient error"}`))
			// Signal that activities has failed
			select {
			case <-activitiesFailed:
				// Already closed
			default:
				close(activitiesFailed)
			}
		case "/web/api/v2.1/cloud-detection/alerts":
			// Wait briefly to let activities fail first (reduces race condition)
			select {
			case <-activitiesFailed:
			case <-time.After(100 * time.Millisecond):
			}
			alertsRequests.Add(1)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data":       []map[string]interface{}{},
				"nextCursor": nil,
			})
		case "/web/api/v2.1/threats":
			// Wait briefly to let activities fail first (reduces race condition)
			select {
			case <-activitiesFailed:
			case <-time.After(100 * time.Millisecond):
			}
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
		TimeBetweenRequests: 100 * time.Millisecond,
		ClientOptions: uspclient.ClientOptions{
			TestSinkMode: true,
			OnError:      func(err error) { t.Logf("ERROR: %v", err) },
			DebugLog:     func(msg string) { t.Logf("DEBUG: %s", msg) },
		},
	}

	adapter, chStopped, err := NewSentinelOneAdapter(ctx, conf)
	require.NoError(t, err)

	// Wait for adapter to stop (due to the bug, it should stop quickly after activities fails)
	select {
	case <-chStopped:
		t.Log("Adapter stopped")
	case <-time.After(5 * time.Second):
		t.Log("Adapter still running after 5 seconds")
	}

	// Check request counts
	activities := activitiesRequests.Load()
	alerts := alertsRequests.Load()
	threats := threatsRequests.Load()

	t.Logf("Request counts - activities: %d, alerts: %d, threats: %d", activities, alerts, threats)

	// BUG VERIFICATION: One endpoint's error stops all endpoints
	assert.True(t, adapter.doStop.IsSet(),
		"BUG CONFIRMED: doStop is set, stopping ALL endpoints due to ONE endpoint's transient error")

	// The activities endpoint should have made exactly 1 request before failing
	assert.Equal(t, int32(1), activities,
		"Activities endpoint should have made exactly 1 request before failing")

	// Due to the bug, other endpoints are stopped. They may have made 0 or 1 requests
	// depending on timing, but they should NOT continue making multiple requests.
	assert.LessOrEqual(t, alerts, int32(1),
		"BUG: Alerts endpoint stopped after at most 1 request due to shared doStop")
	assert.LessOrEqual(t, threats, int32(1),
		"BUG: Threats endpoint stopped after at most 1 request due to shared doStop")

	adapter.Close()
}
