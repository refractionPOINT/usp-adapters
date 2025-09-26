package usp_sentinelone

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// SentinelOneClient represents a SentinelOne API SentinelOneClient
type SentinelOneClient struct {
	baseURL               string
	apiToken              string
	httpSentinelOneClient *http.Client
}

// NewSentinelOneClient creates a new SentinelOne API SentinelOneClient
func NewSentinelOneClient(baseURL, apiToken string) *SentinelOneClient {
	return &SentinelOneClient{
		baseURL:               strings.TrimRight(baseURL, "/"),
		apiToken:              apiToken,
		httpSentinelOneClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// SentinelOnePagedData represents pagination information
type SentinelOnePagedData struct {
	Data       []map[string]interface{} `json:"data"`
	TotalItems int                      `json:"totalItems"`
	NextCursor *string                  `json:"nextCursor"`
}

// GetFromAPI retrieves data from the API based on the provided options
func (c *SentinelOneClient) GetFromAPI(ctx context.Context, endpoint string, opts url.Values) (*SentinelOnePagedData, error) {
	const maxRetryDuration = 1 * time.Minute
	const initialDelay = 1 * time.Second
	const maxDelay = 10 * time.Second

	startTime := time.Now()
	delay := initialDelay
	reqURL := fmt.Sprintf("%s%s", c.baseURL, endpoint)

	for {
		req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request %q: %v", reqURL, err)
		}

		// Add query parameters
		req.URL.RawQuery = opts.Encode()

		// Add authentication
		req.Header.Set("Authorization", "Bearer "+c.apiToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpSentinelOneClient.Do(req)
		if err != nil {
			// Check if we should retry on timeout or connection errors
			if time.Since(startTime) < maxRetryDuration && isRetriableError(err) {
				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled during retry: %v", err)
				case <-time.After(delay):
					delay = delay * 2
					if delay > maxDelay {
						delay = maxDelay
					}
					continue
				}
			}
			return nil, fmt.Errorf("failed to execute request %q: %v", req.URL.String(), err)
		}

		// Read the body once and handle the response
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		if readErr != nil {
			if time.Since(startTime) < maxRetryDuration {
				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled during retry: %v", readErr)
				case <-time.After(delay):
					delay = delay * 2
					if delay > maxDelay {
						delay = maxDelay
					}
					continue
				}
			}
			return nil, fmt.Errorf("failed to read response body %q: %v", req.URL.String(), readErr)
		}

		// Check for retryable status codes (503, 504)
		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
			if time.Since(startTime) < maxRetryDuration {
				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled during retry for status %d: %s", resp.StatusCode, string(body))
				case <-time.After(delay):
					delay = delay * 2
					if delay > maxDelay {
						delay = maxDelay
					}
					continue
				}
			}
			// Retries exhausted
			return nil, fmt.Errorf("unexpected status code %d for %q after retries: %s", resp.StatusCode, req.URL.String(), string(body))
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("unexpected status code %d for %q: %s", resp.StatusCode, req.URL.String(), string(body))
		}

		var result SentinelOnePagedData
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, fmt.Errorf("failed to decode response %q: %v", req.URL.String(), err)
		}

		return &result, nil
	}
}

// isRetriableError checks if an error is retriable (timeout or temporary network error)
func isRetriableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "context deadline exceeded") ||
		strings.Contains(errStr, "Client.Timeout") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset")
}
