package usp_threatlocker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// HTTPError represents a non-2xx response from the ThreatLocker API. It carries
// the status code so callers can classify the error (retry vs. give up) without
// having to parse error strings.
type HTTPError struct {
	StatusCode int
	URL        string
	Body       string
}

func (e *HTTPError) Error() string {
	body := e.Body
	if len(body) > 512 {
		body = body[:512] + "..."
	}
	return fmt.Sprintf("unexpected status code %d for %q: %s", e.StatusCode, e.URL, body)
}

// isTransientError reports whether an error is worth retrying.
//
// Transient (retry):
//   - HTTP 5xx server errors
//   - HTTP 429 Too Many Requests (rate limiting)
//   - network errors (timeouts, connection refused, DNS failures, ...)
//
// Permanent (do not retry):
//   - HTTP 4xx other than 429 (bad request, auth failure, not found, ...)
//   - context cancellation (intentional shutdown)
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		if httpErr.StatusCode >= 500 && httpErr.StatusCode <= 599 {
			return true
		}
		if httpErr.StatusCode == http.StatusTooManyRequests {
			return true
		}
		return false
	}

	// Context cancellation is an intentional shutdown, not a transient blip.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Anything that failed before we got a response (DNS, dial, timeout, ...)
	// is reported by Do() and wrapped with this prefix; treat it as transient.
	if strings.Contains(err.Error(), "failed to execute request") {
		return true
	}

	return false
}

// ThreatLockerClient is a thin wrapper around the ThreatLocker Portal API.
//
// The Portal API is uniform: every queryable resource exposes a
// "<Resource>GetByParameters" endpoint that accepts a POST with a JSON body
// describing the filter, sort and pagination. This client therefore only needs
// a single generic POST helper, which keeps the adapter trivial to extend to
// new resources.
type ThreatLockerClient struct {
	baseURL      string
	apiKey       string
	managedOrgID string
	httpClient   *http.Client
}

// NewThreatLockerClient builds a client. baseURL is the API root, e.g.
// "https://portalapi.<instance>.threatlocker.com/portalapi".
func NewThreatLockerClient(baseURL, apiKey, managedOrgID string) *ThreatLockerClient {
	return &ThreatLockerClient{
		baseURL:      strings.TrimRight(baseURL, "/"),
		apiKey:       apiKey,
		managedOrgID: managedOrgID,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout: 10 * time.Second,
				}).Dial,
			},
		},
	}
}

// Post issues a POST to the given API path with a JSON body and returns the raw
// response body. A non-200 response is returned as an *HTTPError.
func (c *ThreatLockerClient) Post(ctx context.Context, path string, body interface{}) ([]byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %v", err)
	}

	url := c.baseURL + "/" + strings.TrimPrefix(path, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create request %q: %v", url, err)
	}

	// ThreatLocker authenticates with an API token passed verbatim in the
	// Authorization header (tokens are minted under Portal > API Users).
	req.Header.Set("Authorization", c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	// Optional: scope the request to a specific (often a child) organization.
	if c.managedOrgID != "" {
		req.Header.Set("managedOrganizationId", c.managedOrgID)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request %q: %v", url, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response %q: %v", url, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			URL:        url,
			Body:       string(respBody),
		}
	}

	return respBody, nil
}

// Close releases idle connections held by the underlying transport.
func (c *ThreatLockerClient) Close() {
	c.httpClient.CloseIdleConnections()
}
