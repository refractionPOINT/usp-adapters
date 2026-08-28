package usp_netskope

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// HTTPError represents a non-2xx response from the Netskope API. It carries the
// status code so callers can classify the error (retry vs. give up) without
// having to parse error strings, plus an optional RetryAfter hint extracted from
// the rate-limit response headers.
type HTTPError struct {
	StatusCode int
	URL        string
	Body       string
	// RetryAfter is the server's suggested wait before retrying, parsed from the
	// Retry-After or RateLimit-Reset response headers (Netskope sets these on a
	// 429). Zero when the server gave no hint.
	RetryAfter time.Duration
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
//   - HTTP 429 Too Many Requests (Netskope's per-endpoint rate limit is 4 req/s)
//   - network errors (timeouts, connection refused, DNS failures, ...)
//
// Permanent (do not retry):
//   - HTTP 4xx other than 429 (bad request, auth failure, not found, ...). In
//     particular a 401/403 means the API token is invalid or is not scoped to
//     this dataexport endpoint -- retrying cannot help.
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

	// Anything that failed before we got a response (DNS, dial, timeout, ...) is
	// reported by Do() and wrapped with this prefix; treat it as transient.
	if strings.Contains(err.Error(), "failed to execute request") {
		return true
	}

	return false
}

// NetskopeClient is a thin wrapper around the Netskope REST API v2 dataexport
// iterator endpoints. Those endpoints are uniform: a GET to
// .../api/v2/events/dataexport/{events|alerts}/{type} with an "index" (the
// server-side cursor name) and an "operation" query parameter.
type NetskopeClient struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// NewNetskopeClient builds a client. baseURL is the API root, e.g.
// "https://<tenant>.goskope.com/api/v2".
func NewNetskopeClient(baseURL, token string) *NetskopeClient {
	return &NetskopeClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		httpClient: &http.Client{
			// Pages carry up to 10,000 records; allow a generous read budget.
			Timeout: 120 * time.Second,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout: 10 * time.Second,
				}).Dial,
			},
		},
	}
}

// Get issues a GET to the given API path (relative to the API root) with the
// supplied query parameters and returns the raw response body. A non-200
// response is returned as an *HTTPError.
func (c *NetskopeClient) Get(ctx context.Context, path string, query url.Values) ([]byte, error) {
	u := c.baseURL + "/" + strings.TrimPrefix(path, "/")
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request %q: %v", u, err)
	}

	// Netskope v2 authenticates with the API token in a custom header (no Bearer
	// prefix). The token is minted under Settings > Tools > REST API v2 and is
	// scoped per-endpoint. Header name is case-insensitive; we send the casing
	// the official SDK uses.
	req.Header.Set("Netskope-Api-Token", c.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "LimaCharlie-USP-Adapter")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request %q: %v", u, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response %q: %v", u, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			URL:        u,
			Body:       string(respBody),
			RetryAfter: retryAfter(resp.Header),
		}
	}

	return respBody, nil
}

// Close releases idle connections held by the underlying transport.
func (c *NetskopeClient) Close() {
	c.httpClient.CloseIdleConnections()
}

// retryAfter extracts a retry delay from rate-limit response headers. Netskope
// returns "Retry-After" (seconds) on a 429 and also exposes "RateLimit-Reset"
// (seconds until the window resets); either is honoured, Retry-After first.
func retryAfter(h http.Header) time.Duration {
	for _, name := range []string{"Retry-After", "RateLimit-Reset", "Ratelimit-Reset"} {
		if v := strings.TrimSpace(h.Get(name)); v != "" {
			if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
				return time.Duration(secs) * time.Second
			}
		}
	}
	return 0
}
