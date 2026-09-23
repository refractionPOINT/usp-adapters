package usp_cloudflare_access

import (
	"context"
	"encoding/json"
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

// HTTPError represents a non-2xx response from the Cloudflare API. It carries
// the status code so callers can classify the error (retry vs. give up)
// without having to parse error strings.
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
//   - a well-formed response with "success": false (Cloudflare's own error
//     envelope -- typically a bad parameter or a scope/permission problem)
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

// cloudflareMessage is an entry in the Cloudflare API response envelope's
// "errors" or "messages" array.
type cloudflareMessage struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (m cloudflareMessage) String() string {
	if m.Code != 0 {
		return fmt.Sprintf("%d: %s", m.Code, m.Message)
	}
	return m.Message
}

// cloudflareEnvelope mirrors the standard Cloudflare API v4 response envelope.
type cloudflareEnvelope struct {
	Success  bool                `json:"success"`
	Errors   []cloudflareMessage `json:"errors"`
	Messages []cloudflareMessage `json:"messages"`
	Result   []json.RawMessage   `json:"result"`
}

func formatMessages(msgs []cloudflareMessage) string {
	if len(msgs) == 0 {
		return "no error details returned"
	}
	parts := make([]string, 0, len(msgs))
	for _, m := range msgs {
		parts = append(parts, m.String())
	}
	return strings.Join(parts, "; ")
}

// CloudflareAccessClient is a thin wrapper around the Cloudflare Access
// "per-request audit logs" API:
// https://developers.cloudflare.com/cloudflare-one/insights/logs/dashboard-logs/access-authentication-logs/#per-request-audit-logs
type CloudflareAccessClient struct {
	baseURL    string
	accountID  string
	apiToken   string
	httpClient *http.Client
}

// NewCloudflareAccessClient builds a client. baseURL is the API root, e.g.
// "https://api.cloudflare.com/client/v4".
func NewCloudflareAccessClient(baseURL, accountID, apiToken string) *CloudflareAccessClient {
	return &CloudflareAccessClient{
		baseURL:   strings.TrimRight(baseURL, "/"),
		accountID: accountID,
		apiToken:  apiToken,
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

// FetchAccessRequests issues one GET to
// /accounts/{account_id}/access/logs/access_requests for the half-open time
// range [since, until), walking newest-to-oldest is not attempted here --
// callers always request direction=asc so results page forward in time.
func (c *CloudflareAccessClient) FetchAccessRequests(ctx context.Context, since, until time.Time, limit int) ([]json.RawMessage, error) {
	reqURL := fmt.Sprintf("%s/accounts/%s/access/logs/access_requests", c.baseURL, url.PathEscape(c.accountID))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request %q: %v", reqURL, err)
	}

	q := req.URL.Query()
	q.Set("since", since.UTC().Format(time.RFC3339))
	q.Set("until", until.UTC().Format(time.RFC3339))
	q.Set("limit", strconv.Itoa(limit))
	// asc (oldest-first) is required so the adapter can walk forward through a
	// window that holds more records than fit in one response.
	q.Set("direction", "asc")
	req.URL.RawQuery = q.Encode()

	// The Cloudflare API authenticates with a scoped API token as a bearer
	// credential (Account -> Access: Audit Logs Read).
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request %q: %v", reqURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response %q: %v", reqURL, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			URL:        reqURL,
			Body:       string(body),
		}
	}

	var env cloudflareEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("invalid JSON response from %q: %v", reqURL, err)
	}
	if !env.Success {
		// Cloudflare can return HTTP 200 with "success": false to carry a
		// structured error (bad parameter, revoked token, missing scope, ...).
		// Treated as permanent -- isTransientError does not match a plain
		// error here, so the caller will not retry it.
		return nil, fmt.Errorf("cloudflare API returned success=false: %s", formatMessages(env.Errors))
	}

	return env.Result, nil
}

// Close releases idle connections held by the underlying transport.
func (c *CloudflareAccessClient) Close() {
	c.httpClient.CloseIdleConnections()
}
