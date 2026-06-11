package usp_servicenow

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

// HTTPError represents a non-2xx response from the ServiceNow REST API. It
// carries the status code so callers can classify the error (retry vs. give
// up) without having to parse error strings, and the Retry-After delay when
// the instance rate-limits the request (HTTP 429).
type HTTPError struct {
	StatusCode int
	URL        string
	Body       string
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
//   - HTTP 429 Too Many Requests (an instance rate-limit rule fired)
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

// ServiceNowClient is a thin wrapper around the ServiceNow REST Table API
// (GET /api/now/v2/table/{tableName}). The v2 endpoint is used because it
// returns HTTP 200 with an empty result array when a valid query matches
// nothing, where v1 returns a misleading 404.
type ServiceNowClient struct {
	baseURL    string
	username   string
	password   string
	httpClient *http.Client
}

// NewServiceNowClient builds a client. baseURL is the instance root, e.g.
// "https://example.service-now.com".
func NewServiceNowClient(baseURL, username, password string) *ServiceNowClient {
	return &ServiceNowClient{
		baseURL:  strings.TrimRight(baseURL, "/"),
		username: username,
		password: password,
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

// GetTable fetches one page of a table. It returns the raw response body and
// whether the response advertises a further page via a Link rel="next"
// header. The Link header -- not the page's record count -- is the reliable
// end-of-data signal: ServiceNow applies sysparm_limit *before* ACL
// evaluation, so a page can legitimately come back short (or empty) while
// more records remain.
func (c *ServiceNowClient) GetTable(ctx context.Context, table string, params url.Values) ([]byte, bool, error) {
	u := fmt.Sprintf("%s/api/now/v2/table/%s?%s", c.baseURL, url.PathEscape(table), params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create request %q: %v", u, err)
	}

	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("failed to execute request %q: %v", u, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read response %q: %v", u, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, false, &HTTPError{
			StatusCode: resp.StatusCode,
			URL:        u,
			Body:       string(respBody),
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	}

	return respBody, hasNextLink(resp.Header), nil
}

// Close releases idle connections held by the underlying transport.
func (c *ServiceNowClient) Close() {
	c.httpClient.CloseIdleConnections()
}

// hasNextLink reports whether a response's Link header(s) advertise a
// rel="next" page, e.g.:
//
//	Link: <https://example.service-now.com/api/now/v2/table/sys_audit?sysparm_offset=100&sysparm_limit=100>;rel="next"
func hasNextLink(h http.Header) bool {
	for _, link := range h.Values("Link") {
		for _, part := range strings.Split(link, ",") {
			if strings.Contains(part, `rel="next"`) {
				return true
			}
		}
	}
	return false
}

// parseRetryAfter parses a Retry-After header value expressed in seconds (the
// form ServiceNow's rate limiter emits). An absent or unparseable value
// yields 0, meaning "no specific delay requested".
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	secs, err := strconv.Atoi(v)
	if err != nil || secs < 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}
