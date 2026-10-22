package usp_proofpoint_isolation

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

// usageDataPath is the Reporting API endpoint, relative to a product's base URL.
// It is the same for Browser/Email Isolation and URL Isolation; only the host
// differs. V2 differs from V1 only in the JSON it returns (V2 adds new data
// elements as they become available), not in the request interface.
const usageDataPath = "/api/v2/reporting/usage-data"

// queryTimestampLayout is the format of the from/to query parameters: UTC,
// millisecond precision, no zone suffix (as Proofpoint's own iso2web tool
// sends them).
const queryTimestampLayout = "2006-01-02T15:04:05.000"

// HTTPError represents a non-2xx response from the Reporting API. It carries
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

// usageDataRequest is one call to the usage-data endpoint. The first call of a
// query carries only From/To/PageSize; the API answers with a jobId, and every
// follow-up call (polling the job, then walking its pages) echoes that JobID
// and the latest PageToken.
type usageDataRequest struct {
	From      time.Time
	To        time.Time
	PageSize  int
	JobID     string
	PageToken string
}

// usageDataResponse is the Reporting API response envelope. Records are kept
// raw so the adapter can decode them with integer precision intact and ship
// them verbatim.
type usageDataResponse struct {
	JobID     string
	Status    string
	PageToken string
	Data      []json.RawMessage
}

// IsoClient is a thin wrapper around one product's Reporting API.
type IsoClient struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewIsoClient builds a client. baseURL is the product root, e.g.
// "https://proofpointisolation.com" or "https://urlisolation.com".
func NewIsoClient(baseURL, apiKey string) *IsoClient {
	return &IsoClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: 2 * time.Minute,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout: 10 * time.Second,
				}).Dial,
			},
		},
	}
}

// GetUsageData issues one GET to the usage-data endpoint. A non-200 response is
// returned as an *HTTPError.
func (c *IsoClient) GetUsageData(ctx context.Context, r usageDataRequest) (*usageDataResponse, error) {
	q := url.Values{}
	q.Set("from", r.From.UTC().Format(queryTimestampLayout))
	q.Set("to", r.To.UTC().Format(queryTimestampLayout))
	q.Set("pageSize", strconv.Itoa(r.PageSize))
	if r.JobID != "" {
		q.Set("jobId", r.JobID)
	}
	if r.PageToken != "" {
		q.Set("pageToken", r.PageToken)
	}
	endpoint := c.baseURL + usageDataPath
	reqURL := endpoint + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request %q: %v", endpoint, err)
	}
	// The Reporting API key is sent as a bearer token. The API also accepts it
	// as a ?key= query parameter, but a header keeps it out of URLs and logs.
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Report the endpoint without its query string; the error from Do()
		// embeds the full URL, which is harmless (no key) but noisy.
		return nil, fmt.Errorf("failed to execute request %q: %v", endpoint, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response %q: %v", endpoint, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			URL:        endpoint,
			Body:       string(body),
		}
	}

	return parseUsageDataResponse(body)
}

// Close releases idle connections held by the underlying transport.
func (c *IsoClient) Close() {
	c.httpClient.CloseIdleConnections()
}

// parseUsageDataResponse decodes the response envelope. jobId and pageToken are
// treated as opaque: they are echoed back verbatim whether the API sends them
// as strings or numbers.
func parseUsageDataResponse(body []byte) (*usageDataResponse, error) {
	var env map[string]json.RawMessage
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("invalid JSON response: %v", err)
	}

	out := &usageDataResponse{
		JobID:     rawScalar(env["jobId"]),
		Status:    rawScalar(env["status"]),
		PageToken: rawScalar(env["pageToken"]),
	}
	if raw, ok := env["data"]; ok && !isJSONNull(raw) {
		if err := json.Unmarshal(raw, &out.Data); err != nil {
			return nil, fmt.Errorf("response field \"data\" is not an array: %v", err)
		}
	}
	return out, nil
}

// rawScalar renders a JSON scalar as a string: strings are unquoted, numbers
// and booleans keep their literal text, null/absent become "".
func rawScalar(raw json.RawMessage) string {
	if len(raw) == 0 || isJSONNull(raw) {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return strings.TrimSpace(string(raw))
}

func isJSONNull(raw json.RawMessage) bool {
	return strings.TrimSpace(string(raw)) == "null"
}
