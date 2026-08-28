package usp_cortex_xsoar

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// HTTPError represents a non-2xx response from the Cortex XSOAR API. It carries
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
//   - HTTP 429 Too Many Requests (XSOAR publishes no numeric rate limit, but the
//     standard throttling status is still honoured if the instance returns it)
//   - network errors (timeouts, connection refused, DNS failures, ...)
//
// Permanent (do not retry):
//   - HTTP 4xx other than 429 (bad request, auth failure, not found, ...). Note
//     a 401 from an *advanced* key can also be host-clock skew (the signature is
//     time-validated) -- still not worth retrying the same request, the operator
//     must fix the clock.
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

// XSOARClient is a thin wrapper around the Cortex XSOAR REST API. It knows how to
// authenticate (standard or advanced API keys, for both XSOAR 6 and 8) and how
// to POST a JSON body to an endpoint relative to the resolved API root.
//
// The same request shapes serve XSOAR 6 (on-prem, endpoints at the server root)
// and XSOAR 8 (cloud, endpoints under /xsoar/public/v1); only the base URL and
// the presence of the x-xdr-auth-id header differ, both captured at construction.
type XSOARClient struct {
	baseURL    string
	apiKey     string
	apiKeyID   string
	advanced   bool
	httpClient *http.Client
}

// NewXSOARClient builds a client. baseURL is the fully-resolved API root
// (including the /xsoar/public/v1 prefix for XSOAR 8). apiKeyID is the numeric
// key ID surfaced in the XSOAR API Keys table; it is required for XSOAR 8 and for
// advanced keys, and ignored for an XSOAR 6 standard key. When advanced is true
// the request is signed with the Cortex SHA-256 scheme.
func NewXSOARClient(baseURL, apiKey, apiKeyID string, advanced, insecure bool) *XSOARClient {
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: 10 * time.Second,
		}).Dial,
	}
	if insecure {
		// On-prem XSOAR 6 instances are frequently fronted by a self-signed
		// certificate; allow operators to opt out of verification explicitly.
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &XSOARClient{
		baseURL:  strings.TrimRight(baseURL, "/"),
		apiKey:   apiKey,
		apiKeyID: apiKeyID,
		advanced: advanced,
		httpClient: &http.Client{
			Timeout:   90 * time.Second,
			Transport: transport,
		},
	}
}

// Post issues a POST to the given API path with a JSON body and returns the raw
// response body. A non-2xx response is returned as an *HTTPError.
func (c *XSOARClient) Post(ctx context.Context, path string, body interface{}) ([]byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %v", err)
	}

	url := c.baseURL + "/" + strings.TrimPrefix(path, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create request %q: %v", url, err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if err := c.setAuthHeaders(req); err != nil {
		return nil, fmt.Errorf("failed to build auth headers: %v", err)
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

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			URL:        url,
			Body:       string(respBody),
		}
	}

	return respBody, nil
}

// setAuthHeaders applies the right authentication headers for the configured key.
//
// Standard key (XSOAR 6): the raw key in the Authorization header, nothing else.
// Standard key (XSOAR 8): the same, plus x-xdr-auth-id carrying the key's ID.
// Advanced key (either version): the Cortex signed scheme -- a random nonce and a
// millisecond timestamp are sent in x-xdr-nonce / x-xdr-timestamp, the key ID in
// x-xdr-auth-id, and Authorization carries the lowercase hex SHA-256 digest of
// apiKey + nonce + timestamp (plain SHA-256, not HMAC). The server recomputes the
// same digest to verify the request, which is why an advanced key is sensitive to
// the caller's clock.
func (c *XSOARClient) setAuthHeaders(req *http.Request) error {
	if !c.advanced {
		req.Header.Set("Authorization", c.apiKey)
		if c.apiKeyID != "" {
			req.Header.Set("x-xdr-auth-id", c.apiKeyID)
		}
		return nil
	}

	nonce, err := newNonce(64)
	if err != nil {
		return err
	}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	req.Header.Set("x-xdr-timestamp", timestamp)
	req.Header.Set("x-xdr-nonce", nonce)
	req.Header.Set("x-xdr-auth-id", c.apiKeyID)
	req.Header.Set("Authorization", signAdvanced(c.apiKey, nonce, timestamp))
	return nil
}

// signAdvanced computes the advanced-key Authorization value: the lowercase hex
// SHA-256 of the concatenation apiKey + nonce + timestamp, in that exact order.
func signAdvanced(apiKey, nonce, timestamp string) string {
	sum := sha256.Sum256([]byte(apiKey + nonce + timestamp))
	return hex.EncodeToString(sum[:])
}

const nonceAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// newNonce returns a cryptographically-random alphanumeric string of length n,
// matching the nonce shape the Cortex SDK generates for advanced keys.
func newNonce(n int) (string, error) {
	b := make([]byte, n)
	max := big.NewInt(int64(len(nonceAlphabet)))
	for i := range b {
		idx, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", fmt.Errorf("nonce generation failed: %v", err)
		}
		b[i] = nonceAlphabet[idx.Int64()]
	}
	return string(b), nil
}

// Close releases idle connections held by the underlying transport.
func (c *XSOARClient) Close() {
	c.httpClient.CloseIdleConnections()
}
