package usp_withsecure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// HTTPError represents a non-2xx response from the Elements API. It carries the
// status code so callers can classify the error (retry vs. give up) without
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
//   - HTTP 429 Too Many Requests — the Elements API rate-limits the EDR
//     endpoints (devices, incidents, security events, audit logs) at 300
//     requests/minute per source IP, well below its 10,000/minute general
//     limit, so a busy tenant can legitimately meet a 429.
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

// isAuthError reports whether an error is an authentication failure, which the
// adapter treats as fatal (a poll retry cannot fix a bad credential).
func isAuthError(err error) bool {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == http.StatusUnauthorized
	}
	var tokErr *tokenError
	return errors.As(err, &tokErr) && tokErr.fatal
}

// WithSecureClient is a thin wrapper around the WithSecure Elements API.
//
// Everything the adapter needs is a GET or a form-encoded POST against
// https://api.connect.withsecure.com, authenticated with a cached OAuth2
// client-credentials bearer token.
type WithSecureClient struct {
	baseURL    string
	userAgent  string
	httpClient *http.Client
	tokens     *tokenSource
}

// NewWithSecureClient builds a client. baseURL is the API root, e.g.
// "https://api.connect.withsecure.com".
func NewWithSecureClient(baseURL, clientID, clientSecret, userAgent string) *WithSecureClient {
	base := strings.TrimRight(baseURL, "/")
	httpClient := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
		},
	}
	return &WithSecureClient{
		baseURL:    base,
		userAgent:  userAgent,
		httpClient: httpClient,
		tokens: &tokenSource{
			tokenURL:     base + tokenPath,
			clientID:     clientID,
			clientSecret: clientSecret,
			userAgent:    userAgent,
			httpClient:   httpClient,
		},
	}
}

// Get issues a GET with query parameters and returns the raw response body.
func (c *WithSecureClient) Get(ctx context.Context, path string, query url.Values) ([]byte, error) {
	reqURL := c.baseURL + "/" + strings.TrimPrefix(path, "/")
	if len(query) > 0 {
		reqURL += "?" + query.Encode()
	}
	return c.do(ctx, http.MethodGet, reqURL, nil)
}

// PostForm issues a POST with an application/x-www-form-urlencoded body. The
// security-events and missing-updates queries are POSTs that take form bodies
// rather than JSON — an easy detail to get wrong.
func (c *WithSecureClient) PostForm(ctx context.Context, path string, form url.Values) ([]byte, error) {
	reqURL := c.baseURL + "/" + strings.TrimPrefix(path, "/")
	return c.do(ctx, http.MethodPost, reqURL, form)
}

// do performs one authenticated request. A 401 is retried once with a freshly
// minted token: the Elements token lives only ~30 minutes, so a 401 mid-poll is
// far more likely to be an expiry race than a revoked credential.
func (c *WithSecureClient) do(ctx context.Context, method, reqURL string, form url.Values) ([]byte, error) {
	body, err := c.doOnce(ctx, method, reqURL, form)
	if err == nil {
		return body, nil
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusUnauthorized {
		c.tokens.invalidate()
		return c.doOnce(ctx, method, reqURL, form)
	}
	return nil, err
}

func (c *WithSecureClient) doOnce(ctx context.Context, method, reqURL string, form url.Values) ([]byte, error) {
	token, err := c.tokens.token(ctx)
	if err != nil {
		return nil, err
	}

	var payload io.Reader
	if form != nil {
		payload = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, reqURL, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to create request %q: %v", reqURL, err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	// The Elements API rejects any request without a User-Agent outright.
	req.Header.Set("User-Agent", c.userAgent)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request %q: %v", reqURL, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response %q: %v", reqURL, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			URL:        stripQuery(reqURL),
			Body:       string(respBody),
		}
	}
	return respBody, nil
}

// Close releases idle connections held by the underlying transport.
func (c *WithSecureClient) Close() {
	c.httpClient.CloseIdleConnections()
}

// stripQuery removes the query string from a URL for error messages, which are
// logged: the query carries organization and device identifiers.
func stripQuery(raw string) string {
	if i := strings.IndexByte(raw, '?'); i >= 0 {
		return raw[:i]
	}
	return raw
}

// OAuth2 client credentials
// ============================================================================

const (
	tokenPath = "/as/token.oauth2"

	// scopeRead is all this adapter needs — it only ever reads.
	scopeRead = "connect.api.read"

	// tokenExpiryBuffer renews a token this long before it actually expires so
	// an in-flight request never races the expiry boundary.
	tokenExpiryBuffer = 60 * time.Second

	// defaultTokenTTL is assumed when the token endpoint omits expires_in. The
	// documented lifetime is 1797 seconds.
	defaultTokenTTL = 25 * time.Minute
)

// tokenError is a failure from the token endpoint.
type tokenError struct {
	statusCode  int
	code        string
	description string
	// fatal marks a credential problem (as opposed to a transient outage) that
	// no amount of retrying will fix.
	fatal bool
}

func (e *tokenError) Error() string {
	if e.description != "" {
		return fmt.Sprintf("withsecure oauth token error %d %s: %s", e.statusCode, e.code, e.description)
	}
	return fmt.Sprintf("withsecure oauth token error %d %s", e.statusCode, e.code)
}

// tokenSource acquires and caches the client-credentials bearer token.
type tokenSource struct {
	tokenURL     string
	clientID     string
	clientSecret string
	userAgent    string
	httpClient   *http.Client

	mu        sync.Mutex
	value     string
	expiresAt time.Time
}

func (ts *tokenSource) invalidate() {
	ts.mu.Lock()
	ts.value = ""
	ts.expiresAt = time.Time{}
	ts.mu.Unlock()
}

func (ts *tokenSource) token(ctx context.Context) (string, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.value != "" && time.Until(ts.expiresAt) > tokenExpiryBuffer {
		return ts.value, nil
	}
	tok, ttl, err := ts.fetch(ctx)
	if err != nil {
		return "", err
	}
	ts.value = tok
	ts.expiresAt = time.Now().Add(ttl)
	return tok, nil
}

// fetch performs the client-credentials exchange.
//
// The credentials go in an HTTP Basic header and ONLY grant_type/scope may
// appear in the form body: the API documents that "the request will fail if
// parameters other than those allowed are sent in the payload", so the usual
// client_id/client_secret-in-the-body form that most OAuth servers tolerate is
// rejected here.
func (ts *tokenSource) fetch(ctx context.Context) (string, time.Duration, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("scope", scopeRead)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, fmt.Errorf("failed to create token request: %v", err)
	}
	req.SetBasicAuth(ts.clientID, ts.clientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", ts.userAgent)

	resp, err := ts.httpClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("failed to execute request %q: %v", ts.tokenURL, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("failed to read token response: %v", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", 0, parseTokenError(resp.StatusCode, respBody)
	}

	var tr struct {
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(respBody, &tr); err != nil {
		return "", 0, fmt.Errorf("failed to decode token response: %v", err)
	}
	if tr.AccessToken == "" {
		return "", 0, &tokenError{
			statusCode:  resp.StatusCode,
			code:        "no_access_token",
			description: "token endpoint returned no access_token",
			fatal:       true,
		}
	}
	ttl := time.Duration(tr.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = defaultTokenTTL
	}
	return tr.AccessToken, ttl, nil
}

// parseTokenError decodes the OAuth error envelope, falling back to the
// Elements {"message","code"} envelope the gateway sometimes returns instead.
func parseTokenError(status int, body []byte) *tokenError {
	te := &tokenError{statusCode: status}

	var oauthEnv struct {
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	if err := json.Unmarshal(body, &oauthEnv); err == nil && oauthEnv.Error != "" {
		te.code = oauthEnv.Error
		te.description = oauthEnv.ErrorDescription
		switch oauthEnv.Error {
		case "invalid_client", "unauthorized_client", "invalid_grant", "invalid_scope":
			te.fatal = true
		}
		return te
	}

	var apiEnv struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &apiEnv); err == nil && apiEnv.Message != "" {
		te.code = "token_request_failed"
		te.description = apiEnv.Message
	} else {
		te.code = "token_request_failed"
		te.description = string(body)
	}
	// A 4xx from the token endpoint is a credential problem; a 5xx is not.
	te.fatal = status >= 400 && status < 500
	return te
}
