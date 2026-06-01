package usp_quickbooks

import (
	"context"
	"encoding/base64"
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

// HTTPError represents a non-2xx response from the QuickBooks Online API. It
// carries the status code so callers can classify the error (retry vs. give up)
// without parsing error strings.
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

// TokenError wraps a failure to obtain an OAuth access token from the refresh
// token. It lets the adapter tell a credential problem (fatal) apart from a
// data-request error, while still allowing isTransientError to see through to
// the underlying cause (a 5xx/network blip refreshing the token is retryable).
type TokenError struct{ Err error }

func (e *TokenError) Error() string { return "token refresh failed: " + e.Err.Error() }
func (e *TokenError) Unwrap() error { return e.Err }

// isTransientError reports whether an error is worth retrying.
//
// Transient (retry):
//   - HTTP 5xx server errors
//   - HTTP 429 Too Many Requests (QuickBooks throttles at 500 req/min/realm)
//   - network errors (timeouts, connection refused, DNS failures, ...)
//
// Permanent (do not retry):
//   - HTTP 4xx other than 429 (bad request, not found, ...)
//   - HTTP 401 is handled separately by the client (a token refresh + retry),
//     so by the time it surfaces here it is non-transient.
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
	// is wrapped with this prefix by the request helpers; treat it as transient.
	if strings.Contains(err.Error(), "failed to execute request") {
		return true
	}

	return false
}

// tokenResponse is the JSON body returned by Intuit's OAuth 2.0 token endpoint
// for a refresh_token grant.
//
// Reference:
// https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization/oauth-2.0
type tokenResponse struct {
	AccessToken           string `json:"access_token"`
	RefreshToken          string `json:"refresh_token"`
	TokenType             string `json:"token_type"`
	ExpiresIn             int    `json:"expires_in"`                 // access token lifetime, seconds (3600)
	RefreshTokenExpiresIn int    `json:"x_refresh_token_expires_in"` // refresh token lifetime, seconds
}

// QuickBooksClient wraps the QuickBooks Online Accounting API. It owns the
// OAuth 2.0 access token: it mints one from the configured refresh token on
// first use and silently re-mints it when it nears expiry (or when the API
// rejects it with a 401).
//
// QuickBooks access tokens live for one hour; refresh tokens are long-lived but
// MAY rotate on a refresh call. The client always adopts the newest refresh
// token returned by the token endpoint so a rotation does not break a
// long-running collector mid-flight.
type QuickBooksClient struct {
	baseURL      string
	tokenURL     string
	clientID     string
	clientSecret string
	realmID      string
	minorVersion string
	httpClient   *http.Client

	mu           sync.Mutex
	accessToken  string
	tokenExpiry  time.Time
	refreshToken string

	// onRefreshToken, when set, is invoked with the latest refresh token each
	// time the token endpoint returns one. It lets an embedder persist a
	// rotated token. It is never invoked with an empty value.
	onRefreshToken func(string)
}

// tokenRefreshBuffer is how long before an access token's stated expiry the
// client proactively refreshes it, to avoid racing the boundary.
const tokenRefreshBuffer = 5 * time.Minute

// NewQuickBooksClient builds a client. baseURL is the API root (e.g.
// "https://quickbooks.api.intuit.com"); tokenURL is the OAuth 2.0 token
// endpoint.
func NewQuickBooksClient(baseURL, tokenURL, clientID, clientSecret, realmID, minorVersion, refreshToken string) *QuickBooksClient {
	return &QuickBooksClient{
		baseURL:      strings.TrimRight(baseURL, "/"),
		tokenURL:     tokenURL,
		clientID:     clientID,
		clientSecret: clientSecret,
		realmID:      realmID,
		minorVersion: minorVersion,
		refreshToken: refreshToken,
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

// refreshAccessToken exchanges the stored refresh token for a fresh access
// token. The caller must hold c.mu.
//
// The request is a standard OAuth 2.0 refresh_token grant: an
// application/x-www-form-urlencoded body POSTed to the token endpoint, with the
// client id/secret presented via HTTP Basic auth.
func (c *QuickBooksClient) refreshAccessToken(ctx context.Context) error {
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", c.refreshToken)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create token request: %v", err)
	}
	basic := base64.StdEncoding.EncodeToString([]byte(c.clientID + ":" + c.clientSecret))
	req.Header.Set("Authorization", "Basic "+basic)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request %q: %v", c.tokenURL, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read token response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return &HTTPError{StatusCode: resp.StatusCode, URL: c.tokenURL, Body: string(respBody)}
	}

	var tok tokenResponse
	if err := json.Unmarshal(respBody, &tok); err != nil {
		return fmt.Errorf("failed to parse token response: %v", err)
	}
	if tok.AccessToken == "" {
		return errors.New("token endpoint returned an empty access_token")
	}

	c.accessToken = tok.AccessToken
	lifetime := time.Duration(tok.ExpiresIn) * time.Second
	if lifetime <= 0 {
		// Intuit's documented access-token lifetime is one hour; fall back to
		// it if the response omits expires_in.
		lifetime = time.Hour
	}
	c.tokenExpiry = time.Now().Add(lifetime)

	// Adopt a rotated refresh token if one came back. Intuit may return the
	// same value or a new one; persisting the latest is mandatory.
	if tok.RefreshToken != "" && tok.RefreshToken != c.refreshToken {
		c.refreshToken = tok.RefreshToken
		if c.onRefreshToken != nil {
			c.onRefreshToken(tok.RefreshToken)
		}
	}
	return nil
}

// ensureToken returns a valid access token, refreshing it if it is missing or
// within tokenRefreshBuffer of expiry. force re-mints unconditionally (used
// after a 401).
func (c *QuickBooksClient) ensureToken(ctx context.Context, force bool) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if force || c.accessToken == "" || time.Now().After(c.tokenExpiry.Add(-tokenRefreshBuffer)) {
		if err := c.refreshAccessToken(ctx); err != nil {
			return "", &TokenError{Err: err}
		}
	}
	return c.accessToken, nil
}

// CurrentRefreshToken returns the latest refresh token the client holds (which
// may differ from the one it was constructed with, if Intuit rotated it).
func (c *QuickBooksClient) CurrentRefreshToken() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.refreshToken
}

// GetCDC issues a ChangeDataCapture request for the given entities and returns
// the raw response body. entities is the comma-separated entity list;
// changedSince is the lower bound of the change window.
//
// On an HTTP 401 the client refreshes the access token once and retries, so a
// token that expired between two polls does not surface as an error.
//
// Reference:
// https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/changedatacapture
func (c *QuickBooksClient) GetCDC(ctx context.Context, entities string, changedSince time.Time) ([]byte, error) {
	body, err := c.doCDC(ctx, entities, changedSince, false)
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusUnauthorized {
		// The access token was rejected; force a refresh and retry once.
		return c.doCDC(ctx, entities, changedSince, true)
	}
	return body, err
}

func (c *QuickBooksClient) doCDC(ctx context.Context, entities string, changedSince time.Time, forceToken bool) ([]byte, error) {
	token, err := c.ensureToken(ctx, forceToken)
	if err != nil {
		return nil, err
	}

	q := url.Values{}
	q.Set("entities", entities)
	// QuickBooks accepts ISO 8601 / RFC 3339 for changedSince.
	q.Set("changedSince", changedSince.UTC().Format(time.RFC3339))
	if c.minorVersion != "" {
		q.Set("minorversion", c.minorVersion)
	}

	reqURL := fmt.Sprintf("%s/v3/company/%s/cdc?%s", c.baseURL, url.PathEscape(c.realmID), q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request %q: %v", reqURL, err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request %q: %v", reqURL, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response %q: %v", reqURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &HTTPError{StatusCode: resp.StatusCode, URL: reqURL, Body: string(respBody)}
	}
	return respBody, nil
}

// Close releases idle connections held by the underlying transport.
func (c *QuickBooksClient) Close() {
	c.httpClient.CloseIdleConnections()
}
