package usp_gmail

import (
	"context"
	"crypto/rsa"
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

	"github.com/golang-jwt/jwt/v4"
)

// Endpoints and OAuth constants for talking to Google's identity platform.
const (
	// googleTokenEndpoint is Google's OAuth 2.0 token endpoint, used for both
	// the refresh-token grant and the service-account JWT-bearer grant.
	googleTokenEndpoint = "https://oauth2.googleapis.com/token"

	// jwtBearerGrantType is the RFC 7523 grant used by a service account to
	// exchange a signed assertion for an access token.
	jwtBearerGrantType = "urn:ietf:params:oauth:grant-type:jwt-bearer"

	// gmailReadonlyScope is the default OAuth scope: read-only access to the
	// mailbox, which is all an email-telemetry collector needs.
	gmailReadonlyScope = "https://www.googleapis.com/auth/gmail.readonly"

	// tokenRefreshBuffer is how long before an access token's stated expiry the
	// client proactively re-mints it, to avoid racing the boundary.
	tokenRefreshBuffer = 2 * time.Minute

	// accessTokenFallbackLifetime is assumed when the token endpoint omits
	// expires_in. Google access tokens live for one hour.
	accessTokenFallbackLifetime = time.Hour

	// serviceAccountAssertionLifetime is the lifetime of the signed JWT the
	// service-account flow presents. Google caps this at one hour.
	serviceAccountAssertionLifetime = time.Hour
)

// HTTPError represents a non-2xx response from a Google endpoint (the Gmail API
// or the OAuth token endpoint). It carries the status code and, when the body
// is a Google API error envelope, the machine-readable reason -- so callers can
// classify the error (retry vs. give up) without parsing free-text messages.
type HTTPError struct {
	StatusCode int
	URL        string
	Body       string
	// Reason is the Google error reason (e.g. "rateLimitExceeded",
	// "authError"), extracted from the error envelope when present.
	Reason string
}

func (e *HTTPError) Error() string {
	body := e.Body
	if len(body) > 512 {
		body = body[:512] + "..."
	}
	if e.Reason != "" {
		return fmt.Sprintf("unexpected status code %d (%s) for %q: %s", e.StatusCode, e.Reason, e.URL, body)
	}
	return fmt.Sprintf("unexpected status code %d for %q: %s", e.StatusCode, e.URL, body)
}

// TokenError wraps a failure to obtain an OAuth access token. It lets the
// adapter tell a credential problem (fatal) apart from a data-request error,
// while still allowing isTransientError to see through to the underlying cause
// (a 5xx/network blip refreshing the token is retryable).
type TokenError struct{ Err error }

func (e *TokenError) Error() string { return "token request failed: " + e.Err.Error() }
func (e *TokenError) Unwrap() error { return e.Err }

// transientReasons are Google error reasons that indicate a temporary, ret-able
// throttling/backend condition even when carried by a 403.
var transientReasons = map[string]struct{}{
	"rateLimitExceeded":     {},
	"userRateLimitExceeded": {},
	"backendError":          {},
}

// isTransientError reports whether an error is worth retrying.
//
// Transient (retry):
//   - HTTP 5xx server errors
//   - HTTP 429 Too Many Requests
//   - HTTP 403 carrying a rate-limit / backend reason (Gmail signals throttling
//     this way as well as via 429)
//   - network errors (timeouts, connection refused, DNS failures, ...)
//
// Permanent (do not retry):
//   - HTTP 4xx other than the above (400, 404, a plain 403 permission denial)
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
		if httpErr.StatusCode == http.StatusForbidden {
			_, ok := transientReasons[httpErr.Reason]
			return ok
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

// googleErrorEnvelope mirrors the shape of a Google API JSON error body:
//
//	{ "error": { "code": 403, "message": "...", "status": "PERMISSION_DENIED",
//	             "errors": [ { "reason": "rateLimitExceeded", ... } ] } }
//
// The OAuth token endpoint instead returns a flat { "error": "invalid_grant",
// "error_description": "..." }; parseHTTPError tolerates both.
type googleErrorEnvelope struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
		Errors  []struct {
			Reason string `json:"reason"`
		} `json:"errors"`
	} `json:"error"`
}

// parseHTTPError builds an *HTTPError, extracting the Google error reason from
// the body when it is a structured error envelope.
func parseHTTPError(statusCode int, reqURL, body string) *HTTPError {
	e := &HTTPError{StatusCode: statusCode, URL: reqURL, Body: body}
	var env googleErrorEnvelope
	if err := json.Unmarshal([]byte(body), &env); err == nil {
		if len(env.Error.Errors) > 0 && env.Error.Errors[0].Reason != "" {
			e.Reason = env.Error.Errors[0].Reason
		} else if env.Error.Status != "" {
			e.Reason = env.Error.Status
		}
	}
	return e
}

// tokenResponse is the JSON body returned by Google's OAuth 2.0 token endpoint.
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"` // access token lifetime, seconds
	Scope       string `json:"scope"`
}

// grantFunc returns the application/x-www-form-urlencoded body for a token
// request. For the refresh-token flow it is constant; for the service-account
// flow it mints a freshly signed assertion on every call (assertions expire).
type grantFunc func(ctx context.Context) (url.Values, error)

// tokenSource mints and caches OAuth access tokens. It owns the token lifecycle:
// it fetches one on first use and silently re-mints it when it nears expiry (or
// when forced after the API rejects it with a 401). It is safe for concurrent
// use. The two supported grants -- refresh token and service-account JWT bearer
// -- differ only in the grantFunc; everything else is shared.
type tokenSource struct {
	name       string
	tokenURL   string
	grant      grantFunc
	httpClient *http.Client

	mu     sync.Mutex
	token  string
	expiry time.Time
}

// Token returns a valid access token, minting one if it is missing, within
// tokenRefreshBuffer of expiry, or when force is set (used after a 401).
func (s *tokenSource) Token(ctx context.Context, force bool) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !force && s.token != "" && time.Now().Before(s.expiry.Add(-tokenRefreshBuffer)) {
		return s.token, nil
	}
	if err := s.mint(ctx); err != nil {
		return "", &TokenError{Err: err}
	}
	return s.token, nil
}

// mint performs one token request. The caller must hold s.mu.
func (s *tokenSource) mint(ctx context.Context) error {
	form, err := s.grant(ctx)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create token request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request %q: %v", s.tokenURL, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read token response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return parseHTTPError(resp.StatusCode, s.tokenURL, string(respBody))
	}

	var tok tokenResponse
	if err := json.Unmarshal(respBody, &tok); err != nil {
		return fmt.Errorf("failed to parse token response: %v", err)
	}
	if tok.AccessToken == "" {
		return errors.New("token endpoint returned an empty access_token")
	}

	s.token = tok.AccessToken
	lifetime := time.Duration(tok.ExpiresIn) * time.Second
	if lifetime <= 0 {
		lifetime = accessTokenFallbackLifetime
	}
	s.expiry = time.Now().Add(lifetime)
	return nil
}

// Close releases idle connections held by the token source's transport.
func (s *tokenSource) Close() {
	if s.httpClient != nil {
		s.httpClient.CloseIdleConnections()
	}
}

// newRefreshTokenSource builds a token source for the OAuth 2.0 installed-app /
// web flow: a long-lived refresh token exchanged for short-lived access tokens.
// This is the flow for collecting a single user's mailbox.
func newRefreshTokenSource(tokenURL, clientID, clientSecret, refreshToken string, httpClient *http.Client) *tokenSource {
	return &tokenSource{
		name:       "oauth-refresh-token",
		tokenURL:   tokenURL,
		httpClient: httpClient,
		grant: func(context.Context) (url.Values, error) {
			form := url.Values{}
			form.Set("grant_type", "refresh_token")
			form.Set("client_id", clientID)
			form.Set("client_secret", clientSecret)
			form.Set("refresh_token", refreshToken)
			return form, nil
		},
	}
}

// serviceAccountKey holds the fields we use from a Google service-account JSON
// key file.
type serviceAccountKey struct {
	Type         string `json:"type"`
	ProjectID    string `json:"project_id"`
	PrivateKeyID string `json:"private_key_id"`
	PrivateKey   string `json:"private_key"`
	ClientEmail  string `json:"client_email"`
	ClientID     string `json:"client_id"`
	TokenURI     string `json:"token_uri"`
}

// parseServiceAccountKey decodes a service-account JSON key and its RSA private
// key.
func parseServiceAccountKey(raw []byte) (*serviceAccountKey, *rsa.PrivateKey, error) {
	var k serviceAccountKey
	if err := json.Unmarshal(raw, &k); err != nil {
		return nil, nil, fmt.Errorf("invalid service account JSON: %v", err)
	}
	if k.ClientEmail == "" {
		return nil, nil, errors.New("service account JSON is missing client_email")
	}
	if k.PrivateKey == "" {
		return nil, nil, errors.New("service account JSON is missing private_key")
	}
	rsaKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(k.PrivateKey))
	if err != nil {
		return nil, nil, fmt.Errorf("service account private_key is not a valid RSA PEM key: %v", err)
	}
	return &k, rsaKey, nil
}

// newServiceAccountSource builds a token source for Google Workspace
// domain-wide delegation: the service account signs a JWT asserting the right to
// act as `subject` (the mailbox owner) for the requested `scopes`, and exchanges
// it for an access token. `audience` is the token endpoint the assertion targets.
func newServiceAccountSource(key *serviceAccountKey, rsaKey *rsa.PrivateKey, subject string, scopes []string, tokenURL string, httpClient *http.Client) *tokenSource {
	audience := tokenURL
	return &tokenSource{
		name:       "service-account-jwt",
		tokenURL:   tokenURL,
		httpClient: httpClient,
		grant: func(context.Context) (url.Values, error) {
			assertion, err := signServiceAccountAssertion(key, rsaKey, subject, scopes, audience)
			if err != nil {
				return nil, err
			}
			form := url.Values{}
			form.Set("grant_type", jwtBearerGrantType)
			form.Set("assertion", assertion)
			return form, nil
		},
	}
}

// signServiceAccountAssertion builds and RS256-signs the JWT a service account
// presents to the token endpoint. `subject`, when set, is the user the service
// account impersonates via domain-wide delegation.
//
// Reference:
// https://developers.google.com/identity/protocols/oauth2/service-account#delegatingauthority
func signServiceAccountAssertion(key *serviceAccountKey, rsaKey *rsa.PrivateKey, subject string, scopes []string, audience string) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"iss":   key.ClientEmail,
		"scope": strings.Join(scopes, " "),
		"aud":   audience,
		"iat":   now.Unix(),
		"exp":   now.Add(serviceAccountAssertionLifetime).Unix(),
	}
	if subject != "" {
		claims["sub"] = subject
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	if key.PrivateKeyID != "" {
		tok.Header["kid"] = key.PrivateKeyID
	}
	signed, err := tok.SignedString(rsaKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign service account assertion: %v", err)
	}
	return signed, nil
}

// newAuthHTTPClient builds the HTTP client used for token requests.
func newAuthHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{Timeout: 10 * time.Second}).Dial,
		},
	}
}
