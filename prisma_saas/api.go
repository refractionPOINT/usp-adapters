package usp_prisma_saas

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
	"sync"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	tokenPath      = "/oauth/token"
	bulkEventsPath = "/api/v1/log_events_bulk"
	// singleEventPath is the long-poll endpoint documented by Palo Alto
	// Networks: one event per call, 204 after a 20s server-side timeout.
	singleEventPath = "/api/v1/log_events"

	// bulkSize is the maximum number of events log_events_bulk returns per call
	// (a hard server-side cap, per the Cortex XSOAR collector).
	bulkSize = 100

	// maxResponseBytes bounds how much of any response body is read.
	maxResponseBytes = 32 << 20

	// requestTimeout bounds one HTTP exchange. The long-poll endpoint holds a
	// request for 20s server-side, so this leaves ample headroom.
	requestTimeout = 60 * time.Second

	// tokenRefreshMargin is how long before expiry a token is replaced.
	tokenRefreshMargin = 60 * time.Second
)

// HTTPError represents a non-2xx response from the SaaS Security API. It
// carries the status code so callers can classify the error (retry vs. give up)
// without parsing error strings.
type HTTPError struct {
	StatusCode int
	URL        string
	Body       string
	// RetryAfter is the server-requested delay from a Retry-After header, if any.
	RetryAfter time.Duration
}

func (e *HTTPError) Error() string {
	body := e.Body
	if len(body) > 512 {
		body = body[:512] + "..."
	}
	return fmt.Sprintf("unexpected status code %d for %q: %s", e.StatusCode, e.URL, body)
}

// AuthError reports that the credentials, the token or the API client's scope
// were rejected. It is permanent: retrying with the same credentials cannot
// succeed, so the adapter stops.
type AuthError struct {
	Err error
}

func (e *AuthError) Error() string { return e.Err.Error() }
func (e *AuthError) Unwrap() error { return e.Err }

// parseError reports a 2xx response whose body could not be interpreted. The
// events it carried are already gone from the queue, so it is surfaced loudly
// but does not stop the adapter.
type parseError struct {
	URL string
	Err error
}

func (e *parseError) Error() string {
	return fmt.Sprintf("failed to parse response from %q: %v", e.URL, e.Err)
}
func (e *parseError) Unwrap() error { return e.Err }

// isTransientError reports whether an error is worth retrying.
//
// Transient (retry with backoff):
//   - HTTP 5xx and 429
//   - network errors (timeouts, connection refused, DNS failures, ...)
//   - unparseable 2xx bodies
//
// Permanent (do not retry):
//   - authentication failures (*AuthError)
//   - any other HTTP 4xx
//   - context cancellation (intentional shutdown)
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	var authErr *AuthError
	if errors.As(err, &authErr) {
		return false
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode >= 500 || httpErr.StatusCode == http.StatusTooManyRequests
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var perr *parseError
	if errors.As(err, &perr) {
		return true
	}
	// Anything that failed before a response arrived (DNS, dial, timeout, ...)
	// is wrapped with this prefix by do().
	return strings.Contains(err.Error(), "failed to execute request")
}

// isNotFound reports whether err is an HTTP 404.
func isNotFound(err error) bool {
	var httpErr *HTTPError
	return errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound
}

// retryAfterOf returns the Retry-After delay carried by err, if any.
func retryAfterOf(err error) time.Duration {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.RetryAfter
	}
	return 0
}

// parseRetryAfter understands both forms of Retry-After: delta-seconds and an
// HTTP date.
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseFloat(v, 64); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs * float64(time.Second))
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// apiClient talks to one SaaS Security tenant: it obtains and caches the
// OAuth2 client-credentials token and issues authenticated event reads.
// It is safe for concurrent use.
type apiClient struct {
	baseURL      string
	clientID     string
	clientSecret string
	httpClient   *http.Client

	mu        sync.Mutex
	token     string
	expiresAt time.Time // zero: the token does not expire
}

func newAPIClient(baseURL, clientID, clientSecret string, concurrency int) *apiClient {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	tr.MaxIdleConnsPerHost = concurrency + 1
	return &apiClient{
		baseURL:      strings.TrimRight(baseURL, "/"),
		clientID:     clientID,
		clientSecret: clientSecret,
		httpClient:   &http.Client{Timeout: requestTimeout, Transport: tr},
	}
}

func (c *apiClient) Close() {
	c.httpClient.CloseIdleConnections()
}

// do performs a request and returns the status code and (bounded) body.
func (c *apiClient) do(req *http.Request) (int, []byte, http.Header, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Strip the *url.Error wrapper's URL query noise but keep the cause.
		var uerr *url.Error
		if errors.As(err, &uerr) {
			err = uerr.Err
		}
		return 0, nil, nil, fmt.Errorf("failed to execute request %q: %w", req.URL.Path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return 0, nil, nil, fmt.Errorf("failed to execute request %q: failed reading body: %w", req.URL.Path, err)
	}
	return resp.StatusCode, body, resp.Header, nil
}

func (c *apiClient) httpError(code int, path string, body []byte, h http.Header) *HTTPError {
	return &HTTPError{
		StatusCode: code,
		URL:        path,
		Body:       strings.TrimSpace(string(body)),
		RetryAfter: parseRetryAfter(h.Get("Retry-After")),
	}
}

// accessToken returns a valid bearer token, requesting a new one when none is
// cached or the cached one is about to expire. Concurrent callers share a
// single refresh.
func (c *apiClient) accessToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && (c.expiresAt.IsZero() || time.Now().Before(c.expiresAt)) {
		return c.token, nil
	}
	c.token = ""

	tok, lifetime, err := c.requestToken(ctx)
	if err != nil {
		return "", err
	}
	c.token = tok
	c.expiresAt = time.Time{}
	if lifetime > 0 {
		margin := tokenRefreshMargin
		if lifetime <= 2*margin {
			margin = lifetime / 2
		}
		c.expiresAt = time.Now().Add(lifetime - margin)
	}
	return tok, nil
}

// invalidate drops tok from the cache (if it is still the cached one), forcing
// the next accessToken call to request a fresh token. Passing the stale token
// keeps concurrent workers that all saw the same 401 from refreshing N times.
func (c *apiClient) invalidate(tok string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token == tok {
		c.token = ""
	}
}

// requestToken performs the OAuth2 client-credentials exchange.
func (c *apiClient) requestToken(ctx context.Context) (string, time.Duration, error) {
	form := url.Values{"grant_type": {"client_credentials"}, "scope": {"api_access"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+tokenPath, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, fmt.Errorf("failed to create token request: %v", err)
	}
	req.SetBasicAuth(c.clientID, c.clientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	code, body, hdr, err := c.do(req)
	if err != nil {
		return "", 0, err
	}
	if code != http.StatusOK {
		herr := c.httpError(code, tokenPath, body, hdr)
		// A rejected client id/secret answers 401 (per the docs) or 400
		// invalid_client (OAuth2 convention); 403 means the client is not
		// allowed to obtain the scope. None can heal by retrying.
		// Any other non-retryable answer (e.g. 404 from a wrong host) cannot
		// heal either, and must not be mistaken for a missing events endpoint.
		if code != http.StatusTooManyRequests && code < 500 {
			return "", 0, &AuthError{Err: fmt.Errorf("token request rejected (check client_id, client_secret, region and base_url): %w", herr)}
		}
		return "", 0, herr
	}

	var tr struct {
		AccessToken string  `json:"access_token"`
		ExpiresIn   float64 `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", 0, &parseError{URL: tokenPath, Err: err}
	}
	if tr.AccessToken == "" {
		return "", 0, &parseError{URL: tokenPath, Err: errors.New("no access_token in token response")}
	}
	// A missing expires_in means the token does not expire (per the docs).
	return tr.AccessToken, time.Duration(tr.ExpiresIn * float64(time.Second)), nil
}

// fetchEvents reads from one of the event endpoints. It returns no events (and
// no error) on a 204. A 401 refreshes the token and retries the request once;
// a 401 on the retry, or any 403, is an *AuthError.
func (c *apiClient) fetchEvents(ctx context.Context, path string, bulk bool) (eventBatch, error) {
	for attempt := 0; ; attempt++ {
		tok, err := c.accessToken(ctx)
		if err != nil {
			return eventBatch{}, err
		}

		u := c.baseURL + path
		if bulk {
			u += "?size=" + strconv.Itoa(bulkSize)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return eventBatch{}, fmt.Errorf("failed to create request %q: %v", path, err)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Accept", "application/json")

		code, body, hdr, err := c.do(req)
		if err != nil {
			return eventBatch{}, err
		}

		switch {
		case code == http.StatusNoContent:
			return eventBatch{}, nil
		case code == http.StatusOK:
			batch, err := parseEvents(body, bulk)
			if err != nil {
				return eventBatch{}, &parseError{URL: path, Err: err}
			}
			return batch, nil
		case code == http.StatusUnauthorized && attempt == 0:
			// Expired or revoked token: drop it and retry with a fresh one.
			c.invalidate(tok)
			continue
		case code == http.StatusUnauthorized || code == http.StatusForbidden:
			return eventBatch{}, &AuthError{Err: fmt.Errorf(
				"events request rejected (check the API client still exists and has the Log File Access scope): %w",
				c.httpError(code, path, body, hdr))}
		default:
			return eventBatch{}, c.httpError(code, path, body, hdr)
		}
	}
}

// eventBatch is the outcome of one successful read.
type eventBatch struct {
	events []utils.Dict
	// skipped counts elements that were not JSON objects and so could not be
	// shipped.
	skipped int
	// unexpectedShape, when set, describes a 200 bulk response that did not
	// carry an "events" array. Its content is still returned in events, since a
	// 200 means the source already removed it from its queue.
	unexpectedShape string
}

// parseEvents decodes an events response. log_events_bulk answers
// {"events":[...]}; log_events answers a single event object. A bare array is
// accepted for both.
//
// Anything the source answered with 200 has left its queue, so a body that
// parses as JSON is never discarded: a bulk object without an "events" array is
// shipped as one event (with unexpectedShape set so the caller can warn). Only
// a body that is not valid JSON is an error, and its content is not echoed.
func parseEvents(body []byte, bulk bool) (eventBatch, error) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return eventBatch{}, nil
	}
	if !json.Valid([]byte(trimmed)) {
		return eventBatch{}, fmt.Errorf("response body is not valid JSON (%d bytes)", len(body))
	}

	var raws []json.RawMessage
	var unexpected string
	switch trimmed[0] {
	case '[':
		if err := json.Unmarshal([]byte(trimmed), &raws); err != nil {
			return eventBatch{}, fmt.Errorf("response is not a JSON array of events (%d bytes)", len(body))
		}
	case '{':
		raws = []json.RawMessage{json.RawMessage(trimmed)}
		if !bulk {
			break
		}
		var env map[string]json.RawMessage
		if err := json.Unmarshal([]byte(trimmed), &env); err != nil {
			return eventBatch{}, fmt.Errorf("response is not a JSON object (%d bytes)", len(body))
		}
		evs, ok := env["events"]
		switch {
		case !ok:
			// Not the documented envelope. A bare event is shipped as such;
			// anything else is shipped whole rather than lost.
			if _, isEvent := env[eventTypeField]; isEvent {
				unexpected = "an event object instead of an \"events\" envelope"
			} else if len(env) == 0 {
				raws = nil
			} else {
				unexpected = "an object without an \"events\" array"
			}
		case strings.TrimSpace(string(evs)) == "null":
			raws = nil
		default:
			raws = nil
			if err := json.Unmarshal(evs, &raws); err != nil {
				raws = []json.RawMessage{json.RawMessage(trimmed)}
				unexpected = "an \"events\" field that is not an array"
			}
		}
	default:
		return eventBatch{}, fmt.Errorf("response is neither a JSON object nor a JSON array (%d bytes)", len(body))
	}

	batch := eventBatch{events: make([]utils.Dict, 0, len(raws)), unexpectedShape: unexpected}
	for _, r := range raws {
		// UnmarshalCleanJSON keeps integer precision so payloads round-trip
		// verbatim. A non-object element is skipped rather than failing the
		// whole batch: the other events are already off the queue.
		m, err := utils.UnmarshalCleanJSON(string(r))
		if err != nil || len(m) == 0 {
			batch.skipped++
			continue
		}
		batch.events = append(batch.events, utils.Dict(m))
	}
	return batch, nil
}
