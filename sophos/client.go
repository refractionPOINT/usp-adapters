package usp_sophos

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

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	eventsURL = "/siem/v1/events"

	// defaultAuthURL is the Sophos Central OAuth2 token endpoint used when the
	// config does not override it.
	defaultAuthURL = "https://id.sophos.com/api/v2/oauth2/token"

	// defaultPollInterval is how long fetchEvents idles between polls of the
	// SIEM events endpoint.
	defaultPollInterval = 30 * time.Second

	// maxBackoff caps the wait between polls while Sophos keeps failing with
	// a transient error and did not send a usable Retry-After.
	maxBackoff = 5 * time.Minute

	// maxRetryAfter caps how long a server-provided Retry-After can idle the
	// adapter.
	maxRetryAfter = 1 * time.Hour

	// defaultTransientErrorGracePeriod is how long transient failures (rate limiting,
	// 5xx, network errors) are only reported as warnings before being
	// escalated to OnError. OnError is fatal to a cloud-hosted adapter, so
	// escalating on the first 429 would tear down a healthy adapter.
	defaultTransientErrorGracePeriod = 1 * time.Hour

	// tokenExpiryMargin is how long before its expiry a cached JWT is renewed.
	tokenExpiryMargin = 2 * time.Minute

	// defaultTokenLifetime is used when the token response has no expires_in.
	defaultTokenLifetime = 10 * time.Minute
)

// transientError is a failure worth retrying later: HTTP 429, HTTP 5xx or a
// network error. RetryAfter is the delay the server asked for, 0 if none.
type transientError struct {
	err        error
	RetryAfter time.Duration
}

func (e *transientError) Error() string { return e.err.Error() }
func (e *transientError) Unwrap() error { return e.err }

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type opRequest struct {
	Limit     int    `json:"page[size],omitempty"`
	StartTime string `json:"filter[created_at],omitempty"`
	Sort      string `json:"sort,omitempty"`
}

type SophosAdapter struct {
	conf       SophosConfig
	uspClient  uspSink
	httpClient *http.Client

	authURL              string
	pollInterval         time.Duration
	transientGracePeriod time.Duration

	// Cached Sophos Central JWT, only touched by the fetchEvents goroutine.
	token       string
	tokenExpiry time.Time

	// fromDate is the from_date used until the first successful request
	// yields a cursor. It is pinned on the first attempt so retries after a
	// backoff do not skip the events created in between.
	fromDate string

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type SophosConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ClientId      string                  `json:"clientid" yaml:"clientid"`
	ClientSecret  string                  `json:"clientsecret" yaml:"clientsecret"`
	TenantId      string                  `json:"tenantid" yaml:"tenantid"`
	URL           string                  `json:"url" yaml:"url"`

	// AuthURL overrides the Sophos Central OAuth2 token endpoint. Defaults to
	// https://id.sophos.com/api/v2/oauth2/token when empty.
	AuthURL string `json:"auth_url" yaml:"auth_url"`

	// PollInterval overrides the wait between polls of the SIEM events
	// endpoint (default 30s). It is not settable through a config file; it
	// exists as a seam for tests.
	PollInterval time.Duration `json:"-" yaml:"-"`

	// TransientErrorGracePeriod overrides how long transient failures are
	// reported as warnings before being escalated to an error (default 1h).
	// Like PollInterval, it is a test seam only.
	TransientErrorGracePeriod time.Duration `json:"-" yaml:"-"`
}

func (c *SophosConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.URL == "" {
		return errors.New("missing url")
	}
	if c.ClientId == "" {
		return errors.New("missing client id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client secret")
	}
	if c.TenantId == "" {
		return errors.New("missing tenant id")
	}
	return nil
}

func NewSophosAdapter(ctx context.Context, conf SophosConfig) (*SophosAdapter, chan struct{}, error) {
	return newSophosAdapter(ctx, conf, nil)
}

// newSophosAdapter is the implementation behind NewSophosAdapter. When sink is
// non-nil it is used in place of a real LimaCharlie client -- the seam tests
// use to capture shipped events.
func newSophosAdapter(ctx context.Context, conf SophosConfig, sink uspSink) (*SophosAdapter, chan struct{}, error) {
	a := &SophosAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
	}

	a.authURL = conf.AuthURL
	if a.authURL == "" {
		a.authURL = defaultAuthURL
	}
	a.pollInterval = conf.PollInterval
	if a.pollInterval <= 0 {
		a.pollInterval = defaultPollInterval
	}
	a.transientGracePeriod = conf.TransientErrorGracePeriod
	if a.transientGracePeriod <= 0 {
		a.transientGracePeriod = defaultTransientErrorGracePeriod
	}

	if sink != nil {
		a.uspClient = sink
	} else {
		uspClient, err := uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			return nil, nil, err
		}
		a.uspClient = uspClient
	}

	a.httpClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
		},
	}

	a.chStopped = make(chan struct{})

	a.wgSenders.Add(1)
	go a.fetchEvents(eventsURL)

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *SophosAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()
	a.httpClient.CloseIdleConnections()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *SophosAdapter) fetchEvents(url string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", url))

	lastCursor := ""
	has_more := "false"
	wait := a.pollInterval
	nTransient := 0
	var firstTransient time.Time
	for !a.doStop.WaitFor(wait) {
		wait = a.pollInterval
		items, newCursor, has_more_resp, err := a.makeOneRequest(url, lastCursor, has_more)
		if err != nil {
			var te *transientError
			if !errors.As(err, &te) {
				a.conf.ClientOptions.OnError(err)
				continue
			}
			now := time.Now()
			if nTransient == 0 {
				firstTransient = now
			}
			nTransient++
			wait = a.backoff(nTransient, te.RetryAfter)
			if now.Sub(firstTransient) >= a.transientGracePeriod {
				a.conf.ClientOptions.OnError(fmt.Errorf("sophos api failing for %s: %v", now.Sub(firstTransient).Round(time.Second), err))
				// Start a new grace period so a host that does not stop on
				// OnError is not flooded with errors.
				nTransient = 0
				continue
			}
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("%v (retrying in %s)", err, wait))
			continue
		}
		nTransient = 0
		lastCursor = newCursor
		has_more = has_more_resp
		if items == nil {
			continue
		}

		for _, item := range items {
			msg := &protocol.DataMessage{
				JsonPayload: item,
				TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
			}
			if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
				if err == uspclient.ErrorBufferFull {
					a.conf.ClientOptions.OnWarning("stream falling behind")
					err = a.uspClient.Ship(msg, 1*time.Hour)
				}
				if err == nil {
					continue
				}
				a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				a.doStop.Set()
				return
			}
		}
	}
}

// backoff returns how long to wait before the next poll after the n-th
// consecutive transient failure: the server's Retry-After when it sent one,
// otherwise the poll interval doubled per failure, capped at maxBackoff.
func (a *SophosAdapter) backoff(n int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		if retryAfter > maxRetryAfter {
			return maxRetryAfter
		}
		return retryAfter
	}
	d := a.pollInterval
	for i := 0; i < n && d < maxBackoff; i++ {
		d *= 2
	}
	if d > maxBackoff {
		d = maxBackoff
	}
	return d
}

// classifyStatus wraps a non-200 response into an error, flagging rate
// limiting and server errors as transient.
func classifyStatus(resp *http.Response, msg string) error {
	err := errors.New(msg)
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return &transientError{
			err:        err,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	}
	return err
}

// parseRetryAfter parses a Retry-After header given either as seconds or as an
// HTTP date. An absent or unparseable value yields 0.
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// getJwt returns a Sophos Central access token, reusing the cached one until
// it is about to expire. The bool reports whether the token was freshly issued.
func (a *SophosAdapter) getJwt() (string, bool, error) {
	if a.token != "" && time.Now().Before(a.tokenExpiry) {
		return a.token, false, nil
	}

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("scope", "token")
	form.Set("client_id", a.conf.ClientId)
	form.Set("client_secret", a.conf.ClientSecret)

	req, err := http.NewRequest("POST", a.authURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", false, fmt.Errorf("sophos auth request: %v", err)
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", false, &transientError{err: fmt.Errorf("sophos auth http.Client.Do(): %v", err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", false, classifyStatus(resp, fmt.Sprintf("sophos auth api non-200: %s\nRESPONSE: %s", resp.Status, string(body)))
	}

	var respData struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return "", false, &transientError{err: fmt.Errorf("sophos auth api invalid json: %v", err)}
	}
	if respData.AccessToken == "" {
		return "", false, errors.New("sophos auth api returned no access_token")
	}

	lifetime := defaultTokenLifetime
	if respData.ExpiresIn > 0 {
		lifetime = time.Duration(respData.ExpiresIn) * time.Second
	}
	// Renew ahead of expiry, but never cache for less than half the lifetime.
	margin := tokenExpiryMargin
	if margin > lifetime/2 {
		margin = lifetime / 2
	}
	a.token = respData.AccessToken
	a.tokenExpiry = time.Now().Add(lifetime - margin)
	return a.token, true, nil
}

func (a *SophosAdapter) makeOneRequest(url string, lastCursor string, has_more string) ([]utils.Dict, string, string, error) {
	token, isFreshToken, err := a.getJwt()
	if err != nil {
		return nil, lastCursor, has_more, err
	}

	// Prepare the request.
	var req *http.Request
	if has_more != "false" {
		req, err = http.NewRequest("GET", fmt.Sprintf("%s%s?cursor=%s&limit=200", a.conf.URL, url, lastCursor), nil)
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s%s with cursor, has_more: %s", a.conf.URL, url, has_more))
	} else {
		if a.fromDate == "" {
			a.fromDate = strconv.FormatInt(time.Now().Unix()-30, 10)
		}
		req, err = http.NewRequest("GET", fmt.Sprintf("%s%s?from_date=%s&limit=200", a.conf.URL, url, a.fromDate), nil)
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s%s starting at %s, has_more: %s", a.conf.URL, url, a.fromDate, has_more))
	}
	if err != nil {
		a.doStop.Set()
		return nil, lastCursor, has_more, fmt.Errorf("http.NewRequest(): %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", a.conf.TenantId)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	// Issue the request.
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, lastCursor, has_more, &transientError{err: fmt.Errorf("http.Client.Do(): %v", err)}
	}
	defer resp.Body.Close()

	// Evaluate if success.
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		msg := fmt.Sprintf("sophos api non-200: %s\nRESPONSE: %s", resp.Status, string(body))
		if resp.StatusCode == http.StatusUnauthorized {
			// Drop the cached token. If it was reused, it may have been
			// revoked or expired early, so retry with a fresh one. A freshly
			// issued token being rejected is a real configuration problem.
			a.token = ""
			if !isFreshToken {
				return nil, lastCursor, has_more, &transientError{err: errors.New(msg)}
			}
		}
		return nil, lastCursor, has_more, classifyStatus(resp, msg)
	}

	// Parse the response.
	respData := utils.Dict{}
	jsonDecoder := json.NewDecoder(resp.Body)
	if err := jsonDecoder.Decode(&respData); err != nil {
		return nil, lastCursor, has_more, &transientError{err: fmt.Errorf("sophos api invalid json: %v", err)}
	}

	// Report if a cursor was returned
	// as well as the items.
	lastCursor = respData.FindOneString("next_cursor")
	has_more = respData.FindOneString("has_more")
	items, _ := respData.GetListOfDict("items")
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("response data: %s", respData))

	return items, lastCursor, has_more, nil
}
