package usp_ms_graph

import (
	"bytes"
	"context"
	"crypto/tls"
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
	"github.com/refractionPOINT/usp-adapters/internal/vendorerr"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const scope = "https://graph.microsoft.com/.default"
const URLPrefix = "https://graph.microsoft.com/v1.0/"
const defaultLoginEndpoint = "https://login.microsoftonline.com"
const defaultPollInterval = 30 * time.Second
const defaultTimestampField = "createdDateTime"
const maxShipWait = time.Minute

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type MsGraphAdapter struct {
	conf       MsGraphConfig
	uspClient  uspSink
	httpClient *http.Client

	endpoint      string
	loginEndpoint string
	pollInterval  time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup

	ctx      context.Context
	cancel   context.CancelFunc
	notifier vendorerr.Notifier
	runtime  graphRuntime
	retries  graphRetryHealth
}

type MsGraphConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	TenantID      string                  `json:"tenant_id" yaml:"tenant_id"`
	ClientID      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`
	URL           string                  `json:"url" yaml:"url"`

	// LoginEndpoint overrides the Azure AD authority used for the OAuth2
	// client_credentials token exchange. Empty means the public cloud
	// endpoint (https://login.microsoftonline.com).
	LoginEndpoint string `json:"login_endpoint" yaml:"login_endpoint"`

	// GraphEndpoint overrides the Microsoft Graph API root the relative URL
	// is appended to. Empty means the public cloud v1.0 root
	// (https://graph.microsoft.com/v1.0/).
	GraphEndpoint string `json:"graph_endpoint" yaml:"graph_endpoint"`

	// TimestampField is the datetime property the polled collection is
	// filtered and cursored on. Empty means "createdDateTime", which fits
	// collections like auditLogs/signIns or security/alerts; collections such
	// as auditLogs/directoryAudits use "activityDateTime" instead.
	TimestampField string `json:"timestamp_field,omitempty" yaml:"timestamp_field,omitempty"`

	// PollInterval is the wait between polls of the Graph endpoint. It is
	// not settable through a config file; it exists as a seam for tests.
	PollInterval time.Duration `json:"-" yaml:"-"`
}

func (c *MsGraphConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.TenantID == "" {
		return errors.New("missing tenant_id")
	}
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}
	if c.URL == "" {
		return errors.New("missing url")
	}
	return nil
}

func NewMsGraphAdapter(ctx context.Context, conf MsGraphConfig) (*MsGraphAdapter, chan struct{}, error) {
	return newMsGraphAdapter(ctx, conf, nil)
}

// newMsGraphAdapter is the implementation behind NewMsGraphAdapter. When sink
// is non-nil it is used in place of a real LimaCharlie client -- the seam
// tests use to capture shipped events.
func newMsGraphAdapter(ctx context.Context, conf MsGraphConfig, sink uspSink) (*MsGraphAdapter, chan struct{}, error) {
	return newMsGraphAdapterWithRuntime(ctx, conf, sink, defaultGraphRuntime())
}

func newMsGraphAdapterWithRuntime(ctx context.Context, conf MsGraphConfig, sink uspSink, runtime graphRuntime) (*MsGraphAdapter, chan struct{}, error) {
	a := &MsGraphAdapter{
		conf:    conf,
		ctx:     context.Background(),
		runtime: runtime,
		retries: graphRetryHealth{windowStart: runtime.now()},
	}

	a.ctx, a.cancel = context.WithCancel(ctx)

	a.loginEndpoint = strings.TrimSuffix(conf.LoginEndpoint, "/")
	if a.loginEndpoint == "" {
		a.loginEndpoint = defaultLoginEndpoint
	}
	a.pollInterval = conf.PollInterval
	if a.pollInterval <= 0 {
		a.pollInterval = defaultPollInterval
	}

	if sink != nil {
		a.uspClient = sink
	} else {
		uspClient, err := uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			a.cancel()
			return nil, nil, err
		}
		a.uspClient = uspClient
	}

	a.httpClient = &http.Client{
		Timeout: 10 * time.Second,
		// Refuse redirects before either bearer tokens or client credentials can
		// be forwarded. Continuations are validated separately against the origin.
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
		},
	}

	a.chStopped = make(chan struct{})

	graphRoot := a.conf.GraphEndpoint
	if graphRoot == "" {
		graphRoot = URLPrefix
	}
	if !strings.HasSuffix(graphRoot, "/") {
		graphRoot += "/"
	}

	// Strip the / prefix if in the URL.
	url := strings.TrimPrefix(a.conf.URL, "/")

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting to fetch alerts from %s", graphRoot+url))

	a.wgSenders.Add(1)
	go a.fetchEvents(graphRoot + url)

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *MsGraphAdapter) Close() error {
	defer utils.StopBlockedProducers(a.uspClient)()
	a.conf.ClientOptions.DebugLog("closing")
	a.cancel()
	a.wgSenders.Wait()
	a.notifier.Close()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
	a.httpClient.CloseIdleConnections()

	if err1 != nil {
		return err1
	}

	return err2
}

var errRetry = errors.New("retryable Graph failure")
var errPermanent = errors.New("Graph authentication or configuration failure")

const graphTimeLayout = "2006-01-02T15:04:05.0000000Z"

// Carries scheduling and status without exposing unredacted response text.
type graphHTTPError struct {
	error
	status  int
	retryAt time.Time
}

func (e *graphHTTPError) Unwrap() error { return e.error }

func retryAfter(value string, now time.Time) time.Duration {
	if n, err := strconv.ParseInt(value, 10, 32); err == nil && n >= 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(value); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

func (a *MsGraphAdapter) wait(delay time.Duration) error {
	if err := a.retries.escalation(a.runtime.now()); err != nil {
		return err
	}
	if remaining := a.retries.remaining(a.runtime.now()); a.retries.lastError != nil && remaining < delay {
		delay = remaining
	}
	if err := a.runtime.wait(a.ctx, delay); err != nil {
		return err
	}
	return a.retries.escalation(a.runtime.now())
}

// request handles both token and collection exchanges. Error status takes
// precedence over body read failures; only bounded, redacted diagnostics escape.
func (a *MsGraphAdapter) request(method, raw, payload, bearer string) ([]byte, error) {
	secrets := []string{a.conf.ClientSecret, bearer}
	req, err := http.NewRequestWithContext(a.ctx, method, raw, bytes.NewBufferString(payload))
	if err != nil {
		return nil, vendorerr.Kind(errPermanent, "building request: "+err.Error(), err, secrets...)
	}
	if req.URL.Host == "" || req.URL.User != nil || req.URL.Fragment != "" || (req.URL.Scheme != "http" && req.URL.Scheme != "https") {
		return nil, vendorerr.Kind(errPermanent, "invalid request endpoint", nil)
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		kind := errRetry
		var certificate *tls.CertificateVerificationError
		if errors.As(err, &certificate) {
			kind = errPermanent
		}
		return nil, vendorerr.Kind(kind, err.Error(), err, secrets...)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		now := a.runtime.now()
		kind := errPermanent
		if resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
			kind = errRetry
		}
		body, cut := vendorerr.ReadBody(resp)
		return nil, &graphHTTPError{error: vendorerr.Kind(kind, vendorerr.HTTPText(resp, body, cut, secrets...), nil, secrets...), status: resp.StatusCode, retryAt: now.Add(retryAfter(resp.Header.Get("Retry-After"), now))}
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, vendorerr.Kind(errRetry, "reading response: "+err.Error(), err, secrets...)
	}
	return body, nil
}

func (a *MsGraphAdapter) fetchToken() (string, error) {
	tokenURL := fmt.Sprintf("%s/%s/oauth2/v2.0/token", a.loginEndpoint, a.conf.TenantID)
	payload := url.Values{"client_id": {a.conf.ClientID}, "scope": {scope}, "grant_type": {"client_credentials"}, "client_secret": {a.conf.ClientSecret}}.Encode()
	body, err := a.request(http.MethodPost, tokenURL, payload, "")
	if err != nil {
		return "", vendorerr.Wrap("fetching token", err, a.conf.ClientSecret)
	}
	var result struct {
		Token string `json:"access_token"`
	}
	// Never include a successful token response in diagnostics.
	if err := json.Unmarshal(body, &result); err != nil || result.Token == "" {
		return "", vendorerr.Kind(errRetry, "token endpoint returned no usable access token", nil)
	}
	return result.Token, nil
}

// graphCursor is committed only after every page has been shipped. The inclusive
// boundary retains all IDs at the maximum parsed instant, independent of order.
type graphCursor struct {
	since time.Time
	ids   map[string]bool
}

type graphRecordKey struct {
	id      string
	instant time.Time
}

func (a *MsGraphAdapter) fetchEvents(eventsURL string) {
	defer a.wgSenders.Done()
	cursor := graphCursor{since: a.runtime.now().UTC().Truncate(100 * time.Nanosecond), ids: map[string]bool{}}
	// Retain successful shipments while retrying an incomplete scan, including
	// older records that would otherwise be skipped by a premature watermark.
	delivered := map[graphRecordKey]bool{}
	delay := a.pollInterval
	for {
		if err := a.wait(delay); err != nil {
			if a.ctx.Err() == nil {
				a.notifier.Notify(err, a.conf.ClientOptions.OnError)
			}
			return
		}

		next, err := a.poll(eventsURL, cursor, delivered)
		if err != nil {
			if a.ctx.Err() != nil || errors.Is(err, uspclient.ErrorClosed) {
				return
			}
			if errors.Is(err, errRetry) {
				a.retries.note(a.runtime.now(), err)
				if fatal := a.retries.escalation(a.runtime.now()); fatal != nil {
					a.notifier.Notify(fatal, a.conf.ClientOptions.OnError)
					return
				}
				a.conf.ClientOptions.OnWarning(err.Error())
				delay = a.pollInterval
				var httpErr *graphHTTPError
				if errors.As(err, &httpErr) && httpErr.retryAt.Sub(a.runtime.now()) > delay {
					delay = httpErr.retryAt.Sub(a.runtime.now())
				}
				continue
			}
			a.notifier.Notify(err, a.conf.ClientOptions.OnError)
			return
		}
		// A successful final page wins over the old deadline, including a
		// last Ship that completed just after it. Never discard that result.
		a.retries.reset(a.runtime.now())
		cursor = next
		delay = a.pollInterval
		delivered = map[graphRecordKey]bool{}
	}
}

func (a *MsGraphAdapter) timestampField() string {
	if a.conf.TimestampField != "" {
		return a.conf.TimestampField
	}
	return defaultTimestampField
}

// poll streams pages, but keeps the committed watermark unchanged on any
// request, decoding, validation or shipping failure. delivered is retry state.
func (a *MsGraphAdapter) poll(eventsURL string, cursor graphCursor, delivered map[graphRecordKey]bool) (graphCursor, error) {
	malformed := 0
	defer func() {
		if malformed > 0 {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("skipped %d malformed Graph record(s)", malformed))
		}
	}()
	origin, err := url.Parse(eventsURL)
	if err != nil {
		return cursor, errors.New("invalid Graph endpoint")
	}
	if origin.Host == "" || origin.User != nil || (origin.Scheme != "https" && origin.Scheme != "http") {
		return cursor, errors.New("invalid Graph endpoint")
	}
	query, err := url.ParseQuery(origin.RawQuery)
	if err != nil {
		return cursor, errors.New("invalid Graph query")
	}
	filter := a.timestampField() + " ge " + cursor.since.UTC().Truncate(100*time.Nanosecond).Format(graphTimeLayout)
	if existing := query.Get("$filter"); existing != "" {
		filter = "(" + existing + ") and (" + filter + ")"
	}
	query.Set("$filter", filter)
	first := *origin
	first.RawQuery = query.Encode()
	current := &first
	visited := map[string]bool{}
	token := ""
	candidate := graphCursor{since: cursor.since, ids: map[string]bool{}}
	for id := range cursor.ids {
		candidate.ids[id] = true
	}
	for {
		if a.ctx.Err() != nil {
			return cursor, a.ctx.Err()
		}
		if visited[current.String()] {
			return cursor, vendorerr.Kind(errRetry, "Graph continuation cycle", nil)
		}
		visited[current.String()] = true
		body, err := a.requestPage(current.String(), &token)
		if err != nil {
			return cursor, err
		}
		var page struct {
			Value json.RawMessage `json:"value"`
			Next  string          `json:"@odata.nextLink"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return cursor, vendorerr.Kind(errRetry, "invalid Graph page JSON", nil)
		}
		var records []json.RawMessage
		if len(page.Value) == 0 || string(page.Value) == "null" {
			return cursor, vendorerr.Kind(errRetry, "missing Graph value array", nil)
		}
		if err := json.Unmarshal(page.Value, &records); err != nil {
			return cursor, vendorerr.Kind(errRetry, "invalid Graph value array", nil)
		}
		shippedOnPage := false
		for _, raw := range records {
			var record map[string]interface{}
			if err := json.Unmarshal(raw, &record); err != nil || record == nil {
				malformed++
				continue
			}
			id, _ := record["id"].(string)
			stamp, _ := record[a.timestampField()].(string)
			instant, err := time.Parse(time.RFC3339Nano, stamp)
			if err != nil || strings.TrimSpace(id) == "" {
				malformed++
				continue
			}
			instant = instant.UTC()
			if instant.Before(cursor.since) {
				continue
			}
			key := graphRecordKey{id: id, instant: instant}
			if !(instant.Equal(cursor.since) && cursor.ids[id]) && !delivered[key] {
				if a.ctx.Err() != nil {
					return cursor, a.ctx.Err()
				}
				// Accept the fetched page's first bounded shipment even if
				// its successful request crossed the old deadline. A partial
				// shipment does not reset the clock or excuse an unshipped tail.
				if shippedOnPage {
					if fatal := a.retries.escalation(a.runtime.now()); fatal != nil {
						return cursor, fatal
					}
				}
				msg := &protocol.DataMessage{JsonPayload: record, TimestampMs: uint64(time.Now().UnixMilli())}
				err = a.uspClient.Ship(msg, 10*time.Second)
				if err == uspclient.ErrorBufferFull {
					a.retries.note(a.runtime.now(), vendorerr.Kind(errRetry, "shipping buffer full", err))
					a.conf.ClientOptions.OnWarning("stream falling behind")
					remaining := a.retries.remaining(a.runtime.now())
					if remaining <= 0 {
						return cursor, a.retries.escalation(a.runtime.now())
					}
					err = a.uspClient.Ship(msg, min(maxShipWait, remaining))
				}
				if err != nil {
					return cursor, vendorerr.Kind(errRetry, "shipping record: "+err.Error(), err, a.conf.ClientSecret, token)
				}
				delivered[key] = true
				shippedOnPage = true
			}
			if instant.After(candidate.since) {
				candidate.since = instant
				candidate.ids = map[string]bool{}
			}
			if instant.Equal(candidate.since) {
				candidate.ids[id] = true
			}
		}
		var nextURL *url.URL
		if page.Next != "" {
			link, err := url.Parse(page.Next)
			if err != nil {
				return cursor, vendorerr.Kind(errRetry, "invalid Graph continuation", nil)
			}
			nextURL = current.ResolveReference(link)
			if !strings.EqualFold(nextURL.Scheme, origin.Scheme) || !strings.EqualFold(nextURL.Host, origin.Host) || nextURL.User != nil || nextURL.Fragment != "" {
				return cursor, errors.New("Graph continuation must use the configured origin")
			}
		}
		// Count progress only after envelope/continuation validation and every
		// usable record has shipped. Empty valid pages can advance pagination.
		a.retries.pageComplete(a.runtime.now(), current.String(), shippedOnPage)
		if nextURL == nil {
			return candidate, nil
		}

		// nextLink is opaque: never reconstruct its query or add the filter again.
		current = nextURL
	}
}

func (a *MsGraphAdapter) requestPage(pageURL string, token *string) ([]byte, error) {
	refreshed := false
	for attempt := 1; attempt <= 3; attempt++ {
		if fatal := a.retries.escalation(a.runtime.now()); fatal != nil {
			return nil, fatal
		}
		var err error
		if *token == "" {
			*token, err = a.fetchToken()
		}
		if err == nil {
			var body []byte
			body, err = a.request(http.MethodGet, pageURL, "", *token)
			if err == nil {
				// The request already succeeded: let page validation and
				// bounded shipping decide progress, not its prior deadline.
				return body, nil
			}
			var httpErr *graphHTTPError
			if errors.As(err, &httpErr) && httpErr.status == http.StatusUnauthorized && !refreshed {
				refreshed = true
				*token = ""
				attempt--
				continue
			}
		}
		if a.ctx.Err() != nil {
			return nil, a.ctx.Err()
		}
		if !errors.Is(err, errRetry) {
			return nil, err
		}
		a.retries.note(a.runtime.now(), err)
		if fatal := a.retries.escalation(a.runtime.now()); fatal != nil {
			return nil, fatal
		}
		if attempt == 3 {
			return nil, err
		}
		a.conf.ClientOptions.OnWarning(err.Error())
		delay := time.Duration(attempt) * time.Second
		var httpErr *graphHTTPError
		if errors.As(err, &httpErr) && httpErr.retryAt.Sub(a.runtime.now()) > delay {
			delay = httpErr.retryAt.Sub(a.runtime.now())
		}
		if err := a.wait(delay); err != nil {
			return nil, err
		}
	}
	return nil, vendorerr.Kind(errRetry, "Graph retries exhausted", nil)
}
