package usp_mimecast

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultBaseURL      = "https://api.services.mimecast.com"
	defaultPollInterval = 30 * time.Second

	// defaultOverlapPeriod is how far back each poll reaches. It must exceed
	// the Mimecast audit index's ingestion lag: a record is not retrievable
	// until some time after its eventTime, so a lookback shorter than that lag
	// returns nothing at all. 30 seconds was shorter than the observed lag,
	// which meant this adapter never ingested anything. 30 minutes matches the
	// okta and hubspot adapters; the resulting window overlap is absorbed by
	// the dedupe map. It is a chosen bound, not a measured one -- a tenant
	// whose lag is worse can raise it with the overlap_period config.
	defaultOverlapPeriod = 30 * time.Minute

	// maxOverlapPeriod bounds overlap_period. The whole window is re-requested
	// every pollInterval, so an unreasonably large value means re-reading days
	// of audit records every 30 seconds.
	maxOverlapPeriod = 24 * time.Hour

	// auditPageSize is the meta.pagination.pageSize the adapter requests.
	auditPageSize = 50

	// maxPagesPerPoll bounds one poll's pagination. Without it a server that
	// keeps returning a meta.pagination.next token loops forever. At
	// auditPageSize this covers 100k records in a single window, far more than
	// any real overlap period holds.
	maxPagesPerPoll = 2000

	// tokenExpirySafetyMargin is subtracted from the token's advertised
	// lifetime so a request is never sent with a token about to expire.
	tokenExpirySafetyMargin = 60 * time.Second

	// defaultTokenTTL is how long a token is cached when the OAuth response
	// omits expires_in. Short enough to be safe against any real expiry.
	defaultTokenTTL = 5 * time.Minute

	// windowEdgeWarnRatio is the fraction of the overlap period past which an
	// arriving record is considered close to falling out of the window. Once
	// records routinely arrive that late the ingestion lag is approaching
	// overlap_period, and the adapter is heading back towards ingesting
	// nothing -- which is a silent failure, so it is worth a warning.
	windowEdgeWarnRatio = 0.8
)

// mimecastTimeLayout is the ISO 8601 form Mimecast uses for startDateTime,
// endDateTime and eventTime: yyyy-MM-dd'T'HH:mm:ssZ with a colonless numeric
// offset, e.g. "2011-12-03T10:15:30+0000". This is NOT time.RFC3339, which
// requires "+00:00" or "Z".
const mimecastTimeLayout = "2006-01-02T15:04:05-0700"

// parseEventTime parses a Mimecast eventTime, accepting both the documented
// colonless-offset form and RFC3339 in case the API ever emits it. Between the
// two every realistic rendering is covered: "+0000", "Z", "+00:00" and a
// fractional-second component.
func parseEventTime(s string) (time.Time, error) {
	if t, err := time.Parse(mimecastTimeLayout, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type MimecastAdapter struct {
	conf       MimecastConfig
	uspClient  uspSink
	httpClient *http.Client

	baseURL       string
	pollInterval  time.Duration
	overlapPeriod time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context

	// dedupe maps an audit record id to its eventTime in Unix seconds. Because
	// consecutive polls overlap by design, the same record is returned many
	// times; this is what keeps it from shipping more than once. Entries are
	// culled once they fall out of the poll window, which bounds the map to
	// one overlap period of records.
	dedupe map[string]int64

	// tokenMu guards the cached OAuth token. The token is fetched once per
	// poll at most, not once per page.
	tokenMu     sync.Mutex
	token       string
	tokenExpiry time.Time
}

type AuthResponse struct {
	AccessToken string `json:"access_token"`
	// ExpiresIn is the token lifetime in seconds. Absent responses fall back
	// to defaultTokenTTL.
	ExpiresIn int `json:"expires_in"`
}

type AuditRequest struct {
	Data []AuditEvent `json:"data"`
	Meta MetaData     `json:"meta"`
}

type AuditEvent struct {
	StartDateTime string `json:"startDateTime"`
	EndDateTime   string `json:"endDateTime"`
}

type ApiResponse struct {
	Meta MetaData         `json:"meta"`
	Data []AuditLog       `json:"data"`
	Fail []FailureDetails `json:"fail"`
}

type FailureDetails struct {
	Errors []ErrorDetail `json:"errors"`
}

type ErrorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type MetaData struct {
	Pagination Pagination `json:"pagination"`
	Status     int        `json:"status"`
}

type Pagination struct {
	PageSize int    `json:"pageSize"`
	Next     string `json:"next"`
}

type AuditLog struct {
	ID        string `json:"id"`
	AuditType string `json:"auditType"`
	User      string `json:"user"`
	EventTime string `json:"eventTime"`
	EventInfo string `json:"eventInfo"`
	Category  string `json:"category"`
}

type MimecastConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ClientId      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`

	// BaseURL overrides the Mimecast API root. Empty means the global API
	// gateway (https://api.services.mimecast.com).
	BaseURL string `json:"base_url" yaml:"base_url"`

	// OverlapPeriod overrides how far back each poll reaches, as a Go duration
	// string (e.g. "45m"). It must exceed the tenant's audit index ingestion
	// lag or nothing is ingested at all; raising it costs re-reading a larger
	// window on every poll. Empty means 30 minutes.
	OverlapPeriod string `json:"overlap_period" yaml:"overlap_period"`

	// PollInterval overrides the wait between polls of the audit events
	// endpoint. It is not settable through a config file; it exists as a seam
	// for tests. Defaults to 30 seconds.
	PollInterval time.Duration `json:"-" yaml:"-"`
}

// resolveOverlapPeriod returns the configured overlap period, or the default
// when unset. It is the single place overlap_period is interpreted, so
// Validate and the constructor cannot disagree about what a value means.
func (c *MimecastConfig) resolveOverlapPeriod() (time.Duration, error) {
	if c.OverlapPeriod == "" {
		return defaultOverlapPeriod, nil
	}
	d, err := time.ParseDuration(c.OverlapPeriod)
	if err != nil {
		return 0, fmt.Errorf("overlap_period: %v", err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("overlap_period must be positive, got %s", d)
	}
	if d > maxOverlapPeriod {
		return 0, fmt.Errorf("overlap_period must be at most %s, got %s", maxOverlapPeriod, d)
	}
	return d, nil
}

func (c *MimecastConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ClientId == "" {
		return errors.New("missing client id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client secret")
	}
	if _, err := c.resolveOverlapPeriod(); err != nil {
		return err
	}

	return nil
}

func NewMimecastAdapter(ctx context.Context, conf MimecastConfig) (*MimecastAdapter, chan struct{}, error) {
	return newMimecastAdapter(ctx, conf, nil)
}

// newMimecastAdapter is the implementation behind NewMimecastAdapter. When
// sink is non-nil it is used in place of a real LimaCharlie client -- the seam
// tests use to capture shipped events.
func newMimecastAdapter(ctx context.Context, conf MimecastConfig, sink uspSink) (*MimecastAdapter, chan struct{}, error) {
	// The container does not call Validate before constructing an adapter, so
	// overlap_period is resolved here rather than trusted.
	overlapPeriod, err := conf.resolveOverlapPeriod()
	if err != nil {
		return nil, nil, err
	}

	a := &MimecastAdapter{
		conf:          conf,
		ctx:           context.Background(),
		doStop:        utils.NewEvent(),
		dedupe:        make(map[string]int64),
		baseURL:       defaultBaseURL,
		pollInterval:  defaultPollInterval,
		overlapPeriod: overlapPeriod,
	}
	if conf.BaseURL != "" {
		a.baseURL = strings.TrimRight(conf.BaseURL, "/")
	}
	if conf.PollInterval > 0 {
		a.pollInterval = conf.PollInterval
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
	go a.fetchEvents()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *MimecastAdapter) Close() error {
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

func (a *MimecastAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", a.baseURL))

	for !a.doStop.WaitFor(a.pollInterval) {
		// makeOneRequest reports its own errors. It can return records
		// alongside an error when a poll fails part-way through pagination;
		// those records were read successfully and must still ship, because
		// they are already recorded as seen and will not be offered again.
		items, _ := a.makeOneRequest()

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

// makeOneRequest polls one window of audit events, paginating until the API
// stops offering a next token.
//
// The window is always the last overlapPeriod, with no floor and no moving
// cursor. An earlier version passed a cursor into a max(cursor, now-overlap)
// clamp copied from the okta adapter; because that clamp keeps the *later* of
// the two, a cursor could only ever shrink the window, never extend it, which
// is what stopped this adapter from ever returning data. A fixed window has no
// such failure mode: overlapPeriod alone decides the lookback, and the dedupe
// map absorbs the overlap between consecutive polls.
//
// The cost of a fixed window is that a restart re-ships up to one overlap
// period of events, since the dedupe map is in-memory, and that an outage
// longer than one overlap period loses the records in the gap. For security
// telemetry duplicates beat gaps.
//
// Records may be returned alongside a non-nil error: pages read before the
// failure are complete and are reported as seen, so the caller must ship them
// or they are lost for good.
func (a *MimecastAdapter) makeOneRequest() ([]utils.Dict, error) {
	currentTime := time.Now()
	start := currentTime.Add(-a.overlapPeriod).UTC().Format(mimecastTimeLayout)
	end := currentTime.UTC().Format(mimecastTimeLayout)

	auditURL := a.baseURL + "/api/audit/get-audit-events"

	// The token is fetched once per poll rather than once per page: a busy
	// tenant's window can span many pages, and re-running the OAuth exchange
	// for each one doubles the request count against a rate-limited API.
	token, err := a.getAuthToken()
	if err != nil {
		err = fmt.Errorf("mimecast auth: %v", err)
		a.conf.ClientOptions.OnError(err)
		return nil, err
	}
	// A cached token can expire mid-poll. One re-authentication per poll is
	// enough to recover from that without turning a persistent 401 into an
	// unbounded retry loop.
	mayRetryAuth := true

	var allItems []utils.Dict
	// seen holds this poll's dedupe additions, merged into a.dedupe only for
	// records that are actually returned to the caller. Recording an id for a
	// record that never ships would suppress it on every later poll, so the
	// two must move together.
	seen := map[string]int64{}
	totalRecords := 0
	pages := 0
	// oldestNew is the eventTime of the oldest record this poll newly saw. How
	// close it sits to the trailing edge of the window is the only visible
	// signal that the ingestion lag is approaching overlapPeriod.
	var oldestNew time.Time
	// pollErr is the failure that ended pagination early, if any. It is
	// returned alongside whatever the poll did manage to read.
	var pollErr error

	pageToken := ""
	for {
		if pages >= maxPagesPerPoll {
			// Stop rather than loop forever, but say so: silently truncating a
			// window reads as "we collected everything" when we did not.
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("mimecast pagination hit the %d page cap for window %s..%s; the remainder of this window was not read", maxPagesPerPoll, start, end))
			break
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s start %s end %s page %d", auditURL, start, end, pages))

		response, status, pageErr := a.fetchAuditPage(auditURL, token, start, end, pageToken)
		if pageErr != nil {
			if status == http.StatusUnauthorized && mayRetryAuth {
				// The cached token went stale. Drop it, get a fresh one and
				// retry this same page.
				mayRetryAuth = false
				a.invalidateAuthToken()
				newToken, authErr := a.getAuthToken()
				if authErr != nil {
					pollErr = fmt.Errorf("mimecast re-auth: %v", authErr)
					a.conf.ClientOptions.OnError(pollErr)
					break
				}
				token = newToken
				continue
			}
			pollErr = pageErr
			a.conf.ClientOptions.OnError(pollErr)
			break
		}
		pages++
		totalRecords += len(response.Data)
		a.warnOnPartialFailure(response)

		for _, item := range response.Data {
			eventID := item.ID
			if _, ok := a.dedupe[eventID]; ok {
				continue
			}
			if _, ok := seen[eventID]; ok {
				continue
			}

			epoch, perr := parseEventTime(item.EventTime)
			if perr != nil {
				// Without a usable eventTime the dedupe entry would be culled
				// immediately and the record would re-ship every poll. Stamp
				// it with now so it survives the overlap window instead.
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("unparseable eventTime %q: %v", item.EventTime, perr))
				epoch = time.Now()
			}
			if oldestNew.IsZero() || epoch.Before(oldestNew) {
				oldestNew = epoch
			}

			seen[eventID] = epoch.Unix()
			allItems = append(allItems, utils.Dict{
				"id":        item.ID,
				"auditType": item.AuditType,
				"user":      item.User,
				"eventTime": item.EventTime,
				"eventInfo": item.EventInfo,
				"category":  item.Category,
			})
		}

		next := response.Meta.Pagination.Next
		if next == "" {
			break
		}
		if next == pageToken {
			// A token that does not advance would spin until the page cap.
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("mimecast pagination token did not advance (%q); stopping this window early", next))
			break
		}
		pageToken = next
	}

	for k, v := range seen {
		a.dedupe[k] = v
	}

	// Cull dedupe entries that have fallen out of the poll window. The cutoff
	// matches the window start exactly: an entry is dropped only once the
	// record it guards can no longer be returned.
	cutoff := time.Now().Add(-a.overlapPeriod).Unix()
	for k, v := range a.dedupe {
		if v < cutoff {
			delete(a.dedupe, k)
		}
	}

	// A poll that reads nothing at all, forever, is exactly what the too-short
	// lookback looked like from the outside. Make the counts visible.
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("mimecast window %s..%s: %d pages, %d records, %d new, %d tracked", start, end, pages, totalRecords, len(allItems), len(a.dedupe)))
	a.warnIfNearWindowEdge(oldestNew)

	return allItems, pollErr
}

// warnOnPartialFailure surfaces the fail array Mimecast returns alongside an
// HTTP 200. A partially-failed page still carries data, so it is not an error,
// but reporting nothing at all is how this adapter's original silence started.
func (a *MimecastAdapter) warnOnPartialFailure(response *ApiResponse) {
	for _, f := range response.Fail {
		for _, e := range f.Errors {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("mimecast api reported a failure alongside a 200: %s: %s (retryable=%v)", e.Code, e.Message, e.Retryable))
		}
	}
}

// warnIfNearWindowEdge reports records arriving close to the trailing edge of
// the poll window. Once the ingestion lag exceeds overlapPeriod the adapter
// goes back to ingesting nothing with no other outward sign, so the approach
// to that cliff is worth surfacing while it is still recoverable.
func (a *MimecastAdapter) warnIfNearWindowEdge(oldestNew time.Time) {
	if oldestNew.IsZero() {
		return
	}
	age := time.Since(oldestNew)
	threshold := time.Duration(float64(a.overlapPeriod) * windowEdgeWarnRatio)
	if age < threshold {
		return
	}
	a.conf.ClientOptions.OnWarning(fmt.Sprintf("mimecast records are arriving %s after their eventTime, close to the %s overlap_period; raise overlap_period or events will start being missed entirely", age.Truncate(time.Second), a.overlapPeriod))
}

// fetchAuditPage performs a single get-audit-events request and returns the
// parsed page. It exists as its own function so the response body is closed
// when the page is done rather than when the whole poll is, which for a
// multi-page window is the difference between one open body and hundreds.
//
// The HTTP status is returned alongside the error so the caller can tell an
// expired token from any other failure.
func (a *MimecastAdapter) fetchAuditPage(auditURL, token, start, end, pageToken string) (*ApiResponse, int, error) {
	pagination := map[string]interface{}{
		"pageSize": auditPageSize,
	}
	if pageToken != "" {
		pagination["pageToken"] = pageToken
	}
	auditData := map[string]interface{}{
		"meta": map[string]interface{}{
			"pagination": pagination,
		},
		"data": []map[string]string{
			{
				"startDateTime": start,
				"endDateTime":   end,
			},
		},
	}

	jsonData, err := json.Marshal(auditData)
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequest("POST", auditURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("mimecast api read body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, fmt.Errorf("mimecast api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, string(jsonData), string(body))
	}

	var response ApiResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("mimecast api invalid json: %v", err)
	}

	return &response, resp.StatusCode, nil
}

// getAuthToken returns a cached OAuth token, exchanging client credentials for
// a new one when the cache is empty or close to expiring.
func (a *MimecastAdapter) getAuthToken() (string, error) {
	a.tokenMu.Lock()
	defer a.tokenMu.Unlock()

	if a.token != "" && time.Now().Before(a.tokenExpiry) {
		return a.token, nil
	}

	tokenURL := a.baseURL + "/oauth/token"
	// Form-encode rather than concatenate: a client secret containing "+" or
	// "&" would otherwise be silently mangled into a failing authentication.
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", a.conf.ClientId)
	form.Set("client_secret", a.conf.ClientSecret)

	req, err := http.NewRequest("POST", tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get token, status code: %d", resp.StatusCode)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var authResp AuthResponse
	if err := json.Unmarshal(respBody, &authResp); err != nil {
		return "", err
	}
	if authResp.AccessToken == "" {
		return "", errors.New("mimecast oauth response carried no access_token")
	}

	ttl := defaultTokenTTL
	if authResp.ExpiresIn > 0 {
		ttl = time.Duration(authResp.ExpiresIn) * time.Second
	}
	// Never serve a token in its last moments; a very short advertised
	// lifetime simply means no caching.
	if ttl > tokenExpirySafetyMargin {
		ttl -= tokenExpirySafetyMargin
	} else {
		ttl = 0
	}

	a.token = authResp.AccessToken
	a.tokenExpiry = time.Now().Add(ttl)

	return a.token, nil
}

// invalidateAuthToken drops the cached token so the next call re-authenticates.
func (a *MimecastAdapter) invalidateAuthToken() {
	a.tokenMu.Lock()
	defer a.tokenMu.Unlock()
	a.token = ""
	a.tokenExpiry = time.Time{}
}
