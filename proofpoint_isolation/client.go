// Package usp_proofpoint_isolation implements a USP adapter for the Proofpoint
// Isolation Reporting API (usage data).
//
// Proofpoint Isolation is split into two products, each with its own Reporting
// API host:
//
//   - Browser and Email Isolation: https://proofpointisolation.com/api/v2/reporting/usage-data
//   - URL Isolation:               https://urlisolation.com/api/v2/reporting/usage-data
//
// The adapter polls either or both, depending on which keys are configured,
// and ships every usage record (one per isolated request: user, URL,
// classification, disposition, categories, ...) to LimaCharlie verbatim. Each
// product becomes its own event type.
//
// The usage-data endpoint is job based. A GET with from/to/pageSize starts a
// report job and returns its jobId and status; the caller keeps polling with
// the jobId until status is COMPLETED, then walks the result pages by echoing
// the returned pageToken until none is returned.
package usp_proofpoint_isolation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultWebBaseURL = "https://proofpointisolation.com"
	defaultURLBaseURL = "https://urlisolation.com"

	// Event types, one per product.
	eventTypeWeb = "browser_email_isolation"
	eventTypeURL = "url_isolation"

	defaultPageSize = 10000
	maxPageSize     = 10000

	defaultPollIntervalSeconds    = 300
	defaultInitialLookbackMinutes = 60
	maxInitialLookbackMinutes     = 30 * 24 * 60 // the API retains 30 days
	defaultOverlapMinutes         = 15
	defaultJobPollIntervalSeconds = 5
	defaultJobTimeoutMinutes      = 10
	defaultMaxPages               = 1000
	defaultDedupeTTLHours         = 24
	dedupeBucketWindow            = 1 * time.Hour

	defaultMaxRetryAttempts = 3
	defaultRetryBaseDelay   = 5 * time.Second
	defaultMaxRetryDelay    = 30 * time.Second

	statusCompleted = "COMPLETED"

	shipTimeout = 10 * time.Second

	timestampField = "date"
)

// timestampLayouts are the formats accepted for a record's "date" field. The
// API emits e.g. "2025-01-01T19:44:35.000+0000" (numeric zone without a colon);
// RFC 3339 variants are accepted too in case that ever changes.
var timestampLayouts = []string{
	"2006-01-02T15:04:05.999999999-0700",
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
}

// ProofpointIsolationConfig is the adapter configuration.
type ProofpointIsolationConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// WebAPIKey is the Reporting API key of Browser and Email Isolation
	// (Product Settings > Reporting API in that product's console). When set,
	// that product's usage data is collected.
	WebAPIKey string `json:"web_api_key" yaml:"web_api_key"`

	// URLAPIKey is the Reporting API key of URL Isolation. When set, that
	// product's usage data is collected.
	URLAPIKey string `json:"url_api_key" yaml:"url_api_key"`

	// WebBaseURL / URLBaseURL override each product's API root. Defaults:
	// https://proofpointisolation.com and https://urlisolation.com.
	WebBaseURL string `json:"web_base_url" yaml:"web_base_url"`
	URLBaseURL string `json:"url_base_url" yaml:"url_base_url"`

	// PageSize is the number of records requested per page (1-10000).
	// Default 10000.
	PageSize int `json:"page_size" yaml:"page_size"`

	// PollIntervalSeconds is the wait between queries. Default 300.
	PollIntervalSeconds int `json:"poll_interval_seconds" yaml:"poll_interval_seconds"`

	// InitialLookbackMinutes is how far back the first query reaches when the
	// adapter starts. Default 60; at most 43200 (30 days, the API's retention).
	InitialLookbackMinutes int `json:"initial_lookback_minutes" yaml:"initial_lookback_minutes"`

	// OverlapMinutes is how far each query reaches back before the end of the
	// previous one, to catch records the API indexes late. Records in the
	// overlap are deduplicated. Default 15.
	OverlapMinutes int `json:"overlap_minutes" yaml:"overlap_minutes"`

	// JobPollIntervalSeconds is the wait between checks of a report job that
	// has not completed yet. Default 5.
	JobPollIntervalSeconds int `json:"job_poll_interval_seconds" yaml:"job_poll_interval_seconds"`

	// JobTimeoutMinutes bounds how long one report job may stay incomplete
	// before the query is abandoned and retried on the next poll. Default 10.
	JobTimeoutMinutes int `json:"job_timeout_minutes" yaml:"job_timeout_minutes"`

	// MaxPages caps the pages walked per query, as a guard against a
	// pageToken that never ends. Default 1000.
	MaxPages int `json:"max_pages" yaml:"max_pages"`

	// DedupeTTLHours is how long a record is remembered to suppress
	// re-shipping it. Default 24.
	DedupeTTLHours int `json:"dedupe_ttl_hours" yaml:"dedupe_ttl_hours"`

	// MaxRetryAttempts is how many times a request is tried when it fails
	// transiently (5xx, 429, network). Default 3.
	MaxRetryAttempts int `json:"max_retry_attempts" yaml:"max_retry_attempts"`

	// Deduper, when set, replaces the built-in in-memory deduper. It is not
	// settable through a config file; it exists as a seam for tests and for
	// embedders that want to supply a shared deduper.
	Deduper utils.Deduper `json:"-" yaml:"-"`

	// Resolved durations. Validate fills them from the fields above unless
	// they are already set, which lets tests run on millisecond timings.
	pollInterval    time.Duration
	initialLookback time.Duration
	overlap         time.Duration
	jobPollInterval time.Duration
	jobTimeout      time.Duration
	dedupeTTL       time.Duration
	retryBaseDelay  time.Duration
	maxRetryDelay   time.Duration
}

// isoFeed is one product to poll.
type isoFeed struct {
	eventType string
	baseURL   string
	apiKey    string
}

func (c *ProofpointIsolationConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	c.WebAPIKey = strings.TrimSpace(c.WebAPIKey)
	c.URLAPIKey = strings.TrimSpace(c.URLAPIKey)
	if c.WebAPIKey == "" && c.URLAPIKey == "" {
		return errors.New("missing web_api_key and/or url_api_key")
	}

	c.WebBaseURL = strings.TrimRight(strings.TrimSpace(c.WebBaseURL), "/")
	if c.WebBaseURL == "" {
		c.WebBaseURL = defaultWebBaseURL
	}
	c.URLBaseURL = strings.TrimRight(strings.TrimSpace(c.URLBaseURL), "/")
	if c.URLBaseURL == "" {
		c.URLBaseURL = defaultURLBaseURL
	}

	if c.PageSize <= 0 {
		c.PageSize = defaultPageSize
	}
	if c.PageSize > maxPageSize {
		c.PageSize = maxPageSize
	}
	if c.MaxPages <= 0 {
		c.MaxPages = defaultMaxPages
	}
	if c.MaxRetryAttempts <= 0 {
		c.MaxRetryAttempts = defaultMaxRetryAttempts
	}
	if c.InitialLookbackMinutes > maxInitialLookbackMinutes {
		return fmt.Errorf("initial_lookback_minutes %d exceeds the API's 30-day retention (%d)",
			c.InitialLookbackMinutes, maxInitialLookbackMinutes)
	}

	resolve := func(d *time.Duration, n, def int, unit time.Duration) {
		if *d > 0 {
			return
		}
		if n <= 0 {
			n = def
		}
		*d = time.Duration(n) * unit
	}
	resolve(&c.pollInterval, c.PollIntervalSeconds, defaultPollIntervalSeconds, time.Second)
	resolve(&c.initialLookback, c.InitialLookbackMinutes, defaultInitialLookbackMinutes, time.Minute)
	resolve(&c.overlap, c.OverlapMinutes, defaultOverlapMinutes, time.Minute)
	resolve(&c.jobPollInterval, c.JobPollIntervalSeconds, defaultJobPollIntervalSeconds, time.Second)
	resolve(&c.jobTimeout, c.JobTimeoutMinutes, defaultJobTimeoutMinutes, time.Minute)
	resolve(&c.dedupeTTL, c.DedupeTTLHours, defaultDedupeTTLHours, time.Hour)
	if c.retryBaseDelay <= 0 {
		c.retryBaseDelay = defaultRetryBaseDelay
	}
	if c.maxRetryDelay <= 0 {
		c.maxRetryDelay = defaultMaxRetryDelay
	}

	// A record re-read in an overlapping window must still be remembered, or
	// it ships twice.
	if c.dedupeTTL <= c.overlap+c.pollInterval {
		return fmt.Errorf("dedupe_ttl_hours (%v) must exceed overlap_minutes + poll_interval_seconds (%v)",
			c.dedupeTTL, c.overlap+c.pollInterval)
	}
	return nil
}

// feeds returns the products to poll, one per configured API key.
func (c *ProofpointIsolationConfig) feeds() []isoFeed {
	var out []isoFeed
	if c.WebAPIKey != "" {
		out = append(out, isoFeed{eventType: eventTypeWeb, baseURL: c.WebBaseURL, apiKey: c.WebAPIKey})
	}
	if c.URLAPIKey != "" {
		out = append(out, isoFeed{eventType: eventTypeURL, baseURL: c.URLBaseURL, apiKey: c.URLAPIKey})
	}
	return out
}

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

// ProofpointIsolationAdapter polls the Reporting API of one or both Isolation
// products and ships their usage records to LimaCharlie.
type ProofpointIsolationAdapter struct {
	conf        ProofpointIsolationConfig
	uspClient   uspSink
	clients     []*IsoClient
	deduper     utils.Deduper
	ownsDeduper bool

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	closeOnce sync.Once
	closeErr  error

	ctx context.Context
}

// NewProofpointIsolationAdapter creates a Proofpoint Isolation adapter wired to
// LimaCharlie.
func NewProofpointIsolationAdapter(ctx context.Context, conf ProofpointIsolationConfig) (*ProofpointIsolationAdapter, chan struct{}, error) {
	return newProofpointIsolationAdapter(ctx, conf, nil)
}

// newProofpointIsolationAdapter is the implementation behind
// NewProofpointIsolationAdapter. When sink is non-nil it is used in place of a
// real LimaCharlie client -- the seam tests use to capture shipped events.
func newProofpointIsolationAdapter(ctx context.Context, conf ProofpointIsolationConfig, sink uspSink) (*ProofpointIsolationAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &ProofpointIsolationAdapter{
		conf:   conf,
		ctx:    ctx,
		doStop: utils.NewEvent(),
	}

	// Usage records carry no id and consecutive queries overlap, so a deduper
	// keyed on record content is what ships each record exactly once.
	a.deduper = conf.Deduper
	if a.deduper == nil {
		window := dedupeBucketWindow
		if window > conf.dedupeTTL {
			window = conf.dedupeTTL
		}
		deduper, err := utils.NewLocalDeduper(window, conf.dedupeTTL)
		if err != nil {
			return nil, nil, fmt.Errorf("proofpoint_isolation: deduper: %v", err)
		}
		a.deduper = deduper
		a.ownsDeduper = true
	}

	if sink != nil {
		a.uspClient = sink
	} else {
		uspClient, err := uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			if a.ownsDeduper {
				a.deduper.Close()
			}
			return nil, nil, err
		}
		a.uspClient = uspClient
	}

	a.chStopped = make(chan struct{})

	for _, feed := range conf.feeds() {
		client := NewIsoClient(feed.baseURL, feed.apiKey)
		a.clients = append(a.clients, client)
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("proofpoint_isolation: starting %q -> %s", feed.eventType, feed.baseURL))
		a.wgSenders.Add(1)
		go a.runFeed(feed, client)
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

// Close stops the adapter. It is idempotent: repeated calls are no-ops and
// return the result of the first call.
func (a *ProofpointIsolationAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.conf.ClientOptions.DebugLog("proofpoint_isolation: closing")
		a.doStop.Set()
		a.wgSenders.Wait()
		err1 := a.uspClient.Drain(1 * time.Minute)
		_, err2 := a.uspClient.Close()
		for _, c := range a.clients {
			c.Close()
		}
		if a.ownsDeduper {
			a.deduper.Close()
		}
		if err1 != nil {
			a.closeErr = err1
		} else {
			a.closeErr = err2
		}
	})
	return a.closeErr
}

// runFeed polls one product forever, until the adapter is asked to stop.
//
// Each poll queries [from, now]. The first poll starts InitialLookback ago.
// After a query completes, the next one starts Overlap before its end, so
// records the API indexes a little late are still picked up; the deduper
// drops the ones already shipped. A query that fails part way does not move
// the window, so the next poll retries the same range.
func (a *ProofpointIsolationAdapter) runFeed(feed isoFeed, client *IsoClient) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("proofpoint_isolation: %q stopped", feed.eventType))

	from := time.Now().UTC().Add(-a.conf.initialLookback)
	isFirstRun := true
	for isFirstRun || !a.doStop.WaitFor(a.conf.pollInterval) {
		isFirstRun = false
		to := time.Now().UTC()
		if !to.After(from) {
			continue
		}
		if a.runQuery(feed, client, from, to) {
			from = to.Add(-a.conf.overlap)
		}
	}
}

// runQuery runs one report job over [from, to] and ships its records. It
// returns true when every page was read.
func (a *ProofpointIsolationAdapter) runQuery(feed isoFeed, client *IsoClient, from, to time.Time) bool {
	req := usageDataRequest{From: from, To: to, PageSize: a.conf.PageSize}
	jobStarted := time.Now()
	nPages, nSeen, nShipped := 0, 0, 0

	for !a.doStop.IsSet() {
		resp, ok := a.fetch(feed, client, req)
		if !ok {
			return false
		}
		if resp.JobID != "" {
			req.JobID = resp.JobID
		}

		// An empty status is treated as a plain, already-complete response.
		if resp.Status != "" && !strings.EqualFold(resp.Status, statusCompleted) {
			if isFailedStatus(resp.Status) {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf(
					"proofpoint_isolation: %q report job %q ended with status %q (skipping this poll)",
					feed.eventType, req.JobID, resp.Status))
				return false
			}
			if req.JobID == "" {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf(
					"proofpoint_isolation: %q report job is %q but the API returned no jobId (skipping this poll)",
					feed.eventType, resp.Status))
				return false
			}
			if time.Since(jobStarted) > a.conf.jobTimeout {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf(
					"proofpoint_isolation: %q report job %q still %q after %v (skipping this poll)",
					feed.eventType, req.JobID, resp.Status, a.conf.jobTimeout))
				return false
			}
			if a.doStop.WaitFor(a.conf.jobPollInterval) {
				return false
			}
			continue
		}

		nPages++
		for _, raw := range resp.Data {
			item, err := utils.UnmarshalCleanJSON(string(raw))
			if err != nil || len(item) == 0 {
				// A record that is not a non-empty JSON object is skipped
				// rather than failing the whole page.
				continue
			}
			nSeen++
			if a.deduper.CheckAndAdd(dedupeKey(feed, raw)) {
				continue
			}
			if !a.ship(feed, utils.Dict(item)) {
				return false
			}
			nShipped++
		}

		if resp.PageToken == "" {
			a.conf.ClientOptions.DebugLog(fmt.Sprintf(
				"proofpoint_isolation: %q query %s..%s complete (pages=%d seen=%d shipped=%d)",
				feed.eventType, from.Format(time.RFC3339), to.Format(time.RFC3339), nPages, nSeen, nShipped))
			return true
		}
		if resp.PageToken == req.PageToken {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf(
				"proofpoint_isolation: %q API returned the same pageToken twice (skipping this poll)", feed.eventType))
			return false
		}
		if nPages >= a.conf.MaxPages {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf(
				"proofpoint_isolation: %q hit max_pages=%d; the rest of this query is retried next poll",
				feed.eventType, a.conf.MaxPages))
			return false
		}
		req.PageToken = resp.PageToken
	}
	return false
}

// isFailedStatus reports whether a report job status is terminal without
// data. Only COMPLETED is documented; anything that looks like a failure is
// treated as one rather than polled until the job timeout.
func isFailedStatus(status string) bool {
	s := strings.ToUpper(status)
	for _, bad := range []string{"FAIL", "ERROR", "CANCEL", "ABORT", "EXPIRE"} {
		if strings.Contains(s, bad) {
			return true
		}
	}
	return false
}

// fetch issues one request, retrying transient failures with exponential
// backoff. The bool result is false when the query should be abandoned (any
// source-side error, or the adapter is stopping).
//
// Source-side errors (anything that comes back from the Reporting API:
// 4xx/5xx, auth failures, network blips, malformed responses) are reported via
// OnWarning and NEVER stop the adapter. The cloud-sensor host treats any
// adapter OnError as fatal to the instance -- it tears the adapter down and
// relaunches it, and after enough failures disables it entirely. Restarting
// cannot fix a problem on Proofpoint's side, and it would take the healthy
// product's feed down with the broken one. So we log, skip just this poll,
// and try again next interval; a fixed key then heals on its own. Only a
// failure delivering to LimaCharlie (see ship) is fatal.
func (a *ProofpointIsolationAdapter) fetch(feed isoFeed, client *IsoClient, req usageDataRequest) (*usageDataResponse, bool) {
	var resp *usageDataResponse
	var err error
	for attempt := 0; attempt < a.conf.MaxRetryAttempts; attempt++ {
		if a.doStop.IsSet() {
			return nil, false
		}

		resp, err = client.GetUsageData(a.ctx, req)
		if err == nil {
			return resp, true
		}

		if !isTransientError(err) {
			msg := fmt.Sprintf("proofpoint_isolation: %q request failed (skipping this poll): %v", feed.eventType, err)
			var httpErr *HTTPError
			if errors.As(err, &httpErr) &&
				(httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden) {
				msg = fmt.Sprintf(
					"proofpoint_isolation: %q -- HTTP %d, API key rejected. Check that the key is the "+
						"Reporting API key for this product (Product Settings > Reporting API) and has not been "+
						"revoked.",
					feed.eventType, httpErr.StatusCode)
			}
			a.conf.ClientOptions.OnWarning(msg)
			return nil, false
		}

		if attempt+1 >= a.conf.MaxRetryAttempts {
			break
		}
		delay := a.conf.retryBaseDelay * time.Duration(1<<attempt)
		if delay > a.conf.maxRetryDelay {
			delay = a.conf.maxRetryDelay
		}
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"proofpoint_isolation: %q transient error (attempt %d/%d), retrying in %v: %v",
			feed.eventType, attempt+1, a.conf.MaxRetryAttempts, delay, err))
		if a.doStop.WaitFor(delay) {
			return nil, false
		}
	}
	a.conf.ClientOptions.OnWarning(fmt.Sprintf(
		"proofpoint_isolation: %q failed after %d attempts (skipping this poll): %v",
		feed.eventType, a.conf.MaxRetryAttempts, err))
	return nil, false
}

// ship forwards a single record to LimaCharlie. It returns false if the adapter
// should stop (an unrecoverable shipping error).
func (a *ProofpointIsolationAdapter) ship(feed isoFeed, item utils.Dict) bool {
	msg := &protocol.DataMessage{
		JsonPayload: item,
		EventType:   feed.eventType,
		TimestampMs: a.eventTime(feed, item),
	}
	if err := a.uspClient.Ship(msg, shipTimeout); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("proofpoint_isolation: stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("proofpoint_isolation: Ship(): %v", err))
			a.doStop.Set()
			return false
		}
	}
	return true
}

// eventTime extracts the record's event time from its "date" field, falling
// back to now when it is absent or unparseable.
func (a *ProofpointIsolationAdapter) eventTime(feed isoFeed, item utils.Dict) uint64 {
	if raw := item.FindOneString(timestampField); raw != "" {
		if t, ok := parseTimestamp(raw); ok {
			return uint64(t.UnixMilli())
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf(
			"proofpoint_isolation: %q unparseable timestamp %q", feed.eventType, raw))
	}
	return uint64(time.Now().UnixMilli())
}

// dedupeKey returns a deduplication key for a record, namespaced by product.
// Usage records have no id field, so the key is a hash of the record's
// canonical JSON (re-encoded so key order and whitespace do not matter).
func dedupeKey(feed isoFeed, raw json.RawMessage) string {
	b := []byte(raw)
	var v interface{}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err == nil {
		if canon, err := json.Marshal(v); err == nil {
			b = canon
		}
	}
	sum := sha256.Sum256(b)
	return feed.eventType + "|" + hex.EncodeToString(sum[:])
}

func parseTimestamp(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}
