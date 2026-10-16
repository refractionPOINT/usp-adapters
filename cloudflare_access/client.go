// Package usp_cloudflare_access implements a USP adapter for Cloudflare
// Access's per-request audit logs (also called "access authentication logs"):
// https://developers.cloudflare.com/cloudflare-one/insights/logs/dashboard-logs/access-authentication-logs/#per-request-audit-logs
//
// The API exposes a single endpoint,
// GET /accounts/{account_id}/access/logs/access_requests, filtered by a
// since/until time window and capped at a server-side result limit (~1000)
// with no cursor. The adapter walks each poll's window forward in time
// (direction=asc), re-querying with an advanced `since` whenever a response
// comes back full, until a short page shows the window is exhausted. Records
// are forwarded to LimaCharlie in their original Cloudflare JSON form; the
// adapter does not reshape payloads.
package usp_cloudflare_access

import (
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
	defaultBaseURL = "https://api.cloudflare.com/client/v4"

	// eventTypeAccessRequests is the EventType every shipped event carries.
	eventTypeAccessRequests = "access_requests"

	// idField / timestampField are the record fields the adapter relies on for
	// deduplication and event time, per the documented response shape.
	idField        = "ray_id"
	timestampField = "created_at"

	defaultPollInterval     = 1 * time.Minute
	defaultInitialLookback  = 1 * time.Hour
	defaultLimit            = 1000
	maxLimit                = 1000
	defaultMaxPages         = 100
	defaultDedupeTTL        = 24 * time.Hour
	dedupeBucketWindow      = 30 * time.Minute
	defaultMaxRetryAttempts = 3
	defaultRetryBaseDelay   = 5 * time.Second
	defaultMaxRetryDelay    = 30 * time.Second

	shipTimeout = 10 * time.Second
)

// timestampLayouts are the time formats accepted for a record's created_at
// field. Cloudflare documents RFC 3339 with a Z suffix; a couple of lenient
// fallbacks are accepted in case of sub-second precision.
var timestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
}

// CloudflareAccessConfig is the adapter configuration.
type CloudflareAccessConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// APIToken is a Cloudflare API token scoped to Account -> Access: Audit
	// Logs Read, sent as a bearer credential.
	APIToken string `json:"api_token" yaml:"api_token"`

	// AccountID is the Cloudflare account ID that owns the Access application
	// logs (Account Home -> right sidebar, or `wrangler whoami`).
	AccountID string `json:"account_id" yaml:"account_id"`

	// BaseURL overrides the Cloudflare API root (default
	// "https://api.cloudflare.com/client/v4"). Exists as a seam for tests.
	BaseURL string `json:"base_url" yaml:"base_url"`

	// PollInterval is the wait between polls. Default 1 minute.
	PollInterval time.Duration `json:"poll_interval" yaml:"poll_interval"`

	// InitialLookback bounds how far back the first poll reaches for
	// historical events. Default 1 hour.
	InitialLookback time.Duration `json:"initial_lookback" yaml:"initial_lookback"`

	// Limit is the number of records requested per API call. Cloudflare caps
	// this at 1000 server-side regardless of a higher value. Default 1000.
	Limit int `json:"limit" yaml:"limit"`

	// MaxPages caps how many paginated calls are made within a single poll,
	// bounding the work done when a poll's window contains more than Limit
	// records. Default 100.
	MaxPages int `json:"max_pages" yaml:"max_pages"`

	// DedupeTTL is how long a request's ray_id is remembered to suppress
	// re-shipping it across overlapping polls. Default 24 hours.
	DedupeTTL time.Duration `json:"dedupe_ttl" yaml:"dedupe_ttl"`

	// Retry tuning for transient API failures.
	RetryBaseDelay   time.Duration `json:"retry_base_delay" yaml:"retry_base_delay"`
	MaxRetryDelay    time.Duration `json:"max_retry_delay" yaml:"max_retry_delay"`
	MaxRetryAttempts int           `json:"max_retry_attempts" yaml:"max_retry_attempts"`

	// Deduper, when set, replaces the built-in in-memory deduper. It is not
	// settable through a config file; it exists as a seam for tests and for
	// embedders that want to supply a shared deduper.
	Deduper utils.Deduper `json:"-" yaml:"-"`
}

func (c *CloudflareAccessConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}

	c.APIToken = strings.TrimSpace(c.APIToken)
	if c.APIToken == "" {
		return errors.New("missing api_token")
	}
	c.AccountID = strings.TrimSpace(c.AccountID)
	if c.AccountID == "" {
		return errors.New("missing account_id")
	}

	c.BaseURL = strings.TrimSpace(c.BaseURL)
	if c.BaseURL == "" {
		c.BaseURL = defaultBaseURL
	}
	c.BaseURL = strings.TrimRight(c.BaseURL, "/")

	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.InitialLookback <= 0 {
		c.InitialLookback = defaultInitialLookback
	}
	if c.Limit <= 0 {
		c.Limit = defaultLimit
	}
	if c.Limit > maxLimit {
		c.Limit = maxLimit
	}
	if c.MaxPages <= 0 {
		c.MaxPages = defaultMaxPages
	}
	if c.DedupeTTL <= 0 {
		c.DedupeTTL = defaultDedupeTTL
	}
	if c.RetryBaseDelay <= 0 {
		c.RetryBaseDelay = defaultRetryBaseDelay
	}
	if c.MaxRetryDelay <= 0 {
		c.MaxRetryDelay = defaultMaxRetryDelay
	}
	if c.MaxRetryAttempts <= 0 {
		c.MaxRetryAttempts = defaultMaxRetryAttempts
	}
	return nil
}

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

// CloudflareAccessAdapter polls the Cloudflare Access per-request audit log
// endpoint and ships records to LimaCharlie.
type CloudflareAccessAdapter struct {
	conf        CloudflareAccessConfig
	uspClient   uspSink
	client      *CloudflareAccessClient
	deduper     utils.Deduper
	ownsDeduper bool

	// watermark is the start of the next poll's query window. Only the single
	// poll goroutine reads or writes it, so no lock is needed.
	watermark time.Time

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	closeOnce sync.Once
	closeErr  error

	ctx context.Context
}

// NewCloudflareAccessAdapter creates a Cloudflare Access adapter wired to
// LimaCharlie.
func NewCloudflareAccessAdapter(ctx context.Context, conf CloudflareAccessConfig) (*CloudflareAccessAdapter, chan struct{}, error) {
	return newCloudflareAccessAdapter(ctx, conf, nil)
}

// newCloudflareAccessAdapter is the implementation behind
// NewCloudflareAccessAdapter. When sink is non-nil it is used in place of a
// real LimaCharlie client -- the seam tests use to capture shipped events.
func newCloudflareAccessAdapter(ctx context.Context, conf CloudflareAccessConfig, sink uspSink) (*CloudflareAccessAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &CloudflareAccessAdapter{
		conf:      conf,
		ctx:       ctx,
		doStop:    utils.NewEvent(),
		watermark: time.Now().UTC().Add(-conf.InitialLookback),
	}

	// Every poll re-walks its window from the last watermark, and overlapping
	// windows can re-fetch the boundary record; a deduper is required to ship
	// each record exactly once. A deduper may be supplied via the config;
	// otherwise an in-memory one is created (and owned/closed by the adapter).
	a.deduper = conf.Deduper
	if a.deduper == nil {
		window := dedupeBucketWindow
		if window > conf.DedupeTTL {
			window = conf.DedupeTTL
		}
		deduper, err := utils.NewLocalDeduper(window, conf.DedupeTTL)
		if err != nil {
			return nil, nil, fmt.Errorf("cloudflare_access: deduper: %v", err)
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

	a.client = NewCloudflareAccessClient(conf.BaseURL, conf.AccountID, conf.APIToken)
	a.chStopped = make(chan struct{})

	a.wgSenders.Add(1)
	go a.runPoll()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

// Close stops the adapter. It is idempotent: repeated calls are no-ops and
// return the result of the first call.
func (a *CloudflareAccessAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.conf.ClientOptions.DebugLog("cloudflare_access: closing")
		a.doStop.Set()
		a.wgSenders.Wait()
		err1 := a.uspClient.Drain(1 * time.Minute)
		_, err2 := a.uspClient.Close()
		a.client.Close()
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

// runPoll polls forever, until the adapter is asked to stop.
func (a *CloudflareAccessAdapter) runPoll() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("cloudflare_access: polling stopped")

	isFirstRun := true
	for isFirstRun || !a.doStop.WaitFor(a.conf.PollInterval) {
		isFirstRun = false
		a.poll()
	}
}

// poll fetches every record in [watermark, now) once, advancing the watermark
// as it goes so a later poll resumes exactly where this one left off (or, if
// the API errors out mid-window, where the last successful page ended).
//
// Every page within the window is fetched -- the deduper, not an early exit,
// is what keeps a record from being shipped more than once across polls whose
// windows abut or overlap.
func (a *CloudflareAccessAdapter) poll() {
	until := time.Now().UTC()
	since := a.watermark
	if !since.Before(until) {
		return
	}

	nSeen, nShipped := 0, 0

	for page := 0; ; page++ {
		if a.doStop.IsSet() {
			return
		}
		if page >= a.conf.MaxPages {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf(
				"cloudflare_access: hit max_pages=%d before catching up to %s; remaining records "+
					"in this window are collected on a later poll -- raise max_pages if this persists",
				a.conf.MaxPages, until.Format(time.RFC3339)))
			return
		}

		items, ok := a.fetchPage(since, until)
		if !ok {
			// The error has already been reported; abandon this poll (the
			// watermark stays at the last successful point) and try again on
			// the next interval.
			return
		}
		if len(items) == 0 {
			break
		}

		for _, item := range items {
			nSeen++
			if a.deduper.CheckAndAdd(dedupeKey(item)) {
				continue
			}
			if !a.ship(item) {
				return
			}
			nShipped++
		}

		last := items[len(items)-1]
		lastTS, ok2 := recordTime(last)
		if !ok2 {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf(
				"cloudflare_access: could not parse %q on the last record of a page; stopping this poll early", timestampField))
			break
		}

		// A full page whose last record's timestamp did not advance past the
		// start of this fetch means more records than fit in one response
		// share the same instant. Bail out of this poll rather than
		// re-fetching the identical page until max_pages is hit; the deduper
		// has already suppressed re-shipping what was seen.
		if len(items) >= a.conf.Limit && !lastTS.After(since) {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf(
				"cloudflare_access: %d records share timestamp %s (>= limit=%d); some may not be "+
					"collected this poll", len(items), lastTS.Format(time.RFC3339), a.conf.Limit))
			break
		}

		since = lastTS
		a.watermark = since

		if len(items) < a.conf.Limit {
			// A short page means the window is exhausted.
			break
		}
		// A full page: more records may remain between `since` and `until`.
	}

	a.watermark = until
	a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"cloudflare_access: poll complete (seen=%d shipped=%d)", nSeen, nShipped))
}

// fetchPage requests one page of the access_requests window, retrying
// transient failures with exponential backoff. The bool result is false when
// the poll should be abandoned (any source-side error, or the adapter is
// stopping).
//
// Source-side errors (anything the Cloudflare API itself returns: 4xx/5xx,
// auth failures, network blips, malformed responses) are reported via
// OnWarning and NEVER stop the adapter. The cloud-sensor host treats any
// adapter OnError as fatal to the instance -- it tears the adapter down and
// relaunches it, and after enough failures disables it entirely. Restarting
// cannot fix a problem on Cloudflare's side, so we log, skip this poll, and
// let the next interval try again; a recovered token then heals on its own.
// Only a failure delivering to LimaCharlie (see ship) is fatal, since a
// relaunch there reconnects the backend.
func (a *CloudflareAccessAdapter) fetchPage(since, until time.Time) ([]utils.Dict, bool) {
	a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"cloudflare_access: querying base_url=%s account_id=%s since=%s until=%s limit=%d direction=asc",
		a.conf.BaseURL, a.conf.AccountID, since.UTC().Format(time.RFC3339), until.UTC().Format(time.RFC3339), a.conf.Limit))

	var raw []json.RawMessage
	var err error
	for attempt := 0; attempt < a.conf.MaxRetryAttempts; attempt++ {
		if a.doStop.IsSet() {
			return nil, false
		}

		raw, err = a.client.FetchAccessRequests(a.ctx, since, until, a.conf.Limit)
		if err == nil {
			break
		}

		if !isTransientError(err) {
			msg := fmt.Sprintf("cloudflare_access: request failed (skipping this poll): %v", err)
			var httpErr *HTTPError
			if errors.As(err, &httpErr) &&
				(httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden) {
				msg = fmt.Sprintf(
					"cloudflare_access: HTTP %d, token rejected. Verify the API token is scoped to "+
						"Account -> Access: Audit Logs Read for account %q.", httpErr.StatusCode, a.conf.AccountID)
			}
			a.conf.ClientOptions.OnWarning(msg)
			return nil, false
		}

		if attempt+1 >= a.conf.MaxRetryAttempts {
			break
		}
		delay := a.conf.RetryBaseDelay * time.Duration(1<<attempt)
		if delay > a.conf.MaxRetryDelay {
			delay = a.conf.MaxRetryDelay
		}
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"cloudflare_access: transient error (attempt %d/%d), retrying in %v: %v",
			attempt+1, a.conf.MaxRetryAttempts, delay, err))
		if a.doStop.WaitFor(delay) {
			return nil, false
		}
	}
	if err != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"cloudflare_access: failed after %d attempts (skipping this poll): %v", a.conf.MaxRetryAttempts, err))
		return nil, false
	}

	items := make([]utils.Dict, 0, len(raw))
	nDropped := 0
	for _, r := range raw {
		m, err := utils.UnmarshalCleanJSON(string(r))
		if err != nil || len(m) == 0 {
			nDropped++
			continue
		}
		items = append(items, utils.Dict(m))
	}
	a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"cloudflare_access: response had %d raw record(s), %d parsed, %d dropped", len(raw), len(items), nDropped))
	return items, true
}

// ship forwards a single record to LimaCharlie. It returns false if the
// adapter should stop (an unrecoverable shipping error).
func (a *CloudflareAccessAdapter) ship(item utils.Dict) bool {
	msg := &protocol.DataMessage{
		JsonPayload: item,
		EventType:   eventTypeAccessRequests,
		TimestampMs: eventTime(item),
	}
	if err := a.uspClient.Ship(msg, shipTimeout); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("cloudflare_access: stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("cloudflare_access: Ship(): %v", err))
			a.doStop.Set()
			return false
		}
	}
	return true
}

// eventTime extracts a record's event time, falling back to now when
// created_at is absent or unparseable.
func eventTime(item utils.Dict) uint64 {
	if t, ok := recordTime(item); ok {
		return uint64(t.UnixMilli())
	}
	return uint64(time.Now().UnixMilli())
}

// recordTime parses a record's created_at field.
func recordTime(item utils.Dict) (time.Time, bool) {
	raw := item.FindOneString(timestampField)
	if raw == "" {
		return time.Time{}, false
	}
	return parseTimestamp(raw)
}

// dedupeKey returns a stable deduplication key for a record.
func dedupeKey(item utils.Dict) string {
	if id := item.FindOneString(idField); id != "" {
		return id
	}
	// ray_id is expected on every record; a content hash keeps deduplication
	// working even for an anomalous record that lacks one.
	if b, err := json.Marshal(item); err == nil {
		sum := sha256.Sum256(b)
		return "sha256:" + hex.EncodeToString(sum[:])
	}
	return ""
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
