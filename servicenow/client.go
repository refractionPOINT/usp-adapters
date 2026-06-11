// Package usp_servicenow implements a USP adapter for ServiceNow audit and
// system logs, pulled through the ServiceNow REST Table API
// (GET /api/now/v2/table/{tableName}).
//
// ServiceNow keeps its audit telemetry in plain platform tables -- field-level
// change history in sys_audit, per-transaction activity in syslog_transaction,
// system events (including login activity) in sysevent, and so on. The adapter
// models a "feed" as one such table plus an optional encoded-query filter, so
// collecting an additional table is purely a matter of configuration -- no
// code change required.
//
// Each feed is polled incrementally: records are queried in ascending
// sys_created_on order from a per-feed checkpoint, and the checkpoint only
// advances once a poll completes, so transient failures never open a gap. The
// checkpoint query is inclusive (>=) and a per-feed deduper keyed on sys_id
// absorbs the records re-read at the checkpoint boundary.
//
// Events are forwarded to LimaCharlie in their original ServiceNow JSON form;
// the adapter does not reshape payloads.
package usp_servicenow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	defaultPageSize         = 1000
	maxPageSize             = 10000
	defaultPollInterval     = 1 * time.Minute
	defaultBackfill         = 15 * time.Minute
	defaultDedupeTTL        = 7 * 24 * time.Hour
	dedupeBucketWindow      = 1 * time.Hour
	defaultMaxPages         = 100
	defaultTimestampField   = "sys_created_on"
	defaultIDField          = "sys_id"
	defaultMaxRetryAttempts = 3
	defaultRetryBaseDelay   = 5 * time.Second
	defaultMaxRetryDelay    = 30 * time.Second

	shipTimeout = 10 * time.Second

	// serviceNowTimeLayout is the format the Table API uses for date/time
	// values: "yyyy-MM-dd HH:mm:ss". With the default
	// sysparm_display_value=false, these are the database values, which are
	// always UTC.
	serviceNowTimeLayout = "2006-01-02 15:04:05"
)

// timestampLayouts are the time formats accepted for a record's timestamp
// field. The Table API emits "yyyy-MM-dd HH:mm:ss" (UTC); the RFC 3339 forms
// are accepted defensively for customized fields.
var timestampLayouts = []string{
	serviceNowTimeLayout,
	time.RFC3339Nano,
	time.RFC3339,
}

// ServiceNowFeed describes a single ServiceNow table to poll. New event types
// are added by appending feeds -- no code change.
type ServiceNowFeed struct {
	// Name labels the feed and becomes the EventType of every shipped event.
	// Defaults to Table.
	Name string `json:"name" yaml:"name"`

	// Table is the ServiceNow table to read, e.g. "sys_audit",
	// "syslog_transaction", "sysevent".
	Table string `json:"table" yaml:"table"`

	// Query is an optional ServiceNow encoded query ANDed in front of the
	// adapter's own incremental time filter, e.g. "tablename=incident" or
	// "name=login^ORname=login.failed". Column names, operators and values
	// are case-sensitive.
	Query string `json:"query" yaml:"query"`

	// Fields, when set, is sent as sysparm_fields to restrict the columns
	// returned (comma-separated). It must include the feed's timestamp and id
	// fields or incremental polling and deduplication degrade.
	Fields string `json:"fields" yaml:"fields"`

	// TimestampField is the record's event-time column, used both for the
	// incremental checkpoint filter and the shipped event time. Defaults to
	// "sys_created_on" -- the right choice for the insert-only log tables
	// this adapter targets.
	TimestampField string `json:"timestamp_field" yaml:"timestamp_field"`

	// IDField is the record's stable identifier, used for deduplication.
	// Defaults to "sys_id".
	IDField string `json:"id_field" yaml:"id_field"`

	// MaxPages caps how many pages are fetched per poll. The cap does not
	// lose data: records are walked in ascending time order and the
	// checkpoint advances to the last record processed, so a capped poll
	// simply resumes where it left off on the next interval. Defaults to 100.
	MaxPages int `json:"max_pages" yaml:"max_pages"`
}

// ServiceNowConfig is the adapter configuration.
type ServiceNowConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// Instance is the ServiceNow instance name, used to build the API root:
	// https://<instance>.service-now.com
	Instance string `json:"instance" yaml:"instance"`

	// BaseURL fully overrides the API root. When set, Instance is ignored.
	BaseURL string `json:"base_url" yaml:"base_url"`

	// Username / Password authenticate against the instance with HTTP Basic
	// auth (a dedicated service account is recommended). The account needs
	// read access to the polled tables -- sys_audit is readable by the admin
	// and security_admin roles out of the box.
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`

	// Feeds is the set of ServiceNow tables to poll. When empty, the adapter
	// defaults to the sys_audit table (field-level change history).
	Feeds []ServiceNowFeed `json:"feeds" yaml:"feeds"`

	// PageSize is the number of records requested per page (sysparm_limit).
	// Default 1000, maximum 10000.
	PageSize int `json:"page_size" yaml:"page_size"`

	// PollInterval is the wait between polls of a feed. Default 1 minute.
	PollInterval time.Duration `json:"poll_interval" yaml:"poll_interval"`

	// Backfill is how far back the first poll reaches. Default 15 minutes.
	Backfill time.Duration `json:"backfill" yaml:"backfill"`

	// DedupeTTL is how long a record's identifier is remembered to suppress
	// re-shipping it on subsequent polls. Default 7 days.
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

// defaultFeeds returns the out-of-the-box feed set: the sys_audit table,
// ServiceNow's field-level change history (who changed what, with old/new
// values). Other security-relevant tables -- syslog_transaction (every
// user/API transaction with source IP), sysevent (login activity), syslog --
// are added through the Feeds configuration; syslog_transaction in particular
// is high-volume, so it is deliberately not collected by default.
func defaultFeeds() []ServiceNowFeed {
	return []ServiceNowFeed{
		{
			Name:  "sys_audit",
			Table: "sys_audit",
		},
	}
}

func (c *ServiceNowConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Username == "" {
		return errors.New("missing username")
	}
	if c.Password == "" {
		return errors.New("missing password")
	}

	c.BaseURL = strings.TrimSpace(c.BaseURL)
	c.Instance = strings.TrimSpace(c.Instance)
	if c.BaseURL == "" && c.Instance == "" {
		return errors.New("missing instance (or base_url)")
	}

	if c.PageSize <= 0 {
		c.PageSize = defaultPageSize
	}
	if c.PageSize > maxPageSize {
		c.PageSize = maxPageSize
	}
	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.Backfill <= 0 {
		c.Backfill = defaultBackfill
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

	if len(c.Feeds) == 0 {
		c.Feeds = defaultFeeds()
	}
	seenNames := make(map[string]struct{}, len(c.Feeds))
	for i := range c.Feeds {
		f := &c.Feeds[i]
		f.Table = strings.TrimSpace(f.Table)
		if f.Table == "" {
			return fmt.Errorf("feed %d: missing table", i)
		}
		f.Name = strings.TrimSpace(f.Name)
		if f.Name == "" {
			f.Name = f.Table
		}
		if _, ok := seenNames[f.Name]; ok {
			return fmt.Errorf("feed %q: duplicate name", f.Name)
		}
		seenNames[f.Name] = struct{}{}
		if f.TimestampField == "" {
			f.TimestampField = defaultTimestampField
		}
		if f.IDField == "" {
			f.IDField = defaultIDField
		}
		if f.MaxPages <= 0 {
			f.MaxPages = defaultMaxPages
		}
	}
	return nil
}

// uspSink is the subset of *uspclient.Client the adapter depends on.
// Expressing it as an interface lets tests substitute an in-memory sink for
// the real LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

// ServiceNowAdapter polls one or more ServiceNow tables and ships their
// records to LimaCharlie.
type ServiceNowAdapter struct {
	conf        ServiceNowConfig
	uspClient   uspSink
	client      *ServiceNowClient
	deduper     utils.Deduper
	ownsDeduper bool

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	closeOnce sync.Once
	closeErr  error

	ctx context.Context
}

// NewServiceNowAdapter creates a ServiceNow adapter wired to LimaCharlie.
func NewServiceNowAdapter(ctx context.Context, conf ServiceNowConfig) (*ServiceNowAdapter, chan struct{}, error) {
	return newServiceNowAdapter(ctx, conf, nil)
}

// newServiceNowAdapter is the implementation behind NewServiceNowAdapter.
// When sink is non-nil it is used in place of a real LimaCharlie client --
// the seam tests use to capture shipped events.
func newServiceNowAdapter(ctx context.Context, conf ServiceNowConfig, sink uspSink) (*ServiceNowAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &ServiceNowAdapter{
		conf:   conf,
		ctx:    ctx,
		doStop: utils.NewEvent(),
	}

	// The checkpoint query is inclusive (>=), so the records sitting exactly
	// at the checkpoint are re-read on the next poll; the deduper is what
	// keeps them from shipping twice.
	a.deduper = conf.Deduper
	if a.deduper == nil {
		window := dedupeBucketWindow
		if window > conf.DedupeTTL {
			window = conf.DedupeTTL
		}
		deduper, err := utils.NewLocalDeduper(window, conf.DedupeTTL)
		if err != nil {
			return nil, nil, fmt.Errorf("servicenow: deduper: %v", err)
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

	a.client = NewServiceNowClient(resolveBaseURL(conf), conf.Username, conf.Password)
	a.chStopped = make(chan struct{})

	for _, feed := range conf.Feeds {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("servicenow: starting feed %q -> table %s", feed.Name, feed.Table))
		a.wgSenders.Add(1)
		go a.runFeed(feed)
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

// Close stops the adapter. It is idempotent: repeated calls are no-ops and
// return the result of the first call.
func (a *ServiceNowAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.conf.ClientOptions.DebugLog("servicenow: closing")
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

// resolveBaseURL computes the ServiceNow instance root from the config.
func resolveBaseURL(conf ServiceNowConfig) string {
	if conf.BaseURL != "" {
		return strings.TrimRight(conf.BaseURL, "/")
	}
	return fmt.Sprintf("https://%s.service-now.com", strings.Trim(conf.Instance, "/"))
}

// runFeed polls a single feed forever, until the adapter is asked to stop.
// The feed's checkpoint starts Backfill in the past and only moves forward
// when a poll succeeds, so a failed poll is simply retried over the same
// range on the next interval -- no gap.
func (a *ServiceNowAdapter) runFeed(feed ServiceNowFeed) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("servicenow: feed %q stopped", feed.Name))

	checkpoint := time.Now().UTC().Add(-a.conf.Backfill)

	isFirstRun := true
	for isFirstRun || !a.doStop.WaitFor(a.conf.PollInterval) {
		isFirstRun = false
		checkpoint = a.pollFeed(feed, checkpoint)
	}
}

// pollFeed fetches one feed once, walking pages from the checkpoint in
// ascending time order, and returns the new checkpoint.
//
// The end-of-data signal is the Link rel="next" header, not the page's record
// count: ServiceNow applies sysparm_limit before ACL evaluation, so a short
// or even empty page can still be followed by more records.
//
// The returned checkpoint is the timestamp of the newest record processed.
// Because records are walked oldest-first, stopping early (MaxPages) is safe:
// everything up to the new checkpoint has been processed and the next poll
// resumes from there. On failure the original checkpoint is returned so the
// range is retried.
func (a *ServiceNowAdapter) pollFeed(feed ServiceNowFeed, checkpoint time.Time) time.Time {
	nSeen := 0
	nShipped := 0
	maxSeen := checkpoint

	for pageNumber := 1; !a.doStop.IsSet(); pageNumber++ {
		if pageNumber > feed.MaxPages {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf(
				"servicenow: feed %q hit max_pages=%d this poll; the remaining records "+
					"will be collected on the next poll from the advanced checkpoint",
				feed.Name, feed.MaxPages))
			break
		}

		offset := (pageNumber - 1) * a.conf.PageSize
		items, hasNext, ok := a.fetchPage(feed, checkpoint, offset)
		if !ok {
			// The error has already been reported; abandon this poll without
			// advancing the checkpoint so the same range is retried.
			return checkpoint
		}

		for _, item := range items {
			nSeen++
			if raw := item.FindOneString(feed.TimestampField); raw != "" {
				if t, ok := parseTimestamp(raw); ok && t.After(maxSeen) {
					maxSeen = t
				}
			}
			if a.deduper.CheckAndAdd(a.dedupeKey(feed, item)) {
				continue
			}
			if !a.ship(feed, item) {
				return checkpoint
			}
			nShipped++
		}

		if !hasNext {
			break
		}
	}

	a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"servicenow: feed %q poll complete (seen=%d shipped=%d checkpoint=%s)",
		feed.Name, nSeen, nShipped, maxSeen.Format(serviceNowTimeLayout)))
	return maxSeen
}

// fetchPage requests one page of a feed, retrying transient failures with
// exponential backoff (honoring Retry-After on HTTP 429). The bool result is
// false when the poll should be abandoned (a permanent error, or the adapter
// is stopping).
func (a *ServiceNowAdapter) fetchPage(feed ServiceNowFeed, checkpoint time.Time, offset int) ([]utils.Dict, bool, bool) {
	params := a.buildParams(feed, checkpoint, offset)

	var raw []byte
	var hasNext bool
	var err error
	for attempt := 0; attempt < a.conf.MaxRetryAttempts; attempt++ {
		if a.doStop.IsSet() {
			return nil, false, false
		}

		raw, hasNext, err = a.client.GetTable(a.ctx, feed.Table, params)
		if err == nil {
			break
		}

		if !isTransientError(err) {
			a.conf.ClientOptions.OnError(fmt.Errorf("servicenow: feed %q request failed: %v", feed.Name, err))
			// An authentication/authorization failure affects every feed, so
			// stop the whole adapter rather than spin uselessly. Other
			// permanent errors (e.g. a misnamed table) are isolated to this
			// feed: abandon the poll but keep the adapter alive.
			var httpErr *HTTPError
			if errors.As(err, &httpErr) &&
				(httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden) {
				// ServiceNow reports both bad credentials and missing table
				// ACLs this way; surface both possibilities. Reading sys_audit
				// requires the admin or security_admin role (or a custom ACL).
				a.conf.ClientOptions.OnError(fmt.Errorf(
					"servicenow: HTTP %d -- credentials rejected or the account lacks read "+
						"access to table %q. Verify username/password and that the account's "+
						"roles satisfy the table's ACLs (sys_audit requires admin or "+
						"security_admin out of the box).", httpErr.StatusCode, feed.Table))
				a.doStop.Set()
			}
			return nil, false, false
		}

		if attempt+1 >= a.conf.MaxRetryAttempts {
			break
		}
		delay := a.conf.RetryBaseDelay * time.Duration(1<<attempt)
		if delay > a.conf.MaxRetryDelay {
			delay = a.conf.MaxRetryDelay
		}
		// A rate-limited instance tells us exactly how long to back off;
		// honor it even past the configured cap.
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && httpErr.RetryAfter > delay {
			delay = httpErr.RetryAfter
		}
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"servicenow: feed %q transient error (attempt %d/%d), retrying in %v: %v",
			feed.Name, attempt+1, a.conf.MaxRetryAttempts, delay, err))
		if a.doStop.WaitFor(delay) {
			return nil, false, false
		}
	}
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf(
			"servicenow: feed %q failed after %d attempts: %v", feed.Name, a.conf.MaxRetryAttempts, err))
		return nil, false, false
	}

	items, err := extractResult(raw)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("servicenow: feed %q response parse error: %v", feed.Name, err))
		return nil, false, false
	}
	return items, hasNext, true
}

// buildParams assembles the Table API query parameters for one page of a
// feed: the feed's own encoded-query filter ANDed with the incremental
// checkpoint filter, ascending time order, and offset pagination. Database
// (UTC) values are requested rather than display values, and reference-field
// link objects are excluded, so payloads are stable regardless of the service
// account's locale.
func (a *ServiceNowAdapter) buildParams(feed ServiceNowFeed, checkpoint time.Time, offset int) url.Values {
	query := ""
	if feed.Query != "" {
		query = feed.Query + "^"
	}
	query += fmt.Sprintf("%s>=%s^ORDERBY%s",
		feed.TimestampField, checkpoint.UTC().Format(serviceNowTimeLayout), feed.TimestampField)

	params := url.Values{}
	params.Set("sysparm_query", query)
	params.Set("sysparm_limit", strconv.Itoa(a.conf.PageSize))
	params.Set("sysparm_offset", strconv.Itoa(offset))
	params.Set("sysparm_display_value", "false")
	params.Set("sysparm_exclude_reference_link", "true")
	if feed.Fields != "" {
		params.Set("sysparm_fields", feed.Fields)
	}
	return params
}

// ship forwards a single record to LimaCharlie. It returns false if the
// adapter should stop (an unrecoverable shipping error).
func (a *ServiceNowAdapter) ship(feed ServiceNowFeed, item utils.Dict) bool {
	msg := &protocol.DataMessage{
		JsonPayload: item,
		EventType:   feed.Name,
		TimestampMs: a.eventTime(feed, item),
	}
	if err := a.uspClient.Ship(msg, shipTimeout); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("servicenow: stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("servicenow: Ship(): %v", err))
			a.doStop.Set()
			return false
		}
	}
	return true
}

// eventTime extracts the record's event time, falling back to now when the
// configured timestamp field is absent or unparseable.
func (a *ServiceNowAdapter) eventTime(feed ServiceNowFeed, item utils.Dict) uint64 {
	if raw := item.FindOneString(feed.TimestampField); raw != "" {
		if t, ok := parseTimestamp(raw); ok {
			return uint64(t.UnixMilli())
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf(
			"servicenow: feed %q unparseable timestamp %q at field %q", feed.Name, raw, feed.TimestampField))
	}
	return uint64(time.Now().UnixMilli())
}

// dedupeKey returns a stable deduplication key for a record, namespaced by
// feed so identifiers cannot collide across feeds.
func (a *ServiceNowAdapter) dedupeKey(feed ServiceNowFeed, item utils.Dict) string {
	return feed.Name + "|" + recordID(feed, item)
}

// recordID resolves a record's identifier: the feed's IDField (sys_id by
// default), with a content-hash fallback so deduplication still works for a
// record missing it (e.g. a restrictive sysparm_fields).
func recordID(feed ServiceNowFeed, item utils.Dict) string {
	if id := item.FindOneString(feed.IDField); id != "" {
		return id
	}
	if b, err := json.Marshal(item); err == nil {
		sum := sha256.Sum256(b)
		return "sha256:" + hex.EncodeToString(sum[:])
	}
	return ""
}

// extractResult parses a Table API response: a JSON object whose "result" key
// holds the array of records. Each record is decoded with
// utils.UnmarshalCleanJSON, which preserves integer precision (no float
// coercion) so payloads round-trip faithfully. A record that is not a
// non-empty JSON object is skipped rather than failing the whole page.
func extractResult(raw []byte) ([]utils.Dict, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return nil, nil
	}

	var envelope struct {
		Result []json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal([]byte(trimmed), &envelope); err != nil {
		return nil, fmt.Errorf("invalid JSON response: %v", err)
	}
	if envelope.Result == nil && !strings.Contains(trimmed, `"result"`) {
		return nil, errors.New(`response object has no "result" array`)
	}

	items := make([]utils.Dict, 0, len(envelope.Result))
	for _, r := range envelope.Result {
		m, err := utils.UnmarshalCleanJSON(string(r))
		if err != nil || len(m) == 0 {
			continue
		}
		items = append(items, utils.Dict(m))
	}
	return items, nil
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
