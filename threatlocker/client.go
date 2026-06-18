// Package usp_threatlocker implements a generic USP adapter for the ThreatLocker
// Portal API (https://portalapi.<instance>.threatlocker.com/portalapi).
//
// The ThreatLocker Portal API is uniform: every queryable resource exposes a
// "<Resource>GetByParameters" endpoint that takes a POST with a JSON body
// describing the filter, sort order and pagination, and returns the matching
// records. The adapter models a "feed" as one such endpoint plus its request
// parameters, so supporting a new ThreatLocker event type is purely a matter of
// configuration -- no code change required.
//
// Events are forwarded to LimaCharlie in their original ThreatLocker JSON form;
// the adapter does not reshape payloads.
package usp_threatlocker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultPageSize         = 100
	maxPageSize             = 1000
	defaultPollInterval     = 1 * time.Minute
	defaultDedupeTTL        = 7 * 24 * time.Hour
	dedupeBucketWindow      = 1 * time.Hour
	defaultMaxPages         = 100
	defaultOrderBy          = "dateTime"
	defaultTimestampField   = "dateTime"
	defaultMaxRetryAttempts = 3
	defaultRetryBaseDelay   = 5 * time.Second
	defaultMaxRetryDelay    = 30 * time.Second
	defaultStartDateField   = "startDate"
	defaultEndDateField     = "endDate"
	defaultWindow           = 5 * time.Minute

	shipTimeout = 10 * time.Second

	// windowTimestampLayout is the time format the adapter uses to render
	// rolling-window dates into ThreatLocker request bodies. ThreatLocker's API
	// accepts RFC 3339 with a Z suffix and millisecond precision.
	windowTimestampLayout = "2006-01-02T15:04:05.000Z"

	// Names of the three default feeds. Used both to build the default feed set
	// and to select among it from the per-feed enable options.
	feedApprovalRequest = "approval_request"
	feedUnifiedAudit    = "unified_audit"
	feedSystemAudit     = "system_audit"
)

// itemsArrayKeys are the keys the adapter probes, in order, to locate the list
// of records when a ThreatLocker endpoint wraps its results in an object
// instead of returning a bare JSON array. A feed may override this via its
// ItemsPath setting.
var itemsArrayKeys = []string{"data", "pageItems", "items", "value", "result", "results", "records"}

// commonIDFields are the fields the adapter probes, in order, for a stable
// per-record identifier used for deduplication when a feed does not specify an
// IDField explicitly.
var commonIDFields = []string{"approvalRequestId", "actionId", "auditId", "id", "Id", "guid", "uniqueId"}

// timestampLayouts are the time formats accepted for a record's timestamp
// field. ThreatLocker emits ISO-8601, occasionally without a zone or with
// sub-second precision.
var timestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
}

// ThreatLockerFeed describes a single ThreatLocker "*GetByParameters" endpoint
// to poll. New event types are added by appending feeds -- no code change.
type ThreatLockerFeed struct {
	// Name labels the feed and becomes the EventType of every shipped event.
	Name string `json:"name" yaml:"name"`

	// URL is the API path (relative to the API root) of the GetByParameters
	// endpoint, e.g. "ApprovalRequest/ApprovalRequestGetByParameters".
	URL string `json:"url" yaml:"url"`

	// Parameters are merged into the request body sent to the endpoint. Use it
	// for resource-specific filters, e.g. {"statusId": 1} for pending approval
	// requests. The adapter always sets pageNumber/pageSize itself, and sets
	// orderBy/isAscending unless they are provided here.
	Parameters utils.Dict `json:"parameters" yaml:"parameters"`

	// OrderBy is the field the endpoint sorts on. The adapter relies on a
	// newest-first ordering (isAscending=false) for incremental polling.
	OrderBy string `json:"order_by" yaml:"order_by"`

	// ItemsPath is the key under which the records array lives when the
	// endpoint returns an object envelope. Empty means: response is a bare
	// array, or auto-detect a common key.
	ItemsPath string `json:"items_path" yaml:"items_path"`

	// TimestampField is the path to the record's event time (a "/" separated
	// path is supported for nested fields). Defaults to "dateTime".
	TimestampField string `json:"timestamp_field" yaml:"timestamp_field"`

	// IDField is the path to the record's stable identifier, used for
	// deduplication. Empty means: probe a set of common id fields, and fall
	// back to a content hash.
	IDField string `json:"id_field" yaml:"id_field"`

	// MaxPages caps how many pages are fetched per poll, bounding the work of
	// a first poll against a large historical data set. Defaults to 100.
	MaxPages int `json:"max_pages" yaml:"max_pages"`

	// Window, when > 0, enables rolling time-range filtering on each poll. On
	// each call the adapter sets StartDateField to now-Window-PollInterval and
	// EndDateField to now. The +PollInterval ensures consecutive polls overlap
	// (so a slipped or delayed poll cycle does not leave a gap); the deduper
	// suppresses re-shipping records that fall into the overlap. Endpoints
	// like ActionLog and SystemAudit reject requests that omit a date range.
	Window time.Duration `json:"window" yaml:"window"`

	// StartDateField / EndDateField name the request-body fields that carry
	// the rolling-window endpoints. Defaults: "startDate" / "endDate". Only
	// consulted when Window > 0.
	StartDateField string `json:"start_date_field" yaml:"start_date_field"`
	EndDateField   string `json:"end_date_field" yaml:"end_date_field"`
}

// ThreatLockerConfig is the adapter configuration.
type ThreatLockerConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// APIKey is a ThreatLocker API token (Portal > API Users).
	APIKey string `json:"api_key" yaml:"api_key"`

	// Instance is the ThreatLocker instance identifier used to build the API
	// root: https://portalapi.<instance>.threatlocker.com/portalapi
	Instance string `json:"instance" yaml:"instance"`

	// BaseURL fully overrides the API root. When set, Instance is ignored.
	BaseURL string `json:"base_url" yaml:"base_url"`

	// ManagedOrganizationID, when set, scopes every request to that
	// organization via the managedOrganizationId header (useful for parent
	// organizations querying a specific child).
	ManagedOrganizationID string `json:"managed_organization_id" yaml:"managed_organization_id"`

	// IncludeChildOrganizations, when true, makes the default feeds include
	// child (and grandchild) organizations in their results by flipping on the
	// per-endpoint child-org flags (showChildOrganizations / viewChildOrganizations).
	//
	// Set this when the API token is scoped to a parent/master organization and
	// you want to collect the children's data: a parent has no endpoints of its
	// own, so with this off the approval-request feed is empty, the system-audit
	// feed carries only the adapter's own API activity, and the unified-audit
	// (ActionLog) feed can fail with HTTP 500. This only affects the default
	// feeds; when you supply your own `feeds`, set the flags in each feed's
	// `parameters` yourself.
	IncludeChildOrganizations bool `json:"include_child_organizations" yaml:"include_child_organizations"`

	// CollectApprovalRequests / CollectUnifiedAudit / CollectSystemAudit select
	// which of the three default feeds run. A nil (absent) value means enabled,
	// so the zero-config default collects all three. Set one to false to drop
	// that feed. These are ignored when a custom `feeds` list is supplied (in
	// that case the list itself is authoritative). At least one default feed
	// must remain enabled.
	CollectApprovalRequests *bool `json:"collect_approval_requests" yaml:"collect_approval_requests"`
	CollectUnifiedAudit     *bool `json:"collect_unified_audit" yaml:"collect_unified_audit"`
	CollectSystemAudit      *bool `json:"collect_system_audit" yaml:"collect_system_audit"`

	// Feeds is the set of ThreatLocker endpoints to poll. When empty, the
	// adapter defaults to a single feed of pending Application Control
	// approval requests.
	Feeds []ThreatLockerFeed `json:"feeds" yaml:"feeds"`

	// PageSize is the number of records requested per page. Default 100.
	PageSize int `json:"page_size" yaml:"page_size"`

	// PollInterval is the wait between polls of a feed. Default 1 minute.
	PollInterval time.Duration `json:"poll_interval" yaml:"poll_interval"`

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

// defaultFeeds returns the out-of-the-box feed set. The three feeds together
// cover the three main telemetry surfaces ThreatLocker exposes:
//
//   - approval_request: pending Application Control whitelist requests
//     (statusId=1). Each request is shipped exactly once, well suited to
//     triggering automated investigations in LimaCharlie.
//   - unified_audit: the ActionLog -- ThreatLocker's combined event stream of
//     execute/install/network/registry/file/web/powershell/elevate activity
//     across all modules. Polled on a 5-minute rolling window.
//   - system_audit: portal/administrator activity (logins, policy edits,
//     approval decisions, ...). Polled on a 5-minute rolling window.
//
// ActionLog and SystemAudit both *require* a date range in every request;
// the adapter's Window mechanism rewrites startDate/endDate on each poll, and
// the deduper suppresses re-shipping records that fall into successive
// overlapping windows.
//
// includeChildOrgs controls organization scope. ThreatLocker queries are scoped
// to the "currently managed organization" -- the org that minted the token (or
// the one named by the managedOrganizationId header). When false, each feed
// returns only that org's own records. When true, the child-organization flags
// are flipped on, so a parent/master org sees its children's records too.
//
// This matters for parent-org tokens: a parent has no endpoints of its own, so
// with the flags off `approval_request` returns nothing, `system_audit` returns
// only the adapter's own API activity, and `ActionLog` can fail outright (HTTP
// 500). Flipping the flags on is what makes a parent-org token usable.
func defaultFeeds(includeChildOrgs bool) []ThreatLockerFeed {
	return []ThreatLockerFeed{
		{
			Name: feedApprovalRequest,
			URL:  "ApprovalRequest/ApprovalRequestGetByParameters",
			Parameters: utils.Dict{
				// statusId 1 == pending (awaiting an approve/deny decision).
				"statusId":               1,
				"showChildOrganizations": includeChildOrgs,
				"showCurrentTierOnly":    false,
			},
			OrderBy:        defaultOrderBy,
			TimestampField: defaultTimestampField,
			IDField:        "approvalRequestId",
		},
		{
			Name: feedUnifiedAudit,
			URL:  "ActionLog/ActionLogGetByParametersV2",
			// These body fields are documented as optional in the OpenAPI
			// spec but ThreatLocker's own Postman example sends every one --
			// omitting them yields opaque HTTP 500 responses against some
			// tenants. Keep them populated with their identity defaults.
			Parameters: utils.Dict{
				"paramsFieldsDto":        []any{},
				"groupBys":               []any{},
				"exportMode":             false,
				"showTotalCount":         false,
				"showChildOrganizations": includeChildOrgs,
				"onlyTrueDenies":         false,
				"simulateDeny":           false,
			},
			OrderBy:        defaultOrderBy,
			TimestampField: defaultTimestampField,
			IDField:        "actionLogId",
			Window:         defaultWindow,
		},
		{
			Name: feedSystemAudit,
			URL:  "SystemAudit/SystemAuditGetByParameters",
			Parameters: utils.Dict{
				"viewChildOrganizations": includeChildOrgs,
			},
			OrderBy:        defaultOrderBy,
			TimestampField: defaultTimestampField,
			IDField:        "systemAuditId",
			Window:         defaultWindow,
		},
	}
}

// feedEnabled reports whether a named default feed should run, given the
// per-feed Collect* toggles. A nil toggle (absent from config) means enabled,
// so the zero-config default runs every feed. An unrecognized name is enabled.
func (c *ThreatLockerConfig) feedEnabled(name string) bool {
	on := func(b *bool) bool { return b == nil || *b }
	switch name {
	case feedApprovalRequest:
		return on(c.CollectApprovalRequests)
	case feedUnifiedAudit:
		return on(c.CollectUnifiedAudit)
	case feedSystemAudit:
		return on(c.CollectSystemAudit)
	default:
		return true
	}
}

func (c *ThreatLockerConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.APIKey == "" {
		return errors.New("missing api_key")
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
		feeds := make([]ThreatLockerFeed, 0, 3)
		for _, f := range defaultFeeds(c.IncludeChildOrganizations) {
			if !c.feedEnabled(f.Name) {
				continue
			}
			feeds = append(feeds, f)
		}
		if len(feeds) == 0 {
			return errors.New("all default feeds are disabled; enable at least one (collect_*) or supply custom feeds")
		}
		c.Feeds = feeds
	}
	seenNames := make(map[string]struct{}, len(c.Feeds))
	for i := range c.Feeds {
		f := &c.Feeds[i]
		f.Name = strings.TrimSpace(f.Name)
		f.URL = strings.TrimSpace(strings.TrimPrefix(f.URL, "/"))
		if f.URL == "" {
			return fmt.Errorf("feed %d: missing url", i)
		}
		if f.Name == "" {
			return fmt.Errorf("feed %q: missing name", f.URL)
		}
		if _, ok := seenNames[f.Name]; ok {
			return fmt.Errorf("feed %q: duplicate name", f.Name)
		}
		seenNames[f.Name] = struct{}{}
		if f.OrderBy == "" {
			f.OrderBy = defaultOrderBy
		}
		if f.TimestampField == "" {
			f.TimestampField = defaultTimestampField
		}
		if f.MaxPages <= 0 {
			f.MaxPages = defaultMaxPages
		}
		if f.Window > 0 {
			if f.StartDateField == "" {
				f.StartDateField = defaultStartDateField
			}
			if f.EndDateField == "" {
				f.EndDateField = defaultEndDateField
			}
		}
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

// ThreatLockerAdapter polls one or more ThreatLocker feeds and ships their
// records to LimaCharlie.
type ThreatLockerAdapter struct {
	conf        ThreatLockerConfig
	uspClient   uspSink
	client      *ThreatLockerClient
	deduper     utils.Deduper
	ownsDeduper bool

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	closeOnce sync.Once
	closeErr  error

	ctx context.Context
}

// NewThreatLockerAdapter creates a ThreatLocker adapter wired to LimaCharlie.
func NewThreatLockerAdapter(ctx context.Context, conf ThreatLockerConfig) (*ThreatLockerAdapter, chan struct{}, error) {
	return newThreatLockerAdapter(ctx, conf, nil)
}

// newThreatLockerAdapter is the implementation behind NewThreatLockerAdapter.
// When sink is non-nil it is used in place of a real LimaCharlie client -- the
// seam tests use to capture shipped events.
func newThreatLockerAdapter(ctx context.Context, conf ThreatLockerConfig, sink uspSink) (*ThreatLockerAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &ThreatLockerAdapter{
		conf:   conf,
		ctx:    ctx,
		doStop: utils.NewEvent(),
	}

	// Every page is re-fetched on every poll, so a deduper is required to ship
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
			return nil, nil, fmt.Errorf("threatlocker: deduper: %v", err)
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

	a.client = NewThreatLockerClient(resolveBaseURL(conf), conf.APIKey, conf.ManagedOrganizationID)
	a.chStopped = make(chan struct{})

	for _, feed := range conf.Feeds {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("threatlocker: starting feed %q -> %s", feed.Name, feed.URL))
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
func (a *ThreatLockerAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.conf.ClientOptions.DebugLog("threatlocker: closing")
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

// resolveBaseURL computes the ThreatLocker API root from the config.
func resolveBaseURL(conf ThreatLockerConfig) string {
	if conf.BaseURL != "" {
		return strings.TrimRight(conf.BaseURL, "/")
	}
	instance := strings.Trim(conf.Instance, "/")
	return fmt.Sprintf("https://portalapi.%s.threatlocker.com/portalapi", instance)
}

// runFeed polls a single feed forever, until the adapter is asked to stop.
func (a *ThreatLockerAdapter) runFeed(feed ThreatLockerFeed) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("threatlocker: feed %q stopped", feed.Name))

	isFirstRun := true
	for isFirstRun || !a.doStop.WaitFor(a.conf.PollInterval) {
		isFirstRun = false
		a.pollFeed(feed)
	}
}

// pollFeed fetches one feed once. It walks pages until the result set is
// exhausted (a short or empty page) or the feed's MaxPages cap is reached.
//
// Every page is fetched on every poll; the deduper -- not an early exit -- is
// what keeps a record from being shipped more than once. This is deliberate:
// the ThreatLocker API paginates by offset over a live, mutable list, so a
// record can shift across a page boundary between two page fetches. Stopping
// early on a page of already-seen records would let such a shifted record be
// skipped permanently. Re-walking every page each poll costs more requests but
// is correct; bound it per feed with MaxPages and the feed's parameters.
func (a *ThreatLockerAdapter) pollFeed(feed ThreatLockerFeed) {
	nSeen := 0
	nShipped := 0

	for pageNumber := 1; !a.doStop.IsSet(); pageNumber++ {
		if pageNumber > feed.MaxPages {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf(
				"threatlocker: feed %q hit max_pages=%d; records past that are not "+
					"collected this poll -- raise max_pages or narrow the feed's parameters",
				feed.Name, feed.MaxPages))
			break
		}

		items, ok := a.fetchPage(feed, pageNumber)
		if !ok {
			// The error has already been reported; abandon this poll and try
			// again on the next interval.
			return
		}
		if len(items) == 0 {
			break
		}

		for _, item := range items {
			nSeen++
			if a.deduper.CheckAndAdd(a.dedupeKey(feed, item)) {
				continue
			}
			if !a.ship(feed, item) {
				return
			}
			nShipped++
		}

		// A short page is the end of the result set.
		if len(items) < a.conf.PageSize {
			break
		}
	}

	a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"threatlocker: feed %q poll complete (seen=%d shipped=%d)", feed.Name, nSeen, nShipped))
}

// fetchPage requests one page of a feed, retrying transient failures with
// exponential backoff. The bool result is false when the poll should be
// abandoned (any source-side error, or the adapter is stopping).
//
// Source-side errors (anything that comes back from the ThreatLocker API:
// 4xx/5xx, auth failures, network blips, malformed responses) are reported via
// OnWarning and NEVER stop the adapter. This is deliberate: the cloud-sensor
// host treats any adapter OnError as fatal to the instance -- it tears the
// adapter down and relaunches it, and after enough failures disables it
// entirely. Restarting cannot fix a problem on ThreatLocker's side, and it
// would take the adapter's healthy feeds down along with the broken one. So we
// log, skip just this poll, and let the feed try again next interval; a
// recovered token or endpoint then heals on its own. Only a failure delivering
// to LimaCharlie (see ship) is fatal, since a relaunch reconnects the backend.
func (a *ThreatLockerAdapter) fetchPage(feed ThreatLockerFeed, pageNumber int) ([]utils.Dict, bool) {
	body := a.buildRequestBody(feed, pageNumber)

	var raw []byte
	var err error
	for attempt := 0; attempt < a.conf.MaxRetryAttempts; attempt++ {
		if a.doStop.IsSet() {
			return nil, false
		}

		raw, err = a.client.Post(a.ctx, feed.URL, body)
		if err == nil {
			break
		}

		if !isTransientError(err) {
			// A permanent source-side error (bad request, auth failure, a
			// misconfigured feed URL, ...). Reported as a warning, not a fatal
			// OnError -- see the function doc for why we never stop on a
			// source-side problem.
			msg := fmt.Sprintf("threatlocker: feed %q request failed (skipping this poll): %v", feed.Name, err)
			var httpErr *HTTPError
			if errors.As(err, &httpErr) &&
				(httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden) {
				// ThreatLocker returns the same TOKEN_REVOKED response for a
				// genuinely revoked token and for a token presented to the
				// wrong instance. Surface that ambiguity so an operator does
				// not chase a token rotation when the real fix is to flip
				// `instance` to the letter their portal shows next to
				// "ThreatLocker Access" under the Help menu.
				msg = fmt.Sprintf(
					"threatlocker: feed %q -- HTTP %d, token rejected. If the token is known to be "+
						"active, verify `instance` matches the letter shown in the ThreatLocker Portal "+
						"(Help > 'ThreatLocker Access (X)'); a token from a different instance is "+
						"reported as TOKEN_REVOKED.", feed.Name, httpErr.StatusCode)
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
			"threatlocker: feed %q transient error (attempt %d/%d), retrying in %v: %v",
			feed.Name, attempt+1, a.conf.MaxRetryAttempts, delay, err))
		if a.doStop.WaitFor(delay) {
			return nil, false
		}
	}
	if err != nil {
		// Transient failures persisted across every retry. Log and skip this
		// poll (a warning, not a fatal OnError -- see the function doc); the
		// next interval tries again.
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"threatlocker: feed %q failed after %d attempts (skipping this poll): %v",
			feed.Name, a.conf.MaxRetryAttempts, err))
		return nil, false
	}

	items, err := extractItems(raw, feed.ItemsPath)
	if err != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"threatlocker: feed %q response parse error (skipping this poll): %v", feed.Name, err))
		return nil, false
	}
	return items, true
}

// buildRequestBody assembles the JSON body for a "*GetByParameters" call. The
// adapter always controls pagination; sorting is taken from the feed unless the
// feed's Parameters override it. When the feed has Window > 0, the rolling
// window's start/end timestamps are injected too, so a poll always covers
// [now-Window-PollInterval, now] -- the small overlap with the prior poll is
// absorbed by the deduper.
func (a *ThreatLockerAdapter) buildRequestBody(feed ThreatLockerFeed, pageNumber int) utils.Dict {
	body := utils.Dict{}
	for k, v := range feed.Parameters {
		body[k] = v
	}
	body["pageNumber"] = pageNumber
	body["pageSize"] = a.conf.PageSize
	if _, ok := body["orderBy"]; !ok && feed.OrderBy != "" {
		body["orderBy"] = feed.OrderBy
	}
	if _, ok := body["isAscending"]; !ok {
		body["isAscending"] = false
	}
	if feed.Window > 0 {
		end := time.Now().UTC()
		start := end.Add(-feed.Window - a.conf.PollInterval)
		body[feed.StartDateField] = start.Format(windowTimestampLayout)
		body[feed.EndDateField] = end.Format(windowTimestampLayout)
	}
	return body
}

// ship forwards a single record to LimaCharlie. It returns false if the adapter
// should stop (an unrecoverable shipping error).
func (a *ThreatLockerAdapter) ship(feed ThreatLockerFeed, item utils.Dict) bool {
	msg := &protocol.DataMessage{
		JsonPayload: item,
		EventType:   feed.Name,
		TimestampMs: a.eventTime(feed, item),
	}
	if err := a.uspClient.Ship(msg, shipTimeout); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("threatlocker: stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("threatlocker: Ship(): %v", err))
			a.doStop.Set()
			return false
		}
	}
	return true
}

// eventTime extracts the record's event time, falling back to now when the
// configured timestamp field is absent or unparseable.
func (a *ThreatLockerAdapter) eventTime(feed ThreatLockerFeed, item utils.Dict) uint64 {
	field := feed.TimestampField
	if field == "" {
		field = defaultTimestampField
	}
	if raw := item.FindOneString(field); raw != "" {
		if t, ok := parseTimestamp(raw); ok {
			return uint64(t.UnixMilli())
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf(
			"threatlocker: feed %q unparseable timestamp %q at field %q", feed.Name, raw, field))
	}
	return uint64(time.Now().UnixMilli())
}

// dedupeKey returns a stable deduplication key for a record, namespaced by feed
// so identifiers cannot collide across feeds.
func (a *ThreatLockerAdapter) dedupeKey(feed ThreatLockerFeed, item utils.Dict) string {
	return feed.Name + "|" + recordID(feed, item)
}

// recordID resolves a record's identifier: the feed's IDField if set, otherwise
// a probe of common id fields, otherwise a content hash so deduplication still
// works for records with no recognizable identifier.
func recordID(feed ThreatLockerFeed, item utils.Dict) string {
	if feed.IDField != "" {
		if id := fieldAsString(item, feed.IDField); id != "" {
			return id
		}
	}
	for _, k := range commonIDFields {
		if id := fieldAsString(item, k); id != "" {
			return id
		}
	}
	if b, err := json.Marshal(item); err == nil {
		sum := sha256.Sum256(b)
		return "sha256:" + hex.EncodeToString(sum[:])
	}
	return ""
}

// fieldAsString reads a field as a string, accepting numeric values too (ids
// are sometimes integers).
func fieldAsString(item utils.Dict, path string) string {
	if s := item.FindOneString(path); s != "" {
		return s
	}
	// FindInt returns every match at the path; a non-empty result means the
	// field is present -- including a legitimate value of 0, which FindOneInt
	// cannot distinguish from "absent".
	if ints := item.FindInt(path); len(ints) > 0 {
		return strconv.FormatUint(ints[0], 10)
	}
	return ""
}

// extractItems parses a ThreatLocker response into a slice of records. It
// accepts both a bare JSON array and an object envelope; for an envelope it
// uses itemsPath when provided, otherwise it auto-detects a common key.
func extractItems(raw []byte, itemsPath string) ([]utils.Dict, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return nil, nil
	}

	switch trimmed[0] {
	case '[':
		var arr []json.RawMessage
		if err := json.Unmarshal([]byte(trimmed), &arr); err != nil {
			return nil, fmt.Errorf("invalid JSON array: %v", err)
		}
		return rawMessagesToDicts(arr), nil

	case '{':
		var obj map[string]json.RawMessage
		if err := json.Unmarshal([]byte(trimmed), &obj); err != nil {
			return nil, fmt.Errorf("invalid JSON object: %v", err)
		}
		var arrRaw json.RawMessage
		if itemsPath != "" {
			arrRaw = obj[itemsPath]
			if arrRaw == nil {
				return nil, fmt.Errorf("items_path %q not present in response object (keys: %v)", itemsPath, keysOf(obj))
			}
		} else {
			for _, k := range itemsArrayKeys {
				if v, ok := obj[k]; ok {
					arrRaw = v
					break
				}
			}
			if arrRaw == nil {
				return nil, fmt.Errorf("could not locate a records array in response object (keys: %v); set items_path", keysOf(obj))
			}
		}
		var arr []json.RawMessage
		if err := json.Unmarshal(arrRaw, &arr); err != nil {
			return nil, fmt.Errorf("response field is not an array: %v", err)
		}
		return rawMessagesToDicts(arr), nil

	default:
		return nil, errors.New("response is neither a JSON array nor a JSON object")
	}
}

// rawMessagesToDicts decodes each record with utils.UnmarshalCleanJSON, which
// preserves integer precision (no float coercion) so payloads round-trip
// faithfully. A record that is not a non-empty JSON object (a scalar, an array,
// null, or {}) is skipped rather than failing the whole page -- one anomalous
// element should not block every other record.
func rawMessagesToDicts(raw []json.RawMessage) []utils.Dict {
	items := make([]utils.Dict, 0, len(raw))
	for _, r := range raw {
		m, err := utils.UnmarshalCleanJSON(string(r))
		if err != nil || len(m) == 0 {
			continue
		}
		items = append(items, utils.Dict(m))
	}
	return items
}

func keysOf(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
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
