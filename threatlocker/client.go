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

	shipTimeout = 10 * time.Second
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
}

// defaultFeeds returns the out-of-the-box feed set: pending Application Control
// approval requests, which is the primary use case (driving automated
// investigations in LimaCharlie).
func defaultFeeds() []ThreatLockerFeed {
	return []ThreatLockerFeed{
		{
			Name: "approval_request",
			URL:  "ApprovalRequest/ApprovalRequestGetByParameters",
			Parameters: utils.Dict{
				// statusId 1 == pending (awaiting an approve/deny decision).
				"statusId":               1,
				"showChildOrganizations": false,
				"showCurrentTierOnly":    false,
			},
			OrderBy:        defaultOrderBy,
			TimestampField: defaultTimestampField,
			IDField:        "approvalRequestId",
		},
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
		c.Feeds = defaultFeeds()
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
	}
	return nil
}

// ThreatLockerAdapter polls one or more ThreatLocker feeds and ships their
// records to LimaCharlie.
type ThreatLockerAdapter struct {
	conf      ThreatLockerConfig
	uspClient *uspclient.Client
	client    *ThreatLockerClient
	deduper   utils.Deduper

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

func NewThreatLockerAdapter(ctx context.Context, conf ThreatLockerConfig) (*ThreatLockerAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &ThreatLockerAdapter{
		conf:   conf,
		ctx:    ctx,
		doStop: utils.NewEvent(),
	}

	// The same pending record reappears on every poll until it is resolved, so
	// a deduper is required to ship each record exactly once.
	window := dedupeBucketWindow
	if window > conf.DedupeTTL {
		window = conf.DedupeTTL
	}
	deduper, err := utils.NewLocalDeduper(window, conf.DedupeTTL)
	if err != nil {
		return nil, nil, fmt.Errorf("threatlocker: deduper: %v", err)
	}
	a.deduper = deduper

	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		a.deduper.Close()
		return nil, nil, err
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

func (a *ThreatLockerAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("threatlocker: closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()
	a.client.Close()
	a.deduper.Close()

	if err1 != nil {
		return err1
	}
	return err2
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

// pollFeed fetches one feed once, walking pages newest-first.
//
// Pagination stops as soon as either:
//   - a short page is returned (fewer records than the page size => last page);
//   - a page contains no records the adapter has not already shipped. Because
//     feeds are queried newest-first, a brand new record always lands on the
//     first page, so the first fully-seen page means everything below is older
//     and already shipped.
func (a *ThreatLockerAdapter) pollFeed(feed ThreatLockerFeed) {
	nSeen := 0
	nShipped := 0

	for pageNumber := 1; !a.doStop.IsSet(); pageNumber++ {
		if feed.MaxPages > 0 && pageNumber > feed.MaxPages {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf(
				"threatlocker: feed %q reached max_pages=%d, ending this poll early", feed.Name, feed.MaxPages))
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

		nNew := 0
		for _, item := range items {
			nSeen++
			if a.deduper.CheckAndAdd(a.dedupeKey(feed, item)) {
				continue
			}
			nNew++
			if !a.ship(feed, item) {
				return
			}
			nShipped++
		}

		if len(items) < a.conf.PageSize {
			break
		}
		if nNew == 0 {
			break
		}
	}

	a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"threatlocker: feed %q poll complete (seen=%d shipped=%d)", feed.Name, nSeen, nShipped))
}

// fetchPage requests one page of a feed, retrying transient failures with
// exponential backoff. The bool result is false when the poll should be
// abandoned (a permanent error, or the adapter is stopping).
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
			a.conf.ClientOptions.OnError(fmt.Errorf("threatlocker: feed %q request failed: %v", feed.Name, err))
			// An authentication/authorization failure affects every feed, so
			// stop the whole adapter rather than spin uselessly. Other
			// permanent errors (e.g. a misconfigured feed URL) are isolated to
			// this feed: abandon the poll but keep the adapter alive.
			var httpErr *HTTPError
			if errors.As(err, &httpErr) &&
				(httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden) {
				a.doStop.Set()
			}
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
		a.conf.ClientOptions.OnError(fmt.Errorf(
			"threatlocker: feed %q failed after %d attempts: %v", feed.Name, a.conf.MaxRetryAttempts, err))
		return nil, false
	}

	items, err := extractItems(raw, feed.ItemsPath)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("threatlocker: feed %q response parse error: %v", feed.Name, err))
		return nil, false
	}
	return items, true
}

// buildRequestBody assembles the JSON body for a "*GetByParameters" call. The
// adapter always controls pagination; sorting is taken from the feed unless the
// feed's Parameters override it.
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
	if n := item.FindOneInt(path); n != 0 {
		return strconv.FormatUint(n, 10)
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
		return rawMessagesToDicts(arr)

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
		return rawMessagesToDicts(arr)

	default:
		return nil, errors.New("response is neither a JSON array nor a JSON object")
	}
}

// rawMessagesToDicts decodes each record with utils.UnmarshalCleanJSON, which
// preserves integer precision (no float coercion) so payloads round-trip
// faithfully.
func rawMessagesToDicts(raw []json.RawMessage) ([]utils.Dict, error) {
	items := make([]utils.Dict, 0, len(raw))
	for i, r := range raw {
		m, err := utils.UnmarshalCleanJSON(string(r))
		if err != nil {
			return nil, fmt.Errorf("record %d is not a JSON object: %v", i, err)
		}
		items = append(items, utils.Dict(m))
	}
	return items, nil
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
