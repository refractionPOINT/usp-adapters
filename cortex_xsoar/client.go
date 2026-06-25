// Package usp_cortex_xsoar implements a USP adapter for Palo Alto Networks
// Cortex XSOAR (formerly Demisto). It polls the "search incidents by filter"
// endpoint and ships each incident to LimaCharlie in its original XSOAR JSON
// form; the adapter does not reshape payloads.
//
// The same code serves both Cortex XSOAR 6 (on-prem) and Cortex XSOAR 8 (cloud):
// the request and response shapes are identical, only the base path
// (/xsoar/public/v1 on v8) and the authentication headers differ, both selected
// from configuration. See README.md for the API references this is built against.
//
// Incremental collection is by the incident "modified" time: each poll fetches
// incidents modified at or after a high-water cursor, sorted ascending, and
// advances the cursor to the newest "modified" seen. Because an incident's
// "modified" time changes whenever it is updated (re-opened, re-assigned, a new
// note added, ...), updated incidents are re-collected -- deduplication is keyed
// on id+modified so each distinct version ships exactly once.
package usp_cortex_xsoar

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
	maxPageSize             = 1000 // XSOAR caps a search page at 1000 records.
	defaultPollInterval     = 1 * time.Minute
	defaultDedupeTTL        = 7 * 24 * time.Hour
	dedupeBucketWindow      = 1 * time.Hour
	defaultMaxPages         = 200
	defaultInitialLookback  = 24 * time.Hour
	defaultEventType        = "incident"
	defaultTimestampField   = "modified"
	defaultMaxRetryAttempts = 3
	defaultRetryBaseDelay   = 5 * time.Second
	defaultMaxRetryDelay    = 30 * time.Second

	// searchIncidentsPath is the endpoint, relative to the resolved API root, that
	// searches incidents by filter. It is the same on XSOAR 6 and 8; the v8
	// /xsoar/public/v1 prefix is folded into the resolved base URL.
	searchIncidentsPath = "incidents/search"

	// cursorLayout is the time format used to render the modified-time cursor into
	// the Lucene query. XSOAR accepts RFC 3339 / ISO-8601; second precision with a
	// Z suffix is unambiguous and avoids fractional-second quoting concerns.
	cursorLayout = "2006-01-02T15:04:05Z"

	shipTimeout = 10 * time.Second
)

// idFields are the fields probed, in order, for a stable per-incident identifier
// used for deduplication. XSOAR incidents carry "id"; the others are defensive.
var idFields = []string{"id", "investigationId", "_id"}

// timestampLayouts are the formats accepted for an incident time field. XSOAR
// serializes Go time.Time values: RFC 3339 with a variable number of fractional
// digits and either a Z or a numeric offset (e.g. "2020-09-29T17:29:44.162034+03:00"
// or "2022-04-27T08:37:29.197107197Z"). RFC3339Nano covers both; the bare RFC3339
// and offset-less forms are kept as fallbacks.
var timestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
}

// XSOARConfig is the adapter configuration.
type XSOARConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// URL is the XSOAR server base URL.
	//   - XSOAR 6 (on-prem): the server root, e.g. "https://xsoar.example.com".
	//   - XSOAR 8 (cloud): the host shown by the API key's "Copy URL" action,
	//     e.g. "https://api-your-tenant.xdr.us.paloaltonetworks.com". The
	//     /xsoar/public/v1 path is added automatically when api_version is "8".
	URL string `json:"url" yaml:"url"`

	// APIKey is the API key value (Settings -> Integrations -> API Keys).
	APIKey string `json:"api_key" yaml:"api_key"`

	// APIKeyID is the numeric ID of the API key (the "ID" column in the API Keys
	// table), sent as the x-xdr-auth-id header. Required for XSOAR 8 and for
	// advanced keys; ignored for an XSOAR 6 standard key.
	APIKeyID string `json:"api_key_id" yaml:"api_key_id"`

	// APIVersion selects the API surface: "6" (default, endpoints at the server
	// root) or "8" (endpoints under /xsoar/public/v1).
	APIVersion string `json:"api_version" yaml:"api_version"`

	// Advanced selects the advanced (signed) API key authentication scheme. Set
	// this when the key was created as an Advanced key. Requires APIKeyID.
	Advanced bool `json:"advanced" yaml:"advanced"`

	// BasePath overrides the API path prefix added to URL. Empty means: derive
	// from APIVersion ("/xsoar/public/v1" for "8", none for "6"). Provided as an
	// escape hatch for non-standard deployments.
	BasePath string `json:"base_path" yaml:"base_path"`

	// Query is an optional Lucene filter ANDed with the modified-time cursor on
	// every poll, e.g. `type:Phishing and severity:>=2` to collect only certain
	// incidents. Empty means: collect every incident.
	Query string `json:"query" yaml:"query"`

	// EventType is the EventType stamped on every shipped message. Default
	// "incident".
	EventType string `json:"event_type" yaml:"event_type"`

	// TimestampField is the incident field used as the event time. Default
	// "modified" (so an updated incident is timestamped at its update). Set to
	// "created" or "occurred" to use the incident's birth/occurrence time.
	TimestampField string `json:"timestamp_field" yaml:"timestamp_field"`

	// InitialLookback is how far back the first poll reaches. Subsequent polls
	// continue from the cursor. Default 24h.
	InitialLookback time.Duration `json:"initial_lookback" yaml:"initial_lookback"`

	// PageSize is the number of incidents requested per page. Default 100, capped
	// at 1000 by XSOAR.
	PageSize int `json:"page_size" yaml:"page_size"`

	// PollInterval is the wait between polls. Default 1 minute.
	PollInterval time.Duration `json:"poll_interval" yaml:"poll_interval"`

	// MaxPages caps how many pages are fetched per poll, bounding the work of a
	// first poll against a large backlog. Default 200.
	MaxPages int `json:"max_pages" yaml:"max_pages"`

	// DedupeTTL is how long an incident version (id+modified) is remembered to
	// suppress re-shipping it on subsequent overlapping polls. Default 7 days.
	DedupeTTL time.Duration `json:"dedupe_ttl" yaml:"dedupe_ttl"`

	// InsecureSkipVerify disables TLS certificate verification. Only for on-prem
	// XSOAR 6 instances behind a self-signed certificate; leave off otherwise.
	InsecureSkipVerify bool `json:"insecure_skip_verify" yaml:"insecure_skip_verify"`

	// Retry tuning for transient API failures.
	RetryBaseDelay   time.Duration `json:"retry_base_delay" yaml:"retry_base_delay"`
	MaxRetryDelay    time.Duration `json:"max_retry_delay" yaml:"max_retry_delay"`
	MaxRetryAttempts int           `json:"max_retry_attempts" yaml:"max_retry_attempts"`

	// Deduper, when set, replaces the built-in in-memory deduper. It is not
	// settable through a config file; it exists as a seam for tests and for
	// embedders that want to supply a shared deduper.
	Deduper utils.Deduper `json:"-" yaml:"-"`
}

func (c *XSOARConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.APIKey == "" {
		return errors.New("missing api_key")
	}
	c.URL = strings.TrimSpace(c.URL)
	if c.URL == "" {
		return errors.New("missing url")
	}

	c.APIVersion = strings.TrimSpace(c.APIVersion)
	if c.APIVersion == "" {
		c.APIVersion = "6"
	}
	if c.APIVersion != "6" && c.APIVersion != "8" {
		return fmt.Errorf("unsupported api_version %q (expected \"6\" or \"8\")", c.APIVersion)
	}
	// XSOAR 8 always authenticates with an auth ID; an advanced key (either
	// version) needs it to sign. Only an XSOAR 6 standard key may omit it.
	if c.Advanced && c.APIKeyID == "" {
		return errors.New("advanced api key requires api_key_id")
	}
	if c.APIVersion == "8" && c.APIKeyID == "" {
		return errors.New("api_version 8 requires api_key_id (the x-xdr-auth-id of the API key)")
	}

	if c.EventType == "" {
		c.EventType = defaultEventType
	}
	if c.TimestampField == "" {
		c.TimestampField = defaultTimestampField
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
	if c.MaxPages <= 0 {
		c.MaxPages = defaultMaxPages
	}
	if c.InitialLookback <= 0 {
		c.InitialLookback = defaultInitialLookback
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

// XSOARAdapter polls Cortex XSOAR incidents and ships them to LimaCharlie.
type XSOARAdapter struct {
	conf        XSOARConfig
	uspClient   uspSink
	client      *XSOARClient
	deduper     utils.Deduper
	ownsDeduper bool

	// cursor is the modified-time high-water mark; only touched by the single
	// poll goroutine, so it needs no synchronization.
	cursor time.Time

	chStopped chan struct{}
	wg        sync.WaitGroup
	doStop    *utils.Event

	closeOnce sync.Once
	closeErr  error

	ctx context.Context
}

// NewXSOARAdapter creates a Cortex XSOAR adapter wired to LimaCharlie.
func NewXSOARAdapter(ctx context.Context, conf XSOARConfig) (*XSOARAdapter, chan struct{}, error) {
	return newXSOARAdapter(ctx, conf, nil)
}

// newXSOARAdapter is the implementation behind NewXSOARAdapter. When sink is
// non-nil it is used in place of a real LimaCharlie client -- the seam tests use
// to capture shipped events.
func newXSOARAdapter(ctx context.Context, conf XSOARConfig, sink uspSink) (*XSOARAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &XSOARAdapter{
		conf:   conf,
		ctx:    ctx,
		doStop: utils.NewEvent(),
		cursor: time.Now().UTC().Add(-conf.InitialLookback),
	}

	// The modified-time window overlaps successive polls (>= cursor), and a live
	// incident list can shift across page boundaries mid-poll, so a deduper is
	// required to ship each incident version exactly once.
	a.deduper = conf.Deduper
	if a.deduper == nil {
		window := dedupeBucketWindow
		if window > conf.DedupeTTL {
			window = conf.DedupeTTL
		}
		deduper, err := utils.NewLocalDeduper(window, conf.DedupeTTL)
		if err != nil {
			return nil, nil, fmt.Errorf("cortex_xsoar: deduper: %v", err)
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

	a.client = NewXSOARClient(resolveBaseURL(conf), conf.APIKey, conf.APIKeyID, conf.Advanced, conf.InsecureSkipVerify)
	a.chStopped = make(chan struct{})

	a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"cortex_xsoar: starting (api_version=%s base=%s)", conf.APIVersion, a.client.baseURL))
	a.wg.Add(1)
	go a.run()

	go func() {
		a.wg.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

// Close stops the adapter. It is idempotent: repeated calls are no-ops and return
// the result of the first call.
func (a *XSOARAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.conf.ClientOptions.DebugLog("cortex_xsoar: closing")
		a.doStop.Set()
		a.wg.Wait()
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

// resolveBaseURL computes the API root from the config: the configured URL plus
// the version-derived (or explicitly overridden) path prefix.
func resolveBaseURL(conf XSOARConfig) string {
	base := strings.TrimRight(strings.TrimSpace(conf.URL), "/")
	prefix := strings.Trim(strings.TrimSpace(conf.BasePath), "/")
	if conf.BasePath == "" && conf.APIVersion == "8" {
		prefix = "xsoar/public/v1"
	}
	if prefix != "" {
		base = base + "/" + prefix
	}
	return base
}

// run polls forever, until the adapter is asked to stop.
func (a *XSOARAdapter) run() {
	defer a.wg.Done()
	defer a.conf.ClientOptions.DebugLog("cortex_xsoar: poll loop stopped")

	isFirstRun := true
	for isFirstRun || !a.doStop.WaitFor(a.conf.PollInterval) {
		isFirstRun = false
		a.poll()
	}
}

// poll fetches incidents modified since the cursor, ships each new version, and
// advances the cursor to the newest "modified" observed. Pages are walked until
// the result set is exhausted (a short page, or the reported total is reached) or
// MaxPages is hit. On any source-side error the poll is abandoned without
// advancing the cursor, so the next interval retries from the same point.
func (a *XSOARAdapter) poll() {
	query := buildQuery(a.conf.Query, a.cursor)
	maxModified := a.cursor
	nSeen, nShipped, nCollected := 0, 0, 0

	for page := 0; !a.doStop.IsSet(); page++ {
		if page >= a.conf.MaxPages {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf(
				"cortex_xsoar: hit max_pages=%d; incidents past that are collected on a later poll "+
					"as the cursor advances -- raise max_pages or narrow the query to catch up faster",
				a.conf.MaxPages))
			break
		}

		incidents, total, ok := a.fetchPage(query, page)
		if !ok {
			// The error has already been reported; abandon this poll (cursor
			// unchanged) and try again on the next interval.
			return
		}
		if len(incidents) == 0 {
			break
		}

		for _, inc := range incidents {
			nSeen++
			if a.deduper.CheckAndAdd(a.dedupeKey(inc)) {
				continue
			}
			if !a.ship(inc) {
				return
			}
			nShipped++
			if t, ok := incidentModified(inc); ok && t.After(maxModified) {
				maxModified = t
			}
		}

		nCollected += len(incidents)
		// A short page is the end of the result set.
		if len(incidents) < a.conf.PageSize {
			break
		}
		// Or we have collected everything the server says matched.
		if total > 0 && nCollected >= total {
			break
		}
	}

	// Advance the cursor forward only. Using the newest modified time seen (with a
	// >= query and id+modified dedup) means same-millisecond boundary incidents
	// are re-queried next poll but not re-shipped, so none are skipped.
	if maxModified.After(a.cursor) {
		a.cursor = maxModified
	}

	a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"cortex_xsoar: poll complete (seen=%d shipped=%d cursor=%s)",
		nSeen, nShipped, a.cursor.Format(cursorLayout)))
}

// fetchPage requests one page of incidents, retrying transient failures with
// exponential backoff. The bool result is false when the poll should be abandoned
// (any source-side error, or the adapter is stopping).
//
// Source-side errors (anything the XSOAR API returns: 4xx/5xx, auth failures,
// network blips, malformed responses) are reported via OnWarning and NEVER stop
// the adapter. This is deliberate: the cloud-sensor host treats any adapter
// OnError as fatal to the instance -- it tears the adapter down, relaunches it,
// and after enough failures disables it entirely. Restarting cannot fix a problem
// on XSOAR's side (a revoked key, a clock-skewed advanced signature, an
// unreachable instance), so we log, skip this poll, and let the next interval try
// again; a fixed key or healed instance then recovers on its own. Only a failure
// delivering to LimaCharlie (see ship) is fatal, since a relaunch reconnects the
// backend.
func (a *XSOARAdapter) fetchPage(query string, page int) ([]utils.Dict, int, bool) {
	body := buildRequestBody(query, page, a.conf.PageSize)

	var raw []byte
	var err error
	for attempt := 0; attempt < a.conf.MaxRetryAttempts; attempt++ {
		if a.doStop.IsSet() {
			return nil, 0, false
		}

		raw, err = a.client.Post(a.ctx, searchIncidentsPath, body)
		if err == nil {
			break
		}

		if !isTransientError(err) {
			msg := fmt.Sprintf("cortex_xsoar: incident search failed (skipping this poll): %v", err)
			var httpErr *HTTPError
			if errors.As(err, &httpErr) &&
				(httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden) {
				msg = fmt.Sprintf(
					"cortex_xsoar: incident search rejected with HTTP %d. Verify the api_key (and "+
						"api_key_id for XSOAR 8 / advanced keys), that the key's role has API access, "+
						"and -- for an advanced key -- that this host's clock is in sync (the signature "+
						"is time-validated).", httpErr.StatusCode)
			}
			a.conf.ClientOptions.OnWarning(msg)
			return nil, 0, false
		}

		if attempt+1 >= a.conf.MaxRetryAttempts {
			break
		}
		delay := a.conf.RetryBaseDelay * time.Duration(1<<attempt)
		if delay > a.conf.MaxRetryDelay {
			delay = a.conf.MaxRetryDelay
		}
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"cortex_xsoar: transient error (attempt %d/%d), retrying in %v: %v",
			attempt+1, a.conf.MaxRetryAttempts, delay, err))
		if a.doStop.WaitFor(delay) {
			return nil, 0, false
		}
	}
	if err != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"cortex_xsoar: incident search failed after %d attempts (skipping this poll): %v",
			a.conf.MaxRetryAttempts, err))
		return nil, 0, false
	}

	incidents, total, err := extractIncidents(raw)
	if err != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"cortex_xsoar: response parse error (skipping this poll): %v", err))
		return nil, 0, false
	}
	return incidents, total, true
}

// ship forwards a single incident to LimaCharlie. It returns false if the adapter
// should stop (an unrecoverable shipping error).
func (a *XSOARAdapter) ship(inc utils.Dict) bool {
	msg := &protocol.DataMessage{
		JsonPayload: inc,
		EventType:   a.conf.EventType,
		TimestampMs: a.eventTime(inc),
	}
	if err := a.uspClient.Ship(msg, shipTimeout); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("cortex_xsoar: stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("cortex_xsoar: Ship(): %v", err))
			a.doStop.Set()
			return false
		}
	}
	return true
}

// eventTime extracts the incident's event time from the configured field,
// falling back to "modified" and then to now.
func (a *XSOARAdapter) eventTime(inc utils.Dict) uint64 {
	for _, field := range []string{a.conf.TimestampField, defaultTimestampField} {
		if field == "" {
			continue
		}
		if raw := inc.FindOneString(field); raw != "" {
			if t, ok := parseTimestamp(raw); ok {
				return uint64(t.UnixMilli())
			}
		}
	}
	return uint64(time.Now().UnixMilli())
}

// dedupeKey returns a stable deduplication key for an incident version. Keying on
// id+modified means an incident ships once per distinct version: re-fetched
// boundary records and unchanged re-polls are suppressed, but a genuine update
// (which changes "modified") ships again.
func (a *XSOARAdapter) dedupeKey(inc utils.Dict) string {
	return incidentID(inc) + "|" + inc.FindOneString("modified")
}

// incidentModified parses an incident's "modified" time.
func incidentModified(inc utils.Dict) (time.Time, bool) {
	return parseTimestamp(inc.FindOneString("modified"))
}

// incidentID resolves an incident's identifier: the "id" field (string or
// numeric), otherwise a probe of fallback fields, otherwise a content hash so
// deduplication still works for a record with no recognizable identifier.
func incidentID(inc utils.Dict) string {
	for _, k := range idFields {
		if id := fieldAsString(inc, k); id != "" {
			return id
		}
	}
	if b, err := json.Marshal(inc); err == nil {
		sum := sha256.Sum256(b)
		return "sha256:" + hex.EncodeToString(sum[:])
	}
	return ""
}

// fieldAsString reads a field as a string, accepting numeric values too (ids are
// sometimes serialized as numbers).
func fieldAsString(inc utils.Dict, path string) string {
	if s := inc.FindOneString(path); s != "" {
		return s
	}
	// FindInt returns every match at the path; a non-empty result means the field
	// is present -- including a legitimate value of 0, which a string read cannot
	// distinguish from "absent".
	if ints := inc.FindInt(path); len(ints) > 0 {
		return strconv.FormatUint(ints[0], 10)
	}
	return ""
}

// buildQuery assembles the Lucene query for one poll: incidents modified at or
// after the cursor, ANDed with any operator-supplied filter.
func buildQuery(userQuery string, cursor time.Time) string {
	modifiedClause := fmt.Sprintf(`modified:>="%s"`, cursor.UTC().Format(cursorLayout))
	userQuery = strings.TrimSpace(userQuery)
	if userQuery == "" {
		return modifiedClause
	}
	return fmt.Sprintf("(%s) and %s", userQuery, modifiedClause)
}

// buildRequestBody assembles the JSON body for a /incidents/search call: the
// filter with the query, an ascending sort on "modified" (so the cursor advances
// monotonically), and 0-indexed pagination.
func buildRequestBody(query string, page, size int) utils.Dict {
	return utils.Dict{
		"filter": utils.Dict{
			"query": query,
			"sort": []utils.Dict{
				{"field": "modified", "asc": true},
			},
			"page": page,
			"size": size,
		},
	}
}

// extractIncidents parses a /incidents/search response into the incident slice
// and the reported total. The documented envelope is {"data": [...], "total": N};
// a bare array is also accepted defensively.
func extractIncidents(raw []byte) ([]utils.Dict, int, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return nil, 0, nil
	}

	switch trimmed[0] {
	case '[':
		var arr []json.RawMessage
		if err := json.Unmarshal([]byte(trimmed), &arr); err != nil {
			return nil, 0, fmt.Errorf("invalid JSON array: %v", err)
		}
		items := rawMessagesToDicts(arr)
		return items, len(items), nil

	case '{':
		var obj struct {
			Data  []json.RawMessage `json:"data"`
			Total int               `json:"total"`
		}
		if err := json.Unmarshal([]byte(trimmed), &obj); err != nil {
			return nil, 0, fmt.Errorf("invalid JSON object: %v", err)
		}
		return rawMessagesToDicts(obj.Data), obj.Total, nil

	default:
		return nil, 0, errors.New("response is neither a JSON array nor a JSON object")
	}
}

// rawMessagesToDicts decodes each incident with utils.UnmarshalCleanJSON, which
// preserves integer precision (no float coercion) so payloads round-trip
// faithfully. A record that is not a non-empty JSON object is skipped rather than
// failing the whole page -- one anomalous element should not block the rest.
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
