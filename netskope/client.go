// Package usp_netskope implements a USP adapter for the Netskope REST API v2
// "dataexport" iterator endpoints
// (https://docs.netskope.com/en/using-the-rest-api-v2-dataexport-iterator-endpoints/).
//
// Netskope exposes its security telemetry as a set of iterator streams, one per
// event type (page, application, network, audit, incident, infrastructure,
// endpoint, alert) and one per alert type (dlp, malware, malsite, ctep,
// compromisedcredential, uba, policy, quarantine, remediation,
// securityassessment, watchlist). Each stream is consumed by polling
//
//	GET /api/v2/events/dataexport/{events|alerts}/{type}?index=<cursor>&operation=next
//
// where "index" is a consumer-chosen, server-side cursor: Netskope remembers the
// position for that index name, so the adapter does not have to persist any
// state itself -- a restart simply resumes the same index. The response carries
// up to 10,000 records plus a "wait_time" telling the consumer how long to wait
// before the next call.
//
// The adapter models each stream as a "feed" and ships every Netskope record to
// LimaCharlie verbatim (no reshaping); LimaCharlie parses and maps in the cloud.
package usp_netskope

import (
	"bytes"
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
	// defaultPollInterval is the idle cadence used when the server returns no
	// wait_time and there is no backlog to drain.
	defaultPollInterval = 30 * time.Second
	// defaultMinWaitTime is the floor used between calls while a backlog is being
	// drained. It keeps the adapter comfortably under Netskope's documented limit
	// of 4 requests/second/endpoint.
	defaultMinWaitTime = 1 * time.Second
	// defaultMaxWaitTime caps how long the adapter will honour a server-provided
	// wait_time, so a quiet stream is still polled at least this often.
	defaultMaxWaitTime = 5 * time.Minute

	defaultDedupeTTL   = 1 * time.Hour
	dedupeBucketWindow = 5 * time.Minute

	defaultMaxRetryAttempts = 4
	defaultRetryBaseDelay   = 2 * time.Second
	defaultMaxRetryDelay    = 60 * time.Second

	defaultTimestampField = "timestamp"

	// operationNext advances a stream's server-side cursor to the next page.
	operationNext = "next"

	shipTimeout = 10 * time.Second
)

// defaultAlertTypes are the alert streams collected out of the box. They are the
// high-signal, lower-volume detections a SOC cares about most. The full list of
// v2 alert types is collected by default.
var defaultAlertTypes = []string{
	"dlp", "malware", "malsite", "ctep", "compromisedcredential",
	"uba", "policy", "quarantine", "remediation", "securityassessment", "watchlist",
}

// defaultEventTypes are the event streams collected out of the box. Only the
// administrative audit trail is on by default; the page / application / network
// event streams are very high volume and are left opt-in (add them to
// event_types or to a custom feed).
var defaultEventTypes = []string{"audit"}

// validKinds is the set of dataexport categories.
var validKinds = map[string]struct{}{"events": {}, "alerts": {}}

// commonIDFields are the fields probed, in order, for a stable per-record
// identifier used for deduplication when a feed does not set IDField. Netskope
// records do not always carry an explicit id, so a content hash is the final
// fallback (see recordID).
var commonIDFields = []string{"_id", "id"}

// timestampLayouts are the string time formats accepted for a record's timestamp
// field. Netskope normally emits an integer epoch-seconds "timestamp", but some
// fields are ISO-8601 strings.
var timestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
}

// NetskopeFeed describes a single dataexport stream to poll. New streams are
// added purely by configuration -- no code change.
type NetskopeFeed struct {
	// Name labels the feed and becomes the EventType of every shipped record.
	// Defaults to "<kind>_<type>" (e.g. "alert_dlp", "event_audit").
	Name string `json:"name" yaml:"name"`

	// Kind is the dataexport category: "events" or "alerts".
	Kind string `json:"kind" yaml:"kind"`

	// Type is the Netskope stream type within the kind, e.g. "page" / "audit"
	// for events, "dlp" / "malware" for alerts.
	Type string `json:"type" yaml:"type"`

	// Index is the iterator cursor name for this stream. Netskope stores the read
	// position server-side under this name, so it must be unique per consumer and
	// stable across restarts. Defaults to "<index_prefix>_<kind>_<type>".
	Index string `json:"index" yaml:"index"`

	// TimestampField is the record field holding the event time (epoch seconds,
	// or an ISO-8601 string). Defaults to "timestamp".
	TimestampField string `json:"timestamp_field" yaml:"timestamp_field"`

	// IDField is the record field holding a stable identifier, used for
	// deduplication. Empty means: probe common id fields, then fall back to a
	// content hash.
	IDField string `json:"id_field" yaml:"id_field"`

	// seedOp, when non-empty, is the operation used on this feed's first poll of
	// the process (an epoch-seconds value derived from the config's start_time).
	// Subsequent polls always use "next". Not settable from config.
	seedOp string `json:"-" yaml:"-"`
}

// path is the dataexport endpoint path for the feed, relative to the API root.
func (f NetskopeFeed) path() string {
	return "events/dataexport/" + f.Kind + "/" + f.Type
}

// NetskopeConfig is the adapter configuration.
type NetskopeConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// Token is a Netskope REST API v2 token (Settings > Tools > REST API v2). It
	// is sent in the Netskope-Api-Token header and must be scoped to every
	// dataexport endpoint the adapter polls.
	Token string `json:"token" yaml:"token"`

	// Tenant is the Netskope tenant host, e.g. "acme.goskope.com" (a scheme and
	// trailing path are tolerated). The API root becomes https://<tenant>/api/v2.
	Tenant string `json:"tenant" yaml:"tenant"`

	// BaseURL fully overrides the API root (e.g. for a non-goskope.com region or
	// a test server). When set, Tenant is ignored. Should include /api/v2.
	BaseURL string `json:"base_url" yaml:"base_url"`

	// IndexPrefix namespaces the per-feed iterator cursor names. It must be
	// unique to this adapter on the tenant (two consumers sharing an index name
	// corrupt each other's position). Defaults to the sensor_seed_key, or
	// "limacharlie" if that is empty.
	IndexPrefix string `json:"index_prefix" yaml:"index_prefix"`

	// StartTime optionally seeds a brand-new stream's starting position on the
	// first poll of the process. Accepts epoch seconds ("1700000000"), an RFC3339
	// timestamp, or a relative Go duration ago ("24h", "168h"). When empty the
	// adapter uses operation=next, which resumes an existing cursor or, for a new
	// one, starts from Netskope's earliest retained position.
	//
	// Note: start_time re-seeds on every process restart, so leaving it set means
	// a restart re-reads from that point (the deduper absorbs short overlaps).
	// Use it for an initial backfill, then remove it.
	StartTime string `json:"start_time" yaml:"start_time"`

	// AlertTypes / EventTypes select which default feeds run. A nil (absent)
	// value means "use the built-in default set"; an explicit empty list means
	// "none of this kind". Ignored when Feeds is supplied.
	AlertTypes []string `json:"alert_types" yaml:"alert_types"`
	EventTypes []string `json:"event_types" yaml:"event_types"`

	// Feeds is the explicit set of streams to poll. When non-empty it replaces
	// the default set entirely (AlertTypes / EventTypes no longer apply).
	Feeds []NetskopeFeed `json:"feeds" yaml:"feeds"`

	// PollInterval is the idle cadence used when the server returns no wait_time
	// and there is no backlog. Default 30s.
	PollInterval time.Duration `json:"poll_interval" yaml:"poll_interval"`
	// MaxWaitTime caps how long a server-provided wait_time is honoured. Default
	// 5 minutes.
	MaxWaitTime time.Duration `json:"max_wait_time" yaml:"max_wait_time"`

	// DedupeTTL is how long a record id is remembered to suppress re-shipping it
	// (covers the iterator resend/re-seed overlap). Default 1 hour.
	DedupeTTL time.Duration `json:"dedupe_ttl" yaml:"dedupe_ttl"`

	// Retry tuning for transient API failures.
	RetryBaseDelay   time.Duration `json:"retry_base_delay" yaml:"retry_base_delay"`
	MaxRetryDelay    time.Duration `json:"max_retry_delay" yaml:"max_retry_delay"`
	MaxRetryAttempts int           `json:"max_retry_attempts" yaml:"max_retry_attempts"`

	// minWaitTime is the floor between calls while draining a backlog. Not
	// settable from config; derived from PollInterval.
	minWaitTime time.Duration

	// startEpoch is the parsed StartTime (epoch seconds), or 0 for operation=next.
	startEpoch int64

	// Deduper, when set, replaces the built-in in-memory deduper. Not settable
	// from a config file; a seam for tests and embedders.
	Deduper utils.Deduper `json:"-" yaml:"-"`
}

// indexPrefix returns the sanitized cursor-name prefix.
func (c *NetskopeConfig) indexPrefix() string {
	p := strings.TrimSpace(c.IndexPrefix)
	if p == "" {
		p = c.ClientOptions.SensorSeedKey
	}
	if strings.TrimSpace(p) == "" {
		p = "limacharlie"
	}
	return sanitizeIndex(p)
}

func (c *NetskopeConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Token == "" {
		return errors.New("missing token")
	}
	c.BaseURL = strings.TrimSpace(c.BaseURL)
	c.Tenant = strings.TrimSpace(c.Tenant)
	if c.BaseURL == "" && c.Tenant == "" {
		return errors.New("missing tenant (or base_url)")
	}

	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	c.minWaitTime = defaultMinWaitTime
	if c.minWaitTime > c.PollInterval {
		c.minWaitTime = c.PollInterval
	}
	if c.MaxWaitTime <= 0 {
		c.MaxWaitTime = defaultMaxWaitTime
	}
	if c.MaxWaitTime < c.PollInterval {
		c.MaxWaitTime = c.PollInterval
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

	epoch, err := parseStartTime(c.StartTime)
	if err != nil {
		return fmt.Errorf("invalid start_time: %v", err)
	}
	c.startEpoch = epoch

	if len(c.Feeds) == 0 {
		c.Feeds = c.defaultFeeds()
		if len(c.Feeds) == 0 {
			return errors.New("no feeds: enable at least one alert_types/event_types entry or supply custom feeds")
		}
	}

	prefix := c.indexPrefix()
	seenNames := make(map[string]struct{}, len(c.Feeds))
	seenIndex := make(map[string]struct{}, len(c.Feeds))
	for i := range c.Feeds {
		f := &c.Feeds[i]
		f.Kind = strings.ToLower(strings.TrimSpace(f.Kind))
		f.Type = strings.ToLower(strings.TrimSpace(f.Type))
		f.Name = strings.TrimSpace(f.Name)
		if _, ok := validKinds[f.Kind]; !ok {
			return fmt.Errorf("feed %d: kind must be \"events\" or \"alerts\", got %q", i, f.Kind)
		}
		if f.Type == "" {
			return fmt.Errorf("feed %d: missing type", i)
		}
		if f.Name == "" {
			f.Name = singular(f.Kind) + "_" + f.Type
		}
		if _, ok := seenNames[f.Name]; ok {
			return fmt.Errorf("feed %q: duplicate name", f.Name)
		}
		seenNames[f.Name] = struct{}{}

		if f.Index == "" {
			f.Index = sanitizeIndex(prefix + "_" + singular(f.Kind) + "_" + f.Type)
		} else {
			f.Index = sanitizeIndex(f.Index)
		}
		if _, ok := seenIndex[f.Index]; ok {
			return fmt.Errorf("feed %q: duplicate iterator index %q (each stream needs a unique index)", f.Name, f.Index)
		}
		seenIndex[f.Index] = struct{}{}

		if f.TimestampField == "" {
			f.TimestampField = defaultTimestampField
		}
		if c.startEpoch > 0 {
			f.seedOp = strconv.FormatInt(c.startEpoch, 10)
		}
	}
	return nil
}

// defaultFeeds builds the out-of-the-box feed set from AlertTypes / EventTypes,
// applying the built-in defaults for whichever is absent (nil). An explicit
// empty list disables that kind.
func (c *NetskopeConfig) defaultFeeds() []NetskopeFeed {
	alertTypes := c.AlertTypes
	if alertTypes == nil {
		alertTypes = defaultAlertTypes
	}
	eventTypes := c.EventTypes
	if eventTypes == nil {
		eventTypes = defaultEventTypes
	}
	feeds := make([]NetskopeFeed, 0, len(alertTypes)+len(eventTypes))
	for _, t := range alertTypes {
		if t = strings.ToLower(strings.TrimSpace(t)); t != "" {
			feeds = append(feeds, NetskopeFeed{Kind: "alerts", Type: t})
		}
	}
	for _, t := range eventTypes {
		if t = strings.ToLower(strings.TrimSpace(t)); t != "" {
			feeds = append(feeds, NetskopeFeed{Kind: "events", Type: t})
		}
	}
	return feeds
}

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

// NetskopeAdapter polls one or more Netskope dataexport streams and ships their
// records to LimaCharlie.
type NetskopeAdapter struct {
	conf        NetskopeConfig
	uspClient   uspSink
	client      *NetskopeClient
	deduper     utils.Deduper
	ownsDeduper bool

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	closeOnce sync.Once
	closeErr  error

	ctx context.Context
}

// NewNetskopeAdapter creates a Netskope adapter wired to LimaCharlie.
func NewNetskopeAdapter(ctx context.Context, conf NetskopeConfig) (*NetskopeAdapter, chan struct{}, error) {
	return newNetskopeAdapter(ctx, conf, nil)
}

// newNetskopeAdapter is the implementation behind NewNetskopeAdapter. When sink
// is non-nil it is used in place of a real LimaCharlie client -- the seam tests
// use to capture shipped events.
func newNetskopeAdapter(ctx context.Context, conf NetskopeConfig, sink uspSink) (*NetskopeAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &NetskopeAdapter{
		conf:   conf,
		ctx:    ctx,
		doStop: utils.NewEvent(),
	}

	// The iterator can re-deliver records (operation=resend, or a re-seed on
	// restart), so a deduper ships each record exactly once across those
	// overlaps. A deduper may be supplied via the config; otherwise an in-memory
	// one is created (and owned/closed by the adapter).
	a.deduper = conf.Deduper
	if a.deduper == nil {
		window := dedupeBucketWindow
		if window > conf.DedupeTTL {
			window = conf.DedupeTTL
		}
		deduper, err := utils.NewLocalDeduper(window, conf.DedupeTTL)
		if err != nil {
			return nil, nil, fmt.Errorf("netskope: deduper: %v", err)
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

	a.client = NewNetskopeClient(resolveBaseURL(conf), conf.Token)
	a.chStopped = make(chan struct{})

	for _, feed := range conf.Feeds {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("netskope: starting feed %q -> %s (index=%s)", feed.Name, feed.path(), feed.Index))
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
func (a *NetskopeAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.conf.ClientOptions.DebugLog("netskope: closing")
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

// resolveBaseURL computes the Netskope API root (.../api/v2) from the config.
func resolveBaseURL(conf NetskopeConfig) string {
	if conf.BaseURL != "" {
		return strings.TrimRight(conf.BaseURL, "/")
	}
	host := conf.Tenant
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	host = strings.Trim(host, "/")
	return "https://" + host + "/api/v2"
}

// runFeed consumes a single dataexport stream forever, until the adapter is
// asked to stop. Netskope holds the cursor server-side (keyed by feed.Index), so
// the loop is a straight walk of operation=next: each call returns the next page
// and a wait_time hint, and the loop honours that wait_time before the next call.
func (a *NetskopeAdapter) runFeed(feed NetskopeFeed) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("netskope: feed %q stopped", feed.Name))

	// The first poll uses the seed operation when start_time is configured;
	// thereafter (and always, once a poll succeeds) it is operation=next.
	op := operationNext
	if feed.seedOp != "" {
		op = feed.seedOp
	}

	for !a.doStop.IsSet() {
		items, waitTime, ok := a.fetchPage(feed, op)
		if ok {
			op = operationNext
			for _, item := range items {
				if a.deduper.CheckAndAdd(a.dedupeKey(feed, item)) {
					continue
				}
				if !a.ship(feed, item) {
					return
				}
			}
		}

		if a.doStop.WaitFor(a.waitDuration(waitTime, len(items), ok)) {
			return
		}
	}
}

// waitDuration decides how long to wait before the next call to a feed. It
// honours the server's wait_time (capped by MaxWaitTime); absent one, it drains
// quickly while there is a backlog and idles at PollInterval otherwise.
func (a *NetskopeAdapter) waitDuration(waitTimeSec, nItems int, ok bool) time.Duration {
	if waitTimeSec > 0 {
		d := time.Duration(waitTimeSec) * time.Second
		if d > a.conf.MaxWaitTime {
			d = a.conf.MaxWaitTime
		}
		return d
	}
	if ok && nItems > 0 {
		// A full page with no wait hint means there is likely more backlog: poll
		// again promptly, but stay under the 4 req/s/endpoint rate limit.
		return a.conf.minWaitTime
	}
	return a.conf.PollInterval
}

// fetchPage performs one iterator call for a feed, retrying transient failures
// with exponential backoff. ok is false when the poll should be abandoned (any
// source-side error, or the adapter is stopping).
//
// Source-side errors (4xx/5xx, auth failures, network blips, malformed
// responses) are reported via OnWarning and NEVER stop the adapter. This is
// deliberate: the cloud-sensor host treats any adapter OnError as fatal to the
// instance -- it tears the adapter down, relaunches it, and after enough
// failures disables it entirely. Restarting cannot fix a problem on Netskope's
// side (a revoked token, a missing endpoint scope), and it would take the
// adapter's healthy feeds down with the broken one. So we log, skip just this
// poll, and let the feed try again. Only a failure delivering to LimaCharlie
// (see ship) is fatal, since a relaunch reconnects the backend.
func (a *NetskopeAdapter) fetchPage(feed NetskopeFeed, op string) ([]utils.Dict, int, bool) {
	q := url.Values{}
	q.Set("index", feed.Index)
	q.Set("operation", op)

	var raw []byte
	var err error
	for attempt := 0; attempt < a.conf.MaxRetryAttempts; attempt++ {
		if a.doStop.IsSet() {
			return nil, 0, false
		}

		raw, err = a.client.Get(a.ctx, feed.path(), q)
		if err == nil {
			break
		}

		if !isTransientError(err) {
			msg := fmt.Sprintf("netskope: feed %q request failed (skipping this poll): %v", feed.Name, err)
			var httpErr *HTTPError
			if errors.As(err, &httpErr) &&
				(httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden) {
				msg = fmt.Sprintf(
					"netskope: feed %q -- HTTP %d, token rejected. Verify the REST API v2 token is "+
						"valid and scoped to the %q dataexport endpoint (Settings > Tools > REST API v2).",
					feed.Name, httpErr.StatusCode, feed.path())
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
		// Prefer the server's own retry hint (Retry-After / RateLimit-Reset) when
		// it rate-limits us.
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && httpErr.RetryAfter > 0 {
			delay = httpErr.RetryAfter
			if delay > a.conf.MaxRetryDelay {
				delay = a.conf.MaxRetryDelay
			}
		}
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"netskope: feed %q transient error (attempt %d/%d), retrying in %v: %v",
			feed.Name, attempt+1, a.conf.MaxRetryAttempts, delay, err))
		if a.doStop.WaitFor(delay) {
			return nil, 0, false
		}
	}
	if err != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"netskope: feed %q failed after %d attempts (skipping this poll): %v",
			feed.Name, a.conf.MaxRetryAttempts, err))
		return nil, 0, false
	}

	items, waitTime, perr := parseIteratorResponse(raw)
	if perr != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"netskope: feed %q response parse error (skipping this poll): %v", feed.Name, perr))
		return nil, waitTime, false
	}
	if n := len(items); n > 0 {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf(
			"netskope: feed %q fetched %d records (wait_time=%ds)", feed.Name, n, waitTime))
	}
	return items, waitTime, true
}

// ship forwards a single record to LimaCharlie. It returns false if the adapter
// should stop (an unrecoverable shipping error).
func (a *NetskopeAdapter) ship(feed NetskopeFeed, item utils.Dict) bool {
	msg := &protocol.DataMessage{
		JsonPayload: item,
		EventType:   feed.Name,
		TimestampMs: a.eventTime(feed, item),
	}
	if err := a.uspClient.Ship(msg, shipTimeout); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("netskope: stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("netskope: Ship(): %v", err))
			a.doStop.Set()
			return false
		}
	}
	return true
}

// eventTime extracts the record's event time. Netskope emits "timestamp" as
// epoch seconds; an ISO-8601 string is also accepted. Falls back to now when the
// field is absent or unparseable.
func (a *NetskopeAdapter) eventTime(feed NetskopeFeed, item utils.Dict) uint64 {
	field := feed.TimestampField
	if field == "" {
		field = defaultTimestampField
	}
	if secs, ok := epochSeconds(item, field); ok {
		return secs * 1000
	}
	if raw := item.FindOneString(field); raw != "" {
		if t, ok := parseTimestamp(raw); ok {
			return uint64(t.UnixMilli())
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf(
			"netskope: feed %q unparseable timestamp %q at field %q", feed.Name, raw, field))
	}
	return uint64(time.Now().UnixMilli())
}

// epochSeconds reads an integer epoch-seconds value from a record field,
// accepting either a top-level int or a "/"-pathed int. Returns ok=false when
// the field is absent (so a real value of 0 is still treated as absent, which is
// correct -- epoch 0 is not a plausible Netskope event time).
func epochSeconds(item utils.Dict, field string) (uint64, bool) {
	if v, ok := item.GetInt(field); ok && v > 0 {
		return v, true
	}
	if ints := item.FindInt(field); len(ints) > 0 && ints[0] > 0 {
		return ints[0], true
	}
	return 0, false
}

// dedupeKey returns a stable deduplication key for a record, namespaced by feed
// so identifiers cannot collide across feeds.
func (a *NetskopeAdapter) dedupeKey(feed NetskopeFeed, item utils.Dict) string {
	return feed.Name + "|" + recordID(feed, item)
}

// recordID resolves a record's identifier: the feed's IDField if set, otherwise
// a probe of common id fields, otherwise a content hash so deduplication still
// works for records with no recognizable identifier.
func recordID(feed NetskopeFeed, item utils.Dict) string {
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

// fieldAsString reads a field as a string, accepting numeric values too (ids are
// sometimes integers).
func fieldAsString(item utils.Dict, path string) string {
	if s := item.FindOneString(path); s != "" {
		return s
	}
	if ints := item.FindInt(path); len(ints) > 0 {
		return strconv.FormatUint(ints[0], 10)
	}
	return ""
}

// parseIteratorResponse parses a dataexport iterator response envelope
// ({"ok":1,"result":[...],"wait_time":N}) into a slice of records plus the
// server's suggested wait_time (seconds). An ok=0 envelope is an error.
func parseIteratorResponse(raw []byte) ([]utils.Dict, int, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, 0, nil
	}
	var env struct {
		OK       *int              `json:"ok"`
		Result   []json.RawMessage `json:"result"`
		WaitTime int               `json:"wait_time"`
		Message  string            `json:"message"`
		Error    string            `json:"error"`
	}
	if err := json.Unmarshal(trimmed, &env); err != nil {
		return nil, 0, fmt.Errorf("invalid iterator response: %v", err)
	}
	if env.OK != nil && *env.OK == 0 {
		msg := env.Error
		if msg == "" {
			msg = env.Message
		}
		if msg == "" {
			msg = "ok=0"
		}
		return nil, env.WaitTime, fmt.Errorf("iterator returned an error: %s", msg)
	}
	return rawMessagesToDicts(env.Result), env.WaitTime, nil
}

// rawMessagesToDicts decodes each record with utils.UnmarshalCleanJSON, which
// preserves integer precision (no float coercion) so payloads round-trip
// faithfully. A record that is not a non-empty JSON object is skipped rather than
// failing the whole page.
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

// parseStartTime interprets the start_time config value. It accepts epoch
// seconds, an RFC3339 timestamp, or a relative Go duration ago ("24h"). An empty
// value yields 0, meaning operation=next.
func parseStartTime(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n <= 0 {
			return 0, fmt.Errorf("epoch must be positive, got %d", n)
		}
		return n, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.Unix(), nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		if d <= 0 {
			return 0, fmt.Errorf("duration must be positive, got %q", s)
		}
		return time.Now().Add(-d).Unix(), nil
	}
	return 0, fmt.Errorf("expected epoch seconds, RFC3339, or a Go duration, got %q", s)
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

// sanitizeIndex keeps a Netskope iterator index name to a safe character set
// (alphanumerics, underscore, hyphen); anything else becomes an underscore.
func sanitizeIndex(s string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// singular maps a feed kind ("events"/"alerts") to the label prefix used for a
// feed's default Name ("event"/"alert").
func singular(kind string) string {
	return strings.TrimSuffix(kind, "s")
}
