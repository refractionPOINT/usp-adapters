// Package usp_withsecure implements a USP adapter for the WithSecure™ Elements
// API (https://api.connect.withsecure.com), the cloud behind WithSecure
// Elements Endpoint Protection and Endpoint Detection & Response (formerly
// F-Secure).
//
// It collects four streams, each shipped verbatim under its own event type:
//
//   - security_event — the EPP protection-engine event stream (malware
//     scanning, DeepGuard, firewall, browsing/connection control, device
//     control, DataGuard, tamper protection, AMSI, collaboration protection, …)
//   - incident       — Broad Context Detections, WithSecure's correlated EDR
//     incidents, re-shipped each time an incident evolves
//   - detection      — the individual detections that make up a BCD, with the
//     process evidence that triggered them
//   - audit_log      — the Elements administrative audit trail
//
// Every stream is polled incrementally on a timestamp cursor with
// exclusiveStart, so an evolving data set is picked up without re-reading it;
// a deduper absorbs the overlap that remains.
package usp_withsecure

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	// defaultBaseURL is the production Elements API host.
	defaultBaseURL = "https://api.connect.withsecure.com"

	// defaultUserAgent identifies this integration. The Elements API rejects
	// any request without a User-Agent, so this is never empty.
	defaultUserAgent = "LimaCharlie-usp-adapter-withsecure/1.0"

	// API paths.
	pathSecurityEvents = "security-events/v1/security-events"
	pathIncidents      = "incidents/v1/incidents"
	pathDetections     = "incidents/v1/detections"
	pathAuditLogs      = "audit-logs/v1/audit-logs"

	// Feed names, which become the EventType of each shipped record.
	feedSecurityEvents = "security_event"
	feedIncidents      = "incident"
	feedDetections     = "detection"
	feedAuditLogs      = "audit_log"

	defaultPollInterval = 1 * time.Minute
	defaultLookback     = 1 * time.Hour
	defaultDedupeTTL    = 24 * time.Hour
	dedupeBucketWindow  = 1 * time.Hour
	defaultMaxPages     = 100

	defaultMaxRetryAttempts = 3
	defaultRetryBaseDelay   = 5 * time.Second
	defaultMaxRetryDelay    = 30 * time.Second

	shipTimeout = 10 * time.Second

	// Per-endpoint page-size ceilings, from the API specification. Requesting
	// more is rejected, so the config is clamped rather than passed through.
	maxSecurityEventPageSize = 200
	maxAuditLogPageSize      = 200
	maxIncidentPageSize      = 50
	maxDetectionPageSize     = 100

	// maxAuditLogWindow is the API's hard limit on an audit-log query range.
	// A first poll with a longer lookback would be rejected outright.
	maxAuditLogWindow = 30 * 24 * time.Hour

	// maxDetectionsPerIncident bounds how many detections are pulled for one
	// incident in a single poll, so a pathological incident cannot stall the
	// feed.
	maxDetectionsPerIncident = 1000

	// cursorLayout is how the adapter renders its *initial* cursor. Every
	// later cursor is the timestamp string the API itself returned, so the
	// format always round-trips.
	cursorLayout = "2006-01-02T15:04:05.000Z"
)

// timestampLayouts are the formats accepted for a record's timestamp field.
// The Elements API emits RFC 3339 with millisecond precision and a Z suffix,
// but the tolerant list guards against per-endpoint variation.
var timestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
}

// WithSecureConfig is the adapter configuration.
type WithSecureConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// ClientID / ClientSecret are an Elements API client credential, created in
	// the Elements Security Center under Management > Organization Settings >
	// API clients. Read-only access is sufficient for this adapter.
	ClientID     string `json:"client_id" yaml:"client_id"`
	ClientSecret string `json:"client_secret" yaml:"client_secret"`

	// OrganizationID scopes every query to one Elements organization. Optional:
	// when empty the API uses the credential's own organization. A partner
	// (MSSP) credential should set it to collect a specific sub-organization —
	// run the extension's list_organizations, or the /organizations endpoint,
	// to find the UUID.
	OrganizationID string `json:"organization_id" yaml:"organization_id"`

	// BaseURL overrides the API root. Defaults to
	// https://api.connect.withsecure.com.
	BaseURL string `json:"base_url" yaml:"base_url"`

	// CollectSecurityEvents / CollectIncidents / CollectDetections /
	// CollectAuditLogs select which streams are collected. A nil (absent) value
	// means enabled for the first three, so the zero-config default collects
	// security events, incidents and their detections. Audit logs default to
	// OFF: they are administrative activity rather than security telemetry and
	// are noisy for most deployments.
	CollectSecurityEvents *bool `json:"collect_security_events" yaml:"collect_security_events"`
	CollectIncidents      *bool `json:"collect_incidents" yaml:"collect_incidents"`
	CollectDetections     *bool `json:"collect_detections" yaml:"collect_detections"`
	CollectAuditLogs      *bool `json:"collect_audit_logs" yaml:"collect_audit_logs"`

	// Engines / EngineGroups / Severities filter the security event stream.
	// Empty means no filter (every engine, every severity). engine_group
	// accepts epp, edr, ecp (collaboration protection) and xm (exposure
	// management).
	Engines      []string `json:"engines" yaml:"engines"`
	EngineGroups []string `json:"engine_groups" yaml:"engine_groups"`
	Severities   []string `json:"severities" yaml:"severities"`

	// IncludeArchivedIncidents collects archived Broad Context Detections too.
	// Off by default: the API documents that filtering them out is also faster.
	IncludeArchivedIncidents bool `json:"include_archived_incidents" yaml:"include_archived_incidents"`

	// Lookback is how far back the first poll of each stream reaches. Default
	// 1 hour. The audit-log stream is capped at the API's 30-day query limit.
	Lookback time.Duration `json:"lookback" yaml:"lookback"`

	// PollInterval is the wait between polls of a stream. Default 1 minute.
	// Note the API rate-limits these endpoints to 300 requests/minute per
	// source IP.
	PollInterval time.Duration `json:"poll_interval" yaml:"poll_interval"`

	// PageSize is the number of records requested per page. Clamped to each
	// endpoint's own ceiling (security events 200, audit logs 200, incidents
	// 50, detections 100).
	PageSize int `json:"page_size" yaml:"page_size"`

	// MaxPages caps how many pages are walked per poll, bounding the work of a
	// first poll against a long lookback. Default 100.
	MaxPages int `json:"max_pages" yaml:"max_pages"`

	// DedupeTTL is how long a record's identifier is remembered to suppress
	// re-shipping it. Default 24 hours.
	DedupeTTL time.Duration `json:"dedupe_ttl" yaml:"dedupe_ttl"`

	// Retry tuning for transient API failures.
	RetryBaseDelay   time.Duration `json:"retry_base_delay" yaml:"retry_base_delay"`
	MaxRetryDelay    time.Duration `json:"max_retry_delay" yaml:"max_retry_delay"`
	MaxRetryAttempts int           `json:"max_retry_attempts" yaml:"max_retry_attempts"`

	// UserAgent overrides the mandatory User-Agent header.
	UserAgent string `json:"user_agent" yaml:"user_agent"`

	// Deduper, when set, replaces the built-in in-memory deduper. It is not
	// settable through a config file; it exists as a seam for tests and for
	// embedders that want to supply a shared deduper.
	Deduper utils.Deduper `json:"-" yaml:"-"`
}

// enabled reports whether a stream should run. def is the value used when the
// toggle is absent from the config.
func enabled(v *bool, def bool) bool {
	if v == nil {
		return def
	}
	return *v
}

func (c *WithSecureConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	c.ClientID = strings.TrimSpace(c.ClientID)
	c.ClientSecret = strings.TrimSpace(c.ClientSecret)
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}

	c.BaseURL = strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if c.BaseURL == "" {
		c.BaseURL = defaultBaseURL
	}
	c.OrganizationID = strings.TrimSpace(c.OrganizationID)
	if c.UserAgent == "" {
		c.UserAgent = defaultUserAgent
	}

	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.Lookback <= 0 {
		c.Lookback = defaultLookback
	}
	if c.DedupeTTL <= 0 {
		c.DedupeTTL = defaultDedupeTTL
	}
	if c.PageSize <= 0 {
		c.PageSize = maxSecurityEventPageSize
	}
	if c.MaxPages <= 0 {
		c.MaxPages = defaultMaxPages
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

	// Detections hang off the incident stream: they are fetched per incident,
	// because the API has no organization-wide detections endpoint.
	if enabled(c.CollectDetections, true) && !enabled(c.CollectIncidents, true) {
		return errors.New("collect_detections requires collect_incidents: detections are fetched per incident")
	}
	if !c.anyFeedEnabled() {
		return errors.New("every stream is disabled; enable at least one of collect_security_events/collect_incidents/collect_audit_logs")
	}
	return nil
}

func (c *WithSecureConfig) anyFeedEnabled() bool {
	return enabled(c.CollectSecurityEvents, true) ||
		enabled(c.CollectIncidents, true) ||
		enabled(c.CollectAuditLogs, false)
}

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

// WithSecureAdapter polls the WithSecure Elements API and ships its records to
// LimaCharlie.
type WithSecureAdapter struct {
	conf        WithSecureConfig
	uspClient   uspSink
	client      *WithSecureClient
	deduper     utils.Deduper
	ownsDeduper bool

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	closeOnce sync.Once
	closeErr  error

	ctx context.Context
}

// NewWithSecureAdapter creates a WithSecure adapter wired to LimaCharlie.
func NewWithSecureAdapter(ctx context.Context, conf WithSecureConfig) (*WithSecureAdapter, chan struct{}, error) {
	return newWithSecureAdapter(ctx, conf, nil)
}

// newWithSecureAdapter is the implementation behind NewWithSecureAdapter. When
// sink is non-nil it is used in place of a real LimaCharlie client -- the seam
// tests use to capture shipped events.
func newWithSecureAdapter(ctx context.Context, conf WithSecureConfig, sink uspSink) (*WithSecureAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &WithSecureAdapter{
		conf:   conf,
		ctx:    ctx,
		doStop: utils.NewEvent(),
	}

	// Consecutive polls deliberately overlap (see pollWindow), and an incident's
	// detections are re-listed whenever the incident changes, so a deduper is
	// required to ship each record exactly once.
	a.deduper = conf.Deduper
	if a.deduper == nil {
		window := dedupeBucketWindow
		if window > conf.DedupeTTL {
			window = conf.DedupeTTL
		}
		deduper, err := utils.NewLocalDeduper(window, conf.DedupeTTL)
		if err != nil {
			return nil, nil, fmt.Errorf("withsecure: deduper: %v", err)
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

	a.client = NewWithSecureClient(conf.BaseURL, conf.ClientID, conf.ClientSecret, conf.UserAgent)
	a.chStopped = make(chan struct{})

	now := time.Now().UTC()
	if enabled(conf.CollectSecurityEvents, true) {
		a.startFeed(feedSecurityEvents, now, conf.Lookback)
	}
	if enabled(conf.CollectIncidents, true) {
		a.startFeed(feedIncidents, now, conf.Lookback)
	}
	if enabled(conf.CollectAuditLogs, false) {
		// The API rejects an audit-log query spanning more than 30 days.
		lookback := conf.Lookback
		if lookback > maxAuditLogWindow {
			lookback = maxAuditLogWindow
		}
		a.startFeed(feedAuditLogs, now, lookback)
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

// startFeed launches one stream's polling goroutine with its initial cursor.
func (a *WithSecureAdapter) startFeed(name string, now time.Time, lookback time.Duration) {
	f := &feedState{name: name, cursor: now.Add(-lookback).Format(cursorLayout)}
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("withsecure: starting feed %q from %s", name, f.cursor))
	a.wgSenders.Add(1)
	go a.runFeed(f)
}

// Close stops the adapter. It is idempotent: repeated calls are no-ops and
// return the result of the first call.
func (a *WithSecureAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.conf.ClientOptions.DebugLog("withsecure: closing")
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

// feedState is one stream's polling cursor.
//
// The cursor is a timestamp string, and after the first poll it is always a
// value the API itself emitted rather than one this adapter formatted — so the
// format cannot drift from what the endpoint accepts.
type feedState struct {
	name   string
	cursor string
}

// runFeed polls a single stream until the adapter is asked to stop.
func (a *WithSecureAdapter) runFeed(f *feedState) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("withsecure: feed %q stopped", f.name))

	isFirstRun := true
	for isFirstRun || !a.doStop.WaitFor(a.conf.PollInterval) {
		isFirstRun = false
		switch f.name {
		case feedSecurityEvents:
			a.pollSecurityEvents(f)
		case feedIncidents:
			a.pollIncidents(f)
		case feedAuditLogs:
			a.pollAuditLogs(f)
		}
	}
}

// Security events
// ============================================================================

// pollSecurityEvents walks the EPP security event stream forward from the
// cursor.
//
// The query is a POST with a form-encoded body (not JSON, and not a GET), and
// the API rejects it outright if no time bound is given. exclusiveStart is what
// keeps the boundary event from being re-read on every poll: without it the API
// returns records with a persistenceTimestamp *greater than or equal to* the
// bound.
func (a *WithSecureAdapter) pollSecurityEvents(f *feedState) {
	anchor := ""
	newest := f.cursor
	nSeen, nShipped := 0, 0

	for page := 1; !a.doStop.IsSet(); page++ {
		if page > a.conf.MaxPages {
			a.warnMaxPages(f.name)
			break
		}

		form := url.Values{}
		a.setOrganization(form)
		form.Set("persistenceTimestampStart", f.cursor)
		form.Set("exclusiveStart", "true")
		// Ascending order makes the last record of the last page the newest,
		// so the cursor advances naturally as pages are consumed.
		form.Set("order", "asc")
		form.Set("limit", strconv.Itoa(clamp(a.conf.PageSize, maxSecurityEventPageSize)))
		addAll(form, "engine", a.conf.Engines)
		addAll(form, "engineGroup", a.conf.EngineGroups)
		addAll(form, "severity", a.conf.Severities)
		if anchor != "" {
			form.Set("anchor", anchor)
		}

		items, next, ok := a.fetchPage(f.name, func(ctx context.Context) ([]byte, error) {
			return a.client.PostForm(ctx, pathSecurityEvents, form)
		})
		if !ok {
			return
		}
		if len(items) == 0 {
			break
		}

		for _, item := range items {
			nSeen++
			if ts := item.FindOneString("persistenceTimestamp"); ts != "" && ts > newest {
				newest = ts
			}
			key := feedSecurityEvents + "|" + recordID(item, "id")
			if a.deduper.CheckAndAdd(key) {
				continue
			}
			if !a.ship(feedSecurityEvents, item, a.eventTime(item, "persistenceTimestamp", "serverTimestamp", "clientTimestamp")) {
				return
			}
			nShipped++
		}

		if next == "" {
			break
		}
		anchor = next
	}

	f.cursor = newest
	a.debugPoll(f, nSeen, nShipped)
}

// Incidents (Broad Context Detections) and their detections
// ============================================================================

// pollIncidents walks Broad Context Detections forward from the cursor on
// updatedTimestamp, and — when detections are enabled — pulls the detections of
// each incident it sees.
//
// Filtering on updatedTimestamp rather than createdTimestamp is deliberate and
// is the flow WithSecure's own cookbook prescribes: a BCD is a living object
// that accretes detections over its lifetime, so an incident first seen an hour
// ago can gain new evidence now. Re-visiting it on update is how that evidence
// is collected; the incident's own dedupe key includes its updatedTimestamp so
// each *version* ships once, while detections dedupe on their stable id.
func (a *WithSecureAdapter) pollIncidents(f *feedState) {
	anchor := ""
	newest := f.cursor
	nSeen, nShipped := 0, 0
	collectDetections := enabled(a.conf.CollectDetections, true)

	for page := 1; !a.doStop.IsSet(); page++ {
		if page > a.conf.MaxPages {
			a.warnMaxPages(f.name)
			break
		}

		q := url.Values{}
		a.setOrganization(q)
		q.Set("updatedTimestampStart", f.cursor)
		q.Set("exclusiveStart", "true")
		q.Set("order", "asc")
		q.Set("limit", strconv.Itoa(clamp(a.conf.PageSize, maxIncidentPageSize)))
		if !a.conf.IncludeArchivedIncidents {
			q.Set("archived", "false")
		}
		if anchor != "" {
			q.Set("anchor", anchor)
		}

		items, next, ok := a.fetchPage(f.name, func(ctx context.Context) ([]byte, error) {
			return a.client.Get(ctx, pathIncidents, q)
		})
		if !ok {
			return
		}
		if len(items) == 0 {
			break
		}

		for _, item := range items {
			nSeen++
			updated := item.FindOneString("updatedTimestamp")
			if updated != "" && updated > newest {
				newest = updated
			}

			// Key on the incident *version*: a BCD whose status, risk or
			// detection set changed is new information worth shipping again,
			// but re-reading an unchanged one must not duplicate it.
			key := feedIncidents + "|" + recordID(item, "incidentId") + "|" + updated
			if !a.deduper.CheckAndAdd(key) {
				if !a.ship(feedIncidents, item, a.eventTime(item, "updatedTimestamp", "createdTimestamp")) {
					return
				}
				nShipped++
			}

			if collectDetections {
				shipped, ok := a.pollDetections(item.FindOneString("incidentId"))
				if !ok {
					return
				}
				nShipped += shipped
			}
		}

		if next == "" {
			break
		}
		anchor = next
	}

	f.cursor = newest
	a.debugPoll(f, nSeen, nShipped)
}

// pollDetections ships the detections of one incident. The bool result is false
// when the whole poll should be abandoned (a shipping failure); a source-side
// failure to read one incident's detections only skips that incident.
func (a *WithSecureAdapter) pollDetections(incidentID string) (int, bool) {
	if incidentID == "" {
		return 0, true
	}

	anchor := ""
	nShipped := 0
	nSeen := 0

	for page := 1; !a.doStop.IsSet(); page++ {
		if page > a.conf.MaxPages || nSeen >= maxDetectionsPerIncident {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf(
				"withsecure: incident %s has more detections than one poll collects "+
					"(seen=%d); raise max_pages if this repeats", incidentID, nSeen))
			break
		}

		q := url.Values{}
		a.setOrganization(q)
		q.Set("incidentId", incidentID)
		q.Set("limit", strconv.Itoa(clamp(a.conf.PageSize, maxDetectionPageSize)))
		if anchor != "" {
			q.Set("anchor", anchor)
		}

		items, next, ok := a.fetchPage(feedDetections, func(ctx context.Context) ([]byte, error) {
			return a.client.Get(ctx, pathDetections, q)
		})
		if !ok {
			// Already reported. Skip this incident's detections and carry on
			// with the rest of the incident page.
			return nShipped, true
		}
		if len(items) == 0 {
			break
		}

		for _, item := range items {
			nSeen++
			key := feedDetections + "|" + recordID(item, "detectionId")
			if a.deduper.CheckAndAdd(key) {
				continue
			}
			if !a.ship(feedDetections, item, a.eventTime(item, "createdTimestamp", "initialReceivedTimestamp")) {
				return nShipped, false
			}
			nShipped++
		}

		if next == "" {
			break
		}
		anchor = next
	}
	return nShipped, true
}

// Audit logs
// ============================================================================

// pollAuditLogs walks the Elements administrative audit trail forward from the
// cursor. The API caps a query at a 30-day range, which the initial lookback
// already respects.
func (a *WithSecureAdapter) pollAuditLogs(f *feedState) {
	anchor := ""
	newest := f.cursor
	nSeen, nShipped := 0, 0

	for page := 1; !a.doStop.IsSet(); page++ {
		if page > a.conf.MaxPages {
			a.warnMaxPages(f.name)
			break
		}

		q := url.Values{}
		a.setOrganization(q)
		q.Set("serverTimestampStart", f.cursor)
		q.Set("exclusiveStart", "true")
		q.Set("order", "asc")
		q.Set("limit", strconv.Itoa(clamp(a.conf.PageSize, maxAuditLogPageSize)))
		if anchor != "" {
			q.Set("anchor", anchor)
		}

		items, next, ok := a.fetchPage(f.name, func(ctx context.Context) ([]byte, error) {
			return a.client.Get(ctx, pathAuditLogs, q)
		})
		if !ok {
			return
		}
		if len(items) == 0 {
			break
		}

		for _, item := range items {
			nSeen++
			if ts := item.FindOneString("serverTimestamp"); ts != "" && ts > newest {
				newest = ts
			}
			key := feedAuditLogs + "|" + recordID(item, "id")
			if a.deduper.CheckAndAdd(key) {
				continue
			}
			if !a.ship(feedAuditLogs, item, a.eventTime(item, "serverTimestamp")) {
				return
			}
			nShipped++
		}

		if next == "" {
			break
		}
		anchor = next
	}

	f.cursor = newest
	a.debugPoll(f, nSeen, nShipped)
}

// Transport
// ============================================================================

// fetchPage issues one request, retrying transient failures with exponential
// backoff, and parses the {items, nextAnchor} envelope. The bool result is
// false when this poll should be abandoned.
//
// Source-side errors (4xx/5xx from the Elements API, network blips, malformed
// responses) are reported via OnWarning and do NOT stop the adapter: the
// cloud-sensor host treats an adapter OnError as fatal to the instance, tearing
// it down and relaunching it, and disabling it after enough failures. A restart
// cannot fix a problem on WithSecure's side and would take the healthy streams
// down with the broken one. So we log, skip this poll, and try again next
// interval.
//
// The two exceptions are an authentication failure — no amount of retrying
// fixes a bad credential, and silently collecting nothing is worse than
// stopping visibly — and a failure delivering to LimaCharlie (see ship), where
// a relaunch does reconnect the backend.
func (a *WithSecureAdapter) fetchPage(feedName string, do func(context.Context) ([]byte, error)) ([]utils.Dict, string, bool) {
	var raw []byte
	var err error

	for attempt := 0; attempt < a.conf.MaxRetryAttempts; attempt++ {
		if a.doStop.IsSet() {
			return nil, "", false
		}

		raw, err = do(a.ctx)
		if err == nil {
			break
		}

		if isAuthError(err) {
			a.conf.ClientOptions.OnError(fmt.Errorf(
				"withsecure: authentication failed (check client_id/client_secret and that the "+
					"API client still exists): %v", err))
			a.doStop.Set()
			return nil, "", false
		}

		if !isTransientError(err) {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf(
				"withsecure: feed %q request failed (skipping this poll): %v", feedName, err))
			return nil, "", false
		}

		if attempt+1 >= a.conf.MaxRetryAttempts {
			break
		}
		delay := a.conf.RetryBaseDelay * time.Duration(1<<attempt)
		if delay > a.conf.MaxRetryDelay {
			delay = a.conf.MaxRetryDelay
		}
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"withsecure: feed %q transient error (attempt %d/%d), retrying in %v: %v",
			feedName, attempt+1, a.conf.MaxRetryAttempts, delay, err))
		if a.doStop.WaitFor(delay) {
			return nil, "", false
		}
	}
	if err != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"withsecure: feed %q failed after %d attempts (skipping this poll): %v",
			feedName, a.conf.MaxRetryAttempts, err))
		return nil, "", false
	}

	items, next, err := extractPage(raw)
	if err != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"withsecure: feed %q response parse error (skipping this poll): %v", feedName, err))
		return nil, "", false
	}
	return items, next, true
}

// ship forwards a single record to LimaCharlie. It returns false if the adapter
// should stop (an unrecoverable shipping error).
func (a *WithSecureAdapter) ship(eventType string, item utils.Dict, timestampMs uint64) bool {
	msg := &protocol.DataMessage{
		JsonPayload: item,
		EventType:   eventType,
		TimestampMs: timestampMs,
	}
	if err := a.uspClient.Ship(msg, shipTimeout); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("withsecure: stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("withsecure: Ship(): %v", err))
			a.doStop.Set()
			return false
		}
	}
	return true
}

// Helpers
// ============================================================================

// setOrganization applies the configured organization scope. When empty the API
// falls back to the credential's own organization.
func (a *WithSecureAdapter) setOrganization(v url.Values) {
	if a.conf.OrganizationID != "" {
		v.Set("organizationId", a.conf.OrganizationID)
	}
}

func (a *WithSecureAdapter) warnMaxPages(feedName string) {
	a.conf.ClientOptions.OnWarning(fmt.Sprintf(
		"withsecure: feed %q hit max_pages=%d; the remaining records are collected on the "+
			"next poll (the cursor only advances over what was read)", feedName, a.conf.MaxPages))
}

func (a *WithSecureAdapter) debugPoll(f *feedState, nSeen, nShipped int) {
	a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"withsecure: feed %q poll complete (seen=%d shipped=%d cursor=%s)",
		f.name, nSeen, nShipped, f.cursor))
}

// eventTime extracts the record's event time from the first of the candidate
// fields that is present and parseable, falling back to now.
func (a *WithSecureAdapter) eventTime(item utils.Dict, fields ...string) uint64 {
	for _, field := range fields {
		raw := item.FindOneString(field)
		if raw == "" {
			continue
		}
		if t, ok := parseTimestamp(raw); ok {
			return uint64(t.UnixMilli())
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf(
			"withsecure: unparseable timestamp %q at field %q", raw, field))
	}
	return uint64(time.Now().UnixMilli())
}

// recordID resolves a record's stable identifier, falling back to a content
// hash so deduplication still works for a record missing its id field.
func recordID(item utils.Dict, idField string) string {
	if id := fieldAsString(item, idField); id != "" {
		return id
	}
	if b, err := json.Marshal(item); err == nil {
		sum := sha256.Sum256(b)
		return "sha256:" + hex.EncodeToString(sum[:])
	}
	return ""
}

// fieldAsString reads a field as a string, accepting numeric values too.
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

// extractPage parses the Elements collection envelope: {"items": [...],
// "nextAnchor": "..."}. nextAnchor is absent on the last page.
func extractPage(raw []byte) ([]utils.Dict, string, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return nil, "", nil
	}
	var env struct {
		Items      []json.RawMessage `json:"items"`
		NextAnchor string            `json:"nextAnchor"`
	}
	if err := json.Unmarshal([]byte(trimmed), &env); err != nil {
		return nil, "", fmt.Errorf("invalid JSON response: %v", err)
	}
	return rawMessagesToDicts(env.Items), env.NextAnchor, nil
}

// rawMessagesToDicts decodes each record with utils.UnmarshalCleanJSON, which
// preserves integer precision (no float coercion) so payloads round-trip
// faithfully. A record that is not a non-empty JSON object is skipped rather
// than failing the whole page -- one anomalous element should not block every
// other record.
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

// addAll appends each value as a repeated parameter, which is how the Elements
// API models array-valued filters.
func addAll(v url.Values, key string, vals []string) {
	for _, val := range vals {
		if val = strings.TrimSpace(val); val != "" {
			v.Add(key, val)
		}
	}
}

// clamp bounds a page size by the endpoint's own ceiling.
func clamp(size, max int) int {
	if size <= 0 || size > max {
		return max
	}
	return size
}
