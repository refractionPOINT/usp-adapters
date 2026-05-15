package usp_harmony

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultBaseURL = "https://cloudinfra-gw.portal.checkpoint.com"

	authPath = "/auth/external"

	// Infinity Events (Logs-as-a-Service) — unified event stream across Harmony products.
	eventsQueryPath    = "/app/laas-logs-api/api/logs_query"
	eventsRetrievePath = "/app/laas-logs-api/api/logs_query/retrieve"

	// HEC API — Harmony Email & Collaboration entity / event APIs.
	hecSearchEntityPath = "/app/hec-api/v1.0/search/query"

	tokenRefreshSkew = 1 * time.Minute

	// maxRequestAttempts bounds how many times a single gateway call is
	// attempted before the error is surfaced. The Check Point gateway is
	// observed to intermittently fail to return response headers within the
	// client timeout (~a few times/day, across all services and both the
	// status-poll and retrieve endpoints). Absorbing those blips here keeps
	// a transient slowdown from re-running a whole query window — which for
	// the events source would re-ship already-shipped pages, since events
	// are not deduped.
	maxRequestAttempts = 4
)

// requestRetryBackoff is the wait before retry attempt i (1-indexed-ish:
// element 0 is the wait before the 2nd attempt). The last element is reused
// if there are more attempts than entries.
var requestRetryBackoff = []time.Duration{2 * time.Second, 5 * time.Second, 10 * time.Second}

// Defaults for the Infinity Events source.
// Names must match the gateway exactly. The Email & Collaboration service uses
// an ampersand, not the word "and" — using "and" makes the gateway reject the
// query with "The provided Cloud Service is unknown".
var defaultEventsCloudServices = []string{
	"Harmony Endpoint",
	"Harmony Email & Collaboration",
	"Harmony Mobile",
	"Harmony Connect",
	"Harmony Browse",
}

const (
	defaultEventsPollInterval    = 60 * time.Second
	defaultEventsStatusPollEvery = 5 * time.Second
	defaultEventsStatusPollTotal = 10 * time.Minute
	defaultEventsInitialLookback = 1 * time.Hour
	defaultEventsEndLag          = 1 * time.Minute
	defaultEventsPageLimit       = 100
	defaultEventsPerCloudLimit   = 5000
)

// Defaults for the Restore Requests source.
var defaultRestoreSaas = []string{"office365_emails", "google_mail"}

var defaultRestoreSaasEntity = map[string]string{
	"office365_emails": "office365_emails_email",
	"google_mail":      "google_mail_email",
}

const (
	defaultRestorePollInterval = 5 * time.Minute
	defaultRestoreLookback     = 30 * 24 * time.Hour
)

// harmonySource is the contract every ingestion source in this adapter
// implements. Adding a new source is mechanical: implement the four
// methods, add the source-config struct as a field on HarmonyConfig, and
// register a pointer to it in HarmonyConfig.sources(). HarmonyConfig.Validate
// and NewHarmonyAdapter handle the rest.
type harmonySource interface {
	// Name identifies the source in error messages and the
	// "_lc_harmony_source" annotation. Convention: snake_case, matching the
	// source's JSON/YAML key on HarmonyConfig.
	Name() string

	// IsEnabled returns true when the source is configured to run. Validate
	// and Start are only invoked when this returns true.
	IsEnabled() bool

	// Validate is called by HarmonyConfig.Validate when the source is
	// enabled. It should both reject bad configurations and fill in
	// sensible defaults for unset fields.
	Validate() error

	// Start is called by NewHarmonyAdapter when the source is enabled. The
	// source spawns its workers by calling a.wgSenders.Add(1) and launching
	// goroutines that end with `defer a.wgSenders.Done()` and respect
	// a.doStop. Errors here abort adapter construction.
	Start(a *HarmonyAdapter) error

	// Close releases any source-owned resources (deduper, file handles,
	// etc.). It is called from HarmonyAdapter.Close after all workers have
	// exited, and must be safe to call even when Start was never invoked.
	Close()
}

// EventsConfig controls the Infinity Events / Logs-as-a-Service source. When
// Enabled is false this source is skipped entirely.
type EventsConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled"`

	// CloudServices to pull events for. Each is queried independently.
	// Names must match the gateway exactly — the Email service is
	// "Harmony Email & Collaboration" (ampersand, not the word "and"); the
	// gateway rejects the "and" spelling. Empty enables the full Harmony
	// suite ("Harmony Endpoint", "Harmony Email & Collaboration",
	// "Harmony Mobile", "Harmony Connect", "Harmony Browse").
	CloudServices []string `json:"cloud_services" yaml:"cloud_services"`

	// Optional Infinity Events query filter applied to every cloud service.
	Filter string `json:"filter" yaml:"filter"`

	PollInterval time.Duration `json:"poll_interval" yaml:"poll_interval"`
	PageLimit    int           `json:"page_limit" yaml:"page_limit"`
	Limit        int           `json:"limit" yaml:"limit"`
}

func (c *EventsConfig) Name() string    { return "events" }
func (c *EventsConfig) IsEnabled() bool { return c != nil && c.Enabled }

func (c *EventsConfig) Validate() error {
	if len(c.CloudServices) == 0 {
		c.CloudServices = append([]string{}, defaultEventsCloudServices...)
	}
	if c.PollInterval <= 0 {
		c.PollInterval = defaultEventsPollInterval
	}
	if c.PageLimit <= 0 {
		c.PageLimit = defaultEventsPageLimit
	}
	if c.PageLimit < 10 {
		// Gateway rejects values below 10 with HTTP 400.
		c.PageLimit = 10
	}
	if c.Limit <= 0 {
		c.Limit = defaultEventsPerCloudLimit
	}
	if c.Limit < 10 {
		c.Limit = 10
	}
	return nil
}

func (c *EventsConfig) Start(a *HarmonyAdapter) error {
	for _, svc := range c.CloudServices {
		a.wgSenders.Add(1)
		go a.fetchEventsForService(svc)
	}
	return nil
}

func (c *EventsConfig) Close() {}

// RestoreRequestsConfig controls polling of the HEC entity search API to
// surface emails with pending or recently-decided restore requests.
type RestoreRequestsConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled"`

	// Saas platforms to query. Defaults to office365_emails + google_mail
	// (the only two HEC currently supports for the restore-request flags).
	Saas []string `json:"saas" yaml:"saas"`

	PollInterval time.Duration `json:"poll_interval" yaml:"poll_interval"`

	// How far back to search for restore-requested emails. Defaults to 30 days
	// (typical quarantine retention window).
	Lookback time.Duration `json:"lookback" yaml:"lookback"`

	// IncludeResolved, when true, also issues queries filtered on isRestored
	// and isRestoreDeclined. The default (false) only queries
	// isRestoreRequested=true, which assumes that flag stays set after the
	// admin acts on the request. If your tenant clears the flag on
	// resolution, enable this to ensure the "restored" / "declined"
	// transitions are still captured. Dedup eliminates any overlap.
	IncludeResolved bool `json:"include_resolved" yaml:"include_resolved"`

	// Deduper avoids re-emitting the same entity for the same state on
	// every poll. Transitions are still emitted because the dedup key
	// includes the entityUpdated timestamp. If nil, Start allocates one
	// sized to Lookback + 1h (24h floor); a caller-supplied deduper takes
	// precedence. The source's Close releases the deduper unconditionally,
	// matching the ownership convention used by the o365 adapter.
	Deduper utils.Deduper `json:"-" yaml:"-"`
}

func (c *RestoreRequestsConfig) Name() string    { return "restore_requests" }
func (c *RestoreRequestsConfig) IsEnabled() bool { return c != nil && c.Enabled }

func (c *RestoreRequestsConfig) Validate() error {
	if len(c.Saas) == 0 {
		c.Saas = append([]string{}, defaultRestoreSaas...)
	}
	for _, s := range c.Saas {
		if _, ok := defaultRestoreSaasEntity[s]; !ok {
			return fmt.Errorf("restore_requests.saas %q is not supported (only %v carry the restore flags)", s, defaultRestoreSaas)
		}
	}
	if c.PollInterval <= 0 {
		c.PollInterval = defaultRestorePollInterval
	}
	if c.Lookback <= 0 {
		c.Lookback = defaultRestoreLookback
	}
	return nil
}

func (c *RestoreRequestsConfig) Start(a *HarmonyAdapter) error {
	if c.Deduper == nil {
		// Size the TTL to the full lookback window so a still-pending
		// request — which the gateway will keep returning every poll for
		// as long as it remains in the lookback window — doesn't fall out
		// of dedup and get re-emitted as if it were new. Floor at 24h.
		ttl := c.Lookback + 1*time.Hour
		if ttl < 24*time.Hour {
			ttl = 24 * time.Hour
		}
		d, err := utils.NewLocalDeduper(1*time.Hour, ttl)
		if err != nil {
			return err
		}
		c.Deduper = d
	}
	for _, saas := range c.Saas {
		a.wgSenders.Add(1)
		go a.fetchRestoreRequestsForSaas(saas)
	}
	return nil
}

func (c *RestoreRequestsConfig) Close() {
	if c.Deduper != nil {
		// Close any deduper attached to this source — both ones we allocated
		// and caller-supplied ones, matching the convention other adapters
		// in this repo use (e.g. o365).
		c.Deduper.Close()
		c.Deduper = nil
	}
}

type HarmonyConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// Infinity Portal API credentials (Global Settings > API Keys).
	// For Infinity Events the key must include the "Logs as a Service" service.
	// For Restore Requests the key must include the Harmony Email & Collaboration service.
	// One key with both services attached is fine.
	ClientID  string `json:"client_id" yaml:"client_id"`
	AccessKey string `json:"access_key" yaml:"access_key"`

	// Base URL of the Infinity Portal gateway. Defaults to the global gateway
	// "https://cloudinfra-gw.portal.checkpoint.com". Use the regional variant
	// (e.g. "https://cloudinfra-gw-us.portal.checkpoint.com") if your tenant
	// lives in a regional data center. Both /app/laas-logs-api and /app/hec-api
	// share the same hostname per region.
	URL string `json:"url" yaml:"url"`

	Events          EventsConfig          `json:"events" yaml:"events"`
	RestoreRequests RestoreRequestsConfig `json:"restore_requests" yaml:"restore_requests"`
}

// sources returns the registered ingestion sources in a stable order. Adding
// a new source is a single line here plus the source-config struct.
func (c *HarmonyConfig) sources() []harmonySource {
	return []harmonySource{&c.Events, &c.RestoreRequests}
}

func (c *HarmonyConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.AccessKey == "" {
		return errors.New("missing access_key")
	}
	if c.URL == "" {
		c.URL = defaultBaseURL
	}
	c.URL = strings.TrimRight(c.URL, "/")

	anyEnabled := false
	names := make([]string, 0, 4)
	for _, s := range c.sources() {
		names = append(names, s.Name())
		if !s.IsEnabled() {
			continue
		}
		anyEnabled = true
		if err := s.Validate(); err != nil {
			return fmt.Errorf("%s: %v", s.Name(), err)
		}
	}
	if !anyEnabled {
		return fmt.Errorf("at least one source must be enabled (%s)", strings.Join(names, ", "))
	}
	return nil
}

type HarmonyAdapter struct {
	conf       HarmonyConfig
	uspClient  *uspclient.Client
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	tokenMu      sync.Mutex
	cachedToken  string
	tokenExpires time.Time

	ctx context.Context
}

func NewHarmonyAdapter(ctx context.Context, conf HarmonyConfig) (*HarmonyAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &HarmonyAdapter{
		conf:      conf,
		ctx:       context.Background(),
		doStop:    utils.NewEvent(),
		chStopped: make(chan struct{}),
	}

	var err error
	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	a.httpClient = &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
		},
	}

	// Start every enabled source. If one fails after others have already
	// spawned workers we have to fully tear down — signal doStop, wait for
	// goroutines to exit, then release each source's resources — before
	// returning the error to the caller. Otherwise we'd leak running
	// workers and dedupers.
	//
	// We operate on the sources rooted in a.conf (the adapter's copy of
	// the config) rather than the caller's `conf` parameter so the same
	// HarmonyAdapter.Close path releases everything later.
	for _, s := range a.conf.sources() {
		if !s.IsEnabled() {
			continue
		}
		if err := s.Start(a); err != nil {
			a.doStop.Set()
			a.wgSenders.Wait()
			for _, prev := range a.conf.sources() {
				prev.Close()
			}
			_, _ = a.uspClient.Close()
			a.httpClient.CloseIdleConnections()
			return nil, nil, fmt.Errorf("%s.Start: %v", s.Name(), err)
		}
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *HarmonyAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()
	a.httpClient.CloseIdleConnections()
	for _, s := range a.conf.sources() {
		s.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

// ----- Source 1: Infinity Events ----------------------------------------------------------

func (a *HarmonyAdapter) fetchEventsForService(service string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("harmony events worker exiting (service=%q)", service))

	nextStart := time.Now().UTC().Add(-defaultEventsInitialLookback)

	for {
		endTime := time.Now().UTC().Add(-defaultEventsEndLag)
		if !endTime.After(nextStart) {
			if a.doStop.WaitFor(a.conf.Events.PollInterval) {
				return
			}
			continue
		}

		newCursor, err := a.runOneEventsQuery(service, nextStart, endTime)
		var canceled *taskCanceledError
		switch {
		case errors.As(err, &canceled):
			// Soft failure: the gateway accepted the query but couldn't
			// fulfill it for this service (commonly: the service isn't
			// provisioned for the tenant). Warn once per poll with the
			// gateway's error detail, advance the cursor so we don't keep
			// re-querying the same window, and let the next poll try
			// again — most non-provisioned services stay non-provisioned,
			// so the operator should remove them from cloud_services if
			// they want the warnings to stop.
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("harmony[events:%s]: %v — skipping window", service, canceled))
			nextStart = endTime
		case err != nil:
			a.conf.ClientOptions.OnError(fmt.Errorf("harmony[events:%s]: %v", service, err))
		case !newCursor.IsZero():
			nextStart = newCursor
		}

		if a.doStop.WaitFor(a.conf.Events.PollInterval) {
			return
		}
	}
}

func (a *HarmonyAdapter) runOneEventsQuery(service string, startTime, endTime time.Time) (time.Time, error) {
	taskID, err := a.submitEventsQuery(service, startTime, endTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("submitEventsQuery: %v", err)
	}

	pageTokens, err := a.waitForEventsTask(taskID)
	if err != nil {
		// Wrap with %w so callers can errors.As to detect taskCanceledError.
		return time.Time{}, fmt.Errorf("waitForEventsTask: %w", err)
	}
	if len(pageTokens) == 0 {
		return endTime, nil
	}

	latest := time.Time{}
	pageToken := pageTokens[0]
	for {
		if a.doStop.IsSet() {
			return latest, nil
		}
		records, nextToken, err := a.retrieveEventsPage(taskID, pageToken)
		if err != nil {
			return latest, fmt.Errorf("retrieveEventsPage: %v", err)
		}
		for _, rec := range records {
			ts := extractRecordTime(rec)
			if ts.After(latest) {
				latest = ts
			}
			if _, ok := rec["_lc_harmony_source"]; !ok {
				rec["_lc_harmony_source"] = "infinity_events"
			}
			if _, ok := rec["_lc_harmony_service"]; !ok {
				rec["_lc_harmony_service"] = service
			}
			if err := a.shipRecord(rec); err != nil {
				return latest, err
			}
		}
		if nextToken == "" || nextToken == "NULL" {
			break
		}
		pageToken = nextToken
	}

	if latest.IsZero() {
		latest = endTime
	}
	return latest, nil
}

func (a *HarmonyAdapter) submitEventsQuery(service string, startTime, endTime time.Time) (string, error) {
	body := utils.Dict{
		"cloudService": service,
		"timeframe": utils.Dict{
			"startTime": startTime.UTC().Format(time.RFC3339),
			"endTime":   endTime.UTC().Format(time.RFC3339),
		},
		"pageLimit": a.conf.Events.PageLimit,
		"limit":     a.conf.Events.Limit,
	}
	if a.conf.Events.Filter != "" {
		body["filter"] = a.conf.Events.Filter
	}

	resp, err := a.doAuthRequest("POST", a.conf.URL+eventsQueryPath, body, nil)
	if err != nil {
		return "", err
	}
	data, _ := resp.GetDict("data")
	taskID, _ := data.GetString("taskId")
	if taskID == "" {
		return "", fmt.Errorf("missing taskId in response: %s", asJSON(resp))
	}
	return taskID, nil
}

// taskCanceledError is returned when the gateway terminates a logs_query
// task with state "Canceled". This is the gateway's response when it can't
// fulfill the query (e.g. a cloud service that's listed in our default set
// but not provisioned for the tenant), and it surfaces as HTTP 200 with the
// "Canceled" state field — not as a transport error. We carry the gateway's
// error details on the type so the worker can emit a meaningful warning,
// and so callers can errors.As for soft-fail handling.
type taskCanceledError struct {
	TaskID  string
	Details []string
}

func (e *taskCanceledError) Error() string {
	if len(e.Details) == 0 {
		return fmt.Sprintf("task %s canceled by server", e.TaskID)
	}
	return fmt.Sprintf("task %s canceled by server: %s", e.TaskID, strings.Join(e.Details, "; "))
}

func (a *HarmonyAdapter) waitForEventsTask(taskID string) ([]string, error) {
	deadline := time.Now().Add(defaultEventsStatusPollTotal)
	for {
		if a.doStop.IsSet() {
			return nil, nil
		}
		resp, err := a.doAuthRequest("GET", a.conf.URL+eventsQueryPath+"/"+taskID, nil, nil)
		if err != nil {
			return nil, err
		}
		data, _ := resp.GetDict("data")
		state, _ := data.GetString("state")
		switch state {
		case "Ready":
			rawTokens, _ := data.GetList("pageTokens")
			tokens := make([]string, 0, len(rawTokens))
			for _, t := range rawTokens {
				if s, ok := t.(string); ok && s != "" {
					tokens = append(tokens, s)
				}
			}
			return tokens, nil
		case "Done":
			return nil, nil
		case "Canceled":
			details := []string{}
			if rawErrs, ok := data.GetList("errors"); ok {
				for _, e := range rawErrs {
					// Today the gateway emits plain strings here. JSON-marshal
					// anything else so the warning still carries the detail if
					// Check Point ever switches to structured error objects.
					if s, ok := e.(string); ok {
						details = append(details, s)
						continue
					}
					if b, err := json.Marshal(e); err == nil {
						details = append(details, string(b))
					}
				}
			}
			return nil, &taskCanceledError{TaskID: taskID, Details: details}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("task %s did not reach a terminal state within %s (last state=%q)", taskID, defaultEventsStatusPollTotal, state)
		}
		if a.doStop.WaitFor(defaultEventsStatusPollEvery) {
			return nil, nil
		}
	}
}

func (a *HarmonyAdapter) retrieveEventsPage(taskID, pageToken string) ([]utils.Dict, string, error) {
	body := utils.Dict{
		"taskId":    taskID,
		"pageToken": pageToken,
	}
	resp, err := a.doAuthRequest("POST", a.conf.URL+eventsRetrievePath, body, nil)
	if err != nil {
		return nil, "", err
	}
	data, _ := resp.GetDict("data")
	rawRecords, _ := data.GetList("records")
	records := make([]utils.Dict, 0, len(rawRecords))
	for _, r := range rawRecords {
		switch v := r.(type) {
		case map[string]interface{}:
			records = append(records, utils.Dict(v))
		case utils.Dict:
			records = append(records, v)
		}
	}
	nextToken, _ := data.GetString("nextPageToken")
	return records, nextToken, nil
}

// ----- Source 2: HEC Restore Requests -----------------------------------------------------

func (a *HarmonyAdapter) fetchRestoreRequestsForSaas(saas string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("harmony restore-requests worker exiting (saas=%q)", saas))

	// Filters to issue per poll. The primary pass catches every pending and
	// post-decision entity if the gateway keeps isRestoreRequested set after
	// the admin acts. When IncludeResolved is set we additionally search by
	// isRestored=true and isRestoreDeclined=true so the transition is still
	// captured if the tenant clears the original flag on resolution. Dedup
	// suppresses any overlap.
	filters := []utils.Dict{
		{"saasAttrName": "entityPayload.isRestoreRequested", "saasAttrOp": "is", "saasAttrValue": "true"},
	}
	if a.conf.RestoreRequests.IncludeResolved {
		filters = append(filters,
			utils.Dict{"saasAttrName": "entityPayload.isRestored", "saasAttrOp": "is", "saasAttrValue": "true"},
			utils.Dict{"saasAttrName": "entityPayload.isRestoreDeclined", "saasAttrOp": "is", "saasAttrValue": "true"},
		)
	}

	for {
		for _, f := range filters {
			if a.doStop.IsSet() {
				return
			}
			if err := a.runOneRestoreRequestsQuery(saas, f); err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("harmony[restore_requests:%s:%s]: %v", saas, f["saasAttrName"], err))
			}
		}
		if a.doStop.WaitFor(a.conf.RestoreRequests.PollInterval) {
			return
		}
	}
}

// runOneRestoreRequestsQuery scrolls through every email entity matching the
// supplied extended filter within the configured lookback window and ships
// each entity once per (entityId, entityUpdated) pair. The gateway bumps
// entityUpdated on every state change, which is how we naturally surface
// transitions (pending → restored, pending → declined) without re-emitting
// still-pending requests on subsequent polls.
func (a *HarmonyAdapter) runOneRestoreRequestsQuery(saas string, extendedFilter utils.Dict) error {
	saasEntity, ok := defaultRestoreSaasEntity[saas]
	if !ok {
		return fmt.Errorf("unsupported saas %q", saas)
	}
	startDate := time.Now().UTC().Add(-a.conf.RestoreRequests.Lookback).Format(time.RFC3339)
	endDate := time.Now().UTC().Format(time.RFC3339)

	scrollID := ""
	for {
		if a.doStop.IsSet() {
			return nil
		}

		body := utils.Dict{
			"requestData": utils.Dict{
				"entityFilter": utils.Dict{
					"saas":       saas,
					"saasEntity": saasEntity,
					"startDate":  startDate,
					"endDate":    endDate,
				},
				"entityExtendedFilter": []utils.Dict{extendedFilter},
				"scrollId":             scrollID,
			},
		}

		headers := map[string]string{
			"x-av-req-id": newRequestID(),
		}
		resp, err := a.doAuthRequest("POST", a.conf.URL+hecSearchEntityPath, body, headers)
		if err != nil {
			return err
		}

		envelope, _ := resp.GetDict("responseEnvelope")
		nextScroll, _ := envelope.GetString("scrollId")
		records, _ := resp.GetListOfDict("responseData")

		for _, rec := range records {
			key := restoreDedupKey(rec)
			if key == "" {
				continue
			}
			if a.conf.RestoreRequests.Deduper.CheckAndAdd(key) {
				continue
			}
			rec["_lc_harmony_source"] = "restore_requests"
			rec["_lc_harmony_saas"] = saas
			if err := a.shipRecord(rec); err != nil {
				return err
			}
		}

		if nextScroll == "" || nextScroll == scrollID || len(records) == 0 {
			return nil
		}
		scrollID = nextScroll
	}
}

// dictBoolish reads a field that may be encoded as either a JSON boolean or
// a string ("true" / "false"). The HEC API documentation (Feb 2026 ed.)
// shows these fields as strings in its example payloads, but the live
// gateway returns native JSON booleans. We accept both so the adapter
// works regardless of which form a given tenant or version emits.
func dictBoolish(d utils.Dict, key string) (value, found bool) {
	if v, ok := d[key]; ok {
		switch t := v.(type) {
		case bool:
			return t, true
		case string:
			return strings.EqualFold(t, "true"), true
		}
	}
	return false, false
}

// restoreDedupKey returns a key that changes only when the email's restore
// state advances. We anchor on entityId + entityUpdated because the gateway
// bumps entityUpdated on every state change, while a still-pending request
// keeps the same timestamp poll-over-poll.
func restoreDedupKey(rec utils.Dict) string {
	info, _ := rec.GetDict("entityInfo")
	entityID, _ := info.GetString("entityId")
	if entityID == "" {
		return ""
	}
	updated, _ := info.GetString("entityUpdated")
	if updated == "" {
		// Fall back to a fingerprint of the relevant flags so we still
		// dedup if the gateway ever omits entityUpdated.
		payload, _ := rec.GetDict("entityPayload")
		req, _ := dictBoolish(payload, "isRestoreRequested")
		restored, _ := dictBoolish(payload, "isRestored")
		declined, _ := dictBoolish(payload, "isRestoreDeclined")
		updated = fmt.Sprintf("req=%t|restored=%t|declined=%t", req, restored, declined)
	}
	return "restore:" + entityID + "|" + updated
}

// ----- Shared helpers ---------------------------------------------------------------------

func (a *HarmonyAdapter) shipRecord(rec utils.Dict) error {
	if rec == nil {
		return nil
	}
	msg := &protocol.DataMessage{
		JsonPayload: rec,
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			a.doStop.Set()
			return err
		}
	}
	return nil
}

// retryBackoff returns the wait before the given attempt (attempt is
// 1-indexed: the wait before the 2nd attempt is requestRetryBackoff[0]).
// A ±20% jitter is applied so the per-service event workers don't all
// retry in lockstep against an already-struggling gateway.
func retryBackoff(attempt int) time.Duration {
	i := attempt - 1
	if i < 0 {
		i = 0
	}
	if i >= len(requestRetryBackoff) {
		i = len(requestRetryBackoff) - 1
	}
	base := requestRetryBackoff[i]
	span := int64(base) / 5 // 20%
	if span <= 0 {
		return base
	}
	return base + time.Duration(rand.Int64N(2*span)-span)
}

// doHTTPWithRetry executes buildReq()'s request with bounded retry on
// transient failures (client/context timeouts, dropped connections, and
// HTTP 502/503/504). buildReq is invoked once per attempt so a request
// body is a fresh reader each time. It returns the final status+body for
// any non-transient response (including 4xx — the caller decides what those
// mean); err is non-nil only for a non-transient transport error or an
// exhausted retry budget. Backoff is interruptible by doStop.
func (a *HarmonyAdapter) doHTTPWithRetry(label string, buildReq func() (*http.Request, error)) (int, []byte, error) {
	var lastErr error
	for attempt := 0; attempt < maxRequestAttempts; attempt++ {
		if attempt > 0 {
			d := retryBackoff(attempt)
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("%s: transient failure, retry %d/%d after %s: %v",
				label, attempt, maxRequestAttempts-1, d, lastErr))
			if a.doStop.WaitFor(d) {
				return 0, nil, fmt.Errorf("%s: aborted during retry backoff", label)
			}
		}

		req, err := buildReq()
		if err != nil {
			return 0, nil, err
		}
		resp, err := a.httpClient.Do(req)
		if err != nil {
			if !isTransientErr(err) {
				return 0, nil, err
			}
			lastErr = err
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if isTransientStatus(resp.StatusCode) {
			lastErr = fmt.Errorf("%s: HTTP %d: %s", label, resp.StatusCode, string(respBody))
			continue
		}
		return resp.StatusCode, respBody, nil
	}
	return 0, nil, fmt.Errorf("%s: exhausted %d attempts: %v", label, maxRequestAttempts, lastErr)
}

// doAuthRequest issues an HTTP request with a valid bearer token attached.
// If body is nil, no request body is sent. Additional headers (e.g.
// x-av-req-id required by HEC endpoints) can be supplied via extraHeaders.
// Transient gateway failures are absorbed by doHTTPWithRetry; a 401 triggers
// a single token refresh + re-issue (itself transient-retried).
func (a *HarmonyAdapter) doAuthRequest(method, url string, body utils.Dict, extraHeaders map[string]string) (utils.Dict, error) {
	bodyBytes, err := marshalBody(body)
	if err != nil {
		return nil, err
	}

	token, err := a.getToken(false)
	if err != nil {
		return nil, err
	}

	build := func(tok string) func() (*http.Request, error) {
		return func() (*http.Request, error) {
			var reader io.Reader
			if bodyBytes != nil {
				reader = bytes.NewReader(bodyBytes)
			}
			req, err := http.NewRequestWithContext(a.ctx, method, url, reader)
			if err != nil {
				return nil, err
			}
			req.Header.Set("Accept", "application/json")
			if bodyBytes != nil {
				req.Header.Set("Content-Type", "application/json")
			}
			req.Header.Set("Authorization", "Bearer "+tok)
			for k, v := range extraHeaders {
				req.Header.Set(k, v)
			}
			return req, nil
		}
	}

	label := method + " " + url
	status, respBody, err := a.doHTTPWithRetry(label, build(token))
	if err != nil {
		return nil, err
	}

	// 401: token likely expired mid-flight. Refresh once and re-issue;
	// the re-issue is itself transient-retried. A persistent 401 (revoked
	// key / IP allowlist) falls through to the non-2xx return below.
	if status == http.StatusUnauthorized {
		token, err = a.getToken(true)
		if err != nil {
			return nil, err
		}
		status, respBody, err = a.doHTTPWithRetry(label, build(token))
		if err != nil {
			return nil, err
		}
	}

	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("%s: HTTP %d: %s", label, status, string(respBody))
	}

	out := utils.Dict{}
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &out); err != nil {
			return nil, fmt.Errorf("decode response: %v (body=%s)", err, string(respBody))
		}
	}
	return out, nil
}

// isTransientStatus reports whether an HTTP status is a retryable gateway
// hiccup (bad gateway / unavailable / gateway timeout) as opposed to a
// client error we shouldn't retry. 429 is intentionally excluded: the
// adapter's poll cadence stays well under the documented Infinity Events
// limits, so a 429 signals a config problem (too many cloud_services / too
// short a poll_interval) that should surface loudly rather than be masked
// by silent retries.
func isTransientStatus(status int) bool {
	return status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}

// isTransientErr reports whether a transport-level error is worth retrying:
// client/context timeouts and dropped connections. Anything else (bad URL,
// TLS failure, etc.) is returned to the caller as-is.
func isTransientErr(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	msg := err.Error()
	for _, frag := range []string{
		"context deadline exceeded",
		"Client.Timeout",
		"connection reset",
		"connection refused",
		"unexpected EOF",
		"i/o timeout",
		"TLS handshake timeout",
		"server closed idle connection",
	} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}

func (a *HarmonyAdapter) getToken(force bool) (string, error) {
	a.tokenMu.Lock()
	defer a.tokenMu.Unlock()

	if !force && a.cachedToken != "" && time.Now().Add(tokenRefreshSkew).Before(a.tokenExpires) {
		return a.cachedToken, nil
	}

	reqBody, err := json.Marshal(utils.Dict{
		"clientId":  a.conf.ClientID,
		"accessKey": a.conf.AccessKey,
	})
	if err != nil {
		return "", err
	}
	// Route through the shared retry helper so an intermittent gateway
	// blip on /auth/external is absorbed the same way data calls are —
	// otherwise an auth-endpoint timeout still produces the OnError +
	// window-rerun the data-path retry was added to prevent. A 401 here
	// means bad creds / IP allowlist (not transient) and is returned.
	status, respBody, err := a.doHTTPWithRetry("POST "+a.conf.URL+authPath, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(a.ctx, "POST", a.conf.URL+authPath, bytes.NewReader(reqBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})
	if err != nil {
		return "", fmt.Errorf("auth request: %v", err)
	}
	if status < 200 || status >= 300 {
		return "", fmt.Errorf("auth HTTP %d: %s", status, string(respBody))
	}
	parsed := utils.Dict{}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", fmt.Errorf("auth decode: %v", err)
	}
	data, _ := parsed.GetDict("data")
	token, _ := data.GetString("token")
	if token == "" {
		return "", fmt.Errorf("auth response missing token: %s", string(respBody))
	}

	expires := time.Now().Add(25 * time.Minute)
	if n, ok := data.GetInt("expiresIn"); ok && n > 0 {
		expires = time.Now().Add(time.Duration(n) * time.Second)
	} else if s, ok := data.GetString("expires"); ok && s != "" {
		if t, err := time.Parse(time.RFC1123, s); err == nil {
			expires = t
		}
	}

	a.cachedToken = token
	a.tokenExpires = expires
	return token, nil
}

func marshalBody(body utils.Dict) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal body: %v", err)
	}
	return b, nil
}

// extractRecordTime returns the timestamp embedded in an Infinity Events
// record, used to advance the per-service cursor. RFC3339Nano accepts both
// fractional and non-fractional seconds, so a single parse call covers both
// the docs' "2020-01-01T00:00:00.000Z" form and the bare RFC3339 form.
func extractRecordTime(rec utils.Dict) time.Time {
	if rec == nil {
		return time.Time{}
	}
	for _, key := range []string{"time", "eventTime", "timestamp"} {
		if s, ok := rec.GetString(key); ok && s != "" {
			if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
				return t
			}
		}
	}
	return time.Time{}
}

func asJSON(d utils.Dict) string {
	b, err := json.Marshal(d)
	if err != nil {
		return fmt.Sprintf("%+v", d)
	}
	return string(b)
}

// newRequestID returns a UUID-shaped string for the x-av-req-id header HEC
// requires. We don't need cryptographic uniqueness; a timestamp + counter is
// enough to give Check Point a per-request handle for support.
var reqIDCounter struct {
	sync.Mutex
	n uint64
}

func newRequestID() string {
	reqIDCounter.Lock()
	reqIDCounter.n++
	n := reqIDCounter.n
	reqIDCounter.Unlock()
	now := time.Now().UnixNano()
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", uint32(now>>32), uint16(now>>16), uint16(now), uint16(n>>16), uint64(n))
}
