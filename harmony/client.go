package usp_harmony

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
)

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
	defaultEventsPollInterval     = 60 * time.Second
	defaultEventsStatusPollEvery  = 5 * time.Second
	defaultEventsStatusPollTotal  = 10 * time.Minute
	defaultEventsInitialLookback  = 1 * time.Hour
	defaultEventsEndLag           = 1 * time.Minute
	defaultEventsPageLimit        = 100
	defaultEventsPerCloudLimit    = 5000
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

	// Deduper is used by the restore-request source to avoid re-emitting the
	// same entity for the same state on every poll. Transitions are still
	// emitted because the dedup key includes the entityUpdated timestamp.
	Deduper utils.Deduper `json:"-" yaml:"-"`
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

	if !c.Events.Enabled && !c.RestoreRequests.Enabled {
		return errors.New("at least one of events.enabled or restore_requests.enabled must be true")
	}

	if c.Events.Enabled {
		if len(c.Events.CloudServices) == 0 {
			c.Events.CloudServices = append([]string{}, defaultEventsCloudServices...)
		}
		if c.Events.PollInterval <= 0 {
			c.Events.PollInterval = defaultEventsPollInterval
		}
		if c.Events.PageLimit <= 0 {
			c.Events.PageLimit = defaultEventsPageLimit
		}
		if c.Events.PageLimit < 10 {
			// Gateway rejects values below 10 with HTTP 400.
			c.Events.PageLimit = 10
		}
		if c.Events.Limit <= 0 {
			c.Events.Limit = defaultEventsPerCloudLimit
		}
		if c.Events.Limit < 10 {
			c.Events.Limit = 10
		}
	}

	if c.RestoreRequests.Enabled {
		if len(c.RestoreRequests.Saas) == 0 {
			c.RestoreRequests.Saas = append([]string{}, defaultRestoreSaas...)
		}
		for _, s := range c.RestoreRequests.Saas {
			if _, ok := defaultRestoreSaasEntity[s]; !ok {
				return fmt.Errorf("restore_requests.saas %q is not supported (only %v carry the restore flags)", s, defaultRestoreSaas)
			}
		}
		if c.RestoreRequests.PollInterval <= 0 {
			c.RestoreRequests.PollInterval = defaultRestorePollInterval
		}
		if c.RestoreRequests.Lookback <= 0 {
			c.RestoreRequests.Lookback = defaultRestoreLookback
		}
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

	// The deduper is only used by the restore-request source. Size its TTL
	// to cover the full lookback window so a still-pending request — which
	// the gateway will keep returning every poll for as long as it remains
	// in the lookback window — doesn't fall out of dedup and get re-emitted
	// as if it were new. Floor at 24h for sanity even when Lookback is tiny.
	ownedDeduper := false
	if conf.RestoreRequests.Enabled && conf.Deduper == nil {
		ttl := conf.RestoreRequests.Lookback + 1*time.Hour
		if ttl < 24*time.Hour {
			ttl = 24 * time.Hour
		}
		d, err := utils.NewLocalDeduper(1*time.Hour, ttl)
		if err != nil {
			return nil, nil, err
		}
		conf.Deduper = d
		ownedDeduper = true
	}

	a := &HarmonyAdapter{
		conf:      conf,
		ctx:       context.Background(),
		doStop:    utils.NewEvent(),
		chStopped: make(chan struct{}),
	}

	// closeOwnedOnFail releases the deduper goroutine on error paths so the
	// constructor doesn't leak it. We only close the deduper we created
	// ourselves — a caller-supplied one is the caller's to manage.
	closeOwnedOnFail := func() {
		if ownedDeduper && a.conf.Deduper != nil {
			a.conf.Deduper.Close()
		}
	}

	var err error
	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		closeOwnedOnFail()
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

	if a.conf.Events.Enabled {
		for _, svc := range a.conf.Events.CloudServices {
			a.wgSenders.Add(1)
			go a.fetchEventsForService(svc)
		}
	}

	if a.conf.RestoreRequests.Enabled {
		for _, saas := range a.conf.RestoreRequests.Saas {
			a.wgSenders.Add(1)
			go a.fetchRestoreRequestsForSaas(saas)
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
	if d := a.conf.Deduper; d != nil {
		d.Close()
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
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("harmony[events:%s]: %v", service, err))
		} else if !newCursor.IsZero() {
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
		return time.Time{}, fmt.Errorf("waitForEventsTask: %v", err)
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
			return nil, fmt.Errorf("task %s canceled by server", taskID)
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
			if a.conf.Deduper.CheckAndAdd(key) {
				continue
			}
			rec["_lc_harmony_source"] = "restore_requests"
			rec["_lc_harmony_saas"] = saas
			rec["_lc_harmony_state"] = restoreLifecycleState(rec)
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

// restoreLifecycleState classifies the current restore lifecycle stage so
// downstream consumers can filter without re-parsing the flag combination.
func restoreLifecycleState(rec utils.Dict) string {
	payload, _ := rec.GetDict("entityPayload")
	restored, _ := dictBoolish(payload, "isRestored")
	declined, _ := dictBoolish(payload, "isRestoreDeclined")
	switch {
	case restored:
		return "restored"
	case declined:
		return "declined"
	default:
		return "pending"
	}
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

// doAuthRequest issues an HTTP request with a valid bearer token attached.
// If body is nil, no request body is sent. Additional headers (e.g.
// x-av-req-id required by HEC endpoints) can be supplied via extraHeaders.
func (a *HarmonyAdapter) doAuthRequest(method, url string, body utils.Dict, extraHeaders map[string]string) (utils.Dict, error) {
	bodyBytes, err := marshalBody(body)
	if err != nil {
		return nil, err
	}

	token, err := a.getToken(false)
	if err != nil {
		return nil, err
	}

	doOnce := func(tok string) (int, []byte, error) {
		var reader io.Reader
		if bodyBytes != nil {
			reader = bytes.NewReader(bodyBytes)
		}
		req, err := http.NewRequestWithContext(a.ctx, method, url, reader)
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Accept", "application/json")
		if bodyBytes != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		for k, v := range extraHeaders {
			req.Header.Set(k, v)
		}
		resp, err := a.httpClient.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, respBody, nil
	}

	status, respBody, err := doOnce(token)
	if err != nil {
		return nil, err
	}
	if status == http.StatusUnauthorized {
		token, err = a.getToken(true)
		if err != nil {
			return nil, err
		}
		status, respBody, err = doOnce(token)
		if err != nil {
			return nil, err
		}
	}

	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("%s %s: HTTP %d: %s", method, url, status, string(respBody))
	}

	out := utils.Dict{}
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &out); err != nil {
			return nil, fmt.Errorf("decode response: %v (body=%s)", err, string(respBody))
		}
	}
	return out, nil
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
	req, err := http.NewRequestWithContext(a.ctx, "POST", a.conf.URL+authPath, bytes.NewReader(reqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("auth request: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("auth HTTP %d: %s", resp.StatusCode, string(respBody))
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
