package usp_entraid

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

var scope = "https://graph.microsoft.com/.default"
var URL = map[string]string{
	"get_alerts": "https://graph.microsoft.com/v1.0/identityProtection/riskDetections",
}

const (
	defaultLoginEndpoint = "https://login.microsoftonline.com"
	defaultGraphEndpoint = "https://graph.microsoft.com"
	defaultPollInterval  = 30 * time.Second

	// defaultStreams preserves the historical behavior of the adapter, which
	// only polled Identity Protection risk detections.
	defaultStreams = "risk_detections"

	// maxPagesPerPoll bounds how many @odata.nextLink continuations are
	// followed within a single poll. Anything left over is picked up by the
	// next poll through the timestamp cursor; a warning is emitted when the
	// cap is hit so sustained truncation is visible.
	maxPagesPerPoll = 50

	// ingestionLookback compensates for Microsoft Graph ingestion delay: an
	// event can become visible in the API after events with later timestamps
	// have already been returned, and a cursor that only moves forward would
	// then never request it. Each poll re-requests this much history behind
	// the newest timestamp seen and dedups by event ID, so late arrivals
	// inside the window still ship exactly once.
	ingestionLookback = 5 * time.Minute

	// filterTimeLayout is how cursor timestamps are rendered into the OData
	// $filter (UTC with fractional seconds, as Graph emits them).
	filterTimeLayout = "2006-01-02T15:04:05.000000Z"
)

// entraStream describes one Microsoft Graph collection the adapter can poll.
// Each collection filters and cursors on a different timestamp field. orderBy,
// when set, is sent as $orderby so truncated result sets drain oldest-first
// across polls -- Graph does not guarantee an ordering otherwise (and commonly
// returns these collections newest-first).
type entraStream struct {
	name    string
	path    string
	tsField string
	orderBy string
}

// Streams supported through EntraIDConfig.Streams. sign_ins requires the
// tenant to hold an Entra ID P1/P2 license (a Microsoft Graph requirement);
// sign_ins and audit_logs both require the AuditLog.Read.All application
// permission, risk_detections requires IdentityRiskEvent.Read.All.
// risk_detections deliberately sends no $orderby, preserving the historical
// request shape for existing deployments.
var entraStreamsByName = map[string]entraStream{
	"risk_detections": {name: "risk_detections", path: "/v1.0/identityProtection/riskDetections", tsField: "activityDateTime"},
	"sign_ins":        {name: "sign_ins", path: "/v1.0/auditLogs/signIns", tsField: "createdDateTime", orderBy: "createdDateTime asc"},
	"audit_logs":      {name: "audit_logs", path: "/v1.0/auditLogs/directoryAudits", tsField: "activityDateTime", orderBy: "activityDateTime asc"},
}

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type EntraIDAdapter struct {
	conf       EntraIDConfig
	uspClient  uspSink
	httpClient *http.Client

	endpoint     string
	pollInterval time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type EntraIDConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	TenantID      string                  `json:"tenant_id" yaml:"tenant_id"`
	ClientID      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`

	// Streams selects which Entra ID collections to poll, as comma separated
	// values. Supported values: "risk_detections" (Identity Protection risk
	// detections), "sign_ins" (auditLogs/signIns sign-in logs) and
	// "audit_logs" (auditLogs/directoryAudits directory audit logs). Empty
	// selects "risk_detections" only, preserving the historical behavior of
	// existing deployments.
	Streams string `json:"streams,omitempty" yaml:"streams,omitempty"`

	// LoginEndpoint overrides the base URL of the Microsoft identity platform
	// used for the OAuth2 client_credentials token exchange. Defaults to
	// https://login.microsoftonline.com when empty.
	LoginEndpoint string `json:"login_endpoint,omitempty" yaml:"login_endpoint,omitempty"`

	// GraphEndpoint overrides the base URL of the Microsoft Graph API the
	// collections are fetched from. Defaults to https://graph.microsoft.com
	// when empty.
	GraphEndpoint string `json:"graph_endpoint,omitempty" yaml:"graph_endpoint,omitempty"`

	// PollInterval overrides the wait between polls of each stream (default
	// 30s). It is not settable through a config file; it exists as a seam for
	// tests.
	PollInterval time.Duration `json:"-" yaml:"-"`
}

// loginEndpoint returns the Microsoft identity platform base URL to use,
// defaulting to the public endpoint when no override is configured.
func (c EntraIDConfig) loginEndpoint() string {
	if c.LoginEndpoint != "" {
		return strings.TrimRight(c.LoginEndpoint, "/")
	}
	return defaultLoginEndpoint
}

// graphEndpoint returns the Microsoft Graph base URL to use, defaulting to the
// public endpoint when no override is configured.
func (c EntraIDConfig) graphEndpoint() string {
	if c.GraphEndpoint != "" {
		return strings.TrimRight(c.GraphEndpoint, "/")
	}
	return defaultGraphEndpoint
}

// tokenURL is the OAuth2 client_credentials token endpoint for the tenant.
func (c EntraIDConfig) tokenURL() string {
	return fmt.Sprintf("%s/%s/oauth2/v2.0/token", c.loginEndpoint(), c.TenantID)
}

// riskDetectionsURL is the Identity Protection risk detections endpoint.
func (c EntraIDConfig) riskDetectionsURL() string {
	return c.graphEndpoint() + entraStreamsByName["risk_detections"].path
}

// streams resolves the configured comma separated stream names into their
// definitions, defaulting to risk detections when unset. Order is preserved
// and duplicates are collapsed.
func (c EntraIDConfig) streams() ([]entraStream, error) {
	raw := c.Streams
	if strings.TrimSpace(raw) == "" {
		raw = defaultStreams
	}
	streams := []entraStream{}
	seen := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		name := strings.ToLower(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		stream, ok := entraStreamsByName[name]
		if !ok {
			return nil, fmt.Errorf("unknown stream %q, supported streams: risk_detections, sign_ins, audit_logs", name)
		}
		seen[name] = struct{}{}
		streams = append(streams, stream)
	}
	if len(streams) == 0 {
		return nil, errors.New("no streams specified")
	}
	return streams, nil
}

func (c *EntraIDConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.TenantID == "" {
		return errors.New("missing tenant_id")
	}
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}
	if _, err := c.streams(); err != nil {
		return fmt.Errorf("streams: %v", err)
	}
	return nil
}

func NewEntraIDAdapter(ctx context.Context, conf EntraIDConfig) (*EntraIDAdapter, chan struct{}, error) {
	return newEntraIDAdapter(ctx, conf, nil)
}

// newEntraIDAdapter is the implementation behind NewEntraIDAdapter. When sink
// is non-nil it is used in place of a real LimaCharlie client -- the seam tests
// use to capture shipped events.
func newEntraIDAdapter(ctx context.Context, conf EntraIDConfig, sink uspSink) (*EntraIDAdapter, chan struct{}, error) {
	streams, err := conf.streams()
	if err != nil {
		return nil, nil, err
	}

	a := &EntraIDAdapter{
		conf:         conf,
		ctx:          context.Background(),
		doStop:       utils.NewEvent(),
		pollInterval: conf.PollInterval,
	}
	if a.pollInterval <= 0 {
		a.pollInterval = defaultPollInterval
	}

	if sink != nil {
		a.uspClient = sink
	} else {
		uspClient, err := uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			return nil, nil, err
		}
		a.uspClient = uspClient
	}

	a.httpClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
		},
	}

	a.chStopped = make(chan struct{})

	for _, stream := range streams {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting to fetch %s", stream.name))
		a.wgSenders.Add(1)
		go a.fetchEvents(stream)
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *EntraIDAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()
	a.httpClient.CloseIdleConnections()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *EntraIDAdapter) fetchToken() (string, error) {

	url := a.conf.tokenURL()
	payload := fmt.Sprintf("client_id=%s&scope=%s&grant_type=%s&client_secret=%s", a.conf.ClientID, scope, "client_credentials", a.conf.ClientSecret)

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(payload))
	if err != nil {
		return "", fmt.Errorf("no bearer token returned: %s", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("no bearer token returned: %s", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("no bearer token returned: %s", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("no bearer token returned: %s", err)
	}

	accessToken, ok := result["access_token"].(string)
	if !ok {
		return "", fmt.Errorf("no bearer token returned: %#v", result)
	}

	return accessToken, nil

}

// fetchEvents polls one Graph collection on the poll interval. The $filter
// requests everything at or after (latest timestamp seen - ingestionLookback),
// and the shipped map (event ID -> event timestamp) suppresses re-shipping
// anything already sent from that window. This tolerates both many events
// sharing one timestamp (the inclusive "ge" filter refetches them every poll)
// and events surfacing in the API out of timestamp order due to Graph
// ingestion delay, as long as the delay stays within the lookback.
func (a *EntraIDAdapter) fetchEvents(stream entraStream) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", stream.name))

	eventsUrl := a.conf.graphEndpoint() + stream.path
	// Collection starts at adapter startup: the lookback never reaches before
	// processStart, so restarts do not re-ship history (there is no persisted
	// cursor to dedup against across restarts).
	processStart := time.Now().UTC()
	latest := processStart
	shipped := map[string]time.Time{}

	for !a.doStop.WaitFor(a.pollInterval) {
		windowStart := latest.Add(-ingestionLookback)
		if windowStart.Before(processStart) {
			windowStart = processStart
		}

		// makeOneListRequest handles error reporting and retries.
		items, err := a.makeOneListRequest(eventsUrl, stream, windowStart)
		if err != nil {
			continue
		}

		for _, item := range items {
			if _, dup := shipped[item.id]; dup {
				continue
			}

			msg := &protocol.DataMessage{
				JsonPayload: item.payload,
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
					return
				}
			}

			shipped[item.id] = item.ts
			if item.ts.After(latest) {
				latest = item.ts
			}
		}

		// Prune IDs that fell out of the lookback window: the next $filter
		// can never return them again.
		cutoff := latest.Add(-ingestionLookback)
		for id, ts := range shipped {
			if ts.Before(cutoff) {
				delete(shipped, id)
			}
		}
	}
}

// entraEvent is one validated item from a Graph collection response.
type entraEvent struct {
	payload map[string]interface{}
	id      string
	ts      time.Time
}

// makeOneListRequest performs one poll of a Graph collection: it requests
// every item whose timestamp field is at or after windowStart (following
// @odata.nextLink continuations up to maxPagesPerPoll) and returns the items
// that carry a valid ID and timestamp.
func (a *EntraIDAdapter) makeOneListRequest(eventsUrl string, stream entraStream, windowStart time.Time) ([]entraEvent, error) {
	tsField := stream.tsField
	var rawItems []interface{}

	// Retry up to 3 times
	for attempt := 1; attempt <= 3; attempt++ {
		// Request everything at or after the window start. "ge" is inclusive
		// as OData defines it, so boundary events are always refetched and
		// deduped by the caller.
		since := windowStart.UTC().Format(filterTimeLayout)
		requestUrl := eventsUrl + "?%24filter=" + url.QueryEscape(fmt.Sprintf("%s ge %s", tsField, since))
		if stream.orderBy != "" {
			requestUrl += "&%24orderby=" + url.QueryEscape(stream.orderBy)
		}

		authToken, err := a.fetchToken()
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error fetching token: %s", err))
			return nil, err
		}

		items, err, isRetryable := a.fetchAllPages(requestUrl, authToken)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error fetching %s (attempt %d): %s", eventsUrl, attempt, err))
			if isRetryable && attempt < 3 {
				continue
			}
			return nil, err
		}

		rawItems = items
		break
	}

	events := []entraEvent{}
	for _, item := range rawItems {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			a.conf.ClientOptions.DebugLog("error parsing item JSON")
			continue
		}
		id, ok := itemMap["id"].(string)
		if !ok {
			a.conf.ClientOptions.DebugLog("error parsing ID from item JSON")
			continue
		}
		ts, ok := itemMap[tsField].(string)
		if !ok {
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("error parsing %s from item JSON", tsField))
			continue
		}
		tsParsed, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("error parsing %s value %q: %v", tsField, ts, err))
			continue
		}

		events = append(events, entraEvent{payload: itemMap, id: id, ts: tsParsed})
	}

	return events, nil
}

// fetchAllPages GETs a Graph collection URL and follows @odata.nextLink
// continuations, aggregating every "value" item. The second return reports
// the error, the third whether it is worth retrying the poll.
func (a *EntraIDAdapter) fetchAllPages(requestUrl string, authToken string) ([]interface{}, error, bool) {
	allItems := []interface{}{}

	for page := 0; page < maxPagesPerPoll && requestUrl != ""; page++ {
		req, err := http.NewRequest("GET", requestUrl, nil)
		if err != nil {
			return nil, err, false
		}

		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", authToken))
		req.Header.Set("Content-Type", "application/json")

		resp, err := a.httpClient.Do(req)
		if err != nil {
			return nil, err, true
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err, true
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("error response from Microsoft API, be sure to verify permissions and Microsoft API status: %s", body), true
		}

		var data map[string]interface{}
		if err := json.Unmarshal(body, &data); err != nil {
			return nil, fmt.Errorf("error parsing JSON: %v", err), false
		}
		items, _ := data["value"].([]interface{})
		allItems = append(allItems, items...)

		nextLink, _ := data["@odata.nextLink"].(string)
		requestUrl = nextLink
	}

	if requestUrl != "" {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf("page cap (%d) reached before draining the result set, remainder deferred to the next poll", maxPagesPerPoll))
	}

	return allItems, nil, false
}
