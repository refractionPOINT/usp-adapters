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
	// next poll through the timestamp cursor.
	maxPagesPerPoll = 10
)

// entraStream describes one Microsoft Graph collection the adapter can poll.
// Each collection filters and cursors on a different timestamp field.
type entraStream struct {
	name    string
	path    string
	tsField string
}

// Streams supported through EntraIDConfig.Streams. sign_ins requires the
// tenant to hold an Entra ID P1/P2 license (a Microsoft Graph requirement);
// sign_ins and audit_logs both require the AuditLog.Read.All application
// permission, risk_detections requires IdentityRiskEvent.Read.All.
var entraStreamsByName = map[string]entraStream{
	"risk_detections": {name: "risk_detections", path: "/v1.0/identityProtection/riskDetections", tsField: "activityDateTime"},
	"sign_ins":        {name: "sign_ins", path: "/v1.0/auditLogs/signIns", tsField: "createdDateTime"},
	"audit_logs":      {name: "audit_logs", path: "/v1.0/auditLogs/directoryAudits", tsField: "activityDateTime"},
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

	client := &http.Client{}
	resp, err := client.Do(req)
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

// fetchEvents polls one Graph collection on the poll interval, advancing an
// inclusive timestamp cursor. Because the $filter is "ge", items sitting
// exactly on the cursor are refetched on the next poll: shippedAtCursor holds
// the IDs already shipped at the cursor timestamp so they ship exactly once
// even when many events share the same timestamp.
func (a *EntraIDAdapter) fetchEvents(stream entraStream) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", stream.name))

	eventsUrl := a.conf.graphEndpoint() + stream.path
	since := time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")
	shippedAtCursor := map[string]struct{}{}

	for !a.doStop.WaitFor(a.pollInterval) {
		// The makeOneListRequest function handles error
		// handling and fatal error handling.
		items, newSince, newShipped, _ := a.makeOneListRequest(eventsUrl, stream.tsField, since, shippedAtCursor)
		since = newSince
		shippedAtCursor = newShipped
		if items == nil {
			continue
		}

		for _, item := range items {
			msg := &protocol.DataMessage{
				JsonPayload: item,
				TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
			}
			if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
				if err == uspclient.ErrorBufferFull {
					a.conf.ClientOptions.OnWarning("stream falling behind")
					err = a.uspClient.Ship(msg, 1*time.Hour)
				}
				if err == nil {
					continue
				}
				a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				a.doStop.Set()
				return
			}
		}
	}
}

// makeOneListRequest performs one poll of a Graph collection: it requests
// every item whose tsField is at or after since (following @odata.nextLink
// continuations up to maxPagesPerPoll), drops the items already shipped at the
// cursor and returns the new items along with the advanced cursor and the set
// of IDs shipped at it.
func (a *EntraIDAdapter) makeOneListRequest(eventsUrl string, tsField string, since string, shippedAtCursor map[string]struct{}) ([]map[string]interface{}, string, map[string]struct{}, error) {
	var rawItems []interface{}

	// Retry up to 3 times
	for attempt := 1; attempt <= 3; attempt++ {
		// Request everything at or after the cursor. "ge" is inclusive as
		// OData defines it, which is what lets the cursor make progress
		// without missing same-timestamp events.
		requestUrl := eventsUrl + "?%24filter=" + url.QueryEscape(fmt.Sprintf("%s ge %s", tsField, since))

		authToken, err := a.fetchToken()
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error fetching token: %s", err))
			return nil, since, shippedAtCursor, err
		}

		items, err, isRetryable := a.fetchAllPages(requestUrl, authToken)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error fetching %s (attempt %d): %s", eventsUrl, attempt, err))
			if isRetryable && attempt < 3 {
				continue
			}
			return nil, since, shippedAtCursor, err
		}

		rawItems = items
		break
	}

	// Advance the cursor to the latest timestamp seen and rebuild the set of
	// IDs shipped at it; everything strictly before the new cursor can never
	// be refetched so it does not need remembering.
	toShip := []map[string]interface{}{}
	newSince := since
	newSinceParsed, _ := time.Parse(time.RFC3339, since)
	shippedAtNewCursor := map[string]struct{}{}
	seenThisPoll := map[string]struct{}{}

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

		if _, dup := seenThisPoll[id]; dup {
			continue
		}
		seenThisPoll[id] = struct{}{}

		shipped := false
		if _, already := shippedAtCursor[id]; !already {
			toShip = append(toShip, itemMap)
			shipped = true
		}

		if tsParsed.After(newSinceParsed) {
			newSinceParsed = tsParsed
			newSince = ts
			shippedAtNewCursor = map[string]struct{}{}
		}
		if tsParsed.Equal(newSinceParsed) && shipped {
			shippedAtNewCursor[id] = struct{}{}
		}
	}

	// When the cursor did not move, previously shipped IDs are still sitting
	// on it and must stay remembered.
	if newSince == since {
		for id := range shippedAtCursor {
			shippedAtNewCursor[id] = struct{}{}
		}
	}

	return toShip, newSince, shippedAtNewCursor, nil
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

	return allItems, nil, false
}
