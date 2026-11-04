package usp_symantec

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
	defaultURL = "https://api.sep.securitycloud.symantec.com"

	tokenPath         = "/v1/oauth2/tokens"
	eventSearchPath   = "/v1/event-search"
	incidentsPath     = "/v1/incidents"
	incidentEventPath = "/v1/incidents/events"

	// Collectable feeds.
	feedEventSearch    = "event_search"
	feedIncidents      = "incidents"
	feedIncidentEvents = "incident_events"

	// Symantec only accepts SAEP here, it identifies SES events.
	productSAEP = "SAEP"

	// Symantec's ISO 8601 form: yyyy-MM-dd'T'HH:mm:ss.SSSZ.
	sesTimeFormat = "2006-01-02T15:04:05.000Z"

	// Bearer tokens last an hour; renew early so a cycle never straddles an
	// expiry.
	tokenLifetime       = time.Hour
	tokenRenewMargin    = 5 * time.Minute
	defaultPageLimit    = 100
	maxPageLimit        = 2000
	maxPagesPerCycle    = 50
	overlapPeriod       = 30 * time.Second
	maxIncidentLookback = 30 * 24 * time.Hour

	defaultPollIntervalSec = 60

	maxAttempts     = 3
	baseRetryPeriod = 5 * time.Second
)

type SymantecConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// The "OAuth Credentials" blob copied from Integration > Client
	// Applications > Client Secret. Preferred over client_id/client_secret.
	OAuthCredentials string `json:"oauth_credentials" yaml:"oauth_credentials"`
	// Alternative to oauth_credentials; they are combined and encoded here.
	ClientID     string `json:"client_id" yaml:"client_id"`
	ClientSecret string `json:"client_secret" yaml:"client_secret"`

	// Base URL of the SES API, defaults to the commercial cloud.
	URL string `json:"url" yaml:"url"`
	// Comma separated list of event_search, incidents and incident_events.
	EventTypes string `json:"event_types" yaml:"event_types"`
	// Event Search feature filter, defaults to All.
	FeatureName string `json:"feature_name" yaml:"feature_name"`
	// Optional Lucene filter applied to the feeds that accept one.
	Query string `json:"query" yaml:"query"`
	// Fold the events of an incident into the incident record.
	IncludeEvents bool `json:"include_events" yaml:"include_events"`
	// RFC3339 timestamp to start collecting from, defaults to now.
	StartTime string `json:"start_time" yaml:"start_time"`
	// Records per request, defaults to 100 and is capped at 2000.
	PageLimit int `json:"page_limit" yaml:"page_limit"`
	// Seconds between polling cycles, defaults to 60.
	PollIntervalSec uint64 `json:"poll_interval_sec" yaml:"poll_interval_sec"`
}

// feed is one collectable data set, polled by its own goroutine.
type feed struct {
	name string
	path string
}

type SymantecAdapter struct {
	conf       SymantecConfig
	uspClient  *uspclient.Client
	httpClient *http.Client
	feeds      []feed

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx    context.Context
	dedupe utils.Deduper

	// Bearer tokens are shared by every feed, so one refresh serves all.
	mToken      sync.Mutex
	token       string
	tokenExpiry time.Time
}

func (c *SymantecConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}

	if c.OAuthCredentials == "" && (c.ClientID == "" || c.ClientSecret == "") {
		return errors.New("missing oauth_credentials, or client_id and client_secret")
	}

	if c.URL == "" {
		c.URL = defaultURL
	}
	if !strings.HasPrefix(c.URL, "http://") && !strings.HasPrefix(c.URL, "https://") {
		c.URL = "https://" + c.URL
	}
	c.URL = strings.TrimSuffix(c.URL, "/")

	if c.EventTypes == "" {
		c.EventTypes = feedEventSearch
	}

	if c.FeatureName == "" {
		c.FeatureName = "All"
	}

	if c.StartTime != "" {
		if _, err := time.Parse(time.RFC3339, c.StartTime); err != nil {
			return fmt.Errorf("invalid start_time: %v", err)
		}
	}

	if c.PageLimit == 0 {
		c.PageLimit = defaultPageLimit
	}
	if c.PageLimit < 1 || c.PageLimit > maxPageLimit {
		return fmt.Errorf("invalid page_limit: %d (expected 1 to %d)", c.PageLimit, maxPageLimit)
	}

	if c.PollIntervalSec == 0 {
		c.PollIntervalSec = defaultPollIntervalSec
	}

	if _, err := c.buildFeeds(); err != nil {
		return err
	}

	return nil
}

// buildFeeds turns the configured event types into the endpoints to poll.
func (c *SymantecConfig) buildFeeds() ([]feed, error) {
	names := splitList(c.EventTypes)
	if len(names) == 0 {
		return nil, errors.New("missing event_types")
	}

	feeds := make([]feed, 0, len(names))
	seen := map[string]bool{}
	for _, name := range names {
		name = strings.ToLower(name)
		if seen[name] {
			continue
		}
		seen[name] = true

		var path string
		switch name {
		case feedEventSearch:
			path = eventSearchPath
		case feedIncidents:
			path = incidentsPath
		case feedIncidentEvents:
			path = incidentEventPath
		default:
			return nil, fmt.Errorf("invalid event_types value: %s (expected %s, %s or %s)", name, feedEventSearch, feedIncidents, feedIncidentEvents)
		}
		feeds = append(feeds, feed{name: name, path: path})
	}

	return feeds, nil
}

func splitList(v string) []string {
	out := []string{}
	for _, e := range strings.Split(v, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		out = append(out, e)
	}
	return out
}

func NewSymantecAdapter(ctx context.Context, conf SymantecConfig) (*SymantecAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	feeds, err := conf.buildFeeds()
	if err != nil {
		return nil, nil, err
	}

	a := &SymantecAdapter{
		conf:   conf,
		ctx:    ctx,
		doStop: utils.NewEvent(),
		feeds:  feeds,
	}

	if a.dedupe, err = utils.NewLocalDeduper(1*time.Minute, 1*time.Hour); err != nil {
		return nil, nil, err
	}

	if a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions); err != nil {
		a.dedupe.Close()
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

	// Fail fast on bad credentials rather than looping on 401s.
	if _, err := a.getToken(); err != nil {
		a.dedupe.Close()
		a.uspClient.Close()
		return nil, nil, fmt.Errorf("authenticating with symantec: %v", err)
	}

	a.chStopped = make(chan struct{})

	for _, f := range a.feeds {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting to fetch %s from %s", f.name, f.path))
		a.wgSenders.Add(1)
		go a.fetchEvents(f)
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *SymantecAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	a.dedupe.Close()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()
	a.httpClient.CloseIdleConnections()

	if err1 != nil {
		return err1
	}

	return err2
}

// authHeader renders the credential the token endpoint expects. The console
// hands out a pre-encoded blob; client_id and client_secret are encoded here.
func (c *SymantecConfig) authHeader() string {
	if c.OAuthCredentials != "" {
		v := strings.TrimSpace(c.OAuthCredentials)
		// The console value sometimes already carries the scheme.
		if strings.HasPrefix(v, "Basic ") || strings.HasPrefix(v, "Bearer ") {
			return v
		}
		return "Basic " + v
	}
	return "Basic " + basicEncode(c.ClientID, c.ClientSecret)
}

// getToken returns a valid bearer token, renewing it when it is close to
// expiring. Every feed shares the one token.
func (a *SymantecAdapter) getToken() (string, error) {
	a.mToken.Lock()
	defer a.mToken.Unlock()

	if a.token != "" && time.Now().Before(a.tokenExpiry.Add(-tokenRenewMargin)) {
		return a.token, nil
	}

	req, err := http.NewRequestWithContext(a.ctx, http.MethodPost, a.conf.URL+tokenPath, strings.NewReader("grant_type=client_credentials"))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", a.conf.authHeader())
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token endpoint non-200 (%s): %s", resp.Status, string(body))
	}

	var parsed struct {
		AccessToken string      `json:"access_token"`
		TokenType   string      `json:"token_type"`
		ExpiresIn   json.Number `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("token endpoint invalid json: %v", err)
	}
	if parsed.AccessToken == "" {
		return "", errors.New("token endpoint returned no access_token")
	}

	lifetime := tokenLifetime
	if secs, err := parsed.ExpiresIn.Int64(); err == nil && secs > 0 {
		lifetime = time.Duration(secs) * time.Second
	}

	a.token = parsed.AccessToken
	a.tokenExpiry = time.Now().Add(lifetime)
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("acquired bearer token, valid for %s", lifetime))

	return a.token, nil
}

// invalidateToken forces the next call to fetch a fresh token, used when the
// API rejects the one in hand.
func (a *SymantecAdapter) invalidateToken() {
	a.mToken.Lock()
	defer a.mToken.Unlock()
	a.token = ""
}

func (a *SymantecAdapter) fetchEvents(f feed) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s exiting", f.name))

	since := time.Now().UTC().Add(-overlapPeriod)
	if a.conf.StartTime != "" {
		if t, err := time.Parse(time.RFC3339, a.conf.StartTime); err == nil {
			since = t.UTC()
		}
	}

	pollInterval := time.Duration(a.conf.PollIntervalSec) * time.Second

	isFirstRun := true
	for isFirstRun || !a.doStop.WaitFor(pollInterval) {
		isFirstRun = false

		newest, nFetched := a.fetchWindow(f, since)
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetched %d records for %s", nFetched, f.name))

		if a.doStop.IsSet() {
			return
		}
		if newest.After(since) {
			since = newest.Add(-overlapPeriod)
		}
	}
}

// fetchWindow pages through one feed for everything between the watermark and
// now, returning the newest record time it saw.
func (a *SymantecAdapter) fetchWindow(f feed, since time.Time) (time.Time, int) {
	now := time.Now().UTC()

	// Incidents and their events reject a start date older than 30 days.
	if f.name != feedEventSearch {
		if oldest := now.Add(-maxIncidentLookback); since.Before(oldest) {
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("%s: clamping start to the 30 day limit", f.name))
			since = oldest
		}
	}

	newest := since
	nFetched := 0
	next := 0

	for page := 0; page < maxPagesPerCycle && !a.doStop.IsSet(); page++ {
		items, nextIndex, err := a.makeOneRequest(f, since, now, next)
		if err != nil {
			break
		}
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			ts := recordTime(item, now)
			if ts.After(newest) {
				newest = ts
			}
			if a.isDuplicate(f, item) {
				continue
			}
			if !a.ship(f, item, ts) {
				return newest, nFetched
			}
			nFetched++
		}
		// The API signals more pages by echoing a non-zero next index.
		if nextIndex <= 0 || nextIndex == next {
			break
		}
		next = nextIndex
	}

	return newest, nFetched
}

// requestBody builds the JSON payload each feed expects. Symantec takes the
// time window in the body rather than the query string.
func (a *SymantecAdapter) requestBody(f feed, since time.Time, until time.Time, next int) utils.Dict {
	body := utils.Dict{
		"start_date": since.UTC().Format(sesTimeFormat),
		"end_date":   until.UTC().Format(sesTimeFormat),
		"limit":      a.conf.PageLimit,
	}
	if next > 0 {
		body["next"] = next
	}

	switch f.name {
	case feedEventSearch:
		body["product"] = productSAEP
		body["feature_name"] = a.conf.FeatureName
		// The field is required, an empty filter matches everything.
		body["query"] = a.conf.Query
	case feedIncidents:
		body["include_events"] = a.conf.IncludeEvents
		if a.conf.Query != "" {
			body["query"] = a.conf.Query
		}
	case feedIncidentEvents:
		body["incident_associate"] = true
	}

	return body
}

func (a *SymantecAdapter) ship(f feed, item utils.Dict, ts time.Time) bool {
	msg := &protocol.DataMessage{
		EventType:   f.name,
		JsonPayload: item,
		TimestampMs: uint64(ts.UnixMilli()),
	}
	if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			a.doStop.Set()
			return false
		}
	}
	return true
}

func (a *SymantecAdapter) isDuplicate(f feed, item utils.Dict) bool {
	id := recordID(item)
	if id == "" {
		return false
	}
	return a.dedupe.CheckAndAdd(fmt.Sprintf("%s/%s", f.name, id))
}

// makeOneRequest issues a single page request, retrying what is worth retrying
// and re-authenticating once if the token was rejected.
func (a *SymantecAdapter) makeOneRequest(f feed, since time.Time, until time.Time, next int) ([]utils.Dict, int, error) {
	var lastErr error
	didReauth := false

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		items, nextIndex, retryIn, err := a.doRequest(f, since, until, next)
		if err == nil {
			return items, nextIndex, nil
		}
		lastErr = err

		if errors.Is(err, errUnauthorized) && !didReauth {
			// The token may have been revoked or expired early.
			didReauth = true
			a.invalidateToken()
			continue
		}
		if retryIn <= 0 {
			break
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("retrying %s in %v: %v", f.name, retryIn, err))
		if a.doStop.WaitFor(retryIn * time.Duration(attempt)) {
			break
		}
	}

	a.conf.ClientOptions.OnError(fmt.Errorf("symantec api: %v", lastErr))
	return nil, 0, lastErr
}

var errUnauthorized = errors.New("unauthorized")

// doRequest performs one HTTP request. A returned retryIn greater than zero
// tells the caller the failure is worth retrying after that delay.
func (a *SymantecAdapter) doRequest(f feed, since time.Time, until time.Time, next int) ([]utils.Dict, int, time.Duration, error) {
	token, err := a.getToken()
	if err != nil {
		return nil, 0, baseRetryPeriod, err
	}

	payload, err := json.Marshal(a.requestBody(f, since, until, next))
	if err != nil {
		return nil, 0, 0, err
	}

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting %s%s %s", a.conf.URL, f.path, string(payload)))

	req, err := http.NewRequestWithContext(a.ctx, http.MethodPost, a.conf.URL+f.path, strings.NewReader(string(payload)))
	if err != nil {
		return nil, 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, 0, baseRetryPeriod, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, baseRetryPeriod, err
	}

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("non-200 (%s) from %s: %s", resp.Status, f.path, string(body))
		if resp.StatusCode == http.StatusUnauthorized {
			return nil, 0, 0, fmt.Errorf("%w: %v", errUnauthorized, err)
		}
		// Quotas are enforced per hour, so back off rather than hammering.
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return nil, 0, retryAfter(resp.Header.Get("Retry-After")), err
		}
		return nil, 0, 0, err
	}

	items, nextIndex, err := parseResponse(body)
	if err != nil {
		return nil, 0, 0, err
	}

	return items, nextIndex, 0, nil
}

// parseResponse pulls the records and the pagination index out of a response.
// The envelope field differs per endpoint, so look for whichever is present.
func parseResponse(body []byte) ([]utils.Dict, int, error) {
	var envelope map[string]interface{}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, 0, fmt.Errorf("invalid json: %v", err)
	}

	var items []utils.Dict
	for _, key := range []string{"events", "incidents", "result", "data"} {
		raw, ok := envelope[key]
		if !ok {
			continue
		}
		list, ok := raw.([]interface{})
		if !ok {
			continue
		}
		for _, e := range list {
			if d, ok := e.(map[string]interface{}); ok {
				items = append(items, utils.Dict(d))
			}
		}
		break
	}

	next := 0
	if v, ok := envelope["next"]; ok {
		next = asInt(v)
	}

	return items, next, nil
}

func asInt(v interface{}) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case string:
		n, err := strconv.Atoi(t)
		if err != nil {
			return 0
		}
		return n
	case json.Number:
		n, err := t.Int64()
		if err != nil {
			return 0
		}
		return int(n)
	}
	return 0
}

// recordID finds something stable to dedupe on. Event Search and the incident
// APIs each name their identifier differently.
func recordID(item utils.Dict) string {
	for _, key := range []string{"uuid", "event_id", "incident_uid", "atp_incident_id", "id"} {
		if v, ok := item[key]; ok {
			if s := idKey(v); s != "" {
				return s
			}
		}
	}
	return ""
}

// recordTime finds the record's own timestamp, falling back to now so an
// unrecognised shape is still shipped with a sane time.
func recordTime(item utils.Dict, fallback time.Time) time.Time {
	for _, key := range []string{"time", "device_time", "event_time", "created", "modified"} {
		v, ok := item[key]
		if !ok {
			continue
		}
		if t, ok := parseTime(v); ok {
			return t
		}
	}
	return fallback
}

func parseTime(v interface{}) (time.Time, bool) {
	switch t := v.(type) {
	case string:
		for _, layout := range []string{sesTimeFormat, time.RFC3339Nano, time.RFC3339} {
			if parsed, err := time.Parse(layout, t); err == nil {
				return parsed.UTC(), true
			}
		}
	case float64:
		// Epoch, in seconds or milliseconds depending on the field.
		if t > 1e11 {
			return time.UnixMilli(int64(t)).UTC(), true
		}
		if t > 0 {
			return time.Unix(int64(t), 0).UTC(), true
		}
	}
	return time.Time{}, false
}

func retryAfter(header string) time.Duration {
	if header == "" {
		return baseRetryPeriod
	}
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return baseRetryPeriod
}

// basicEncode builds the HTTP Basic credential from a client ID and secret.
func basicEncode(clientID string, clientSecret string) string {
	return base64.StdEncoding.EncodeToString([]byte(clientID + ":" + clientSecret))
}

// idKey renders an identifier as a stable string without altering the payload.
func idKey(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case json.Number:
		return t.String()
	default:
		return fmt.Sprintf("%v", t)
	}
}
