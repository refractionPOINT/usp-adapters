package usp_sentinelone

import (
	"context"
	"errors"
	"fmt"
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

const (
	maxRetryAttempts = 3
	baseRetryDelay   = 5 * time.Second
	maxRetryDelay    = 30 * time.Second

	agentsEndpoint            = "/web/api/v2.1/agents"
	agentsEventType           = "agents"
	agentsPageLimit           = "1000"
	defaultAgentsPollInterval = 15 * time.Minute

	// s1TimeFormat is the timestamp layout the Management API emits and
	// accepts in createdAt/updatedAt style fields and filters.
	s1TimeFormat = "2006-01-02T15:04:05.999999Z"
)

// isTransientError determines if an error is transient and should be retried.
// Transient errors include:
// - HTTP 5xx server errors (500, 502, 503, 504)
// - HTTP 429 Too Many Requests (rate limiting)
// - Network errors (timeouts, connection refused, etc.)
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	// Check for HTTPError type which contains the status code directly
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		// 5xx errors are transient server errors
		if httpErr.StatusCode >= 500 && httpErr.StatusCode <= 599 {
			return true
		}
		// 429 is rate limiting - transient
		if httpErr.StatusCode == http.StatusTooManyRequests {
			return true
		}
		// 4xx errors (except 429) are permanent - bad request, auth failure, etc.
		return false
	}

	// Network-related errors are typically transient
	errStr := err.Error()
	if strings.Contains(errStr, "failed to execute request") {
		// This could be timeout, connection refused, DNS failure, etc.
		return true
	}

	// Context cancellation is not transient (intentional shutdown)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	return false
}

// uspSink is the subset of *uspclient.Client the adapter depends on.
// Expressing it as an interface lets tests substitute an in-memory sink for
// the real LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type SentinelOneAdapter struct {
	conf       SentinelOneConfig
	uspClient  uspSink
	httpClient *http.Client
	s1Client   *SentinelOneClient
	urls       []string

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type SentinelOneConfig struct {
	ClientOptions       uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Domain              string                  `json:"domain" yaml:"domain"`
	APIKey              string                  `json:"api_key" yaml:"api_key"`
	URLs                string                  `json:"urls" yaml:"urls"`
	StartTime           string                  `json:"start_time" yaml:"start_time"`
	TimeBetweenRequests time.Duration           `json:"time_between_requests" yaml:"time_between_requests"`
	RetryBaseDelay      time.Duration           `json:"retry_base_delay" yaml:"retry_base_delay"`
	MaxRetryDelay       time.Duration           `json:"max_retry_delay" yaml:"max_retry_delay"`
	MaxRetryAttempts    int                     `json:"max_retry_attempts" yaml:"max_retry_attempts"`

	// SiteIDs / AccountIDs scope every API request to the given SentinelOne
	// sites / accounts (comma-separated numeric ids, sent as the standard
	// siteIds / accountIds query filters). Use them with a console-wide or
	// MSP/partner token to pull in a single tenant instead of every tenant
	// the token can see. Empty means no scoping (current behavior).
	SiteIDs    string `json:"site_ids" yaml:"site_ids"`
	AccountIDs string `json:"account_ids" yaml:"account_ids"`

	// CollectAgents, when true, also polls the agent (endpoint) inventory and
	// ships one "agents" record per agent, re-shipping a record whenever its
	// updatedAt changes. Combined with the site/account scoping above this
	// pulls a tenant's endpoints into LimaCharlie as individual sensors even
	// before they produce any threat/alert/activity telemetry.
	// Decommissioned agents are excluded so historical registrations do not
	// create dead sensors.
	CollectAgents      bool          `json:"collect_agents" yaml:"collect_agents"`
	AgentsPollInterval time.Duration `json:"agents_poll_interval" yaml:"agents_poll_interval"`
}

func (c *SentinelOneConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Domain == "" {
		return errors.New("missing domain")
	}
	if c.APIKey == "" {
		return errors.New("missing api_key")
	}
	if !strings.HasPrefix(c.Domain, "https://") {
		c.Domain = "https://" + c.Domain
	}
	c.Domain = strings.TrimSuffix(c.Domain, "/")
	if _, err := time.Parse(s1TimeFormat, c.StartTime); c.StartTime != "" && err != nil {
		return fmt.Errorf("invalid start_time: %v", err)
	}
	c.applyDefaults()
	return nil
}

// applyDefaults fills unset knobs and normalizes inputs. It runs both from
// Validate() and from the constructor, because the general adapter runner
// constructs the adapter without calling Validate().
func (c *SentinelOneConfig) applyDefaults() {
	if c.TimeBetweenRequests == 0 {
		c.TimeBetweenRequests = 1 * time.Minute
	}
	if c.RetryBaseDelay == 0 {
		c.RetryBaseDelay = baseRetryDelay
	}
	if c.MaxRetryDelay == 0 {
		c.MaxRetryDelay = maxRetryDelay
	}
	if c.MaxRetryAttempts == 0 {
		c.MaxRetryAttempts = maxRetryAttempts
	}
	c.SiteIDs = normalizeCSV(c.SiteIDs)
	c.AccountIDs = normalizeCSV(c.AccountIDs)
	if c.AgentsPollInterval == 0 {
		c.AgentsPollInterval = defaultAgentsPollInterval
	}
}

// normalizeCSV trims whitespace around a comma-separated list's elements and
// drops empty ones, so " 123, 456 " serializes as the "123,456" the API
// expects.
func normalizeCSV(s string) string {
	if s == "" {
		return ""
	}
	parts := []string{}
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, ",")
}

func NewSentinelOneAdapter(ctx context.Context, conf SentinelOneConfig) (*SentinelOneAdapter, chan struct{}, error) {
	return newSentinelOneAdapter(ctx, conf, nil)
}

// newSentinelOneAdapter is the implementation behind NewSentinelOneAdapter.
// When sink is non-nil it is used in place of a real LimaCharlie client -- the
// seam tests use to capture shipped events.
func newSentinelOneAdapter(ctx context.Context, conf SentinelOneConfig, sink uspSink) (*SentinelOneAdapter, chan struct{}, error) {
	// Ensure defaults are set (these may not be set if Validate() wasn't called,
	// e.g. when launched through the general adapter runner).
	conf.applyDefaults()

	a := &SentinelOneAdapter{
		conf:     conf,
		ctx:      ctx,
		doStop:   utils.NewEvent(),
		s1Client: NewSentinelOneClient(conf.Domain, conf.APIKey),
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

	// Set sane default for the content types.
	if a.conf.URLs == "" {
		a.urls = []string{
			"/web/api/v2.1/activities",
			"/web/api/v2.1/cloud-detection/alerts",
			"/web/api/v2.1/threats",
		}
	} else {
		for _, s := range strings.Split(a.conf.URLs, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				a.urls = append(a.urls, s)
			}
		}
	}
	for i, url := range a.urls {
		if !strings.HasPrefix(url, "/") {
			url = "/" + url
		}
		a.urls[i] = url
	}

	nCollecting := 0
	for _, ct := range a.urls {
		if ct == "" {
			continue
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting to fetch %s events", ct))

		nCollecting++

		a.wgSenders.Add(1)
		go a.fetchEvents(ct)
	}

	if a.conf.CollectAgents {
		a.conf.ClientOptions.DebugLog("starting to fetch the agents inventory")
		nCollecting++
		a.wgSenders.Add(1)
		go a.fetchAgents()
	}

	if nCollecting == 0 {
		a.Close()
		return nil, nil, errors.New("no content types specified")
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *SentinelOneAdapter) Close() error {
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

func (a *SentinelOneAdapter) fetchEvents(endpoint string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("fetching of events exiting")

	// Make the last component of the endpoint the event type.
	ets := strings.Split(endpoint, "/")
	eventType := ets[len(ets)-1]
	isFirstRun := true
	lastCreatedAt := ""
	isDataFound := false
	for isFirstRun || isDataFound || !a.doStop.WaitFor(a.conf.TimeBetweenRequests) {
		isDataFound = false
		qValues := url.Values{}
		now := time.Now().UTC()
		if isFirstRun {
			start := a.conf.StartTime
			if start == "" {
				start = now.Add(-15 * time.Second).Format(s1TimeFormat)
			}
			lastCreatedAt = start
			end := now.Format(s1TimeFormat)
			qValues.Set("createdAt__gte", start)
			qValues.Set("createdAt__lte", end)
		} else {
			qValues.Set("createdAt__gt", lastCreatedAt)
			qValues.Set("createdAt__lte", now.Format(s1TimeFormat))
		}
		isFirstRun = false
		nextPage := ""
		nFetched := 0
		for !a.doStop.IsSet() {
			if nextPage != "" {
				qValues.Set("cursor", nextPage)
			}

			resp, keepRunning := a.getWithRetry(endpoint, qValues)
			if !keepRunning {
				return
			}
			if resp == nil {
				// Retries exhausted; break out of the pagination loop to try
				// again after TimeBetweenRequests.
				break
			}
			nextPage = resp.NextPageCursor()
			for _, event := range resp.Data {
				isDataFound = true
				nFetched++

				// The creeatedAt field varies per data source, but it's always either at the root
				// or one level deep. So look for both.
				lastCreatedAt, _ = getTimestampElement(event, "createdAt")
				var ts uint64
				if lastCreatedAt != "" {
					if it, err := time.Parse(s1TimeFormat, lastCreatedAt); err == nil {
						ts = uint64(it.UnixMilli())
					} else {
						lastCreatedAt = ""
					}
				}
				if lastCreatedAt == "" {
					a.conf.ClientOptions.OnError(fmt.Errorf("createdAt not found in event: %v", event))
					now = time.Now()
					lastCreatedAt = now.Format(s1TimeFormat)
					ts = uint64(now.UnixMilli())
				}

				if !a.ship(&protocol.DataMessage{
					EventType:   eventType,
					JsonPayload: event,
					TimestampMs: ts,
				}) {
					break
				}
			}
			if nextPage == "" {
				break
			}
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetched %d events", nFetched))
	}
}

// applyScope restricts a request to the configured sites/accounts. Every
// Management API list endpoint accepts the same siteIds/accountIds filters.
// It is called from getWithRetry — the single chokepoint every API request
// goes through — so a future feed cannot forget the tenant scoping.
func (a *SentinelOneAdapter) applyScope(qValues url.Values) {
	if a.conf.SiteIDs != "" {
		qValues.Set("siteIds", a.conf.SiteIDs)
	}
	if a.conf.AccountIDs != "" {
		qValues.Set("accountIds", a.conf.AccountIDs)
	}
}

// ship forwards one record to LimaCharlie, absorbing backpressure. It returns
// false when delivery failed for good; the adapter is stopping at that point.
func (a *SentinelOneAdapter) ship(msg *protocol.DataMessage) bool {
	err := a.uspClient.Ship(msg, 10*time.Second)
	if err == uspclient.ErrorBufferFull {
		if a.conf.ClientOptions.OnWarning != nil {
			a.conf.ClientOptions.OnWarning("stream falling behind")
		}
		err = a.uspClient.Ship(msg, 1*time.Hour)
	}
	if err != nil {
		if a.conf.ClientOptions.OnError != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
		}
		a.doStop.Set()
		return false
	}
	return true
}

// getWithRetry fetches one page, retrying transient errors with exponential
// backoff. The bool result is false when the calling goroutine should exit:
// the adapter is stopping, or a permanent error (auth failure, bad request)
// stopped it. A (nil, true) result means the retries were exhausted and the
// caller should abandon the current poll and try again next interval.
func (a *SentinelOneAdapter) getWithRetry(endpoint string, qValues url.Values) (*SentinelOnePagedData, bool) {
	a.applyScope(qValues)
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching from %s?%s", endpoint, qValues.Encode()))

	var resp *SentinelOnePagedData
	var err error
	for attempt := 0; attempt < a.conf.MaxRetryAttempts; attempt++ {
		if a.doStop.IsSet() {
			return nil, false
		}

		resp, err = a.s1Client.GetFromAPI(a.ctx, endpoint, qValues)
		if err == nil {
			return resp, true
		}

		// Check if this is a transient error worth retrying
		if !isTransientError(err) {
			// Permanent error (auth failure, bad request, etc.) - stop the adapter
			if a.conf.ClientOptions.OnError != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("GetFromAPI(): %v", err))
			}
			a.doStop.Set()
			return nil, false
		}

		// Transient error - log and retry with exponential backoff
		// Only retry if we haven't exhausted all attempts
		if attempt+1 >= a.conf.MaxRetryAttempts {
			break // Exit retry loop, will be handled below
		}

		// Exponential backoff: baseDelay * 2^attempt, capped at MaxRetryDelay
		retryDelay := a.conf.RetryBaseDelay * time.Duration(1<<attempt)
		if retryDelay > a.conf.MaxRetryDelay {
			retryDelay = a.conf.MaxRetryDelay
		}
		if a.conf.ClientOptions.OnWarning != nil {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf(
				"transient error (attempt %d/%d), waiting %v before retry: %v",
				attempt+1, a.conf.MaxRetryAttempts, retryDelay, err))
		}

		// Wait before retry, but respect doStop
		if a.doStop.WaitFor(retryDelay) {
			return nil, false
		}
	}

	// Retries exhausted; report the error but let the caller try again on the
	// next interval (don't stop the entire adapter for persistent transient
	// errors).
	if a.conf.ClientOptions.OnError != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf(
			"GetFromAPI(): failed after %d attempts: %v", a.conf.MaxRetryAttempts, err))
	}
	return nil, true
}

// fetchAgents polls the agent (endpoint) inventory and ships one "agents"
// record per agent, re-shipping a record whenever its updatedAt changes. The
// first poll walks the full inventory, which is what imports a tenant's
// existing endpoints into LimaCharlie as individual sensors; later polls only
// ship agents that changed. Decommissioned agents are filtered out so
// historical registrations don't materialize as dead sensors.
func (a *SentinelOneAdapter) fetchAgents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("fetching of agents inventory exiting")

	// Last shipped updatedAt per agent, to only ship changes. Replaced by the
	// walk's own map after every completed walk so agents that left the
	// inventory (deleted, decommissioned, moved out of scope) don't accumulate
	// forever.
	shippedAgents := map[string]string{}

	isFirstRun := true
	for isFirstRun || !a.doStop.WaitFor(a.conf.AgentsPollInterval) {
		isFirstRun = false
		nFetched := 0
		nShipped := 0
		seenAgents := make(map[string]string, len(shippedAgents))
		completedWalk := false

		qValues := url.Values{}
		qValues.Set("limit", agentsPageLimit)
		qValues.Set("isDecommissioned", "false")
		for !a.doStop.IsSet() {
			resp, keepRunning := a.getWithRetry(agentsEndpoint, qValues)
			if !keepRunning {
				return
			}
			if resp == nil {
				// Retries exhausted; abandon this poll, try again next interval.
				break
			}

			for _, agent := range resp.Data {
				nFetched++
				// The numeric id is the stable identity (same namespace the
				// threats/alerts/activities events carry); fall back to the
				// agent uuid so a missing id doesn't degrade to re-shipping
				// the record every poll.
				agentID, _ := agent["id"].(string)
				if agentID == "" {
					agentID, _ = agent["uuid"].(string)
				}
				updatedAt, _ := agent["updatedAt"].(string)
				if agentID != "" {
					seenAgents[agentID] = updatedAt
					// Distinguish "never shipped" from "shipped with this
					// exact updatedAt": a missing updatedAt must not read as
					// already-shipped just because both are "".
					if prev, ok := shippedAgents[agentID]; ok && prev == updatedAt {
						continue
					}
				}

				var ts uint64
				if t, err := time.Parse(s1TimeFormat, updatedAt); err == nil {
					ts = uint64(t.UnixMilli())
				} else {
					ts = uint64(time.Now().UnixMilli())
				}

				if !a.ship(&protocol.DataMessage{
					EventType:   agentsEventType,
					JsonPayload: agent,
					TimestampMs: ts,
				}) {
					return
				}
				if agentID != "" {
					shippedAgents[agentID] = updatedAt
				}
				nShipped++
			}

			cursor := resp.NextPageCursor()
			if cursor == "" {
				completedWalk = true
				break
			}
			qValues.Set("cursor", cursor)
		}

		if completedWalk {
			// The walk covered the whole inventory: what wasn't seen is gone.
			shippedAgents = seenAgents
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("agents inventory poll complete (seen=%d shipped=%d)", nFetched, nShipped))
	}
}

func getTimestampElement(event map[string]interface{}, path string) (string, bool) {
	if lca, ok := event[path].(string); ok {
		return lca, true
	}
	for _, v := range event {
		subEvent, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		if lca, ok := subEvent[path].(string); ok {
			return lca, true
		}
	}
	return "", false
}
