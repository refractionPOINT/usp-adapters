package usp_defender

import (
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

// environment describes one Microsoft national cloud deployment. Keeping the
// three values that must move together (alerts endpoint, token host and scope)
// in a single struct makes a partial/desynced configuration impossible -- the
// alertsURL, tokenHost and scope for an environment can never disagree.
type environment struct {
	// alertsURL is the MS Graph security alerts_v2 endpoint.
	alertsURL string
	// tokenHost is the Azure AD token host; the full token endpoint is
	// <tokenHost>/<tenant_id>/oauth2/v2.0/token.
	tokenHost string
	// scope is the OAuth2 scope, which must match the MS Graph service root.
	scope string
}

// environments maps an endpoint name to its Microsoft national cloud
// deployment. gcc-gov (US Government GCC / moderate) uses the worldwide
// endpoints -- identical to enterprise -- and is kept as a named option for
// parity with the o365 adapter, which exposes the same four names.
// Reference: https://learn.microsoft.com/en-us/graph/deployments
var environments = map[string]environment{
	"enterprise": {
		alertsURL: "https://graph.microsoft.com/v1.0/security/alerts_v2",
		tokenHost: "https://login.microsoftonline.com",
		scope:     "https://graph.microsoft.com/.default",
	},
	"gcc-gov": {
		alertsURL: "https://graph.microsoft.com/v1.0/security/alerts_v2",
		tokenHost: "https://login.microsoftonline.com",
		scope:     "https://graph.microsoft.com/.default",
	},
	"gcc-high-gov": {
		alertsURL: "https://graph.microsoft.us/v1.0/security/alerts_v2",
		tokenHost: "https://login.microsoftonline.us",
		scope:     "https://graph.microsoft.us/.default",
	},
	"dod-gov": {
		alertsURL: "https://dod-graph.microsoft.us/v1.0/security/alerts_v2",
		tokenHost: "https://login.microsoftonline.us",
		scope:     "https://dod-graph.microsoft.us/.default",
	},
}

// defaultEndpoint is the environment used when Endpoint is left empty.
const defaultEndpoint = "enterprise"

const (
	// defaultTokenURLTemplate is the Azure AD token endpoint template,
	// parameterized by the token host and tenant id.
	defaultTokenURLTemplate = "%s/%s/oauth2/v2.0/token"

	// defaultPollInterval is how long the adapter waits between polls of the
	// alerts endpoint.
	defaultPollInterval = 30 * time.Second
)

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type DefenderAdapter struct {
	conf       DefenderConfig
	uspClient  uspSink
	httpClient *http.Client

	endpoint     string
	pollInterval time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type DefenderConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	TenantID      string                  `json:"tenant_id" yaml:"tenant_id"`
	ClientID      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`

	// Endpoint selects the Microsoft national cloud deployment. Valid values:
	// "enterprise" (default, global/commercial), "gcc-gov" (US Government GCC /
	// moderate), "gcc-high-gov" (US Government GCC High / L4) and "dod-gov"
	// (US Government DoD / L5). An empty value defaults to "enterprise", so
	// existing configs keep talking to the commercial cloud unchanged.
	// Reference: https://learn.microsoft.com/en-us/graph/deployments
	Endpoint string `json:"endpoint" yaml:"endpoint"`

	// TokenURL overrides the Azure AD token endpoint derived from Endpoint and
	// TenantID (e.g. https://login.microsoftonline.com/<tenant_id>/oauth2/v2.0/token
	// for the enterprise endpoint). It only overrides the token URL; the OAuth2
	// scope still follows Endpoint, so a token_url pointed at a gov host without
	// also setting endpoint sends the commercial scope and will fail auth.
	TokenURL string `json:"token_url" yaml:"token_url"`

	// AlertsURL overrides the MS Graph security alerts endpoint derived from
	// Endpoint (e.g. https://graph.microsoft.com/v1.0/security/alerts_v2 for
	// the enterprise endpoint). It only overrides the alerts URL; the OAuth2
	// scope still follows Endpoint, so an alerts_url pointed at a gov host
	// without also setting endpoint sends the commercial scope and will 401.
	AlertsURL string `json:"alerts_url" yaml:"alerts_url"`

	// PollInterval overrides how long the adapter waits between polls of the
	// alerts endpoint (default: 30s). Not exposed in json/yaml configs
	// (time.Duration does not deserialize meaningfully); it is a seam for
	// tests, like in the other polling adapters.
	PollInterval time.Duration `json:"-" yaml:"-"`
}

// endpoint returns the configured Microsoft national cloud deployment name,
// defaulting to "enterprise" when unset.
func (c *DefenderConfig) endpoint() string {
	if c.Endpoint == "" {
		return defaultEndpoint
	}
	return c.Endpoint
}

// tokenURL returns the token endpoint to use: the configured override, or the
// Azure AD token endpoint for the configured environment and tenant.
func (c *DefenderConfig) tokenURL() string {
	if c.TokenURL != "" {
		return c.TokenURL
	}
	return fmt.Sprintf(defaultTokenURLTemplate, environments[c.endpoint()].tokenHost, c.TenantID)
}

// alertsURL returns the alerts endpoint to use: the configured override, or
// the MS Graph security alerts endpoint for the configured environment.
func (c *DefenderConfig) alertsURL() string {
	if c.AlertsURL != "" {
		return c.AlertsURL
	}
	return environments[c.endpoint()].alertsURL
}

// scope returns the OAuth2 scope for the configured environment, which must
// match that environment's MS Graph service root.
func (c *DefenderConfig) scope() string {
	return environments[c.endpoint()].scope
}

func (c *DefenderConfig) Validate() error {
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
	if c.Endpoint != "" {
		if _, ok := environments[c.Endpoint]; !ok {
			return fmt.Errorf("invalid endpoint %q, must be one of: enterprise, gcc-gov, gcc-high-gov, dod-gov", c.Endpoint)
		}
	}
	return nil
}

func NewDefenderAdapter(ctx context.Context, conf DefenderConfig) (*DefenderAdapter, chan struct{}, error) {
	return newDefenderAdapter(ctx, conf, nil)
}

// newDefenderAdapter is the implementation behind NewDefenderAdapter. When
// sink is non-nil it is used in place of a real LimaCharlie client -- the seam
// tests use to capture shipped events.
func newDefenderAdapter(ctx context.Context, conf DefenderConfig, sink uspSink) (*DefenderAdapter, chan struct{}, error) {
	var err error

	// Resolve and validate the endpoint up front. Nothing on the runtime path
	// (containers/general/tool.go) calls Validate(), so guard here too: a
	// non-empty but unknown endpoint would otherwise resolve to empty URLs and
	// scope and silently poll nothing.
	if _, ok := environments[conf.endpoint()]; !ok {
		return nil, nil, fmt.Errorf("not a valid api endpoint: %s", conf.Endpoint)
	}

	a := &DefenderAdapter{
		conf:         conf,
		ctx:          context.Background(),
		doStop:       utils.NewEvent(),
		endpoint:     conf.endpoint(),
		pollInterval: conf.PollInterval,
	}
	if a.pollInterval <= 0 {
		a.pollInterval = defaultPollInterval
	}

	if sink != nil {
		a.uspClient = sink
	} else {
		a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			return nil, nil, err
		}
	}

	a.httpClient = &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
		},
	}

	a.chStopped = make(chan struct{})

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting to fetch alerts"))

	a.wgSenders.Add(1)
	go a.fetchEvents(a.conf.alertsURL())

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *DefenderAdapter) Close() error {
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

func (a *DefenderAdapter) fetchToken() (string, error) {

	tokenURL := a.conf.tokenURL()
	form := url.Values{}
	form.Set("client_id", a.conf.ClientID)
	form.Set("scope", a.conf.scope())
	form.Set("grant_type", "client_credentials")
	form.Set("client_secret", a.conf.ClientSecret)

	req, err := http.NewRequest("POST", tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("no bearer token returned: %s", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Use the adapter's httpClient (10s timeout): a bare http.Client{} has no
	// timeout, so a token host that accepts the TCP connection but never
	// responds would hang the poll goroutine forever.
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

func (a *DefenderAdapter) fetchEvents(url string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", url))

	lastEventId := ""
	// since := time.Date(2022, time.June, 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02T15:04:05.000000Z")
	since := time.Now().Format("2006-01-02T15:04:05.000000Z")

	for !a.doStop.WaitFor(a.pollInterval) {
		// The makeOneRequest function handles error
		// handling and fatal error handling.
		items, newSince, eventId, _ := a.makeOneListRequest(url, since, lastEventId)
		since = newSince
		lastEventId = eventId
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

func (a *DefenderAdapter) makeOneListRequest(eventsUrl string, since string, lastEventId string) ([]map[string]interface{}, string, string, error) {
	var alerts []map[string]interface{}
	var lastDetectionTime, eventId string

	// Retry up to 3 times
	for attempt := 1; attempt <= 3; attempt++ {
		// Create query parameters
		filter := "%24"
		query := "%20ge%20"
		date_filter := fmt.Sprintf("?%sfilter=createdDateTime%s%s", filter, query, strings.Replace(since, ":", "%3A", -1))

		// Create the full request URL with query parameters (don't modify eventsUrl to avoid corruption on retries)
		requestUrl := eventsUrl + date_filter

		authToken, err := a.fetchToken()
		if err != nil {
			// Retry if token fetch failed, but continue to the next iteration
			if attempt < 3 {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("error fetching token (attempt %d), retrying: %s", attempt, err))
				time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
				continue
			}
			// Return after 3 failed attempts
			a.conf.ClientOptions.OnError(fmt.Errorf("error fetching token after 3 attempts: %s", err))
			return nil, since, "", fmt.Errorf("error fetching token after 3 attempts: %s", err)
		}

		req, err := http.NewRequest("GET", requestUrl, nil)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Error creating request: %s\n", err))
			return nil, since, "", err
		}

		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", authToken))
		req.Header.Set("Content-Type", "application/json")

		// Use the adapter's httpClient (10s timeout) rather than a bare
		// http.Client{}, which has no timeout and would hang the poll goroutine
		// forever against an endpoint that never responds.
		resp, err := a.httpClient.Do(req)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Error making request: %s\n", err))
			return nil, since, "", err
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Error reading response: %s\n", err))
			return nil, since, "", err
		}

		if resp.StatusCode != http.StatusOK {
			// Check for retryable status codes (503, 504) - likely Microsoft infrastructure issues
			isRetryable := resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout

			if isRetryable {
				// Retry for transient infrastructure errors
				if attempt < 3 {
					a.conf.ClientOptions.OnWarning(fmt.Sprintf("Microsoft API returned %d (attempt %d), retrying: %s\n", resp.StatusCode, attempt, body))
					time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
					continue
				}
				// Final attempt for retryable error
				a.conf.ClientOptions.OnError(fmt.Errorf("Microsoft API returned %d after 3 attempts: %s\n", resp.StatusCode, body))
				return nil, since, "", fmt.Errorf("Microsoft API returned %d after 3 attempts: %s\n", resp.StatusCode, body)
			} else {
				// Non-retryable error (auth, permission, etc) - fail immediately
				a.conf.ClientOptions.OnError(fmt.Errorf("Error response from Microsoft API, be sure to verify permissions and Microsoft API status: %s\n", body))
				return nil, since, "", fmt.Errorf("Error response from Microsoft API, be sure to verify permissions and Microsoft API status: %s\n", body)
			}
		}

		// If the response is OK, parse the body and process detections
		var data map[string]interface{}
		err = json.Unmarshal(body, &data)
		detections, _ := data["value"].([]interface{})
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Error parsing JSON: %v", err))
			return nil, since, "", err
		}

		items := detections

		lastDetectionTime = since
		for _, detection := range items {
			detectMap, ok := detection.(map[string]interface{})
			if !ok {
				a.conf.ClientOptions.DebugLog("Error parsing detectMap JSON")
				continue
			}

			id, ok := detectMap["id"].(string)
			if !ok {
				a.conf.ClientOptions.DebugLog("Error parsing ID from detectMap JSON")
				continue
			}
			eventId = id

			if id != lastEventId {
				createdDateTime, ok := detectMap["createdDateTime"].(string)
				if !ok {
					a.conf.ClientOptions.DebugLog("Error parsing createdDateTime from detectMap JSON")
					continue
				}

				lastDetectionTime = createdDateTime
				alerts = append(alerts, detectMap)
			}
		}

		// Break out of the loop if successful
		break
	}

	return alerts, lastDetectionTime, eventId, nil
}
