package usp_bitwarden

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	tokenEndpointUS = "https://identity.bitwarden.com/connect/token"
	tokenEndpointEU = "https://identity.bitwarden.eu/connect/token"
	eventsBaseURLUS = "https://api.bitwarden.com"
	eventsBaseURLEU = "https://api.bitwarden.eu"
)

type BitwardenConfig struct {
	ClientOptions     uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ClientID          string                  `json:"client_id" yaml:"client_id"`
	ClientSecret      string                  `json:"client_secret" yaml:"client_secret"`
	Region            string                  `json:"region" yaml:"region"`                         // "us" or "eu", defaults to "us"
	TokenEndpointURL  string                  `json:"token_endpoint_url" yaml:"token_endpoint_url"` // Custom token endpoint URL for self-hosted instances
	EventsBaseURL     string                  `json:"events_base_url" yaml:"events_base_url"`       // Custom events base URL for self-hosted instances
	Filters           []string                `json:"filters,omitempty" yaml:"filters,omitempty"`
}

func (c *BitwardenConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}

	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}

	// Check if custom URLs are provided
	hasCustomURLs := c.TokenEndpointURL != "" || c.EventsBaseURL != ""

	if hasCustomURLs {
		// If custom URLs are provided, both must be set and region must be empty
		if c.TokenEndpointURL == "" {
			return errors.New("token_endpoint_url must be set when events_base_url is provided")
		}
		if c.EventsBaseURL == "" {
			return errors.New("events_base_url must be set when token_endpoint_url is provided")
		}
		if c.Region != "" {
			return errors.New("region cannot be set when using custom URLs (token_endpoint_url and events_base_url)")
		}
	} else {
		// If custom URLs are not provided, use region-based configuration
		if c.Region == "" {
			c.Region = "us"
		}
		if c.Region != "us" && c.Region != "eu" {
			return fmt.Errorf("invalid region: %s (must be 'us' or 'eu')", c.Region)
		}
	}

	return nil
}

type BitwardenAdapter struct {
	conf          BitwardenConfig
	uspClient     utils.Shipper
	httpClient    *http.Client
	chStopped     chan struct{}
	wgSenders     sync.WaitGroup
	doStop        *utils.Event
	ctx           context.Context
	accessToken   string
	tokenExpiry   time.Time
	tokenMutex    sync.RWMutex
	eventsBaseURL string
	tokenEndpoint string
	lastStartDate *time.Time
}

func NewBitwardenAdapter(conf BitwardenConfig) (*BitwardenAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	var err error
	a := &BitwardenAdapter{
		conf:      conf,
		ctx:       context.Background(),
		doStop:    utils.NewEvent(),
		chStopped: make(chan struct{}),
	}

	// Set URLs: use custom URLs if provided, otherwise use region-specific URLs
	if conf.TokenEndpointURL != "" && conf.EventsBaseURL != "" {
		a.tokenEndpoint = conf.TokenEndpointURL
		a.eventsBaseURL = conf.EventsBaseURL
	} else if conf.Region == "eu" {
		a.eventsBaseURL = eventsBaseURLEU
		a.tokenEndpoint = tokenEndpointEU
	} else {
		a.eventsBaseURL = eventsBaseURLUS
		a.tokenEndpoint = tokenEndpointUS
	}

	client, err := uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	// Wrap with filtering if configured
	if len(conf.Filters) > 0 {
		filtered, err := utils.NewFilteredClient(client, conf.Filters, conf.ClientOptions.DebugLog)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create filter: %w", err)
		}
		a.uspClient = filtered
	} else {
		a.uspClient = client
	}

	a.httpClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
		},
	}

	a.wgSenders.Add(1)
	go a.fetchEvents()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *BitwardenAdapter) Close() error {
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

func (a *BitwardenAdapter) getAccessToken() (string, error) {
	a.tokenMutex.RLock()
	// Check if token is still valid (with 5 minute buffer)
	if a.accessToken != "" && time.Now().Add(5*time.Minute).Before(a.tokenExpiry) {
		token := a.accessToken
		a.tokenMutex.RUnlock()
		return token, nil
	}
	a.tokenMutex.RUnlock()

	a.tokenMutex.Lock()
	defer a.tokenMutex.Unlock()

	// Double-check after acquiring write lock
	if a.accessToken != "" && time.Now().Add(5*time.Minute).Before(a.tokenExpiry) {
		return a.accessToken, nil
	}

	a.conf.ClientOptions.DebugLog("requesting new access token")

	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("scope", "api.organization")
	data.Set("client_id", a.conf.ClientID)
	data.Set("client_secret", a.conf.ClientSecret)

	req, err := http.NewRequest("POST", a.tokenEndpoint, bytes.NewBufferString(data.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token request failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return "", fmt.Errorf("failed to parse token response: %v", err)
	}

	accessToken, ok := result["access_token"].(string)
	if !ok {
		return "", errors.New("access_token missing in response")
	}

	expiresIn, ok := result["expires_in"].(float64)
	if !ok {
		// Default to 60 minutes if not specified
		expiresIn = 3600
	}

	a.accessToken = accessToken
	a.tokenExpiry = time.Now().Add(time.Duration(expiresIn) * time.Second)

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("received new access token, expires in %d seconds", int(expiresIn)))

	return a.accessToken, nil
}

func (a *BitwardenAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("Bitwarden event collection stopping")

	for !a.doStop.WaitFor(30 * time.Second) {
		// Capture end time before fetching to avoid gaps
		endTime := time.Now()

		items, continuationToken, err := a.makeOneRequest("", &endTime)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("failed to fetch events: %v", err))
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
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
					a.doStop.Set()
					return
				}
			}
		}

		// If there's a continuation token, fetch more pages immediately
		for continuationToken != "" {
			if a.doStop.IsSet() {
				return
			}

			items, newToken, err := a.makeOneRequest(continuationToken, &endTime)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("failed to fetch events with continuation: %v", err))
				break
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
					if err != nil {
						a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
						a.doStop.Set()
						return
					}
				}
			}

			continuationToken = newToken
		}

		// Update checkpoint to the endTime we used for this fetch cycle
		a.lastStartDate = &endTime
	}
}

func (a *BitwardenAdapter) makeOneRequest(continuationToken string, endTime *time.Time) ([]utils.Dict, string, error) {
	token, err := a.getAccessToken()
	if err != nil {
		return nil, "", fmt.Errorf("failed to get access token: %v", err)
	}

	// Build URL with query parameters
	eventsURL := fmt.Sprintf("%s/public/events", a.eventsBaseURL)
	queryParams := url.Values{}

	if continuationToken != "" {
		queryParams.Set("continuationToken", continuationToken)
	} else if a.lastStartDate != nil && endTime != nil {
		// Use explicit start and end times for the query
		queryParams.Set("start", a.lastStartDate.Format(time.RFC3339))
		queryParams.Set("end", endTime.Format(time.RFC3339))
	}

	if len(queryParams) > 0 {
		eventsURL = fmt.Sprintf("%s?%s", eventsURL, queryParams.Encode())
	}

	req, err := http.NewRequest("GET", eventsURL, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusTooManyRequests {
		a.conf.ClientOptions.OnWarning("rate limit exceeded, will retry")
		return nil, "", fmt.Errorf("rate limit exceeded (429)")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var respData map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &respData); err != nil {
		return nil, "", fmt.Errorf("failed to parse response: %v", err)
	}

	// Extract data array
	dataArray, ok := respData["data"].([]interface{})
	if !ok {
		return nil, "", fmt.Errorf("missing or invalid 'data' field in response")
	}

	items := make([]utils.Dict, 0, len(dataArray))
	for _, item := range dataArray {
		if itemDict, ok := item.(map[string]interface{}); ok {
			items = append(items, utils.Dict(itemDict))
		}
	}

	// Extract continuation token
	newToken := ""
	if ct, ok := respData["continuationToken"].(string); ok {
		newToken = ct
	}

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetched %d events, continuationToken=%v", len(items), newToken != ""))

	return items, newToken, nil
}
