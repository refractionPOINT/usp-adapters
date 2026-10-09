package usp_trendmicro

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// defaultPollInterval is the historical fixed wait between polls of the
// Vision One Workbench alerts endpoint.
const defaultPollInterval = 60 * time.Second

var regionalDomains = map[string]string{
	"us": "https://api.xdr.trendmicro.com",
	"eu": "https://api.eu.xdr.trendmicro.com",
	"sg": "https://api.sg.xdr.trendmicro.com",
	"jp": "https://api.xdr.trendmicro.co.jp",
	"in": "https://api.in.xdr.trendmicro.com",
	"au": "https://api.au.xdr.trendmicro.com",
}

type TrendMicroConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	APIToken      string                  `json:"api_token" yaml:"api_token"`
	Region        string                  `json:"region" yaml:"region"` // "us", "eu", "sg", "jp", "in", "au" - defaults to "us"

	// URL optionally overrides the regional Vision One API root (e.g.
	// "https://api.xdr.trendmicro.com"). When empty, the root is derived from
	// Region as before.
	URL string `json:"url" yaml:"url"`

	// PollInterval overrides the fixed wait between polls of the alerts
	// endpoint. It is not settable through a config file; it exists as a seam
	// for tests (the production interval stays the historical 60 seconds).
	PollInterval time.Duration `json:"-" yaml:"-"`
}

func (c *TrendMicroConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}

	if c.APIToken == "" {
		return errors.New("missing api_token")
	}
	if c.Region == "" {
		c.Region = "us"
	}
	if _, ok := regionalDomains[c.Region]; !ok {
		return fmt.Errorf("invalid region: %s (must be one of: us, eu, sg, jp, in, au)", c.Region)
	}
	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	return nil
}

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type TrendMicroAdapter struct {
	conf       TrendMicroConfig
	uspClient  uspSink
	httpClient *http.Client
	chStopped  chan struct{}
	wgSenders  sync.WaitGroup
	doStop     *utils.Event
	ctx        context.Context
	baseURL    string
	lastFetch  time.Time
}

func NewTrendMicroAdapter(ctx context.Context, conf TrendMicroConfig) (*TrendMicroAdapter, chan struct{}, error) {
	return newTrendMicroAdapter(ctx, conf, nil)
}

// newTrendMicroAdapter is the implementation behind NewTrendMicroAdapter. When
// sink is non-nil it is used in place of a real LimaCharlie client -- the seam
// tests use to capture shipped events.
func newTrendMicroAdapter(ctx context.Context, conf TrendMicroConfig, sink uspSink) (*TrendMicroAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &TrendMicroAdapter{
		conf:      conf,
		ctx:       context.Background(),
		doStop:    utils.NewEvent(),
		chStopped: make(chan struct{}),
		lastFetch: time.Now().Add(-24 * time.Hour), // Start by fetching last 24 hours
	}

	// Set regional base URL, unless explicitly overridden.
	a.baseURL = regionalDomains[conf.Region]
	if conf.URL != "" {
		a.baseURL = strings.TrimRight(conf.URL, "/")
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

	a.wgSenders.Add(1)
	go a.fetchAlerts()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *TrendMicroAdapter) Close() error {
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

func (a *TrendMicroAdapter) fetchAlerts() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("Trend Micro alert collection stopping")

	for !a.doStop.WaitFor(a.conf.PollInterval) {
		items, err := a.fetchAllPages()
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("failed to fetch alerts: %v", err))
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

		// Update last fetch time to now for next iteration
		a.lastFetch = time.Now()
	}
}

func (a *TrendMicroAdapter) fetchAllPages() ([]utils.Dict, error) {
	allItems := []utils.Dict{}
	nextLink := ""
	isFirstPage := true

	for {
		items, newNextLink, err := a.makeOneRequest(nextLink, isFirstPage)
		if err != nil {
			return nil, err
		}

		allItems = append(allItems, items...)

		if newNextLink == "" {
			break
		}

		nextLink = newNextLink
		isFirstPage = false

		if a.doStop.IsSet() {
			break
		}
	}

	return allItems, nil
}

func (a *TrendMicroAdapter) makeOneRequest(nextLink string, isFirstPage bool) ([]utils.Dict, string, error) {
	var alertsURL string

	if nextLink != "" {
		// Use the full nextLink URL provided by the API
		alertsURL = nextLink
	} else {
		// Build initial request with date filters
		alertsURL = fmt.Sprintf("%s/v3.0/workbench/alerts", a.baseURL)

		queryParams := url.Values{}
		// Format times in ISO 8601 with UTC timezone
		startTime := a.lastFetch.UTC().Format(time.RFC3339)
		endTime := time.Now().UTC().Format(time.RFC3339)

		queryParams.Set("startDateTime", startTime)
		queryParams.Set("endDateTime", endTime)

		alertsURL = fmt.Sprintf("%s?%s", alertsURL, queryParams.Encode())
	}

	req, err := http.NewRequest("GET", alertsURL, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.conf.APIToken))
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

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, "", fmt.Errorf("authentication failed (401) - check your API token")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var respData map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &respData); err != nil {
		return nil, "", fmt.Errorf("failed to parse response: %v", err)
	}

	// Extract items array
	itemsArray, ok := respData["items"].([]interface{})
	if !ok {
		// If no items field, this might be an empty response or error
		if len(bodyBytes) > 0 {
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("response does not contain 'items' array: %s", string(bodyBytes)))
		}
		return []utils.Dict{}, "", nil
	}

	items := make([]utils.Dict, 0, len(itemsArray))
	for _, item := range itemsArray {
		if itemDict, ok := item.(map[string]interface{}); ok {
			items = append(items, utils.Dict(itemDict))
		}
	}

	// Extract nextLink for pagination
	newNextLink := ""
	if nl, ok := respData["nextLink"].(string); ok {
		newNextLink = nl
	}

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetched %d alerts, nextLink=%v", len(items), newNextLink != ""))

	return items, newNextLink, nil
}
