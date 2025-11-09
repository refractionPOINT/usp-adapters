package usp_trendmicro

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/adaptertypes"
	"github.com/refractionPOINT/usp-adapters/utils"
)

var regionalDomains = map[string]string{
	"us": "https://api.xdr.trendmicro.com",
	"eu": "https://api.eu.xdr.trendmicro.com",
	"sg": "https://api.sg.xdr.trendmicro.com",
	"jp": "https://api.xdr.trendmicro.co.jp",
	"in": "https://api.in.xdr.trendmicro.com",
	"au": "https://api.au.xdr.trendmicro.com",
}

type TrendMicroAdapter struct {
	conf       adaptertypes.TrendMicroConfig
	uspClient  *uspclient.Client
	httpClient *http.Client
	chStopped  chan struct{}
	wgSenders  sync.WaitGroup
	doStop     *utils.Event
	ctx        context.Context
	baseURL    string
	lastFetch  time.Time
}

func NewTrendMicroAdapter(conf adaptertypes.TrendMicroConfig) (*TrendMicroAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	var err error
	a := &TrendMicroAdapter{
		conf:      conf,
		ctx:       context.Background(),
		doStop:    utils.NewEvent(),
		chStopped: make(chan struct{}),
		lastFetch: time.Now().Add(-24 * time.Hour), // Start by fetching last 24 hours
	}

	// Set regional base URL
	a.baseURL = regionalDomains[conf.Region]

	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		return nil, nil, err
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

	for !a.doStop.WaitFor(60 * time.Second) {
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
