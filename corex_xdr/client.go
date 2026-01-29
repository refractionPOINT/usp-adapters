package usp_cortex_xdr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	queryInterval     = 60
	incidentsEndpoint = "/public_api/v1/incidents/get_incidents/"
	alertsEndpoint    = "/public_api/v1/alerts/get_alerts_multi_events/"
	dedupeWindow      = 30 * time.Minute
	maxRetries        = 3
	initialRetryDelay = 5 * time.Second
)

type CortexXDRConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	FQDN            string                  `json:"fqdn" yaml:"fqdn"`
	APIKey          string                  `json:"api_key" yaml:"api_key"`
	APIKeyID        string                  `json:"api_key_id" yaml:"api_key_id"`
	InitialLookback time.Duration           `json:"initial_lookback,omitempty" yaml:"initial_lookback,omitempty"` // eg, 24h, 30m, 168h, 1h30m
}

type CortexXDRAdapter struct {
	conf       CortexXDRConfig
	uspClient  *uspclient.Client
	httpClient *http.Client
	chStopped  chan struct{}

	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc

	incidentsDedupe map[string]int64
	alertsDedupe    map[string]int64
}

type CortexXDRResponse interface {
	GetData() []utils.Dict
	GetResultCount() int
	GetTotalCount() int
}

type CortexXDRIncidentsResponse struct {
	Reply struct {
		ResultCount int          `json:"result_count"`
		TotalCount  int          `json:"total_count"`
		Incidents   []utils.Dict `json:"incidents"`
	} `json:"reply"`
}

func (r *CortexXDRIncidentsResponse) GetData() []utils.Dict {
	return r.Reply.Incidents
}

func (r *CortexXDRIncidentsResponse) GetResultCount() int {
	return r.Reply.ResultCount
}

func (r *CortexXDRIncidentsResponse) GetTotalCount() int {
	return r.Reply.TotalCount
}

type CortexXDRAlertsResponse struct {
	Reply struct {
		ResultCount int          `json:"result_count"`
		TotalCount  int          `json:"total_count"`
		Alerts      []utils.Dict `json:"alerts"`
	} `json:"reply"`
}

func (r *CortexXDRAlertsResponse) GetData() []utils.Dict {
	return r.Reply.Alerts
}

func (r *CortexXDRAlertsResponse) GetResultCount() int {
	return r.Reply.ResultCount
}

func (r *CortexXDRAlertsResponse) GetTotalCount() int {
	return r.Reply.TotalCount
}

func NewCortexXDRAdapter(ctx context.Context, conf CortexXDRConfig) (*CortexXDRAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}
	a := &CortexXDRAdapter{
		conf:            conf,
		incidentsDedupe: make(map[string]int64),
		alertsDedupe:    make(map[string]int64),
	}

	rootCtx, cancel := context.WithCancel(ctx)
	a.ctx = rootCtx
	a.cancel = cancel

	var err error
	a.uspClient, err = uspclient.NewClient(rootCtx, conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	a.httpClient = &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	a.chStopped = make(chan struct{})

	go a.fetchEvents()

	return a, a.chStopped, nil
}

func (c *CortexXDRConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.FQDN == "" {
		return errors.New("missing fqdn")
	}
	if c.APIKey == "" {
		return errors.New("missing api_key")
	}
	if c.APIKeyID == "" {
		return errors.New("missing api_key_id")
	}
	// InitialLookback defaults to zero (current time, no lookback)
	return nil
}

func (a *CortexXDRAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	var err1, err2 error
	a.once.Do(func() {
		a.cancel()
		err1 = a.uspClient.Drain(1 * time.Minute)
		_, err2 = a.uspClient.Close()
		a.httpClient.CloseIdleConnections()
		close(a.chStopped)
	})
	if err1 != nil {
		return err1
	}
	return err2
}

type API struct {
	Endpoint     string
	Key          string
	ResponseType CortexXDRResponse
	Dedupe       map[string]int64
	timeField    string
	idField      string
}

func (a *CortexXDRAdapter) fetchEvents() {
	since := map[string]time.Time{
		"incidents": time.Now().Add(-1 * a.conf.InitialLookback).UTC(),
		"alerts":    time.Now().Add(-1 * a.conf.InitialLookback).UTC(),
	}

	APIs := []API{
		{
			Endpoint:     incidentsEndpoint,
			Key:          "incidents",
			ResponseType: &CortexXDRIncidentsResponse{},
			timeField:    "creation_time",
			idField:      "incident_id",
			Dedupe:       a.incidentsDedupe,
		},
		{
			Endpoint:     alertsEndpoint,
			Key:          "alerts",
			ResponseType: &CortexXDRAlertsResponse{},
			timeField:    "server_creation_time",
			idField:      "alert_id",
			Dedupe:       a.alertsDedupe,
		},
	}

	// Helper function to fetch and process events for all APIs
	fetchAllAPIs := func() {
		cycleTime := time.Now()
		allItems := []utils.Dict{}

		for _, api := range APIs {
			items, err := a.getEvents(since[api.Key], cycleTime, api)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("%s fetch failed: %w", api.Key, err))
				// Don't update since time on failure to avoid data loss
				continue
			}

			// Only update since time on successful fetch
			since[api.Key] = cycleTime.Add(-queryInterval * time.Second)

			if len(items) > 0 {
				allItems = append(allItems, items...)
			}
		}

		if len(allItems) > 0 {
			a.submitEvents(allItems)
		}
	}

	// Execute immediately on startup
	a.conf.ClientOptions.DebugLog("performing initial fetch")
	fetchAllAPIs()

	ticker := time.NewTicker(queryInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", a.conf.FQDN))
			return
		case <-ticker.C:
			fetchAllAPIs()
		}
	}
}

func (a *CortexXDRAdapter) getEvents(since time.Time, cycleTime time.Time, api API) ([]utils.Dict, error) {
	var allItems []utils.Dict

	// Clean up old dedupe entries (30 minute window)
	cutoffTime := cycleTime.Add(-dedupeWindow).Unix()
	for k, v := range api.Dedupe {
		if v < cutoffTime {
			delete(api.Dedupe, k)
		}
	}

	sinceMs := since.UTC().UnixMilli()

	searchFrom := 0
	pageSize := 100

	for {
		requestBody := map[string]interface{}{
			"request_data": map[string]interface{}{
				"filters": []map[string]interface{}{
					{
						"field":    api.timeField,
						"operator": "gte",
						"value":    sinceMs,
					},
				},
				"search_from": searchFrom,
				"search_to":   searchFrom + pageSize,
				"sort": map[string]string{
					"field":   api.timeField,
					"keyword": "asc",
				},
			},
		}

		response, err := a.doRequest(api.Endpoint, requestBody, api)
		if err != nil {
			return nil, err
		}

		resultCount := response.GetResultCount()
		totalCount := response.GetTotalCount()

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("%s: fetched %d results (total: %d, from: %d)",
			api.Key, resultCount, totalCount, searchFrom))

		for _, event := range response.GetData() {
			var dedupeID string
			if idValue, exists := event[api.idField]; exists {
				dedupeID = fmt.Sprintf("%v", idValue)
			} else {
				dedupeID = a.generateLogHash(event)
			}

			var timeValue time.Time
			timeField, exists := event[api.timeField]
			if !exists {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("%s: event missing time field '%s'", api.Key, api.timeField))
				continue
			}

			switch v := timeField.(type) {
			case float64:
				// Handle numeric timestamp (milliseconds)
				timeValue = time.UnixMilli(int64(v))
			case int64:
				// Handle int64 timestamp (milliseconds)
				timeValue = time.UnixMilli(v)
			case uint64:
				// Handle uint64 timestamp (milliseconds)
				timeValue = time.UnixMilli(int64(v))
			case int:
				// Handle int timestamp (milliseconds)
				timeValue = time.UnixMilli(int64(v))
			case string:
				// Handle string timestamp - try to parse as milliseconds
				if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
					timeValue = time.UnixMilli(ms)
				} else {
					a.conf.ClientOptions.OnWarning(fmt.Sprintf("%s: could not parse string timestamp '%s'", api.Key, v))
					continue
				}
			default:
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("%s: event time field '%s' has unsupported type %T with value: %v", api.Key, api.timeField, timeField, timeField))
				continue
			}

			if _, seen := api.Dedupe[dedupeID]; !seen {
				if timeValue.After(since) || timeValue.Equal(since) {
					api.Dedupe[dedupeID] = timeValue.Unix()
					allItems = append(allItems, event)
				}
			}
		}

		if resultCount == 0 || searchFrom+resultCount >= totalCount {
			// No more results to fetch
			break
		}

		searchFrom += pageSize
	}

	return allItems, nil
}

func (a *CortexXDRAdapter) generateLogHash(logMap map[string]interface{}) string {
	// Extract and sort keys
	keys := make([]string, 0, len(logMap))
	for k := range logMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build deterministic string representation
	var buf bytes.Buffer
	for _, k := range keys {
		fmt.Fprintf(&buf, "%s:%v|", k, logMap[k])
	}

	hash := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(hash[:])
}

func (a *CortexXDRAdapter) doRequest(endpoint string, requestBody map[string]interface{}, api API) (CortexXDRResponse, error) {
	retryDelay := initialRetryDelay
	retries := 0

	for {
		select {
		case <-a.ctx.Done():
			return nil, a.ctx.Err()
		default:
		}

		var respBody []byte
		var status int
		var isTransientError bool

		err := func() error {
			loopCtx, cancel := context.WithTimeout(a.ctx, 30*time.Second)
			defer cancel()

			bodyJSON, err := json.Marshal(requestBody)
			if err != nil {
				return err
			}

			url := fmt.Sprintf("https://%s%s", a.conf.FQDN, endpoint)
			req, err := http.NewRequestWithContext(loopCtx, "POST", url, bytes.NewBuffer(bodyJSON))
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("cortex xdr %s api request error: %v", api.Key, err))
				return err
			}

			// Set authentication headers (Standard mode)
			req.Header.Set("Authorization", a.conf.APIKey)
			req.Header.Set("x-xdr-auth-id", a.conf.APIKeyID)
			req.Header.Set("Content-Type", "application/json")

			resp, err := a.httpClient.Do(req)
			if err != nil {
				isTransientError = true
				a.conf.ClientOptions.OnError(fmt.Errorf("cortex xdr %s api do error: %v", api.Key, err))
				return err
			}

			defer resp.Body.Close()

			respBody, err = io.ReadAll(resp.Body)
			if err != nil {
				isTransientError = true
				a.conf.ClientOptions.OnError(fmt.Errorf("cortex xdr %s api read error: %v", api.Key, err))
				return err
			}
			status = resp.StatusCode
			return nil
		}()

		// Handle transient network errors with retry
		if err != nil && isTransientError {
			retries++
			if retries > maxRetries {
				return nil, fmt.Errorf("cortex xdr %s api failed after %d retries: %w", api.Key, maxRetries, err)
			}
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("cortex xdr %s api transient error, retry %d/%d in %v", api.Key, retries, maxRetries, retryDelay))
			if err := a.sleepContext(retryDelay); err != nil {
				return nil, err
			}
			retryDelay *= 2 // Exponential backoff
			continue
		} else if err != nil {
			return nil, err
		}

		// Handle rate limiting
		if status == http.StatusTooManyRequests {
			a.conf.ClientOptions.OnWarning("getEventsRequest got 429, sleeping 60s before retry")
			if err := a.sleepContext(60 * time.Second); err != nil {
				return nil, err
			}
			continue
		}

		// Handle server errors (5xx) with retry
		if status >= 500 && status < 600 {
			retries++
			if retries > maxRetries {
				return nil, fmt.Errorf("cortex xdr %s api server error %d after %d retries\nRESPONSE: %s", api.Key, status, maxRetries, string(respBody))
			}
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("cortex xdr %s api server error %d, retry %d/%d in %v", api.Key, status, retries, maxRetries, retryDelay))
			if err := a.sleepContext(retryDelay); err != nil {
				return nil, err
			}
			retryDelay *= 2 // Exponential backoff
			continue
		}

		if status != http.StatusOK {
			return nil, fmt.Errorf("cortex xdr %s api non-200: %d\nRESPONSE %s", api.Key, status, string(respBody))
		}

		err = json.Unmarshal(respBody, &api.ResponseType)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("cortex xdr %s api invalid json: %v\nResponse body: %s", api.Key, err, string(respBody)))
			return nil, err
		}

		return api.ResponseType, nil
	}
}

func (a *CortexXDRAdapter) submitEvents(items []utils.Dict) {
	for _, item := range items {
		msg := &protocol.DataMessage{
			JsonPayload: item,
			TimestampMs: uint64(time.Now().UTC().UnixNano() / int64(time.Millisecond)),
		}
		if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
			if err == uspclient.ErrorBufferFull {
				a.conf.ClientOptions.OnWarning("stream falling behind")
				if err := a.uspClient.Ship(msg, 1*time.Hour); err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
					a.Close()
					return
				}
			} else {
				a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			}
		}
	}
}

func (a *CortexXDRAdapter) sleepContext(d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-a.ctx.Done():
		return a.ctx.Err()
	}
}
