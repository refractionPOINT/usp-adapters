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
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	// queryInterval controls both the polling ticker and the since-cursor overlap.
	// Each cycle queries [since, cycleTime - queryBuffer] and then sets since back
	// by queryInterval from cycleTime. This creates an overlap between consecutive
	// query windows so that events near the boundary are not missed. The dedupe map
	// prevents duplicate delivery. Changing queryInterval without updating the
	// ticker (or vice versa) will break the overlap guarantee.
	queryInterval     = 60
	incidentsEndpoint = "/public_api/v1/incidents/get_incidents/"
	alertsEndpoint    = "/public_api/v1/alerts/get_alerts_multi_events/"
	dedupeWindow      = 30 * time.Minute
	maxRetries        = 3
	initialRetryDelay = 5 * time.Second
	// queryBuffer excludes recent events from queries to avoid race conditions
	// where the API's total_count includes events not yet fully queryable.
	queryBuffer = 5 * time.Second
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
	c.FQDN = strings.TrimPrefix(c.FQDN, "https://")
	c.FQDN = strings.TrimPrefix(c.FQDN, "http://")
	c.FQDN = strings.TrimRight(c.FQDN, "/")
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
	Endpoint      string
	Key           string
	Dedupe        map[string]int64
	filterField   string // field name used in API filter/sort requests
	responseField string // field name in the response data for timestamp
	idField       string
}

func (a *CortexXDRAdapter) fetchEvents() {
	since := map[string]time.Time{
		"incidents": time.Now().Add(-1 * a.conf.InitialLookback).UTC(),
		"alerts":    time.Now().Add(-1 * a.conf.InitialLookback).UTC(),
	}

	APIs := []API{
		{
			Endpoint:      incidentsEndpoint,
			Key:           "incidents",
			filterField:   "creation_time",
			responseField: "creation_time",
			idField:       "incident_id",
			Dedupe:        a.incidentsDedupe,
		},
		{
			Endpoint:      alertsEndpoint,
			Key:           "alerts",
			filterField:   "creation_time",
			responseField: "detection_timestamp",
			idField:       "alert_id",
			Dedupe:        a.alertsDedupe,
		},
	}

	// Helper function to fetch and process events for all APIs
	fetchAllAPIs := func() {
		cycleTime := time.Now()

		for _, api := range APIs {
			items, err := a.getEvents(since[api.Key], cycleTime, api)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("%s fetch failed: %w", api.Key, err))
				// Don't update since time on failure to avoid data loss
				continue
			}

			if len(items) > 0 {
				if err := a.submitEvents(items); err != nil {
					// submitEvents already called Close(), stop processing
					return
				}
			}

			// Only update since time after successful fetch and ship
			since[api.Key] = cycleTime.Add(-queryInterval * time.Second)
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
	untilMs := cycleTime.Add(-queryBuffer).UTC().UnixMilli()

	searchFrom := 0
	pageSize := 100

	for {
		select {
		case <-a.ctx.Done():
			return nil, a.ctx.Err()
		default:
		}

		requestBody := map[string]interface{}{
			"request_data": map[string]interface{}{
				"filters": []map[string]interface{}{
					{
						"field":    api.filterField,
						"operator": "gte",
						"value":    sinceMs,
					},
					{
						"field":    api.filterField,
						"operator": "lte",
						"value":    untilMs,
					},
				},
				"search_from": searchFrom,
				"search_to":   searchFrom + pageSize,
				"sort": map[string]string{
					"field":   api.filterField,
					"keyword": "asc",
				},
			},
		}

		response, err := a.doRequest(api.Endpoint, requestBody, api)
		if err != nil {
			return nil, err
		}

		data := response.GetData()
		resultCount := response.GetResultCount()
		totalCount := response.GetTotalCount()

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("%s: fetched %d results (total: %d, items_in_response: %d, from: %d)",
			api.Key, resultCount, totalCount, len(data), searchFrom))

		for _, event := range data {
			var dedupeID string
			if idValue, exists := event[api.idField]; exists {
				dedupeID = fmt.Sprintf("%v", idValue)
			} else {
				dedupeID = a.generateLogHash(event)
			}

			var timeValue time.Time
			tsValue, exists := event[api.responseField]
			if !exists {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("%s: event missing time field '%s'", api.Key, api.responseField))
				continue
			}

			switch v := tsValue.(type) {
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
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("%s: event time field '%s' has unsupported type %T with value: %v", api.Key, api.responseField, tsValue, tsValue))
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

		searchFrom += resultCount
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
	rateLimitHits := 0

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
			rateLimitHits++
			if rateLimitHits%10 == 0 {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("cortex xdr %s api has been rate limited %d consecutive times", api.Key, rateLimitHits))
			}
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("cortex xdr %s api got 429, sleeping 60s before retry (%d consecutive)", api.Key, rateLimitHits))
			if err := a.sleepContext(60 * time.Second); err != nil {
				return nil, err
			}
			continue
		}

		// Handle authentication errors as fatal
		if status == http.StatusUnauthorized || status == http.StatusForbidden {
			err := fmt.Errorf("cortex xdr %s api authentication failed (%d) - check api_key and api_key_id configuration\nRESPONSE: %s", api.Key, status, string(respBody))
			a.conf.ClientOptions.OnError(err)
			a.Close()
			return nil, err
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

		var response CortexXDRResponse
		switch api.Key {
		case "incidents":
			response = &CortexXDRIncidentsResponse{}
		case "alerts":
			response = &CortexXDRAlertsResponse{}
		}

		err = json.Unmarshal(respBody, response)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("cortex xdr %s api invalid json: %v\nResponse body: %s", api.Key, err, string(respBody)))
			return nil, err
		}

		return response, nil
	}
}

func (a *CortexXDRAdapter) submitEvents(items []utils.Dict) error {
	for _, item := range items {
		msg := &protocol.DataMessage{
			JsonPayload: item,
			TimestampMs: uint64(time.Now().UTC().UnixNano() / int64(time.Millisecond)),
		}
		if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
			if err == uspclient.ErrorBufferFull {
				a.conf.ClientOptions.OnWarning("stream falling behind")
				err = a.uspClient.Ship(msg, 1*time.Hour)
			}
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				a.Close()
				return err
			}
		}
	}
	return nil
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
