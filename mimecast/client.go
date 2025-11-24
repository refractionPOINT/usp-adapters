package usp_mimecast

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
	"net/url"
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
	overlapPeriod = 30 * time.Second
	queryInterval = 30 // seconds
)

type MimecastAdapter struct {
	conf        MimecastConfig
	uspClient   *uspclient.Client
	httpClient  *http.Client
	closeOnce   sync.Once
	fetchOnce   sync.Once
	chStopped   chan struct{}
	chFetchLoop chan struct{}

	ctx    context.Context
	cancel context.CancelFunc

	oauthToken  string
	tokenExpiry time.Time
	tokenMu     sync.Mutex
}

type AuditRequest struct {
	Data []AuditEvent `json:"data"`
	Meta MetaData     `json:"meta"`
}

type AuditEvent struct {
	StartDateTime string `json:"startDateTime"`
	EndDateTime   string `json:"endDateTime"`
}

type ApiResponse struct {
	Meta MetaData                 `json:"meta"`
	Data []map[string]interface{} `json:"data"`
	Fail []FailureDetails         `json:"fail"`
}

type FailureDetails struct {
	Errors []ErrorDetail `json:"errors"`
}

type ErrorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type MetaData struct {
	Pagination Pagination `json:"pagination"`
	Status     int        `json:"status"`
}

type Pagination struct {
	PageSize int    `json:"pageSize"`
	Next     string `json:"next"`
}

type AuditLog struct {
	ID        string `json:"id"`
	AuditType string `json:"auditType"`
	User      string `json:"user"`
	EventTime string `json:"eventTime"`
	EventInfo string `json:"eventInfo"`
	Category  string `json:"category"`
}

type MimecastConfig struct {
	ClientOptions        uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ClientId             string                  `json:"client_id" yaml:"client_id"`
	ClientSecret         string                  `json:"client_secret" yaml:"client_secret"`
	BaseURL              string                  `json:"base_url,omitempty" yaml:"base_url,omitempty"`
	InitialLookback      time.Duration           `json:"initial_lookback,omitempty" yaml:"initial_lookback,omitempty"` // eg, 24h, 30m, 168h, 1h30m
	MaxConcurrentWorkers int                     `json:"max_concurrent_workers,omitempty" yaml:"max_concurrent_workers,omitempty"`
}

func (c *MimecastConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err) // Required
	}

	if c.ClientId == "" {
		return errors.New("missing client id") // Required
	}
	if c.ClientSecret == "" {
		return errors.New("missing client secret") // Required
	}

	if c.BaseURL == "" {
		c.BaseURL = "https://api.services.mimecast.com" // Default Global - May process data in other regions during failover
		// US Specific - Ensures data stays within the US instance: https://us-api.services.mimecast.com
		// UK Specific - Ensures data stays within the UK instance: https://uk-api.services.mimecast.com
	}

	if c.InitialLookback == 0 {
		c.InitialLookback = 24 * time.Hour // Default
	} else {
		if c.InitialLookback < 1*time.Minute {
			return fmt.Errorf("initial_lookback must be at least 1 minute, got %s", c.InitialLookback)
		}
		if c.InitialLookback > 30*24*time.Hour { // 30 days
			return fmt.Errorf("initial_lookback must be less than 30 days, got %s", c.InitialLookback)
		}
	}

	if c.MaxConcurrentWorkers == 0 {
		c.MaxConcurrentWorkers = 10 // Default
	} else if c.MaxConcurrentWorkers > 100 {
    	return fmt.Errorf("max_concurrent_workers cannot exceed 100, got %d", c.MaxConcurrentWorkers)
	}

	return nil
}

func NewMimecastAdapter(ctx context.Context, conf MimecastConfig) (*MimecastAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	var err error
	ctxChild, cancel := context.WithCancel(ctx)
	a := &MimecastAdapter{
		conf:   conf,
		ctx:    ctxChild,
		cancel: cancel,
	}

	a.uspClient, err = uspclient.NewClient(ctxChild, conf.ClientOptions)
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
	a.chFetchLoop = make(chan struct{})

	go a.fetchEvents()

	return a, a.chStopped, nil
}

func (a *MimecastAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	var err1, err2 error
	a.closeOnce.Do(func() {
		a.cancel()
		select {
		case <-a.chFetchLoop:
		case <-time.After(10 * time.Second):
			a.conf.ClientOptions.OnWarning("timeout waiting for fetch loop to exit; proceeding with cleanup")
		}
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

// getAuthHeaders gets valid OAuth bearer token headers
func (a *MimecastAdapter) getAuthHeaders(ctx context.Context) (map[string]string, error) {
	// Check if we have a valid token
	a.tokenMu.Lock()
	if a.oauthToken != "" && time.Now().Before(a.tokenExpiry) {
		token := a.oauthToken
		a.tokenMu.Unlock()

		return map[string]string{
			"Authorization": fmt.Sprintf("Bearer %s", token),
			"Accept":        "application/json",
			"Content-Type":  "application/json",
		}, nil
	}
	
	defer a.tokenMu.Unlock()

	// Need to get a new token
	return a.refreshOAuthToken(ctx)
}

// refreshOAuthToken gets a new OAuth 2.0 access token
func (a *MimecastAdapter) refreshOAuthToken(ctx context.Context) (map[string]string, error) {
	tokenURL := a.conf.BaseURL + "/oauth/token"

	// Prepare form data
	data := url.Values{}
	data.Set("client_id", a.conf.ClientId)
	data.Set("client_secret", a.conf.ClientSecret)
	data.Set("grant_type", "client_credentials")

	loopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(loopCtx, "POST", tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create token request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to request token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("oauth token request failed: %d - %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("failed to decode token response: %w", err)
	}

	gracePeriod := 60
	if tokenResp.ExpiresIn < gracePeriod {
		gracePeriod = 0  // or tokenResp.ExpiresIn / 2, or some other logic
	}
	a.oauthToken = tokenResp.AccessToken
	a.tokenExpiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn-gracePeriod) * time.Second)

	return map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", tokenResp.AccessToken),
		"Accept":        "application/json",
		"Content-Type":  "application/json",
	}, nil
}

type API struct {
	mu                  sync.Mutex
	key                 string
	endpoint            string
	since               time.Time
	dedupe              map[string]int64
	active              bool
	idField             string
	timeField           string
	startTimestampField string
	endTimestampField   string
}

func (api *API) IsActive() bool {
	api.mu.Lock()
	defer api.mu.Unlock()
	return api.active
}

func (api *API) SetInactive() {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.active = false
}

// Returned results from fetch routines
type routineResult struct {
	key   string
	items []utils.Dict
	err   error
}

func (a *MimecastAdapter) fetchEvents() {
	APIs := []*API{
		{
			key:                 "auditEvents",
			endpoint:            "/api/audit/get-audit-events",
			since:               time.Now().Add(-1 * a.conf.InitialLookback),
			dedupe:              make(map[string]int64),
			active:              true,
			idField:             "id",
			timeField:           "eventTime",
			startTimestampField: "startDateTime",
			endTimestampField:   "endDateTime",
		},
		{
			key:       "attachment",
			endpoint:  "/api/ttp/attachment/get-logs",
			since:     time.Now().Add(-1 * a.conf.InitialLookback),
			dedupe:    make(map[string]int64),
			active:    true,
			idField:   "",
			timeField: "date",
		},
		{
			key:       "impersonation",
			endpoint:  "/api/ttp/impersonation/get-logs",
			since:     time.Now().Add(-1 * a.conf.InitialLookback),
			dedupe:    make(map[string]int64),
			active:    true,
			idField:   "id",
			timeField: "eventTime",
		},
		{
			key:       "url",
			endpoint:  "/api/ttp/url/get-logs",
			since:     time.Now().Add(-1 * a.conf.InitialLookback),
			dedupe:    make(map[string]int64),
			active:    true,
			idField:   "",
			timeField: "date",
		},
		{
			key:       "dlp",
			endpoint:  "/api/dlp/get-logs",
			since:     time.Now().Add(-1 * a.conf.InitialLookback),
			dedupe:    make(map[string]int64),
			active:    true,
			idField:   "",
			timeField: "eventTime",
		},
	}

	a.RunFetchLoop(APIs)
}

func (a *MimecastAdapter) shouldShutdown(apis []*API) bool {
	// If no APIs are active due to 'Forbidden' messages, shutdown
	for _, api := range apis {
		if api.active {
			return false
		}
	}
	a.conf.ClientOptions.OnWarning("all apis are disabled due to forbidden messages. shutting down")
	return true
}

func (a *MimecastAdapter) RunFetchLoop(apis []*API) {
	cycleSem := make(chan struct{}, 1)
	shipperSem := make(chan struct{}, 2)
	workerSem := make(chan struct{}, a.conf.MaxConcurrentWorkers)
	ticker := time.NewTicker(queryInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", a.conf.BaseURL))

			// Wait for any ongoing cycle to complete with timeout
			select {
			case cycleSem <- struct{}{}:
				// No cycle in progress, safe to exit
				<-cycleSem
			case <-time.After(30 * time.Second):
				a.conf.ClientOptions.OnWarning("timeout waiting for fetch cycle to complete during shutdown")
			}

			// Fetch loop isn't running, safe to close
			a.fetchOnce.Do(func() { close(a.chFetchLoop) })
			return
		case <-ticker.C:
			select {
			case cycleSem <- struct{}{}:
				go func() {
					// Hold cycle semaphore until cycle completes
					defer func() { <-cycleSem }()

					// If no APIs are active due to forbidden messages, shutdown
					if a.shouldShutdown(apis) {
						a.Close()
						return
					}

					// Capture current time once for all APIs in this cycle
					cycleTime := time.Now()

					// Communication channel to send results to shipper routine
					shipCh := make(chan []utils.Dict)
					// Used to flag when the shipper routine is done.
					shipDone := make(chan struct{})

					// shipper routine
					go func() {
						var shipperWg sync.WaitGroup
						var mu sync.Mutex

						count := 0

						defer func() {
							close(shipDone)
						}()

						// shipper routine will run until shipCh closes
						for events := range shipCh {
							eventsCopy := events
							// Consume a slot when spinning up a shipper routine
							shipperSem <- struct{}{}
							shipperWg.Add(1)
							go func(events []utils.Dict) {
								// Release a slot when done shipping
								// Decrement shipperWg
								defer func() {
									<-shipperSem
									shipperWg.Done()
								}()
								mu.Lock()
								count += len(events)
								mu.Unlock()
								a.submitEvents(events)
							}(eventsCopy)
						}
						shipperWg.Wait()
					}()

					// Channel for returning fetch data and checking for errors
					resultCh := make(chan routineResult)

					var wg sync.WaitGroup

					// fetchApi routines
					for i := range apis {
						// Check if signal to close has been sent before starting any fetches
						select {
						case <-a.ctx.Done():
							return
						default:
						}
						// If the current api is disabled due to forbidden message, skip
						if !apis[i].IsActive() {
							continue
						}
						workerSem <- struct{}{}
						wg.Add(1)
						go func(api *API) {
							defer func() {
								<-workerSem
								wg.Done()
							}()
							a.fetchApi(api, cycleTime, resultCh)
						}(apis[i])
					}

					go func() {
						wg.Wait()
						// Wait until all fetch goroutines are done to close the channel
						close(resultCh)
					}()

					// Blocking while fetchApi routines collect events
					// Events are passed off as they come in to the shipper routine
					for res := range resultCh {
						if res.err != nil {
							a.conf.ClientOptions.OnError(fmt.Errorf("%s fetch failed: %w", res.key, res.err))
							continue
						}
						if len(res.items) > 0 {
							shipCh <- res.items
						}
					}

					// resultCh has closed, meaning all events have been pooled for shipping
					close(shipCh)

					// Wait until shipping is done
					<-shipDone
				}()
			default:
				a.conf.ClientOptions.OnWarning("previous fetch cycle is still in progress, skipping this cycle")
			}
		}
	}
}

func (a *MimecastAdapter) fetchApi(api *API, cycleTime time.Time, resultCh chan<- routineResult) {
	fetchCtx, cancelFetch := context.WithCancel(a.ctx)
	defer cancelFetch()

	// Check for a close signal
	select {
	case <-fetchCtx.Done():
		return
	default:
	}

	items, err := a.makeOneRequest(api, cycleTime)
	if err != nil {
		resultCh <- routineResult{api.key, nil, err}
		return
	}

	if len(items) > 0 {
		resultCh <- routineResult{api.key, items, nil}
	}
}

func (a *MimecastAdapter) makeOneRequest(api *API, cycleTime time.Time) ([]utils.Dict, error) {
	var allItems []utils.Dict
	var start string
	var retryCount int
	var querySucceeded bool // Track if we successfully completed the query

	api.mu.Lock()
	start = api.since.UTC().Format(time.RFC3339)
	api.mu.Unlock()

	end := cycleTime.UTC().Format(time.RFC3339)

	pageToken := ""
	url := a.conf.BaseURL + api.endpoint

	for {
		var respBody []byte
		var status int
		var retryAfterInt int
		var retryAfterTime time.Time

		var startTimestampField string
		var endTimestampField string

		if api.startTimestampField == "" {
			startTimestampField = "from"
		} else {
			startTimestampField = api.startTimestampField
		}
		if api.endTimestampField == "" {
			endTimestampField = "to"
		} else {
			endTimestampField = api.endTimestampField
		}

		auditData := map[string]interface{}{
			"meta": map[string]interface{}{
				"pagination": map[string]interface{}{
					"pageSize": 500,
				},
			},
			"data": []map[string]string{
				{
					startTimestampField: start,
					endTimestampField:   end,
				},
			},
		}

		if pageToken != "" {
			if meta, ok := auditData["meta"].(map[string]interface{}); ok {
				if pagination, ok := meta["pagination"].(map[string]interface{}); ok {
					pagination["pageToken"] = pageToken
				}
			}
		}

		err := func() error {
			jsonData, err := json.Marshal(auditData)
			if err != nil {
				return err
			}

			loopCtx, cancel := context.WithTimeout(a.ctx, 30*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(loopCtx, "POST", url, bytes.NewBuffer(jsonData))
			if err != nil {
				return err
			}

			headers, err := a.getAuthHeaders(loopCtx)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("failed to get OAuth token: %v", err))
				return err
			}

			// Set all required Mimecast headers
			for key, value := range headers {
				req.Header.Set(key, value)
			}
			resp, err := a.httpClient.Do(req)

			if err != nil {
				return err
			}
			defer resp.Body.Close()

			respBody, err = io.ReadAll(resp.Body)

			status = resp.StatusCode
			ra := resp.Header.Get("Retry-After")

			if ra == "" {
				retryAfterInt = 0
				retryAfterTime = time.Time{}
			} else if secs, err := strconv.Atoi(ra); err == nil {
				retryAfterInt = secs
				retryAfterTime = time.Time{}
			} else if t, err := http.ParseTime(ra); err == nil {
				retryAfterInt = 0
				retryAfterTime = t
			}

			if err != nil {
				return err
			}

			return nil
		}()

		if err != nil {
			return nil, err
		}

		if status == http.StatusTooManyRequests {
			if retryAfterInt != 0 {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("makeOneRequest got 429 with 'Retry-After' header, sleeping %ds before retry", retryAfterInt))
				if err := sleepContext(a.ctx, time.Duration(retryAfterInt)*time.Second); err != nil {
					if len(allItems) > 0 {
						return allItems, err
					}
					return nil, err
				}
			} else if retryAfterTime.Before(time.Now()) { 
				continue 
			} else if !retryAfterTime.IsZero() {
				retryUntilTime := time.Until(retryAfterTime).Seconds()
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("makeOneRequest got 429 with 'Retry-After' header with time %v, sleeping %vs before retry", retryAfterTime, retryUntilTime))
				if err := sleepContext(a.ctx, time.Duration(retryUntilTime)*time.Second); err != nil {
					if len(allItems) > 0 {
						return allItems, err
					}
					return nil, err
				}
			} else {
				a.conf.ClientOptions.OnWarning("makeOneRequest got 429 without 'Retry-After' header, sleeping 60s before retry")
				if err := sleepContext(a.ctx, 60*time.Second); err != nil {
					if len(allItems) > 0 {
						return allItems, err
					}
					return nil, err
				}
			}
			// Try again after waiting 'Retry-After' value
			continue
		}
		if status == http.StatusUnauthorized {
			// Clear the cached token in the ADAPTER and retry once
			a.tokenMu.Lock()
			a.oauthToken = ""
			a.tokenExpiry = time.Time{}
			a.tokenMu.Unlock()

			if retryCount < 3 {
				a.conf.ClientOptions.OnWarning("received 401, clearing token cache and trying again")
				retryCount++
				continue
			}
			err := fmt.Errorf("mimecast unauthorized: could not reauthenticate for API %s", api.key)
			if len(allItems) > 0 {
				return allItems, err
			}
			return nil, err
		}
		if status == http.StatusForbidden {
			err := fmt.Errorf("mimecast forbidden: %d\nRESPONSE: %s\nAPI '%s' will be disabled", status, string(respBody), api.key)
			a.conf.ClientOptions.OnError(err)
			api.SetInactive()
			// Since this API is now disabled, update api.since to prevent it from getting stuck
			// on this time window forever
			api.mu.Lock()
			api.since = cycleTime.Add(-overlapPeriod)
			api.mu.Unlock()
			if len(allItems) > 0 {
				return allItems, nil
			}
			return nil, nil
		}
		if status >= 500 && status < 600 {
			err := fmt.Errorf("mimecast server error: %d\nRESPONSE: %s", status, string(respBody))
			a.conf.ClientOptions.OnError(err)
			// We don't want this to be handled like an error
			// The hope is these errors are temporary
			if len(allItems) > 0 {
				return allItems, nil
			}
			return nil, nil
		}
		if status != http.StatusOK {
			err := fmt.Errorf("mimecast api non-200: %d\nRESPONSE: %s", status, string(respBody))
			a.conf.ClientOptions.OnError(err)
			if len(allItems) > 0 {
				return allItems, err
			}
			return nil, err
		}

		// Parse the response.
		var response ApiResponse
		err = json.Unmarshal(respBody, &response)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("mimecast api invalid json: %v", err))
			return nil, err
		}

		// Check for Mimecast-specific errors in the fail array
		if len(response.Fail) > 0 {
			var errorMessages []string
			for _, failure := range response.Fail {
				for _, errDetail := range failure.Errors {
					errorMessages = append(errorMessages, fmt.Sprintf("%s: %s (retryable: %v)", errDetail.Code, errDetail.Message, errDetail.Retryable))
				}
			}
			a.conf.ClientOptions.OnError(fmt.Errorf("mimecast api returned errors: %v", errorMessages))
			return nil, fmt.Errorf("mimecast api errors: %v", errorMessages)
		}

		// Collect items.
		items := response.Data
		var newItems []utils.Dict

		for _, item := range items {
			// Check if this item has nested structure (logType -> array of logs)
			// or if it's flat structure (item IS the log itself)
			// Known nested log types that contain arrays of logs
			knownLogTypes := []string{"attachmentLogs", "impersonationLogs", "urlLogs", "dlpLogs"}
			hasNestedStructure := false
			for _, logType := range knownLogTypes {
				if _, exists := item[logType]; exists {
					hasNestedStructure = true
					break
				}
			}

			if hasNestedStructure {
				for logType, logsInterface := range item {
					logs, ok := logsInterface.([]interface{})
					if !ok {
						// Skip non-array fields (like resultCount, metadata, etc.)
						continue
					}

					for _, logInterface := range logs {
						logMap, ok := logInterface.(map[string]interface{})
						if !ok {
							continue
						}

						// Handle deduplication FIRST
						if !a.processLogItem(api, logMap) {
							continue
						}

						// Create a new item with the log data plus the logType
						newItem := utils.Dict{
							"logType":   logType,
							"eventType": api.key,
						}

						// Copy all fields from the log
						for k, v := range logMap {
							newItem[k] = v
						}

						newItems = append(newItems, newItem)
					}
				}
			} else {
				// Handle deduplication FIRST
				if !a.processLogItem(api, item) {
					continue
				}

				newItem := utils.Dict{
					"logType":   api.key,
					"eventType": api.key,
				}

				for k, v := range item {
					newItem[k] = v
				}

				newItems = append(newItems, newItem)
			}
		}

		allItems = append(allItems, newItems...)

		// Check if we need to make another request.
		if response.Meta.Pagination.Next != "" {
			pageToken = response.Meta.Pagination.Next
		} else {
			break
		}
	}

	// Mark query as successful - we completed pagination successfully
	querySucceeded = true

	// Update api.since to the end time of this request window
	// This ensures we don't miss any events in future requests
	// ONLY update if query succeeded
	if querySucceeded {
		api.mu.Lock()
		api.since = cycleTime.Add(-overlapPeriod)
		api.mu.Unlock()
	}

	// Cull old dedupe entries - keep entries from the last lookback period
	// to handle duplicates during the overlap window
	// We can clean up regardless of query success
	cutoffTime := cycleTime.Add(-2 * overlapPeriod).Unix()
	api.mu.Lock()
	for k, v := range api.dedupe {
		if v < cutoffTime {
			delete(api.dedupe, k)
		}
	}

	api.mu.Unlock()

	return allItems, nil
}

func (a *MimecastAdapter) processLogItem(api *API, logMap map[string]interface{}) bool {
	var dedupeID string

	// Determine the deduplication ID
	if api.idField != "" {
		// Try to use the specified ID field
		if idVal, ok := logMap[api.idField]; ok {
			if id, ok := idVal.(string); ok && id != "" {
				dedupeID = id
			}
		}
	}

	// If no ID field specified or ID is empty, generate hash from log content
	if dedupeID == "" {
		dedupeID = a.generateLogHash(logMap)
	}

	// Check for duplicates
	api.mu.Lock()
	if _, exists := api.dedupe[dedupeID]; exists {
		api.mu.Unlock()
		return false // Skip duplicate
	}

	dedupeTimestamp := time.Now().Unix() // Default fallback
	if timeVal, ok := logMap[api.timeField]; ok {
		if timestamp, ok := timeVal.(string); ok {
			if epoch, err := time.Parse(time.RFC3339, timestamp); err == nil {
				dedupeTimestamp = epoch.Unix()
			}
		}
	}
	api.dedupe[dedupeID] = dedupeTimestamp
	api.mu.Unlock()

	return true // Include this item
}

func (a *MimecastAdapter) generateLogHash(logMap map[string]interface{}) string {
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

func (a *MimecastAdapter) submitEvents(events []utils.Dict) {
	for _, item := range events {
		msg := &protocol.DataMessage{
			JsonPayload: item,
			TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}
		if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
			if err == uspclient.ErrorBufferFull {
				a.conf.ClientOptions.OnWarning("stream falling behind")
				if err := a.uspClient.Ship(msg, 1*time.Hour); err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
					a.Close()
					return
				}
			}
		}
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
