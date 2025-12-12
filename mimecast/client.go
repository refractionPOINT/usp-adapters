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
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
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

	oauthToken        string
	tokenExpiry       time.Time
	tokenMu           sync.Mutex
	tokenRefreshGroup singleflight.Group
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

type MimecastConfig struct {
	ClientOptions         uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ClientId              string                  `json:"client_id" yaml:"client_id"`
	ClientSecret          string                  `json:"client_secret" yaml:"client_secret"`
	BaseURL               string                  `json:"base_url,omitempty" yaml:"base_url,omitempty"`
	InitialLookback       time.Duration           `json:"initial_lookback,omitempty" yaml:"initial_lookback,omitempty"` // eg, 24h, 30m, 168h, 1h30m
	MaxConcurrentWorkers int `json:"max_concurrent_workers,omitempty" yaml:"max_concurrent_workers,omitempty"`
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

	// InitialLookback defaults to zero (current time, no lookback)

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
	a.tokenMu.Unlock()

	// Need to get a new token - refreshOAuthToken handles its own locking
	return a.refreshOAuthToken(ctx)
}

// refreshOAuthToken gets a new OAuth 2.0 access token using singleflight to prevent thundering herd
func (a *MimecastAdapter) refreshOAuthToken(ctx context.Context) (map[string]string, error) {
	// singleflight ensures only one token refresh happens at a time
	result, err, _ := a.tokenRefreshGroup.Do("token", func() (interface{}, error) {
		// Check if token was refreshed while waiting for singleflight
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
		a.tokenMu.Unlock()

		tokenURL := a.conf.BaseURL + "/oauth/token"

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
			gracePeriod = 0
		}

		a.tokenMu.Lock()
		a.oauthToken = tokenResp.AccessToken
		a.tokenExpiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn-gracePeriod) * time.Second)
		a.tokenMu.Unlock()

		return map[string]string{
			"Authorization": fmt.Sprintf("Bearer %s", tokenResp.AccessToken),
			"Accept":        "application/json",
			"Content-Type":  "application/json",
		}, nil
	})

	if err != nil {
		return nil, err
	}
	return result.(map[string]string), nil
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
		if api.IsActive() {
			return false
		}
	}
	a.conf.ClientOptions.OnWarning("all apis are disabled due to forbidden messages. shutting down")
	return true
}

func (a *MimecastAdapter) RunFetchLoop(apis []*API) {
	ticker := time.NewTicker(queryInterval * time.Second)
	defer ticker.Stop()
	defer a.fetchOnce.Do(func() { close(a.chFetchLoop) })

	for {
		select {
		case <-a.ctx.Done():
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", a.conf.BaseURL))
			return
		case <-ticker.C:
			if err := a.runOneCycle(apis); err != nil {
				if a.ctx.Err() != nil {
					return // Context cancelled, exit gracefully
				}
				// All APIs disabled, shutdown
				a.cancel()
				return
			}
		}
	}
}

func (a *MimecastAdapter) runOneCycle(apis []*API) error {
	if a.shouldShutdown(apis) {
		return fmt.Errorf("all APIs disabled")
	}

	cycleTime := time.Now()
	g, ctx := errgroup.WithContext(a.ctx)
	g.SetLimit(a.conf.MaxConcurrentWorkers)

	// Buffered channel to collect results from workers
	resultCh := make(chan []utils.Dict, len(apis))

	// Launch fetch workers
	for _, api := range apis {
		if !api.IsActive() {
			continue
		}
		api := api // capture for goroutine
		g.Go(func() error {
			items, err := a.makeOneRequest(api, cycleTime)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("%s fetch failed: %w", api.key, err))
				return nil // Don't fail the whole group for one API error
			}
			if len(items) > 0 {
				select {
				case resultCh <- items:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}

	// Close resultCh when all fetchers are done
	go func() {
		g.Wait()
		close(resultCh)
	}()

	// Ship results as they arrive
	for items := range resultCh {
		a.submitEvents(items)
	}

	return g.Wait()
}

func (a *MimecastAdapter) makeOneRequest(api *API, cycleTime time.Time) ([]utils.Dict, error) {
	var allItems []utils.Dict
	var start string
	var retryCount int
	var retryableErrorCount int
	var retryCount5xx int
	retryDeadline := time.Now().Add(1 * time.Hour)

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
			if err != nil {
				return err
			}

			status = resp.StatusCode
			ra := resp.Header.Get("Retry-After")

			if ra == "" {
				retryAfterInt = 0
				retryAfterTime = time.Time{}
			} else if secs, parseErr := strconv.Atoi(ra); parseErr == nil {
				retryAfterInt = secs
				retryAfterTime = time.Time{}
			} else if t, parseErr := http.ParseTime(ra); parseErr == nil {
				retryAfterInt = 0
				retryAfterTime = t
			}

			return nil
		}()

		if err != nil {
			return nil, err
		}

		if status == http.StatusTooManyRequests {
			// Check if we've exceeded the 1 hour retry deadline
			if time.Now().After(retryDeadline) {
				err := fmt.Errorf("mimecast rate limit: exceeded 1 hour retry deadline for API %s", api.key)
				if len(allItems) > 0 {
					return allItems, err
				}
				return nil, err
			}
			if retryAfterInt != 0 {
				// Retry-After header with integer seconds value
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("makeOneRequest got 429 with 'Retry-After' header, sleeping %ds before retry", retryAfterInt))
				if err := sleepContext(a.ctx, time.Duration(retryAfterInt)*time.Second); err != nil {
					if len(allItems) > 0 {
						return allItems, err
					}
					return nil, err
				}
			} else if !retryAfterTime.IsZero() {
				// Retry-After header with HTTP-date value
				if retryAfterTime.Before(time.Now()) {
					// Retry-After time already passed, wait a minimum of 1 second to avoid tight loop
					if err := sleepContext(a.ctx, 1*time.Second); err != nil {
						if len(allItems) > 0 {
							return allItems, err
						}
						return nil, err
					}
				} else {
					retryDuration := time.Until(retryAfterTime)
					a.conf.ClientOptions.OnWarning(fmt.Sprintf("makeOneRequest got 429 with 'Retry-After' header with time %v, sleeping %v before retry", retryAfterTime, retryDuration))
					if err := sleepContext(a.ctx, retryDuration); err != nil {
						if len(allItems) > 0 {
							return allItems, err
						}
						return nil, err
					}
				}
			} else {
				// No Retry-After header, use default 60s wait
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
			// Check if we've exceeded the 1 hour retry deadline
			if time.Now().After(retryDeadline) {
				err := fmt.Errorf("mimecast server error: %d after 1h retries\nRESPONSE: %s", status, string(respBody))
				a.conf.ClientOptions.OnError(err)
				if len(allItems) > 0 {
					return allItems, err
				}
				return nil, err
			}
			// Exponential backoff: 30s, 60s, 90s, ... capped at 5 minutes
			backoff := time.Duration(retryCount5xx+1) * 30 * time.Second
			if backoff > 5*time.Minute {
				backoff = 5 * time.Minute
			}
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("mimecast server error %d, retrying in %v", status, backoff))
			if err := sleepContext(a.ctx, backoff); err != nil {
				if len(allItems) > 0 {
					return allItems, err
				}
				return nil, err
			}
			retryCount5xx++
			continue
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
			allRetryable := true
			
			for _, failure := range response.Fail {
				for _, errDetail := range failure.Errors {
					errorMessages = append(errorMessages, fmt.Sprintf("%s: %s (retryable: %v)", 
						errDetail.Code, errDetail.Message, errDetail.Retryable))
					if !errDetail.Retryable {
						allRetryable = false
					}
				}
			}
			
			if allRetryable {
				retryableErrorCount++

				if retryableErrorCount >= 5 {
					a.conf.ClientOptions.OnError(fmt.Errorf("max retries exceeded for retryable errors: %v", errorMessages))
					if len(allItems) > 0 {
						return allItems, fmt.Errorf("mimecast api errors after %d retries: %v", retryableErrorCount, errorMessages)
					}
					return nil, fmt.Errorf("mimecast api errors after %d retries: %v", retryableErrorCount, errorMessages)
				}

				// Log warning and retry
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("mimecast api returned retryable errors: %v, retrying", errorMessages))
				
				if err := sleepContext(a.ctx, 5*time.Second); err != nil {
					if len(allItems) > 0 {
						return allItems, err
					}
					return nil, err
				}
				continue  // Retry the request
			}
			
			// Non-retryable error - fail
			a.conf.ClientOptions.OnError(fmt.Errorf("mimecast api returned non-retryable errors: %v", errorMessages))
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

	// Update api.since to the end time of this request window
	// This ensures we don't miss any events in future requests	
	api.mu.Lock()
	api.since = cycleTime.Add(-overlapPeriod)
	api.mu.Unlock()

	// Cull old dedupe entries - keep entries from the last lookback period
	// to handle duplicates during the overlap window
	// We can clean up regardless of query success
	cutoffTime := cycleTime.Add(-1 * time.Hour).Unix()
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

	// Build deterministic string representation using JSON for complex values
	var buf bytes.Buffer
	for _, k := range keys {
		v := logMap[k]
		// Use JSON marshaling for deterministic representation of complex types
		valueBytes, err := json.Marshal(v)
		if err != nil {
			// Fallback to fmt if JSON fails
			fmt.Fprintf(&buf, "%s:%v|", k, v)
		} else {
			fmt.Fprintf(&buf, "%s:%s|", k, valueBytes)
		}
	}

	hash := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(hash[:])
}

func (a *MimecastAdapter) submitEvents(events []utils.Dict) {
	for _, item := range events {
		// Check if we're shutting down
		select {
		case <-a.ctx.Done():
			return
		default:
		}

		msg := &protocol.DataMessage{
			JsonPayload: item,
			TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}
		if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
			if err == uspclient.ErrorBufferFull {
				a.conf.ClientOptions.OnWarning("stream falling behind")
				if err := a.uspClient.Ship(msg, 1*time.Hour); err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
					// Signal shutdown via context cancellation instead of calling Close()
					// to avoid deadlock (Close waits for fetch loop which spawned us)
					a.cancel()
					return
				}
				continue
			}
			// Handle non-ErrorBufferFull errors
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
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
