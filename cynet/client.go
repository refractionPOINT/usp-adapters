package usp_cynet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	queryInterval        = 60
	pageSize             = 100
	authEndpoint         = "/api/account/token"
	alertsEndpoint       = "/api/alerts/bulk"
	maxConsecutiveErrors = 5
)

type CynetConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	// (AccessKey + SecretKey) OR (User + Password)
	AccessKey string `json:"access_key,omitempty" yaml:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty" yaml:"secret_key,omitempty"`
	User      string `json:"user,omitempty" yaml:"user,omitempty"`
	Password  string `json:"password,omitempty" yaml:"password,omitempty"`
	SiteID    string `json:"site_id" yaml:"site_id"`
	URL       string `json:"url,omitempty" yaml:"url,omitempty"` // https://DOMAIN.api.cynet.com
}

type CynetAdapter struct {
	conf        CynetConfig
	uspClient   *uspclient.Client
	httpClient  *http.Client
	chStopped   chan struct{}
	chFetchLoop chan struct{}
	closeOnce   sync.Once
	fetchOnce   sync.Once
	ctx         context.Context
	cancel      context.CancelFunc

	// Authentication
	accessToken string

	// Deduplication
	fnvHasher    hash.Hash64
	alertsDedupe map[string]int64

	// State tracking
	since             time.Time
	consecutiveErrors int
}

// Authentication structures
type AuthRequest struct {
	User     string `json:"user_name"`
	Password string `json:"password"`
}

type AuthResponse struct {
	AccessToken string `json:"access_token"`
}

// Alerts response structure
type AlertsResponse struct {
	SyncTimeUtc string       `json:"SyncTimeUtc"`
	Entities    []utils.Dict `json:"Entities"`
}

func NewCynetAdapter(ctx context.Context, conf CynetConfig) (*CynetAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &CynetAdapter{
		conf:         conf,
		alertsDedupe: make(map[string]int64),
		since:        time.Now().Add(-1 * 24 * time.Hour).UTC(), // On start, pull the last day's alerts
	}

	// Logs don't have IDs, this generates an "ID" by hashing the event.
	a.fnvHasher = fnv.New64a()

	rootCtx, cancel := context.WithCancel(ctx)
	a.ctx = rootCtx
	a.cancel = cancel

	var err error
	a.uspClient, err = uspclient.NewClient(rootCtx, conf.ClientOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create USP client: %w", err)
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
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 2,
		},
	}

	a.chStopped = make(chan struct{})
	a.chFetchLoop = make(chan struct{})

	// Get initial token
	if err := a.authenticate(); err != nil {
		a.Close()
		return nil, nil, fmt.Errorf("initial authentication failed: %w", err)
	}

	go a.fetchEvents()

	return a, a.chStopped, nil
}

func (c *CynetConfig) Validate() error {
	var Keys bool
	var UserPass bool

	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	// Verify a user value was provided
	if c.AccessKey == "" && c.User == "" {
		return errors.New("missing access_key or user")
	}
	// Verify a password value was provided
	if c.SecretKey == "" && c.Password == "" {
		return errors.New("missing secret_key or password")
	}
	// Verify the AccessKey/SecretKey pair is complete
	if c.AccessKey != "" && c.SecretKey != "" {
		Keys = true
	}
	// Verify the User/Password pair is complete
	if c.User != "" && c.Password != "" {
		UserPass = true
	}
	// If neither pair is complete, throw error
	if !Keys && !UserPass {
		return errors.New("missing access_key/secret_key or user/password pairs")
	}
	if c.SiteID == "" {
		return errors.New("missing site id")
	}
	if c.URL == "" {
		return errors.New("missing URL")
	}
	return nil
}

func (a *CynetAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing Cynet adapter")
	var err1, err2 error
	a.closeOnce.Do(func() {
		a.cancel()
		select {
		case <-a.chFetchLoop:
		case <-time.After(10 * time.Second):
			a.conf.ClientOptions.OnWarning("timeout waiting for fetch loop to exit; proceeding with cleanup")
			a.fetchOnce.Do(func() { close(a.chFetchLoop) })
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

func (a *CynetAdapter) authenticate() error {
	// a.conf.ClientOptions.DebugLog("authenticating with Cynet API")

	authURL := fmt.Sprintf("%s%s", a.conf.URL, authEndpoint)

	var authReq AuthRequest

	// If both AccessKey/SecretKey and User/Password pairs are provided, default to AccessKey/SecretKey
	if a.conf.AccessKey != "" && a.conf.SecretKey != "" {
		authReq = AuthRequest{
			User:     a.conf.AccessKey,
			Password: a.conf.SecretKey,
		}
	} else {
		authReq = AuthRequest{
			User:     a.conf.User,
			Password: a.conf.Password,
		}
	}

	jsonData, err := json.Marshal(authReq)
	if err != nil {
		return fmt.Errorf("failed to marshal auth request: %w", err)
	}

	ctx, cancel := context.WithTimeout(a.ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", authURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create auth request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("auth request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read auth response: %w", err)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("authentication failed: invalid credentials (401 Unauthorized)")
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("authentication failed with status %d: %s", resp.StatusCode, string(body))
	}

	var authResp AuthResponse
	if err := json.Unmarshal(body, &authResp); err != nil {
		return fmt.Errorf("failed to unmarshal auth response: %w", err)
	}

	if authResp.AccessToken == "" {
		return fmt.Errorf("received empty access token from auth endpoint")
	}

	a.accessToken = authResp.AccessToken

	return nil
}

func (a *CynetAdapter) fetchEvents() {
	defer func() {
		a.fetchOnce.Do(func() { close(a.chFetchLoop) })
	}()

	ticker := time.NewTicker(queryInterval * time.Second)
	defer ticker.Stop()

	// Initial fetch
	a.runFetchCycle()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.runFetchCycle()
		}
	}
}

func (a *CynetAdapter) runFetchCycle() {
	// a.conf.ClientOptions.DebugLog("starting fetch cycle")

	// Fetch alerts
	alerts, err := a.fetchAlerts()
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("failed to fetch alerts: %w", err))
		a.consecutiveErrors++
		if a.consecutiveErrors >= maxConsecutiveErrors {
			a.conf.ClientOptions.OnError(fmt.Errorf("shutting down after %d consecutive fetch failures", maxConsecutiveErrors))
			go a.Close()
			return
		}
		return
	}

	// Reset error count on successful fetch
	a.consecutiveErrors = 0

	// Submit events
	if len(alerts) > 0 {
		//a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetched %d new alerts", len(alerts)))
		a.submitEvents(alerts)
	} else {
		//a.conf.ClientOptions.DebugLog("no new alerts found")
	}
}

func (a *CynetAdapter) fetchAlerts() ([]utils.Dict, error) {
	var allAlerts []utils.Dict
	var latestTimestamp time.Time
	var offset int

	for {
		select {
		case <-a.ctx.Done():
			return nil, a.ctx.Err()
		default:
		}

		// Clean up old dedupe entries
		a.cleanupDedupe()

		// Build URL with pagination
		u, err := url.Parse(fmt.Sprintf("%s%s", a.conf.URL, alertsEndpoint))

		if err != nil {
			return nil, fmt.Errorf("failed to parse alerts URL: %w", err)
		}

		q := u.Query()

		//a.conf.ClientOptions.DebugLog(fmt.Sprintf("Since: %s", a.since.Format("2006-01-02 15:04:05")))
		q.Set("LastSeen", a.since.Format("2006-01-02 15:04:05"))
		q.Set("Limit", strconv.Itoa(pageSize))
		q.Set("Offset", strconv.Itoa(offset))
		u.RawQuery = q.Encode()

		// Make request
		resp, err := a.makeRequest(u.String())
		if err != nil {
			return nil, err
		}

		// Process alerts
		newAlerts, latest := a.processAlerts(resp)
		allAlerts = append(allAlerts, newAlerts...)

		if latest.After(latestTimestamp) {
			latestTimestamp = latest
		}

		// Check if there are more pages
		if len(resp.Entities) < pageSize || len(resp.Entities) == 0 {
			break
		}

		offset += len(resp.Entities)

		// Rate limiting between pages
		select {
		case <-a.ctx.Done():
			return nil, a.ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Small delay between pages to avoid hitting rate limits
		}
	}

	// Update last fetch time
	if latestTimestamp.After(a.since) {
		a.since = latestTimestamp
	}

	return allAlerts, nil
}

func (a *CynetAdapter) makeRequest(url string) (*AlertsResponse, error) {
	var lastErr error
	retryCount := 0
	maxRetries := 3

	ctx, cancel := context.WithTimeout(a.ctx, 180*time.Second)
	defer cancel()

	for retryCount < maxRetries {
		select {
		case <-a.ctx.Done():
			return nil, a.ctx.Err()
		default:
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("access_token", a.accessToken)
		req.Header.Set("client_id", a.conf.SiteID)
		req.Header.Set("Accept", "application/json")

		resp, err := a.httpClient.Do(req)
		if err != nil {
			// Check if it's a context error
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				lastErr = err
				retryCount++
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("request timeout/cancelled (attempt %d/%d): %v", retryCount, maxRetries, err))

				// Wait before retry with exponential backoff
				backoff := time.Duration(retryCount) * time.Second
				select {
				case <-a.ctx.Done():
					return nil, a.ctx.Err()
				case <-time.After(backoff):
					continue
				}
			}
			return nil, fmt.Errorf("request failed: %w", err)
		}

		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}

		// Handle different status codes
		switch resp.StatusCode {
		case http.StatusOK:
			var alertsResp AlertsResponse
			if err := json.Unmarshal(body, &alertsResp); err != nil {
				return nil, fmt.Errorf("failed to unmarshal alerts response: %w", err)
			}
			return &alertsResp, nil

		case http.StatusUnauthorized:
			// Try to refresh token once
			a.conf.ClientOptions.OnWarning("received 401, attempting to refresh token")
			if err := a.authenticate(); err != nil {
				return nil, fmt.Errorf("re-authentication failed: %w", err)
			}
			// Retry the request with new token
			retryCount++
			continue

		case http.StatusForbidden:
			// Access forbidden - shut down adapter
			a.conf.ClientOptions.OnError(fmt.Errorf("received 403 Forbidden - access denied to alerts API"))
			go a.Close()
			return nil, fmt.Errorf("access forbidden (403) - shutting down adapter")

		case http.StatusTooManyRequests:
			// Handle rate limiting
			retryAfter := resp.Header.Get("Retry-After")
			waitDuration := 60 * time.Second // Default wait

			if retryAfter != "" {
				if seconds, err := strconv.Atoi(retryAfter); err == nil {
					waitDuration = time.Duration(seconds) * time.Second
				}
			}

			a.conf.ClientOptions.OnWarning(fmt.Sprintf("rate limited (429), waiting %v before retry", waitDuration))

			select {
			case <-a.ctx.Done():
				return nil, a.ctx.Err()
			case <-time.After(waitDuration):
				retryCount++
				continue
			}

		case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable:
			// Server errors - retry with exponential backoff
			lastErr = fmt.Errorf("server error %d: %s", resp.StatusCode, string(body))
			a.conf.ClientOptions.DebugLog(lastErr.Error())
			retryCount++

			if retryCount < maxRetries {
				backoff := time.Duration(retryCount*retryCount) * time.Second
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("server error %d, retrying in %v (attempt %d/%d)",
					resp.StatusCode, backoff, retryCount, maxRetries))

				select {
				case <-a.ctx.Done():
					return nil, a.ctx.Err()
				case <-time.After(backoff):
					continue
				}
			}

		default:
			return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("request failed after %d retries: %w", maxRetries, lastErr)
	}

	return nil, fmt.Errorf("request failed after %d retries", maxRetries)
}

func (a *CynetAdapter) processAlerts(resp *AlertsResponse) ([]utils.Dict, time.Time) {
	var newAlerts []utils.Dict
	var latestTime time.Time

	// Parse the SyncTimeUtc
	syncTime, err := time.Parse(time.RFC3339Nano, resp.SyncTimeUtc)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("failed to parse SyncTimeUtc '%s': %v", resp.SyncTimeUtc, err))
		syncTime = a.since
	}

	for _, entity := range resp.Entities {
		// Generate hash ID for the entire entity
		a.fnvHasher.Reset()
		b, err := json.Marshal(entity)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("failed to marshal alert entity for hashing: %v", err))
			continue
		}
		a.fnvHasher.Write(b)
		entityID := fmt.Sprintf("%d", a.fnvHasher.Sum64())

		// Check if we've seen this exact entity before
		if _, seen := a.alertsDedupe[entityID]; seen {
			continue
		}

		entity["event-type"] = "cynet-alert"

		// Track for deduplication using sync time
		a.alertsDedupe[entityID] = time.Now().Unix()

		newAlerts = append(newAlerts, entity)

		if syncTime.After(latestTime) {
			latestTime = syncTime
		}
	}

	return newAlerts, latestTime
}

func (a *CynetAdapter) cleanupDedupe() {
	cutoff := a.since.Add(-1 * queryInterval).Unix() // Only keep dedupes that are within one queryInterval of the last known received timestamp

	for id, timestamp := range a.alertsDedupe {
		if timestamp < cutoff {
			delete(a.alertsDedupe, id)
		}
	}

	//a.conf.ClientOptions.DebugLog(fmt.Sprintf("dedupe cleanup: %d entries remaining", len(a.alertsDedupe)))
}

func (a *CynetAdapter) submitEvents(events []utils.Dict) {
	for _, item := range events {
		msg := &protocol.DataMessage{
			JsonPayload: item,
			TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}

		if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
			if err == uspclient.ErrorBufferFull {
				a.conf.ClientOptions.OnWarning("USP stream falling behind, waiting for buffer space")
				if err := a.uspClient.Ship(msg, 1*time.Hour); err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("failed to ship event after extended wait: %v", err))
					go a.Close()
					return
				}
			} else {
				a.conf.ClientOptions.OnError(fmt.Errorf("failed to ship event: %v", err))
			}
		}
	}

	//a.conf.ClientOptions.DebugLog(fmt.Sprintf("submitted %d events to USP", len(events)))
}
