package usp_cylance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	queryInterval             = 60
	consecutiveAuthFailsLimit = 10
	pageLimit                 = 100
	defaultLoggingBaseURL     = "https://protectapi.cylance.com"
	authEndpoint              = "/auth/v2/token"
	detectionsEndpoint        = "/detections/v2"
	threatsEndpoint           = "/threats/v2"
	memoryProtectionEndpoint  = "/memoryprotection/v2"
)

type CylanceConfig struct {
	ClientOptions  uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	TenantID       string                  `json:"tenant_id" yaml:"tenant_id"`
	AppID          string                  `json:"app_id" yaml:"app_id"`
	AppSecret      string                  `json:"app_secret" yaml:"app_secret"`
	LoggingBaseURL string                  `json:"logging_base_url" yaml:"logging_base_url"`
}

type CylanceAdapter struct {
	conf       CylanceConfig
	uspClient  *uspclient.Client
	httpClient *http.Client
	chStopped  chan struct{}

	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc

	detectionDedupe        map[string]int64
	threatDedupe           map[string]int64
	memoryProtectionDedupe map[string]int64

	accessToken         string
	tokenExpiresAt      time.Time
	refreshFailLimitMet bool
}

type CylanceEventsResponse struct {
	Events []utils.Dict `json:"events"`
}

func NewCylanceAdapter(conf CylanceConfig) (*CylanceAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}
	a := &CylanceAdapter{
		conf:                   conf,
		detectionDedupe:        make(map[string]int64),
		threatDedupe:           make(map[string]int64),
		memoryProtectionDedupe: make(map[string]int64),
		refreshFailLimitMet:    false,
	}

	rootCtx, cancel := context.WithCancel(context.Background())
	a.ctx = rootCtx
	a.cancel = cancel

	var err error
	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
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

func (c *CylanceConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.TenantID == "" {
		return errors.New("missing tenant id")
	}
	if c.AppID == "" {
		return errors.New("missing app id")
	}
	if c.AppSecret == "" {
		return errors.New("missing app secret")
	}
	if c.LoggingBaseURL == "" {
		c.LoggingBaseURL = defaultLoggingBaseURL
	}
	return nil
}

func (a *CylanceAdapter) Close() error {
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

func (a *CylanceAdapter) fetchEvents() {
	since := map[string]time.Time{
		"detections":        time.Now().Add(-1 * queryInterval * time.Minute),
		"threats":           time.Now().Add(-1 * queryInterval * time.Minute),
		"memoryProtections": time.Now().Add(-1 * queryInterval * time.Minute),
	}

	apis := []struct {
		key       string
		endpoint  string
		idField   string
		timeField string
		detailFn  func(ctx context.Context, id string) (*CylanceEventsResponse, error)
		dedupe    map[string]int64
	}{
		{
			key:       "detections",
			endpoint:  detectionsEndpoint,
			idField:   "Id",
			timeField: "ReceivedTime",
			detailFn: func(ctx context.Context, id string) (*CylanceEventsResponse, error) {
				return a.requestDetectionDetails(ctx, id)
			},
			dedupe: a.detectionDedupe,
		},
		{
			key:       "threats",
			endpoint:  threatsEndpoint,
			idField:   "sha256",
			timeField: "dateDetected",
			detailFn: func(ctx context.Context, id string) (*CylanceEventsResponse, error) {
				return a.requestThreatDetails(ctx, id)
			},
			dedupe: a.threatDedupe,
		},
		{
			key:       "memoryProtection",
			endpoint:  memoryProtectionEndpoint,
			idField:   "device_image_file_event_id",
			timeField: "created",
			detailFn:  nil,
			dedupe:    a.memoryProtectionDedupe,
		},
	}

	ticker := time.NewTicker(queryInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", a.conf.LoggingBaseURL))
			return
		case <-ticker.C:

			allItems := []utils.Dict{}

			for _, api := range apis {
				pageUrl := fmt.Sprintf("%s%s", a.conf.LoggingBaseURL, api.endpoint)
				items, newSince, err := a.getEventsRequest(a.ctx, pageUrl, api.key, api.dedupe, api.idField, api.timeField, since[api.key])
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("%s fetch failed: %w", since[api.key], err))
					continue
				}
				since[api.key] = newSince
				allItems = append(allItems, items...)

				if api.detailFn != nil && !a.refreshFailLimitMet {
					for _, event := range items {
						rawID, ok := event[api.idField]
						if !ok {
							a.conf.ClientOptions.OnWarning(fmt.Sprintf("no %s field on %s event: %v", api.idField, api.key, event))
							continue
						}
						id, ok := rawID.(string)
						if !ok {
							a.conf.ClientOptions.OnWarning(fmt.Sprintf("%s field is not a string on %s event: %v", api.idField, api.key, event))
							continue
						}
						response, err := api.detailFn(a.ctx, id)
						if err != nil {
							if a.refreshFailLimitMet {
								break
							}
							a.conf.ClientOptions.OnError(fmt.Errorf("%s details fetch failed: %w", api.key, err))
							continue
						}
						allItems = append(allItems, response.Events...)
					}
				}
			}

			if len(allItems) > 0 {
				a.submitEvents(allItems)
			}

			if a.refreshFailLimitMet {
				a.conf.ClientOptions.OnError(errors.New("authentication failed too many times; shutting down"))
				_ = a.Close()
				return
			}
		}
	}
}

func (a *CylanceAdapter) getToken(ctx context.Context) (string, error) {
	if a.refreshFailLimitMet {
		return "", fmt.Errorf("AuthFailLimit met")
	}

	if a.accessToken == "" || time.Until(a.tokenExpiresAt) < queryInterval*time.Second {
		if err := a.refreshToken(ctx); err != nil {
			return "", err
		}
	}

	return a.accessToken, nil
}

func (a *CylanceAdapter) tokenError(errorMessage string, refreshFails int, sleepTime time.Duration) int {
	refreshFails++
	a.conf.ClientOptions.OnError(errors.New(errorMessage))
	if refreshFails > consecutiveAuthFailsLimit {
		a.refreshFailLimitMet = true
		return refreshFails
	}
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("attempting to refresh token in %s", sleepTime.String()))
	if errSleep := sleepContext(a.ctx, sleepTime); errSleep != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("could not sleep: %v", errSleep))
		return refreshFails
	}
	return refreshFails
}

func (a *CylanceAdapter) refreshToken(ctx context.Context) error {
	refreshFails := 0

	for {
		if a.refreshFailLimitMet {
			return errors.New("token refresh failure limit met")
		}

		if refreshFails > consecutiveAuthFailsLimit/2 && refreshFails <= consecutiveAuthFailsLimit {
			_ = a.tokenError("high failure rate to refresh token", refreshFails, 5*time.Minute)
		}

		jwtExpiry := time.Now().UTC().Add(30 * time.Minute).Unix()
		jti := uuid.New().String()

		claims := jwt.MapClaims{
			"exp": jwtExpiry,
			"iat": time.Now().UTC().Unix(),
			"iss": "http://cylance.com",
			"sub": a.conf.AppID,
			"tid": a.conf.TenantID,
			"jti": jti,
			"scp": []string{"opticsdetect:list", "opticsdetect:read", "device:list", "threat:read", "memoryprotection:list"},
		}

		jwtToken := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		signedJwtToken, err := jwtToken.SignedString([]byte(a.conf.AppSecret))
		if err != nil {
			refreshFails = a.tokenError(fmt.Sprintf("could not sign jwt token: %v", err), refreshFails, 10*time.Second)
			continue
		}
		body := map[string]string{"auth_token": signedJwtToken}
		jsonBody, err := json.Marshal(body)
		if err != nil {
			refreshFails = a.tokenError(fmt.Sprintf("could not marshal token request body: %v", err), refreshFails, 10*time.Second)
			continue
		}

		var respBody []byte
		var status int

		errString := func() string {
			refreshCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(refreshCtx, "POST", fmt.Sprintf("%s%s", a.conf.LoggingBaseURL, authEndpoint), bytes.NewBuffer(jsonBody))
			if err != nil {
				return fmt.Sprintf("error while creating new http post request for token refresh: %v", err)
			}
			req.Header.Set("Content-Type", "application/json; charset=utf-8")

			resp, err := a.httpClient.Do(req)
			if err != nil {
				return fmt.Sprintf("cylance auth api request error: %v", err)
			}
			defer resp.Body.Close()
			respBody, err = io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Sprintf("cylance auth api read error: %v", err)
			}
			status = resp.StatusCode
			return ""
		}()
		if errString != "" {
			refreshFails = a.tokenError(errString, refreshFails, 60*time.Second)
			continue
		}

		if status == http.StatusTooManyRequests {
			a.conf.ClientOptions.OnWarning("refreshToken got 429, sleeping 60s before retry")
			if err := sleepContext(a.ctx, 60*time.Second); err != nil {
				a.conf.ClientOptions.OnWarning("failed to sleep during refresh")
				return err
			}
			continue
		}
		if status != http.StatusOK {
			refreshFails = a.tokenError(fmt.Sprintf("cylance auth api non-200: %d\nRESPONSE %s", status, string(respBody)), refreshFails, 60*time.Second)
			continue
		}

		var response struct {
			AccessToken string `json:"access_token"`
			ExpiresAt   int64  `json:"expires_at"`
		}
		err = json.Unmarshal(respBody, &response)
		if err != nil {
			refreshFails = a.tokenError(fmt.Sprintf("cylance auth api invalid json: %v", err), refreshFails, 10*time.Second)
			continue
		}

		a.accessToken = response.AccessToken
		a.tokenExpiresAt = time.Unix(response.ExpiresAt, 0).UTC()
		refreshFails = 0

		return nil
	}
}

func (a *CylanceAdapter) submitEvents(items []utils.Dict) {
	for _, item := range items {
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

func (a *CylanceAdapter) doWithRetry(ctx context.Context, url string, apiName string) (*CylanceEventsResponse, error) {
	for {
		var respBody []byte
		var status int

		accessToken, err := a.getToken(ctx)
		if err != nil {
			return nil, err
		}

		err = func() error {
			loopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(loopCtx, "GET", url, nil)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("cylance %s api request error: %v", apiName, err))
				return err
			}

			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))
			req.Header.Set("Accept", "application/json")
			resp, err := a.httpClient.Do(req)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("cylance %s api do error: %v", apiName, err))
				return err
			}

			defer resp.Body.Close()

			respBody, err = io.ReadAll(resp.Body)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("cylance %s api read error: %v", apiName, err))
				return err
			}
			status = resp.StatusCode
			return nil
		}()
		if err != nil {
			return nil, err
		}

		if status == http.StatusTooManyRequests {
			a.conf.ClientOptions.OnWarning("getEventsRequest got 429, sleeping 60s before retry")
			if err := sleepContext(a.ctx, 60*time.Second); err != nil {
				return nil, err
			}
			continue
		}
		if status != http.StatusOK {
			if status == http.StatusUnauthorized {
				a.accessToken = ""
				a.tokenExpiresAt = time.Now()
				continue
			}
			a.conf.ClientOptions.OnError(fmt.Errorf("cylance %s api non-200: %d\nRESPONSE %s", apiName, status, string(respBody)))
			return nil, fmt.Errorf("cylance %s api non-200: %d\nRESPONSE %s", apiName, status, string(respBody))
		}

		var response CylanceEventsResponse

		err = json.Unmarshal(respBody, &response)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("cylance %s api invalid json: %v", apiName, err))
			return nil, err
		}

		return &response, nil
	}
}

func (a *CylanceAdapter) getEventsRequest(ctx context.Context, pageUrl string, apiName string, apiDedupe map[string]int64, apiIDField string, apiTimeField string, since time.Time) ([]utils.Dict, time.Time, error) {
	var allItems []utils.Dict
	lastDetectionTime := since
	page := 1
	pageSize := pageLimit

	defer func() {
		for k, v := range apiDedupe {
			if v < since.Unix() {
				delete(apiDedupe, k)
			}
		}
	}()

	for {
		url := fmt.Sprintf("%s?page=%d&page_size=%d&start=%s", pageUrl, page, pageSize, lastDetectionTime.UTC().Format(time.RFC3339))
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s", url))

		response, err := a.doWithRetry(ctx, url, apiName)
		if err != nil {
			return nil, lastDetectionTime, err
		}

		var newItems []utils.Dict
		for _, event := range response.Events {
			id, ok := event[apiIDField].(string)
			if !ok {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("event id not a string: %s", event))
				continue
			}
			createdAtStr, ok := event[apiTimeField].(string)
			if !ok {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("event time not a string: %s", event))
				continue
			}

			if _, seen := apiDedupe[id]; !seen {
				CreatedAt, err := time.Parse(time.RFC3339, createdAtStr)
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("cylance %s api invalid timestamp: %v", apiName, err))
					continue
				}
				if CreatedAt.After(since) {
					apiDedupe[id] = CreatedAt.Unix()
					newItems = append(newItems, event)
					if CreatedAt.After(lastDetectionTime) {
						lastDetectionTime = CreatedAt
					}
				}
			}
		}

		allItems = append(allItems, newItems...)

		if len(response.Events) < pageSize {
			break
		}
		page++
	}

	return allItems, lastDetectionTime, nil
}

func (a *CylanceAdapter) requestDetectionDetails(ctx context.Context, detectionId string) (*CylanceEventsResponse, error) {
	url := fmt.Sprintf("%s%s/%s/details", a.conf.LoggingBaseURL, detectionsEndpoint, detectionId)
	return a.doWithRetry(ctx, url, "detect")
}

func (a *CylanceAdapter) requestThreatDetails(ctx context.Context, sha256 string) (*CylanceEventsResponse, error) {
	url := fmt.Sprintf("%s%s/%s", a.conf.LoggingBaseURL, threatsEndpoint, sha256)
	return a.doWithRetry(ctx, url, "threat")
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
