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

	apis := []Api{
		{
			key:                "detections",
			objectType:         "Detection",
			endpoint:           detectionsEndpoint,
			idField:            "Id",
			timeField:          "ReceivedTime",
			startFilterField:   "start",
			returnedTimeFormat: time.RFC3339,
			detailFn: func(ctx context.Context, id string) (CylanceResponse, error) {
				return a.requestDetectionDetails(ctx, id)
			},
			dedupe: a.detectionDedupe,
		},
		{
			key:                "threats",
			objectType:         "Threat",
			endpoint:           threatsEndpoint,
			idField:            "sha256",
			timeField:          "last_found",
			startFilterField:   "start_time",
			returnedTimeFormat: "2006-01-02T15:04:05",
			detailFn: func(ctx context.Context, id string) (CylanceResponse, error) {
				return a.requestThreatDetails(ctx, id)
			},
			dedupe: a.threatDedupe,
		},
		{
			key:                "memoryProtections",
			objectType:         "Memory Protection",
			endpoint:           memoryProtectionEndpoint,
			idField:            "device_image_file_event_id",
			timeField:          "created",
			startFilterField:   "start_time",
			returnedTimeFormat: "2006-01-02T15:04:05",
			detailFn: func(ctx context.Context, id string) (CylanceResponse, error) {
				return a.requestMemoryProtectionDetails(a.ctx, id)
			},
			dedupe: a.memoryProtectionDedupe,
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
				items, newSince, err := a.getEventsRequest(a.ctx, pageUrl, api, since[api.key])
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("%s fetch failed: %w", since[api.key], err))
					continue
				}
				since[api.key] = newSince

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
						detailResponse, err := api.detailFn(a.ctx, id)
						if err != nil {
							if a.refreshFailLimitMet {
								break
							}
							a.conf.ClientOptions.OnError(fmt.Errorf("%s details fetch failed: %w", api.key, err))
							continue
						}
						detailResponse.SetMeta("Cylance", api.objectType)
						allItems = append(allItems, detailResponse.GetDict()...)
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

type CylanceResponse interface {
	GetDict() []utils.Dict
	HasNextPage() bool
	SetMeta(ObjectSource string, ObjectType string)
}

type CylanceEventsResponse struct {
	PageItems          []utils.Dict `json:"page_items"`
	PageSize           int64        `json:"page_size"`
	TotalPages         int64        `json:"total_pages"`
	TotalNumberOfItems int64        `json:"total_number_of_items"`
	PageNumber         int64        `json:"page_number"`
}

func (r CylanceEventsResponse) GetDict() []utils.Dict {
	return r.PageItems
}

func (r CylanceEventsResponse) HasNextPage() bool {
	return !(r.PageNumber >= r.TotalPages)
}

func (r *CylanceEventsResponse) SetMeta(ObjectSource string, ObjectType string) {
	for _, item := range r.PageItems {
		if val, exists := item["ObjectSource"]; !exists || val == nil {
			// set a default—or compute it however you like
			item["ObjectSource"] = ObjectSource
		}
		if val, exists := item["ObjectType"]; !exists || val == nil {
			// set a default—or compute it however you like
			item["ObjectType"] = ObjectType
		}
	}
}

type CylanceEventResponse struct {
	Event []utils.Dict
}

func (r CylanceEventResponse) GetDict() []utils.Dict {
	return r.Event
}

func (r CylanceEventResponse) HasNextPage() bool {
	return false
}

func (r *CylanceEventResponse) SetMeta(ObjectSource string, ObjectType string) {
	for _, item := range r.Event {
		if val, exists := item["ObjectSource"]; !exists || val == nil {
			// set a default—or compute it however you like
			item["ObjectSource"] = ObjectSource
		}
		if val, exists := item["ObjectType"]; !exists || val == nil {
			// set a default—or compute it however you like
			item["ObjectType"] = ObjectType
		}
	}
}

type Api struct {
	key                string
	objectType         string
	endpoint           string
	idField            string
	timeField          string
	startFilterField   string
	returnedTimeFormat string
	detailFn           func(ctx context.Context, id string) (CylanceResponse, error)
	dedupe             map[string]int64
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
			"scp": []string{"opticsdetect:list", "opticsdetect:read", "device:list", "device:read", "threat:list", "threat:read", "memoryprotection:list", "memoryprotection:read"},
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
			refreshFails = a.tokenError(fmt.Sprintf("cylance auth api non-200: %dnRESPONSE %s", status, string(respBody)), refreshFails, 60*time.Second)
			continue
		}

		var response struct {
			AccessToken string `json:"access_token"`
		}
		err = json.Unmarshal(respBody, &response)
		if err != nil {
			refreshFails = a.tokenError(fmt.Sprintf("cylance auth api invalid json: %v", err), refreshFails, 10*time.Second)
			continue
		}

		a.accessToken = response.AccessToken
		a.tokenExpiresAt, err = getJWTExpiration(a.accessToken)
		if err != nil {
			a.tokenExpiresAt = time.Time{}
		}
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

func (a *CylanceAdapter) doWithRetry(ctx context.Context, url string, apiName string, responseType CylanceResponse) (CylanceResponse, error) {
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
			a.conf.ClientOptions.OnError(fmt.Errorf("cylance %s api non-200: %dnRESPONSE %s", apiName, status, string(respBody)))
			return nil, fmt.Errorf("cylance %s api non-200: %dnRESPONSE %s", apiName, status, string(respBody))
		}

		if eventResponse, ok := responseType.(*CylanceEventResponse); ok {
			var singleEvent utils.Dict
			err = json.Unmarshal(respBody, &singleEvent)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("cylance %s api invalid json: %v", apiName, err))
				return nil, err
			}
			eventResponse.Event = []utils.Dict{singleEvent}
			return eventResponse, nil
		}

		err = json.Unmarshal(respBody, &responseType)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("cylance %s api invalid json: %v", apiName, err))
			return nil, err
		}

		return responseType, nil
	}
}

func (a *CylanceAdapter) getEventsRequest(ctx context.Context, pageUrl string, api Api, since time.Time) ([]utils.Dict, time.Time, error) {
	var allItems []utils.Dict
	lastDetectionTime := since
	page := 1
	pageSize := pageLimit

	defer func() {
		for k, v := range api.dedupe {
			if v < since.Unix() {
				delete(api.dedupe, k)
			}
		}
	}()

	for {
		url := fmt.Sprintf("%s?page=%d&page_size=%d&%s=%s", pageUrl, page, pageSize, api.startFilterField, lastDetectionTime.UTC().Format(api.returnedTimeFormat))

		response, err := a.doWithRetry(ctx, url, api.key, &CylanceEventsResponse{})
		if err != nil {
			return nil, lastDetectionTime, err
		}

		var newItems []utils.Dict
		for _, event := range response.GetDict() {
			id, ok := event[api.idField].(string)
			if !ok {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("event id not a string: %s", event))
				continue
			}
			createdAtStr, ok := event[api.timeField].(string)
			if !ok {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("event time not a string: %s", event))
				continue
			}

			if _, seen := api.dedupe[id]; !seen {
				CreatedAt, err := time.Parse(api.returnedTimeFormat, createdAtStr)
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("cylance %s api invalid timestamp: %v", api.key, err))
					continue
				}
				if CreatedAt.After(since) {
					api.dedupe[id] = CreatedAt.Unix()
					newItems = append(newItems, event)
					if CreatedAt.After(lastDetectionTime) {
						lastDetectionTime = CreatedAt
					}
				}
			}
		}

		allItems = append(allItems, newItems...)

		if !response.HasNextPage() {
			break
		}
		page++
	}

	return allItems, lastDetectionTime, nil
}

func (a *CylanceAdapter) requestDetectionDetails(ctx context.Context, detectionId string) (CylanceResponse, error) {
	url := fmt.Sprintf("%s%s/%s/details", a.conf.LoggingBaseURL, detectionsEndpoint, detectionId)
	return a.doWithRetry(ctx, url, "detect", &CylanceEventResponse{})
}

func (a *CylanceAdapter) requestThreatDetails(ctx context.Context, sha256 string) (CylanceResponse, error) {
	url := fmt.Sprintf("%s%s/%s", a.conf.LoggingBaseURL, threatsEndpoint, sha256)
	return a.doWithRetry(ctx, url, "threat", &CylanceEventResponse{})
}

func (a *CylanceAdapter) requestMemoryProtectionDetails(ctx context.Context, memoryProtectionId string) (CylanceResponse, error) {
	url := fmt.Sprintf("%s%s/%s", a.conf.LoggingBaseURL, memoryProtectionEndpoint, memoryProtectionId)
	return a.doWithRetry(ctx, url, "memoryProtection", &CylanceEventResponse{})
}

func getJWTExpiration(tokenString string) (time.Time, error) {
	var expirationTime time.Time
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse token: %v", err)
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		if exp, ok := claims["exp"].(float64); ok {
			expirationTime = time.Unix(int64(exp), 0).UTC()
		} else {
			return time.Time{}, fmt.Errorf("exp claim not found or invalid")
		}
	}
	return expirationTime, nil
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
