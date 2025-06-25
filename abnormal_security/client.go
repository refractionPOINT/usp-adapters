package usp_abnormal_security

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	queryInterval                     = 60
	pageSize                          = 100
	defaultBaseURL                    = "https://api.abnormalplatform.com/v1"
	abuseCampaignsEndpoint            = "/abusecampaigns"
	abuseCampaignsNotAnalyzedEndpoint = "/abuse_mailbox/not_analyzed"
	auditLogsEndpoint                 = "/auditlogs"
	casesEndpoint                     = "/cases"
	threatsEndpoint                   = "/threats"
	vendorCasesEndpoint               = "/vendor-cases"
)

type AbnormalSecurityConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	AccessToken   string                  `json:"access_token" yaml:"access_token"`
	BaseURL       string                  `json:"base_url" yaml:"base_url"`
}

type AbnormalSecurityAdapter struct {
	conf       AbnormalSecurityConfig
	uspClient  *uspclient.Client
	httpClient *http.Client
	chStopped  chan struct{}
	once       sync.Once
	ctx        context.Context
	cancel     context.CancelFunc

	fnvHasher                       hash.Hash64
	abuseCampaignsDedupe            map[string]int64
	abuseCampaignsNotAnalyzedDedupe map[string]int64
	auditLogsDedupe                 map[string]int64
	casesDedupe                     map[string]int64
	threatsDedupe                   map[string]int64
	vendorCasesDedupe               map[string]int64
}

func NewAbnormalSecurityAdapter(conf AbnormalSecurityConfig) (*AbnormalSecurityAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &AbnormalSecurityAdapter{
		conf:                            conf,
		abuseCampaignsDedupe:            make(map[string]int64),
		abuseCampaignsNotAnalyzedDedupe: make(map[string]int64),
		auditLogsDedupe:                 make(map[string]int64),
		casesDedupe:                     make(map[string]int64),
		threatsDedupe:                   make(map[string]int64),
		vendorCasesDedupe:               make(map[string]int64),
	}

	a.fnvHasher = fnv.New64a()

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

func (c *AbnormalSecurityConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.AccessToken == "" {
		return errors.New("missing access token")
	}
	if c.BaseURL == "" {
		c.BaseURL = defaultBaseURL
	}
	return nil
}

func (a *AbnormalSecurityAdapter) Close() error {
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

type AbnormalSecurityReponse interface {
	GetDicts() []utils.Dict
	HasNextPage() bool
}

type AbnormalSecurityCampaignsResponse struct {
	Campaigns      []utils.Dict `json:"campaigns"`
	PageNumber     int64        `json:"pageNumber"`
	NextPageNumber int64        `json:"nextPageNumber"`
}

func (r AbnormalSecurityCampaignsResponse) GetDicts() []utils.Dict {
	return r.Campaigns
}

func (r AbnormalSecurityCampaignsResponse) HasNextPage() bool {
	return r.NextPageNumber != 0
}

type AbnormalSecurityCampaignsNotAnalyzedResponse struct {
	Results        []utils.Dict `json:"results"`
	PageNumber     int64        `json:"pageNumber"`
	NextPageNumber int64        `json:"nextPageNumber"`
}

func (r AbnormalSecurityCampaignsNotAnalyzedResponse) GetDicts() []utils.Dict {
	return r.Results
}

func (r AbnormalSecurityCampaignsNotAnalyzedResponse) HasNextPage() bool {
	return r.NextPageNumber != 0
}

type AbnormalSecurityCasesResponse struct {
	Cases          []utils.Dict `json:"cases"`
	PageNumber     int64        `json:"pageNumber"`
	NextPageNumber int64        `json:"nextPageNumber"`
}

func (r AbnormalSecurityCasesResponse) GetDicts() []utils.Dict {
	return r.Cases
}

func (r AbnormalSecurityCasesResponse) HasNextPage() bool {
	return r.NextPageNumber != 0
}

type AbnormalSecurityThreatsResponse struct {
	Threats        []utils.Dict `json:"threats"`
	PageNumber     int64        `json:"pageNumber"`
	NextPageNumber int64        `json:"nextPageNumber"`
}

func (r AbnormalSecurityThreatsResponse) GetDicts() []utils.Dict {
	return r.Threats
}

func (r AbnormalSecurityThreatsResponse) HasNextPage() bool {
	return r.NextPageNumber != 0
}

type AbnormalSecurityAuditLogsResponse struct {
	AuditLogs      []utils.Dict `json:"auditLogs"`
	PageNumber     int64        `json:"pageNumber"`
	NextPageNumber int64        `json:"nextPageNumber"`
}

func (r AbnormalSecurityAuditLogsResponse) GetDicts() []utils.Dict {
	return r.AuditLogs
}

func (r AbnormalSecurityAuditLogsResponse) HasNextPage() bool {
	return r.NextPageNumber != 0
}

type AbnormalSecurityVendorCasesResponse struct {
	VendorCases    []utils.Dict `json:"vendorCases"`
	PageNumber     int64        `json:"pageNumber"`
	NextPageNumber int64        `json:"nextPageNumber"`
}

func (r AbnormalSecurityVendorCasesResponse) GetDicts() []utils.Dict {
	return r.VendorCases
}

func (r AbnormalSecurityVendorCasesResponse) HasNextPage() bool {
	return r.NextPageNumber != 0
}

type AbnormalSecurityFlatSingleResponse struct {
	Event []utils.Dict
}

func (r AbnormalSecurityFlatSingleResponse) GetDicts() []utils.Dict {
	return r.Event
}

func (r AbnormalSecurityFlatSingleResponse) HasNextPage() bool {
	return false
}

type Api struct {
	key          string
	endpoint     string
	idField      string
	timeField    string
	dedupe       map[string]int64
	responseType AbnormalSecurityReponse
	parameters   []string
	detailFn     func(ctx context.Context, id string) ([]utils.Dict, error)
}

func (a *AbnormalSecurityAdapter) fetchEvents() {
	since := map[string]time.Time{
		"abuseCampaigns":            time.Now().Add(-1 * queryInterval * time.Second),
		"abuseCampaignsNotAnalyzed": time.Now().Add(-1 * queryInterval * time.Second),
		"auditLogs":                 time.Now().Add(-1 * queryInterval * time.Second),
		"cases":                     time.Now().Add(-1 * queryInterval * time.Second),
		"threats":                   time.Now().Add(-1 * queryInterval * time.Second),
		"vendorCases":               time.Now().Add(-1 * queryInterval * time.Second),
	}

	apis := []Api{
		{
			key:          "abuseCampaigns",
			endpoint:     abuseCampaignsEndpoint,
			idField:      "campaignId",
			timeField:    "receivedTime",
			dedupe:       a.abuseCampaignsDedupe,
			responseType: &AbnormalSecurityCampaignsResponse{},
			detailFn: func(ctx context.Context, id string) ([]utils.Dict, error) {
				response, err := a.doWithRetry(ctx, fmt.Sprintf("%s/%s/%s", a.conf.BaseURL, abuseCampaignsEndpoint, id), "abuseCampaigns", &AbnormalSecurityFlatSingleResponse{})
				if err != nil {
					return nil, err
				}
				return response.GetDicts(), nil
			},
		},
		{
			key:          "abuseCampaignsNotAnalyzed",
			endpoint:     abuseCampaignsNotAnalyzedEndpoint,
			idField:      "abx_message_id",
			timeField:    "reported_datetime",
			dedupe:       a.abuseCampaignsNotAnalyzedDedupe,
			responseType: &AbnormalSecurityCampaignsNotAnalyzedResponse{},
			parameters:   []string{"start"},
			detailFn:     nil,
		},
		{
			key:          "auditLogs",
			endpoint:     auditLogsEndpoint,
			idField:      "",
			timeField:    "timestamp",
			dedupe:       a.auditLogsDedupe,
			responseType: &AbnormalSecurityAuditLogsResponse{},
			detailFn:     nil,
		},
		{
			key:          "cases",
			endpoint:     casesEndpoint,
			idField:      "caseId",
			timeField:    "lastModifiedTime",
			dedupe:       a.casesDedupe,
			responseType: &AbnormalSecurityCasesResponse{},
			detailFn: func(ctx context.Context, id string) ([]utils.Dict, error) {
				response, err := a.doWithRetry(ctx, fmt.Sprintf("%s/%s/%s", a.conf.BaseURL, casesEndpoint, id), "cases", &AbnormalSecurityFlatSingleResponse{})
				if err != nil {
					return nil, err
				}
				return response.GetDicts(), nil
			},
		},
		{
			key:          "threats",
			endpoint:     threatsEndpoint,
			idField:      "threatId",
			timeField:    "receivedTime",
			dedupe:       a.threatsDedupe,
			responseType: &AbnormalSecurityThreatsResponse{},
			detailFn: func(ctx context.Context, id string) ([]utils.Dict, error) {
				response, err := a.doWithRetry(ctx, fmt.Sprintf("%s/%s/%s", a.conf.BaseURL, threatsEndpoint, id), "threats", &AbnormalSecurityFlatSingleResponse{})
				if err != nil {
					return nil, err
				}
				return response.GetDicts(), nil
			},
		},
		{
			key:          "vendorCases",
			endpoint:     vendorCasesEndpoint,
			idField:      "vendorCaseId",
			timeField:    "lastModifiedTime",
			dedupe:       a.vendorCasesDedupe,
			responseType: &AbnormalSecurityVendorCasesResponse{},
			detailFn: func(ctx context.Context, id string) ([]utils.Dict, error) {
				response, err := a.doWithRetry(ctx, fmt.Sprintf("%s/%s/%s", a.conf.BaseURL, vendorCasesEndpoint, id), "vendorCases", &AbnormalSecurityFlatSingleResponse{})
				if err != nil {
					return nil, err
				}
				return response.GetDicts(), nil
			},
		},
	}

	ticker := time.NewTicker(queryInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", a.conf.BaseURL))
		case <-ticker.C:

			allItems := []utils.Dict{}

			for _, api := range apis {
				pageURL := fmt.Sprintf("%s%s", a.conf.BaseURL, api.endpoint)
				items, newSince, err := a.getEvents(a.ctx, pageURL, api, since[api.key])
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("%s fetch failed: %w", api.key, err))
					continue
				}
				since[api.key] = newSince
				allItems = append(allItems, items...)

				if api.detailFn != nil {
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
							a.conf.ClientOptions.OnError(fmt.Errorf("%s details fetch failed: %w", api.key, err))
							continue
						}
						allItems = append(allItems, response...)
					}
				}
			}

			if len(allItems) > 0 {
				a.submitEvents(allItems)
			}
		}
	}
}

func (a *AbnormalSecurityAdapter) getEvents(ctx context.Context, pageUrl string, api Api, since time.Time) ([]utils.Dict, time.Time, error) {
	var allItems []utils.Dict
	lastDetectionTime := since
	page := 1

	defer func() {
		for k, v := range api.dedupe {
			if v < since.Unix() {
				delete(api.dedupe, k)
			}
		}
	}()

	if api.parameters == nil {
		api.parameters = []string{"filter", "pageNumber", "pageSize"}
	}

	for {
		url := fmt.Sprintf("%s%s", pageUrl, "?")

		if slices.Contains(api.parameters, "filter") {
			url = fmt.Sprintf("%sfilter=%s gte %s", url, api.timeField, lastDetectionTime.UTC().Format(time.RFC3339))
		} else if slices.Contains(api.parameters, "start") {
			url = fmt.Sprintf("%sstart=%s", url, lastDetectionTime.UTC().Format(time.RFC3339))
		}

		if slices.Contains(api.parameters, "pageNumber") {
			url = fmt.Sprintf("%s&pageNumber=%d", url, page)
		}
		if slices.Contains(api.parameters, "pageSize") {
			url = fmt.Sprintf("%s&pageSize=%d", url, pageSize)
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s", url))

		response, err := a.doWithRetry(ctx, url, api.key, api.responseType)
		if err != nil {
			return nil, lastDetectionTime, err
		}

		var newItems []utils.Dict
		for _, event := range response.GetDicts() {
			var id string
			if event[api.idField] != "" {
				rawID, ok := event[api.idField].(string)
				if !ok {
					a.conf.ClientOptions.OnWarning(fmt.Sprintf("abnormal security %s event contains an id that's not a string: %s", api.key, event))
					continue
				} else {
					id = rawID
				}
			} else {
				a.fnvHasher.Reset()
				b, err := json.Marshal(event)
				if err != nil {
					a.conf.ClientOptions.OnWarning(fmt.Sprintf("abnormal security %s event does not contain an id and could not be marshaled: %s", api.key, event))
					continue
				}
				if _, err := a.fnvHasher.Write(b); err != nil {
					a.conf.ClientOptions.OnWarning(fmt.Sprintf("abnormal security %s event does not contain an id and could not be hashed: %s", api.key, event))
					continue
				}
				id = fmt.Sprintf("%d", a.fnvHasher.Sum64())
			}
			timeStr, ok := event[api.timeField].(string)
			if !ok {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("abnormal security %s event time not a string: %s", api.key, event))
				continue
			}

			if _, seen := api.dedupe[id]; !seen {
				parsedTime, err := time.Parse(time.RFC3339, timeStr)
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("abnormal security %s api invalid timestamp: %v", api.key, err))
					continue
				}
				if parsedTime.After(since) {
					api.dedupe[id] = parsedTime.Unix()
					newItems = append(newItems, event)
					if parsedTime.After(lastDetectionTime) {
						lastDetectionTime = parsedTime
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

func (a *AbnormalSecurityAdapter) doWithRetry(ctx context.Context, url string, apiName string, responseType AbnormalSecurityReponse) (AbnormalSecurityReponse, error) {
	for {
		var respBody []byte
		var status int
		var retryAfterInt int
		var retryAfterTime time.Time

		err := func() error {
			loopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(loopCtx, "GET", url, nil)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("abnormal security %s api request error: %v", apiName, err))
				return err
			}

			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.conf.AccessToken))
			req.Header.Set("Accept", "application/json")
			resp, err := a.httpClient.Do(req)

			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("abnormal security %s api do error: %v", apiName, err))
				return err
			}

			defer resp.Body.Close()

			respBody, err = io.ReadAll(resp.Body)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("abnormal security %s api read error: %v", apiName, err))
				return err
			}
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

			return nil
		}()
		if err != nil {
			return nil, err
		}

		if status == http.StatusTooManyRequests {
			if retryAfterInt != 0 {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("getEvents got 429 with 'Retry-After' header, sleeping %ds before retry", retryAfterInt))
				if err := sleepContext(a.ctx, time.Duration(retryAfterInt)*time.Second); err != nil {
					return nil, err
				}
			} else if !retryAfterTime.IsZero() {
				retryUntilTime := time.Until(retryAfterTime).Seconds()
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("getEvents got 429 with 'Retry-After' header with time %v, sleeping %vs before retry", retryAfterTime, retryUntilTime))
				if err := sleepContext(a.ctx, time.Duration(retryUntilTime)*time.Second); err != nil {
					return nil, err
				}
			} else {
				a.conf.ClientOptions.OnWarning("getEvents got 429 without 'Retry-After' header, sleeping 60s before retry")
				if err := sleepContext(a.ctx, 60*time.Second); err != nil {
					return nil, err
				}
			}
			continue
		}
		if status == http.StatusUnauthorized {
			return nil, errors.New("getEvents got 401 'Unauthorized' response")
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("abnormal security %s api non-200: %d\nRESPONSE %s", apiName, status, string(respBody))
		}

		if flatResponse, ok := responseType.(*AbnormalSecurityFlatSingleResponse); ok {
			var singleEvent utils.Dict
			err = json.Unmarshal(respBody, &singleEvent)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("abnormal security %s api invalid json: %v", apiName, err))
				return nil, err
			}
			flatResponse.Event = []utils.Dict{singleEvent}
			return flatResponse, nil
		}

		err = json.Unmarshal(respBody, &responseType)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("abnormal security %s api invalid json: %v", apiName, err))
			return nil, err
		}
		return responseType, nil
	}
}

func (a *AbnormalSecurityAdapter) submitEvents(events []utils.Dict) {
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
