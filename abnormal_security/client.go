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
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
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
	ClientOptions        uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	AccessToken          string                  `json:"access_token" yaml:"access_token"`
	BaseURL              string                  `json:"base_url,omitempty" yaml:"base_url,omitempty"`
	MaxConcurrentWorkers int                     `json:"max_concurrent_workers,omitempty" yaml:"max_concurrent_workers,omitempty"`
}

type AbnormalSecurityAdapter struct {
	conf        AbnormalSecurityConfig
	uspClient   *uspclient.Client
	httpClient  *http.Client
	chStopped   chan struct{}
	chFetchLoop chan struct{}
	chRetry     chan InternalServerErrorRetry
	closeOnce   sync.Once
	fetchOnce   sync.Once
	ctx         context.Context
	cancel      context.CancelFunc

	fnvHasher                       hash.Hash64
	abuseCampaignsDedupe            map[string]int64
	abuseCampaignsNotAnalyzedDedupe map[string]int64
	auditLogsDedupe                 map[string]int64
	casesDedupe                     map[string]int64
	threatsDedupe                   map[string]int64
	vendorCasesDedupe               map[string]int64
}

func NewAbnormalSecurityAdapter(ctx context.Context, conf AbnormalSecurityConfig) (*AbnormalSecurityAdapter, chan struct{}, error) {
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
	a.chFetchLoop = make(chan struct{})
	a.chRetry = make(chan InternalServerErrorRetry, 1000)

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
	if c.MaxConcurrentWorkers == 0 {
		// If unset, default to 10
		c.MaxConcurrentWorkers = 10
	}
	return nil
}

func (a *AbnormalSecurityAdapter) Close() error {
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
		close(a.chRetry)
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
	HasID() bool
	HasTimestamp() bool
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
	return r.NextPageNumber != 0 && r.NextPageNumber != r.PageNumber
}

func (r AbnormalSecurityCampaignsResponse) HasID() bool {
	return true
}

func (r AbnormalSecurityCampaignsResponse) HasTimestamp() bool {
	return false
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
	return r.NextPageNumber != 0 && r.NextPageNumber != r.PageNumber
}

func (r AbnormalSecurityCampaignsNotAnalyzedResponse) HasID() bool {
	return true
}

func (r AbnormalSecurityCampaignsNotAnalyzedResponse) HasTimestamp() bool {
	return true
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
	return r.NextPageNumber != 0 && r.NextPageNumber != r.PageNumber
}

func (r AbnormalSecurityCasesResponse) HasID() bool {
	return true
}

func (r AbnormalSecurityCasesResponse) HasTimestamp() bool {
	return false
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
	return r.NextPageNumber != 0 && r.NextPageNumber != r.PageNumber
}

func (r AbnormalSecurityThreatsResponse) HasID() bool {
	return true
}

func (r AbnormalSecurityThreatsResponse) HasTimestamp() bool {
	return false
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
	return r.NextPageNumber != 0 && r.NextPageNumber != r.PageNumber
}

func (r AbnormalSecurityAuditLogsResponse) HasID() bool {
	return false
}

func (r AbnormalSecurityAuditLogsResponse) HasTimestamp() bool {
	return true
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
	return r.NextPageNumber != 0 && r.NextPageNumber != r.PageNumber
}

func (r AbnormalSecurityVendorCasesResponse) HasID() bool {
	return true
}

func (r AbnormalSecurityVendorCasesResponse) HasTimestamp() bool {
	return false
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

func (r AbnormalSecurityFlatSingleResponse) HasID() bool {
	return true
}

func (r AbnormalSecurityFlatSingleResponse) HasTimestamp() bool {
	return true
}

type AbnormalSecurityThreatsFlatSingleResponse struct {
	ThreatId       string       `json:"threatId"`
	Event          []utils.Dict `json:"messages"`
	PageNumber     int          `json:"pageNumber"`
	NextPageNumber int          `json:"nextPageNumber"`
}

func (r AbnormalSecurityThreatsFlatSingleResponse) GetDicts() []utils.Dict {
	return r.Event
}

func (r AbnormalSecurityThreatsFlatSingleResponse) HasNextPage() bool {
	return false
}

func (r AbnormalSecurityThreatsFlatSingleResponse) HasID() bool {
	return true
}

func (r AbnormalSecurityThreatsFlatSingleResponse) HasTimestamp() bool {
	return true
}

type Api struct {
	mu                 sync.Mutex
	key                string
	endpoint           string
	since              time.Time
	idField            string
	timeField          string
	timeFieldSecondary string
	active             bool
	dedupe             map[string]int64
	responseType       AbnormalSecurityReponse
	parameters         []string
	detailFn           func(ctx context.Context, id string) ([]utils.Dict, time.Time, error)
	detailResponseType AbnormalSecurityReponse
	baseTimestamp      bool
}

type InternalServerErrorRetry struct {
	ctx       context.Context
	pageUrl   string
	api       *Api
	details   bool
	retryTime time.Time
	attempt   int
}

func (a *AbnormalSecurityAdapter) fetchEvents() {
	apis := []Api{
		{
			key:                "abuseCampaigns",
			endpoint:           abuseCampaignsEndpoint,
			since:              time.Now().Add(-1 * queryInterval * time.Second),
			idField:            "campaignId",
			timeField:          "receivedTime",
			timeFieldSecondary: "lastReported",
			active:             true,
			dedupe:             a.abuseCampaignsDedupe,
			responseType:       &AbnormalSecurityCampaignsResponse{},
			detailResponseType: &AbnormalSecurityFlatSingleResponse{},
		},
		{
			key:                "cases",
			endpoint:           casesEndpoint,
			since:              time.Now().Add(-1 * queryInterval * time.Second),
			idField:            "caseId",
			timeField:          "lastModifiedTime",
			timeFieldSecondary: "customerVisibleTime",
			active:             true,
			dedupe:             a.casesDedupe,
			responseType:       &AbnormalSecurityCasesResponse{},
			detailResponseType: &AbnormalSecurityFlatSingleResponse{},
		},
		{
			key:                "threats",
			endpoint:           threatsEndpoint,
			since:              time.Now().Add(-1 * queryInterval * time.Second),
			idField:            "threatId",
			timeField:          "receivedTime",
			active:             true,
			dedupe:             a.threatsDedupe,
			responseType:       &AbnormalSecurityThreatsResponse{},
			detailResponseType: &AbnormalSecurityThreatsFlatSingleResponse{},
		},
		{
			key:                "vendorCases",
			endpoint:           vendorCasesEndpoint,
			since:              time.Now().Add(-1 * queryInterval * time.Second),
			idField:            "vendorCaseId",
			timeField:          "lastModifiedTime",
			active:             true,
			dedupe:             a.vendorCasesDedupe,
			responseType:       &AbnormalSecurityVendorCasesResponse{},
			detailResponseType: &AbnormalSecurityFlatSingleResponse{},
		},
		{
			key:           "abuseCampaignsNotAnalyzed",
			endpoint:      abuseCampaignsNotAnalyzedEndpoint,
			since:         time.Now().Add(-1 * queryInterval * time.Second),
			idField:       "abx_message_id",
			timeField:     "reported_datetime",
			active:        true,
			dedupe:        a.abuseCampaignsNotAnalyzedDedupe,
			responseType:  &AbnormalSecurityCampaignsNotAnalyzedResponse{},
			parameters:    []string{"start"},
			baseTimestamp: true,
		},
		{
			key:           "auditLogs",
			endpoint:      auditLogsEndpoint,
			since:         time.Now().Add(-1 * queryInterval * time.Second),
			idField:       "",
			timeField:     "timestamp",
			active:        true,
			dedupe:        a.auditLogsDedupe,
			responseType:  &AbnormalSecurityAuditLogsResponse{},
			baseTimestamp: true,
		},
	}

	// Each Api struct needs to be self referencial for the detailFn (if applied)
	// Because of this, the detailFn needs to be set after initialization.
	apis[0].detailFn = func(ctx context.Context, id string) ([]utils.Dict, time.Time, error) {
		newEvents, newSince, err := a.getEvents(ctx, fmt.Sprintf("%s%s/%s", a.conf.BaseURL, abuseCampaignsEndpoint, id), &apis[0], true)
		if err != nil {
			return nil, time.Time{}, err
		}
		return newEvents, newSince, err
	}
	apis[1].detailFn = func(ctx context.Context, id string) ([]utils.Dict, time.Time, error) {
		newEvents, newSince, err := a.getEvents(ctx, fmt.Sprintf("%s%s/%s", a.conf.BaseURL, casesEndpoint, id), &apis[1], true)
		if err != nil {
			return nil, newSince, err
		}
		return newEvents, time.Time{}, err
	}
	apis[2].detailFn = func(ctx context.Context, id string) ([]utils.Dict, time.Time, error) {
		newEvents, newSince, err := a.getEvents(ctx, fmt.Sprintf("%s%s/%s", a.conf.BaseURL, threatsEndpoint, id), &apis[2], true)
		if err != nil {
			return nil, time.Time{}, err
		}
		return newEvents, newSince, err
	}
	apis[3].detailFn = func(ctx context.Context, id string) ([]utils.Dict, time.Time, error) {
		newEvents, newSince, err := a.getEvents(ctx, fmt.Sprintf("%s%s/%s", a.conf.BaseURL, vendorCasesEndpoint, id), &apis[3], true)
		if err != nil {
			return nil, time.Time{}, err
		}
		return newEvents, newSince, err
	}

	pApis := make([]*Api, len(apis))
	for i := range apis {
		pApis[i] = &apis[i]
	}

	go func() {
		retryCheck := 30 * time.Second
		ticker := time.NewTicker(retryCheck)
		defer ticker.Stop()

		retryManager := []InternalServerErrorRetry{}

		for {
			select {
			case <-a.ctx.Done():
				if len(retryManager) > 0 {
					a.conf.ClientOptions.DebugLog(fmt.Sprintf("retryManager abandoned %d retries", len(retryManager)))
				}
				return
			case newRetry := <-a.chRetry:
				switch newRetry.attempt {
				case 1:
					newRetry.retryTime = time.Now().Add(1 * time.Minute)
				case 2:
					newRetry.retryTime = time.Now().Add(5 * time.Minute)
				case 3:
					newRetry.retryTime = time.Now().Add(10 * time.Minute)
				}
				retryManager = append(retryManager, newRetry)
			case <-ticker.C:
				for i := len(retryManager) - 1; i >= 0; i-- {
					if time.Now().After(retryManager[i].retryTime) {
						retry := retryManager[i]
						retryManager = slices.Delete(retryManager, i, i+1)

						items, _, err := a.getEvents(retry.ctx, retry.pageUrl, retry.api, retry.details)
						if err != nil {
							if retry.attempt >= 3 {
								a.conf.ClientOptions.OnError(fmt.Errorf("retry abandoned after 3 attempts for %s: %w", retry.pageUrl, err))
							} else {
								retry.attempt++
								a.chRetry <- retry
							}
						}
						if len(items) > 0 {
							a.submitEvents(items)
						}
					}
				}
			}
		}
	}()

	a.RunFetchLoop(pApis)
}

func (a *AbnormalSecurityAdapter) shouldShutdown(apis []*Api) bool {
	// If no APIs are active due to 'Forbidden' messages, shutdown
	for _, api := range apis {
		if api.active {
			return false
		}
	}
	a.conf.ClientOptions.OnWarning("all apis are disabled due to forbidden messages. shutting down")
	return true
}

// Returned results from fetch routines
type routineResult struct {
	key   string
	items []utils.Dict
	err   error
}

func (a *AbnormalSecurityAdapter) RunFetchLoop(apis []*Api) {
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
						// acknowledge fetch loop isn't running
						a.Close()
						return
					}

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
							a.conf.ClientOptions.DebugLog(fmt.Sprintf("shipped %d events", count))
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
						if !(apis[i].active) {
							continue
						}
						workerSem <- struct{}{}
						wg.Add(1)
						go func(api *Api) {
							defer func() {
								<-workerSem
								wg.Done()
							}()
							a.fetchApi(api, resultCh, workerSem, &wg)
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
						shipCh <- res.items
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

func (a *AbnormalSecurityAdapter) fetchApi(api *Api, resultCh chan<- routineResult, workerSem chan struct{}, wg *sync.WaitGroup) {
	fetchCtx, cancelFetch := context.WithCancel(a.ctx)
	defer cancelFetch()

	toReturn := []utils.Dict{}
	pageURL := fmt.Sprintf("%s%s", a.conf.BaseURL, api.endpoint)
	// Check for a close signal
	select {
	case <-fetchCtx.Done():
		return
	default:
	}

	items, newSince, err := a.getEvents(a.ctx, pageURL, api, false)

	if newSince.After(api.since) {
		api.mu.Lock()
		api.since = newSince
		api.mu.Unlock()
	}

	if err != nil {
		resultCh <- routineResult{api.key, nil, err}
		return
	}

	// Append events to return -- ommitting non-event data
	for _, item := range items {
		if _, ok := item[api.timeField]; ok {
			toReturn = append(toReturn, item)
		}
	}

	if len(toReturn) > 0 {
		resultCh <- routineResult{api.key, toReturn, nil}
	}

	// If non-event data was returned, use it to pull the events
	if api.detailFn != nil {
		type detailRoutineResult struct {
			detailItems []utils.Dict
			newSince    time.Time
			err         error
		}
		var mu sync.Mutex
		var latestTimeStamp time.Time
		detailResult := make(chan detailRoutineResult)

		go func(latestTimeStamp *time.Time) {
			for result := range detailResult {
				if result.newSince.After(*latestTimeStamp) {
					mu.Lock()
					*latestTimeStamp = result.newSince
					mu.Unlock()
				}
				// Because detailFn is sending a request for every specific event,
				// and because dealing with errors involves waiting and retrying,
				// we don't want to pool and hold onto all events until we have them all collected
				resultCh <- routineResult{api.key, result.detailItems, result.err}
			}
		}(&latestTimeStamp)

		for _, event := range items {
			// Check for a close signal
			select {
			case <-fetchCtx.Done():
				return
			default:
			}
			// Get Event ID
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

			workerSem <- struct{}{}
			wg.Add(1)
			go func(id string) {
				defer func() {
					<-workerSem
					wg.Done()
				}()
				detailItems, newSince, err := api.detailFn(a.ctx, id)
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("%s details fetch failed: %w", api.key, err))
					return
				}
				detailResult <- detailRoutineResult{detailItems, newSince, err}
			}(id)
		}

		go func() {
			wg.Wait()
			close(detailResult)
		}()

		if latestTimeStamp.After(api.since) {
			api.mu.Lock()
			api.since = latestTimeStamp
			api.mu.Unlock()
		}
	}
}

func (a *AbnormalSecurityAdapter) getEvents(ctx context.Context, pageUrl string, api *Api, details bool) ([]utils.Dict, time.Time, error) {
	var allItems []utils.Dict
	lastDetectionTime := api.since
	page := 1

	defer func() {
		api.mu.Lock()
		for k, v := range api.dedupe {
			if v < api.since.Unix() {
				delete(api.dedupe, k)
			}
		}
		api.mu.Unlock()
	}()

	if api.parameters == nil {
		api.mu.Lock()
		api.parameters = []string{"filter", "pageNumber", "pageSize"}
		api.mu.Unlock()
	}

	for {
		select {
		case <-ctx.Done():
			return nil, time.Time{}, ctx.Err()
		default:
		}

		u, err := url.Parse(pageUrl)
		if err != nil {
			return nil, time.Time{}, err
		}

		has := func(p string) bool { return slices.Contains(api.parameters, p) }

		q := u.Query()

		if has("filter") {
			q.Set("filter", fmt.Sprintf("%s gte %s", api.timeField, lastDetectionTime.UTC().Format(time.RFC3339)))
		} else if has("start") {
			q.Set("start", lastDetectionTime.UTC().Format(time.RFC3339))
		}

		if has("pageNumber") {
			q.Set("pageNumber", strconv.Itoa(page))
		}
		if has("pageSize") {
			q.Set("pageSize", strconv.Itoa(pageSize))
		}

		u.RawQuery = q.Encode()
		url := u.String()

		var response AbnormalSecurityReponse
		if details {
			response, err = a.doWithRetry(ctx, url, api, true)
			if err != nil {
				return nil, time.Time{}, err
			}
		} else {
			response, err = a.doWithRetry(ctx, url, api, false)
			if err != nil {
				return nil, time.Time{}, err
			}
		}

		var newItems []utils.Dict

		for _, event := range response.GetDicts() {
			var id string

			if response.HasID() && event[api.idField] != "" {
				switch t := event[api.idField].(type) {
				case string:
					id = t

				case int:
					id = strconv.Itoa(t)

				case int32:
					id = strconv.FormatInt(int64(t), 10)

				case int64:
					id = strconv.FormatInt(t, 10)

				case uint:
					id = strconv.FormatUint(uint64(t), 10)

				case uint32:
					id = strconv.FormatUint(uint64(t), 10)

				case uint64:
					id = strconv.FormatUint(t, 10)

				case json.Number:
					if idInt, err := t.Int64(); err == nil {
						id = strconv.FormatInt(idInt, 10)
					} else {
						id = t.String()
					}

				default:
					id = fmt.Sprintf("%v", t) // Fallback for anything else
				}
			} else {
				// If the event doesn't have an id, generate an id by hashing the event
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

			var parsedTime time.Time
			var raw any
			if response.HasTimestamp() {
				val, ok := event[api.timeField]
				if ok && val != nil {
					raw = val
				} else {
					val2, ok2 := event[api.timeFieldSecondary]
					if ok2 && val2 != nil {
						raw = val2
					} else {
						a.conf.ClientOptions.OnError(fmt.Errorf("abnormal security %s api invalid timestamp: %s", api.key, event))
					}
				}

				switch t := raw.(type) {
				case string:
					iso := strings.ReplaceAll(t, " ", "T")
					parsedTime, err = time.Parse(time.RFC3339, iso)
					if err != nil {
						a.conf.ClientOptions.OnError(fmt.Errorf("bad timestamp %q: %w", t, err))
					}

				case json.Number:
					if i, err := t.Int64(); err == nil {
						parsedTime = time.Unix(i, 0)
					} else {
						a.conf.ClientOptions.OnError(fmt.Errorf("bad json.Number %q: %w", t, err))
					}

				case float64:
					parsedTime = time.Unix(int64(t), 0)

				case int, int32, int64:
					parsedTime = time.Unix(reflect.ValueOf(t).Int(), 0)

				default:
					a.conf.ClientOptions.OnError(fmt.Errorf("unsupported timestamp type %T for %v", t, t))
				}
			}

			// We don't want to reprocess if the ID already exists in the dedupe for both reference or event.
			if _, seen := api.dedupe[id]; !seen {
				// If a timestamp is not provided, it's a reference to the event
				// The event will be added to the dedupe after it gets requested
				if response.HasTimestamp() && (parsedTime != time.Time{}) && parsedTime.After(api.since) {
					api.mu.Lock()
					api.dedupe[id] = parsedTime.Unix()
					api.mu.Unlock()
					newItems = append(newItems, event)
					if parsedTime.After(lastDetectionTime) {
						lastDetectionTime = parsedTime
					}
				} else if !response.HasTimestamp() {
					newItems = append(newItems, event)
				}
			}
		}

		allItems = append(allItems, newItems...)

		if !response.HasNextPage() {
			break
		}

		page++
	}

	for i := range allItems {
		allItems[i]["event-type"] = api.key
	}

	return allItems, lastDetectionTime, nil
}

func (a *AbnormalSecurityAdapter) doWithRetry(ctx context.Context, url string, api *Api, details bool) (AbnormalSecurityReponse, error) {
	var respType AbnormalSecurityReponse
	if details {
		respType = api.detailResponseType
	} else {
		respType = api.responseType
	}

	t := reflect.TypeOf(respType)
	if t.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("expected a pointer, got %v", t)
	}

	v := reflect.New(t.Elem())
	resp := v.Interface().(AbnormalSecurityReponse)

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
				a.conf.ClientOptions.OnError(fmt.Errorf("abnormal security %s api request error: %v", api.key, err))
				return err
			}

			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.conf.AccessToken))
			req.Header.Set("Accept", "application/json")
			resp, err := a.httpClient.Do(req)

			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("abnormal security %s api do error: %v", api.key, err))
				return err
			}

			defer resp.Body.Close()

			respBody, err = io.ReadAll(resp.Body)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("abnormal security %s api read error: %v", api.key, err))
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
		if status == http.StatusForbidden {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("due to 'Forbidden' response, the %s api will be deactivated for query", api.key))
			api.mu.Lock()
			api.active = false
			api.mu.Unlock()
			return nil, errors.New("getEvents got a 403 'Forbidden' response")
		}
		if status >= 500 && status < 600 && details { // internal server error
			a.chRetry <- InternalServerErrorRetry{ctx, url, api, details, time.Time{}, 1}
			return nil, fmt.Errorf("server error %d scheduled for retry", status)
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("abnormal security %s api non-200: %d\nRESPONSE %s", api.key, status, string(respBody))
		}

		switch concrete := resp.(type) {
		case *AbnormalSecurityFlatSingleResponse:
			var singleEvent utils.Dict
			err = json.Unmarshal(respBody, &singleEvent)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("abnormal security %s api invalid json: %v", api.key, err))
				return nil, err
			}
			concrete.Event = []utils.Dict{singleEvent}
			return resp, nil
		default:
			err = json.Unmarshal(respBody, concrete)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("abnormal security %s api invalid json: %v", api.key, err))
				return nil, err
			}
		}
		return resp, nil
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