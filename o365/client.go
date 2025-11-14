package usp_o365

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/gjson"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"

	"golang.org/x/oauth2/clientcredentials"
)

type listItem struct {
	ContentType       string `json:"contentType,omitempty"`
	ContentID         string `json:"contentId,omitempty"`
	ContentURI        string `json:"contentUri,omitempty"`
	ContentCreated    string `json:"contentCreated,omitempty"`
	ContentExpiration string `json:"contentExpiration,omitempty"`
}

// Office 365 Management API endpoints for different cloud environments
// Reference: https://docs.microsoft.com/en-us/office/office-365-management-api/office-365-management-apis-overview#office-365-management-apis-overview
var URL = map[string]string{
	"enterprise":   "https://manage.office.com/api/v1.0/",
	"gcc-gov":      "https://manage-gcc.office.com/api/v1.0/",
	"gcc-high-gov": "https://manage.office365.us/api/v1.0/",
	"dod-gov":      "https://manage.protection.apps.mil/api/v1.0/",
}

// OAuth2 token endpoints for different cloud environments
// References:
// - Commercial/GCC: https://docs.microsoft.com/en-us/azure/active-directory/develop/authentication-national-clouds#azure-ad-authentication-endpoints
// - GCC High/DoD: https://docs.microsoft.com/en-us/azure/azure-government/compare-azure-government-global-azure#azure-active-directory-premium-p1-and-p2
// - Government clouds: https://docs.microsoft.com/en-us/graph/deployments#microsoft-graph-and-graph-explorer-service-root-endpoints
var TokenURL = map[string]string{
	"enterprise":   "https://login.windows.net/",
	"gcc-gov":      "https://login.windows.net/",
	"gcc-high-gov": "https://login.microsoftonline.us/",
	"dod-gov":      "https://login.microsoftonline.us/",
}

// Resource scopes for OAuth2 authentication, must match the target API environment
// Reference: https://docs.microsoft.com/en-us/office/office-365-management-api/get-started-with-office-365-management-apis#specify-the-permissions-your-app-requires-to-access-the-office-365-management-apis
var ResourceScope = map[string]string{
	"enterprise":   "https://manage.office.com",
	"gcc-gov":      "https://manage.office.com",
	"gcc-high-gov": "https://manage.office365.us",
	"dod-gov":      "https://manage.protection.apps.mil",
}

type Office365Adapter struct {
	conf       Office365Config
	uspClient  *uspclient.Client
	httpClient *http.Client

	endpoint     string
	endpointType string

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type Office365Config struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Domain        string                  `json:"domain" yaml:"domain"`
	TenantID      string                  `json:"tenant_id" yaml:"tenant_id"`
	PublisherID   string                  `json:"publisher_id" yaml:"publisher_id"`
	ClientID      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`
	Endpoint      string                  `json:"endpoint" yaml:"endpoint"`
	ContentTypes  string                  `json:"content_types" yaml:"content_types"`
	StartTime     string                  `json:"start_time" yaml:"start_time"`

	Deduper utils.Deduper `json:"-" yaml:"-"`
}

func (c *Office365Config) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Domain == "" {
		return errors.New("missing domain")
	}
	if c.TenantID == "" {
		return errors.New("missing tenant_id")
	}
	if c.PublisherID == "" {
		return errors.New("missing publisher_id")
	}
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}
	if c.Endpoint == "" {
		return errors.New("missing endpoint")
	}
	_, ok := URL[c.Endpoint]
	if !strings.HasPrefix(c.Endpoint, "https://") && !ok {
		return fmt.Errorf("invalid endpoint, not https or in %v", URL)
	}
	return nil
}

func NewOffice365Adapter(ctx context.Context, conf Office365Config) (*Office365Adapter, chan struct{}, error) {
	var err error

	// If no deduper is provided, use a local one.
	if conf.Deduper == nil {
		conf.Deduper, err = utils.NewLocalDeduper(1*time.Hour, 24*time.Hour)
		if err != nil {
			return nil, nil, err
		}
	}

	a := &Office365Adapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
	}

	if strings.HasPrefix(conf.Endpoint, "https://") {
		a.endpoint = conf.Endpoint
		a.endpointType = "custom"
	} else if v, ok := URL[conf.Endpoint]; ok {
		a.endpoint = v
		a.endpointType = conf.Endpoint
	} else {
		return nil, nil, fmt.Errorf("not a valid api endpoint: %s", conf.Endpoint)
	}

	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
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

	a.chStopped = make(chan struct{})

	if err := a.updateBearerToken(); err != nil {
		return nil, nil, err
	}

	// Set sane default for the content types.
	if a.conf.ContentTypes == "" {
		a.conf.ContentTypes = "Audit.AzureActiveDirectory,Audit.Exchange,Audit.SharePoint,Audit.General,DLP.All"
	}

	nCollecting := 0
	for _, ct := range strings.Split(a.conf.ContentTypes, ",") {
		ct = strings.TrimSpace(ct)
		if ct == "" {
			continue
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting to fetch %s events", ct))

		url := fmt.Sprintf("%s%s/activity/feed/subscriptions/start?contentType=%s&PublisherIdentifier=%s", a.endpoint, a.conf.TenantID, ct, a.conf.PublisherID)
		sub, err := a.makeOneRegistrationRequest(url)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("failed to register subscription to %s: %v", url, err))
			a.Close()
			return nil, nil, fmt.Errorf("failed to register subscription to %s: %v", url, err)
		}
		if len(sub) != 0 {
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("subscription created: %+v", sub))
		}

		nCollecting++

		a.wgSenders.Add(1)
		url = fmt.Sprintf("%s%s/activity/feed/subscriptions/content?contentType=%s&PublisherIdentifier=%s", a.endpoint, a.conf.TenantID, ct, a.conf.PublisherID)
		go a.fetchEvents(url)
	}

	if nCollecting == 0 {
		a.Close()
		return nil, nil, errors.New("no content types specified")
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *Office365Adapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()
	a.httpClient.CloseIdleConnections()

	a.conf.Deduper.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *Office365Adapter) fetchEvents(url string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("fetching of events exiting")

	nextPage := ""
	isFirstRun := true
	for isFirstRun || (nextPage != "" && !a.doStop.IsSet()) || !a.doStop.WaitFor(5*time.Minute) {
		if nextPage == "" {
			now := time.Now().UTC()
			start := a.conf.StartTime
			if !isFirstRun || start == "" {
				start = now.Add(-3 * time.Hour).Format("2006-01-02T15:04:05")
			}
			end := now.Format("2006-01-02T15:04:05")
			nextPage = fmt.Sprintf("%s&startTime=%s&endTime=%s", url, start, end)
		}
		isFirstRun = false
		var items []listItem
		items, nextPage = a.makeOneListRequest(nextPage)
		if len(items) == 0 {
			continue
		}

		nFetched := 0
		nShipped := 0
		nSkipped := 0
		nEmpty := 0
		for _, item := range items {
			if a.conf.Deduper.CheckAndAdd(item.ContentID) {
				nSkipped++
				continue
			}

			events := a.makeOneContentRequest(item.ContentURI)
			if len(events) == 0 {
				nEmpty++
				continue
			}

			nFetched++

			gjson.ParseBytes(events).ForEach(func(_, event gjson.Result) bool {
				// There is apparently no standard deduplication key in these logs
				// and MS makes no guarantees of uniqueness, so we have to dedup
				// ourselves. We will take a best stab by using the ID per event.
				ID := gjson.Parse(event.Raw).Get("Id").String()
				if ID != "" {
					if a.conf.Deduper.CheckAndAdd(ID) {
						nSkipped++
						return true
					}

				}
				msg := &protocol.DataMessage{
					TextPayload: event.Raw,
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
						return true
					}
				}
				nShipped++
				return true
			})
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetched %d, shipped %d: skipped: %d empty: %d", nFetched, nShipped, nSkipped, nEmpty))
	}
}

func (a *Office365Adapter) updateBearerToken() error {
	// Determine the correct OAuth2 token URL and resource scope based on the endpoint type
	// This is critical for government clouds to avoid "Confidential Client is not supported in Cross Cloud request" errors
	// Reference: https://docs.microsoft.com/en-us/azure/azure-government/compare-azure-government-global-azure#guidance-for-developers
	var tokenURL, resourceScope string

	if a.endpointType == "custom" {
		// For custom endpoints, default to enterprise settings
		tokenURL = fmt.Sprintf("https://login.windows.net/%s/oauth2/token?api-version=1.0", a.conf.Domain)
		resourceScope = "https://manage.office.com"
	} else if baseTokenURL, ok := TokenURL[a.endpointType]; ok {
		// Use the correct OAuth2 endpoint for the cloud environment
		if a.endpointType == "gcc-high-gov" || a.endpointType == "dod-gov" {
			// For GCC High and DoD, use tenant ID and v2.0 endpoint
			tokenURL = fmt.Sprintf("%s%s/oauth2/v2.0/token", baseTokenURL, a.conf.TenantID)
		} else {
			// For enterprise and GCC, use domain and v1.0 endpoint
			tokenURL = fmt.Sprintf("%s%s/oauth2/token?api-version=1.0", baseTokenURL, a.conf.Domain)
		}
		resourceScope = ResourceScope[a.endpointType]
	} else {
		// Fallback to enterprise settings
		tokenURL = fmt.Sprintf("https://login.windows.net/%s/oauth2/token?api-version=1.0", a.conf.Domain)
		resourceScope = "https://manage.office.com"
	}

	var conf *clientcredentials.Config

	// GCC High and DoD use v2.0 endpoints which require 'scope' parameter instead of 'resource'
	if a.endpointType == "gcc-high-gov" || a.endpointType == "dod-gov" {
		conf = &clientcredentials.Config{
			ClientID:     a.conf.ClientID,
			ClientSecret: a.conf.ClientSecret,
			TokenURL:     tokenURL,
			Scopes:       []string{resourceScope + "/.default"},
		}
	} else {
		// Enterprise and regular GCC use v1.0 endpoints with 'resource' parameter
		conf = &clientcredentials.Config{
			ClientID:     a.conf.ClientID,
			ClientSecret: a.conf.ClientSecret,
			TokenURL:     tokenURL,
			EndpointParams: url.Values{
				"resource": []string{resourceScope},
			},
		}
	}

	a.httpClient = conf.Client(context.Background())

	return nil
}

func (a *Office365Adapter) makeOneRegistrationRequest(url string) (utils.Dict, error) {
	// Prepare the request.
	req, err := http.NewRequest("POST", url, &bytes.Buffer{})
	if err != nil {
		a.doStop.Set()
		a.conf.ClientOptions.OnError(fmt.Errorf("http.NewRequest(): %v", err))
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	// Issue the request.
	resp, err := a.httpClient.Do(req)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusBadRequest && strings.Contains(string(body), "already enabled") {
		return nil, nil
	}

	// Evaluate if success.
	if resp.StatusCode != http.StatusOK {
		a.conf.ClientOptions.OnError(fmt.Errorf("office365 start api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, url, string(body)))
		return nil, errors.New(resp.Status)
	}

	// Parse the response.
	respData := utils.Dict{}
	if err := json.Unmarshal(body, &respData); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("office365 start api invalid json: %v (%s)", err, string(body)))
		return nil, fmt.Errorf("office365 start api invalid json: %v (%s)", err, string(body))
	}

	return respData, nil
}

func (a *Office365Adapter) makeOneListRequest(url string) ([]listItem, string) {
	const maxRetryDuration = 5 * time.Minute
	const initialDelay = 1 * time.Second
	const maxDelay = 30 * time.Second

	startTime := time.Now()
	delay := initialDelay

	for {
		// Check if we should stop
		if a.doStop.IsSet() {
			return nil, ""
		}

		// Prepare the request.
		req, err := http.NewRequest("GET", url, &bytes.Buffer{})
		if err != nil {
			a.doStop.Set()
			a.conf.ClientOptions.OnError(fmt.Errorf("http.NewRequest(): %v", err))
			return nil, ""
		}
		req.Header.Set("Content-Type", "application/json")

		// Issue the request.
		resp, err := a.httpClient.Do(req)
		if err != nil {
			// Check if we should retry or if we've been asked to stop
			if time.Since(startTime) < maxRetryDuration && !a.doStop.IsSet() {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("HTTP request failed, retrying in %v: %v", delay, err))
				if !a.doStop.WaitFor(delay) {
					// We were asked to stop during the wait
					return nil, ""
				}
				delay = time.Duration(float64(delay) * 1.5)
				if delay > maxDelay {
					delay = maxDelay
				}
				continue
			}
			a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
			return nil, ""
		}

		// Read the body once and handle the response
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		if readErr != nil {
			if time.Since(startTime) < maxRetryDuration && !a.doStop.IsSet() {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("Failed to read response body, retrying in %v: %v", delay, readErr))
				if !a.doStop.WaitFor(delay) {
					return nil, ""
				}
				delay = time.Duration(float64(delay) * 1.5)
				if delay > maxDelay {
					delay = maxDelay
				}
				continue
			}
			a.conf.ClientOptions.OnError(fmt.Errorf("failed to read response body: %v", readErr))
			return nil, ""
		}

		// Check for retryable status codes (503, 504)
		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
			if time.Since(startTime) < maxRetryDuration && !a.doStop.IsSet() {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("office365 list api %d, retrying in %v: %s\nREQUEST: %s\nRESPONSE: %s",
					resp.StatusCode, delay, resp.Status, url, string(body)))
				if !a.doStop.WaitFor(delay) {
					return nil, ""
				}
				delay = time.Duration(float64(delay) * 1.5)
				if delay > maxDelay {
					delay = maxDelay
				}
				continue
			}
			// Retries exhausted
			a.conf.ClientOptions.OnError(fmt.Errorf("office365 list api %d after retries: %s\nREQUEST: %s\nRESPONSE: %s",
				resp.StatusCode, resp.Status, url, string(body)))
			return nil, url
		}

		// Evaluate if success.
		if resp.StatusCode != http.StatusOK {
			a.conf.ClientOptions.OnError(fmt.Errorf("office365 list api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, url, string(body)))
			return nil, url
		}

		// Parse the response.
		respData := []listItem{}
		if err := json.Unmarshal(body, &respData); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("office365 list api invalid json: %v", err))
			return nil, ""
		}

		nextPage := resp.Header.Get("NextPageUri")

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("listed %d events (with page: %v)", len(respData), nextPage != ""))

		return respData, nextPage
	}
}

func (a *Office365Adapter) makeOneContentRequest(url string) []byte {
	const maxRetryDuration = 5 * time.Minute
	const initialDelay = 1 * time.Second
	const maxDelay = 30 * time.Second

	startTime := time.Now()
	delay := initialDelay

	for {
		// Check if we should stop
		if a.doStop.IsSet() {
			return nil
		}

		// Prepare the request.
		req, err := http.NewRequest("GET", url, &bytes.Buffer{})
		if err != nil {
			a.doStop.Set()
			a.conf.ClientOptions.OnError(fmt.Errorf("http.NewRequest(): %v", err))
			return nil
		}
		req.Header.Set("Content-Type", "application/json")

		// Issue the request.
		resp, err := a.httpClient.Do(req)
		if err != nil {
			// Check if we should retry or if we've been asked to stop
			if time.Since(startTime) < maxRetryDuration && !a.doStop.IsSet() {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("HTTP request failed, retrying in %v: %v", delay, err))
				if !a.doStop.WaitFor(delay) {
					// We were asked to stop during the wait
					return nil
				}
				delay = time.Duration(float64(delay) * 1.5)
				if delay > maxDelay {
					delay = maxDelay
				}
				continue
			}
			a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
			return nil
		}

		// Read the body once and handle the response
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		if readErr != nil {
			if time.Since(startTime) < maxRetryDuration && !a.doStop.IsSet() {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("Failed to read response body, retrying in %v: %v", delay, readErr))
				if !a.doStop.WaitFor(delay) {
					return nil
				}
				delay = time.Duration(float64(delay) * 1.5)
				if delay > maxDelay {
					delay = maxDelay
				}
				continue
			}
			a.conf.ClientOptions.OnError(fmt.Errorf("failed to read response body: %v", readErr))
			return nil
		}

		// Check for retryable status codes (503, 504)
		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
			if time.Since(startTime) < maxRetryDuration && !a.doStop.IsSet() {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("office365 content api %d, retrying in %v: %s\nREQUEST: %s\nRESPONSE: %s",
					resp.StatusCode, delay, resp.Status, url, string(body)))
				if !a.doStop.WaitFor(delay) {
					return nil
				}
				delay = time.Duration(float64(delay) * 1.5)
				if delay > maxDelay {
					delay = maxDelay
				}
				continue
			}
			// Retries exhausted
			a.conf.ClientOptions.OnError(fmt.Errorf("office365 content api %d after retries: %s\nREQUEST: %s\nRESPONSE: %s",
				resp.StatusCode, resp.Status, url, string(body)))
			return nil
		}

		// Evaluate if success.
		if resp.StatusCode != http.StatusOK {
			a.conf.ClientOptions.OnError(fmt.Errorf("office365 content api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, url, string(body)))
			return nil
		}

		// Return the successful response body
		return body
	}
}
