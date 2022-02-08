package usp_o365

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

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

var URL = map[string]string{
	"enterprise":   "https://manage.office.com/api/v1.0/",
	"gcc-gov":      "https://manage-gcc.office.com/api/v1.0/",
	"gcc-high-gov": "https://manage.office365.us/api/v1.0/",
	"dod-gov":      "https://manage.protection.apps.mil/api/v1.0/",
}

type Office365Adapter struct {
	conf       Office365Config
	uspClient  *uspclient.Client
	httpClient *http.Client

	endpoint string

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
}

func NewOffice365Adapter(conf Office365Config) (*Office365Adapter, chan struct{}, error) {
	var err error
	a := &Office365Adapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
	}

	if strings.HasPrefix(conf.Endpoint, "https://") {
		a.endpoint = conf.Endpoint
	} else if v, ok := URL[conf.Endpoint]; ok {
		a.endpoint = v
	} else {
		return nil, nil, fmt.Errorf("not a valid api endpoint: %s", conf.Endpoint)
	}

	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
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
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("failed to register subscription to %s: %v", url, err))
			continue
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
	_, err := a.uspClient.Close()
	a.httpClient.CloseIdleConnections()

	return err
}

func (a *Office365Adapter) fetchEvents(url string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("fetching of events exiting")

	lastContent := map[string]struct{}{}
	newContent := map[string]struct{}{}

	nextPage := ""
	isFirstRun := true
	for isFirstRun || nextPage != "" || !a.doStop.WaitFor(5*time.Minute) {
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
		for _, item := range items {
			if _, ok := lastContent[item.ContentID]; ok {
				newContent[item.ContentID] = struct{}{}
				continue
			}
			events := a.makeOneContentRequest(item.ContentURI)
			if len(events) == 0 {
				continue
			}

			nFetched++
			newContent[item.ContentID] = struct{}{}

			for _, event := range events {
				msg := &protocol.DataMessage{
					JsonPayload: event,
					TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
				}
				if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
					if err == uspclient.ErrorBufferFull {
						a.conf.ClientOptions.OnWarning("stream falling behind")
						err = a.uspClient.Ship(msg, 0)
					}
					if err != nil {
						a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
					}
					a.doStop.Set()
					return
				}
			}
		}

		lastContent = newContent
		newContent = map[string]struct{}{}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetched %d events", nFetched))
	}
}

func (a *Office365Adapter) updateBearerToken() error {
	conf := &clientcredentials.Config{
		ClientID:     a.conf.ClientID,
		ClientSecret: a.conf.ClientSecret,
		TokenURL:     fmt.Sprintf("https://login.windows.net/%s/oauth2/token?api-version=1.0", a.conf.Domain),
		EndpointParams: url.Values{
			"resource": []string{"https://manage.office.com"},
		},
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

	body, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusBadRequest && strings.Contains(string(body), "already enabled") {
		return nil, nil
	}

	// Evaluate if success.
	if resp.StatusCode != http.StatusOK {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf("office365 start api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, url, string(body)))
		return nil, errors.New(resp.Status)
	}

	// Parse the response.
	respData := utils.Dict{}
	jsonDecoder := json.NewDecoder(resp.Body)
	if err := jsonDecoder.Decode(&respData); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("office365 start api invalid json: %v", err))
		return nil, err
	}

	return respData, nil
}

func (a *Office365Adapter) makeOneListRequest(url string) ([]listItem, string) {
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
		a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
		return nil, ""
	}
	defer resp.Body.Close()

	// Evaluate if success.
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		a.conf.ClientOptions.OnWarning(fmt.Sprintf("office365 list api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, url, string(body)))
		return nil, url
	}

	// Parse the response.
	respData := []listItem{}
	jsonDecoder := json.NewDecoder(resp.Body)
	if err := jsonDecoder.Decode(&respData); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("office365 list api invalid json: %v", err))
		return nil, ""
	}

	nextPage := resp.Header.Get("NextPageUri")

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("listed %d events (with page: %v)", len(respData), nextPage != ""))

	return respData, nextPage
}

func (a *Office365Adapter) makeOneContentRequest(url string) []utils.Dict {
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
		a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
		return nil
	}
	defer resp.Body.Close()

	// Evaluate if success.
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		a.conf.ClientOptions.OnWarning(fmt.Sprintf("office365 content api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, url, string(body)))
		return nil
	}

	// Parse the response.
	respData := []utils.Dict{}
	jsonDecoder := json.NewDecoder(resp.Body)
	if err := jsonDecoder.Decode(&respData); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("office365 content api invalid json: %v", err))
		return nil
	}

	return respData
}
