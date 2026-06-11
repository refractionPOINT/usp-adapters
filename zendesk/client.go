package usp_zendesk

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	logsEndpoint  = "/api/v2/audit_logs"
	overlapPeriod = 30 * time.Second

	// defaultPollInterval is how long fetchEvents idles between polling ticks.
	defaultPollInterval = 30 * time.Second
)

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type opRequest struct {
	Limit     int    `json:"page[size],omitempty"`
	StartTime string `json:"filter[created_at],omitempty"`
	Sort      string `json:"sort,omitempty"`
}

type ZendeskAdapter struct {
	conf       ZendeskConfig
	uspClient  uspSink
	httpClient *http.Client

	baseURL      string
	pollInterval time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context

	dedupe map[string]int64
}

type ZendeskConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ApiToken      string                  `json:"api_token" yaml:"api_token"`
	ZendeskDomain string                  `json:"zendesk_domain" yaml:"zendesk_domain"`
	ZendeskEmail  string                  `json:"zendesk_email" yaml:"zendesk_email"`

	// BaseURL overrides the scheme and host used to reach the Zendesk API.
	// Empty means "https://" + ZendeskDomain.
	BaseURL string `json:"base_url" yaml:"base_url"`

	// PollInterval overrides the wait between polls of the audit logs
	// endpoint (default 30s). It is not settable through a config file; it
	// exists as a seam for tests.
	PollInterval time.Duration `json:"-" yaml:"-"`
}

func (c *ZendeskConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ApiToken == "" {
		return errors.New("missing api token")
	}
	if c.ZendeskDomain == "" {
		return errors.New("missing zendesk domain (e.g., 'your-company.zendesk.com')")
	}
	if c.ZendeskEmail == "" {
		return errors.New("missing zendesk email")
	}

	return nil
}

func NewZendeskAdapter(ctx context.Context, conf ZendeskConfig) (*ZendeskAdapter, chan struct{}, error) {
	return newZendeskAdapter(ctx, conf, nil)
}

// newZendeskAdapter is the implementation behind NewZendeskAdapter. When sink
// is non-nil it is used in place of a real LimaCharlie client -- the seam
// tests use to capture shipped events.
func newZendeskAdapter(ctx context.Context, conf ZendeskConfig, sink uspSink) (*ZendeskAdapter, chan struct{}, error) {
	a := &ZendeskAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
		dedupe: make(map[string]int64),
	}

	a.baseURL = strings.TrimSuffix(conf.BaseURL, "/")
	if a.baseURL == "" {
		a.baseURL = fmt.Sprintf("https://%s", conf.ZendeskDomain)
	}
	a.pollInterval = conf.PollInterval
	if a.pollInterval <= 0 {
		a.pollInterval = defaultPollInterval
	}

	if sink != nil {
		a.uspClient = sink
	} else {
		uspClient, err := uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			return nil, nil, err
		}
		a.uspClient = uspClient
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

	a.wgSenders.Add(1)
	go a.fetchEvents()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *ZendeskAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()
	a.httpClient.CloseIdleConnections()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *ZendeskAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", logsEndpoint))

	since := time.Now()

	for !a.doStop.WaitFor(a.pollInterval) {
		// The makeOneRequest function handles error
		// handling and fatal error handling.
		items, newSince, _ := a.makeOneRequest(since)
		since = newSince
		if items == nil {
			continue
		}

		for _, item := range items {
			msg := &protocol.DataMessage{
				JsonPayload: item,
				TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
			}
			if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
				if err == uspclient.ErrorBufferFull {
					a.conf.ClientOptions.OnWarning("stream falling behind")
					err = a.uspClient.Ship(msg, 1*time.Hour)
				}
				if err == nil {
					continue
				}
				a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				a.doStop.Set()
				return
			}
		}
	}
}

func (a *ZendeskAdapter) makeOneRequest(since time.Time) ([]utils.Dict, time.Time, error) {
	var allItems []utils.Dict
	currentTime := time.Now()
	var start string
	var lastDetectionTime time.Time

	if t := currentTime.Add(-overlapPeriod); t.Before(since) {
		start = since.UTC().Format(time.RFC3339)
	} else {
		start = currentTime.Add(-overlapPeriod).UTC().Format(time.RFC3339)
	}
	until := currentTime.UTC().Format(time.RFC3339)

	for {
		// Prepare the request.
		req, err := http.NewRequest("GET", fmt.Sprintf("%s%s?filter[created_at][]=%s&filter[created_at][]=%s&page[size]=100", a.baseURL, logsEndpoint, start, until), nil)
		//a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s%s?filter[created_at][]=%s&filter[created_at][]=%s&page[size]=100", a.baseURL, logsEndpoint, start, until))
		if err != nil {
			a.doStop.Set()
			return nil, lastDetectionTime, err
		}

		// Format the authentication string as "email/token:api_token"
		authString := fmt.Sprintf("%s/token:%s", a.conf.ZendeskEmail, a.conf.ApiToken)

		// Encode to Base64
		authEncoded := base64.StdEncoding.EncodeToString([]byte(authString))

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", fmt.Sprintf("Basic %s", authEncoded))

		// Issue the request.
		resp, err := a.httpClient.Do(req)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
			return nil, lastDetectionTime, err
		}
		defer resp.Body.Close()

		// Evaluate if success.
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			a.conf.ClientOptions.OnError(fmt.Errorf("zendesk api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, string(body), string(body)))
			return nil, lastDetectionTime, err
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error: %v", err))
			return nil, lastDetectionTime, err
		}

		// Parse the response.
		var response struct {
			AuditLogs []utils.Dict `json:"audit_logs"`
			Meta      struct {
				HasMore      bool   `json:"has_more"`
				AfterCursor  string `json:"after_cursor"`
				BeforeCursor string `json:"before_cursor"`
			}
			Links struct {
				Prev string `json:"prev"`
				Next string `json:"next"`
			}
		}
		err = json.Unmarshal(body, &response)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("zendesk api invalid json: %v", err))
			return nil, lastDetectionTime, err
		}
		//a.conf.ClientOptions.DebugLog(fmt.Sprintf("results: %s", response))

		// Collect items.
		items := response.AuditLogs
		var newItems []utils.Dict
		lastDetectionTime = since
		for _, item := range items {
			timestamp, _ := item["created_at"].(string)
			eventid, _ := item["id"].(string)
			if _, ok := a.dedupe[eventid]; ok {
				continue
			}
			epoch, _ := time.Parse(time.RFC3339, timestamp)
			a.dedupe[eventid] = epoch.Unix()
			newItems = append(newItems, item)
			lastDetectionTime = epoch
		}
		allItems = append(allItems, newItems...)

		// Handle pagination if there is a next link.
		if !response.Meta.HasMore {
			break
		}
		start = response.Links.Next
	}

	// Cull old dedupe entries.
	for k, v := range a.dedupe {
		if v < time.Now().Add(-overlapPeriod).Unix() {
			delete(a.dedupe, k)
		}
	}

	return allItems, lastDetectionTime, nil
}
