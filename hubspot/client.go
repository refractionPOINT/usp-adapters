package usp_hubspot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	logsURL             = "https://api.hubapi.com/account-info/v3/activity/audit-logs"
	overlapPeriod       = 30 * time.Minute
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

type HubSpotAdapter struct {
	conf       HubSpotConfig
	uspClient  uspSink
	httpClient *http.Client

	apiURL       string
	pollInterval time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context

	dedupe map[string]int64
	scan   *auditScan
}

// auditScan retains a fixed query window and its collected pages on request errors.
// Records are not added to the overlap deduper until the complete result is returned.
type auditScan struct {
	start, until, after string
	items               []utils.Dict
	seen                map[string]bool
}

type HubSpotConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	AccessToken   string                  `json:"access_token" yaml:"access_token"`

	// URL overrides the audit-logs endpoint queried. Empty means the standard
	// HubSpot API endpoint
	// (https://api.hubapi.com/account-info/v3/activity/audit-logs).
	URL string `json:"url" yaml:"url"`

	// PollInterval overrides the wait between polls of the audit-logs
	// endpoint. It is not settable through a config file; it exists as a seam
	// for tests. Zero means the default (30 seconds).
	PollInterval time.Duration `json:"-" yaml:"-"`
}

func (c *HubSpotConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.AccessToken == "" {
		return errors.New("missing access token")
	}
	return nil
}

func NewHubSpotAdapter(ctx context.Context, conf HubSpotConfig) (*HubSpotAdapter, chan struct{}, error) {
	return newHubSpotAdapter(ctx, conf, nil)
}

// newHubSpotAdapter is the implementation behind NewHubSpotAdapter. When sink
// is non-nil it is used in place of a real LimaCharlie client -- the seam tests
// use to capture shipped events.
func newHubSpotAdapter(ctx context.Context, conf HubSpotConfig, sink uspSink) (*HubSpotAdapter, chan struct{}, error) {
	a := &HubSpotAdapter{
		conf:         conf,
		ctx:          context.Background(),
		doStop:       utils.NewEvent(),
		dedupe:       make(map[string]int64),
		apiURL:       conf.URL,
		pollInterval: conf.PollInterval,
	}
	if a.apiURL == "" {
		a.apiURL = logsURL
	}
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

func (a *HubSpotAdapter) Close() error {
	defer utils.StopBlockedProducers(a.uspClient)()
	a.conf.ClientOptions.DebugLog("closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
	a.httpClient.CloseIdleConnections()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *HubSpotAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", a.apiURL))

	adapterStart := time.Now()
	for !a.doStop.WaitFor(a.pollInterval) {
		// The makeOneRequest function handles error
		// handling and fatal error handling.
		items := a.makeOneRequest(adapterStart)
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
				if err != uspclient.ErrorClosed {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				}
				a.doStop.Set()
				return
			}
		}
	}
}

func (a *HubSpotAdapter) makeOneRequest(notBefore time.Time) []utils.Dict {
	if a.scan == nil {
		currentTime := time.Now()
		var start string

		if t := currentTime.Add(-overlapPeriod); t.Before(notBefore) {
			start = notBefore.UTC().Format(time.RFC3339)
		} else {
			start = currentTime.Add(-overlapPeriod).UTC().Format(time.RFC3339)
		}
		until := currentTime.UTC().Format(time.RFC3339)

		a.scan = &auditScan{start: start, until: until, seen: make(map[string]bool)}
	}
	scan := a.scan

	for {
		// Prepare the request.
		req, err := http.NewRequest("GET", a.apiURL, nil)
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s starting at %s until %s", a.apiURL, scan.start, scan.until))
		if err != nil {
			a.doStop.Set()
			return nil
		}
		q := req.URL.Query()
		q.Set("occurredAfter", scan.start)
		q.Set("occurredBefore", scan.until)
		if scan.after != "" {
			q.Set("after", scan.after)
		}
		req.URL.RawQuery = q.Encode()

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.conf.AccessToken))

		// Issue the request.
		resp, err := a.httpClient.Do(req)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
			return nil
		}

		// Evaluate if success.
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			a.conf.ClientOptions.OnError(fmt.Errorf("hubspot api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, string(body), string(body)))
			return nil
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error: %v", err))
			return nil
		}

		// Parse the response.
		var response struct {
			Paging struct {
				Next struct {
					Link  string `json:"link"`
					After string `json:"after"`
				} `json:"next"`
			} `json:"paging"`
			Results []utils.Dict `json:"results"`
		}
		err = json.Unmarshal(body, &response)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("hubspot api invalid json: %v", err))
			return nil
		}
		//a.conf.ClientOptions.DebugLog(fmt.Sprintf("results: %s", response))

		// Collect items.
		items := response.Results
		var newItems []utils.Dict
		for _, item := range items {
			eventid, _ := item["id"].(string)
			if _, ok := a.dedupe[eventid]; ok || scan.seen[eventid] {
				continue
			}
			scan.seen[eventid] = true
			newItems = append(newItems, item)
		}
		scan.items = append(scan.items, newItems...)

		// Follow the opaque token without changing the time window.
		if response.Paging.Next.After == "" {
			break
		}
		scan.after = response.Paging.Next.After
	}

	for _, item := range scan.items {
		timestamp, _ := item["occurredAt"].(string)
		eventid, _ := item["id"].(string)
		epoch, _ := time.Parse(time.RFC3339, timestamp)
		a.dedupe[eventid] = epoch.Unix()
	}
	a.scan = nil

	// Cull old dedupe entries.
	for k, v := range a.dedupe {
		if v < time.Now().Add(-overlapPeriod).Unix() {
			delete(a.dedupe, k)
		}
	}

	return scan.items
}
