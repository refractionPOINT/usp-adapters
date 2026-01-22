package usp_pandadoc

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
	logsEndpoint  = "https://api.pandadoc.com/public/v1/logs"
	overlapPeriod = 30 * time.Second
)

type opRequest struct {
	Limit     int    `json:"page[size],omitempty"`
	StartTime string `json:"filter[created_at],omitempty"`
	Sort      string `json:"sort,omitempty"`
}

type PandaDocAdapter struct {
	conf       PandaDocConfig
	uspClient  utils.Shipper
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context

	dedupe map[string]int64
}

type PandaDocConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ApiKey        string                  `json:"api_key" yaml:"api_key"`
	Filters    []utils.FilterPattern `json:"filters,omitempty" yaml:"filters,omitempty"`
	FilterMode utils.FilterMode       `json:"filter_mode,omitempty" yaml:"filter_mode,omitempty"`
}

func (c *PandaDocConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ApiKey == "" {
		return errors.New("missing api key")
	}

	return nil
}

func NewPandaDocAdapter(ctx context.Context, conf PandaDocConfig) (*PandaDocAdapter, chan struct{}, error) {
	var err error
	a := &PandaDocAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
		dedupe: make(map[string]int64),
	}

	client, err := uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	// Wrap with filtering if configured
	if len(conf.Filters) > 0 {
		filtered, err := utils.NewFilteredClient(client, conf.Filters, conf.FilterMode, conf.ClientOptions.DebugLog)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create filter: %w", err)
		}
		a.uspClient = filtered
	} else {
		a.uspClient = client
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

func (a *PandaDocAdapter) Close() error {
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

func (a *PandaDocAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", logsEndpoint))

	since := time.Now()

	for !a.doStop.WaitFor(30 * time.Second) {
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

func (a *PandaDocAdapter) makeOneRequest(since time.Time) ([]utils.Dict, time.Time, error) {
	var allItems []utils.Dict
	currentTime := time.Now()
	var start string
	var lastDetectionTime time.Time

	if t := currentTime.Add(-overlapPeriod); t.Before(since) {
		start = since.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000")
	} else {
		start = currentTime.Add(-overlapPeriod).UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000")
	}
	until := currentTime.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000")

	for page := 1; ; page++ {
		// Prepare the request.
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s?since=%s&to=%s&page=%d", logsEndpoint, start, until, page))
		req, err := http.NewRequest("GET", fmt.Sprintf("%s?since=%s&to=%s&page=%d", logsEndpoint, start, until, page), nil)
		if err != nil {
			a.doStop.Set()
			return nil, lastDetectionTime, err
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", fmt.Sprintf("Api-Key %s", a.conf.ApiKey))

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
			a.conf.ClientOptions.OnError(fmt.Errorf("pandadoc api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, string(body), string(body)))
			return nil, lastDetectionTime, err
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error: %v", err))
			return nil, lastDetectionTime, err
		}

		// Parse the response.
		var response struct {
			Logs []utils.Dict `json:"results"`
		}
		err = json.Unmarshal(body, &response)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("pandadoc api invalid json: %v", err))
			return nil, lastDetectionTime, err
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("results: %s", response))

		// Collect items.
		items := response.Logs
		var newItems []utils.Dict
		lastDetectionTime = since
		for _, item := range items {
			timestamp, _ := item["request_time"].(string)
			eventid, _ := item["id"].(string)
			if _, ok := a.dedupe[eventid]; ok {
				continue
			}
			epoch, _ := time.Parse(time.RFC3339Nano, timestamp)
			a.dedupe[eventid] = epoch.Unix()
			newItems = append(newItems, item)
			lastDetectionTime = epoch
		}
		allItems = append(allItems, newItems...)

		// There is no paging data returned in the requests so you only know if there is more data if you iterate through
		// additional pages after the first 100 items are returned
		if len(items) < 100 {
			break
		}
	}

	// Cull old dedupe entries.
	for k, v := range a.dedupe {
		if v < time.Now().Add(-overlapPeriod).Unix() {
			delete(a.dedupe, k)
		}
	}

	return allItems, lastDetectionTime, nil
}
