package usp_sublime

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
	defaultBaseURL = "https://platform.sublime.security"
	logsPath       = "/v0/audit-log/events"
	overlapPeriod  = 30 * time.Second
	pageLimit      = 500
)

type SublimeAdapter struct {
	conf       SublimeConfig
	uspClient  utils.Shipper
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx    context.Context
	dedupe map[string]int64
}

type SublimeConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ApiKey        string                  `json:"api_key" yaml:"api_key"`
	BaseURL       string                  `json:"base_url" yaml:"base_url"`
	Filters []utils.FilterPattern `json:"filters,omitempty" yaml:"filters,omitempty"`
}

func (c *SublimeConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ApiKey == "" {
		return errors.New("missing api key")
	}
	if c.BaseURL == "" {
		c.BaseURL = defaultBaseURL
	}
	return nil
}

func NewSublimeAdapter(conf SublimeConfig) (*SublimeAdapter, chan struct{}, error) {
	var err error
	a := &SublimeAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
		dedupe: make(map[string]int64),
	}

	client, err := uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	// Wrap with filtering if configured
	if len(conf.Filters) > 0 {
		filtered, err := utils.NewFilteredClient(client, conf.Filters, conf.ClientOptions.DebugLog)
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

func (a *SublimeAdapter) Close() error {
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

func (a *SublimeAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s%s events exiting", a.conf.BaseURL, logsPath))

	since := time.Now()

	for !a.doStop.WaitFor(30 * time.Second) {
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

func (a *SublimeAdapter) makeOneRequest(since time.Time) ([]utils.Dict, time.Time, error) {
	var allItems []utils.Dict
	var offset int
	lastDetectionTime := since

	for {
		url := fmt.Sprintf("%s%s?limit=%d&offset=%d", a.conf.BaseURL, logsPath, pageLimit, offset)
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s", url))

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			a.doStop.Set()
			return nil, lastDetectionTime, err
		}

		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.conf.ApiKey))
		req.Header.Set("Accept", "application/json")

		resp, err := a.httpClient.Do(req)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
			return nil, lastDetectionTime, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			a.conf.ClientOptions.OnError(fmt.Errorf("sublime api non-200: %s\nRESPONSE: %s", resp.Status, string(body)))
			return nil, lastDetectionTime, err
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("read body error: %v", err))
			return nil, lastDetectionTime, err
		}

		var response struct {
			Events []utils.Dict `json:"events"`
		}
		err = json.Unmarshal(body, &response)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("sublime api invalid json: %v", err))
			return nil, lastDetectionTime, err
		}

		var newItems []utils.Dict
		for _, event := range response.Events {
			id, _ := event["id"].(string)
			createdAtStr, _ := event["created_at"].(string)

			if _, seen := a.dedupe[id]; seen {
				continue
			}

			createdAt, err := time.Parse(time.RFC3339Nano, createdAtStr)
			if err != nil {
				continue
			}

			if createdAt.After(since) {
				a.dedupe[id] = createdAt.Unix()
				newItems = append(newItems, event)
				if createdAt.After(lastDetectionTime) {
					lastDetectionTime = createdAt
				}
			}
		}

		allItems = append(allItems, newItems...)

		if len(response.Events) < pageLimit {
			break
		}
		offset += pageLimit
	}

	for k, v := range a.dedupe {
		if v < time.Now().Add(-overlapPeriod).Unix() {
			delete(a.dedupe, k)
		}
	}

	return allItems, lastDetectionTime, nil
}
