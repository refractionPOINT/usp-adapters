package usp_slack

import (
	"bytes"
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
	apiURL = "https://api.slack.com/audit/v1/logs"
)

type SlackAdapter struct {
	conf       SlackConfig
	uspClient  utils.Shipper
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type slackResponse struct {
	Entries  []utils.Dict `json:"entries,omitempty"`
	Metadata struct {
		NextCursor string `json:"next_cursor,omitempty"`
	} `json:"response_metadata"`
}

type SlackConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Token         string                  `json:"token" yaml:"token"`
	Filters    []utils.FilterPattern `json:"filters,omitempty" yaml:"filters,omitempty"`
	FilterMode utils.FilterMode       `json:"filter_mode,omitempty" yaml:"filter_mode,omitempty"`
}

func (c *SlackConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Token == "" {
		return errors.New("missing token")
	}
	return nil
}

func NewSlackAdapter(ctx context.Context, conf SlackConfig) (*SlackAdapter, chan struct{}, error) {
	var err error
	a := &SlackAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
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
	go func() {
		defer a.wgSenders.Done()
		defer close(a.chStopped)
		defer a.conf.ClientOptions.DebugLog("fetching of events exiting")
		a.fetchEvents()
	}()

	return a, a.chStopped, nil
}

func (a *SlackAdapter) Close() error {
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

func (a *SlackAdapter) fetchEvents() {
	// Initianlize the search parameters
	lastTime := time.Now().Unix()

	for !a.doStop.WaitFor(5 * time.Second) {
		isItemsProcessed := false

		// Do a non-paged fetch based on time.
		entries, nextPage, err := a.makeOneContentRequest(lastTime, "")
		if err != nil {
			return
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("got %d entries", len(entries)))

		// If we got data, feed it.
		if len(entries) != 0 {
			isItemsProcessed = true
			lastTime, err = a.shipEntries(entries)
			if err != nil {
				return
			}
		}

		// If a page was given back, exhaust the cursor.
		for nextPage != "" {
			entries, nextPage, err = a.makeOneContentRequest(lastTime, nextPage)
			if err != nil {
				return
			}
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("got %d entries from page", len(entries)))

			// If we got data, feed it.
			if len(entries) != 0 {
				isItemsProcessed = true
				lastTime, err = a.shipEntries(entries)
				if err != nil {
					return
				}
			}
		}

		if isItemsProcessed {
			lastTime++
		}
	}
}

func (a *SlackAdapter) shipEntries(entries []utils.Dict) (int64, error) {
	latestTs := uint64(0)
	now := uint64(time.Now().UnixNano() / int64(time.Millisecond))
	for _, event := range entries {
		ts, _ := event.GetInt("date_create")
		if ts > latestTs {
			latestTs = ts
		}
		msg := &protocol.DataMessage{
			JsonPayload: event,
			TimestampMs: now,
		}
		if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
			if err == uspclient.ErrorBufferFull {
				a.conf.ClientOptions.OnWarning("stream falling behind")
				err = a.uspClient.Ship(msg, 1*time.Hour)
			}
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				return int64(latestTs), err
			}
		}
	}
	return int64(latestTs), nil
}

func (a *SlackAdapter) makeOneContentRequest(lastTime int64, page string) ([]utils.Dict, string, error) {
	// Prepare the request.
	req, err := http.NewRequest("GET", apiURL, &bytes.Buffer{})
	if err != nil {
		a.doStop.Set()
		a.conf.ClientOptions.OnError(fmt.Errorf("http.NewRequest(): %v", err))
		return nil, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.conf.Token))

	q := req.URL.Query()
	if lastTime != 0 {
		q.Add("oldest", fmt.Sprintf("%d", lastTime))
	}
	if page != "" {
		q.Add("cursor", page)
	}
	req.URL.RawQuery = q.Encode()
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("query: %s", req.URL.RawQuery))

	// Issue the request.
	resp, err := a.httpClient.Do(req)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
		return nil, "", err
	}
	defer resp.Body.Close()

	// Evaluate if success.
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		a.conf.ClientOptions.OnError(fmt.Errorf("slack content api non-200: %s\nRESPONSE: %s", resp.Status, string(body)))
		return nil, "", err
	}

	// Parse the response.
	respData := slackResponse{}
	jsonDecoder := json.NewDecoder(resp.Body)
	if err := jsonDecoder.Decode(&respData); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("slack content api invalid json: %v", err))
		return nil, "", err
	}

	return respData.Entries, respData.Metadata.NextCursor, nil
}
