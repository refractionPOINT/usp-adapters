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
	uspClient  *uspclient.Client
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	nextPage string
	lastTime int64

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

func NewSlackAdapter(conf SlackConfig) (*SlackAdapter, chan struct{}, error) {
	var err error
	a := &SlackAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
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

	// Initianlize the search parameters
	a.lastTime = time.Now().Unix()

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
	for !a.doStop.WaitFor(5 * time.Second) {
		// Do a non-paged fetch based on time.
		entries, nextPage, err := a.makeOneContentRequest(a.lastTime, "")
		if err != nil {
			return
		}

		// If we got data, feed it.
		if err := a.shipEntries(entries); err != nil {
			return
		}

		// If a page was given back, exhaust the cursor.
		for nextPage != "" {
			entries, nextPage, err = a.makeOneContentRequest(a.lastTime, nextPage)
			if err != nil {
				return
			}

			// If we got data, feed it.
			if len(entries) != 0 {
				if err := a.shipEntries(entries); err != nil {
					return
				}
			}
		}
	}
}

func (a *SlackAdapter) shipEntries(entries []utils.Dict) error {
	if len(entries) == 0 {
		return nil
	}
	now := uint64(time.Now().UnixNano() / int64(time.Millisecond))
	for _, event := range entries {
		msg := &protocol.DataMessage{
			JsonPayload: event,
			TimestampMs: now,
		}
		if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
			if err == uspclient.ErrorBufferFull {
				a.conf.ClientOptions.OnWarning("stream falling behind")
				err = a.uspClient.Ship(msg, 0)
			}
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				return err
			}
		}
	}
	return nil
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
		q.Add("latest", fmt.Sprintf("%d", lastTime))
	}
	if page != "" {
		q.Add("cursor", page)
	}
	req.URL.RawQuery = q.Encode()

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
