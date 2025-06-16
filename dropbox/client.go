package usp_dropbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const dropboxEventsURL = "https://api.dropboxapi.com/2/team_log/get_events"

type DropboxConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	AccessToken   string                  `json:"access_token" yaml:"access_token"`
}

func (c *DropboxConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.AccessToken == "" {
		return errors.New("missing Dropbox access token")
	}
	return nil
}

type DropboxAdapter struct {
	conf        DropboxConfig
	uspClient   *uspclient.Client
	httpClient  *http.Client
	chStopped   chan struct{}
	wgSenders   sync.WaitGroup
	doStop      *utils.Event
	ctx         context.Context
	dedupe      map[string]int64
	cursor      string
	initialized bool
}

func NewDropboxAdapter(conf DropboxConfig) (*DropboxAdapter, chan struct{}, error) {
	a := &DropboxAdapter{
		conf:        conf,
		ctx:         context.Background(),
		doStop:      utils.NewEvent(),
		dedupe:      make(map[string]int64),
		cursor:      "",
		initialized: false,
	}

	var err error
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
	a.wgSenders.Add(1)
	go a.fetchEvents()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *DropboxAdapter) Close() error {
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

func (a *DropboxAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("Dropbox event collection stopping")

	var lastEventTime string

	if !a.initialized {
		items, cursor, lastTime, err := a.makeOneRequest("", "")
		a.initialized = true
		if err == nil {
			a.cursor = cursor
			lastEventTime = lastTime
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
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
					a.doStop.Set()
					return
				}
			}
		}
	}

	for !a.doStop.WaitFor(30 * time.Second) {
		items, newCursor, lastTime, _ := a.makeOneRequest(a.cursor, lastEventTime)
		if newCursor != "" {
			a.cursor = newCursor
		}
		if lastTime != "" {
			lastEventTime = lastTime
		}
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
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
					a.doStop.Set()
					return
				}
			}
		}
	}
}

func (a *DropboxAdapter) makeOneRequest(cursor, lastEventTime string) ([]utils.Dict, string, string, error) {
	var allItems []utils.Dict

	var reqUrl string
	var bodyPayload []byte
	var err error

	// Add 1 second to lastEventTime to avoid sending the same event repeatedly
	var startTime string
	if lastEventTime != "" {
		parsedTime, parseErr := time.Parse(time.RFC3339, lastEventTime)
		if parseErr == nil {
			startTime = parsedTime.Add(1 * time.Second).UTC().Format(time.RFC3339)
		} else {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("failed to parse lastEventTime: %v", parseErr))
			startTime = time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
		}
	} else {
		startTime = time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
	}
	endTime := time.Now().UTC().Format(time.RFC3339)

	reqUrl = dropboxEventsURL
	bodyPayload, _ = json.Marshal(map[string]interface{}{
		"limit": 100,
		"time": map[string]interface{}{
			".tag":       "range",
			"start_time": startTime,
			"end_time":   endTime,
		},
	})

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting: %s, start_time: %s, end_time: %s, cursor: %s", reqUrl, startTime, endTime, cursor))

	req, err := http.NewRequest("POST", reqUrl, bytes.NewBuffer(bodyPayload))
	if err != nil {
		return nil, cursor, "", err
	}
	req.Header.Set("Authorization", "Bearer "+a.conf.AccessToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, cursor, "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("data: %s", string(body)))
	if resp.StatusCode != 200 {
		a.conf.ClientOptions.OnError(fmt.Errorf("dropbox api non-200: %s", string(body)))
		return nil, cursor, "", fmt.Errorf("non-200 from Dropbox")
	}

	var parsed struct {
		Events []utils.Dict `json:"events"`
		Cursor string       `json:"cursor"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("unmarshal: %v", err))
		return nil, cursor, "", err
	}

	allItems = append(allItems, parsed.Events...)

	// Update the existing lastEventTime
	if len(parsed.Events) > 0 {
		if ts, ok := parsed.Events[len(parsed.Events)-1]["timestamp"].(string); ok {
			lastEventTime = ts
		}
	}

	return allItems, parsed.Cursor, lastEventTime, nil
}
