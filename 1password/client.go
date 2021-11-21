package usp_1password

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-essentials"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

const (
	itemsURL = "/api/v1/itemusages"
	usersURL = "/api/v1/signinattempts"
)

type opRequest struct {
	Limit     int    `json:"limit,omitempty"`
	StartTime string `json:"start_time,omitempty"`
	EndTime   string `json:"end_time,omitempty"`
	Cursor    string `json:"cursor,omitempty"`
}

var URL = map[string]string{
	"business":   "https://events.1password.com",
	"enterprise": "https://events.ent.1password.com",
	"ca":         "https://events.1password.ca",
	"eu":         "https://events.1password.eu",
}

type OnePasswordAdapter struct {
	conf       OnePasswordConfig
	dbgLog     func(string)
	uspClient  *uspclient.Client
	httpClient *http.Client

	endpoint string

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *essentials.Event

	ctx context.Context
}

type OnePasswordConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Token         string                  `json:"token" yaml:"token"`
	Endpoint      string                  `json:"endpoint" yaml:"endpoint"`
}

func NewOnePasswordpAdapter(conf OnePasswordConfig) (*OnePasswordAdapter, chan struct{}, error) {
	var err error
	a := &OnePasswordAdapter{
		conf: conf,
		dbgLog: func(s string) {
			if conf.ClientOptions.DebugLog == nil {
				return
			}
			conf.ClientOptions.DebugLog(s)
		},
		ctx:    context.Background(),
		doStop: essentials.NewEvent(),
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

	a.wgSenders.Add(2)
	go a.fetchEvents(itemsURL)
	go a.fetchEvents(usersURL)

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *OnePasswordAdapter) Close() error {
	a.dbgLog("closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	_, err := a.uspClient.Close()
	a.httpClient.CloseIdleConnections()

	return err
}

func (a *OnePasswordAdapter) fetchEvents(url string) {
	defer a.wgSenders.Done()
	defer a.dbgLog(fmt.Sprintf("fetching of %s events exiting", url))

	lastCursor := ""
	for !a.doStop.WaitFor(30 * time.Second) {
		// The makeOneRequest function handles error
		// handling and fatal error handling.
		items, newCursor := a.makeOneRequest(url, lastCursor)
		lastCursor = newCursor
		if items == nil {
			continue
		}

		for _, item := range items {
			ts := uint64(0)
			if tsString, ok := item.GetString("timestamp"); ok && tsString != "" {
				if t, err := time.Parse(time.RFC3339, tsString); err == nil {
					ts = uint64(t.UnixNano() / int64(time.Millisecond))
				}
			}
			if ts == 0 {
				ts = uint64(time.Now().UnixNano() / int64(time.Millisecond))
			}
			et := item.FindOneString("category")
			if et == "" {
				et = "item_usage"
			}
			msg := &protocol.DataMessage{
				JsonPayload: item,
				TimestampMs: ts,
				EventType:   et,
			}
			if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
				if err == uspclient.ErrorBufferFull {
					a.dbgLog("stream falling behind")
					err = a.uspClient.Ship(msg, 0)
				}
				if err != nil {
					a.dbgLog(fmt.Sprintf("Ship(): %v", err))
				}
				a.doStop.Set()
				return
			}
		}
	}
}

func (a *OnePasswordAdapter) makeOneRequest(url string, lastCursor string) ([]essentials.Dict, string) {
	// Prepare the request body.
	reqData := opRequest{}
	if lastCursor != "" {
		reqData.Cursor = lastCursor
	} else {
		a.dbgLog(fmt.Sprintf("requesting from %s starting now", url))
		reqData.StartTime = time.Now().Format(time.RFC3339)
		reqData.Limit = 1000
	}
	b, err := json.Marshal(reqData)
	if err != nil {
		a.doStop.Set()
		return nil, ""
	}

	// Prepare the request.
	req, err := http.NewRequest("POST", fmt.Sprintf("%s%s", a.endpoint, url), bytes.NewBuffer(b))
	if err != nil {
		a.doStop.Set()
		return nil, ""
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.conf.Token))

	// Issue the request.
	resp, err := a.httpClient.Do(req)
	if err != nil {
		a.dbgLog(fmt.Sprintf("http.Client.Do(): %v", err))
		return nil, lastCursor
	}
	defer resp.Body.Close()

	// Evaluate if success.
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		a.dbgLog(fmt.Sprintf("1password api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, string(b), string(body)))
		return nil, lastCursor
	}

	// Parse the response.
	respData := essentials.Dict{}
	jsonDecoder := json.NewDecoder(resp.Body)
	if err := jsonDecoder.Decode(&respData); err != nil {
		a.dbgLog(fmt.Sprintf("1password api invalid json: %v", err))
		return nil, lastCursor
	}

	// Report if a cursor was returned
	// as well as the items.
	lastCursor = respData.FindOneString("cursor")
	items, _ := respData.GetListOfDict("items")
	return items, lastCursor
}
