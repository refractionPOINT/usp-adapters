package usp_okta

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
	logsURL       = "/api/v1/logs"
	overlapPeriod = 30 * time.Minute
)

type opRequest struct {
	Limit     int    `json:"page[size],omitempty"`
	StartTime string `json:"filter[created_at],omitempty"`
	Sort      string `json:"sort,omitempty"`
}

type OktaAdapter struct {
	conf       OktaConfig
	uspClient  *uspclient.Client
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context

	dedupe map[string]int64
}

type OktaConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ApiKey        string                  `json:"apikey" yaml:"apikey"`
	URL           string                  `json:"url" yaml:"url"`
}

func (c *OktaConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.URL == "" {
		return errors.New("missing url")
	}
	if c.ApiKey == "" {
		return errors.New("missing api key")
	}
	return nil
}

func NewOktaAdapter(conf OktaConfig) (*OktaAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	var err error
	a := &OktaAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
		dedupe: make(map[string]int64),
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

	a.wgSenders.Add(1)
	go a.fetchEvents(logsURL)

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *OktaAdapter) Close() error {
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

func (a *OktaAdapter) fetchEvents(url string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", url))

	adapterStart := time.Now()
	for !a.doStop.WaitFor(30 * time.Second) {
		// The makeOneRequest function handles error
		// handling and fatal error handling.
		items := a.makeOneRequest(url, adapterStart)
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

func (a *OktaAdapter) makeOneRequest(url string, notBefore time.Time) []utils.Dict {

	// Prepare the request body.
	reqData := opRequest{}
	b, err := json.Marshal(reqData)
	if err != nil {
		a.doStop.Set()
		return nil
	}

	// Get request timestamp
	currentTime := time.Now()
	var start string
	if t := currentTime.Add(-overlapPeriod); t.Before(notBefore) {
		start = notBefore.UTC().Format(time.RFC3339)
	} else {
		start = currentTime.Add(-overlapPeriod).UTC().Format(time.RFC3339)
	}
	until := currentTime.UTC().Format(time.RFC3339)

	// Prepare the request.
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s?since=%s&until=%s", a.conf.URL, url, start, until), nil)
	// a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s%s starting at %s until %s", a.conf.URL, url, start, until))
	if err != nil {
		a.doStop.Set()
		return nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("SSWS %s", a.conf.ApiKey))

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
		//a.conf.ClientOptions.DebugLog(fmt.Sprintf("response: %s", body))
		//a.conf.ClientOptions.DebugLog(fmt.Sprintf("error code: %s", resp.StatusCode))

		a.conf.ClientOptions.OnError(fmt.Errorf("okta api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, string(b), string(body)))
		return nil
	}

	body, _ := ioutil.ReadAll(resp.Body)
	//a.conf.ClientOptions.DebugLog(fmt.Sprintf("response data: %s", body))

	// Parse the response.
	var data []utils.Dict
	err = json.Unmarshal(body, &data)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("okta api invalid json: %v", err))
		return nil
	}

	// Report if a cursor was returned
	// as well as the items.
	items := data

	var newItems []utils.Dict

	for _, item := range items {
		timestamp, _ := item["published"].(string)
		eventid, _ := item["uuid"].(string)
		if _, ok := a.dedupe[eventid]; ok {
			continue
		}
		epoch, _ := time.Parse(time.RFC3339, timestamp)
		a.dedupe[eventid] = epoch.Unix()
		newItems = append(newItems, item)
	}

	// Cull old dedupe entries.
	for k, v := range a.dedupe {
		if v < time.Now().Add(-overlapPeriod).Unix() {
			delete(a.dedupe, k)
		}
	}

	//a.conf.ClientOptions.DebugLog(fmt.Sprintf("response data: %s", timestamp))

	return newItems
}
