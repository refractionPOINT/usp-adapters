package usp_itglue

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
	logsURL = "/logs"
	URL     = "https://api.itglue.com"
)

type opRequest struct {
	Limit     int    `json:"page[size],omitempty"`
	StartTime string `json:"filter[created_at],omitempty"`
	Sort      string `json:"sort,omitempty"`
}

type ITGlueAdapter struct {
	conf       ITGlueConfig
	uspClient  utils.Shipper
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type ITGlueConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Token         string                  `json:"token" yaml:"token"`
	Filters       []string                `json:"filters,omitempty" yaml:"filters,omitempty"`
}

func (c *ITGlueConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Token == "" {
		return errors.New("missing token")
	}
	return nil
}

func NewITGlueAdapter(conf ITGlueConfig) (*ITGlueAdapter, chan struct{}, error) {
	var err error
	a := &ITGlueAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
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
	go a.fetchEvents(logsURL)

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *ITGlueAdapter) Close() error {
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

func (a *ITGlueAdapter) fetchEvents(url string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", url))

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

func (a *ITGlueAdapter) makeOneRequest(url string, lastCursor string) ([]utils.Dict, string) {

	// Prepare the request body.
	reqData := opRequest{}
	b, err := json.Marshal(reqData)
	if err != nil {
		a.doStop.Set()
		return nil, ""
	}

	// Get request timestamp
	currentTime := time.Now().UTC()
	timeFormat := "2006-01-02T15:04:05.000000Z07:00"
	thirtySecondsAgo := currentTime.Add(-30 * time.Second)
	formattedTime := thirtySecondsAgo.Format(timeFormat)

	// Prepare the request.
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s?filter[created_at]=%s&sort=created_at&page[size]=1000", URL, url, formattedTime), nil)
	//debugTimestamp := formattedTime
	if lastCursor != "" {
		req, err = http.NewRequest("GET", fmt.Sprintf("%s", lastCursor), nil)
		//	debugTimestamp = lastCursor
	}
	if err != nil {
		a.doStop.Set()
		return nil, ""
	}
	req.Header.Set("Content-Type", "application/vnd.api+json")
	req.Header.Set("x-api-key", a.conf.Token)

	//
	//a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s%s starting at %s", URL, url, debugTimestamp))

	// Issue the request.
	resp, err := a.httpClient.Do(req)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
		return nil, lastCursor
	}
	defer resp.Body.Close()

	// Evaluate if success.
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		//	a.conf.ClientOptions.DebugLog(fmt.Sprintf("response: %s", body))
		//	a.conf.ClientOptions.DebugLog(fmt.Sprintf("error code: %s", resp.StatusCode))

		a.conf.ClientOptions.OnWarning(fmt.Sprintf("itglue api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, string(b), string(body)))
		return nil, lastCursor
	}

	// Parse the response.
	respData := utils.Dict{}
	jsonDecoder := json.NewDecoder(resp.Body)
	if err := jsonDecoder.Decode(&respData); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("itglue api invalid json: %v", err))
		return nil, lastCursor
	}

	// Report if a cursor was returned
	// as well as the items.
	lastCursor = respData.FindOneString("next")
	items, _ := respData.GetListOfDict("data")
	//a.conf.ClientOptions.DebugLog(fmt.Sprintf("response data: %s", respData))

	return items, lastCursor
}
