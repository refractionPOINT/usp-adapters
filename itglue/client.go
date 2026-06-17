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

	defaultPollInterval = 30 * time.Second
)

type opRequest struct {
	Limit     int    `json:"page[size],omitempty"`
	StartTime string `json:"filter[created_at],omitempty"`
	Sort      string `json:"sort,omitempty"`
}

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type ITGlueAdapter struct {
	conf       ITGlueConfig
	uspClient  uspSink
	httpClient *http.Client

	baseURL      string
	pollInterval time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type ITGlueConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Token         string                  `json:"token" yaml:"token"`

	// BaseURL overrides the IT Glue API root. Empty means the public API:
	// https://api.itglue.com
	BaseURL string `json:"base_url" yaml:"base_url"`

	// PollInterval overrides the wait between polls of the logs endpoint
	// (default 30s). It is not settable through a config file; it exists as a
	// seam for tests.
	PollInterval time.Duration `json:"-" yaml:"-"`
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

func NewITGlueAdapter(ctx context.Context, conf ITGlueConfig) (*ITGlueAdapter, chan struct{}, error) {
	return newITGlueAdapter(ctx, conf, nil)
}

// newITGlueAdapter is the implementation behind NewITGlueAdapter. When sink is
// non-nil it is used in place of a real LimaCharlie client -- the seam tests
// use to capture shipped events.
func newITGlueAdapter(ctx context.Context, conf ITGlueConfig, sink uspSink) (*ITGlueAdapter, chan struct{}, error) {
	a := &ITGlueAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
	}

	a.baseURL = conf.BaseURL
	if a.baseURL == "" {
		a.baseURL = URL
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
	for !a.doStop.WaitFor(a.pollInterval) {
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
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s?filter[created_at]=%s&sort=created_at&page[size]=1000", a.baseURL, url, formattedTime), nil)
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
