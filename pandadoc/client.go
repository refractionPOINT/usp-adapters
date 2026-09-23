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
	defaultLogsEndpoint = "https://api.pandadoc.com/public/v1/logs"

	// overlapPeriod is how far back each poll reaches. It must exceed the
	// API's ingestion lag -- the delay between an event happening and the log
	// list returning it -- or every poll asks only for a slice of time the
	// backend has not caught up to yet and returns nothing. The resulting
	// window overlap is absorbed by the dedupe map.
	overlapPeriod = 30 * time.Minute

	// defaultPollInterval is how long fetchEvents idles between polling ticks.
	defaultPollInterval = 30 * time.Second
)

// queryTimeLayout is the layout the adapter renders the since/to query bounds
// in, and the layout PandaDoc uses for request_time in its responses: ISO-8601
// with millisecond precision and no zone designator, e.g.
// "2024-07-15T18:59:38.000". Note this is NOT time.RFC3339Nano, which requires
// a zone.
const queryTimeLayout = "2006-01-02T15:04:05.000"

// parseLogTime parses a PandaDoc request_time, accepting the documented
// zone-less form and falling back to RFC3339Nano for zoned values.
func parseLogTime(s string) (time.Time, error) {
	if t, err := time.ParseInLocation(queryTimeLayout, s, time.UTC); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

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

type PandaDocAdapter struct {
	conf       PandaDocConfig
	uspClient  uspSink
	httpClient *http.Client

	logsURL      string
	pollInterval time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context

	dedupe map[string]int64
}

type PandaDocConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ApiKey        string                  `json:"api_key" yaml:"api_key"`

	// URL overrides the PandaDoc audit-logs endpoint. Empty means the public
	// PandaDoc API (https://api.pandadoc.com/public/v1/logs).
	URL string `json:"url" yaml:"url"`

	// PollInterval is the wait between polls of the logs endpoint. It is not
	// settable through a config file; it exists as a seam for tests. Empty
	// means the default of 30 seconds.
	PollInterval time.Duration `json:"-" yaml:"-"`
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
	return newPandaDocAdapter(ctx, conf, nil)
}

// newPandaDocAdapter is the implementation behind NewPandaDocAdapter. When
// sink is non-nil it is used in place of a real LimaCharlie client -- the seam
// tests use to capture shipped events.
func newPandaDocAdapter(ctx context.Context, conf PandaDocConfig, sink uspSink) (*PandaDocAdapter, chan struct{}, error) {
	a := &PandaDocAdapter{
		conf:         conf,
		ctx:          context.Background(),
		doStop:       utils.NewEvent(),
		dedupe:       make(map[string]int64),
		logsURL:      conf.URL,
		pollInterval: conf.PollInterval,
	}
	if a.logsURL == "" {
		a.logsURL = defaultLogsEndpoint
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
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", a.logsURL))

	// notBefore floors how far back the lookback may reach. It is deliberately
	// fixed, not a moving cursor: the window start is
	// max(notBefore, now-overlapPeriod), so feeding a moving cursor in here
	// shrinks the window on every event instead of extending it.
	//
	// The floor sits one overlapPeriod before start-up so records still
	// working through the backend's ingestion lag at restart are not dropped.
	// The cost is that a restart may re-ship up to one overlap window, since
	// the dedupe map is in-memory; duplicates beat gaps.
	notBefore := time.Now().Add(-overlapPeriod)

	for !a.doStop.WaitFor(a.pollInterval) {
		// The makeOneRequest function handles error
		// handling and fatal error handling.
		items, _, _ := a.makeOneRequest(notBefore)
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
		start = since.UTC().Truncate(time.Millisecond).Format(queryTimeLayout)
	} else {
		start = currentTime.Add(-overlapPeriod).UTC().Truncate(time.Millisecond).Format(queryTimeLayout)
	}
	until := currentTime.UTC().Truncate(time.Millisecond).Format(queryTimeLayout)

	for page := 1; ; page++ {
		// Prepare the request.
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s?since=%s&to=%s&page=%d", a.logsURL, start, until, page))
		req, err := http.NewRequest("GET", fmt.Sprintf("%s?since=%s&to=%s&page=%d", a.logsURL, start, until, page), nil)
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
			// err is nil here; build a real one so callers can tell a failure
			// apart from a successful empty poll.
			err = fmt.Errorf("pandadoc api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, req.URL.String(), string(body))
			a.conf.ClientOptions.OnError(err)
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
			epoch, perr := parseLogTime(timestamp)
			if perr != nil {
				// Without a usable request_time the dedupe entry would be
				// culled immediately and the record would re-ship every poll.
				// Stamp it with now so it survives the overlap window.
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("unparseable request_time %q: %v", timestamp, perr))
				epoch = time.Now()
			}
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
