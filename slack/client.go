package usp_slack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	defaultApiURL       = "https://api.slack.com/audit/v1/logs"
	defaultPollInterval = 5 * time.Second
	// maxErrorBackoff caps the wait between polls while requests keep
	// failing; the wait starts at the poll interval and doubles per failure.
	maxErrorBackoff = 5 * time.Minute
	// maxErrorBodyLog bounds how much of an error response body is logged.
	maxErrorBodyLog = 1024
)

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type SlackAdapter struct {
	conf       SlackConfig
	uspClient  uspSink
	httpClient *http.Client

	apiURL       string
	pollInterval time.Duration

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

	// ApiURL overrides the Slack Audit Logs API endpoint. Empty means the
	// public endpoint: https://api.slack.com/audit/v1/logs
	ApiURL string `json:"api_url" yaml:"api_url"`

	// PollInterval overrides the wait between polls of the audit logs
	// endpoint (default 5s). It is not settable through a config file; it
	// exists as a seam for tests.
	PollInterval time.Duration `json:"-" yaml:"-"`
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
	return newSlackAdapter(ctx, conf, nil)
}

// newSlackAdapter is the implementation behind NewSlackAdapter. When sink is
// nil a real LimaCharlie client is created from the config; tests pass an
// in-memory sink instead.
func newSlackAdapter(ctx context.Context, conf SlackConfig, sink uspSink) (*SlackAdapter, chan struct{}, error) {
	a := &SlackAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
	}

	a.apiURL = conf.ApiURL
	if a.apiURL == "" {
		a.apiURL = defaultApiURL
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
	go func() {
		defer a.wgSenders.Done()
		defer close(a.chStopped)
		defer a.conf.ClientOptions.DebugLog("fetching of events exiting")
		a.fetchEvents()
	}()

	return a, a.chStopped, nil
}

func (a *SlackAdapter) Close() error {
	defer utils.StopBlockedProducers(a.uspClient)()
	a.conf.ClientOptions.DebugLog("closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
	a.httpClient.CloseIdleConnections()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *SlackAdapter) fetchEvents() {
	// Initianlize the search parameters
	lastTime := time.Now().Unix()

	// A failed request (e.g. the API unreachable or a TLS error) is retried
	// from the same point after a growing wait, rather than every poll.
	failures := 0
	wait := a.pollInterval
	retryLater := func() {
		failures++
		wait = errorBackoff(a.pollInterval, failures)
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("request failed %d time(s) in a row, retrying in %v", failures, wait))
	}

	for !a.doStop.WaitFor(wait) {
		wait = a.pollInterval
		isItemsProcessed := false

		// Do a non-paged fetch based on time.
		entries, nextPage, err := a.makeOneContentRequest(lastTime, "")
		if err != nil {
			retryLater()
			continue
		}
		failures = 0
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
				retryLater()
				break
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

// errorBackoff is the wait before the next poll after the given number of
// consecutive failures: the poll interval, doubled per failure, capped.
func errorBackoff(pollInterval time.Duration, failures int) time.Duration {
	wait := pollInterval
	for i := 1; i < failures && wait < maxErrorBackoff; i++ {
		wait *= 2
	}
	if wait > maxErrorBackoff {
		wait = maxErrorBackoff
	}
	return wait
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
				if err != uspclient.ErrorClosed {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				}
				return int64(latestTs), err
			}
		}
	}
	return int64(latestTs), nil
}

func (a *SlackAdapter) makeOneContentRequest(lastTime int64, page string) ([]utils.Dict, string, error) {
	// Prepare the request.
	req, err := http.NewRequest("GET", a.apiURL, &bytes.Buffer{})
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
		body, _ := ioutil.ReadAll(io.LimitReader(resp.Body, maxErrorBodyLog+1))
		truncated := ""
		if len(body) > maxErrorBodyLog {
			body = body[:maxErrorBodyLog]
			truncated = "... (truncated)"
		}
		err := fmt.Errorf("slack content api non-200: %s\nRESPONSE: %s%s", resp.Status, string(body), truncated)
		a.conf.ClientOptions.OnError(err)
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
