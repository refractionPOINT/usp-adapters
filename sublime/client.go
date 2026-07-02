package usp_sublime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultBaseURL      = "https://platform.sublime.security"
	logsPath            = "/v0/audit-log/events"
	overlapPeriod       = 30 * time.Second
	pageLimit           = 500
	defaultPollInterval = 30 * time.Second

	// maxPagesPerPoll caps how deep a single poll will paginate. With the
	// created_at[gte] server-side filter a poll only ever walks the recent
	// window, so this is a safety backstop against a misbehaving API that
	// keeps returning full pages -- it prevents an unbounded offset walk.
	maxPagesPerPoll = 1000
)

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type SublimeAdapter struct {
	conf       SublimeConfig
	uspClient  uspSink
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

	// PollInterval overrides the fixed wait between polls of the audit log
	// API. It is not settable through a config file; it exists as a seam for
	// tests (the production interval stays the historical 30 seconds).
	PollInterval time.Duration `json:"-" yaml:"-"`
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
	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	return nil
}

func NewSublimeAdapter(ctx context.Context, conf SublimeConfig) (*SublimeAdapter, chan struct{}, error) {
	return newSublimeAdapter(ctx, conf, nil)
}

// newSublimeAdapter is the implementation behind NewSublimeAdapter. When sink
// is non-nil it is used in place of a real LimaCharlie client -- the seam
// tests use to capture shipped events.
func newSublimeAdapter(ctx context.Context, conf SublimeConfig, sink uspSink) (*SublimeAdapter, chan struct{}, error) {
	a := &SublimeAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
		dedupe: make(map[string]int64),
	}

	// The general adapter runner constructs the adapter without calling
	// Validate(), so backfill the defaults here as well. Without the base URL
	// default, an unset base_url produces a schemeless request URL and every
	// poll fails with `unsupported protocol scheme ""`; without the poll
	// interval default the loop would spin on WaitFor(0).
	if a.conf.BaseURL == "" {
		a.conf.BaseURL = defaultBaseURL
	}
	if a.conf.PollInterval <= 0 {
		a.conf.PollInterval = defaultPollInterval
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

	for !a.doStop.WaitFor(a.conf.PollInterval) {
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
	var pages int
	lastDetectionTime := since

	// Only ask the API for events in the recent window instead of walking the
	// entire audit log from offset 0 on every poll. The overlap is re-fetched
	// each time and the dedupe map suppresses re-shipping, so no event that the
	// old full-scan would have shipped is missed. Without this filter a large
	// backlog forces hundreds of ever-deeper offset pages per poll, which is
	// what makes a single request exceed the HTTP timeout on busy tenants.
	gteFilter := since.Add(-overlapPeriod).UTC().Format(time.RFC3339Nano)

	for {
		reqURL := fmt.Sprintf("%s%s?limit=%d&offset=%d&created_at[gte]=%s",
			a.conf.BaseURL, logsPath, pageLimit, offset, url.QueryEscape(gteFilter))
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s", reqURL))

		req, err := http.NewRequest("GET", reqURL, nil)
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
			err = fmt.Errorf("sublime api non-200: %s\nRESPONSE: %s", resp.Status, string(body))
			a.conf.ClientOptions.OnError(err)
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
		pages++
		if pages >= maxPagesPerPoll {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("sublime: stopping pagination after %d pages (offset=%d); remaining events will be picked up on the next poll", pages, offset))
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
