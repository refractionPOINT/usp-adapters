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

	// Reduce pagination requests with the documented maximum. Windows that
	// reach count*page == 10000 still require subdivision.
	logsPageSize    = 1000
	logsWindowLimit = 10000
)

// queryTimeLayout is the layout the adapter renders the since/to query bounds
// in, and the layout PandaDoc uses for request_time in its responses: ISO-8601
// with millisecond precision and no zone designator, e.g.
// "2024-07-15T18:59:38.000". Note this is NOT time.RFC3339Nano, which requires
// a zone.
const queryTimeLayout = "2006-01-02T15:04:05.000"

// parseLogTime parses a PandaDoc request_time, accepting the documented
// zone-less form and falling back to RFC3339Nano for zoned values. UTC for
// zone-less values is an adapter assumption; the API does not specify a zone.
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
	now          func() time.Time
	wait         func(time.Duration) bool

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx    context.Context
	cancel context.CancelFunc

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

	a.ctx, a.cancel = context.WithCancel(a.ctx)
	a.now = time.Now
	a.wait = a.doStop.WaitFor
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
	defer utils.StopBlockedProducers(a.uspClient)()
	a.conf.ClientOptions.DebugLog("closing")
	a.doStop.Set()
	if a.cancel != nil {
		a.cancel()
	}
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
	a.httpClient.CloseIdleConnections()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *PandaDocAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", a.logsURL))

	var state *pollState
	delay := a.pollInterval
	for !a.wait(delay) {
		now := a.now()
		if state == nil {
			state = newPollState(now, overlapPeriod)
		}
		if err := state.failureError(now); err != nil {
			a.doStop.Set()
			a.conf.ClientOptions.OnError(err)
			return
		}
		start, end, expired := state.window(now)
		if expired {
			a.conf.ClientOptions.OnWarning("poll catch-up exceeded 24 hours; older unread records may be lost")
		}
		// Successful pages are already marked seen. Ship them before handling
		// errors: the consumer's OnError callback may terminate this instance.
		pollCtx, cancel := state.requestContext(a.ctx, now)
		items, _, pollErr := a.requestWindowContext(pollCtx, start, end)
		cancel()
		// Record failure before shipping partial pages so back-pressure cannot
		// postpone the continuous-failure deadline.
		retryDelay, retryable := state.retryDelay(pollErr, a.now())

		for _, item := range items {
			msg := &protocol.DataMessage{
				JsonPayload: item,
				TimestampMs: uint64(a.now().UnixNano() / int64(time.Millisecond)),
			}
			if err := a.uspClient.Ship(msg, state.shipTimeout(10*time.Second, a.now())); err != nil {
				if err == uspclient.ErrorBufferFull {
					a.conf.ClientOptions.OnWarning("stream falling behind")
					err = a.uspClient.Ship(msg, state.shipTimeout(time.Hour, a.now()))
				}
				if err == nil {
					continue
				}
				if err != uspclient.ErrorClosed {
					if failureErr := state.failureError(a.now()); failureErr != nil {
						a.conf.ClientOptions.OnError(failureErr)
					} else {
						a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
					}
				}
				a.doStop.Set()
				return
			}
		}
		if a.doStop.IsSet() && errors.Is(pollErr, context.Canceled) {
			return
		}
		// A late successful response cannot erase a deadline already passed.
		if err := state.failureError(a.now()); err != nil {
			a.doStop.Set()
			a.conf.ClientOptions.OnError(err)
			return
		}
		if pollErr == nil {
			state.complete(end)
			delay = a.pollInterval
		} else if retryable {
			delay = state.budgetDelay(retryDelay, a.now())
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("poll failed; backing off for up to %s: %v", delay, pollErr))
		} else {
			a.doStop.Set()
			a.conf.ClientOptions.OnError(pollErr)
			return
		}
	}
}

func (a *PandaDocAdapter) makeOneRequest(since time.Time) ([]utils.Dict, time.Time, error) {
	now := a.now()
	if floor := now.Add(-overlapPeriod); since.Before(floor) {
		since = floor
	}
	return a.requestWindow(since, now)
}

func (a *PandaDocAdapter) requestWindow(since, currentTime time.Time) ([]utils.Dict, time.Time, error) {
	return a.requestWindowContext(context.Background(), since, currentTime)
}

func (a *PandaDocAdapter) requestWindowContext(ctx context.Context, since, currentTime time.Time) ([]utils.Dict, time.Time, error) {
	var allItems []utils.Dict
	// Retain seen IDs for the entire unread range, including catch-up after
	// an outage. Culling at now-overlap would replay already shipped pages.
	for id, timestamp := range a.dedupe {
		if timestamp < since.Unix() {
			delete(a.dedupe, id)
		}
	}
	start := since.UTC().Truncate(time.Millisecond)
	until := currentTime.UTC().Truncate(time.Millisecond)
	var lastDetectionTime time.Time

	// PandaDoc forbids count*page > 10000 and returns no total. A full
	// final legal page is ambiguous, so revisit smaller time windows. Keep
	// collected records (and their dedupe entries) across those revisits.
	type logWindow struct{ start, end time.Time }
	windows := []logWindow{{start, until}}
	var windowErr error
	for len(windows) != 0 {
		window := windows[len(windows)-1]
		windows = windows[:len(windows)-1]
		start, until := window.start.Format(queryTimeLayout), window.end.Format(queryTimeLayout)
		for page := 1; page <= logsWindowLimit/logsPageSize; page++ {
			if a.doStop.IsSet() {
				return allItems, lastDetectionTime, context.Canceled
			}
			if err := ctx.Err(); err != nil {
				return allItems, lastDetectionTime, newPollRetry(err, "")
			}
			// Prepare the request.
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s?since=%s&to=%s&page=%d&count=%d", a.logsURL, start, until, page, logsPageSize))
			req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s?since=%s&to=%s&page=%d&count=%d", a.logsURL, start, until, page, logsPageSize), nil)
			if err == nil && ((req.URL.Scheme != "http" && req.URL.Scheme != "https") || req.URL.Host == "") {
				err = errors.New("logs URL must be an absolute HTTP or HTTPS URL")
			}
			if err != nil {
				return allItems, lastDetectionTime, err
			}

			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", fmt.Sprintf("Api-Key %s", a.conf.ApiKey))

			// Issue the request.
			resp, err := a.httpClient.Do(req)
			if err != nil {
				return allItems, lastDetectionTime, newPollRetry(fmt.Errorf("http.Client.Do(): %w", err), "")
			}
			body, readErr := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil && resp.StatusCode == http.StatusOK {
				return allItems, lastDetectionTime, newPollRetry(readErr, resp.Header.Get("Retry-After"))
			}

			// Evaluate if success.
			if resp.StatusCode != http.StatusOK {
				// err is nil here; build a real one so callers can tell a failure
				// apart from a successful empty poll.
				err = fmt.Errorf("pandadoc api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, req.URL.String(), string(body))
				if pollRetryableStatus(resp.StatusCode) {
					return allItems, lastDetectionTime, newPollRetry(err, resp.Header.Get("Retry-After"))
				}
				return allItems, lastDetectionTime, err
			}

			// Parse the response.
			var response struct {
				Logs []utils.Dict `json:"results"`
			}
			err = json.Unmarshal(body, &response)
			if err != nil {
				return allItems, lastDetectionTime, newPollRetry(fmt.Errorf("pandadoc api invalid json: %w", err), "")
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
					epoch = a.now()
				}
				a.dedupe[eventid] = epoch.Unix()
				newItems = append(newItems, item)
				lastDetectionTime = epoch
			}
			allItems = append(allItems, newItems...)

			if len(items) < logsPageSize {
				break
			}
			if page == logsWindowLimit/logsPageSize {
				if window.start.Equal(window.end) {
					// Even exactly 10000 records is ambiguous at this precision.
					// Report it rather than silently declaring the window complete.
					windowErr = fmt.Errorf("pandadoc pagination limit reached at %s: cannot subdivide below one millisecond", start)
					break
				}
				mid := window.start.Add(window.end.Sub(window.start) / 2).Truncate(time.Millisecond)
				rightStart := mid
				if mid.Equal(window.start) {
					// Adjacent milliseconds can only be queried individually.
					rightStart = window.end
				}
				// Overlap at the midpoint where possible; dedupe absorbs it.
				// Both children strictly shrink, bounding the subdivision depth.
				windows = append(windows, logWindow{rightStart, window.end}, logWindow{window.start, mid})
			}
		}
	}

	return allItems, lastDetectionTime, windowErr
}
