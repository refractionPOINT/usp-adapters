package usp_sublime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
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

	// maxPagesPerWindow caps how deep a single time window is paginated. The
	// audit log is served newest-first, so a window can only be committed once
	// it has been read to its oldest event. A window holding more events than
	// this is split in half and its older half is fetched first, which keeps
	// every request bounded no matter how far behind the adapter is.
	maxPagesPerWindow = 100

	// minSplitWindow is the smallest window worth splitting further. A window
	// this short that still exceeds maxPagesPerWindow is committed with what
	// was fetched, and the shortfall is reported as a warning.
	minSplitWindow = time.Second
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

	ctx context.Context
	now func() time.Time

	// start is when the adapter started: older events are not replayed.
	// cursor is the exclusive upper bound of the time range already shipped.
	// dedupe holds the ids (and created_at) of shipped events that are still
	// inside the overlap re-fetched on the next poll.
	start  time.Time
	cursor time.Time
	dedupe map[string]time.Time
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
	c.BaseURL = strings.TrimRight(c.BaseURL, "/")
	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	return nil
}

func NewSublimeAdapter(ctx context.Context, conf SublimeConfig) (*SublimeAdapter, chan struct{}, error) {
	return newSublimeAdapter(ctx, conf, nil, nil)
}

// newSublimeAdapter is the implementation behind NewSublimeAdapter. When sink
// is non-nil it is used in place of a real LimaCharlie client, and when now is
// non-nil it replaces the wall clock -- the seams tests use to capture shipped
// events and to control the polling window.
func newSublimeAdapter(ctx context.Context, conf SublimeConfig, sink uspSink, now func() time.Time) (*SublimeAdapter, chan struct{}, error) {
	if now == nil {
		now = time.Now
	}
	a := &SublimeAdapter{
		conf:   conf,
		ctx:    context.Background(),
		now:    now,
		doStop: utils.NewEvent(),
		dedupe: make(map[string]time.Time),
	}
	a.start = now().UTC().Truncate(time.Microsecond)
	a.cursor = a.start

	// The general adapter runner constructs the adapter without calling
	// Validate(), so backfill the defaults here as well. Without the base URL
	// default, an unset base_url produces a schemeless request URL and every
	// poll fails with `unsupported protocol scheme ""`; without the poll
	// interval default the loop would spin on WaitFor(0).
	if a.conf.BaseURL == "" {
		a.conf.BaseURL = defaultBaseURL
	}
	// Tolerate a configured base URL with a trailing slash (as a user might
	// copy it from the dashboard); otherwise it produces a `//v0/...` path.
	a.conf.BaseURL = strings.TrimRight(a.conf.BaseURL, "/")
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

	for !a.doStop.WaitFor(a.conf.PollInterval) {
		// Events returned alongside an error belong to windows that were fully
		// read before the failure; they are committed and must still ship.
		items, _ := a.poll()

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

// poll ships everything created between the cursor and now. The range is read
// as one or more bounded windows; a window only advances the cursor once it
// has been read completely, so a failed request leaves the cursor where it was
// and the next poll retries the same range instead of skipping it.
//
// The returned events are committed (the cursor has moved past them) and are
// returned oldest-first, even when an error is also returned.
func (a *SublimeAdapter) poll() ([]utils.Dict, error) {
	until := a.now().UTC().Truncate(time.Microsecond)
	var committed []utils.Dict

	for a.cursor.Before(until) {
		end := until
		var events []utils.Dict
		for {
			var truncated bool
			var err error
			events, truncated, err = a.fetchWindow(a.cursor.Add(-overlapPeriod), end)
			if err != nil {
				return committed, err
			}
			if !truncated {
				break
			}
			if end.Sub(a.cursor) <= minSplitWindow {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("sublime: more than %d audit events between %s and %s, some may be skipped", maxPagesPerWindow*pageLimit, a.cursor.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano)))
				break
			}
			// Too many events to read in one window: fetch the older half
			// first, the remainder is picked up by the next iteration.
			end = a.cursor.Add(end.Sub(a.cursor) / 2).Truncate(time.Microsecond)
		}

		// The API returns newest-first; ship in chronological order.
		for i := len(events) - 1; i >= 0; i-- {
			event := events[i]
			id, _ := event["id"].(string)
			if _, seen := a.dedupe[id]; seen {
				continue
			}
			createdAtStr, _ := event["created_at"].(string)
			createdAt, err := time.Parse(time.RFC3339Nano, createdAtStr)
			if err != nil {
				continue
			}
			if createdAt.Before(a.start) {
				continue
			}
			a.dedupe[id] = createdAt
			committed = append(committed, event)
		}
		a.cursor = end

		// Only ids still inside the next poll's overlap can be seen again.
		horizon := a.cursor.Add(-overlapPeriod)
		for k, v := range a.dedupe {
			if v.Before(horizon) {
				delete(a.dedupe, k)
			}
		}
	}

	return committed, nil
}

// fetchWindow returns every event with from <= created_at < to, newest-first.
// Pinning the upper bound keeps offset pagination stable: events created while
// the pages are being read fall outside the window and cannot shift offsets.
// truncated is true when the window holds more than maxPagesPerWindow pages.
func (a *SublimeAdapter) fetchWindow(from, to time.Time) ([]utils.Dict, bool, error) {
	var events []utils.Dict
	seen := map[string]struct{}{}

	for page := 0; page < maxPagesPerWindow; page++ {
		if a.doStop.IsSet() {
			return nil, false, errors.New("adapter stopping")
		}
		reqURL := fmt.Sprintf("%s%s?limit=%d&offset=%d&created_at[gte]=%s&created_at[lt]=%s",
			a.conf.BaseURL, logsPath, pageLimit, page*pageLimit,
			url.QueryEscape(from.UTC().Format(time.RFC3339Nano)),
			url.QueryEscape(to.UTC().Format(time.RFC3339Nano)))

		pageEvents, err := a.fetchPage(reqURL)
		if err != nil {
			return nil, false, err
		}
		for _, event := range pageEvents {
			// A late-committed event inside the window can shift later pages
			// by one; drop the resulting repeats.
			id, _ := event["id"].(string)
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			events = append(events, event)
		}
		if len(pageEvents) < pageLimit {
			return events, false, nil
		}
	}
	return events, true, nil
}

func (a *SublimeAdapter) fetchPage(reqURL string) ([]utils.Dict, error) {
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s", reqURL))

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("http.NewRequest(): %v", err))
		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.conf.ApiKey))
	req.Header.Set("Accept", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("read body error: %v", err))
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("sublime api non-200: %s\nRESPONSE: %s", resp.Status, string(body))
		a.conf.ClientOptions.OnError(err)
		return nil, err
	}

	var response struct {
		Events []utils.Dict `json:"events"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("sublime api invalid json: %v", err))
		return nil, err
	}
	return response.Events, nil
}
