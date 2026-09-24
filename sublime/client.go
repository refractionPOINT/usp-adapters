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

	// maxPagesPerWindow caps how many pages of not-yet-shipped events a single
	// time window may hold. The audit log is served newest-first, so a window
	// can only be committed once it has been read to its oldest event. A
	// window holding more than this is split in half and its older half is
	// fetched first, which bounds both the requests per window and the events
	// held in memory before they ship.
	maxPagesPerWindow = 100

	// maxOverlapPages caps how much of the overlap (events already shipped by
	// the previous poll, re-read to catch late-committed ones) is re-read.
	// Overlap pages do not count against maxPagesPerWindow, so a burst that
	// was already shipped cannot force windows to be split.
	maxOverlapPages = 10

	// clockSkewWarning is the host/Sublime clock difference above which a
	// warning is emitted. Skew is compensated either way.
	clockSkewWarning = 10 * time.Second

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
	// Both are on Sublime's clock. dedupe holds the keys (and created_at) of
	// shipped events that are still inside the overlap re-fetched on the next
	// poll.
	start  time.Time
	cursor time.Time
	dedupe map[string]time.Time

	// skew is Sublime's clock minus the host clock, measured from the Date
	// header of API responses at the start of every poll. Windows end at the
	// host's now plus skew, so a host clock running ahead -- or corrected
	// between polls -- cannot move the cursor past events Sublime has not
	// created yet.
	skew       time.Duration
	started    bool
	skewWarned bool
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
		// Errors are reported where they happen; a failed window is retried
		// on the next poll.
		_ = a.poll(a.ship)
	}
}

// ship sends a batch of events to LimaCharlie. A failure stops the adapter.
func (a *SublimeAdapter) ship(items []utils.Dict) error {
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
			return err
		}
	}
	return nil
}

// poll ships everything created between the cursor and now. The range is read
// as one or more bounded windows, oldest first. A window only advances the
// cursor once it has been read completely, and is handed to ship right away,
// so a failed request leaves the cursor where it was and the next poll retries
// the same range instead of skipping it.
func (a *SublimeAdapter) poll(ship func([]utils.Dict) error) error {
	// Measure Sublime's clock right before choosing where this poll's
	// windows end. A skew carried over from an earlier poll would be wrong
	// if the host clock was stepped in between. The request is a single
	// event at most.
	reqURL := fmt.Sprintf("%s%s?limit=1&created_at[gte]=%s",
		a.conf.BaseURL, logsPath, url.QueryEscape(a.cursor.Format(time.RFC3339Nano)))
	a.skew = 0
	if _, err := a.fetchPage(reqURL); err != nil {
		return err
	}
	if !a.started {
		// start was taken from the host clock, but it is compared with
		// Sublime's timestamps.
		a.started = true
		a.start = a.start.Add(a.skew).Truncate(time.Microsecond)
		a.cursor = a.start
	}

	until := a.now().Add(a.skew).UTC().Truncate(time.Microsecond)

	for a.cursor.Before(until) {
		end := until
		var events []utils.Dict
		for {
			var truncated bool
			var err error
			events, truncated, err = a.fetchWindow(a.cursor, end)
			if err != nil {
				return err
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
		var batch []utils.Dict
		for i := len(events) - 1; i >= 0; i-- {
			event := events[i]
			key := eventKey(event)
			if _, seen := a.dedupe[key]; seen {
				continue
			}
			createdAtStr, _ := event["created_at"].(string)
			createdAt, err := time.Parse(time.RFC3339Nano, createdAtStr)
			if err != nil {
				// The server matched it against the window, so it belongs
				// here even if its timestamp is not one we can parse.
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("sublime: audit event with unparseable created_at %q shipped as-is", createdAtStr))
				createdAt = end
			} else if createdAt.Before(a.start) {
				continue
			}
			a.dedupe[key] = createdAt
			batch = append(batch, event)
		}
		a.cursor = end

		// Only events still inside the next poll's overlap can be seen again.
		horizon := a.cursor.Add(-overlapPeriod)
		for k, v := range a.dedupe {
			if v.Before(horizon) {
				delete(a.dedupe, k)
			}
		}

		if len(batch) != 0 {
			if err := ship(batch); err != nil {
				return err
			}
		}
	}

	return nil
}

// eventKey identifies an event for deduplication: its id, or its whole content
// for the (unexpected) event without one.
func eventKey(event utils.Dict) string {
	if id, _ := event["id"].(string); id != "" {
		return id
	}
	b, _ := json.Marshal(event)
	return "raw:" + string(b)
}

// fetchWindow returns the events with cursor - overlapPeriod <= created_at <
// to, newest-first. Pinning the upper bound keeps offset pagination stable:
// events created while the pages are being read fall outside the window and
// cannot shift offsets. truncated is true when the events at or after cursor
// span more than maxPagesPerWindow pages. The overlap below cursor was shipped
// by the previous poll and is only re-read up to maxOverlapPages, to catch
// events that became visible late.
func (a *SublimeAdapter) fetchWindow(cursor, to time.Time) ([]utils.Dict, bool, error) {
	var events []utils.Dict
	seen := map[string]struct{}{}
	from := cursor.Add(-overlapPeriod)
	newPages, overlapPages := 0, 0

	for offset := 0; ; offset += pageLimit {
		if newPages >= maxPagesPerWindow {
			return events, true, nil
		}
		if overlapPages >= maxOverlapPages {
			return events, false, nil
		}
		if a.doStop.IsSet() {
			return nil, false, errors.New("adapter stopping")
		}
		reqURL := fmt.Sprintf("%s%s?limit=%d&offset=%d&created_at[gte]=%s&created_at[lt]=%s",
			a.conf.BaseURL, logsPath, pageLimit, offset,
			url.QueryEscape(from.UTC().Format(time.RFC3339Nano)),
			url.QueryEscape(to.UTC().Format(time.RFC3339Nano)))

		pageEvents, err := a.fetchPage(reqURL)
		if err != nil {
			return nil, false, err
		}
		for _, event := range pageEvents {
			// A late-committed event inside the window can shift later pages
			// by one; drop the resulting repeats.
			key := eventKey(event)
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			events = append(events, event)
		}
		if len(pageEvents) < pageLimit {
			return events, false, nil
		}

		// Pages are newest-first: once a page reaches below the cursor, the
		// rest of the window is overlap.
		oldestStr, _ := pageEvents[len(pageEvents)-1]["created_at"].(string)
		if oldest, err := time.Parse(time.RFC3339Nano, oldestStr); err == nil && oldest.Before(cursor) {
			overlapPages++
		} else {
			newPages++
		}
	}
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
	a.measureSkew(resp.Header.Get("Date"))

	var response struct {
		Events []utils.Dict `json:"events"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("sublime api invalid json: %v", err))
		return nil, err
	}
	return response.Events, nil
}

// measureSkew updates the Sublime-minus-host clock difference from a response
// Date header. The header has one-second resolution and is stamped before the
// response travels back, so the estimate errs towards Sublime being behind,
// which only makes windows end slightly earlier.
func (a *SublimeAdapter) measureSkew(date string) {
	if date == "" {
		return
	}
	serverNow, err := http.ParseTime(date)
	if err != nil {
		return
	}
	a.skew = serverNow.Sub(a.now())
	if !a.skewWarned && (a.skew > clockSkewWarning || a.skew < -clockSkewWarning) {
		a.skewWarned = true
		a.conf.ClientOptions.OnWarning(fmt.Sprintf("sublime: host clock differs from the Sublime API clock by %s; compensating", -a.skew))
	}
}
