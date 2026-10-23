package usp_zendesk

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	logsEndpoint = "/api/v2/audit_logs"

	// overlapPeriod is how far back each poll reaches. It must exceed the
	// API's ingestion lag -- the delay between an event happening and the
	// audit log returning it -- or every poll asks only for a slice of time
	// the backend has not caught up to yet and returns nothing. The resulting
	// window overlap is absorbed by the dedupe map.
	overlapPeriod = 30 * time.Minute

	// defaultPollInterval is how long fetchEvents idles between polling ticks.
	defaultPollInterval = 30 * time.Second
)

// recordID renders an audit log id as a dedupe key. Zendesk types id as an
// integer, which utils.Dict decodes as uint64. A plain string assertion
// collapses these IDs onto the empty dedupe key. Keep compatibility with
// float64 and json.Number from other JSON decoders as well.
func recordID(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case uint64:
		return strconv.FormatUint(t, 10)
	case float64:
		return strconv.FormatInt(int64(t), 10)
	case json.Number:
		return t.String()
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
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

type ZendeskAdapter struct {
	conf       ZendeskConfig
	uspClient  uspSink
	httpClient *http.Client

	baseURL      string
	pollInterval time.Duration
	now          func() time.Time
	wait         func(time.Duration) bool

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context

	dedupe map[string]int64
}

type ZendeskConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ApiToken      string                  `json:"api_token" yaml:"api_token"`
	ZendeskDomain string                  `json:"zendesk_domain" yaml:"zendesk_domain"`
	ZendeskEmail  string                  `json:"zendesk_email" yaml:"zendesk_email"`

	// BaseURL overrides the scheme and host used to reach the Zendesk API.
	// Empty means "https://" + ZendeskDomain.
	BaseURL string `json:"base_url" yaml:"base_url"`

	// PollInterval overrides the wait between polls of the audit logs
	// endpoint (default 30s). It is not settable through a config file; it
	// exists as a seam for tests.
	PollInterval time.Duration `json:"-" yaml:"-"`
}

func (c *ZendeskConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ApiToken == "" {
		return errors.New("missing api token")
	}
	if c.ZendeskDomain == "" {
		return errors.New("missing zendesk domain (e.g., 'your-company.zendesk.com')")
	}
	if c.ZendeskEmail == "" {
		return errors.New("missing zendesk email")
	}

	return nil
}

func NewZendeskAdapter(ctx context.Context, conf ZendeskConfig) (*ZendeskAdapter, chan struct{}, error) {
	return newZendeskAdapter(ctx, conf, nil)
}

// newZendeskAdapter is the implementation behind NewZendeskAdapter. When sink
// is non-nil it is used in place of a real LimaCharlie client -- the seam
// tests use to capture shipped events.
func newZendeskAdapter(ctx context.Context, conf ZendeskConfig, sink uspSink) (*ZendeskAdapter, chan struct{}, error) {
	a := &ZendeskAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
		dedupe: make(map[string]int64),
	}

	a.baseURL = strings.TrimSuffix(conf.BaseURL, "/")
	if a.baseURL == "" {
		a.baseURL = fmt.Sprintf("https://%s", conf.ZendeskDomain)
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

func (a *ZendeskAdapter) Close() error {
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

func (a *ZendeskAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", logsEndpoint))

	var state *utils.LogPollState
	delay := a.pollInterval
	for !a.wait(delay) {
		now := a.now()
		if state == nil {
			state = utils.NewLogPollState(now, overlapPeriod)
		}
		start, end, expired := state.Window(now)
		if expired {
			a.conf.ClientOptions.OnWarning("poll catch-up exceeded 24 hours; older unread records may be lost")
		}
		// Successful pages are already marked seen. Ship them before handling
		// errors: the consumer's OnError callback may terminate this instance.
		items, _, pollErr := a.requestWindow(start, end)

		for _, item := range items {
			msg := &protocol.DataMessage{
				JsonPayload: item,
				TimestampMs: uint64(a.now().UnixNano() / int64(time.Millisecond)),
			}
			if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
				if err == uspclient.ErrorBufferFull {
					a.conf.ClientOptions.OnWarning("stream falling behind")
					err = a.uspClient.Ship(msg, 1*time.Hour)
				}
				if err == nil {
					continue
				}
				if err != uspclient.ErrorClosed {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				}
				a.doStop.Set()
				return
			}
		}
		if pollErr == nil {
			state.Complete(end)
			delay = a.pollInterval
		} else if a.doStop.IsSet() && errors.Is(pollErr, context.Canceled) {
			return
		} else if retryDelay, retryable := state.RetryDelay(pollErr, a.now()); retryable {
			delay = retryDelay
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("poll failed; retrying in %s: %v", delay, pollErr))
		} else {
			a.doStop.Set()
			a.conf.ClientOptions.OnError(pollErr)
			return
		}
	}
}

func (a *ZendeskAdapter) makeOneRequest(since time.Time) ([]utils.Dict, time.Time, error) {
	now := a.now()
	if floor := now.Add(-overlapPeriod); since.Before(floor) {
		since = floor
	}
	return a.requestWindow(since, now)
}

func (a *ZendeskAdapter) requestWindow(since, currentTime time.Time) ([]utils.Dict, time.Time, error) {
	var allItems []utils.Dict
	// Retain seen IDs for the entire unread range, including catch-up after
	// an outage. Culling at now-overlap would replay already shipped pages.
	for id, timestamp := range a.dedupe {
		if timestamp < since.Unix() {
			delete(a.dedupe, id)
		}
	}
	start, until := since.UTC().Format(time.RFC3339), currentTime.UTC().Format(time.RFC3339)
	var lastDetectionTime time.Time

	// The first page carries the created_at window; every page after it is
	// fetched from links.next, which Zendesk returns as a complete URL
	// carrying the page[after] cursor.
	url := fmt.Sprintf("%s%s?filter[created_at][]=%s&filter[created_at][]=%s&page[size]=100", a.baseURL, logsEndpoint, start, until)

	for {
		if a.doStop.IsSet() {
			return allItems, lastDetectionTime, context.Canceled
		}
		// Prepare the request.
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s", url))
		req, err := http.NewRequest("GET", url, nil)
		if err == nil && ((req.URL.Scheme != "http" && req.URL.Scheme != "https") || req.URL.Host == "") {
			err = errors.New("logs URL must be an absolute HTTP or HTTPS URL")
		}
		if err != nil {
			return allItems, lastDetectionTime, err
		}

		// Format the authentication string as "email/token:api_token"
		authString := fmt.Sprintf("%s/token:%s", a.conf.ZendeskEmail, a.conf.ApiToken)

		// Encode to Base64
		authEncoded := base64.StdEncoding.EncodeToString([]byte(authString))

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", fmt.Sprintf("Basic %s", authEncoded))

		// Issue the request.
		resp, err := a.httpClient.Do(req)
		if err != nil {
			return allItems, lastDetectionTime, utils.NewLogPollRetry(fmt.Errorf("http.Client.Do(): %w", err), "")
		}
		body, readErr := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil && resp.StatusCode == http.StatusOK {
			return allItems, lastDetectionTime, utils.NewLogPollRetry(readErr, resp.Header.Get("Retry-After"))
		}

		// Evaluate if success.
		if resp.StatusCode != http.StatusOK {
			// err is nil here; build a real one so callers can tell a failure
			// apart from a successful empty poll.
			err = fmt.Errorf("zendesk api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, url, string(body))
			if utils.LogPollRetryableStatus(resp.StatusCode) {
				return allItems, lastDetectionTime, utils.NewLogPollRetry(err, resp.Header.Get("Retry-After"))
			}
			return allItems, lastDetectionTime, err
		}

		// Parse the response.
		var response struct {
			AuditLogs []utils.Dict `json:"audit_logs"`
			Meta      struct {
				HasMore      bool   `json:"has_more"`
				AfterCursor  string `json:"after_cursor"`
				BeforeCursor string `json:"before_cursor"`
			}
			Links struct {
				Prev string `json:"prev"`
				Next string `json:"next"`
			}
		}
		err = json.Unmarshal(body, &response)
		if err != nil {
			return allItems, lastDetectionTime, utils.NewLogPollRetry(fmt.Errorf("zendesk api invalid json: %w", err), "")
		}
		//a.conf.ClientOptions.DebugLog(fmt.Sprintf("results: %s", response))

		// Collect items.
		items := response.AuditLogs
		var newItems []utils.Dict
		lastDetectionTime = since
		for _, item := range items {
			timestamp, _ := item["created_at"].(string)
			eventid := recordID(item["id"])
			if _, ok := a.dedupe[eventid]; ok {
				continue
			}
			epoch, perr := time.Parse(time.RFC3339, timestamp)
			if perr != nil {
				// Without a usable created_at the dedupe entry would be culled
				// immediately and the record would re-ship every poll. Stamp
				// it with now so it survives the overlap window.
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("unparseable created_at %q: %v", timestamp, perr))
				epoch = a.now()
			}
			a.dedupe[eventid] = epoch.Unix()
			newItems = append(newItems, item)
			lastDetectionTime = epoch
		}
		allItems = append(allItems, newItems...)

		if !response.Meta.HasMore {
			break
		}
		if response.Links.Next == "" {
			// An incomplete page chain cannot advance the completed window.
			return allItems, lastDetectionTime, utils.NewLogPollRetry(errors.New("zendesk pagination has_more without links.next"), "")
		}
		url = response.Links.Next
	}

	return allItems, lastDetectionTime, nil
}
