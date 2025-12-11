package usp_darktrace

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	queryInterval     = 60
	aiAnalystAlerts   = "/aianalyst/incidentevents?includeacknowledged=true&includeincidenteventurl=true"
	modelBreachAlerts = "/modelbreaches?expandenums=true&historicmodelonly=true&includeacknowledged=true&includebreachurl=true"
)

type DarktraceConfig struct {
	ClientOptions 			uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Url           			string                  `json:"url" yaml:"url"`
	PublicToken   			string                  `json:"public_token" yaml:"public_token"`
	PrivateToken  			string                  `json:"private_token" yaml:"private_token"`
	InitialLookback       	time.Duration           `json:"initial_lookback,omitempty" yaml:"initial_lookback,omitempty"` // eg, 24h, 30m, 168h, 1h30m
}

type DarktraceAdapter struct {
	conf       DarktraceConfig
	uspClient  *uspclient.Client
	httpClient *http.Client
	chStopped  chan struct{}

	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc

	aiAnalystDedupe   map[string]int64
	modelBreachDedupe map[string]int64
}

type DarktraceResponse interface {
	GetDict() []utils.Dict
}

type DarktraceEventsResponse []utils.Dict

func (r DarktraceEventsResponse) GetDict() []utils.Dict {
	return []utils.Dict(r)
}

func NewDarktraceAdapter(ctx context.Context, conf DarktraceConfig) (*DarktraceAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}
	a := &DarktraceAdapter{
		conf:              conf,
		aiAnalystDedupe:   make(map[string]int64),
		modelBreachDedupe: make(map[string]int64),
	}

	rootCtx, cancel := context.WithCancel(ctx)
	a.ctx = rootCtx
	a.cancel = cancel

	var err error
	a.uspClient, err = uspclient.NewClient(rootCtx, conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	a.httpClient = &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	a.chStopped = make(chan struct{})

	go a.fetchEvents()

	return a, a.chStopped, nil
}

func (c *DarktraceConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Url == "" {
		return errors.New("missing url")
	}
	if c.PublicToken == "" {
		return errors.New("missing public token")
	}
	if c.PrivateToken == "" {
		return errors.New("missing private token")
	}
	return nil
}

func (a *DarktraceAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	var err1, err2 error
	a.once.Do(func() {
		a.cancel()
		err1 = a.uspClient.Drain(1 * time.Minute)
		_, err2 = a.uspClient.Close()
		a.httpClient.CloseIdleConnections()
		close(a.chStopped)
	})
	if err1 != nil {
		return err1
	}
	return err2
}

type API struct {
	Endpoint     string
	Key          string
	ResponseType DarktraceResponse
	Dedupe       map[string]int64
	timeField    string
	timeFormat   string
}

func (a *DarktraceAdapter) fetchEvents() {

	since := map[string]time.Time{
		"aiAnalyst":     time.Now().Add(-1*a.conf.InitialLookback).UTC(),
		"modelBreaches": time.Now().Add(-1*a.conf.InitialLookback).UTC(),
	}

	APIs := []API{
		{
			Endpoint:     aiAnalystAlerts,
			Key:          "aiAnalyst",
			ResponseType: &DarktraceEventsResponse{},
			timeFormat:   "20060102T150405",
			timeField:    "createdAt",
			Dedupe:       a.aiAnalystDedupe,
		},
		{
			Endpoint:     modelBreachAlerts,
			Key:          "modelBreaches",
			ResponseType: &DarktraceEventsResponse{},
			timeFormat:   "20060102T150405",
			timeField:    "creationTime",
			Dedupe:       a.modelBreachDedupe,
		},
	}

	ticker := time.NewTicker(queryInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", a.conf.Url))
			return
		case <-ticker.C:
			// Capture current time once for all APIs in this cycle
			cycleTime := time.Now()

			allItems := []utils.Dict{}

			for _, api := range APIs {
				pageURL := fmt.Sprintf("%s%s", a.conf.Url, api.Endpoint)
				items, err := a.getEvents(pageURL, since[api.Key], cycleTime, api)
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("%s fetch failed: %w", api.Key, err))
					continue
				}
				
				if len(items) > 0 {
					since[api.Key] = cycleTime.Add(-queryInterval * time.Second)
					allItems = append(allItems, items...)
				}
			}

			if len(allItems) > 0 {
				a.submitEvents(allItems)
			}
		}
	}
}

func (a *DarktraceAdapter) getEvents(pageUrl string, since time.Time, cycleTime time.Time, api API) ([]utils.Dict, error) {
	var allItems []utils.Dict
	
	// Cull old dedupe entries - keep entries from the last lookback period
	// to handle duplicates during the overlap window
	cutoffTime := cycleTime.Add(-2 * queryInterval * time.Second).Unix()
	for k, v := range api.Dedupe {
		if v < cutoffTime {
			delete(api.Dedupe, k)
		}
	}

	// Build URL with time range including overlap
	sinceMs := since.UTC().UnixMilli()
	endMs := cycleTime.UTC().UnixMilli()

	urlWithTimes := fmt.Sprintf("%s&starttime=%d&endtime=%d", pageUrl, sinceMs, endMs)

	response, err := a.doRequest(urlWithTimes, api)
	if err != nil {
		return nil, err
	}

	for _, event := range response.GetDict() {
		// Always generate hash-based ID since Darktrace logs don't have ID fields
		dedupeID := a.generateLogHash(event)

		// Get timestamp - handle both string and numeric formats
		var timeString time.Time

		timeValue, exists := event[api.timeField]
		if !exists {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("%s: event missing time field '%s'", api.Key, api.timeField))
			continue
		}
		switch v := timeValue.(type) {
		case string:
			// Parse string timestamp
			timeString, err = time.Parse(api.timeFormat, v)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("darktrace %s api invalid string timestamp: %v\n%v", api.Key, err, event))
				continue
			}
		case float64:
			// Handle numeric timestamp (milliseconds)
			timeString = time.UnixMilli(int64(v))
		case int64:
			// Handle int64 timestamp (milliseconds)
			timeString = time.UnixMilli(v)
		case uint64:
			// Handle uint64 timestamp (milliseconds)
			timeString = time.UnixMilli(int64(v))
		case int:
			// Handle int timestamp (milliseconds)
			timeString = time.UnixMilli(int64(v))
		default:
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("%s: event time field '%s' has unsupported type %T with value: %v", api.Key, api.timeField, timeValue, timeValue))
			continue
		}

		// Check for duplicates using hash-based ID
		if _, seen := api.Dedupe[dedupeID]; !seen {
			if timeString.After(since) || timeString.Equal(since) {
				// Store the event timestamp for dedupe cleanup
				api.Dedupe[dedupeID] = timeString.Unix()
				allItems = append(allItems, event)
			}
		}
	}

	return allItems, nil
}

func (a *DarktraceAdapter) generateLogHash(logMap map[string]interface{}) string {
	// Extract and sort keys
	keys := make([]string, 0, len(logMap))
	for k := range logMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build deterministic string representation
	var buf bytes.Buffer
	for _, k := range keys {
		fmt.Fprintf(&buf, "%s:%v|", k, logMap[k])
	}

	hash := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(hash[:])
}

func (a *DarktraceAdapter) generateSignature(timeString string, fullURL string) (string, error) {
	u, err := url.Parse(fullURL)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha1.New, []byte(a.conf.PrivateToken))
	payload := fmt.Sprintf("%s\n%s\n%s", u.RequestURI(), a.conf.PublicToken, timeString)
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func (a *DarktraceAdapter) doRequest(url string, api API) (DarktraceResponse, error) {
	for {
		select {
		case <-a.ctx.Done():
			return nil, a.ctx.Err()
		default:
		}
		var respBody []byte
		var status int

		err := func() error {
			loopCtx, cancel := context.WithTimeout(a.ctx, 30*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(loopCtx, "GET", url, nil)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("darktrace %s api request error: %v", api.Key, err))
				return err
			}

			nowTime := time.Now().UTC().Format(api.timeFormat)

			signature, err := a.generateSignature(nowTime, url)
			if err != nil {
				return err
			}

			req.Header.Set("DTAPI-Token", a.conf.PublicToken)
			req.Header.Set("DTAPI-Date", nowTime)
			req.Header.Set("DTAPI-Signature", signature)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			resp, err := a.httpClient.Do(req)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("darktrace %s api do error: %v", api.Key, err))
				return err
			}

			defer resp.Body.Close()

			respBody, err = io.ReadAll(resp.Body)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("darktrace %s api read error: %v", api.Key, err))
				return err
			}
			status = resp.StatusCode
			return nil
		}()
		if err != nil {
			return nil, err
		}

		if status == http.StatusTooManyRequests {
			a.conf.ClientOptions.OnWarning("getEventsRequest got 429, sleeping 60s before retry")
			if err := a.sleepContext(60 * time.Second); err != nil {
				return nil, err
			}
			continue
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("darktrace %s api non-200: %d\nRESPONSE %s", api.Key, status, string(respBody))
		}

		err = json.Unmarshal(respBody, &api.ResponseType)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("darktrace %s api invalid json: %v\nResponse body: %s", api.Key, err, string(respBody)))
			return nil, err
		}

		return api.ResponseType, nil
	}
}

func (a *DarktraceAdapter) submitEvents(items []utils.Dict) {
	for _, item := range items {
		msg := &protocol.DataMessage{
			JsonPayload: item,
			TimestampMs: uint64(time.Now().UTC().UnixNano() / int64(time.Millisecond)),
		}
		if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
			if err == uspclient.ErrorBufferFull {
				a.conf.ClientOptions.OnWarning("stream falling behind")
				if err := a.uspClient.Ship(msg, 1*time.Hour); err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
					a.Close()
					return
				}
			} else {
				a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			}
		}
	}
}

func (a *DarktraceAdapter) sleepContext(d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-a.ctx.Done():
		return a.ctx.Err()
	}
}
