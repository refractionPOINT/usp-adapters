package usp_box

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	boxBaseURL    = "https://api.box.com/2.0/events"
	tokenEndpoint = "https://api.box.com/oauth2/token"
)

type BoxConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ClientID      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`
	SubjectID     string                  `json:"subject_id" yaml:"subject_id"`
	Filters []utils.FilterPattern `json:"filters,omitempty" yaml:"filters,omitempty"`
}

func (c *BoxConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}

	if c.ClientID == "" || c.ClientSecret == "" || c.SubjectID == "" {
		return errors.New("missing Box client ID, secret, or subject ID")
	}
	return nil
}

type BoxAdapter struct {
	conf           BoxConfig
	uspClient      utils.Shipper
	httpClient     *http.Client
	chStopped      chan struct{}
	wgSenders      sync.WaitGroup
	doStop         *utils.Event
	ctx            context.Context
	dedupe         map[string]int64
	streamPosition string
	initialized    bool
}

func NewBoxAdapter(conf BoxConfig) (*BoxAdapter, chan struct{}, error) {
	var err error
	a := &BoxAdapter{
		conf:           conf,
		ctx:            context.Background(),
		doStop:         utils.NewEvent(),
		dedupe:         make(map[string]int64),
		streamPosition: "",
		initialized:    false,
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
	go a.fetchEvents()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *BoxAdapter) Close() error {
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

func (a *BoxAdapter) getAccessToken() (string, error) {
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", a.conf.ClientID)
	data.Set("client_secret", a.conf.ClientSecret)
	data.Set("box_subject_type", "enterprise")
	data.Set("box_subject_id", fmt.Sprintf("%v", a.conf.SubjectID))

	req, err := http.NewRequest("POST", tokenEndpoint, bytes.NewBufferString(data.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token request failed: %s", string(bodyBytes))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return "", err
	}

	accessToken, ok := result["access_token"].(string)
	if !ok {
		return "", errors.New("access_token missing in response")
	}
	return accessToken, nil
}

func (a *BoxAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("Box event collection stopping")

	if !a.initialized {
		items, streamPos, err := a.makeOneRequest("now")
		a.initialized = true
		if err == nil {
			a.streamPosition = streamPos
		}
		_ = items // discard
	}

	for !a.doStop.WaitFor(30 * time.Second) {
		items, newStreamPosition, _ := a.makeOneRequest(a.streamPosition)
		if newStreamPosition != "" {
			a.streamPosition = newStreamPosition
		}
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

func (a *BoxAdapter) makeOneRequest(streamPosition string) ([]utils.Dict, string, error) {
	var allItems []utils.Dict
	token, err := a.getAccessToken()
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("token: %v", err))
		return nil, streamPosition, err
	}

	reqUrl := boxBaseURL + "?stream_type=admin_logs"
	if streamPosition == "now" || streamPosition == "0" {
		createdAfter := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339)
		reqUrl += "&created_after=" + createdAfter
	} else {
		reqUrl += "&stream_position=" + url.QueryEscape(streamPosition)
	}
	// print the request url
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting: %s", reqUrl))

	req, err := http.NewRequest("GET", reqUrl, nil)
	if err != nil {
		return nil, streamPosition, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, streamPosition, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		a.conf.ClientOptions.OnError(fmt.Errorf("box api non-200: %s", string(body)))
		return nil, streamPosition, fmt.Errorf("non-200 from Box")
	}

	var parsed struct {
		Entries          []utils.Dict `json:"entries"`
		NextStreamPos    json.Number  `json:"next_stream_position"`
		CurrentStreamPos string       `json:"current_stream_position"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("unmarshal: %v", err))
		return nil, streamPosition, err
	}

	nextStreamPosStr := parsed.NextStreamPos.String()
	if nextStreamPosStr == "0" {
		nextStreamPosStr = "now"
	}

	for _, entry := range parsed.Entries {
		id, _ := entry["event_id"].(string)
		createdAt, _ := entry["created_at"].(string)
		if _, seen := a.dedupe[id]; seen {
			continue
		}
		ts, err := time.Parse(time.RFC3339, createdAt)
		if err == nil {
			a.dedupe[id] = ts.Unix()
		}
		allItems = append(allItems, entry)
	}

	for k, v := range a.dedupe {
		if v < time.Now().Add(-1*time.Hour).Unix() {
			delete(a.dedupe, k)
		}
	}

	return allItems, nextStreamPosStr, nil
}
