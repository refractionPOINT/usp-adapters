package usp_proofpoint_tap

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	logsEndpoint   = "https://tap-api-v2.proofpoint.com/v2/siem/all"
	queryInterval  = 60
	warnDedupeSize = 100000
)

type ProofpointTapAdapter struct {
	conf       ProofpointTapConfig
	uspClient  *uspclient.Client
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	messageDedupe map[string]int64
	clickDedupe   map[string]int64
}

type ProofpointTapConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Principal     string                  `json:"principal" yaml:"principal"`
	Secret        string                  `json:"secret" yaml:"secret"`
}

func (c *ProofpointTapConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Principal == "" {
		return errors.New("missing principal")
	}
	if c.Secret == "" {
		return errors.New("missing secret")
	}
	return nil
}

func NewProofpointTapAdapter(conf ProofpointTapConfig) (*ProofpointTapAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &ProofpointTapAdapter{
		conf:          conf,
		doStop:        utils.NewEvent(),
		messageDedupe: make(map[string]int64),
		clickDedupe:   make(map[string]int64),
	}
	var err error

	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		return nil, nil, err
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

func (a *ProofpointTapAdapter) Close() error {
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

func (a *ProofpointTapAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", logsEndpoint))

	since := time.Now()

	for !a.doStop.WaitFor(queryInterval * time.Second) {
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

func (a *ProofpointTapAdapter) makeOneRequest(since time.Time) ([]utils.Dict, time.Time, error) {
	var newItems []utils.Dict

	sinceWithOverlap := since.Add(-2 * time.Minute).UTC()
	sinceWithOverlapString := sinceWithOverlap.Format(time.RFC3339)
	sinceWithOverlapUnix := sinceWithOverlap.Unix()
	nowTimestamp := time.Now().UTC()

	timeDiff := nowTimestamp.Sub(sinceWithOverlap)
	var url string

	if timeDiff > 1*time.Hour {
		url = fmt.Sprintf("%s?format=json&interval=%s/%s", logsEndpoint, sinceWithOverlapString, sinceWithOverlap.Add(1*time.Hour).Add(-1*time.Minute).Format(time.RFC3339))
	} else {
		url = fmt.Sprintf("%s?format=json&interval=%s/%s", logsEndpoint, sinceWithOverlapString, nowTimestamp.Format(time.RFC3339))
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		a.doStop.Set()
		return nil, since, err
	}

	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(a.conf.Principal, a.conf.Secret)

	resp, err := a.httpClient.Do(req)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
		return nil, since, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		a.conf.ClientOptions.OnError(fmt.Errorf("proofpoint tap api non-200: %s\nRESPONSE: %s", resp.Status, string(body)))
		return nil, since, err
	}
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("read body error: %v", err))
		return nil, since, err
	}

	var response struct {
		QueryEndTime      time.Time    `json:"queryEndTime"`
		MessagesDelivered []utils.Dict `json:"messagesDelivered"`
		MessagesBlocked   []utils.Dict `json:"messagesBlocked"`
		ClicksPermitted   []utils.Dict `json:"clicksPermitted"`
		ClicksBlocked     []utils.Dict `json:"clicksBlocked"`
	}

	err = json.Unmarshal(body, &response)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("proofpoint tap api invalid json: %v", err))
		return nil, since, err
	}

	for _, event := range response.MessagesDelivered {
		guid, _ := event["GUID"].(string)

		if _, seen := a.messageDedupe[guid]; seen {
			continue
		}

		a.messageDedupe[guid] = time.Now().Unix()
		event["eventType"] = "message_delivered"
		newItems = append(newItems, event)
	}

	for _, event := range response.MessagesBlocked {
		guid, _ := event["GUID"].(string)

		if _, seen := a.messageDedupe[guid]; seen {
			continue
		}

		a.messageDedupe[guid] = time.Now().Unix()
		event["eventType"] = "message_blocked"
		newItems = append(newItems, event)
	}

	for k, v := range a.messageDedupe {
		if v < sinceWithOverlapUnix {
			delete(a.messageDedupe, k)
		}
	}

	if len(a.messageDedupe) > warnDedupeSize {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf("message dedupe map size: %d", len(a.messageDedupe)))
	}

	for _, event := range response.ClicksPermitted {
		id, _ := event["id"].(string)

		if _, seen := a.clickDedupe[id]; seen {
			continue
		}

		a.clickDedupe[id] = time.Now().Unix()
		event["eventType"] = "click_permitted"
		newItems = append(newItems, event)
	}

	for _, event := range response.ClicksBlocked {
		id, _ := event["id"].(string)

		if _, seen := a.clickDedupe[id]; seen {
			continue
		}

		a.clickDedupe[id] = time.Now().Unix()
		event["eventType"] = "Click Blocked"
		newItems = append(newItems, event)
	}

	for k, v := range a.clickDedupe {
		if v < sinceWithOverlapUnix {
			delete(a.clickDedupe, k)
		}
	}

	if len(a.clickDedupe) > warnDedupeSize {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf("click dedupe map size: %d", len(a.clickDedupe)))
	}

	return newItems, response.QueryEndTime, nil
}
