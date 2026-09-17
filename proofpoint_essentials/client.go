package usp_proofpoint_essentials

import (
	"context"
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
	usRegionHost   = "https://us-siem.proofpointessentials.com"
	euRegionHost   = "https://eu-siem.proofpointessentials.com"
	siemPath       = "/v2/siem/all"
	queryIntervalS = 60
	warnDedupeSize = 100000
)

type ProofpointEssentialsConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Principal     string                  `json:"principal" yaml:"principal"`
	Secret        string                  `json:"secret" yaml:"secret"`
	// Region selects the SIEM host. Valid values: "us" (default) or "eu".
	Region string `json:"region" yaml:"region"`
}

func (c *ProofpointEssentialsConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Principal == "" {
		return errors.New("missing principal")
	}
	if c.Secret == "" {
		return errors.New("missing secret")
	}
	if c.Region != "" && c.Region != "us" && c.Region != "eu" {
		return fmt.Errorf("invalid region %q: must be \"us\" or \"eu\"", c.Region)
	}
	return nil
}

func (c *ProofpointEssentialsConfig) siemHost() string {
	if c.Region == "eu" {
		return euRegionHost
	}
	return usRegionHost
}

type ProofpointEssentialsAdapter struct {
	conf       ProofpointEssentialsConfig
	uspClient  *uspclient.Client
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	messageDedupe map[string]int64
	clickDedupe   map[string]int64
}

func NewProofpointEssentialsAdapter(ctx context.Context, conf ProofpointEssentialsConfig) (*ProofpointEssentialsAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &ProofpointEssentialsAdapter{
		conf:          conf,
		doStop:        utils.NewEvent(),
		messageDedupe: make(map[string]int64),
		clickDedupe:   make(map[string]int64),
	}

	var err error
	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
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

func (a *ProofpointEssentialsAdapter) Close() error {
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

func (a *ProofpointEssentialsAdapter) fetchEvents() {
	defer a.wgSenders.Done()

	logsEndpoint := a.conf.siemHost() + siemPath
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", logsEndpoint))

	since := time.Now()

	for !a.doStop.WaitFor(queryIntervalS * time.Second) {
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

func (a *ProofpointEssentialsAdapter) makeOneRequest(since time.Time) ([]utils.Dict, time.Time, error) {
	var newItems []utils.Dict

	sinceWithOverlap := since.Add(-2 * time.Minute).UTC()
	sinceWithOverlapString := sinceWithOverlap.Format(time.RFC3339)
	sinceWithOverlapUnix := sinceWithOverlap.Unix()
	nowTimestamp := time.Now().UTC()

	timeDiff := nowTimestamp.Sub(sinceWithOverlap)

	logsEndpoint := a.conf.siemHost() + siemPath

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
		a.conf.ClientOptions.OnError(fmt.Errorf("proofpoint essentials api non-200: %s\nRESPONSE: %s", resp.Status, string(body)))
		return nil, since, err
	}
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("read body error: %v", err))
		return nil, since, err
	}

	var response struct {
		QueryEndTime      string       `json:"queryEndTime"`
		MessagesDelivered []utils.Dict `json:"messagesDelivered"`
		MessagesBlocked   []utils.Dict `json:"messagesBlocked"`
		ClicksPermitted   []utils.Dict `json:"clicksPermitted"`
		ClicksBlocked     []utils.Dict `json:"clicksBlocked"`
	}

	if err = json.Unmarshal(body, &response); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("proofpoint essentials api invalid json: %v", err))
		return nil, since, err
	}

	queryEndTime := parseFlexibleTime(response.QueryEndTime, nowTimestamp)

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
		event["eventType"] = "click_blocked"
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

	return newItems, queryEndTime, nil
}

// parseFlexibleTime tries RFC3339 first, then Go's time.String() format, falling back to fallback.
func parseFlexibleTime(s string, fallback time.Time) time.Time {
	if s == "" {
		return fallback
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	if t, err := time.Parse("2006-01-02 15:04:05 -0700 MST", s); err == nil {
		return t
	}
	return fallback
}
