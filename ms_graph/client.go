package usp_ms_graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const scope = "https://graph.microsoft.com/.default"
const URLPrefix = "https://graph.microsoft.com/v1.0/"

type MsGraphAdapter struct {
	conf       MsGraphConfig
	uspClient  *uspclient.Client
	httpClient *http.Client

	endpoint string

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type MsGraphConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	TenantID      string                  `json:"tenant_id" yaml:"tenant_id"`
	ClientID      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`
	URL           string                  `json:"url" yaml:"url"`
}

func (c *MsGraphConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.TenantID == "" {
		return errors.New("missing tenant_id")
	}
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}
	if c.URL == "" {
		return errors.New("missing url")
	}
	return nil
}

func NewMsGraphAdapter(conf MsGraphConfig) (*MsGraphAdapter, chan struct{}, error) {
	var err error
	a := &MsGraphAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
	}

	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	a.httpClient = &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
		},
	}

	a.chStopped = make(chan struct{})

	// Strip the / prefix if in the URL.
	url := strings.TrimPrefix(a.conf.URL, "/")

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting to fetch alerts from %s", URLPrefix+url))

	a.wgSenders.Add(1)
	go a.fetchEvents(URLPrefix + url)

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *MsGraphAdapter) Close() error {
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

func (a *MsGraphAdapter) fetchToken() (token string) {

	url := fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token", a.conf.TenantID)
	payload := fmt.Sprintf("client_id=%s&scope=%s&grant_type=%s&client_secret=%s", a.conf.ClientID, scope, "client_credentials", a.conf.ClientSecret)

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(payload))
	if err != nil {
		return fmt.Sprintf("No bearer token returned: %s", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Sprintf("No bearer token returned: %s", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Sprintf("No bearer token returned: %s", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Sprintf("No bearer token returned: %s", err)
	}

	if accessToken, ok := result["access_token"].(string); ok {
		return accessToken
	}

	return ""

}

func (a *MsGraphAdapter) fetchEvents(url string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", url))

	lastEventId := ""
	// since := time.Date(2022, time.June, 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02T15:04:05.000000Z")
	since := time.Now().Format("2006-01-02T15:04:05.000000Z")

	for !a.doStop.WaitFor(30 * time.Second) {
		// The makeOneRequest function handles error
		// handling and fatal error handling.
		items, newSince, eventId, _ := a.makeOneListRequest(url, since, lastEventId)
		since = newSince
		lastEventId = eventId
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

func (a *MsGraphAdapter) makeOneListRequest(eventsUrl string, since string, lastEventId string) ([]map[string]interface{}, string, string, error) {
	var alerts []map[string]interface{}
	var lastDetectionTime, eventId string

	// Retry up to 3 times
	for attempt := 1; attempt <= 3; attempt++ {
		// Create query parameters
		filter := "%24"
		query := "%20ge%20"
		date_filter := fmt.Sprintf("?%sfilter=createdDateTime%s%s", filter, query, strings.Replace(since, ":", "%3A", -1))

		// Append query parameters to the URL
		eventsUrl += date_filter

		req, err := http.NewRequest("GET", eventsUrl, nil)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error creating request: %s", err))
			return nil, since, "", err
		}

		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.fetchToken()))
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error making request: %s", err))
			return nil, since, "", err
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error reading response: %s", err))
			return nil, since, "", err
		}

		if resp.StatusCode != http.StatusOK {
			a.conf.ClientOptions.OnError(fmt.Errorf("error response from Microsoft API, be sure to verify permissions and Microsoft API status (attempt %d): %s", attempt, body))
			// Retry if the status code is not OK, but continue to the next iteration
			if attempt < 3 {
				continue
			}
			// Return after 3 failed attempts
			return nil, since, "", fmt.Errorf("error response from Microsoft API, be sure to verify permissions and Microsoft API status (attempt 3): %s", body)
		}

		// If the response is OK, parse the body and process detections
		var data map[string]interface{}
		err = json.Unmarshal(body, &data)
		detections, _ := data["value"].([]interface{})
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error parsing JSON: %v", err))
			return nil, since, "", err
		}

		items := detections

		lastDetectionTime = since
		for _, detection := range items {
			detectMap, ok := detection.(map[string]interface{})
			if !ok {
				a.conf.ClientOptions.DebugLog("error parsing detectMap JSON")
				continue
			}

			id, ok := detectMap["id"].(string)
			if !ok {
				a.conf.ClientOptions.DebugLog("error parsing ID from detectMap JSON")
				continue
			}
			eventId = id

			if id != lastEventId {
				createdDateTime, ok := detectMap["createdDateTime"].(string)
				if !ok {
					a.conf.ClientOptions.DebugLog("error parsing createdDateTime from detectMap JSON")
					continue
				}

				lastDetectionTime = createdDateTime
				alerts = append(alerts, detectMap)
			}
		}

		// Break out of the loop if successful
		break
	}

	return alerts, lastDetectionTime, eventId, nil
}
