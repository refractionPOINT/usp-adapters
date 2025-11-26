package usp_defender

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

var scope = "https://graph.microsoft.com/.default"
var URL = map[string]string{
	"get_alerts": "https://graph.microsoft.com/v1.0/security/alerts_v2",
}

type DefenderAdapter struct {
	conf       DefenderConfig
	uspClient  utils.Shipper
	httpClient *http.Client

	endpoint string

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type DefenderConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	TenantID      string                  `json:"tenant_id" yaml:"tenant_id"`
	ClientID      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`
	Filters []utils.FilterPattern `json:"filters,omitempty" yaml:"filters,omitempty"`
}

func (c *DefenderConfig) Validate() error {
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
	return nil
}

func NewDefenderAdapter(ctx context.Context, conf DefenderConfig) (*DefenderAdapter, chan struct{}, error) {
	var err error
	a := &DefenderAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
	}

	client, err := uspclient.NewClient(ctx, conf.ClientOptions)
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
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
		},
	}

	a.chStopped = make(chan struct{})

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting to fetch alerts"))

	a.wgSenders.Add(1)
	go a.fetchEvents(URL["get_alerts"])

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *DefenderAdapter) Close() error {
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

func (a *DefenderAdapter) fetchToken() (string, error) {

	url := fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token", a.conf.TenantID)
	payload := fmt.Sprintf("client_id=%s&scope=%s&grant_type=%s&client_secret=%s", a.conf.ClientID, scope, "client_credentials", a.conf.ClientSecret)

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(payload))
	if err != nil {
		return "", fmt.Errorf("no bearer token returned: %s", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("no bearer token returned: %s", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("no bearer token returned: %s", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("no bearer token returned: %s", err)
	}

	accessToken, ok := result["access_token"].(string)
	if !ok {
		return "", fmt.Errorf("no bearer token returned: %#v", result)
	}

	return accessToken, nil

}

func (a *DefenderAdapter) fetchEvents(url string) {
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

func (a *DefenderAdapter) makeOneListRequest(eventsUrl string, since string, lastEventId string) ([]map[string]interface{}, string, string, error) {
	var alerts []map[string]interface{}
	var lastDetectionTime, eventId string

	// Retry up to 3 times
	for attempt := 1; attempt <= 3; attempt++ {
		// Create query parameters
		filter := "%24"
		query := "%20ge%20"
		date_filter := fmt.Sprintf("?%sfilter=createdDateTime%s%s", filter, query, strings.Replace(since, ":", "%3A", -1))

		// Create the full request URL with query parameters (don't modify eventsUrl to avoid corruption on retries)
		requestUrl := eventsUrl + date_filter

		authToken, err := a.fetchToken()
		if err != nil {
			// Retry if token fetch failed, but continue to the next iteration
			if attempt < 3 {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("error fetching token (attempt %d), retrying: %s", attempt, err))
				time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
				continue
			}
			// Return after 3 failed attempts
			a.conf.ClientOptions.OnError(fmt.Errorf("error fetching token after 3 attempts: %s", err))
			return nil, since, "", fmt.Errorf("error fetching token after 3 attempts: %s", err)
		}

		req, err := http.NewRequest("GET", requestUrl, nil)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Error creating request: %s\n", err))
			return nil, since, "", err
		}

		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", authToken))
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Error making request: %s\n", err))
			return nil, since, "", err
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Error reading response: %s\n", err))
			return nil, since, "", err
		}

		if resp.StatusCode != http.StatusOK {
			// Check for retryable status codes (503, 504) - likely Microsoft infrastructure issues
			isRetryable := resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout

			if isRetryable {
				// Retry for transient infrastructure errors
				if attempt < 3 {
					a.conf.ClientOptions.OnWarning(fmt.Sprintf("Microsoft API returned %d (attempt %d), retrying: %s\n", resp.StatusCode, attempt, body))
					time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
					continue
				}
				// Final attempt for retryable error
				a.conf.ClientOptions.OnError(fmt.Errorf("Microsoft API returned %d after 3 attempts: %s\n", resp.StatusCode, body))
				return nil, since, "", fmt.Errorf("Microsoft API returned %d after 3 attempts: %s\n", resp.StatusCode, body)
			} else {
				// Non-retryable error (auth, permission, etc) - fail immediately
				a.conf.ClientOptions.OnError(fmt.Errorf("Error response from Microsoft API, be sure to verify permissions and Microsoft API status: %s\n", body))
				return nil, since, "", fmt.Errorf("Error response from Microsoft API, be sure to verify permissions and Microsoft API status: %s\n", body)
			}
		}

		// If the response is OK, parse the body and process detections
		var data map[string]interface{}
		err = json.Unmarshal(body, &data)
		detections, _ := data["value"].([]interface{})
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Error parsing JSON: %v", err))
			return nil, since, "", err
		}

		items := detections

		lastDetectionTime = since
		for _, detection := range items {
			detectMap, ok := detection.(map[string]interface{})
			if !ok {
				a.conf.ClientOptions.DebugLog("Error parsing detectMap JSON")
				continue
			}

			id, ok := detectMap["id"].(string)
			if !ok {
				a.conf.ClientOptions.DebugLog("Error parsing ID from detectMap JSON")
				continue
			}
			eventId = id

			if id != lastEventId {
				createdDateTime, ok := detectMap["createdDateTime"].(string)
				if !ok {
					a.conf.ClientOptions.DebugLog("Error parsing createdDateTime from detectMap JSON")
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
