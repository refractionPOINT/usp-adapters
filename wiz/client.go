package usp_wiz

import (
	"bytes"
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

type WizAdapter struct {
	conf       WizConfig
	uspClient  *uspclient.Client
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type WizConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ClientID      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`
	URL           string                  `json:"url" yaml:"url"`
	Query         string                  `json:"query" yaml:"query"`
	Variables     map[string]interface{}  `json:"variables" yaml:"variables"`
	TimeField     string                  `json:"time_field" yaml:"time_field"` // e.g., "createdAt", "updatedAt"
	DataPath      []string                `json:"data_path" yaml:"data_path"`   // e.g., ["data", "securityIssues", "issues"]
	IDField       string                  `json:"id_field" yaml:"id_field"`     // e.g., "id"
}

func (c *WizConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
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
	if c.Query == "" {
		return errors.New("missing query")
	}
	if c.TimeField == "" {
		return errors.New("missing time_field")
	}
	if len(c.DataPath) == 0 {
		return errors.New("missing data_path")
	}
	if c.IDField == "" {
		return errors.New("missing id_field")
	}
	return nil
}

func NewWizAdapter(conf WizConfig) (*WizAdapter, chan struct{}, error) {
	var err error
	a := &WizAdapter{
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

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting to fetch alerts from %s", conf.URL))

	a.wgSenders.Add(1)
	go a.fetchEvents()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *WizAdapter) Close() error {
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

func (a *WizAdapter) fetchToken() (string, error) {
	a.conf.ClientOptions.DebugLog("fetching token")

	url := "https://auth.app.wiz.io/oauth/token"
	payload := fmt.Sprintf("grant_type=client_credentials&client_id=%s&client_secret=%s&audience=beyond-api",
		a.conf.ClientID,
		a.conf.ClientSecret)

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(payload))
	if err != nil {
		return "", fmt.Errorf("error creating token request: %v", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("error making token request: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error reading token response: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("error parsing token response: %v", err)
	}

	if accessToken, ok := result["access_token"].(string); ok {
		return accessToken, nil
	}

	return "", fmt.Errorf("no access token in response")
}

func (a *WizAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("fetching of Wiz events exiting")

	lastEventId := ""
	since := time.Now().Add(-24 * time.Hour).Format(time.RFC3339)

	for !a.doStop.WaitFor(30 * time.Second) {
		items, newSince, eventId, err := a.makeOneGraphQLRequest(since, lastEventId)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error making GraphQL request: %v", err))
			continue
		}

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

func (a *WizAdapter) makeOneGraphQLRequest(since string, lastEventId string) ([]map[string]interface{}, string, string, error) {
	var alerts []map[string]interface{}
	var lastDetectionTime, eventId string

	// Create a copy of the variables to avoid modifying the original
	variables := make(map[string]interface{})
	for k, v := range a.conf.Variables {
		variables[k] = v
	}

	// Add time filter to variables
	if timeFilter, ok := variables["filter"].(map[string]interface{}); ok {
		timeFilter[a.conf.TimeField] = map[string]string{
			"after": since,
		}
	} else {
		variables["filter"] = map[string]interface{}{
			a.conf.TimeField: map[string]string{
				"after": since,
			},
		}
	}

	requestBody := map[string]interface{}{
		"query":     a.conf.Query,
		"variables": variables,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, since, "", fmt.Errorf("error marshaling request body: %v", err)
	}

	token, err := a.fetchToken()
	if err != nil {
		return nil, since, "", fmt.Errorf("error fetching token: %v", err)
	}

	req, err := http.NewRequest("POST", a.conf.URL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, since, "", fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, since, "", fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, since, "", fmt.Errorf("error reading response: %v", err)
	}

	fmt.Printf("response: %s\n", string(body))

	if resp.StatusCode != http.StatusOK {
		return nil, since, "", fmt.Errorf("error response from Wiz API (%d): %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, since, "", fmt.Errorf("error parsing response: %v", err)
	}

	// Navigate through the data path to find the items
	current := result
	for i, path := range a.conf.DataPath {
		isLastPath := i == len(a.conf.DataPath)-1

		if isLastPath {
			// Last path element should be an array
			items, ok := current[path].([]interface{})
			if !ok {
				return nil, since, "", fmt.Errorf("expected array at path %s, got %T", path, current[path])
			}

			for _, item := range items {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					continue
				}

				id, ok := itemMap[a.conf.IDField].(string)
				if !ok {
					continue
				}

				if id != lastEventId {
					timeValue, ok := itemMap[a.conf.TimeField].(string)
					if !ok {
						continue
					}

					lastDetectionTime = timeValue
					alerts = append(alerts, itemMap)
					eventId = id
				}
			}
			return alerts, lastDetectionTime, eventId, nil
		}

		// Not the last path element, should be an object
		next, ok := current[path].(map[string]interface{})
		if !ok {
			return nil, since, "", fmt.Errorf("expected object at path %s, got %T", path, current[path])
		}
		current = next
	}

	return alerts, lastDetectionTime, eventId, nil
}
