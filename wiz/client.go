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

const (
	graphqlEndpoint = "https://api.wiz.io/graphql"
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
	url := "https://auth.wiz.io/oauth/token"
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

	// GraphQL query for security issues
	query := a.conf.Query
	query = `
	query GetSecurityIssues($filter: SecurityIssueFilters) {
		securityIssues(filter: $filter) {
			issues {
				id
				createdAt
				severity
				status
				title
				description
				details
			}
		}
	}`

	variables := map[string]interface{}{
		"filter": map[string]interface{}{
			"createdAt": map[string]string{
				"after": since,
			},
		},
	}

	requestBody := map[string]interface{}{
		"query":     query,
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

	req, err := http.NewRequest("POST", graphqlEndpoint, bytes.NewBuffer(jsonBody))
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

	if resp.StatusCode != http.StatusOK {
		return nil, since, "", fmt.Errorf("error response from Wiz API: %s", body)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, since, "", fmt.Errorf("error parsing response: %v", err)
	}

	data, ok := result["data"].(map[string]interface{})
	if !ok {
		return nil, since, "", fmt.Errorf("invalid response format")
	}

	securityIssues, ok := data["securityIssues"].(map[string]interface{})
	if !ok {
		return nil, since, "", fmt.Errorf("invalid security issues format")
	}

	issues, ok := securityIssues["issues"].([]interface{})
	if !ok {
		return nil, since, "", fmt.Errorf("invalid issues format")
	}

	for _, issue := range issues {
		issueMap, ok := issue.(map[string]interface{})
		if !ok {
			continue
		}

		id, ok := issueMap["id"].(string)
		if !ok {
			continue
		}

		if id != lastEventId {
			createdAt, ok := issueMap["createdAt"].(string)
			if !ok {
				continue
			}

			lastDetectionTime = createdAt
			alerts = append(alerts, issueMap)
			eventId = id
		}
	}

	return alerts, lastDetectionTime, eventId, nil
}
