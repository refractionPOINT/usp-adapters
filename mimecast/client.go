package usp_mimecast

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

const (
	baseURL       = "https://api.services.mimecast.com"
	overlapPeriod = 30 * time.Second
)

type MimecastAdapter struct {
	conf       MimecastConfig
	uspClient  *uspclient.Client
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context

	dedupe map[string]int64
	urls   []string
}

type AuthResponse struct {
	AccessToken string `json:"access_token"`
}

type AuditRequest struct {
	Data []AuditEvent `json:"data"`
	Meta MetaData     `json:"meta"`
}

type AuditEvent struct {
	StartDateTime string `json:"startDateTime"`
	EndDateTime   string `json:"endDateTime"`
}

type ApiResponse struct {
	Meta MetaData         `json:"meta"`
	Data []AuditLog       `json:"data"`
	Fail []FailureDetails `json:"fail"`
}

type FailureDetails struct {
	Errors []ErrorDetail `json:"errors"`
}

type ErrorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type MetaData struct {
	Pagination Pagination `json:"pagination"`
	Status     int        `json:"status"`
}

type Pagination struct {
	PageSize int    `json:"pageSize"`
	Next     string `json:"next"`
}

type AuditLog struct {
	ID        string `json:"id"`
	AuditType string `json:"auditType"`
	User      string `json:"user"`
	EventTime string `json:"eventTime"`
	EventInfo string `json:"eventInfo"`
	Category  string `json:"category"`
}

type MimecastConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ClientId      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`
	URLs          string                  `json:"urls" yaml:"urls"`
}

func (c *MimecastConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ClientId == "" {
		return errors.New("missing client id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client secret")
	}
	// URLs is optional as we have a default
	return nil
}

func NewMimecastAdapter(conf MimecastConfig) (*MimecastAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	var err error
	a := &MimecastAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
		dedupe: make(map[string]int64),
	}

	// Initialize base URLs
	if conf.URLs != "" {
		// Split URLs and trim spaces
		for _, url := range strings.Split(conf.URLs, ",") {
			url = strings.TrimSpace(url)
			if url == "" {
				continue
			}
			// Make sure the URL starts with a slash
			if !strings.HasPrefix(url, "/") {
				url = "/" + url
			}
			a.urls = append(a.urls, url)
		}
	}
	// Add default URL if no URLs specified
	if len(a.urls) == 0 {
		a.urls = []string{"/api/audit/get-audit-events"}
	}

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

	// Start a fetcher for each URL
	for _, url := range a.urls {
		a.wgSenders.Add(1)
		go a.fetchEvents(baseURL + url)
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *MimecastAdapter) Close() error {
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

func (a *MimecastAdapter) fetchEvents(url string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", baseURL))

	since := time.Now().Add(-400 * time.Hour)

	for !a.doStop.WaitFor(30 * time.Second) {
		items, newSince, _ := a.makeOneRequest(url, since)
		since = newSince
		if items == nil {
			continue
		}

		for _, item := range items {
			// Add source URL to the event data
			item["source_url"] = url

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

func (a *MimecastAdapter) makeOneRequest(url string, since time.Time) ([]utils.Dict, time.Time, error) {
	var allItems []utils.Dict
	currentTime := time.Now()
	var start string
	var lastDetectionTime time.Time

	if t := currentTime.Add(-overlapPeriod); t.Before(since) {
		start = since.UTC().Format("2006-01-02T15:04:05-0700")
	} else {
		start = currentTime.Add(-overlapPeriod).UTC().Format("2006-01-02T15:04:05-0700")
	}
	end := currentTime.UTC().Format("2006-01-02T15:04:05-0700")

	pageToken := ""

	for {
		// Prepare the request.
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s start %s end %s", url, start, end))

		auditData := map[string]interface{}{
			"meta": map[string]interface{}{
				"pagination": map[string]interface{}{
					"pageSize": 50,
				},
			},
			"data": []map[string]string{
				{
					"startDateTime": start,
					"endDateTime":   end,
				},
			},
		}

		if pageToken != "" {
			auditData["meta"].(map[string]interface{})["pagination"].(map[string]interface{})["pageToken"] = pageToken
		}

		jsonData, err := json.Marshal(auditData)
		if err != nil {
			return nil, lastDetectionTime, err
		}

		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
		if err != nil {
			return nil, lastDetectionTime, err
		}
		token, err := a.getAuthToken()
		if err != nil {
			return nil, lastDetectionTime, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return nil, lastDetectionTime, err
		}
		defer resp.Body.Close()

		// Evaluate if success.
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			a.conf.ClientOptions.OnError(fmt.Errorf("mimecast api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, string(body), string(body)))
			return nil, lastDetectionTime, err
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error: %v", err))
			return nil, lastDetectionTime, err
		}

		// Parse the response.
		var response ApiResponse
		//a.conf.ClientOptions.DebugLog(fmt.Sprintf("results: %s", body))
		err = json.Unmarshal(body, &response)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("mimecast api invalid json: %v", err))
			return nil, lastDetectionTime, err
		}
		//responseStr, _ := json.Marshal(response)
		//a.conf.ClientOptions.DebugLog(fmt.Sprintf("results: %s", responseStr))

		// Collect items.
		items := response.Data
		var newItems []utils.Dict
		lastDetectionTime = since
		for _, item := range items {
			newItem := utils.Dict{
				"id":        item.ID,
				"auditType": item.AuditType,
				"user":      item.User,
				"eventTime": item.EventTime,
				"eventInfo": item.EventInfo,
				"category":  item.Category,
			}

			timestamp := item.EventTime
			eventid := item.ID
			if _, ok := a.dedupe[eventid]; ok {
				continue
			}
			epoch, _ := time.Parse(time.RFC3339, timestamp)
			a.dedupe[eventid] = epoch.Unix()
			newItems = append(newItems, newItem)
			lastDetectionTime = epoch
		}
		allItems = append(allItems, newItems...)

		// Check if we need to make another request.
		if response.Meta.Pagination.Next != "" {
			pageToken = response.Meta.Pagination.Next
		} else {
			break
		}
	}

	// Cull old dedupe entries.
	for k, v := range a.dedupe {
		if v < time.Now().Add(-overlapPeriod).Unix() {
			delete(a.dedupe, k)
		}
	}

	return allItems, lastDetectionTime, nil
}

func (a *MimecastAdapter) getAuthToken() (string, error) {
	url := baseURL + "/oauth/token"
	body := "grant_type=client_credentials&client_id=" + a.conf.ClientId + "&client_secret=" + a.conf.ClientSecret

	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(body)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get token, status code: %d", resp.StatusCode)
	}

	respBody, _ := ioutil.ReadAll(resp.Body)
	var authResp AuthResponse
	if err := json.Unmarshal(respBody, &authResp); err != nil {
		return "", err
	}

	return authResp.AccessToken, nil
}
