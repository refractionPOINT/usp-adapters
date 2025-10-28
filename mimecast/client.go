package usp_mimecast

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
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
	AccessKey     string                  `json:"access_key" yaml:"access_key"`
	SecretKey     string                  `json:"secret_key" yaml:"secret_key"`
	AppId         string                  `json:"app_id" yaml:"app_id"`
	AppKey        string                  `json:"app_key" yaml:"app_key"`
	BaseURL       string                  `json:"base_url" yaml:"base_url"`
}

func (c *MimecastConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.AccessKey == "" {
		return errors.New("missing access_key")
	}
	if c.SecretKey == "" {
		return errors.New("missing secret_key")
	}
	if c.AppId == "" {
		return errors.New("missing app_id")
	}
	if c.AppKey == "" {
		return errors.New("missing app_key")
	}
	if c.BaseURL == "" {
		return errors.New("missing base_url (e.g., https://us-api.mimecast.com)")
	}

	return nil
}

func NewMimecastAdapter(conf MimecastConfig) (*MimecastAdapter, chan struct{}, error) {
	var err error
	a := &MimecastAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
		dedupe: make(map[string]int64),
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

	a.wgSenders.Add(1)
	go a.fetchEvents()

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

// generateHMACHeaders generates the required Mimecast HMAC-SHA1 authentication headers
func (a *MimecastAdapter) generateHMACHeaders(uri string) (map[string]string, error) {
	// Generate request ID (GUID)
	reqId := uuid.New().String()

	// Generate timestamp in RFC format: "Mon, 02 Jan 2006 15:04:05 MST"
	// Mimecast expects UTC timezone
	timestamp := time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 MST")

	// Construct the data to sign: {timestamp}:{reqId}:{uri}:{appKey}
	dataToSign := fmt.Sprintf("%s:%s:%s:%s", timestamp, reqId, uri, a.conf.AppKey)

	// Base64-decode the secret key
	decodedSecret, err := base64.StdEncoding.DecodeString(a.conf.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decode secret key: %v", err)
	}

	// Generate HMAC-SHA1 signature
	h := hmac.New(sha1.New, decodedSecret)
	h.Write([]byte(dataToSign))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	// Return all required headers
	headers := map[string]string{
		"Authorization": fmt.Sprintf("MC %s:%s", a.conf.AccessKey, signature),
		"x-mc-req-id":   reqId,
		"x-mc-date":     timestamp,
		"x-mc-app-id":   a.conf.AppId,
	}

	return headers, nil
}

func (a *MimecastAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", a.conf.BaseURL))

	since := time.Now().Add(-400 * time.Hour)

	for !a.doStop.WaitFor(30 * time.Second) {
		// The makeOneRequest function handles error
		// handling and fatal error handling.
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

func (a *MimecastAdapter) makeOneRequest(since time.Time) ([]utils.Dict, time.Time, error) {
	var allItems []utils.Dict
	currentTime := time.Now()
	var start string
	var lastDetectionTime time.Time

	if t := currentTime.Add(-overlapPeriod); t.Before(since) {
		start = since.UTC().Format(time.RFC3339)
	} else {
		start = currentTime.Add(-overlapPeriod).UTC().Format(time.RFC3339)
	}
	end := currentTime.UTC().Format(time.RFC3339)

	pageToken := ""
	endpoint := "/api/audit/get-audit-events"
	url := a.conf.BaseURL + endpoint

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

		// Generate HMAC authentication headers
		headers, err := a.generateHMACHeaders(endpoint)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("failed to generate HMAC headers: %v", err))
			return nil, lastDetectionTime, err
		}

		// Set all required Mimecast headers
		for key, value := range headers {
			req.Header.Set(key, value)
		}
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
			err := fmt.Errorf("mimecast api non-200: %s\nRESPONSE: %s", resp.Status, string(body))
			a.conf.ClientOptions.OnError(err)
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

		// Check for Mimecast-specific errors in the fail array
		if len(response.Fail) > 0 {
			var errorMessages []string
			for _, failure := range response.Fail {
				for _, errDetail := range failure.Errors {
					errorMessages = append(errorMessages, fmt.Sprintf("%s: %s (retryable: %v)", errDetail.Code, errDetail.Message, errDetail.Retryable))
				}
			}
			a.conf.ClientOptions.OnError(fmt.Errorf("mimecast api returned errors: %v", errorMessages))
			return nil, lastDetectionTime, fmt.Errorf("mimecast api errors: %v", errorMessages)
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
