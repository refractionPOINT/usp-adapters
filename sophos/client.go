package usp_sophos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	eventsURL = "/siem/v1/events"
)

type opRequest struct {
	Limit     int    `json:"page[size],omitempty"`
	StartTime string `json:"filter[created_at],omitempty"`
	Sort      string `json:"sort,omitempty"`
}

type SophosAdapter struct {
	conf       SophosConfig
	uspClient  *uspclient.Client
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type SophosConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ClientId      string                  `json:"clientid" yaml:"clientid"`
	ClientSecret  string                  `json:"clientsecret" yaml:"clientsecret"`
	TenantId      string                  `json:"tenantid" yaml:"tenantid"`
	URL           string                  `json:"url" yaml:"url"`
}

func (c *SophosConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.URL == "" {
		return errors.New("missing url")
	}
	if c.ClientId == "" {
		return errors.New("missing client id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client secret")
	}
	if c.TenantId == "" {
		return errors.New("missing tenant id")
	}
	return nil
}

func NewSophosAdapter(conf SophosConfig) (*SophosAdapter, chan struct{}, error) {
	var err error
	a := &SophosAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
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
	go a.fetchEvents(eventsURL)

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *SophosAdapter) Close() error {
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

func (a *SophosAdapter) fetchEvents(url string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of %s events exiting", url))

	lastCursor := ""
	has_more := "false"
	for !a.doStop.WaitFor(30 * time.Second) {
		// The makeOneRequest function handles error
		// handling and fatal error handling.
		items, newCursor, has_more_resp := a.makeOneRequest(url, lastCursor, has_more)
		lastCursor = newCursor
		has_more = has_more_resp
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

func (a *SophosAdapter) getJwt() string {

	auth_url := "https://id.sophos.com/api/v2/oauth2/token"
	method := "POST"

	payload := strings.NewReader(fmt.Sprintf("grant_type=client_credentials&scope=token&client_id=%s&client_secret=%s", a.conf.ClientId, a.conf.ClientSecret))

	client := &http.Client{}
	req, err := http.NewRequest(method, auth_url, payload)

	if err != nil {
		fmt.Println(err)
		return ""
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer resp.Body.Close()

	// Evaluate if success.
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		//	a.conf.ClientOptions.DebugLog(fmt.Sprintf("response: %s", body))
		//	a.conf.ClientOptions.DebugLog(fmt.Sprintf("error code: %s", resp.StatusCode))

		a.conf.ClientOptions.OnError(fmt.Errorf("sophos api non-200: %s\nRESPONSE: %s", resp.Status, string(body)))
		return ""
	}

	// Parse the response.
	respData := utils.Dict{}
	jsonDecoder := json.NewDecoder(resp.Body)
	if err := jsonDecoder.Decode(&respData); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("sophos api invalid json: %v", err))
		return ""
	}

	// Report if a cursor was returned
	// as well as the items.
	token := respData.FindOneString("access_token")
	//a.conf.ClientOptions.DebugLog(fmt.Sprintf("response data: %s", respData))

	return token
}

func (a *SophosAdapter) makeOneRequest(url string, lastCursor string, has_more string) ([]utils.Dict, string, string) {

	// Get JWT
	token := a.getJwt()

	// Prepare the request body.
	reqData := opRequest{}
	b, err := json.Marshal(reqData)
	if err != nil {
		a.doStop.Set()
		return nil, "", ""
	}

	// Get request timestamp
	currentTime := time.Now().Unix()
	thirtySecondsAgo := currentTime - 30
	strTimestamp := strconv.FormatInt(thirtySecondsAgo, 10)

	// Prepare the request.
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s?from_date=%s&limit=200", a.conf.URL, url, strTimestamp), nil)
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s%s starting at %s, has_more: %s", a.conf.URL, url, strTimestamp, has_more))
	if has_more != "false" {
		req, err = http.NewRequest("GET", fmt.Sprintf("%s%s?cursor=%s&limit=200", a.conf.URL, url, lastCursor), nil)
		//a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s%s starting at %s, has_more: %s", a.conf.URL, url, strTimestamp, has_more))
	}
	if err != nil {
		a.doStop.Set()
		return nil, "", ""
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", a.conf.TenantId)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	//
	//a.conf.ClientOptions.DebugLog(fmt.Sprintf("requesting from %s%s starting at %s", a.conf.URL, url, strTimestamp))

	// Issue the request.
	resp, err := a.httpClient.Do(req)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
		return nil, lastCursor, has_more
	}
	defer resp.Body.Close()

	// Evaluate if success.
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		//a.conf.ClientOptions.DebugLog(fmt.Sprintf("response: %s", body))
		//a.conf.ClientOptions.DebugLog(fmt.Sprintf("error code: %s", resp.StatusCode))

		a.conf.ClientOptions.OnError(fmt.Errorf("sophos api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, string(b), string(body)))
		return nil, lastCursor, has_more
	}

	// Parse the response.
	respData := utils.Dict{}
	jsonDecoder := json.NewDecoder(resp.Body)
	if err := jsonDecoder.Decode(&respData); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("sophos api invalid json: %v", err))
		return nil, lastCursor, has_more
	}

	// Report if a cursor was returned
	// as well as the items.
	lastCursor = respData.FindOneString("next_cursor")
	has_more = respData.FindOneString("has_more")
	items, _ := respData.GetListOfDict("items")
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("response data: %s", respData))

	return items, lastCursor, has_more
}
