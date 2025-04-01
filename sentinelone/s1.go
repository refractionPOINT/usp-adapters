package sentinelone

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

type SentinelOneAdapter struct {
	conf       SentinelOneConfig
	uspClient  *uspclient.Client
	httpClient *http.Client
	s1Client   *SentinelOneClient

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type SentinelOneConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Domain        string                  `json:"domain" yaml:"domain"`
	APIKey        string                  `json:"api_key" yaml:"api_key"`
	URLs          []string                `json:"urls" yaml:"urls"`
	StartTime     string                  `json:"start_time" yaml:"start_time"`
}

func (c *SentinelOneConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Domain == "" {
		return errors.New("missing domain")
	}
	if c.APIKey == "" {
		return errors.New("missing api_key")
	}
	if !strings.HasPrefix(c.Domain, "https://") {
		c.Domain = "https://" + c.Domain
	}
	c.Domain = strings.TrimSuffix(c.Domain, "/")
	return nil
}

func NewSentinelOneAdapter(conf SentinelOneConfig) (*SentinelOneAdapter, chan struct{}, error) {
	var err error

	a := &SentinelOneAdapter{
		conf:     conf,
		ctx:      context.Background(),
		doStop:   utils.NewEvent(),
		s1Client: NewSentinelOneClient(conf.Domain, conf.APIKey),
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

	// Set sane default for the content types.
	if len(a.conf.URLs) == 0 {
		a.conf.URLs = []string{
			"/web/api/v2.1/actvites",
			"/web/api/v2.1/cloud-detecton/alerts",
			"/web/api/v2.1/threats",
		}
	}
	for i, url := range a.conf.URLs {
		if !strings.HasPrefix(url, "/") {
			url = "/" + url
		}
		a.conf.URLs[i] = url
	}

	nCollecting := 0
	for _, ct := range a.conf.URLs {
		ct = strings.TrimSpace(ct)
		if ct == "" {
			continue
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting to fetch %s events", ct))

		nCollecting++

		a.wgSenders.Add(1)
		url := fmt.Sprintf("%s%s", a.conf.Domain, ct)
		go a.fetchEvents(url)
	}

	if nCollecting == 0 {
		a.Close()
		return nil, nil, errors.New("no content types specified")
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *SentinelOneAdapter) Close() error {
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

func (a *SentinelOneAdapter) fetchEvents(endpoint string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("fetching of events exiting")

	isFirstRun := true
	lastCreatedAt := ""
	for isFirstRun || (!a.doStop.IsSet()) || !a.doStop.WaitFor(1*time.Minute) {
		qValues := url.Values{}
		now := time.Now().UTC()
		if isFirstRun {
			start := a.conf.StartTime
			if start == "" {
				start = now.Add(-15 * time.Second).Format("2006-01-02T15:04:05Z")
			}
			end := now.Format("2006-01-02T15:04:05Z")
			qValues.Set("createdat__gte", start)
			qValues.Set("createdat__lte", end)
		} else {
			qValues.Set("createdat__gt", lastCreatedAt)
			qValues.Set("createdat__lte", now.Format("2006-01-02T15:04:05Z"))
		}
		isFirstRun = false
		nextPage := ""
		nFetched := 0
		for {
			if nextPage != "" {
				qValues.Set("cursor", nextPage)
			}

			resp, err := a.s1Client.GetFromAPI(a.ctx, endpoint, qValues)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("GetFromAPI(): %v", err))
				a.doStop.Set()
				return
			}
			if resp.NextCursor != nil {
				nextPage = *resp.NextCursor
			} else {
				nextPage = ""
			}
			for _, event := range resp.Data {
				msg := &protocol.DataMessage{
					JsonPayload: event,
					TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
				}
				if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
					if err == uspclient.ErrorBufferFull {
						a.conf.ClientOptions.OnWarning("stream falling behind")
						err = a.uspClient.Ship(msg, 1*time.Hour)
					}
					if err != nil {
						a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
						a.doStop.Set()
						break
					}
				}
				// The creeatedAt field varies per data source, but it's always either at the root
				// or one level deep. So look for both.
				if lca, ok := event["createdAt"].(string); ok {
					lastCreatedAt = lca
				} else {
					isFound := false
					for _, v := range event {
						subEvent, ok := v.(map[string]interface{})
						if !ok {
							continue
						}
						if lca, ok := subEvent["createdAt"].(string); ok {
							lastCreatedAt = lca
							isFound = true
							break
						}
					}
					if !isFound {
						a.conf.ClientOptions.OnError(fmt.Errorf("createdAt not found in event: %v", event))
					}
				}
			}
			if nextPage == "" {
				break
			}
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetched %d events", nFetched))
	}
}
