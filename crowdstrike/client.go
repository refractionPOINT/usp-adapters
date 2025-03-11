package usp_crowdstrike

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"

	"github.com/crowdstrike/gofalcon/falcon"
	"github.com/crowdstrike/gofalcon/falcon/client/event_streams"
)

const (
	defaultWriteTimeout = 60 * 10
)

type CrowdStrikeConfig struct {
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ClientId        string                  `json:"client_id" yaml:"client_id"`
	ClientSecret    string                  `json:"client_secret" yaml:"client_secret"`
}

func (c *CrowdStrikeConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ClientId == "" {
		return errors.New("missing client id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client secret")
	}
	return nil
}

type CrowdStrikeAdapter struct {
	conf         CrowdStrikeConfig
	wg           sync.WaitGroup
	isRunning    uint32
	mRunning     sync.RWMutex
	uspClient    *uspclient.Client
	writeTimeout time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup

	ctx context.Context
}

func NewCrowdStrikeAdapter(conf CrowdStrikeConfig) (*CrowdStrikeAdapter, chan struct{}, error) {
	a := &CrowdStrikeAdapter{
		conf:      conf,
		isRunning: 1,
	}

	if a.conf.WriteTimeoutSec == 0 {
		a.conf.WriteTimeoutSec = defaultWriteTimeout
	}
	a.writeTimeout = time.Duration(a.conf.WriteTimeoutSec) * time.Second

	var err error
	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	a.chStopped = make(chan struct{})

	a.wgSenders.Add(1)
	go a.handleEvent(a.conf.ClientId, a.conf.ClientSecret)

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *CrowdStrikeAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")

	a.mRunning.Lock()
	a.isRunning = 0
	a.mRunning.Unlock()

	a.wg.Done()
	a.wg.Wait()

	_, err := a.uspClient.Close()
	if err != nil {
		return err
	}
	return nil
}

func (a *CrowdStrikeAdapter) convertStructToMap(obj interface{}) map[string]interface{} {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil
	}

	var mapRepresentation map[string]interface{}
	err = json.Unmarshal(data, &mapRepresentation)
	if err != nil {
		return nil
	}

	return mapRepresentation
}

func (a *CrowdStrikeAdapter) handleEvent(clientId string, clientSecret string) uintptr {
	appName := flag.String("app-name", "falcon_event_stream", "Application name (needs to be unique in your Falcon environment)")

	client, err := falcon.NewClient(&falcon.ApiConfig{
		ClientId:     clientId,
		ClientSecret: clientSecret,
		Context:      context.Background(),
	})
	if err != nil {
		panic(err)
	}

	jsonFormat := "json"
	response, err := client.EventStreams.ListAvailableStreamsOAuth2(&event_streams.ListAvailableStreamsOAuth2Params{
		AppID:   *appName,
		Format:  &jsonFormat,
		Context: context.Background(),
	})
	if err != nil {
		panic(falcon.ErrorExplain(err))
	}

	if err = falcon.AssertNoError(response.Payload.Errors); err != nil {
		panic(err)
	}

	availableStreams := response.Payload.Resources
	if len(availableStreams) == 0 {
		fmt.Printf("No available stream was found for AppID=%s. Ensure no other instance is running or check API permissions.\n", *appName)
		return 0
	}

	for _, availableStream := range availableStreams {
		stream, err := falcon.NewStream(context.Background(), client, *appName, availableStream, 0)
		if err != nil {
			panic(err)
		}
		defer stream.Close()

		var fatalErr error
		for fatalErr == nil {
			select {
			case err := <-stream.Errors:
				if err.Fatal {
					fatalErr = err.Err
				} else {
					fmt.Fprintln(os.Stderr, err)
				}
			case event := <-stream.Events:
				msg := &protocol.DataMessage{
					JsonPayload: a.convertStructToMap(event),
					TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
				}
				err := a.uspClient.Ship(msg, a.writeTimeout)
				if err == uspclient.ErrorBufferFull {
					a.conf.ClientOptions.OnWarning("stream falling behind")
					err = a.uspClient.Ship(msg, 0)
				}
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				}
			}
		}
		panic(fatalErr)
	}

	return 0
}
