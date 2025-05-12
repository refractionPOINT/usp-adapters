package usp_falconcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"

	"github.com/crowdstrike/gofalcon/falcon"
	"github.com/crowdstrike/gofalcon/falcon/client"
	"github.com/crowdstrike/gofalcon/falcon/client/event_streams"
	"github.com/crowdstrike/gofalcon/falcon/models"
)

const (
	defaultWriteTimeout = 60 * 10
)

type FalconCloudConfig struct {
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ClientId        string                  `json:"client_id" yaml:"client_id"`
	ClientSecret    string                  `json:"client_secret" yaml:"client_secret"`
}

func (c *FalconCloudConfig) Validate() error {
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

type FalconCloudAdapter struct {
	conf         FalconCloudConfig
	isRunning    uint32
	mRunning     sync.RWMutex
	uspClient    *uspclient.Client
	writeTimeout time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

func NewFalconCloudAdapter(conf FalconCloudConfig) (*FalconCloudAdapter, chan struct{}, error) {
	a := &FalconCloudAdapter{
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

	a.ctx, a.cancel = context.WithCancel(context.Background())
	a.chStopped = make(chan struct{})

	a.wgSenders.Add(1)
	go func() {
		defer a.wgSenders.Done()
		a.handleEvent(a.conf.ClientId, a.conf.ClientSecret)
	}()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *FalconCloudAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")

	a.mRunning.Lock()
	a.isRunning = 0
	a.mRunning.Unlock()

	// Cancel the context to stop all streams
	a.cancel()

	a.wgSenders.Wait()

	_, err := a.uspClient.Close()
	if err != nil {
		return err
	}
	return nil
}

func (a *FalconCloudAdapter) convertStructToMap(obj interface{}) map[string]interface{} {
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

func (a *FalconCloudAdapter) handleEvent(clientId string, clientSecret string) {
	client, err := falcon.NewClient(&falcon.ApiConfig{
		ClientId:     clientId,
		ClientSecret: clientSecret,
		Context:      a.ctx,
	})
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("falcon.NewClient(): %v", err))
		return
	}

	jsonFormat := "json"
	appName := fmt.Sprintf("lc-adapter-%s", uuid.New().String()[:8])
	response, err := client.EventStreams.ListAvailableStreamsOAuth2(&event_streams.ListAvailableStreamsOAuth2Params{
		AppID:   appName,
		Format:  &jsonFormat,
		Context: a.ctx,
	})
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("falcon.EventStreams.ListAvailableStreamsOAuth2(): %v", err))
		return
	}

	if err = falcon.AssertNoError(response.Payload.Errors); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("falcon.AssertNoError(): %v", err))
		return
	}

	availableStreams := response.Payload.Resources
	if len(availableStreams) == 0 {
		a.conf.ClientOptions.OnError(fmt.Errorf("No available stream was found for AppID=%s. Ensure no other instance is running or check API permissions.\n", appName))
		return
	}

	var wg sync.WaitGroup
	for _, streamInfo := range availableStreams {
		wg.Add(1)
		go func(streamInfo *models.MainAvailableStreamV2) {
			defer wg.Done()
			a.handleStream(client, appName, streamInfo)
		}(streamInfo)
	}
	wg.Wait()
}

func (a *FalconCloudAdapter) handleStream(client *client.CrowdStrikeAPISpecification, appName string, streamInfo *models.MainAvailableStreamV2) {
	streamHandle, err := falcon.NewStream(a.ctx, client, appName, streamInfo, 0)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("falcon.NewStream(): %v", err))
		return
	}
	defer streamHandle.Close()

	for {
		select {
		case <-a.ctx.Done():
			return
		case err := <-streamHandle.Errors:
			a.conf.ClientOptions.OnError(fmt.Errorf("stream error: %v", err))
			return
		case event := <-streamHandle.Events:
			msg := &protocol.DataMessage{
				JsonPayload: a.convertStructToMap(event),
				TimestampMs: uint64(time.Now().UnixMilli()),
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
}
