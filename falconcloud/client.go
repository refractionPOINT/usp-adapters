package usp_falconcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"

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
	IsUsingOffset   bool                    `json:"is_using_offset" yaml:"is_using_offset"`
	Offset          uint64                  `json:"offset" yaml:"offset"`
	NotBefore       *time.Time              `json:"not_before,omitempty" yaml:"not_before,omitempty"`
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

func NewFalconCloudAdapter(ctx context.Context, conf FalconCloudConfig) (*FalconCloudAdapter, chan struct{}, error) {
	a := &FalconCloudAdapter{
		conf:      conf,
		isRunning: 1,
	}

	if a.conf.WriteTimeoutSec == 0 {
		a.conf.WriteTimeoutSec = defaultWriteTimeout
	}
	a.writeTimeout = time.Duration(a.conf.WriteTimeoutSec) * time.Second

	var err error
	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
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
	defer utils.StopBlockedProducers(a.uspClient)()
	a.conf.ClientOptions.DebugLog("closing")

	a.mRunning.Lock()
	a.isRunning = 0
	a.mRunning.Unlock()

	// Cancel the context to stop all streams
	a.cancel()

	a.wgSenders.Wait()

	_, err := utils.DrainCloseUSP(a.uspClient, a.conf.ClientOptions)
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
	appName := newAppID()
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
	// A failed or ended feed stops sibling feeds too, allowing rediscovery.
	defer a.cancel()
	// If IsUsingOffset is true, use the offset, otherwise use 0
	// and drop all events where the timestamp is before the start
	// time of the stream.
	var notBefore time.Time
	offset := uint64(0)
	if a.conf.IsUsingOffset {
		offset = a.conf.Offset
	} else {
		// If NotBefore is configured, use it; otherwise use current time
		if a.conf.NotBefore != nil && !a.conf.NotBefore.IsZero() {
			notBefore = *a.conf.NotBefore
		} else {
			notBefore = time.Now()
		}
	}
	streamHandle, err := a.newStream(client, appName, streamInfo, offset)
	if err != nil {
		if a.ctx.Err() == nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("falcon.NewStream(): %v", err))
		}
		return
	}
	defer streamHandle.Close()

	a.consumeStream(streamHandle, notBefore, a.uspClient.Ship)
}

func newAppID() string {
	return "lcadapter" + uuid.New().String()[:8]
}

// The SDK refreshes partition zero by default. Bind the discovered partition
// to a private client copy so concurrent streams cannot change each other.
type partitionStreams struct {
	event_streams.ClientService
	partition int64
}

func (p *partitionStreams) RefreshActiveStreamSession(params *event_streams.RefreshActiveStreamSessionParams, opts ...event_streams.ClientOption) (*event_streams.RefreshActiveStreamSessionOK, error) {
	copy := *params
	copy.Partition = p.partition
	return p.ClientService.RefreshActiveStreamSession(&copy, opts...)
}

func (a *FalconCloudAdapter) newStream(api *client.CrowdStrikeAPISpecification, appName string, info *models.MainAvailableStreamV2, offset uint64) (*falcon.StreamingHandle, error) {
	if info == nil || info.RefreshActiveSessionURL == nil {
		return nil, errors.New("missing stream refresh URL")
	}
	refreshURL, err := url.Parse(*info.RefreshActiveSessionURL)
	if err != nil {
		return nil, errors.New("invalid stream refresh URL")
	}
	partition, err := strconv.ParseInt(path.Base(refreshURL.Path), 10, 64)
	if err != nil || partition < 0 {
		return nil, errors.New("invalid stream refresh partition")
	}
	scoped := *api
	scoped.EventStreams = &partitionStreams{ClientService: api.EventStreams, partition: partition}
	return falcon.NewStream(a.ctx, &scoped, appName, info, offset)
}

func (a *FalconCloudAdapter) consumeStream(streamHandle *falcon.StreamingHandle, notBefore time.Time, ship func(*protocol.DataMessage, time.Duration) error) {
	events := streamHandle.Events
	for {
		select {
		case <-a.ctx.Done():
			return
		case err, ok := <-streamHandle.Errors:
			if !ok {
				return
			}
			if a.ctx.Err() != nil {
				return
			}
			if !err.Fatal {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("stream error: %v", err))
				continue
			}
			a.conf.ClientOptions.OnError(fmt.Errorf("stream error: %v", err))
			return
		case event, ok := <-events:
			if !ok {
				// The SDK queues the final error before closing Events. Keep reading
				// Errors so a closed feed cannot disappear without OnError.
				events = nil
				continue
			}
			if event == nil {
				continue
			}
			if !notBefore.IsZero() && notBefore.After(time.UnixMilli(int64(event.Metadata.EventCreationTime))) {
				// This is a very ugly hack for what is a bad API design.
				// It implies we HAVE to consume the entire 30 days of events
				// retained by Crowdstrike every time the adapter starts.
				continue
			}
			msg := &protocol.DataMessage{
				JsonPayload: a.convertStructToMap(event),
				TimestampMs: uint64(time.Now().UnixMilli()),
			}
			err := ship(msg, a.writeTimeout)
			if err == uspclient.ErrorBufferFull {
				a.conf.ClientOptions.OnWarning("stream falling behind")
				err = ship(msg, 0)
			}
			if err != nil {
				if err != uspclient.ErrorClosed {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				}
				// A failed admission cannot be skipped: stop this stream before
				// consuming any more events. Buffer pressure already retries the
				// same message above; a closed client cannot accept a retry.
				return
			}
		}
	}
}
