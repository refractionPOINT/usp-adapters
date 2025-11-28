package usp_evtx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"

	"github.com/refractionPOINT/evtx"
)

const (
	defaultWriteTimeout = 60 * 10
)

type EVTXAdapter struct {
	conf         EVTXConfig
	wg           sync.WaitGroup
	uspClient    utils.Shipper
	writeTimeout time.Duration

	chEvents chan evtx.GeneratedEvent
	fClose   func()
}

type EVTXConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	FilePath        string                  `json:"file_path" yaml:"file_path"`
	Filters []utils.FilterPattern `json:"filters,omitempty" yaml:"filters,omitempty"`
}

func (c *EVTXConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.FilePath == "" {
		return errors.New("file_path missing")
	}
	return nil
}

func NewEVTXAdapter(ctx context.Context, conf EVTXConfig) (*EVTXAdapter, chan struct{}, error) {
	a := &EVTXAdapter{
		conf: conf,
	}

	if a.conf.WriteTimeoutSec == 0 {
		a.conf.WriteTimeoutSec = defaultWriteTimeout
	}
	a.writeTimeout = time.Duration(a.conf.WriteTimeoutSec) * time.Second

	fd, err := os.Open(a.conf.FilePath)
	if err != nil {
		return nil, nil, err
	}

	a.chEvents, a.fClose, err = evtx.GenerateEvents(fd)

	if err != nil {
		return nil, nil, err
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

	chStopped := make(chan struct{})
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer close(chStopped)
		a.handleInput()
		a.conf.ClientOptions.DebugLog("finished processing file, waiting to drain")
		a.uspClient.Drain(10 * time.Minute)
	}()

	return a, chStopped, nil
}

func (a *EVTXAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.fClose()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *EVTXAdapter) handleInput() {
	for rec := range a.chEvents {
		if rec.Err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("evtx.Parse(): %v", rec.Err))
			break
		}
		a.handleEvent(rec.Event)
	}
}

func (a *EVTXAdapter) handleEvent(event map[string]interface{}) {
	if event == nil {
		return
	}
	// The underlying map contains some custom datastructure so we
	// need to do a JSON roundtrip to normalize it.
	b, err := json.Marshal(event)
	if err != nil {
		return
	}
	m := map[string]interface{}{}
	if err := json.Unmarshal(b, &m); err != nil {
		return
	}
	msg := &protocol.DataMessage{
		JsonPayload: m,
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	err = a.uspClient.Ship(msg, a.writeTimeout)
	if err == uspclient.ErrorBufferFull {
		a.conf.ClientOptions.OnWarning("stream falling behind")
		err = a.uspClient.Ship(msg, 1*time.Hour)
	}
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
	}
}
