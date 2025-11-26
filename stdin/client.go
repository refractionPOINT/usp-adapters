package usp_stdin

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultWriteTimeout = 60 * 10
)

type StdinAdapter struct {
	conf         StdinConfig
	wg           sync.WaitGroup
	isRunning    uint32
	uspClient    utils.Shipper
	writeTimeout time.Duration
}

type StdinConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	Filters []utils.FilterPattern `json:"filters,omitempty" yaml:"filters,omitempty"`
}

func (c *StdinConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	return nil
}

func NewStdinAdapter(ctx context.Context, conf StdinConfig) (*StdinAdapter, chan struct{}, error) {
	a := &StdinAdapter{
		conf:      conf,
		isRunning: 1,
	}

	if a.conf.WriteTimeoutSec == 0 {
		a.conf.WriteTimeoutSec = defaultWriteTimeout
	}
	a.writeTimeout = time.Duration(a.conf.WriteTimeoutSec) * time.Second

	var err error
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
	}()

	return a, chStopped, nil
}

func (a *StdinAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	atomic.StoreUint32(&a.isRunning, 0)
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *StdinAdapter) handleInput() {
	readBufferSize := 1024 * 16
	st := utils.StreamTokenizer{
		ExpectedSize: readBufferSize * 2,
		Token:        0x0a,
	}

	readBuffer := make([]byte, readBufferSize)
	for atomic.LoadUint32(&a.isRunning) == 1 {
		sizeRead, err := os.Stdin.Read(readBuffer)
		if err != nil {
			if err != io.EOF {
				a.conf.ClientOptions.OnError(fmt.Errorf("os.Stdin.Read(): %v", err))
			}
			return
		}

		data := readBuffer[:sizeRead]

		chunks, err := st.Add(data)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("tokenizer: %v", err))
		}
		for _, chunk := range chunks {
			a.handleLine(chunk)
		}
	}
}

func (a *StdinAdapter) handleLine(line []byte) {
	if len(line) == 0 {
		return
	}
	msg := &protocol.DataMessage{
		TextPayload: string(line),
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	err := a.uspClient.Ship(msg, a.writeTimeout)
	if err == uspclient.ErrorBufferFull {
		a.conf.ClientOptions.OnWarning("stream falling behind")
		err = a.uspClient.Ship(msg, 1*time.Hour)
	}
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
	}
}
