package usp_simulator

import (
	"encoding/json"
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
	sleepDelta          = 1 * time.Second
)

type SimulatorAdapter struct {
	conf         SimulatorConfig
	wg           sync.WaitGroup
	isRunning    uint32
	uspClient    utils.Shipper
	writeTimeout time.Duration
	dataReader   io.ReadCloser

	lastEventTime int64
	lastSentTime  int64
}

type SimulatorConfig struct {
	ClientOptions  uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Reader         io.ReadCloser           `json:"-" yaml:"-"`
	FilePath       string                  `json:"file_path" yaml:"file_path"`
	IsReplayTiming bool                    `json:"is_replay_timing" yaml:"is_replay_timing"`
	Filters []utils.FilterPattern `json:"filters,omitempty" yaml:"filters,omitempty"`
}

type basicLCEvent struct {
	Routing struct {
		EventTime int64 `json:"event_time"`
	} `json:"routing"`
}

func (c *SimulatorConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	return nil
}

func NewSimulatorAdapter(conf SimulatorConfig) (*SimulatorAdapter, chan struct{}, error) {
	a := &SimulatorAdapter{
		conf:      conf,
		isRunning: 1,
	}

	a.writeTimeout = defaultWriteTimeout

	if conf.FilePath != "" {
		f, err := os.Open(conf.FilePath)
		if err != nil {
			return nil, nil, err
		}
		a.dataReader = f
	} else {
		a.dataReader = conf.Reader
	}

	var err error
	client, err := uspclient.NewClient(conf.ClientOptions)
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

func (a *SimulatorAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	atomic.StoreUint32(&a.isRunning, 0)
	a.dataReader.Close()
	a.wg.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *SimulatorAdapter) handleInput() {
	readBufferSize := 1024 * 16
	st := utils.StreamTokenizer{
		ExpectedSize: readBufferSize * 2,
		Token:        0x0a,
	}

	readBuffer := make([]byte, readBufferSize)
	for atomic.LoadUint32(&a.isRunning) == 1 {
		sizeRead, err := a.dataReader.Read(readBuffer)
		if err != nil {
			if err != io.EOF {
				a.conf.ClientOptions.OnError(fmt.Errorf("io.Read(): %v", err))
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

func (a *SimulatorAdapter) handleLine(line []byte) {
	if len(line) == 0 {
		return
	}

	// If Replay Timing is enabled, parse the line
	// and look at replaying it based on the routing.
	if a.conf.IsReplayTiming {
		a.replayTimedEvent(line)
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

func (a *SimulatorAdapter) replayTimedEvent(line []byte) bool {
	evt := basicLCEvent{}
	err := json.Unmarshal(line, &evt)
	if err != nil {
		return false
	}
	if a.lastEventTime > evt.Routing.EventTime {
		return false
	}
	evtDelta := evt.Routing.EventTime - a.lastEventTime
	now := time.Now().UnixMilli()
	clockDelta := now - a.lastSentTime
	if clockDelta < evtDelta {
		totalSleep := time.Duration(evtDelta-clockDelta) * time.Millisecond
		for atomic.LoadUint32(&a.isRunning) == 1 && totalSleep != 0 {
			if totalSleep > sleepDelta {
				time.Sleep(sleepDelta)
				totalSleep -= sleepDelta
			} else {
				time.Sleep(totalSleep)
				totalSleep = 0
			}
		}
	}
	a.lastEventTime = evt.Routing.EventTime
	a.lastSentTime = now
	return true
}
