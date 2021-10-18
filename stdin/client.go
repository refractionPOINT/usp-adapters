package usp_stdin

import (
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
	dbgLog       func(string)
	uspClient    *uspclient.Client
	writeTimeout time.Duration
}

type StdinConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
}

func NewStdinAdapter(conf StdinConfig) (*StdinAdapter, chan struct{}, error) {
	a := &StdinAdapter{
		conf: conf,
		dbgLog: func(s string) {
			if conf.ClientOptions.DebugLog == nil {
				return
			}
			conf.ClientOptions.DebugLog(s)
		},
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
	a.dbgLog("closing")
	atomic.StoreUint32(&a.isRunning, 0)
	_, err := a.uspClient.Close()
	if err != nil {
		return err
	}
	return nil
}

func (a *StdinAdapter) handleInput() {
	readBufferSize := 1024 * 16
	st := utils.StreamTokenizer{
		ExpectedSize: readBufferSize * 2,
		Token:        0x0a,
	}

	readBuffer := make([]byte, readBufferSize)
	for atomic.LoadUint32(&a.isRunning) == 1 {
		sizeRead, err := os.Stdin.Read(readBuffer[:])
		if err != nil {
			if err != io.EOF {
				a.dbgLog(fmt.Sprintf("os.Stdin.Read(): %v", err))
			}
			return
		}

		data := readBuffer[:sizeRead]

		chunks, err := st.Add(data)
		if err != nil {
			a.dbgLog(fmt.Sprintf("tokenizer: %v", err))
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
		a.dbgLog("stream falling behind")
		err = a.uspClient.Ship(msg, 0)
	}
	if err != nil {
		a.dbgLog(fmt.Sprintf("Ship(): %v", err))
	}
}