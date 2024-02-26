//go:build darwin
// +build darwin

package usp_mac

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"encoding/json"
	"syscall"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

const (
	defaultWriteTimeout = 60 * 10
)

type MacAdapter struct {
	conf         MacConfig
	wg           sync.WaitGroup
	isRunning    uint32
	mRunning     sync.RWMutex
	uspClient    *uspclient.Client
	writeTimeout time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup

	ctx context.Context
}

func NewMacAdapter(conf MacConfig) (*MacAdapter, chan struct{}, error) {
	a := &MacAdapter{
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
	go a.handleEvent()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *MacAdapter) Close() error {
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

func (a *MacAdapter) convertStructToMap(obj interface{}) (map[string]interface{}) {
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

func (a *MacAdapter) handleEvent() uintptr {

	logs := NewLogs()

	signalChannel := make(chan os.Signal)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChannel
		logs.StopGathering()
		os.Exit(0)
	}()

	if err := logs.StartGathering(); err != nil {
		panic(err)
	}

	for log := range logs.Channel {
		msg := &protocol.DataMessage{
			JsonPayload: a.convertStructToMap(log),
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

	return 0
}
