//go:build darwin
// +build darwin

package usp_mac_unified_logging

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

const (
	defaultWriteTimeout = 60 * 10
)

type MacUnifiedLoggingAdapter struct {
	conf         MacUnifiedLoggingConfig
	uspClient    *uspclient.Client
	writeTimeout time.Duration

	logs *Logs

	chStopped chan struct{}
	wgSenders sync.WaitGroup

	ctx context.Context
}

func NewMacUnifiedLoggingAdapter(ctx context.Context, conf MacUnifiedLoggingConfig) (*MacUnifiedLoggingAdapter, chan struct{}, error) {
	a := &MacUnifiedLoggingAdapter{
		conf: conf,
		ctx:  ctx,
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

	a.logs = NewLogs()
	if err := a.logs.StartGathering(a.conf.Predicate, a.conf.ClientOptions.OnWarning); err != nil {
		a.uspClient.Close()
		return nil, nil, err
	}

	// Make sure the `log stream` subprocess does not outlive us if the
	// process is signaled: stop gathering (which kills the subprocess)
	// before exiting.
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChannel
		a.logs.StopGathering()
		os.Exit(0)
	}()

	a.chStopped = make(chan struct{})

	a.wgSenders.Add(1)
	go a.handleEvents()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *MacUnifiedLoggingAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")

	// Stops the subprocess and supervisor; this closes logs.Channel,
	// which winds down handleEvents.
	a.logs.StopGathering()
	a.wgSenders.Wait()

	_, err := a.uspClient.Close()
	if err != nil {
		return err
	}
	return nil
}

func (a *MacUnifiedLoggingAdapter) convertStructToMap(obj interface{}) map[string]interface{} {
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

func (a *MacUnifiedLoggingAdapter) handleEvents() {
	defer a.wgSenders.Done()

	// The channel is closed by StopGathering once the gatherer has fully
	// wound down, so this loop is guaranteed to exit on shutdown.
	for log := range a.logs.Channel {
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
}
