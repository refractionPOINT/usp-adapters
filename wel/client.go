// +build windows

package usp_wel

import (
	"fmt"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

const (
	defaultWriteTimeout = 60 * 10
)

type WELAdapter struct {
	conf         WELConfig
	wg           sync.WaitGroup
	isRunning    uint32
	mRunning     sync.RWMutex
	uspClient    *uspclient.Client
	writeTimeout time.Duration

	hSub EVT_HANDLE
}

func NewWELAdapter(conf WELConfig) (*WELAdapter, chan struct{}, error) {
	a := &WELAdapter{
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

	a.hSub, err = EvtSubscribe(NULL, NULL, a.conf.ChannelPath, a.conf.Query, NULL, NULL, a.handleEvent, EvtSubscribeToFutureEvents)
	if a.hSub == NULL || err != nil {
		err = fmt.Errorf("failed creating event subscription: %v", err)
		a.conf.ClientOptions.OnError(err)
		return nil, nil, err
	}
	a.conf.ClientOptions.DebugLog("EvtSubscribe successful")

	chStopped := make(chan struct{})
	a.wg.Add(1)
	go func() {
		a.wg.Wait()
		defer close(chStopped)
	}()

	return a, chStopped, nil
}

func (a *WELAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")

	EvtClose(a.hSub)
	a.conf.ClientOptions.DebugLog("EvtClose")

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

func (a *WELAdapter) handleEvent(Action EVT_SUBSCRIBE_NOTIFY_ACTION, UserContext PVOID, Event EVT_HANDLE) uintptr {
	// The lock pattern is a bit more complex than usual
	// because we're dealing with an async API we don't control.
	a.mRunning.RLock()
	if a.isRunning == 0 {
		return 0
	}
	a.wg.Add(1)
	a.mRunning.RUnlock()
	defer a.wg.Done()

	if Action == EvtSubscribeActionError {
		return 0
	}
	if Action != EvtSubscribeActionDeliver {
		return 0
	}

	renderedEvent, err := EvtRenderXML(Event)
	if err != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf("failed to render event to xml: %v", err))
		return 0
	}

	msg := &protocol.DataMessage{
		TextPayload: string(renderedEvent),
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	err = a.uspClient.Ship(msg, a.writeTimeout)
	if err == uspclient.ErrorBufferFull {
		a.conf.ClientOptions.OnWarning("stream falling behind")
		err = a.uspClient.Ship(msg, 0)
	}
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
	}
	return 0
}
