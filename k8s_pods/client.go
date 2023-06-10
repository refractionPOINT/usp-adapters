//go:build windows || darwin || linux || solaris || netbsd || openbsd || freebsd
// +build windows darwin linux solaris netbsd openbsd freebsd

package usp_k8s_pods

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultWriteTimeout = 60 * 10
)

type K8sPodsAdapter struct {
	conf         K8sPodsConfig
	uspClient    *uspclient.Client
	writeTimeout time.Duration
	wg           sync.WaitGroup

	engine *K8sLogProcessor
}

type K8sPodsConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	Root            string                  `json:"root" yaml:"root"`
}

func (c *K8sPodsConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Root == "" {
		return errors.New("file_path missing")
	}
	return nil
}

func NewK8sPodsAdapter(conf K8sPodsConfig) (*K8sPodsAdapter, chan struct{}, error) {
	a := &K8sPodsAdapter{
		conf: conf,
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

	a.engine, err = NewK8sLogProcessor(a.conf.Root, a.conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	a.wg.Add(1)
	go a.consumeLogs()

	chStopped := make(chan struct{})

	go func() {
		a.wg.Wait()
		close(chStopped)
	}()

	return a, chStopped, nil
}

func (a *K8sPodsAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.engine.Close()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *K8sPodsAdapter) consumeLogs() {
	defer a.wg.Done()

	for line := range a.engine.Lines() {
		msg := &protocol.DataMessage{
			JsonPayload: utils.Dict{
				"metadata": line.Entity,
				"message":  line.Line,
			},
		}
		if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
			if err == uspclient.ErrorBufferFull {
				a.conf.ClientOptions.OnWarning("stream falling behind")
				err = a.uspClient.Ship(msg, 1*time.Hour)
			}
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				return
			}
		}
	}
}
