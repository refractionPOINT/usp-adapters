package usp_k8s

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
)

const (
	defaultWriteTimeout = 60 * 10
)

type K8sAdapter struct {
	conf         K8sConfig
	wg           sync.WaitGroup
	uspClient    *uspclient.Client
	writeTimeout time.Duration
}

type K8sConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	Root            string                  `json:"root" yaml:"root"`
}

func (c *K8sConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Root == "" {
		return errors.New("file_path missing")
	}
	return nil
}

func NewK8sAdapter(conf K8sConfig) (*K8sAdapter, chan struct{}, error) {
	a := &K8sAdapter{
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

	chStopped := make(chan struct{})

	return a, chStopped, nil
}

func (a *K8sAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}
