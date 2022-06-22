package usp_duo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"

	"github.com/duosecurity/duo_api_golang"
	duoadmin "github.com/duosecurity/duo_api_golang/admin"
)

const (
	inTenYears = 10 * 365 * 24 * time.Hour
)

type DuoAdapter struct {
	conf        DuoConfig
	uspClient   *uspclient.Client
	duoClient   *duoapi.DuoApi
	adminClient *duoadmin.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type DuoConfig struct {
	ClientOptions  uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	IntegrationKey string                  `json:"integration_key" yaml:"integration_key"`
	SecretKey      string                  `json:"secret_key" yaml:"secret_key"`
	APIHostname    string                  `json:"api_hostname" yaml:"api_hostname"`
}

func (c *DuoConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.IntegrationKey == "" {
		return errors.New("missing integration_key")
	}
	if c.SecretKey == "" {
		return errors.New("missing secret_key")
	}
	if c.APIHostname == "" {
		return errors.New("missing api_hostname")
	}
	return nil
}

func NewDuoAdapter(conf DuoConfig) (*DuoAdapter, chan struct{}, error) {
	var err error
	a := &DuoAdapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
	}

	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	a.duoClient = duoapi.NewDuoApi(a.conf.IntegrationKey, a.conf.SecretKey, a.conf.APIHostname, "limacharlie", duoapi.SetTimeout(15*time.Second))
	a.adminClient = duoadmin.New(*a.duoClient)

	a.chStopped = make(chan struct{})

	a.wgSenders.Add(1)
	go func() {
		defer a.wgSenders.Done()
		a.fetchAuthLogs()
	}()
	a.wgSenders.Add(1)
	go func() {
		defer a.wgSenders.Done()
		a.fetchAdminLogs()
	}()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *DuoAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *DuoAdapter) fetchAuthLogs() {
	minTime := time.Now()
	window := inTenYears
	results, err := a.adminClient.GetAuthLogs(minTime, window)
	fmt.Printf("1: %#v %v\n", results, err)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("GetAuthLogs: %v", err))
		return
	}
	next := results.Response.Metadata.GetNextOffset()
	for !a.doStop.WaitFor(1 * time.Minute) {
		if next == nil {
			results, err = a.adminClient.GetAuthLogs(minTime, window)
		} else {
			results, err = a.adminClient.GetAuthLogs(minTime, window, next)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("GetAuthLogs: %v", err))
			continue
		}
		next = results.Response.Metadata.GetNextOffset()
		for _, al := range results.Response.Logs {
			msg := &protocol.DataMessage{
				JsonPayload: al,
				TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
			}
			if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
				if err == uspclient.ErrorBufferFull {
					a.conf.ClientOptions.OnWarning("stream falling behind")
					err = a.uspClient.Ship(msg, 0)
				}
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
					a.doStop.Set()
					return
				}
			}
			if next == nil {
				// Then we must use the time in the last response.
				// This logic is based on https://github.com/duosecurity/duo_log_sync/blob/master/duologsync/producer/producer.py#L159
				// Ideally we use iso timestamp that could have ms.
				if its, ok := utils.Dict(al).GetString("isotimestamp"); ok {
					nt, err := time.Parse(time.RFC3339, its)
					if err == nil {
						minTime = nt.Add(1 * time.Millisecond)
					}
				} else if ts, ok := utils.Dict(al).GetInt("timestamp"); ok {
					// Otherwise we use the timestamp in seconds.
					minTime = time.Unix(int64(ts), 0).Add(1 * time.Second)
				} else {
					a.conf.ClientOptions.DebugLog(fmt.Sprintf("failed to get next time: %#v", al))
				}
			}
		}
	}
}

func (a *DuoAdapter) fetchAdminLogs() {
	minTime := time.Now()
	results, err := a.adminClient.GetAdminLogs(minTime)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("GetAdminLogs: %v", err))
		return
	}
	next := results.Logs.GetNextOffset(time.Now().Add(inTenYears))
	for !a.doStop.WaitFor(1 * time.Minute) {
		if next == nil {
			results, err = a.adminClient.GetAdminLogs(minTime)
		} else {
			results, err = a.adminClient.GetAdminLogs(minTime, next)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("GetAdminLogs: %v", err))
			continue
		}
		next = results.Logs.GetNextOffset(time.Now().Add(inTenYears))
		for _, al := range results.Logs {
			msg := &protocol.DataMessage{
				JsonPayload: al,
				TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
			}
			if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
				if err == uspclient.ErrorBufferFull {
					a.conf.ClientOptions.OnWarning("stream falling behind")
					err = a.uspClient.Ship(msg, 0)
				}
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
					a.doStop.Set()
					return
				}
			}
			if next == nil {
				// Then we must use the time in the last response.
				// This logic is based on https://github.com/duosecurity/duo_log_sync/blob/master/duologsync/producer/producer.py#L159
				// Ideally we use iso timestamp that could have ms.
				if its, ok := utils.Dict(al).GetString("isotimestamp"); ok {
					nt, err := time.Parse(time.RFC3339, its)
					if err == nil {
						minTime = nt.Add(1 * time.Second)
					}
				} else if ts, ok := utils.Dict(al).GetInt("timestamp"); ok {
					// Otherwise we use the timestamp in seconds.
					minTime = time.Unix(int64(ts), 0).Add(1 * time.Second)
				} else {
					a.conf.ClientOptions.DebugLog(fmt.Sprintf("failed to get next time: %#v", al))
				}
			}
		}
	}
}
