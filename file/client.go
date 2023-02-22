//go:build windows || darwin || linux || solaris
// +build windows darwin linux solaris

package usp_file

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"

	"github.com/nxadm/tail"
)

const (
	defaultWriteTimeout = 60 * 10
)

type FileAdapter struct {
	conf         FileConfig
	wg           sync.WaitGroup
	uspClient    *uspclient.Client
	writeTimeout time.Duration
	tailFile     *tail.Tail
}

func (c *FileConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.FilePath == "" {
		return errors.New("file_path missing")
	}
	return nil
}

func NewFileAdapter(conf FileConfig) (*FileAdapter, chan struct{}, error) {
	a := &FileAdapter{
		conf: conf,
	}

	if a.conf.WriteTimeoutSec == 0 {
		a.conf.WriteTimeoutSec = defaultWriteTimeout
	}
	a.writeTimeout = time.Duration(a.conf.WriteTimeoutSec) * time.Second

	var err error
	a.tailFile, err = tail.TailFile(a.conf.FilePath, tail.Config{
		ReOpen:    !a.conf.NoFollow,
		MustExist: true,
		Follow:    !a.conf.NoFollow,
	})
	if err != nil {
		return nil, nil, err
	}

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
		if a.conf.NoFollow {
			a.conf.ClientOptions.DebugLog("finished tailing, waiting to drain")
			time.Sleep(2 * time.Second)
			a.uspClient.Drain(10 * time.Minute)
		}
	}()
	if a.conf.NoFollow {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			time.Sleep(2 * time.Second)
			a.tailFile.StopAtEOF()
		}()
	}

	return a, chStopped, nil
}

func (a *FileAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.tailFile.Stop()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *FileAdapter) handleInput() {
	for line := range a.tailFile.Lines {
		if line.Err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("tail.Line(): %v", line.Err))
			break
		}
		a.handleLine(line.Text)
	}
}

func (a *FileAdapter) handleLine(line string) {
	if len(line) == 0 {
		return
	}
	msg := &protocol.DataMessage{
		TextPayload: line,
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
