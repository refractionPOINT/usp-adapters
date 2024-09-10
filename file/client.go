//go:build windows || darwin || linux || solaris || netbsd || openbsd || freebsd
// +build windows darwin linux solaris netbsd openbsd freebsd

package usp_file

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"

	"github.com/nxadm/tail"
)

const (
	defaultWriteTimeout    = 60 * 10
	defaultPollingInterval = 10 * time.Second
)

type FileAdapter struct {
	conf         FileConfig
	wg           sync.WaitGroup
	uspClient    *uspclient.Client
	writeTimeout time.Duration
	tailFiles    map[string]*tail.Tail
	mu           sync.Mutex
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
		conf:      conf,
		tailFiles: make(map[string]*tail.Tail),
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
		a.pollFiles()
	}()

	return a, chStopped, nil
}

func (a *FileAdapter) pollFiles() {
	for {
		matches, err := filepath.Glob(a.conf.FilePath)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("glob error: %v", err))
			return
		}

		a.mu.Lock()
		for _, match := range matches {
			if _, ok := a.tailFiles[match]; !ok {
				t, err := tail.TailFile(match, tail.Config{
					ReOpen:        !a.conf.NoFollow,
					MustExist:     true,
					Follow:        !a.conf.NoFollow,
					CompleteLines: true,
				})
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("tail error: %v", err))
					continue
				}
				a.tailFiles[match] = t
				a.wg.Add(1)
				go func(t *tail.Tail) {
					defer a.wg.Done()
					a.handleInput(t)
				}(t)
			}
		}
		a.mu.Unlock()

		time.Sleep(defaultPollingInterval)
	}
}

func (a *FileAdapter) handleInput(t *tail.Tail) {
	for line := range t.Lines {
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

func (a *FileAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.mu.Lock()
	for _, t := range a.tailFiles {
		t.Stop()
	}
	a.mu.Unlock()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}
