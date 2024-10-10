//go:build windows || darwin || linux || solaris || netbsd || openbsd || freebsd
// +build windows darwin linux solaris netbsd openbsd freebsd

package usp_file

import (
	"errors"
	"fmt"
	"io"
	"os"
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

var (
	inactivityThreshold   = 30 * time.Second
	reactivationThreshold = 60 * time.Second
)

// contain a tail with a fields that help to now if a file is actively modified/in use
type tailInfo struct {
	tail       *tail.Tail
	lastActive time.Time
	isInactive bool
	lastOffset int64
}

type FileAdapter struct {
	conf         FileConfig
	wg           sync.WaitGroup
	uspClient    *uspclient.Client
	writeTimeout time.Duration
	tailFiles    map[string]*tailInfo
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
		tailFiles: make(map[string]*tailInfo),
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
	if a.conf.InactivityThreshold != 0 {
		inactivityThreshold = time.Duration(a.conf.InactivityThreshold) * time.Second
	}
	if a.conf.ReactivationThreshold != 0 {
		reactivationThreshold = time.Duration(a.conf.ReactivationThreshold) * time.Second
	}

	for {
		matches, err := filepath.Glob(a.conf.FilePath)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("glob error: %v", err))
			return
		}

		a.mu.Lock()
		now := time.Now()

		// check all files against what we have in the tailfiles map
		for path, info := range a.tailFiles {
			if stat, err := os.Stat(path); err == nil {
				modTime := stat.ModTime()
				if info.isInactive {
					// validate if an inactive file has been modified recently and we need to tail it
					if now.Sub(modTime) <= reactivationThreshold {
						a.conf.ClientOptions.OnError(fmt.Errorf("file reactivated: %s", path))
						t, err := tail.TailFile(path, tail.Config{
							ReOpen:        !a.conf.NoFollow,
							MustExist:     true,
							Follow:        !a.conf.NoFollow,
							CompleteLines: true,
							Location:      &tail.SeekInfo{Offset: info.lastOffset, Whence: io.SeekStart}, // start to ingest from last known position
						})
						if err != nil {
							a.conf.ClientOptions.OnError(fmt.Errorf("tail error on reactivation: %v", err))
							continue
						}
						info.tail = t
						info.isInactive = false
						info.lastActive = modTime
						a.wg.Add(1)

						go func(t *tail.Tail) {
							defer a.wg.Done()
							a.handleInput(t)
						}(t)
					}
				} else {
					// validate if an active file has become inactive and we need to stop tailing it
					if now.Sub(modTime) > inactivityThreshold {
						a.conf.ClientOptions.OnError(fmt.Errorf("file inactive: %s", path))
						info.lastOffset, err = info.tail.Tell() // store current position before stopping the tail in case we need to resume
						err := info.tail.Stop()
						if err != nil {
							a.conf.ClientOptions.OnError(fmt.Errorf("error stopping tail: %v", err))
						}
						info.isInactive = true
					} else if modTime.After(info.lastActive) {
						info.lastActive = modTime
					}
				}
			} else {
				// fle no longer exists on disk, close and remove all the tail resources
				a.conf.ClientOptions.OnError(fmt.Errorf("file removed: %s", path))
				err := info.tail.Stop()
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("error stopping tail: %v", err))
				}
				info.tail.Cleanup()
				delete(a.tailFiles, path)
			}
		}

		for _, match := range matches {
			if _, ok := a.tailFiles[match]; !ok {
				stat, err := os.Stat(match)
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("error getting file stats: %v", err))
					continue
				}

				if now.Sub(stat.ModTime()) > inactivityThreshold {
					a.conf.ClientOptions.OnWarning(fmt.Sprintf("file too old to open: %s", match))
					continue
				}

				t, err := tail.TailFile(match, tail.Config{
					ReOpen:        !a.conf.NoFollow,
					MustExist:     true,
					Follow:        !a.conf.NoFollow,
					CompleteLines: true,
					Location:      &tail.SeekInfo{Offset: 0, Whence: io.SeekEnd}, // start to ingest at the end for new files
				})
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("tail error: %v", err))
					continue
				}
				a.tailFiles[match] = &tailInfo{
					tail:       t,
					lastActive: time.Now(),
					isInactive: false,
				}
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
	for _, info := range a.tailFiles {
		info.tail.Stop()
	}
	a.mu.Unlock()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}
