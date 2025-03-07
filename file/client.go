//go:build windows || darwin || linux || solaris || netbsd || openbsd || freebsd
// +build windows darwin linux solaris netbsd openbsd freebsd

package usp_file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"

	"github.com/nxadm/tail"

	"golang.org/x/sync/semaphore"
)

const (
	defaultWriteTimeout    = 60 * 10
	defaultPollingInterval = 10 * time.Second
)

var (
	inactivityThreshold   = 24 * time.Hour
	reactivationThreshold = 60 * time.Second
)

// contain a tail with a fields that help to now if a file is actively modified/in use
type tailInfo struct {
	tail       *tail.Tail
	lastActive time.Time
	isInactive bool
	lastOffset int64
	lastData   int64
}

type FileAdapter struct {
	ctx          context.Context
	conf         FileConfig
	wg           sync.WaitGroup
	uspClient    *uspclient.Client
	writeTimeout time.Duration
	tailFiles    map[string]*tailInfo
	mu           sync.Mutex
	serialFeed   *semaphore.Weighted
	lineCb       func(line string) // callback for each line for testing
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
		conf:       conf,
		tailFiles:  make(map[string]*tailInfo),
		serialFeed: semaphore.NewWeighted(1),
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
	ctx, cancel := context.WithCancel(context.Background())
	a.ctx = ctx
	defer cancel()

	if a.conf.InactivityThreshold != 0 {
		inactivityThreshold = time.Duration(a.conf.InactivityThreshold) * time.Second
	}
	if a.conf.ReactivationThreshold != 0 {
		reactivationThreshold = time.Duration(a.conf.ReactivationThreshold) * time.Second
	}

	isFirstRun := true

	for {
		matches, err := filepath.Glob(a.conf.FilePath)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("glob error: %v", err))
			return
		}
		sort.Strings(matches)

		a.mu.Lock()
		now := time.Now()

		// check all files against what we have in the tailfiles map
		for path, info := range a.tailFiles {
			if stat, err := os.Stat(path); err == nil {
				modTime := stat.ModTime()
				lastData := atomic.LoadInt64(&info.lastData)
				if info.isInactive {
					// validate if an inactive file has been modified recently and we need to tail it
					if now.Sub(modTime) <= reactivationThreshold {
						a.conf.ClientOptions.OnError(fmt.Errorf("file reactivated: %s", path))
						t, err := tail.TailFile(path, tail.Config{
							ReOpen:        !a.conf.NoFollow,
							MustExist:     true,
							Follow:        !a.conf.NoFollow,
							CompleteLines: true,
							Poll:          a.conf.Poll,
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
							a.handleInput(t, &info.lastData)
						}(t)
					}
				} else {
					// validate if an active file has become inactive and we need to stop tailing it
					// We check for the last modified time AND we also check for the last time we saw
					// data flow from the file. We do this because Microsoft Windows sometimes decides to
					// stop updating the last modified time for things like IIS.
					if now.Sub(modTime) > inactivityThreshold && now.Sub(time.Unix(lastData, 0)) > inactivityThreshold {
						a.conf.ClientOptions.OnError(fmt.Errorf("file inactive: %s", path))
						info.lastOffset, _ = info.tail.Tell() // store current position before stopping the tail in case we need to resume
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

				a.conf.ClientOptions.DebugLog(fmt.Sprintf("opening file: %s", match))

				// in general, tail existing files, but if a file appears after we started
				// (or we are backfilling) then start from the beginning of the file so as not to miss any data
				location := &tail.SeekInfo{Offset: 0, Whence: io.SeekEnd}
				if a.conf.Backfill || !isFirstRun {
					location = &tail.SeekInfo{Offset: 0, Whence: io.SeekStart}
				}

				t, err := tail.TailFile(match, tail.Config{
					ReOpen:        !a.conf.NoFollow,
					MustExist:     true,
					Follow:        !a.conf.NoFollow,
					CompleteLines: true,
					Poll:          a.conf.Poll,
					Location:      location,
				})
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("tail error: %v", err))
					continue
				}
				info := &tailInfo{
					tail:       t,
					lastActive: time.Now(),
					isInactive: false,
				}
				a.tailFiles[match] = info
				a.wg.Add(1)
				go func(t *tail.Tail) {
					defer a.wg.Done()
					a.handleInput(t, &info.lastData)
				}(t)
			}
		}
		a.mu.Unlock()

		isFirstRun = false
		time.Sleep(defaultPollingInterval)
	}
}

func (a *FileAdapter) handleInput(t *tail.Tail, pLastData *int64) {
	if a.conf.SerializeFiles {
		// If we are serializing files, we need to acquire a semaphore to ensure we only tail one file at a time.
		if err := a.serialFeed.Acquire(a.ctx, 1); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error acquiring semaphore: %v", err))
			return
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting file %s in serial mode", t.Filename))
		defer a.serialFeed.Release(1)
	}
	if !a.conf.MultiLineJSON {
		for line := range t.Lines {
			if line.Err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("tail.Line(): %v", line.Err))
				break
			}
			atomic.StoreInt64(pLastData, time.Now().Unix())
			a.handleLine(line.Text)
		}
	} else {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting file %s in multi-line JSON mode", t.Filename))
		var jsonLines []string
		braceCount := 0

		for line := range t.Lines {
			line := strings.TrimSpace(line.Text)
			if line == "" { // Skip empty lines.
				continue
			}
			jsonLines = append(jsonLines, line)
			braceCount += strings.Count(line, "{")
			braceCount -= strings.Count(line, "}")

			if braceCount == 0 && len(jsonLines) > 0 {
				rawJSON := []byte(strings.Join(jsonLines, ""))
				a.handleLine(string(rawJSON))
				jsonLines = nil // Reset for the next object.
			}
		}
	}
	if a.conf.SerializeFiles {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("finished file %s in serial mode", t.Filename))
	}
}

func (a *FileAdapter) handleLine(line string) {
	if len(line) == 0 {
		return
	}
	if a.lineCb != nil {
		a.lineCb(line)
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
