// adapted from https://github.com/MaxSchaefer/macos-log-stream/blob/main/pkg/mls/logs.go

package usp_mac_unified_logging

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

type Log struct {
	TraceID            int64       `json:"traceID"`
	EventMessage       string      `json:"eventMessage"`
	EventType          string      `json:"eventType"`
	Source             interface{} `json:"source"`
	FormatString       string      `json:"formatString"`
	ActivityIdentifier int         `json:"activityIdentifier"`
	Subsystem          string      `json:"subsystem"`
	Category           string      `json:"category"`
	ThreadID           int         `json:"threadID"`
	SenderImageUUID    string      `json:"senderImageUUID"`
	Backtrace          struct {
		Frames []struct {
			ImageOffset int    `json:"imageOffset"`
			ImageUUID   string `json:"imageUUID"`
		} `json:"frames"`
	} `json:"backtrace"`
	BootUUID                 string `json:"bootUUID"`
	ProcessImagePath         string `json:"processImagePath"`
	Timestamp                string `json:"timestamp"`
	SenderImagePath          string `json:"senderImagePath"`
	MachTimestamp            int64  `json:"machTimestamp"`
	MessageType              string `json:"messageType"`
	ProcessImageUUID         string `json:"processImageUUID"`
	ProcessID                int    `json:"processID"`
	SenderProgramCounter     int    `json:"senderProgramCounter"`
	ParentActivityIdentifier int    `json:"parentActivityIdentifier"`
	TimezoneName             string `json:"timezoneName"`
}

const (
	// logChannelBuffer gives the consumer (Ship) slack before we start
	// shedding. The buffer is the decoupling point between the local
	// `log stream` subprocess and the network: if the consumer stalls
	// longer than this absorbs, we DROP (and count) rather than stop
	// draining stdout. Blocking the pipe instead would stall logd's
	// writer for as long as the consumer is stuck, which both loses the
	// data anyway and can leave the long-lived `log stream` process in a
	// degraded state that persists until it is restarted.
	logChannelBuffer = 4096
	// dropReportInterval is how often a non-zero shed count is reported.
	dropReportInterval = 60 * time.Second
)

// restartBackoff is the delay between `log stream` subprocess restarts
// (subprocess exit, persistent decode failure, ...). Long enough to avoid a
// tight respawn loop if `log` is unrunnable, short enough to not lose
// meaningful coverage. A var (not const) so tests can shorten it.
var restartBackoff = 5 * time.Second

// streamCommand builds the `log stream` subprocess. It is a package var so
// tests can substitute a fake stream without a real macOS `log` binary
// (log.go is not build-constrained, only client.go/noop.go are).
var streamCommand = func(predicate string) *exec.Cmd {
	args := []string{"stream", "--color=none", "--style=ndjson"}
	if predicate != "" {
		args = append(args, "--predicate", predicate)
	}
	return exec.Command("log", args...)
}

type Logs struct {
	// Channel delivers parsed log entries. It is closed when gathering
	// stops for good (StopGathering), so consumers can range over it.
	Channel chan Log

	predicate string
	onWarning func(string)

	chStop   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	dropped atomic.Uint64
}

func NewLogs() *Logs {
	return &Logs{
		Channel: make(chan Log, logChannelBuffer),
		chStop:  make(chan struct{}),
	}
}

func (logs *Logs) warn(format string, args ...interface{}) {
	if logs.onWarning != nil {
		logs.onWarning(fmt.Sprintf(format, args...))
	}
}

// StartGathering starts and supervises a `log stream` subprocess,
// restarting it (with backoff) if it exits or its output desyncs, until
// StopGathering is called. The returned error only reflects supervisor
// startup; subprocess failures are reported through onWarning and retried.
func (logs *Logs) StartGathering(predicate string, onWarning func(string)) error {
	logs.predicate = predicate
	logs.onWarning = onWarning

	// Periodically report shed counts. Dropping is preferable to stalling
	// the subprocess pipe, but it must never be silent.
	logs.wg.Add(1)
	go func() {
		defer logs.wg.Done()
		t := time.NewTicker(dropReportInterval)
		defer t.Stop()
		for {
			select {
			case <-logs.chStop:
				if n := logs.dropped.Swap(0); n != 0 {
					logs.warn("shed %d events while output was stalled", n)
				}
				return
			case <-t.C:
				if n := logs.dropped.Swap(0); n != 0 {
					logs.warn("shed %d events while output was stalled", n)
				}
			}
		}
	}()

	logs.wg.Add(1)
	go func() {
		defer logs.wg.Done()
		defer close(logs.Channel)
		for {
			logs.runOnce()
			select {
			case <-logs.chStop:
				return
			case <-time.After(restartBackoff):
			}
			logs.warn("restarting `log stream`")
		}
	}()

	return nil
}

// runOnce runs a single `log stream` subprocess until it exits, its output
// desyncs, or StopGathering is called.
func (logs *Logs) runOnce() {
	cmd := streamCommand(logs.predicate)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logs.warn("log stream stdout pipe: %v", err)
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		logs.warn("log stream stderr pipe: %v", err)
		return
	}
	if err := cmd.Start(); err != nil {
		logs.warn("log stream start: %v", err)
		return
	}
	// `log stream` writes informational notices to stderr (including
	// drop/throttle warnings under pressure). Surface them; they are not
	// fatal and must never tear down gathering.
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		s := bufio.NewScanner(stderr)
		for s.Scan() {
			if line := s.Text(); line != "" {
				logs.warn("log stream stderr: %s", line)
			}
		}
	}()

	// Always reap the subprocess: kill on every exit path, wait for the
	// stderr reader to drain to EOF (Wait must not race pipe reads), then
	// Wait so the subprocess cannot linger as a zombie.
	defer func() {
		cmd.Process.Kill()
		<-stderrDone
		cmd.Wait()
	}()

	// A stop request must unblock the decoder even if it is parked reading
	// a quiet stream: killing the subprocess closes stdout, which fails the
	// pending Decode.
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		select {
		case <-logs.chStop:
			cmd.Process.Kill()
		case <-stopWatch:
		}
	}()

	r := bufio.NewReader(stdout)
	// Drop the first line: `log stream` prints a non-JSON header.
	r.ReadLine()

	dec := json.NewDecoder(r)
	for {
		select {
		case <-logs.chStop:
			return
		default:
		}

		entry := Log{}
		if err := dec.Decode(&entry); err != nil {
			// On a stop request the subprocess was killed on purpose, so
			// the decode error is expected — exit quietly.
			select {
			case <-logs.chStop:
				return
			default:
			}
			// A json.Decoder does not resync after garbage and EOF means
			// the subprocess is gone; either way this stream is done.
			// Return and let the supervisor restart the subprocess.
			logs.warn("log stream decode failed, restarting subprocess: %v", err)
			return
		}

		select {
		case logs.Channel <- entry:
		case <-logs.chStop:
			return
		default:
			// Consumer stalled and the buffer is full: shed instead of
			// blocking. Blocking here backs the stall up through the
			// stdout pipe into logd (see logChannelBuffer).
			logs.dropped.Add(1)
		}
	}
}

// StopGathering stops the supervisor and subprocess, closes Channel once
// everything has wound down, and is safe to call more than once.
func (logs *Logs) StopGathering() {
	logs.stopOnce.Do(func() {
		close(logs.chStop)
	})
	logs.wg.Wait()
}
