// adapted from https://github.com/MaxSchaefer/macos-log-stream/blob/main/pkg/mls/logs.go

package usp_mac_unified_logging

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os/exec"
	"sync"
)

// log stream events can carry large payloads (eventMessage, backtraces),
// well past bufio.Scanner's 64KB default line limit.
const maxLogLineSize = 4 * 1024 * 1024

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

type Logs struct {
	m        sync.Mutex
	Channel  chan Log
	exit     chan struct{}
	exitOnce sync.Once
}

func NewLogs() *Logs {
	return &Logs{
		Channel: make(chan Log),
		exit:    make(chan struct{}),
	}
}

func (logs *Logs) StartGathering(predicate string) error {

	args := []string{"stream", "--color=none", "--style=ndjson"}
	if predicate != "" {
		args = append(args, "--predicate", predicate)
	}
	cmd := exec.Command("log", args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	// Kill the subprocess on StopGathering so a reader blocked in
	// Scan() unblocks and the gathering goroutine can exit.
	go func() {
		<-logs.exit
		cmd.Process.Kill()
	}()

	go func() {
		logs.m.Lock()
		defer logs.m.Unlock()

		defer cmd.Process.Kill()
		// Closing the channel lets consumers terminate instead of
		// waiting forever on a stream that has ended.
		defer close(logs.Channel)

		// Parse the ndjson output line by line. A json.Decoder is
		// unsuitable here: it never advances past a SyntaxError, so a
		// single bad line (like the non-JSON "Filtering the log data"
		// header) would wedge it in a permanent error loop while the
		// pipe fills up and `log stream` blocks forever.
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, 64*1024), maxLogLineSize)

		for scanner.Scan() {
			select {
			case <-logs.exit:
				return
			default:
			}

			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			log := Log{}
			if err := json.Unmarshal(line, &log); err != nil {
				// Skip non-JSON lines (e.g. the header line) without
				// stalling the stream.
				fmt.Printf("skipping non-json line from log stream: %v\n", err)
				continue
			}

			select {
			case logs.Channel <- log:
			case <-logs.exit:
				return
			}
		}

		if err := scanner.Err(); err != nil {
			fmt.Printf("error reading log stream output: %v\n", err)
		}
	}()

	go func() {
		stderrBuf := bufio.NewReader(stderr)
		for {
			line, _, err := stderrBuf.ReadLine()
			if len(line) > 0 {
				logs.StopGathering()
				panic(fmt.Errorf("log stream error: %s", string(line)))
			}
			if err != nil {
				return
			}
		}
	}()

	return nil
}

func (logs *Logs) StopGathering() {
	logs.exitOnce.Do(func() {
		close(logs.exit)
	})
}
