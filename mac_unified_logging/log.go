// adapted from https://github.com/MaxSchaefer/macos-log-stream/blob/main/pkg/mls/logs.go

package usp_mac_unified_logging

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os/exec"
	"sync"
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

type Logs struct {
	m       sync.Mutex
	Channel chan Log
	exit    chan bool
}

func NewLogs() *Logs {
	return &Logs{
		Channel: make(chan Log),
		exit:    make(chan bool),
	}
}

func (logs *Logs) StartGathering(predicate string) error {

	cmd := exec.Command("log", "stream", "--color=none", "--style=ndjson")
	if predicate != "" {
		cmd = exec.Command("log", "stream", "--color=none", "--style=ndjson", "--predicate", predicate)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	go func() {
		logs.m.Lock()
		defer logs.m.Unlock()

		cmd.Start()
		defer cmd.Process.Kill()

		// drop first message
		bufio.NewReader(stdout).ReadLine()

		dec := json.NewDecoder(stdout)

		for {
			select {
			case <-logs.exit:
				return
			default:
				log := Log{}

				if err := dec.Decode(&log); err != nil {
					fmt.Println("error decoding json")
					fmt.Println(err.Error())
				} else {
					logs.Channel <- log
				}
			}
		}
	}()

	go func() {
		stderrBuf := bufio.NewReader(stderr)
		for {
			line, _, _ := stderrBuf.ReadLine()
			if len(line) > 0 {
				logs.StopGathering()
				panic(err)
			}
		}
	}()

	return nil
}

func (logs *Logs) StopGathering() {
	logs.exit <- true
}
