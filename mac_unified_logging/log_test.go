package usp_mac_unified_logging

import (
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeStream installs a streamCommand that runs `sh -c script` and restores
// the original on cleanup. The script stands in for macOS `log stream`.
func fakeStream(t *testing.T, script string) {
	t.Helper()
	orig := streamCommand
	streamCommand = func(predicate string) *exec.Cmd {
		return exec.Command("sh", "-c", script)
	}
	t.Cleanup(func() { streamCommand = orig })
}

func collectWarnings() (func(string), *[]string, *sync.Mutex) {
	var mu sync.Mutex
	var got []string
	return func(s string) {
		mu.Lock()
		got = append(got, s)
		mu.Unlock()
	}, &got, &mu
}

func TestLogs_DropsHeaderAndDelivers(t *testing.T) {
	// First line is the `log stream` header and must be discarded; the
	// following ndjson objects must be delivered in order.
	fakeStream(t, `printf 'Filtering the log data\n{"processID":11}\n{"processID":22}\n'; exec cat`)

	logs := NewLogs()
	if err := logs.StartGathering("", nil); err != nil {
		t.Fatalf("StartGathering: %v", err)
	}
	defer logs.StopGathering()

	for _, want := range []int{11, 22} {
		select {
		case got := <-logs.Channel:
			if got.ProcessID != want {
				t.Fatalf("ProcessID = %d, want %d", got.ProcessID, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for entry processID=%d", want)
		}
	}
}

func TestLogs_RestartsOnSubprocessExit(t *testing.T) {
	restoreBackoff := restartBackoff
	restartBackoff = 10 * time.Millisecond
	t.Cleanup(func() { restartBackoff = restoreBackoff })

	// Each subprocess invocation emits one event then exits, forcing the
	// supervisor to restart it repeatedly. We must receive several events
	// across multiple subprocess lifetimes.
	fakeStream(t, `printf 'hdr\n{"processID":7}\n'`)

	logs := NewLogs()
	if err := logs.StartGathering("", nil); err != nil {
		t.Fatalf("StartGathering: %v", err)
	}
	defer logs.StopGathering()

	for i := 0; i < 3; i++ {
		select {
		case got := <-logs.Channel:
			if got.ProcessID != 7 {
				t.Fatalf("ProcessID = %d, want 7", got.ProcessID)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("did not get restart-produced event #%d", i+1)
		}
	}
}

func TestLogs_ShedsWhenConsumerStalls(t *testing.T) {
	// Emit many events while nobody reads. With a tiny buffer the producer
	// must shed (and count) rather than block the subprocess pipe.
	var b strings.Builder
	b.WriteString("hdr\n")
	const emitted = 200
	for i := 0; i < emitted; i++ {
		b.WriteString(fmt.Sprintf(`{"processID":%d}`, i))
		b.WriteByte('\n')
	}
	// printf the payload, then idle so the subprocess stays alive.
	fakeStream(t, `printf `+shellQuote(b.String())+`; exec cat`)

	warn, warnings, wmu := collectWarnings()
	logs := NewLogs()
	logs.Channel = make(chan Log, 2) // tiny buffer to force shedding
	if err := logs.StartGathering("", warn); err != nil {
		t.Fatalf("StartGathering: %v", err)
	}

	// Give the producer time to fill the buffer and shed the rest.
	deadline := time.After(5 * time.Second)
	for {
		if logs.dropped.Load() > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected some events to be shed, dropped=0")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Shedding must never lose more than what was emitted, and the buffered
	// events must still be readable (producer didn't deadlock).
	if d := logs.dropped.Load(); d > emitted {
		t.Fatalf("dropped=%d exceeds emitted=%d", d, emitted)
	}
	select {
	case <-logs.Channel:
	case <-time.After(2 * time.Second):
		t.Fatal("buffered event not readable; producer may have blocked")
	}

	// StopGathering reports a non-zero shed count.
	logs.StopGathering()
	wmu.Lock()
	defer wmu.Unlock()
	foundShedReport := false
	for _, w := range *warnings {
		if strings.Contains(w, "shed") {
			foundShedReport = true
		}
	}
	if !foundShedReport {
		t.Fatalf("expected a shed-count warning, got %v", *warnings)
	}
}

func TestLogs_StopGatheringClosesChannelAndIsIdempotent(t *testing.T) {
	fakeStream(t, `printf 'hdr\n'; exec cat`)

	logs := NewLogs()
	if err := logs.StartGathering("", nil); err != nil {
		t.Fatalf("StartGathering: %v", err)
	}

	logs.StopGathering()
	logs.StopGathering() // must not panic (double close guarded by stopOnce)

	// Channel must be closed once gathering has fully stopped.
	if _, ok := <-logs.Channel; ok {
		t.Fatal("Channel should be closed and drained after StopGathering")
	}
}

// shellQuote wraps s in single quotes for use in `sh -c`, escaping any
// embedded single quotes. The test payloads contain none, but keep it safe.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
