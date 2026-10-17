package usp_pubsub

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
)

const crashHelperEnv = "USP_PUBSUB_CONFIRMATION_CRASH_HELPER"

func TestMain(m *testing.M) {
	if os.Getenv(crashHelperEnv) == "1" {
		if err := runCrashHelper(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	os.Exit(m.Run())
}

// Run the real adapter in a child that the test can terminate abruptly while
// its Pub/Sub server and proxy peer remain alive in the parent.
func runCrashHelper() error {
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		return fmt.Errorf("crash helper requires its local source emulator")
	}
	conf := testConfig(os.Getenv("USP_CRASH_PROJECT"), os.Getenv("USP_CRASH_SUB"))
	conf.ClientOptions.TestSinkMode = false
	conf.ClientOptions.DestURL = os.Getenv("USP_CRASH_PROXY")
	a, stopped, err := NewPubSubAdapter(context.Background(), conf)
	if err != nil {
		return err
	}
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, os.Getenv("USP_CRASH_READY"), nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("crash helper readiness failed: %s", response.Status)
	}
	<-stopped
	return fmt.Errorf("crash helper Receive stopped before it was terminated")
}

func TestPubSubAbruptProcessStopPreservesSourceRedelivery(t *testing.T) {
	// Parent and child together use at most two scheduler threads.
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)
	source, topic, project, sub := newFakePubSub(t)
	peer := newConfirmationPeer(t)
	ready := make(chan struct{})
	var readyOnce sync.Once
	readyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		readyOnce.Do(func() { close(ready) })
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(readyServer.Close)
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), crashHelperEnv+"=1", "USP_CRASH_PROJECT="+project,
		"USP_CRASH_SUB="+sub, "USP_CRASH_PROXY="+"ws"+strings.TrimPrefix(peer.server.URL, "http"), "USP_CRASH_READY="+readyServer.URL, "GOMAXPROCS=1")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var killOnce sync.Once
	var killErr, waitErr error
	kill := func() { killOnce.Do(func() { killErr = cmd.Process.Kill(); waitErr = cmd.Wait() }) }
	t.Cleanup(kill)
	select {
	case <-ready:
	case <-time.After(6 * time.Second):
		kill()
		t.Fatalf("adapter child did not initialize: %s", output.String())
	}
	const count = 8
	var ids []string
	expected := make(map[string]bool)
	for i := 0; i < count; i++ {
		payload := "crash-event-" + strconv.Itoa(i)
		expected[payload] = true
		id, err := topic.Publish(context.Background(), &pubsub.Message{Data: []byte(payload)}).Get(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	for i := 0; i < count; i++ {
		peer.next(t)
	}
	// Allow an admission-only adapter's real source ACK batch to flush while
	// the peer deliberately confirms none of its received frames.
	time.Sleep(250 * time.Millisecond)
	acked := 0
	for _, id := range ids {
		acked += source.Message(id).Acks
	}
	kill()
	if killErr != nil || waitErr == nil {
		t.Fatalf("adapter process was not deliberately terminated: kill=%v wait=%v; child output: %s", killErr, waitErr, output.String())
	}
	if runtime.GOOS == "linux" && cmd.ProcessState.String() != "signal: killed" {
		t.Fatalf("adapter child was not killed by SIGKILL: %s; child output: %s", cmd.ProcessState, output.String())
	}
	if child := output.String(); strings.Contains(child, "WARNING: DATA RACE") || strings.Contains(child, "panic:") || strings.Contains(child, "fatal error:") {
		t.Fatalf("adapter child reported a runtime failure: %s", child)
	}
	t.Logf("PUBSUB-ACK-RESULT scenario=abrupt-process-stop sent=%d source_acked_before_kill=%d proxy_confirmed=0 process_state=%q", count, acked, cmd.ProcessState.String())
	if acked != 0 {
		t.Fatalf("abrupt stop abandoned %d source-acknowledged events before proxy confirmation", acked)
	}
	// Its volatile SDK buffer is gone. Recovery must come from the original
	// Pub/Sub subscription after the dead receiver's message leases expire.
	a, stopped := confirmedAdapter(t, peer, project, sub)
	seen := make(map[string]bool)
	var tail uint64
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	for len(seen) < count {
		select {
		case message := <-peer.received:
			if !expected[message.TextPayload] {
				t.Fatal("replacement received an unexpected payload")
			}
			if seen[message.TextPayload] {
				t.Fatal("replacement received a duplicate before confirmation")
			}
			seen[message.TextPayload] = true
			tail = message.SeqNum
		case <-deadline.C:
			t.Fatalf("replacement recovered %d/%d source identities; child output: %s", len(seen), count, output.String())
		}
	}
	peer.ack(t, tail)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, stopped, time.Second, "replacement Receive")
	waitSourceAcks(t, source, ids, 1)
	for _, id := range ids {
		if source.Message(id).Deliveries < 2 {
			t.Fatal("original source ID was not redelivered")
		}
	}
	t.Logf("PUBSUB-ACK-RESULT scenario=abrupt-process-stop-recovery sent=%d redelivered=%d source_acked=%d lost=0", count, len(seen), count)
}
