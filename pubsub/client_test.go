package usp_pubsub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

// newFakePubSub starts an in-process Pub/Sub server with one topic and one
// subscription, and points the client library at it.
func newFakePubSub(t *testing.T, reactors ...pstest.ServerReactorOption) (srv *pstest.Server, topic *pubsub.Topic, project, sub string) {
	t.Helper()
	srv = pstest.NewServer(reactors...)
	t.Cleanup(func() { srv.Close() })
	t.Setenv("PUBSUB_EMULATOR_HOST", srv.Addr)

	project, sub = "test-project", "test-sub"
	ctx := context.Background()
	c, err := pubsub.NewClient(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	topic, err = c.CreateTopic(ctx, "test-topic")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.CreateSubscription(ctx, sub, pubsub.SubscriptionConfig{Topic: topic}); err != nil {
		t.Fatal(err)
	}
	return srv, topic, project, sub
}

func testConfig(project, sub string) PubSubConfig {
	return PubSubConfig{
		ClientOptions: uspclient.ClientOptions{
			Identity:     uspclient.Identity{Oid: uuid.NewString(), InstallationKey: "iid"},
			Platform:     "text",
			Architecture: "usp_adapter",
			TestSinkMode: true,
			DebugLog:     func(string) {},
			OnError:      func(error) {},
			OnWarning:    func(string) {},
		},
		SubscriptionName:    sub,
		ProjectName:         project,
		ServiceAccountCreds: "-",
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, within time.Duration, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(within):
		t.Fatalf("%s: not closed within %v", what, within)
	}
}

// Close() must make the receive goroutine exit: the caller waits on the
// returned channel to know the adapter is fully stopped. Before the fix the
// loop only ended when buildSub became nil, which never happens.
func TestCloseStopsReceiveLoop(t *testing.T) {
	srv, topic, project, sub := newFakePubSub(t)
	a, chStopped, err := NewPubSubAdapter(context.Background(), testConfig(project, sub))
	if err != nil {
		t.Fatalf("NewPubSubAdapter(): %v", err)
	}

	// The adapter really works: a published message gets acked.
	ctx := context.Background()
	id, err := topic.Publish(ctx, &pubsub.Message{Data: []byte("hello")}).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for srv.Message(id).Acks == 0 {
		if time.Now().After(deadline) {
			t.Fatal("published message was never acked")
		}
		time.Sleep(20 * time.Millisecond)
	}

	select {
	case <-chStopped:
		t.Fatal("receive loop stopped before Close()")
	default:
	}

	start := time.Now()
	if err := a.Close(); err != nil {
		t.Logf("Close(): %v", err)
	}
	waitClosed(t, chStopped, 5*time.Second, "chStopped after Close")
	t.Logf("receive loop stopped %v after Close", time.Since(start))
}

// A failed start (here an unknown subscription) returns an error, and must not
// leave the receive loop running forever against a closed client.
func TestFailedStartStopsReceiveLoop(t *testing.T) {
	_, _, project, _ := newFakePubSub(t)
	_, _, err := NewPubSubAdapter(context.Background(), testConfig(project, "no-such-sub"))
	if err == nil {
		t.Fatal("expected an error for a subscription that does not exist")
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		buf := make([]byte, 1<<20)
		stacks := string(buf[:runtime.Stack(buf, true)])
		if !strings.Contains(stacks, "usp-adapters/pubsub.NewPubSubAdapter.func") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("receive loop still running after NewPubSubAdapter failed")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A backend that accepts connections and data but never acks, so the client's
// buffer (window of 1) stays full.
func newNeverAckServer(t *testing.T) string {
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if err := conn.ReadJSON(&protocol.ConnectionHeader{}); err != nil {
			return
		}
		if err := conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageREADY}); err != nil {
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/usp"
}

// A message handler blocked on a full buffer must return promptly once the
// adapter is closing: Receive waits for every handler, so a handler stuck for
// up to an hour keeps the adapter from ever stopping.
func TestProcessEventReturnsWhenClosedWhileBufferFull(t *testing.T) {
	for _, how := range []string{"subscription stopped", "client closed"} {
		t.Run(how, func(t *testing.T) {
			var onErrors atomic.Int32
			opts := uspclient.ClientOptions{
				Identity:     uspclient.Identity{Oid: uuid.NewString(), InstallationKey: "iid"},
				DestURL:      newNeverAckServer(t),
				Hostname:     "host",
				Platform:     "text",
				Architecture: "usp_adapter",
				OnError:      func(error) { onErrors.Add(1) },
				DebugLog:     func(string) {},
				OnWarning:    func(string) {},
			}
			client, err := uspclient.NewClient(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			a := &PubSubAdapter{conf: PubSubConfig{ClientOptions: opts}, uspClient: client}

			// The window starts at 1 and nothing is acked: one message fills it.
			if err := client.Ship(&protocol.DataMessage{TextPayload: "fill"}, time.Second); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				a.processEvent(ctx, &pubsub.Message{Data: []byte("blocked")})
			}()
			select {
			case <-done:
				t.Fatal("handler returned while the buffer was full and the adapter running")
			case <-time.After(500 * time.Millisecond):
			}

			if how == "client closed" {
				// The host tears the adapter down: the client is closed while
				// the subscription context is still live. The blocked Ship is
				// woken at once and reports ErrorClosed.
				client.Close()
				waitClosed(t, done, time.Second, "handler after the client is closed")
			} else {
				cancel()
				waitClosed(t, done, 2*shipRetrySlice+time.Second, "handler after the subscription is stopped")
			}
			if onErrors.Load() != 0 {
				t.Fatal("a shutdown must not be reported as a Ship error")
			}
		})
	}
}

type blockedAckReactor struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockedAckReactor) React(interface{}) (bool, interface{}, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return false, nil, nil
}

// Closing the transport before Receive joins can abandon already-buffered
// source acknowledgements and replay the same events after lease handoff.
func TestCloseFlushesSourceAcksBeforeClosingTransport(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		name := "ack_completes"
		if timeout {
			name = "ack_stalls"
		}
		t.Run(name, func(t *testing.T) {
			block := &blockedAckReactor{entered: make(chan struct{}), release: make(chan struct{})}
			var release sync.Once
			defer release.Do(func() { close(block.release) })
			srv, topic, project, sub := newFakePubSub(t, pstest.ServerReactorOption{FuncName: "Acknowledge", Reactor: block})
			conf := testConfig(project, sub)
			var warnings atomic.Int32
			conf.ClientOptions.OnWarning = func(string) { warnings.Add(1) }
			a, stopped, err := NewPubSubAdapter(context.Background(), conf)
			if err != nil {
				t.Fatal(err)
			}
			id, err := topic.Publish(context.Background(), &pubsub.Message{Data: []byte("source-tail")}).Get(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-block.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("source ACK was never sent")
			}
			if timeout {
				a.receiveDrainTimeout = 100 * time.Millisecond
			}
			done := make(chan error, 1)
			go func() { done <- a.Close() }()
			if timeout {
				select {
				case err := <-done:
					if err == nil || warnings.Load() != 1 {
						t.Fatalf("stalled source flush not reported: %v, warnings=%d", err, warnings.Load())
					}
				case <-time.After(2 * time.Second):
					t.Fatal("Close hung on source ACK RPC")
				}
				release.Do(func() { close(block.release) })
			} else {
				select {
				case err := <-done:
					t.Fatalf("transport closed before source ACK RPC completed: %v", err)
				case <-time.After(80 * time.Millisecond):
				}
				release.Do(func() { close(block.release) })
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("Close did not join completed Receive")
				}
				if got := srv.Message(id).Acks; got != 1 {
					t.Fatalf("source ACK count=%d want1", got)
				}
			}
			waitClosed(t, stopped, time.Second, "receive stopped")
		})
	}
}
