package usp_pubsub

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/gorilla/websocket"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type confirmationPeer struct {
	server   *httptest.Server
	mu       sync.Mutex
	conn     *websocket.Conn
	received chan protocol.DataMessage
}

func newConfirmationPeer(t *testing.T) *confirmationPeer {
	t.Helper()
	p := &confirmationPeer{received: make(chan protocol.DataMessage, 2048)}
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if err := conn.ReadJSON(&protocol.ConnectionHeader{}); err != nil {
			return
		}
		p.mu.Lock()
		p.conn = conn
		err = conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageREADY})
		if err == nil {
			err = conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageFLOW, WindowSize: 10000})
		}
		p.mu.Unlock()
		if err != nil {
			return
		}
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var message protocol.DataMessage
			if err := msgpack.Unmarshal(raw, &message); err != nil {
				return
			}
			p.received <- message
		}
	}))
	t.Cleanup(p.server.Close)
	return p
}

func (p *confirmationPeer) next(t *testing.T) protocol.DataMessage {
	t.Helper()
	select {
	case message := <-p.received:
		return message
	case <-time.After(5 * time.Second):
		t.Fatal("proxy did not receive source message")
		return protocol.DataMessage{}
	}
}

func (p *confirmationPeer) ack(t *testing.T, seq uint64) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageACK, SeqNum: seq}); err != nil {
		t.Fatal(err)
	}
}

func confirmedAdapter(t *testing.T, p *confirmationPeer, project, sub string, configure ...func(*PubSubConfig)) (*PubSubAdapter, <-chan struct{}) {
	t.Helper()
	conf := testConfig(project, sub)
	conf.ClientOptions.TestSinkMode = false
	conf.ClientOptions.DestURL = "ws" + strings.TrimPrefix(p.server.URL, "http")
	for _, apply := range configure {
		apply(&conf)
	}
	a, stopped, err := NewPubSubAdapter(context.Background(), conf)
	if err != nil {
		t.Fatal(err)
	}
	a.confirmationTimeout = 2 * time.Second
	t.Cleanup(func() { a.Close() })
	return a, stopped
}

func waitSourceAcks(t *testing.T, srv *pstest.Server, ids []string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		all := true
		for _, id := range ids {
			if srv.Message(id).Acks != want {
				all = false
				break
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("source ACKs did not reach %d for all %d messages", want, len(ids))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPubSubSourceAckWaitsForProxyConfirmationDuringClose(t *testing.T) {
	srv, topic, project, sub := newFakePubSub(t)
	peer := newConfirmationPeer(t)
	a, stopped := confirmedAdapter(t, peer, project, sub)
	var ids []string
	for i := 0; i < 8; i++ {
		id, err := topic.Publish(context.Background(), &pubsub.Message{Data: []byte("held-event")}).Get(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	var tail uint64
	for range ids {
		tail = peer.next(t).SeqNum
	}
	// The old adapter ACKed locally admitted messages while the proxy still
	// owned no data. Observe the actual source ACK batch interval as well.
	time.Sleep(250 * time.Millisecond)
	acked := 0
	for _, id := range ids {
		acked += srv.Message(id).Acks
	}
	t.Logf("PUBSUB-ACK-RESULT scenario=held-confirmation proxy_received=%d source_acked=%d proxy_confirmed=0", len(ids), acked)
	if acked != 0 {
		t.Fatalf("source acknowledged before proxy confirmation: %d", acked)
	}
	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	select {
	case err := <-done:
		t.Fatalf("Close returned before proxy confirmation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	for _, id := range ids {
		if srv.Message(id).Acks != 0 {
			t.Fatal("Receive cancellation acknowledged an unconfirmed event")
		}
		for _, modack := range srv.Message(id).Modacks {
			if modack.AckDeadline == 0 {
				t.Fatal("Receive cancellation returned source ownership while the SDK could still transmit")
			}
		}
	}
	peer.ack(t, tail)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not join confirmed source callbacks")
	}
	waitClosed(t, stopped, time.Second, "confirmed Receive")
	waitSourceAcks(t, srv, ids, 1)
}

func TestPubSubUnconfirmedCloseHandsMessagesBackToSource(t *testing.T) {
	srv, topic, project, sub := newFakePubSub(t)
	peer := newConfirmationPeer(t)
	a, stopped := confirmedAdapter(t, peer, project, sub)
	a.confirmationTimeout = 100 * time.Millisecond
	const count = 8
	ids := make(map[string]bool)
	for i := 0; i < count; i++ {
		id, err := topic.Publish(context.Background(), &pubsub.Message{Data: []byte("recoverable-event")}).Get(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ids[id] = true
	}
	for i := 0; i < count; i++ {
		peer.next(t)
	}
	start := time.Now()
	err := a.Close()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("never-ACK close error=%v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("never-ACK shutdown exceeded bound: %s", elapsed)
	}
	waitClosed(t, stopped, time.Second, "never-ACK Receive")
	for id := range ids {
		if srv.Message(id).Acks != 0 {
			t.Fatal("unconfirmed source event was acknowledged")
		}
	}
	// A new real subscriber must recover every original ID after the old SDK
	// has stopped sending, rather than relying on its volatile buffer.
	c, err := pubsub.NewClient(context.Background(), project)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var mu sync.Mutex
	seen := make(map[string]bool)
	err = c.Subscription(sub).Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()
		if !ids[m.ID] {
			t.Errorf("unexpected recovered source ID")
		}
		seen[m.ID] = true
		m.Ack()
		if len(seen) == count {
			cancel()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != count {
		t.Fatalf("recovered %d/%d original source IDs", len(seen), count)
	}
}

func TestPubSubOutstandingLimitCoversUnconfirmedMessages(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		limit, payloadBytes, maxBytes int
	}{
		{"message_count", pubsub.DefaultReceiveSettings.MaxOutstandingMessages, 32, 0},
		{"source_bytes", 4, 1024, 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, topic, project, sub := newFakePubSub(t)
			peer := newConfirmationPeer(t)
			a, stopped := confirmedAdapter(t, peer, project, sub, func(c *PubSubConfig) { c.MaxPSBuffer = tc.maxBytes })
			var ids []string
			for i := 0; i < tc.limit+5; i++ {
				id, err := topic.Publish(context.Background(), &pubsub.Message{Data: []byte(strings.Repeat("x", tc.payloadBytes))}).Get(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			var tail uint64
			for i := 0; i < tc.limit; i++ {
				tail = peer.next(t).SeqNum
			}
			select {
			case message := <-peer.received:
				t.Fatalf("source outstanding limit exceeded before confirmation: seq=%d limit=%d", message.SeqNum, tc.limit)
			case <-time.After(200 * time.Millisecond):
			}
			pending, err := a.uspClient.GetUnsent()
			if err != nil || len(pending) != tc.limit {
				t.Fatalf("pending=%d want=%d err=%v", len(pending), tc.limit, err)
			}
			for _, id := range ids {
				if srv.Message(id).Acks != 0 {
					t.Fatal("unconfirmed event freed a source permit")
				}
			}
			t.Logf("PUBSUB-ACK-RESULT scenario=outstanding-bound mode=%s source_limit=%d unconfirmed=%d source_acked=0 payload_bytes=%d", tc.name, tc.limit, len(pending), tc.payloadBytes)
			pullStopped := make(chan struct{})
			stopPulls := a.stopSub
			a.stopSub = func() { stopPulls(); close(pullStopped) }
			done := make(chan error, 1)
			go func() { done <- a.Close() }()
			waitClosed(t, pullStopped, time.Second, "source pull cancellation")
			// Ensure cancellation of Receive cannot free these accepted messages
			// before their final cumulative confirmation.
			select {
			case err := <-done:
				t.Fatalf("Close returned before ACK: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			peer.ack(t, tail)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("bounded source Close did not join")
			}
			waitClosed(t, stopped, time.Second, "bounded source Receive")
			acked := 0
			for _, id := range ids {
				acked += srv.Message(id).Acks
			}
			if acked != tc.limit {
				t.Fatalf("confirmed source ACKs=%d want=%d", acked, tc.limit)
			}
		})
	}
}

func TestPubSubFullSharedBudgetCloseReturnsUnconfirmedSourceMessages(t *testing.T) {
	srv, topic, project, sub := newFakePubSub(t)
	peer := newConfirmationPeer(t)
	pool := uspclient.NewByteBudget(1)
	a, stopped := confirmedAdapter(t, peer, project, sub, func(c *PubSubConfig) { c.ClientOptions.BufferOptions.SharedBudget = pool })
	a.confirmationTimeout = 100 * time.Millisecond
	var ids []string
	for i := 0; i < 8; i++ {
		id, err := topic.Publish(context.Background(), &pubsub.Message{Data: []byte("pool-full")}).Get(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	peer.next(t)
	select {
	case <-peer.received:
		t.Fatal("a full pool admitted more than its one-message progress allowance")
	case <-time.After(100 * time.Millisecond):
	}
	if pool.Used() <= pool.Total() {
		t.Fatal("shared pool was not saturated")
	}
	start := time.Now()
	if err := a.Close(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("full-pool close=%v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("blocked source producers held Close past its bound")
	}
	waitClosed(t, stopped, time.Second, "full-pool Receive")
	if pool.Used() != 0 {
		t.Fatal("closed client retained shared pool bytes")
	}
	for _, id := range ids {
		if srv.Message(id).Acks != 0 {
			t.Fatal("full-pool shutdown acknowledged an unconfirmed source message")
		}
	}
}
