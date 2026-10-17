package usp_pubsub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/gorilla/websocket"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

// This measures the in-process source, adapter and SDK, rather than a deployed
// proxy or endpoint. Its peer ACKs requested frames and a periodic 100ms tail.
// Run with a fixed event count to compare source-ACK admission with confirmation.
func BenchmarkPubSubConfirmationPipeline(b *testing.B) {
	source, topic, project, sub := newFakePubSub(b)
	var received, ackWrites atomic.Int64
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := up.Upgrade(w, req, nil)
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
		if err := conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageFLOW, WindowSize: 100000}); err != nil {
			return
		}
		var writeMu sync.Mutex
		var tail atomic.Uint64
		ack := func(seq uint64) {
			if seq == 0 {
				return
			}
			writeMu.Lock()
			defer writeMu.Unlock()
			if err := conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageACK, SeqNum: seq}); err == nil {
				ackWrites.Add(1)
			}
		}
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			timer := time.NewTicker(100 * time.Millisecond)
			defer timer.Stop()
			for {
				select {
				case <-stop:
					return
				case <-timer.C:
					ack(tail.Load())
				}
			}
		}()
		defer func() { close(stop); wg.Wait() }()
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var msg protocol.DataMessage
			if err := msgpack.Unmarshal(raw, &msg); err != nil {
				return
			}
			tail.Store(msg.SeqNum)
			received.Add(1)
			if msg.AckRequested {
				ack(msg.SeqNum)
			}
		}
	}))
	b.Cleanup(server.Close)
	conf := testConfig(project, sub)
	conf.ClientOptions.TestSinkMode = false
	conf.ClientOptions.DestURL = "ws" + strings.TrimPrefix(server.URL, "http")
	a, stopped, err := NewPubSubAdapter(context.Background(), conf)
	if err != nil {
		b.Fatal(err)
	}
	a.confirmationTimeout = 5 * time.Second
	b.Cleanup(func() {
		if err := a.Close(); err != nil {
			b.Error(err)
		}
		select {
		case <-stopped:
		case <-time.After(time.Second):
			b.Error("benchmark Receive did not stop")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	payload := []byte(strings.Repeat("x", 256))
	results := make([]*pubsub.PublishResult, 0, b.N)
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		results = append(results, topic.Publish(ctx, &pubsub.Message{Data: payload}))
	}
	for _, result := range results {
		if _, err := result.Get(ctx); err != nil {
			b.Fatal(err)
		}
	}
	for {
		acked := 0
		for _, message := range source.Messages() {
			acked += message.Acks
		}
		if acked == b.N && received.Load() == int64(b.N) {
			break
		}
		select {
		case <-ctx.Done():
			b.Fatalf("benchmark did not finish: source_acked=%d proxy_received=%d wanted=%d", acked, received.Load(), b.N)
		case <-time.After(10 * time.Millisecond):
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "events/s")
	b.ReportMetric(float64(ackWrites.Load())/float64(b.N), "proxy-acks/event")
}
