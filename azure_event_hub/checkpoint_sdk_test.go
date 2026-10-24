package usp_azure_event_hub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// Actual pinned USP SDK: websocket receipt, withheld/partial/cumulative ACKs,
// and accepted history surviving a lost checkpoint save fence. The proxy
// withholds all ACKs until the test explicitly sends them.
func TestEventHubsRealSDKACKAndPendingHistoryAfterSaveConflict(t *testing.T) {
	var connMu, writeMu sync.Mutex
	var socket *websocket.Conn
	received := make(chan uint64, 16)
	upgrader := websocket.Upgrader{}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if conn.ReadJSON(&protocol.ConnectionHeader{}) != nil {
			return
		}
		writeMu.Lock()
		err = conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageREADY})
		if err == nil {
			err = conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageFLOW, WindowSize: 16})
		}
		writeMu.Unlock()
		if err != nil {
			return
		}
		connMu.Lock()
		socket = conn
		connMu.Unlock()
		for {
			_, raw, e := conn.ReadMessage()
			if e != nil {
				return
			}
			var msg protocol.DataMessage
			if msgpack.Unmarshal(raw, &msg) != nil {
				return
			}
			select {
			case received <- msg.SeqNum:
			case <-r.Context().Done():
				return
			}
		}
	}))
	t.Cleanup(proxy.Close)
	client, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		Identity: uspclient.Identity{Oid: uuid.NewString(), InstallationKey: "test-installation"},
		DestURL:  "ws" + strings.TrimPrefix(proxy.URL, "http") + "/usp", Hostname: "test", Platform: "text", Architecture: "usp_adapter",
		OnError: func(error) {}, OnWarning: func(string) {}, DebugLog: func(string) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	a, g, s := harness(t, client)
	receipt := func(want uint64) {
		t.Helper()
		select {
		case seq := <-received:
			require.Equal(t, want, seq)
		case <-time.After(5 * time.Second):
			t.Fatal("SDK did not send")
		}
	}
	ack := func(seq uint64, count int) {
		t.Helper()
		connMu.Lock()
		conn := socket
		connMu.Unlock()
		require.NotNil(t, conn)
		writeMu.Lock()
		e := conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageACK, SeqNum: seq})
		writeMu.Unlock()
		require.NoError(t, e)
		require.Eventually(t, func() bool { p, e := client.GetUnsent(); return e == nil && len(p) == count }, time.Second, time.Millisecond)
	}
	for i := int64(0); i < 3; i++ {
		require.NoError(t, process(t, a, g, "0", 100+i, i))
		receipt(uint64(i + 1))
	}
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Empty(t, savedPositions(t, s), "receipt must never advance a checkpoint")
	ack(1, 2)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, "100", savedPositions(t, s)["0"].Offset)
	// An actual lost save fence must preserve the same accepted SDK history
	// and traversal, without replaying it as a replacement generation.
	s.mu.Lock()
	s.value.Nonce, s.value.Revision = "successor", "successor"
	s.mu.Unlock()
	oldClient, oldTracker, oldPart := a.uspClient, a.tracker, g.parts["0"]
	ack(2, 1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, "100", g.confirmed["0"].Offset, "coalesced tail still depends on ACK 3")
	require.NoError(t, process(t, a, g, "0", 103, 3))
	receipt(4)
	ack(3, 1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Nil(t, g.session, "lost save fence must disable persistence")
	s.mu.Lock()
	loads, swaps := s.loads, s.swaps
	s.mu.Unlock()
	require.NoError(t, process(t, a, g, "0", 104, 4))
	receipt(5)
	require.Same(t, oldClient, a.uspClient)
	require.Same(t, oldTracker, a.tracker)
	require.Same(t, oldPart, g.parts["0"])
	pending, err := client.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	require.EqualValues(t, 4, pending[0].SeqNum)
	require.EqualValues(t, 5, pending[1].SeqNum)
	ack(4, 1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, "103", g.confirmed["0"].Offset, "coalesced tail still depends on ACK 5")
	ack(5, 0)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, "104", g.confirmed["0"].Offset)
	require.Equal(t, "100", savedPositions(t, s)["0"].Offset)
	s.mu.Lock()
	require.Equal(t, loads, s.loads)
	require.Equal(t, swaps, s.swaps)
	s.mu.Unlock()

}

func TestPublicConstructorReturnsBeforeUnavailableCheckpointLoad(t *testing.T) {
	upgrader := websocket.Upgrader{}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if conn.ReadJSON(&protocol.ConnectionHeader{}) != nil {
			return
		}
		if conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageREADY}) != nil {
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer proxy.Close()
	store := &testStore{blockLoad: true}
	conf := EventHubConfig{ConnectionString: testConnection, CheckpointStore: store, ClientOptions: uspclient.ClientOptions{
		Identity: uspclient.Identity{Oid: uuid.NewString(), InstallationKey: "test-installation"}, DestURL: "ws" + strings.TrimPrefix(proxy.URL, "http") + "/usp", Hostname: "test", Platform: "text", Architecture: "usp_adapter",
		OnError: func(error) { t.Error("checkpoint failure reached vendor error callback") }, OnWarning: func(string) {}, DebugLog: func(string) {},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		a       *EventHubAdapter
		stopped chan struct{}
		err     error
	}
	returned := make(chan result, 1)
	go func() { a, stopped, err := NewEventHubAdapter(ctx, conf); returned <- result{a, stopped, err} }()
	var got result
	select {
	case got = <-returned:
	case <-time.After(time.Second):
		t.Error("constructor blocked on checkpoint loading")
		cancel()
		select {
		case got = <-returned:
		case <-time.After(3 * time.Second):
			t.Fatal("constructor did not honor cancellation")
		}
	}
	require.NoError(t, got.err)
	require.NotNil(t, got.stopped)
	require.NoError(t, got.a.Close())
}
