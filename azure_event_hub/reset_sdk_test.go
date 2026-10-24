package usp_azure_event_hub

import (
	"context"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// Azure source work is mocked; receipt and cumulative ACK behavior use the actual pinned USP SDK.
func TestRealSDKPendingHistoryAcrossVerifiedPartitionReset(t *testing.T) {
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
	s := resetStore(t)
	h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0", "1"}}}}
	var last atomic.Int64
	last.Store(600)
	h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		if id == "0" {
			return runtimePosition(id, last.Load()), nil
		}
		return runtimePosition(id, 600), nil
	}
	var vendorErrors atomic.Int32
	a := resetAdapter(t, h, client, s, nil, func(error) { vendorErrors.Add(1) })
	worker := startWorker(t, a)
	old := resetCall(t, &h.testHub, "0", 1)
	other := resetCall(t, &h.testHub, "1", 1)
	require.Greater(t, h.queries.Load(), int32(0))
	require.NoError(t, old.handler(context.Background(), fixtureEvent(1001, 101)))
	receipt(1)
	require.NoError(t, other.handler(context.Background(), fixtureEvent(5001, 501)))
	receipt(2)
	last.Store(10)
	replacement := resetCall(t, &h.testHub, "0", 2)
	require.Empty(t, replacement.start.Offset)
	pending, err := client.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 2, "reset must retain the real SDK's old accepted messages")
	last.Store(11)
	require.NoError(t, replacement.handler(context.Background(), fixtureEvent(50, 11)))
	receipt(3)
	require.Error(t, old.handler(context.Background(), fixtureEvent(1002, 102)))
	require.NotContains(t, savedPositions(t, s), "0", "websocket receipt cannot certify a new source position")
	ack(1, 2)
	time.Sleep(20 * time.Millisecond)
	require.NotContains(t, savedPositions(t, s), "0", "late cumulative old ACK cannot restore the previous incarnation")
	require.Equal(t, "5000", savedPositions(t, s)["1"].Offset)
	ack(2, 1)
	require.Eventually(t, func() bool { return savedPositionOffset(s.snapshot().Position, "1") == "5001" }, time.Second, time.Millisecond)
	require.NotContains(t, savedPositions(t, s), "0")
	ack(3, 0)
	require.Eventually(t, func() bool { return savedPositionOffset(s.snapshot().Position, "0") == "50" }, time.Second, time.Millisecond)
	require.Len(t, h.snapshot(), 3)
	require.Same(t, client, a.uspClient)
	require.NoError(t, a.Close())
	<-worker
	require.Zero(t, vendorErrors.Load())
}
