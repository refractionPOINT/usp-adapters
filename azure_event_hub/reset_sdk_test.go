package usp_azure_event_hub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"

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
	a, g := harness(t, client)
	h := &resetRuntimeHub{discoveryHub: discoveryHub{testHub: testHub{ids: []string{"0", "1"}}}}
	a.hub = h
	require.NoError(t, a.discover(g, true))
	old, other := callFor(t, &h.testHub, "0", 1), callFor(t, &h.testHub, "1", 1)
	require.Empty(t, old.start.Offset)
	require.Empty(t, other.start.Offset)
	require.NoError(t, old.handler(context.Background(), fixtureEvent(1000, 100)))
	receipt(1)
	require.NoError(t, other.handler(context.Background(), fixtureEvent(5000, 500)))
	receipt(2)
	ack(2, 0)
	require.NoError(t, a.confirm(g))
	require.Equal(t, map[string]int64{"0": 100, "1": 500}, g.confirmed)
	require.NoError(t, old.handler(context.Background(), fixtureEvent(1001, 101)))
	receipt(3)
	require.NoError(t, other.handler(context.Background(), fixtureEvent(5001, 501)))
	receipt(4)
	h.runtime = func(_ context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, error) {
		if id == "0" {
			return runtimePosition(id, 10), nil
		}
		return runtimePosition(id, 600), nil
	}
	g.runtimePreferred = "0"
	require.NoError(t, a.discover(g, false))
	replacement := callFor(t, &h.testHub, "0", 2)
	require.Empty(t, replacement.start.Offset)
	pending, err := client.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 2, "reset retains the real SDK's accepted messages")
	require.NoError(t, replacement.handler(context.Background(), fixtureEvent(50, 11)))
	receipt(5)
	require.Error(t, old.handler(context.Background(), fixtureEvent(1002, 102)))
	require.NoError(t, a.confirm(g))
	require.NotContains(t, g.confirmed, "0", "receipt is not an ACK")
	ack(3, 2)
	require.NoError(t, a.confirm(g))
	require.NotContains(t, g.confirmed, "0", "old candidate ACK cannot restore retired source")
	require.EqualValues(t, 500, g.confirmed["1"])
	ack(4, 1)
	require.NoError(t, a.confirm(g))
	require.EqualValues(t, 501, g.confirmed["1"])
	require.NotContains(t, g.confirmed, "0")
	ack(5, 0)
	require.NoError(t, a.confirm(g))
	require.EqualValues(t, 11, g.confirmed["0"])
	require.Len(t, h.snapshot(), 3)
	require.Same(t, client, a.uspClient)
	a.stopGeneration(g)
}
