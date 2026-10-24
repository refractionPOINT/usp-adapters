package usp_entraid

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

// Exercise the real SDK's assigned sequences and cumulative ACK buffer, rather
// than conflating successful Ship or socket receipt with upstream acceptance.
func TestEntraCheckpointRealSDKCumulativeACK(t *testing.T) {
	var connectionMu, writeMu sync.Mutex
	var connection *websocket.Conn
	received := make(chan uint64, 8)
	upgrader := websocket.Upgrader{}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		socket, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer socket.Close()
		if socket.ReadJSON(&protocol.ConnectionHeader{}) != nil {
			return
		}
		writeMu.Lock()
		err = socket.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageREADY})
		if err == nil {
			err = socket.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageFLOW, WindowSize: 10})
		}
		writeMu.Unlock()
		if err != nil {
			return
		}
		connectionMu.Lock()
		connection = socket
		connectionMu.Unlock()
		for {
			_, raw, err := socket.ReadMessage()
			if err != nil {
				return
			}
			var message protocol.DataMessage
			if msgpack.Unmarshal(raw, &message) != nil {
				return
			}
			select {
			case received <- message.SeqNum:
			case <-r.Context().Done():
				return
			}
		}
	}))
	defer proxy.Close()
	client, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{Identity: uspclient.Identity{Oid: uuid.NewString(), InstallationKey: "fixture-installation"}, DestURL: "ws" + strings.TrimPrefix(proxy.URL, "http") + "/usp", Hostname: "fixture", Platform: "entraid", Architecture: "usp_adapter", OnError: func(error) {}, OnWarning: func(string) {}, DebugLog: func(string) {}})
	require.NoError(t, err)
	defer client.Close()
	vendor := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := bounds(t, r)
		sendPage(w, []map[string]interface{}{item(start.String(), start.Add(time.Second))}, "")
	})
	a, g, store := harness(t, vendor, client)
	initial := store.snapshot().Position
	first := scan(t, a, g, a.streams[0])
	second := scan(t, a, g, a.streams[0])
	for want := uint64(1); want <= 2; want++ {
		select {
		case seq := <-received:
			require.Equal(t, want, seq)
		case <-time.After(5 * time.Second):
			t.Fatal("SDK did not send fixture")
		}
	}
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, initial, store.snapshot().Position, "socket receipt is not ACK")
	ack := func(seq uint64, pending int) {
		t.Helper()
		connectionMu.Lock()
		socket := connection
		connectionMu.Unlock()
		require.NotNil(t, socket)
		writeMu.Lock()
		err := socket.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageACK, SeqNum: seq})
		writeMu.Unlock()
		require.NoError(t, err)
		require.Eventually(t, func() bool { messages, err := client.GetUnsent(); return err == nil && len(messages) == pending }, time.Second, time.Millisecond)
	}
	ack(1, 1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err := decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, first, p[a.streams[0].name].Boundary)
	require.Len(t, p[a.streams[0].name].IDs, 1)
	ack(2, 0)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err = decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, second, p[a.streams[0].name].Boundary)
	require.Len(t, p[a.streams[0].name].IDs, 2)
}
