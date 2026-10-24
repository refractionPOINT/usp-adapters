package usp_okta

import (
	"context"
	"fmt"
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

func TestOktaCheckpointRealSDKCumulativeACK(t *testing.T) {
	var connMu, writeMu sync.Mutex
	var socket *websocket.Conn
	received := make(chan uint64, 8)
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
			err = conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageFLOW, WindowSize: 10})
		}
		writeMu.Unlock()
		if err != nil {
			return
		}
		connMu.Lock()
		socket = conn
		connMu.Unlock()
		for {
			_, raw, err := conn.ReadMessage()
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
	t.Cleanup(proxy.Close)
	client, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		Identity: uspclient.Identity{Oid: uuid.NewString(), InstallationKey: "test-installation"},
		DestURL:  "ws" + strings.TrimPrefix(proxy.URL, "http") + "/usp", Hostname: "test", Platform: "okta", Architecture: "usp_adapter",
		OnError: func(error) {}, OnWarning: func(string) {}, DebugLog: func(string) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	var calls atomic.Int32
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s?after=%d>; rel=\"next\"", r.Host, logsURL, n))
		if n <= 2 {
			fmt.Fprintf(w, `[{"uuid":"event-%d"}]`, n)
		} else {
			fmt.Fprint(w, `[]`)
		}
	}))
	defer vendor.Close()
	a, g, store := checkpointHarness(t, vendor.URL, client)
	stop := runCollector(t, a, g)
	for want := uint64(1); want <= 2; want++ {
		select {
		case seq := <-received:
			require.Equal(t, want, seq)
		case <-time.After(5 * time.Second):
			t.Fatal("SDK did not send data")
		}
	}
	require.Eventually(t, func() bool { return calls.Load() >= 4 }, time.Second, time.Millisecond)
	stop()
	initial := store.snapshot().Position
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, initial, store.snapshot().Position, "socket receipt is not an ACK")
	ack := func(seq uint64, count int) {
		t.Helper()
		connMu.Lock()
		conn := socket
		connMu.Unlock()
		require.NotNil(t, conn)
		writeMu.Lock()
		err := conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageACK, SeqNum: seq})
		writeMu.Unlock()
		require.NoError(t, err)
		require.Eventually(t, func() bool { pending, err := client.GetUnsent(); return err == nil && len(pending) == count }, time.Second, time.Millisecond)
	}
	ack(1, 1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err := a.decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Contains(t, p.Next, "after=1")
	require.Len(t, p.IDs, 1)
	ack(2, 0)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err = a.decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.NotContains(t, p.Next, "after=1")
	require.Len(t, p.IDs, 2)
}
