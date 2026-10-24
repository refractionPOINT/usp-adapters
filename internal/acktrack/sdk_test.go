package acktrack

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

// Same JSON READY/FLOW and msgpack data framing as the adapter websocket
// fixtures. The test explicitly sends cumulative ACKs, including an unflagged
// partial ACK: neither successful Ship nor socket receipt is acceptance.
func TestACKTrackerRealSDKCumulativeACK(t *testing.T) {
	var connMu, writeMu sync.Mutex
	var serverConn *websocket.Conn
	received := make(chan uint64, 10)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		serverConn = conn
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
	t.Cleanup(server.Close)
	client, err := NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: false,
		Identity:     uspclient.Identity{Oid: uuid.NewString(), InstallationKey: "test-installation"},
		DestURL:      "ws" + strings.TrimPrefix(server.URL, "http") + "/usp",
		Hostname:     "test", Platform: "text", Architecture: "usp_adapter",
		OnError: func(error) {}, OnWarning: func(string) {}, DebugLog: func(string) {},
	})
	require.NoError(t, err)
	require.IsType(t, &uspclient.Client{}, client, "false flag must keep the ordinary SDK and real ACK gating")
	t.Cleanup(func() { client.Close() })
	tracker := observerTracker(t, client)
	a := observerShip(t, tracker)
	require.NoError(t, tracker.Candidate("stream", []byte("first page"), a))
	b := observerShip(t, tracker)
	require.NoError(t, tracker.Candidate("stream", []byte("second page"), b))
	for want := uint64(1); want <= 2; want++ {
		select {
		case seq := <-received:
			require.Equal(t, want, seq)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for SDK websocket data")
		}
	}
	// Both records really reached the server, but it has withheld all ACKs.
	observerConfirm(t, tracker, map[string]string{})
	ack := func(seq uint64, pendingCount int) {
		t.Helper()
		connMu.Lock()
		conn := serverConn
		connMu.Unlock()
		require.NotNil(t, conn)
		writeMu.Lock()
		err := conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageACK, SeqNum: seq})
		writeMu.Unlock()
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			pending, err := client.GetUnsent()
			return err == nil && len(pending) == pendingCount
		}, 5*time.Second, time.Millisecond, "SDK must process the cumulative ACK before observation")
	}
	ack(1, 1)
	observerConfirm(t, tracker, map[string]string{"stream": "first page"})
	observerConfirm(t, tracker, map[string]string{})
	ack(2, 0)
	observerConfirm(t, tracker, map[string]string{"stream": "second page"})
	// The real SDK's ErrorClosed must yield no valid admission dependency.
	client.Close()
	failed, err := tracker.Ship(&protocol.DataMessage{SeqNum: 2}, time.Millisecond)
	require.ErrorIs(t, err, uspclient.ErrorClosed)
	require.ErrorIs(t, tracker.Candidate("stream", []byte("failed page"), failed), ErrDependency)
}
