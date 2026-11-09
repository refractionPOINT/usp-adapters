package lookbacktest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// Client ACKs real SDK frames only after gate opens (nil means immediate).
func Client(t *testing.T, gate <-chan struct{}) (*uspclient.Client, <-chan *protocol.DataMessage) {
	t.Helper()
	received := make(chan *protocol.DataMessage, 2048)
	stopped := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		socket, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer socket.Close()
		if socket.ReadJSON(&protocol.ConnectionHeader{}) != nil ||
			socket.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageREADY}) != nil ||
			socket.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageFLOW, WindowSize: 1024}) != nil {
			return
		}

		for {
			_, raw, err := socket.ReadMessage()
			if err != nil {
				return
			}
			message := &protocol.DataMessage{}
			if msgpack.Unmarshal(raw, message) != nil {
				return
			}
			select {
			case received <- message:
			case <-stopped:
				return
			}
			if message.AckRequested {
				if gate != nil {
					select {
					case <-gate:
					case <-stopped:
						return
					}
				}
				if socket.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageACK, SeqNum: message.SeqNum}) != nil {
					return
				}
			}
		}
	}))
	client, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{Identity: uspclient.Identity{Oid: uuid.NewString(), InstallationKey: "fixture"}, DestURL: "ws" + strings.TrimPrefix(server.URL, "http"), Hostname: "fixture", Platform: "json", Architecture: "usp_adapter", OnError: func(error) {}, OnWarning: func(string) {}, DebugLog: func(string) {}})
	require.NoError(t, err)
	t.Cleanup(func() { close(stopped); client.Close(); server.Close() })
	return client, received
}
func Receive(t *testing.T, received <-chan *protocol.DataMessage) *protocol.DataMessage {
	t.Helper()
	select {
	case m := <-received:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("no USP frame")
		return nil
	}
}
