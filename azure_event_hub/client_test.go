package usp_azure_event_hub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

// An event handled while the adapter is shutting down (its client is closed)
// must not be reported as an error, and must not hand the Event Hub library an
// error either: the library would report that as a listener failure.
func TestProcessEventAfterClientClosedIsNotAnError(t *testing.T) {
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
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
	defer srv.Close()

	var onErrors atomic.Int32
	opts := uspclient.ClientOptions{
		Identity:     uspclient.Identity{Oid: uuid.NewString(), InstallationKey: "iid"},
		DestURL:      "ws" + strings.TrimPrefix(srv.URL, "http") + "/usp",
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
	a := &EventHubAdapter{conf: EventHubConfig{ClientOptions: opts}, uspClient: client}

	if err := a.processEvent(context.Background(), &eventhub.Event{Data: []byte("x")}); err != nil {
		t.Fatalf("processEvent on a live client: %v", err)
	}

	client.Close()
	if err := a.processEvent(context.Background(), &eventhub.Event{Data: []byte("x")}); err != nil {
		t.Fatalf("processEvent after the client closed returned %v, want nil", err)
	}
	if n := onErrors.Load(); n != 0 {
		t.Fatalf("a closed client was reported %d times as an error", n)
	}
}
