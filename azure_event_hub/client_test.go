package usp_azure_event_hub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

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
	a, g, store := harness(t, client)
	a.conf.ClientOptions = opts
	p := &partitionState{}
	g.parts["0"] = p

	if err := a.processPartitionEvent(context.Background(), g, "0", p, fixtureEvent(100, 0)); err != nil {
		t.Fatalf("processEvent on a live client: %v", err)
	}

	client.Close()
	if err := a.processPartitionEvent(context.Background(), g, "0", p, fixtureEvent(101, 1)); err != nil {
		t.Fatalf("processEvent after the client closed returned %v, want nil", err)
	}
	if err := a.saveConfirmed(context.Background(), g); err == nil {
		if len(savedPositions(t, store)) != 0 {
			t.Fatal("closed SDK admission advanced the checkpoint")
		}
	}
	if n := onErrors.Load(); n != 0 {
		t.Fatalf("a closed client was reported %d times as an error", n)
	}
}
