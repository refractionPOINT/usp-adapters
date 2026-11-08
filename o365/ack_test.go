package usp_o365

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/refractionPOINT/gjson"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

type ackFrame struct {
	conn *websocket.Conn
	msg  protocol.DataMessage
}

type trackingACKClient struct {
	*uspclient.Client
	receipts chan uspclient.AdmissionReceipt
	calls    int
	failOn   int
}

func (c *trackingACKClient) ShipWithReceipt(m *protocol.DataMessage, d time.Duration) (uspclient.AdmissionReceipt, error) {
	c.calls++
	if c.calls == c.failOn {
		return uspclient.AdmissionReceipt{}, errors.New("synthetic admission failure")
	}
	r, e := c.Client.ShipWithReceipt(m, d)
	if e == nil {
		c.receipts <- r
	}
	return r, e
}

// Keep the same admission fault injection when testing an older Ship caller.
func (c *trackingACKClient) Ship(m *protocol.DataMessage, d time.Duration) error {
	_, err := c.ShipWithReceipt(m, d)
	return err
}

// A real compressed USP websocket peer. Tests explicitly decide which frames
// the proxy accepts; admission and network transmission alone never ACK them.
func ackClient(t *testing.T) (*uspclient.Client, <-chan ackFrame) {
	t.Helper()
	frames := make(chan ackFrame, 2048)
	var mu sync.Mutex
	var conns []*websocket.Conn
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		mu.Lock()
		conns = append(conns, c)
		mu.Unlock()
		var h protocol.ConnectionHeader
		if c.ReadJSON(&h) != nil {
			return
		}
		if c.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageREADY}) != nil {
			return
		}
		if c.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageFLOW, WindowSize: 1024}) != nil {
			return
		}
		for {
			_, raw, err := c.ReadMessage()
			if err != nil {
				return
			}
			z, err := gzip.NewReader(bytes.NewReader(raw))
			if err != nil {
				return
			}
			var msg protocol.DataMessage
			err = msgpack.NewDecoder(z).Decode(&msg)
			z.Close()
			if err != nil {
				return
			}
			frames <- ackFrame{c, msg}
		}
	}))
	c, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		Identity: uspclient.Identity{Oid: "00000000-0000-4000-8000-000000000001", InstallationKey: "synthetic-only"},
		DestURL:  "ws" + strings.TrimPrefix(server.URL, "http") + "/usp", Platform: "text", IsCompressed: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		c.Close()
		mu.Lock()
		for _, c := range conns {
			c.Close()
		}
		mu.Unlock()
		server.Close()
	})
	return c, frames
}

func nextACKFrame(t *testing.T, frames <-chan ackFrame) ackFrame {
	t.Helper()
	select {
	case f := <-frames:
		return f
	case <-time.After(5 * time.Second):
		t.Fatal("no USP frame")
		return ackFrame{}
	}
}
func acceptFrame(t *testing.T, f ackFrame) {
	t.Helper()
	require.NoError(t, f.conn.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageACK, SeqNum: f.msg.SeqNum}))
}
func pollAsync(a *Office365Adapter, s *blobServer) <-chan bool {
	done := make(chan bool, 1)
	go func() { done <- a.pollOnce(s.listURL(), &failureState{}) }()
	return done
}
func pollResult(t *testing.T, done <-chan bool) bool {
	t.Helper()
	select {
	case ok := <-done:
		return ok
	case <-time.After(5 * time.Second):
		t.Fatal("poll did not stop")
		return false
	}
}

func TestConfirmedACKReplacementReplay(t *testing.T) {
	for _, idless := range []bool{false, true} {
		t.Run(map[bool]string{false: "identified", true: "idless"}[idless], func(t *testing.T) {
			srv := newBlobServer(t)
			raw := eventN("a", 1)
			if idless {
				raw = event("", "2026-10-28T12:00:00")
			}
			srv.add("blob-1", "", raw)
			dd := newFakeBatchDeduper()
			c, frames := ackClient(t)
			a, _ := newBatchAdapter(t, srv, dd, c)
			done := pollAsync(a, srv)
			first := nextACKFrame(t, frames)
			require.Equal(t, raw, first.msg.TextPayload)
			require.Never(t, func() bool { dd.mu.Lock(); defer dd.mu.Unlock(); return len(dd.addMany) != 0 }, 80*time.Millisecond, 5*time.Millisecond)
			// Abrupt client loss, with no adapter drain, models a crashed instance.
			_, err := c.Close()
			require.NoError(t, err)
			require.False(t, pollResult(t, done))
			require.False(t, dd.has("blob-1"))
			require.False(t, dd.has("a-0001"))
			replacement, replayed := ackClient(t)
			b, _ := newBatchAdapter(t, srv, dd, replacement)
			done = pollAsync(b, srv)
			f := nextACKFrame(t, replayed)
			require.Equal(t, raw, f.msg.TextPayload)
			acceptFrame(t, f)
			require.True(t, pollResult(t, done))
			require.True(t, dd.has("blob-1"))
			if !idless {
				require.True(t, dd.has("a-0001"))
			}
			require.True(t, b.pollOnce(srv.listURL(), &failureState{}))
			select {
			case <-replayed:
				t.Fatal("confirmed blob replayed")
			default:
			}
			// Event dedup also survives a new blob containing the same event.
			if !idless {
				srv.add("blob-2", "", raw)
				require.True(t, b.pollOnce(srv.listURL(), &failureState{}))
				select {
				case <-replayed:
					t.Fatal("confirmed event replayed")
				default:
				}
			}
		})
	}
}

func TestConfirmedACKPartialAndShutdown(t *testing.T) {
	srv := newBlobServer(t)
	srv.add("blob-1", "", eventN("a", 1), event("", ""), eventN("a", 2))
	dd := newFakeBatchDeduper()
	c, frames := ackClient(t)
	tracked := &trackingACKClient{Client: c, receipts: make(chan uspclient.AdmissionReceipt, 3)}
	a, _ := newBatchAdapter(t, srv, dd, tracked)
	done := pollAsync(a, srv)
	first := nextACKFrame(t, frames)
	nextACKFrame(t, frames)
	nextACKFrame(t, frames)
	acceptFrame(t, first)
	receipt := <-tracked.receipts
	require.Eventually(t, func() bool { return c.ObserveReceipt(receipt).Frame == uspclient.ReceiptAcknowledged }, time.Second, time.Millisecond)
	// Cancellation must release the wait without marking an unconfirmed tail.
	a.cancelStop()
	require.False(t, pollResult(t, done))
	require.True(t, dd.has("a-0001"))
	require.False(t, dd.has("a-0002"))
	require.False(t, dd.has("blob-1"))
	replacement, replay := ackClient(t)
	b, _ := newBatchAdapter(t, srv, dd, replacement)
	done = pollAsync(b, srv)
	f := nextACKFrame(t, replay)
	require.Empty(t, gjson.Get(f.msg.TextPayload, "Id").String())
	f = nextACKFrame(t, replay)
	require.Equal(t, eventN("a", 2), f.msg.TextPayload)
	acceptFrame(t, f)
	require.True(t, pollResult(t, done))
	require.True(t, dd.has("blob-1"))
}
func TestConfirmedACKTimeout(t *testing.T) {
	srv := newBlobServer(t)
	srv.add("blob-1", "", eventN("a", 1))
	dd := newFakeBatchDeduper()
	c, frames := ackClient(t)
	a, _ := newBatchAdapter(t, srv, dd, c)
	a.conf.confirmationTimeout = 200 * time.Millisecond
	done := pollAsync(a, srv)
	nextACKFrame(t, frames)
	require.False(t, pollResult(t, done))
	require.False(t, dd.has("a-0001"))
	require.False(t, dd.has("blob-1"))
}

func TestConfirmedACKIgnoresLegacyMarkers(t *testing.T) {
	srv := newBlobServer(t)
	srv.add("blob-1", "", eventN("a", 1))
	dd := newFakeBatchDeduper()
	dd.AddMany([]utils.DedupeKey{{Key: "blob-1"}, {Key: "a-0001"}})
	c, frames := ackClient(t)
	a, _ := newBatchAdapter(t, srv, dd, c)
	done := pollAsync(a, srv)
	f := nextACKFrame(t, frames)
	acceptFrame(t, f)
	require.True(t, pollResult(t, done))
	require.True(t, dd.has("blob-1"))
	require.True(t, dd.has("a-0001"))
}

func TestConfirmedACKPartialAdmissionFailure(t *testing.T) {
	srv := newBlobServer(t)
	srv.add("blob-1", "", eventN("a", 1), eventN("a", 2), eventN("a", 3))
	dd := newFakeBatchDeduper()
	c, frames := ackClient(t)
	tracked := &trackingACKClient{Client: c, receipts: make(chan uspclient.AdmissionReceipt, 3), failOn: 3}
	a, _ := newBatchAdapter(t, srv, dd, tracked)
	done := pollAsync(a, srv)
	first := nextACKFrame(t, frames)
	nextACKFrame(t, frames)
	acceptFrame(t, first)
	receipt := <-tracked.receipts
	require.Eventually(t, func() bool { return c.ObserveReceipt(receipt).Frame == uspclient.ReceiptAcknowledged }, time.Second, time.Millisecond)
	a.cancelStop()
	require.False(t, pollResult(t, done))
	require.True(t, dd.has("a-0001"))
	require.False(t, dd.has("a-0002"))
	require.False(t, dd.has("a-0003"))
	require.False(t, dd.has("blob-1"))
	replacement, replay := ackClient(t)
	b, _ := newBatchAdapter(t, srv, dd, replacement)
	done = pollAsync(b, srv)
	require.Equal(t, eventN("a", 2), nextACKFrame(t, replay).msg.TextPayload)
	f := nextACKFrame(t, replay)
	require.Equal(t, eventN("a", 3), f.msg.TextPayload)
	acceptFrame(t, f)
	require.True(t, pollResult(t, done))
	require.True(t, dd.has("blob-1"))
}
