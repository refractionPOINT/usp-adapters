package usp_gcs

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/api/option"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func objectSink(t testing.TB) (*uspclient.Client, func(uint64), <-chan uint64, uspclient.ClientOptions) {
	t.Helper()
	received := make(chan uint64, 128)
	var mu sync.Mutex
	var ws *websocket.Conn
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer c.Close()
		if c.ReadJSON(&protocol.ConnectionHeader{}) != nil {
			return
		}
		mu.Lock()
		ws = c
		_ = c.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageREADY})
		_ = c.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageFLOW, WindowSize: 128})
		mu.Unlock()
		for {
			_, raw, e := c.ReadMessage()
			if e != nil {
				return
			}
			var m protocol.DataMessage
			if msgpack.Unmarshal(raw, &m) == nil {
				if string(m.BundlePayload) != "first\nother\n" {
					t.Error("object records must stay together in their acknowledged bundle")
				}
				received <- m.SeqNum
			}
		}
	}))
	t.Cleanup(server.Close)
	opts := uspclient.ClientOptions{Identity: uspclient.Identity{Oid: uuid.NewString(), InstallationKey: "synthetic"}, DestURL: "ws" + strings.TrimPrefix(server.URL, "http"), Hostname: "test", Platform: "text", Architecture: "usp_adapter", OnError: func(e error) { t.Log(e) }, OnWarning: func(s string) { t.Log(s) }, DebugLog: func(string) {}}
	c, e := uspclient.NewClient(context.Background(), opts)
	require.NoError(t, e)
	t.Cleanup(func() { _, _ = c.Close() })
	ack := func(n uint64) {
		mu.Lock()
		defer mu.Unlock()
		require.NoError(t, ws.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageACK, SeqNum: n}))
	}
	return c, ack, received, opts
}

func objectAdapter(t *testing.T, sink acktrack.SDKClient, opts uspclient.ClientOptions, filename string, readFailure bool) (*GCSAdapter, <-chan struct{}) {
	t.Helper()
	deleted := make(chan struct{}, 10)
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		switch r.Method {
		case "DELETE":
			if r.URL.Query().Get("generation") != "" || r.URL.Query().Get("ifGenerationMatch") != "1" {
				t.Error("delete must condition the live object, without a generation delete target")
			}
			if filename == "cannot-delete.log" {
				w.WriteHeader(403)
				return
			}
			deleted <- struct{}{}
			w.WriteHeader(http.StatusNoContent)
		case "GET":
			if strings.HasSuffix(r.URL.Path, "/o") {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"items":[{"name":%q,"size":"12","generation":"1"}]}`, filename)
			} else {
				if readFailure {
					w.WriteHeader(403)
					return
				}
				fmt.Fprint(w, "first\nother\n")
			}
		default:
			t.Errorf("unexpected method: %s", r.Method)
		}
	}))
	t.Cleanup(store.Close)
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(store.URL), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	a := &GCSAdapter{ctx: context.Background(), conf: GCSConfig{BucketName: "test", ParallelFetch: 1, ClientOptions: opts}, uspClient: sink, client: client, bucket: client.Bucket("test")}
	return a, deleted
}

func TestObjectDeletionWaitsForDelivery(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	a, deleted := objectAdapter(t, sink, opts, "test.log", false)
	done := make(chan error, 1)
	go func() { _, err := a.lookForFiles(); done <- err }()
	select {
	case <-received:
	case <-time.After(objectTestTimeout):
		t.Fatal("no admission")
	}
	select {
	case <-deleted:
		t.Fatal("source deleted before delivery ACK")
	case <-time.After(100 * time.Millisecond):
	}
	ack(1)
	select {
	case <-deleted:
	case <-time.After(objectTestTimeout):
		t.Fatal("source not deleted after ACK")
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(objectTestTimeout):
		t.Fatal("collector stuck")
	}
}

func TestObjectRetainedOnFailure(t *testing.T) {
	for _, mode := range []string{"read", "decode", "ship"} {
		t.Run(mode, func(t *testing.T) {
			sink, _, _, opts := objectSink(t)
			filename := "test.log"
			if mode == "decode" {
				filename = "test.parquet"
			}
			a, deleted := objectAdapter(t, sink, opts, filename, mode == "read")
			if mode == "ship" {
				sink.BeginDrain()
			}
			_, err := a.lookForFiles()
			require.NoError(t, err)
			select {
			case <-deleted:
				t.Fatal("failed object deleted")
			default:
			}
		})
	}
}

func TestOneTimeLoadRetainsObject(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	a, deleted := objectAdapter(t, sink, opts, "test.log", false)
	a.conf.IsOneTimeLoad = true
	_, err := a.lookForFiles()
	require.NoError(t, err)
	select {
	case <-received:
	case <-time.After(objectTestTimeout):
		t.Fatal("no admission")
	}
	ack(1)
	select {
	case <-deleted:
		t.Fatal("one time load deleted source")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestCloseFlushesConfirmedDeletion(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	a, deleted := objectAdapter(t, sink, opts, "test.log", false)
	a.wg.Add(1)
	go func() { defer a.wg.Done(); _, _ = a.lookForFiles() }()
	select {
	case <-received:
	case <-time.After(objectTestTimeout):
		t.Fatal("no admission")
	}
	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	// BeginDrain has sealed admission; the collector must join without waiting
	// for an ACK, so the final Drain can complete the pending source deletion.
	require.Eventually(t, func() bool { return atomic.LoadUint32(&a.isStop) == 1 }, objectTestTimeout, time.Millisecond)
	select {
	case <-deleted:
		t.Fatal("Close deleted unconfirmed object")
	case <-time.After(50 * time.Millisecond):
	}
	ack(1)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(objectTestTimeout):
		t.Fatal("Close stuck")
	}
	select {
	case <-deleted:
	default:
		t.Fatal("Close failed to delete confirmed object")
	}
	require.NoError(t, a.Close())
}

type failedDrainSink struct{ acktrack.SDKClient }

func (s failedDrainSink) Drain(time.Duration) error { return errors.New("synthetic drain timeout") }
func TestCloseRetainsUnconfirmedObject(t *testing.T) {
	sink, _, received, opts := objectSink(t)
	a, deleted := objectAdapter(t, failedDrainSink{sink}, opts, "test.log", false)
	a.wg.Add(1)
	go func() { defer a.wg.Done(); _, _ = a.lookForFiles() }()
	select {
	case <-received:
	case <-time.After(objectTestTimeout):
		t.Fatal("no admission")
	}
	require.Error(t, a.Close())
	select {
	case <-deleted:
		t.Fatal("Close deleted unconfirmed object")
	default:
	}
}

func TestDeleteFailureStopsCollection(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	a, deleted := objectAdapter(t, sink, opts, "cannot-delete.log", false)
	done := make(chan error, 1)
	go func() { _, err := a.lookForFiles(); done <- err }()
	select {
	case <-received:
	case <-time.After(objectTestTimeout):
		t.Fatal("no admission")
	}
	ack(1)
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(objectTestTimeout):
		t.Fatal("delete failure did not stop collector")
	}
	require.Error(t, a.deleteErr, "other source errors must remain sticky")
	require.ErrorIs(t, a.confirmDelete(), a.deleteErr, "later confirmation must preserve the sticky failure")
	select {
	case <-deleted:
		t.Fatal("failed delete removed object")
	default:
	}
}

func batchAdapter(t testing.TB, sink acktrack.SDKClient, opts uspclient.ClientOptions, count int, onDelete ...func()) (*GCSAdapter, <-chan string) {
	return batchAdapterWithPages(t, sink, opts, count, 0, onDelete...)
}
func batchAdapterWithPages(t testing.TB, sink acktrack.SDKClient, opts uspclient.ClientOptions, count, pageSize int, onDelete ...func()) (*GCSAdapter, <-chan string) {
	t.Helper()
	deleted := make(chan string, 128)
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			require.Empty(t, r.URL.Query().Get("generation"))
			require.Equal(t, "1", r.URL.Query().Get("ifGenerationMatch"))
			if len(onDelete) > 0 {
				onDelete[0]()
			}
			deleted <- path.Base(r.URL.Path)
			w.WriteHeader(204)
			return
		}

		if strings.HasSuffix(r.URL.Path, "/o") {
			first, last := 1, count
			if token := r.URL.Query().Get("pageToken"); token != "" {
				_, _ = fmt.Sscanf(token, "%d", &first)
			}
			if pageSize > 0 {
				last = min(count, first+pageSize-1)
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"items":[`)
			for i := first; i <= last; i++ {
				if i > first {
					fmt.Fprint(w, ",")
				}
				fmt.Fprintf(w, `{"name":"test-%02d.log","size":"12","generation":"1"}`, i)
			}
			if last < count {
				fmt.Fprintf(w, `],"nextPageToken":"%d"}`, last+1)
			} else {
				fmt.Fprint(w, `]}`)
			}
			return
		}
		w.Header().Set("ETag", `"original"`)
		w.Header().Set("Content-Length", "12")
		w.Header().Set("Content-Range", "bytes 0-11/12")
		fmt.Fprint(w, "first\nother\n")
	}))
	t.Cleanup(store.Close)
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(store.URL), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	a := &GCSAdapter{ctx: context.Background(), conf: GCSConfig{BucketName: "test", ParallelFetch: 2, ClientOptions: opts}, uspClient: sink, client: client, bucket: client.Bucket("test")}
	return a, deleted
}

func TestBoundedBatchPartialACK(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	observedSink := &countingObjectSink{SDKClient: sink}
	snapshotCounts := make(chan uint64, 128)
	a, deleted := batchAdapter(t, observedSink, opts, 40, func() { snapshotCounts <- observedSink.snapshots.Load() })
	done := make(chan error, 1)
	go func() { _, err := a.lookForFiles(); done <- err }()
	// Ensure failures cannot leave the collector polling after test cleanup.
	defer func() {
		atomic.StoreUint32(&a.isStop, 1)
		sink.BeginDrain()
		select {
		case <-done:
		case <-time.After(objectTestTimeout):
		}
	}()
	receive := func(first, last int) {
		t.Helper()
		for i := first; i <= last; i++ {
			select {
			case n := <-received:
				require.Equal(t, uint64(i), n)
			case <-time.After(objectTestTimeout):
				t.Fatalf("admission %d blocked before the 32-object limit", i)
			}
		}
	}
	removals := func(first, last int) {
		t.Helper()
		var observation uint64
		for i := first; i <= last; i++ {
			select {
			case key := <-deleted:
				require.Equal(t, fmt.Sprintf("test-%02d.log", i), key)
				count := <-snapshotCounts
				if i == first {
					observation = count
				} else {
					require.Equal(t, observation, count, "one SDK snapshot must cover the entire confirmed prefix")
				}
			case <-time.After(objectTestTimeout):
				t.Fatalf("confirmed object %d was not deleted", i)
			}
		}
	}
	receive(1, 32)
	select {
	case n := <-received:
		t.Fatalf("unbounded admission %d", n)
	case <-time.After(75 * time.Millisecond):
	}
	select {
	case key := <-deleted:
		t.Fatalf("unconfirmed deletion %s", key)
	default:
	}
	ack(16)
	removals(1, 16)
	receive(33, 40)
	select {
	case key := <-deleted:
		t.Fatalf("deleted beyond partial ACK: %s", key)
	case <-time.After(75 * time.Millisecond):
	}
	ack(32)
	removals(17, 32)
	select {
	case key := <-deleted:
		t.Fatalf("deleted beyond second partial ACK: %s", key)
	case <-time.After(50 * time.Millisecond):
	}
	ack(40)
	removals(33, 40)
	select {
	case err := <-done:
		require.NoError(t, err)
		done <- err
	case <-time.After(objectTestTimeout):
		t.Fatal("end did not flush pending objects")
	}
}

type countingObjectSink struct {
	acktrack.SDKClient
	snapshots atomic.Uint64
}

func (s *countingObjectSink) GetUnsent() ([]*protocol.DataMessage, error) {
	s.snapshots.Add(1)
	return s.SDKClient.GetUnsent()
}

func TestCloseFlushesBoundedBatch(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	a, deleted := batchAdapter(t, sink, opts, 40)
	a.wg.Add(1)
	go func() { defer a.wg.Done(); _, _ = a.lookForFiles() }()
	for i := 1; i <= 32; i++ {
		select {
		case n := <-received:
			require.Equal(t, uint64(i), n)
		case <-time.After(objectTestTimeout):
			t.Fatal("window did not fill")
		}
	}
	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	require.Eventually(t, func() bool { return atomic.LoadUint32(&a.isStop) == 1 }, objectTestTimeout, time.Millisecond)
	select {
	case <-deleted:
		t.Fatal("Close deleted before ACK")
	case <-time.After(50 * time.Millisecond):
	}
	ack(32)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(objectTestTimeout):
		t.Fatal("Close stuck")
	}
	for i := 1; i <= 32; i++ {
		select {
		case key := <-deleted:
			require.Equal(t, fmt.Sprintf("test-%02d.log", i), key)
		default:
			t.Fatal("missing final prefix deletion")
		}
	}
	select {
	case n := <-received:
		t.Fatalf("admitted after stop: %d", n)
	default:
	}
}

func TestCloseRetainsUnconfirmedBatchSuffix(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	a, deleted := batchAdapter(t, failedDrainSink{sink}, opts, 6)
	a.wg.Add(1)
	go func() { defer a.wg.Done(); _, _ = a.lookForFiles() }()
	for i := 1; i <= 6; i++ {
		select {
		case <-received:
		case <-time.After(objectTestTimeout):
			t.Fatal("batch did not fill")
		}
	}
	ack(3)
	require.Eventually(t, func() bool { pending, err := sink.GetUnsent(); return err == nil && len(pending) == 3 }, objectTestTimeout, time.Millisecond)
	require.Error(t, a.Close())
	for i := 1; i <= 3; i++ {
		select {
		case key := <-deleted:
			require.Equal(t, fmt.Sprintf("test-%02d.log", i), key)
		default:
			t.Fatal("missing confirmed prefix deletion")
		}
	}
	select {
	case key := <-deleted:
		t.Fatalf("unconfirmed suffix deleted: %s", key)
	default:
	}
}

// Local fixtures isolate batching overhead from vendor service latency. Each
// operation delivers 32 objects; the peer ACKs each immediately.
func BenchmarkBatchObjectDelivery(b *testing.B) {
	sink, ack, received, opts := objectSink(b)
	a, deleted := batchAdapter(b, sink, opts, 32)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case seq := <-received:
				ack(seq)
			case <-stop:
				return
			}
		}
	}()
	defer func() { close(stop); <-done }()
	b.SetBytes(32 * 12)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := a.lookForFiles()
		require.NoError(b, err)
		for j := 0; j < 32; j++ {
			<-deleted
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*32), "ns/object")
}

type failedObservationSink struct{ acktrack.SDKClient }

func (s failedObservationSink) GetUnsent() ([]*protocol.DataMessage, error) {
	return nil, errors.New("synthetic observation failure")
}
func TestObservationFailureRetainsBatch(t *testing.T) {
	sink, _, received, opts := objectSink(t)
	a, deleted := batchAdapter(t, failedObservationSink{sink}, opts, 6)
	done := make(chan error, 1)
	go func() { _, err := a.lookForFiles(); done <- err }()
	for i := 1; i <= 6; i++ {
		select {
		case <-received:
		case <-time.After(objectTestTimeout):
			t.Fatal("batch did not fill")
		}
	}
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(objectTestTimeout):
		t.Fatal("failed observation did not stop collection")
	}
	select {
	case key := <-deleted:
		t.Fatalf("observation failure deleted source %s", key)
	default:
	}
}

func TestListingPageFlushesBeforeNextPage(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	a, deleted := batchAdapterWithPages(t, sink, opts, 4, 2)
	done := make(chan error, 1)
	go func() { _, err := a.lookForFiles(); done <- err }()
	defer func() {
		atomic.StoreUint32(&a.isStop, 1)
		sink.BeginDrain()
		select {
		case <-done:
		case <-time.After(objectTestTimeout):
		}
	}()
	for i := 1; i <= 2; i++ {
		select {
		case n := <-received:
			require.Equal(t, uint64(i), n)
		case <-time.After(objectTestTimeout):
			t.Fatal("first page blocked")
		}
	}
	select {
	case n := <-received:
		t.Fatalf("next page admitted before first page flushed: %d", n)
	case <-time.After(75 * time.Millisecond):
	}
	ack(2)
	for i := 1; i <= 2; i++ {
		select {
		case key := <-deleted:
			require.Equal(t, fmt.Sprintf("test-%02d.log", i), key)
		case <-time.After(objectTestTimeout):
			t.Fatal("first page did not flush")
		}
	}
	for i := 3; i <= 4; i++ {
		select {
		case n := <-received:
			require.Equal(t, uint64(i), n)
		case <-time.After(objectTestTimeout):
			t.Fatal("next page blocked")
		}
	}
	ack(4)
	select {
	case err := <-done:
		require.NoError(t, err)
		done <- err
	case <-time.After(objectTestTimeout):
		t.Fatal("end did not flush")
	}
	for i := 3; i <= 4; i++ {
		select {
		case key := <-deleted:
			require.Equal(t, fmt.Sprintf("test-%02d.log", i), key)
		default:
			t.Fatal("last page not flushed")
		}
	}
}

const objectTestTimeout = 10 * time.Second
