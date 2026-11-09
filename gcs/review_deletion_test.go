package usp_gcs

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// A local mutable object store. The first key can change or disappear after
// download; a second valid key must still be deleted and collection must return.
type objectRaceState struct {
	mu       sync.Mutex
	first    int // 0: removed, 1: downloaded original, 2: replacement
	nextLive bool
}

func raceAdapter(t *testing.T, sink acktrack.SDKClient, opts uspclient.ClientOptions) (*GCSAdapter, *objectRaceState, <-chan string) {
	state := &objectRaceState{first: 1, nextLive: true}
	deleted := make(chan string, 8)
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state.mu.Lock()
		defer state.mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/o") {
			keys := []string{}
			if state.first != 0 {
				keys = append(keys, "first.log")
			}
			if state.nextLive {
				keys = append(keys, "next.log")
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"items":[`)
			for i, key := range keys {
				if i > 0 {
					fmt.Fprint(w, ",")
				}
				generation := 1
				if key == "first.log" {
					generation = state.first
				}
				fmt.Fprintf(w, `{"name":%q,"size":"12","generation":"%d"}`, key, generation)
			}
			fmt.Fprint(w, `]}`)
			return
		}
		key := path.Base(r.URL.Path)
		if r.Method == "DELETE" {
			if key == "first.log" && state.first == 0 {
				w.WriteHeader(404)
				return
			}
			if got := r.URL.Query().Get("generation"); got != "" {
				t.Errorf("delete targets generation %q instead of the live object", got)
			}
			expected := "1"
			if key == "first.log" {
				expected = fmt.Sprint(state.first)
			}
			if r.URL.Query().Get("ifGenerationMatch") != expected {
				w.WriteHeader(412)
				return
			}
			if key == "first.log" {
				state.first = 0
			} else {
				state.nextLive = false
			}
			deleted <- key
			w.WriteHeader(204)
			return
		}
		expected := "1"
		if key == "first.log" {
			expected = fmt.Sprint(state.first)
		}
		if got := r.URL.Query().Get("generation"); got != expected {
			t.Errorf("download generation = %q, want %q", got, expected)
			w.WriteHeader(400)
			return
		}
		w.Header().Set("Content-Length", "12")
		w.Header().Set("Content-Range", "bytes 0-11/12")
		fmt.Fprint(w, "first\nother\n")
	}))
	t.Cleanup(store.Close)
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(store.URL), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	a := &GCSAdapter{ctx: context.Background(), conf: GCSConfig{BucketName: "test", ParallelFetch: 2, ClientOptions: opts}, uspClient: sink, client: client, bucket: client.Bucket("test")}
	return a, state, deleted
}

func TestDeleteRaceContinuesToNextObject(t *testing.T) {
	for _, race := range []string{"replacement", "external_deletion"} {
		t.Run(race, func(t *testing.T) {
			sink, ack, received, opts := objectSink(t)
			var errorsReported atomic.Int32
			opts.OnError = func(error) { errorsReported.Add(1) }
			a, state, deleted := raceAdapter(t, sink, opts)
			done := make(chan error, 1)
			collect := func() { go func() { _, err := a.lookForFiles(); done <- err }() }
			defer func() { atomic.StoreUint32(&a.isStop, 1); sink.BeginDrain() }()
			awaitAdmission := func(want uint64) {
				t.Helper()
				select {
				case n := <-received:
					require.Equal(t, want, n)
				case <-time.After(objectTestTimeout):
					t.Fatal("object was not admitted")
				}
			}
			awaitCollection := func() {
				t.Helper()
				select {
				case err := <-done:
					require.NoError(t, err, "normal deletion race must not stop collection")
				case <-time.After(objectTestTimeout):
					t.Fatal("collection stuck")
				}
			}
			collect()
			awaitAdmission(1)
			awaitAdmission(2)
			select {
			case key := <-deleted:
				t.Fatalf("deleted before ACK: %s", key)
			case <-time.After(50 * time.Millisecond):
			}
			state.mu.Lock()
			if race == "replacement" {
				state.first = 2
			} else {
				state.first = 0
			}
			state.mu.Unlock()
			ack(2)
			awaitCollection()
			select {
			case key := <-deleted:
				require.Equal(t, "next.log", key)
			default:
				t.Fatal("next valid object was not deleted")
			}
			select {
			case key := <-deleted:
				t.Fatalf("raced object was deleted: %s", key)
			default:
			}
			require.Nil(t, a.deleteErr, "404/412 must not become sticky")
			require.Zero(t, errorsReported.Load(), "normal deletion races must not report fatal source errors")
			state.mu.Lock()
			nextLive, first := state.nextLive, state.first
			state.mu.Unlock()
			require.False(t, nextLive)
			if race == "replacement" {
				require.Equal(t, 2, first, "replacement must remain for relisting")
				collect()
				awaitAdmission(3)
				ack(3)
				awaitCollection()
				select {
				case key := <-deleted:
					require.Equal(t, "first.log", key)
				default:
					t.Fatal("replacement was not relisted and delivered")
				}
			} else {
				require.Zero(t, first)
				collect()
				awaitCollection()
			}
		})
	}
}

func TestVersionedLiveObjectDeleteRequest(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	a, deleted := objectAdapter(t, sink, opts, "test.log", false)
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
		require.NoError(t, err)
	case <-time.After(objectTestTimeout):
		t.Fatal("delete stuck")
	}
	select {
	case <-deleted:
	default:
		t.Fatal("live object was not deleted")
	}
}
