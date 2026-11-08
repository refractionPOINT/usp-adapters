package usp_s3

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"github.com/stretchr/testify/require"
)

// A local mutable object store. The first key can change or disappear after
// download; a second valid key must still be deleted and collection must return.
type objectRaceState struct {
	mu       sync.Mutex
	first    int // 0: removed, 1: downloaded original, 2: replacement
	nextLive bool
}

func raceAdapter(t *testing.T, sink acktrack.SDKClient, opts uspclient.ClientOptions) (*S3Adapter, *objectRaceState, <-chan string) {
	state := &objectRaceState{first: 1, nextLive: true}
	deleted := make(chan string, 8)
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state.mu.Lock()
		defer state.mu.Unlock()
		if r.URL.Query().Get("list-type") == "2" {
			keys := []string{}
			if state.first != 0 {
				keys = append(keys, "first.log")
			}
			if state.nextLive {
				keys = append(keys, "next.log")
			}
			fmt.Fprint(w, `<ListBucketResult>`)
			for _, key := range keys {
				fmt.Fprintf(w, `<Contents><Key>%s</Key><Size>12</Size></Contents>`, key)
			}
			fmt.Fprint(w, `</ListBucketResult>`)
			return
		}
		key := path.Base(r.URL.Path)
		if r.Method == "DELETE" {
			if key == "first.log" && state.first == 0 {
				w.WriteHeader(404)
				return
			}
			if got := r.URL.Query().Get("versionId"); got != "" {
				t.Errorf("key delete unexpectedly targets version %q", got)
				w.WriteHeader(400)
				return
			}
			expected := `"generation-1"`
			if key == "first.log" && state.first == 2 {
				expected = `"generation-2"`
			}
			if r.Header.Get("If-Match") != expected {
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
		tag := `"generation-1"`
		if key == "first.log" && state.first == 2 {
			tag = `"generation-2"`
		}
		w.Header().Set("ETag", tag)
		w.Header().Set("x-amz-version-id", "synthetic-version")
		w.Header().Set("Content-Length", "12")
		w.Header().Set("Content-Range", "bytes 0-11/12")
		fmt.Fprint(w, "first\nother\n")
	}))
	t.Cleanup(store.Close)
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(store.URL), S3ForcePathStyle: aws.Bool(true), Credentials: credentials.AnonymousCredentials, MaxRetries: aws.Int(0)}))
	a := &S3Adapter{ctx: context.Background(), conf: S3Config{BucketName: "test", ParallelFetch: 2, ClientOptions: opts}, uspClient: sink, awsS3: s3.New(sess), awsDownloader: s3manager.NewDownloader(sess)}
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
