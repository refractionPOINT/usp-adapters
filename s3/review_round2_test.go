package usp_s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"
)

func TestEmptyRangedObjectContinues(t *testing.T) {
	sink, ack, received, opts := objectSink(t, true)
	deleted := make(chan string, 4)
	var ranged atomic.Bool
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := path.Base(r.URL.Path)
		if r.Method == "DELETE" {
			want := `"valid"`
			if key == "empty.log" {
				want = `"empty"`
			}
			require.Equal(t, want, r.Header.Get("If-Match"))
			deleted <- key
			w.WriteHeader(204)
			return
		}
		if r.URL.Query().Get("list-type") == "2" {
			fmt.Fprint(w, `<ListBucketResult><Contents><Key>empty.log</Key><Size>0</Size><ETag>"empty"</ETag></Contents><Contents><Key>next.log</Key><Size>12</Size><ETag>"valid"</ETag></Contents></ListBucketResult>`)
			return
		}
		if key == "empty.log" {
			ranged.Store(r.Header.Get("Range") != "")
			w.Header().Set("Content-Range", "bytes */0")
			w.WriteHeader(416)
			fmt.Fprint(w, `<Error><Code>InvalidRange</Code></Error>`)
			return
		}
		w.Header().Set("ETag", `"valid"`)
		w.Header().Set("Content-Length", "12")
		w.Header().Set("Content-Range", "bytes 0-11/12")
		fmt.Fprint(w, "first\nother\n")
	}))
	defer store.Close()
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(store.URL), S3ForcePathStyle: aws.Bool(true), Credentials: credentials.AnonymousCredentials, MaxRetries: aws.Int(0)}))
	a := &S3Adapter{ctx: context.Background(), conf: S3Config{BucketName: "test", ParallelFetch: 2, ClientOptions: opts}, uspClient: sink, awsS3: s3.New(sess), awsDownloader: s3manager.NewDownloader(sess)}
	done := make(chan error, 1)
	go func() { _, err := a.lookForFiles(); done <- err }()
	defer func() { atomic.StoreUint32(&a.isStop, 1); sink.BeginDrain() }()
	for i := 1; i <= 2; i++ {
		select {
		case seq := <-received:
			require.Equal(t, uint64(i), seq)
		case <-time.After(objectTestTimeout):
			t.Fatal("objects not admitted")
		}
	}
	select {
	case key := <-deleted:
		t.Fatalf("delete before receipt: %s", key)
	case <-time.After(50 * time.Millisecond):
	}
	require.True(t, ranged.Load(), "fixture must exercise downloader's actual ranged 416 path")
	ack(1)
	select {
	case key := <-deleted:
		require.Equal(t, "empty.log", key)
	case err := <-done:
		t.Fatalf("zero-byte ranged object stopped collection: %v", err)
	case <-time.After(objectTestTimeout):
		t.Fatal("empty object not retired")
	}
	select {
	case key := <-deleted:
		t.Fatalf("valid object deleted before receipt: %s", key)
	case <-time.After(50 * time.Millisecond):
	}
	ack(2)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(objectTestTimeout):
		t.Fatal("collection did not finish")
	}
	select {
	case key := <-deleted:
		require.Equal(t, "next.log", key)
	default:
		t.Fatal("next valid object not deleted")
	}
	require.Nil(t, a.deleteErr)
}

func TestDeleteConcurrencyBoundedAndPartialACK(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	var active, peak atomic.Int32
	started := make(chan struct{}, 64)
	gate := make(chan struct{}, 32)
	observed := &countingObjectSink{SDKClient: sink}
	a, deleted := batchAdapter(t, observed, opts, 32, func() {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		started <- struct{}{}
		<-gate
	})
	done := make(chan error, 1)
	go func() { _, err := a.lookForFiles(); done <- err }()
	defer func() {
		atomic.StoreUint32(&a.isStop, 1)
		sink.BeginDrain()
		close(gate)
		select {
		case <-done:
		case <-time.After(objectTestTimeout):
		}
	}()
	for i := 1; i <= 32; i++ {
		select {
		case <-received:
		case <-time.After(objectTestTimeout):
			t.Fatal("window not filled")
		}
	}
	select {
	case <-started:
		t.Fatal("delete before receipt")
	case <-time.After(50 * time.Millisecond):
	}
	wave := func() {
		t.Helper()
		for i := 0; i < 8; i++ {
			select {
			case <-started:
			case <-time.After(objectTestTimeout):
				t.Fatalf("only %d parallel deletes started; want 8", i)
			}
		}
	}
	release := func() {
		for i := 0; i < 8; i++ {
			gate <- struct{}{}
		}
	}
	removals := func(first, last int) {
		t.Helper()
		want, got := []string{}, []string{}
		for i := first; i <= last; i++ {
			want = append(want, fmt.Sprintf("test-%02d.log", i))
			select {
			case key := <-deleted:
				got = append(got, key)
			case <-time.After(objectTestTimeout):
				t.Fatal("missing completed delete")
			}
		}
		require.ElementsMatch(t, want, got)
	}
	ack(16)
	wave()
	snapshot := observed.snapshots.Load()
	select {
	case <-started:
		t.Fatal("more than 8 deletes in flight")
	case <-time.After(50 * time.Millisecond):
	}
	release()
	wave()
	require.Equal(t, snapshot, observed.snapshots.Load(), "entire confirmed prefix must precede I/O and share one SDK snapshot")
	release()
	removals(1, 16)
	select {
	case <-started:
		t.Fatal("unconfirmed suffix entered cleanup")
	case <-time.After(50 * time.Millisecond):
	}
	ack(32)
	wave()
	release()
	wave()
	release()
	removals(17, 32)
	select {
	case err := <-done:
		require.NoError(t, err)
		done <- err
	case <-time.After(objectTestTimeout):
		t.Fatal("cleanup did not join")
	}
	require.Equal(t, int32(8), peak.Load())
	require.Zero(t, active.Load())
}

// Intercept only locally addressed DELETEs; successful requests still hit the
// fixture. Error responses go through the real AWS SDK response/error parser.
type deleteTestTransport struct {
	base   http.RoundTripper
	before func(*http.Request) *http.Response
}

func (d deleteTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == "DELETE" {
		if response := d.before(r); response != nil {
			return response, nil
		}
	}
	return d.base.RoundTrip(r)
}

func TestConcurrentDeleteErrorsJoinBeforeReturn(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	var active, peak, attempts, reported atomic.Int32
	opts.OnError = func(error) { reported.Add(1) }
	a, deleted := batchAdapter(t, sink, opts, 16)
	started := make(chan struct{}, 32)
	gate := make(chan struct{})
	var release sync.Once
	client := *a.awsS3.Config.HTTPClient
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.Transport = deleteTestTransport{base: base, before: func(r *http.Request) *http.Response {
		n := active.Add(1)
		defer active.Add(-1)
		attempts.Add(1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		started <- struct{}{}
		<-gate
		code, status := "", 0
		switch path.Base(r.URL.Path) {
		case "test-01.log":
			status, code = 403, "AccessDenied"
		case "test-02.log":
			status, code = 404, "NoSuchKey"
		case "test-03.log":
			status, code = 412, "PreconditionFailed"
		}
		if status == 0 {
			return nil
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("<Error><Code>" + code + "</Code></Error>")), Request: r}
	}}
	a.awsS3.Config.HTTPClient = &client
	done := make(chan error, 1)
	go func() { _, err := a.lookForFiles(); done <- err }()
	defer func() { release.Do(func() { close(gate) }); atomic.StoreUint32(&a.isStop, 1); sink.BeginDrain() }()
	for i := 0; i < 16; i++ {
		select {
		case <-received:
		case <-time.After(objectTestTimeout):
			t.Fatal("window did not fill")
		}
	}
	ack(16)
	for i := 0; i < 8; i++ {
		select {
		case <-started:
		case <-time.After(objectTestTimeout):
			t.Fatal("cleanup concurrency did not fill")
		}
	}
	select {
	case err := <-done:
		t.Fatalf("cleanup returned before joining requests: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	release.Do(func() { close(gate) })
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(objectTestTimeout):
		t.Fatal("error cleanup stuck")
	}
	require.Zero(t, active.Load())
	require.Equal(t, int32(16), attempts.Load())
	require.Equal(t, int32(8), peak.Load())
	require.Equal(t, int32(1), reported.Load())
	require.Len(t, a.pendingDeletes, 1, "only the fatal source identity remains; 404/412 and successful identities retire")
	require.Equal(t, uint64(1), a.pendingDeletes[0].marker)
	require.ErrorIs(t, a.confirmDelete(), a.deleteErr)
	require.Equal(t, int32(16), attempts.Load(), "sticky failure must not retry cleanup")
	expectDeletedPrefix(t, deleted, 4, 16)
}

func TestCloseJoinsConcurrentDeleteCleanup(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	gate := make(chan struct{})
	started := make(chan struct{}, 64)
	var release sync.Once
	var active atomic.Int32
	a, deleted := batchAdapter(t, sink, opts, 32, func() { active.Add(1); defer active.Add(-1); started <- struct{}{}; <-gate })
	defer func() { atomic.StoreUint32(&a.isStop, 1); sink.BeginDrain(); release.Do(func() { close(gate) }) }()
	a.wg.Add(1)
	go func() { defer a.wg.Done(); _, _ = a.lookForFiles() }()
	for i := 0; i < 32; i++ {
		select {
		case <-received:
		case <-time.After(objectTestTimeout):
			t.Fatal("window did not fill")
		}
	}
	ack(32)
	for i := 0; i < 8; i++ {
		select {
		case <-started:
		case <-time.After(objectTestTimeout):
			t.Fatal("delete workers did not start")
		}
	}
	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	require.Eventually(t, func() bool { return atomic.LoadUint32(&a.isStop) == 1 }, objectTestTimeout, time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Close returned with active cleanup: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	release.Do(func() { close(gate) })
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(objectTestTimeout):
		t.Fatal("Close did not join cleanup")
	}
	require.Zero(t, active.Load())
	expectDeletedPrefix(t, deleted, 1, 32)
}

// A 20 ms local DELETE response delay models network RTT. Each iteration uses
// the same 32 fully acknowledged identities and actual local SDK DELETE calls.
// The serial reference reproduces the former one-at-a-time removal loop.
func BenchmarkDeleteLatency(b *testing.B) {
	for _, mode := range []string{"serial", "concurrent8"} {
		b.Run(mode, func(b *testing.B) {
			sink, ack, received, opts := objectSink(b)
			a, deleted := batchAdapter(b, sink, opts, 32, func() { time.Sleep(20 * time.Millisecond) })
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
			b.ResetTimer()
			b.StopTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				for i := 1; i <= 32; i++ {
					require.True(b, a.processEvent([]byte("first\nother\n"), false))
					key := fmt.Sprintf("test-%02d.log", i)
					a.pendingDeletes = append(a.pendingDeletes, pendingObjectDelete{marker: a.admitted, dependency: a.lastDependency, remove: func() error { return a.deleteDeliveredObject(key, s3ObjectIdentity{ETag: `"original"`}) }})
				}
				require.Eventually(b, func() bool { pending, err := sink.GetUnsent(); return err == nil && len(pending) == 0 }, objectTestTimeout, time.Millisecond)
				b.StartTimer()
				if mode == "serial" {
					count, err := a.confirmedDeletePrefix()
					require.NoError(b, err)
					require.Equal(b, 32, count)
					for _, entry := range a.pendingDeletes {
						require.NoError(b, entry.remove())
					}
					clear(a.pendingDeletes)
					a.pendingDeletes = nil
				} else {
					require.NoError(b, a.confirmDelete())
				}
				b.StopTimer()
				for i := 0; i < 32; i++ {
					<-deleted
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*32), "ns/object")
		})
	}
}

func TestListedETagCannotBlessUnvalidatedDownload(t *testing.T) {
	for _, mode := range []string{"nonempty_listed_416", "nonempty_response_missing_identity", "empty_missing_listed_etag", "mixed_download_identity"} {
		t.Run(mode, func(t *testing.T) {
			sink, ack, received, opts := objectSink(t)
			deleted := make(chan string, 4)
			var badReads atomic.Int32
			store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				key := path.Base(r.URL.Path)
				if r.Method == "DELETE" {
					if key != "next.log" {
						t.Errorf("unvalidated source was deleted: %s", key)
						w.WriteHeader(403)
						return
					}
					deleted <- key
					w.WriteHeader(204)
					return
				}
				if r.URL.Query().Get("list-type") == "2" {
					size := 0
					if mode == "nonempty_listed_416" {
						size = 12
					}
					tag := `"listed"`
					if mode == "empty_missing_listed_etag" {
						tag = ""
					}
					fmt.Fprintf(w, `<ListBucketResult><Contents><Key>bad.log</Key><Size>%d</Size><ETag>%s</ETag></Contents><Contents><Key>next.log</Key><Size>12</Size></Contents></ListBucketResult>`, size, tag)
					return
				}
				if key == "bad.log" {
					badReads.Add(1)
					switch mode {
					case "nonempty_listed_416", "empty_missing_listed_etag":
						w.Header().Set("Content-Range", "bytes */0")
						w.WriteHeader(416)
						fmt.Fprint(w, `<Error><Code>InvalidRange</Code></Error>`)
						return
					case "mixed_download_identity":
						const part = 5 * 1024 * 1024
						if strings.HasPrefix(r.Header.Get("Range"), "bytes=0-") {
							w.Header().Set("ETag", `"first"`)
							w.Header().Set("Content-Length", fmt.Sprint(part))
							w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", part-1, part+12))
							_, _ = w.Write(bytes.Repeat([]byte{'a'}, part))
							return
						}
						w.Header().Set("ETag", `"other"`)
						w.Header().Set("Content-Length", "12")
						w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", part, part+11, part+12))
						fmt.Fprint(w, "first\nother\n")
						return
					case "nonempty_response_missing_identity":
						// No ETag: a listed zero size cannot bless a nonempty response.
					}
				} else {
					w.Header().Set("ETag", `"valid"`)
				}
				w.Header().Set("Content-Length", "12")
				w.Header().Set("Content-Range", "bytes 0-11/12")
				fmt.Fprint(w, "first\nother\n")
			}))
			defer store.Close()
			sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(store.URL), S3ForcePathStyle: aws.Bool(true), Credentials: credentials.AnonymousCredentials, MaxRetries: aws.Int(0)}))
			a := &S3Adapter{ctx: context.Background(), conf: S3Config{BucketName: "test", ParallelFetch: 2, ClientOptions: opts}, uspClient: sink, awsS3: s3.New(sess), awsDownloader: s3manager.NewDownloader(sess)}
			done := make(chan error, 1)
			go func() { _, err := a.lookForFiles(); done <- err }()
			defer func() { atomic.StoreUint32(&a.isStop, 1); sink.BeginDrain() }()
			select {
			case seq := <-received:
				require.Equal(t, uint64(1), seq)
			case <-time.After(objectTestTimeout):
				t.Fatal("next valid object blocked")
			}
			ack(1)
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(objectTestTimeout):
				t.Fatal("unvalidated object stopped collection")
			}
			select {
			case key := <-deleted:
				require.Equal(t, "next.log", key)
			default:
				t.Fatal("next valid object not deleted")
			}
			select {
			case <-received:
				t.Fatal("unvalidated object admitted")
			default:
			}
			require.Nil(t, a.deleteErr)
			if mode == "mixed_download_identity" {
				require.GreaterOrEqual(t, badReads.Load(), int32(2), "must exercise mismatched ranged responses")
			}
		})
	}
}
