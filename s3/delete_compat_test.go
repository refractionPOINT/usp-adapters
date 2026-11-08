package usp_s3

import (
	"context"
	"fmt"
	"html"
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
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"github.com/stretchr/testify/require"
)

type compatReply struct {
	status        int
	code, message string
}
type compatCounts struct{ conditional, plain atomic.Int32 }

// Both the rejection and the plain DELETE go through the real SDK and local
// HTTP server. Optional blocking lets tests hold eight compatibility workers.
func compatAdapter(t *testing.T, sink acktrack.SDKClient, opts uspclient.ClientOptions, count int, reply compatReply, block func(), plainFailure ...compatReply) (*S3Adapter, *compatCounts, <-chan string) {
	t.Helper()
	counts := &compatCounts{}
	deleted := make(chan string, 128)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			if r.URL.Query().Get("versionId") != "" {
				t.Error("compatibility must preserve key delete semantics")
			}
			if r.Header.Get("If-Match") != "" {
				counts.conditional.Add(1)
				if reply.status == -1 {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
					return
				}
				if reply.status != 0 {
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(reply.status)
					fmt.Fprintf(w, "<Error><Code>%s</Code><Message>%s</Message></Error>", reply.code, html.EscapeString(reply.message))
					return
				}
			} else {
				counts.plain.Add(1)
				if block != nil {
					block()
				}
				if len(plainFailure) > 0 {
					failure := plainFailure[0]
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(failure.status)
					fmt.Fprintf(w, "<Error><Code>%s</Code><Message>%s</Message></Error>", failure.code, html.EscapeString(failure.message))
					return
				}
			}
			deleted <- path.Base(r.URL.Path)
			w.WriteHeader(204)
			return
		}
		if r.URL.Query().Get("list-type") == "2" {
			fmt.Fprint(w, `<ListBucketResult>`)
			for i := 1; i <= count; i++ {
				fmt.Fprintf(w, `<Contents><Key>test-%02d.log</Key><Size>12</Size><ETag>"original"</ETag></Contents>`, i)
			}
			fmt.Fprint(w, `</ListBucketResult>`)
			return
		}
		w.Header().Set("ETag", `"original"`)
		w.Header().Set("Content-Length", "12")
		w.Header().Set("Content-Range", "bytes 0-11/12")
		fmt.Fprint(w, "first\nother\n")
	}))
	t.Cleanup(server.Close)
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(server.URL), S3ForcePathStyle: aws.Bool(true), Credentials: credentials.AnonymousCredentials, MaxRetries: aws.Int(0)}))
	a := &S3Adapter{ctx: context.Background(), conf: S3Config{BucketName: "test", ParallelFetch: 2, ClientOptions: opts}, uspClient: sink, awsS3: s3.New(sess), awsDownloader: s3manager.NewDownloader(sess)}
	return a, counts, deleted
}

func TestUnsupportedConditionalDeleteCompatibility(t *testing.T) {
	for _, reply := range []compatReply{
		{501, "NotImplemented", "A header you provided implies functionality that is not implemented"},
		{501, "", ""},
		{400, "NotImplemented", "Conditional delete is not implemented"},
		{400, "InvalidRequest", "If-Match header is not supported"},
		{400, "InvalidArgument", "Unsupported header: If-Match"},
		{400, "UnsupportedHeader", "If-Match"},
		{405, "NotSupported", "Conditional DELETE is not supported"},
	} {
		t.Run(fmt.Sprintf("%d_%s_%s", reply.status, reply.code, reply.message), func(t *testing.T) {
			sink, ack, received, opts := objectSink(t)
			var warnings []string
			var reported atomic.Int32
			opts.OnWarning = func(message string) { warnings = append(warnings, message) }
			opts.OnError = func(error) { reported.Add(1) }
			a, counts, deleted := compatAdapter(t, sink, opts, 2, reply, nil)
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
			// Neither the rejected conditional request nor the plain request may run
			// before their source record's receipt, including on later objects.
			select {
			case <-deleted:
				t.Fatal("deleted before receipt")
			case <-time.After(50 * time.Millisecond):
			}
			require.Zero(t, counts.conditional.Load())
			require.Zero(t, counts.plain.Load())
			ack(1)
			select {
			case key := <-deleted:
				require.Equal(t, "test-01.log", key)
			case err := <-done:
				t.Fatalf("unsupported conditional DELETE stopped collection: %v", err)
			case <-time.After(objectTestTimeout):
				t.Fatal("fallback did not delete")
			}
			select {
			case key := <-deleted:
				t.Fatalf("next object deleted before its receipt: %s", key)
			case <-time.After(50 * time.Millisecond):
			}
			require.Equal(t, int32(1), counts.plain.Load())
			ack(2)
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(objectTestTimeout):
				t.Fatal("next object blocked")
			}
			expectDeletedPrefix(t, deleted, 2, 2)
			require.Zero(t, reported.Load())
			require.Nil(t, a.deleteErr)
			require.Len(t, warnings, 2)
			require.Contains(t, warnings[0], "count=1 total=1")
			require.Contains(t, warnings[1], "count=1 total=2")
			for _, warning := range warnings {
				require.Contains(t, warning, "replacement protection is reduced")
			}
		})
	}
}

func TestConditionalDeleteDoesNotFallbackForOtherFailures(t *testing.T) {
	for _, tc := range []struct {
		reply compatReply
		race  bool
	}{
		{compatReply{412, "PreconditionFailed", "If-Match precondition failed"}, true},
		{compatReply{412, "NotImplemented", "If-Match is unsupported"}, true},
		{compatReply{404, "NotImplemented", "If-Match is unsupported"}, true},
		{compatReply{-1, "transport", ""}, false},
		{compatReply{404, "NoSuchKey", "gone"}, true},
		{compatReply{403, "AccessDenied", "If-Match is not supported by your permissions"}, false},
		{compatReply{403, "NotImplemented", "If-Match is not supported"}, false},
		{compatReply{500, "InternalError", "If-Match processing failed"}, false},
		{compatReply{503, "NotImplemented", "If-Match not implemented"}, false},
		{compatReply{400, "InvalidArgument", "Invalid If-Match value"}, false},
		{compatReply{400, "InvalidRequest", "Unknown header: Authorization"}, false},
		{compatReply{400, "AccessDenied", "If-Match is unsupported"}, false},
		{compatReply{0, "", ""}, false},
	} {
		t.Run(fmt.Sprintf("%d_%s", tc.reply.status, tc.reply.code), func(t *testing.T) {
			sink, ack, received, opts := objectSink(t)
			var warnings []string
			var reported atomic.Int32
			opts.OnWarning = func(message string) { warnings = append(warnings, message) }
			opts.OnError = func(error) { reported.Add(1) }
			a, counts, deleted := compatAdapter(t, sink, opts, 1, tc.reply, nil)
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
				if tc.race || tc.reply.status == 0 {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			case <-time.After(objectTestTimeout):
				t.Fatal("collection stuck")
			}
			require.Zero(t, counts.plain.Load())
			require.Empty(t, warnings)
			if tc.reply.status == -1 {
				require.GreaterOrEqual(t, counts.conditional.Load(), int32(1))
			} else {
				require.Equal(t, int32(1), counts.conditional.Load())
			}
			if tc.reply.status == 0 {
				expectDeletedPrefix(t, deleted, 1, 1)
			} else {
				select {
				case key := <-deleted:
					t.Fatalf("failed conditional request removed %s", key)
				default:
				}
			}
			if tc.race || tc.reply.status == 0 {
				require.Zero(t, reported.Load())
			} else {
				require.Equal(t, int32(1), reported.Load())
			}
		})
	}
}

func TestConcurrentCompatibilityFallbackIsCounted(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	var warnings []string
	var reported atomic.Int32
	// This callback intentionally uses an ordinary slice: compatibility warnings
	// must be delivered by the collector after the cleanup workers have joined.
	opts.OnWarning = func(message string) { warnings = append(warnings, message) }
	opts.OnError = func(error) { reported.Add(1) }
	gate := make(chan struct{})
	started := make(chan struct{}, 128)
	var release sync.Once
	a, counts, deleted := compatAdapter(t, sink, opts, 32, compatReply{501, "NotImplemented", ""}, func() { started <- struct{}{}; <-gate })
	done := make(chan error, 1)
	defer func() { release.Do(func() { close(gate) }); atomic.StoreUint32(&a.isStop, 1); sink.BeginDrain() }()
	collect := func() { go func() { _, err := a.lookForFiles(); done <- err }() }
	receive := func() {
		t.Helper()
		for i := 0; i < 32; i++ {
			select {
			case <-received:
			case <-time.After(objectTestTimeout):
				t.Fatal("window not filled")
			}
		}
	}
	collect()
	receive()
	select {
	case <-started:
		t.Fatal("compatibility fallback before receipt")
	case <-time.After(50 * time.Millisecond):
	}
	ack(32)
	for i := 0; i < 8; i++ {
		select {
		case <-started:
		case err := <-done:
			t.Fatalf("compatibility cleanup stopped: %v", err)
		case <-time.After(objectTestTimeout):
			t.Fatal("eight workers not started")
		}
	}
	require.Equal(t, int32(8), counts.plain.Load())
	select {
	case <-started:
		t.Fatal("more than eight workers")
	case <-time.After(50 * time.Millisecond):
	}
	release.Do(func() { close(gate) })
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(objectTestTimeout):
		t.Fatal("cleanup did not join")
	}
	expectDeletedPrefix(t, deleted, 1, 32)
	collect()
	receive()
	ack(64)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(objectTestTimeout):
		t.Fatal("next collection failed")
	}
	expectDeletedPrefix(t, deleted, 1, 32)
	require.Zero(t, reported.Load())
	require.Equal(t, int32(64), counts.plain.Load())
	require.Equal(t, int32(64), counts.conditional.Load(), "each fallback needs its own explicit unsupported response")
	require.Len(t, warnings, 2)
	require.Contains(t, warnings[0], "count=32 total=32")
	require.Contains(t, warnings[1], "count=32 total=64")
}

func TestCompatibilityFallbackPlainFailureRemainsSticky(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	var warnings []string
	var reported atomic.Int32
	opts.OnWarning = func(message string) { warnings = append(warnings, message) }
	opts.OnError = func(error) { reported.Add(1) }
	a, counts, deleted := compatAdapter(t, sink, opts, 1, compatReply{501, "NotImplemented", ""}, nil, compatReply{403, "AccessDenied", "plain DELETE denied"})
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
		require.ErrorContains(t, err, "AccessDenied")
	case <-time.After(objectTestTimeout):
		t.Fatal("plain failure not returned")
	}
	require.Equal(t, int32(1), counts.conditional.Load())
	require.Equal(t, int32(1), counts.plain.Load())
	require.Equal(t, int32(1), reported.Load())
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0], "count=1 total=1")
	require.ErrorIs(t, a.confirmDelete(), a.deleteErr)
	require.Equal(t, int32(1), counts.plain.Load(), "sticky error must not retry plain deletion")
	select {
	case key := <-deleted:
		t.Fatalf("failed plain DELETE removed %s", key)
	default:
	}
}

func TestUnsupportedObjectDoesNotDisableLaterPreconditions(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	var warnings []string
	var reported atomic.Int32
	opts.OnWarning = func(message string) { warnings = append(warnings, message) }
	opts.OnError = func(error) { reported.Add(1) }
	a, counts, deleted := compatAdapter(t, sink, opts, 2, compatReply{501, "NotImplemented", ""}, nil)
	client := *a.awsS3.Config.HTTPClient
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	var checkedReplacement atomic.Int32
	client.Transport = deleteTestTransport{base: base, before: func(r *http.Request) *http.Response {
		if path.Base(r.URL.Path) == "test-02.log" && r.Header.Get("If-Match") != "" {
			checkedReplacement.Add(1)
			return &http.Response{StatusCode: 412, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`<Error><Code>PreconditionFailed</Code></Error>`)), Request: r}
		}
		return nil
	}}
	a.awsS3.Config.HTTPClient = &client
	done := make(chan error, 1)
	go func() { _, err := a.lookForFiles(); done <- err }()
	for i := 0; i < 2; i++ {
		select {
		case <-received:
		case <-time.After(objectTestTimeout):
			t.Fatal("objects not admitted")
		}
	}
	ack(1)
	select {
	case key := <-deleted:
		require.Equal(t, "test-01.log", key)
	case <-time.After(objectTestTimeout):
		t.Fatal("first compatibility fallback failed")
	}
	ack(2)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(objectTestTimeout):
		t.Fatal("later precondition stalled collection")
	}
	require.Equal(t, int32(1), checkedReplacement.Load())
	require.Equal(t, int32(1), counts.plain.Load())
	require.Zero(t, reported.Load())
	require.Len(t, warnings, 1)
	select {
	case key := <-deleted:
		t.Fatalf("replacement incorrectly deleted: %s", key)
	default:
	}
}

func TestCloseCompatibilityFallbackAfterFinalReceipt(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	var warnings []string
	var reported atomic.Int32
	opts.OnWarning = func(message string) { warnings = append(warnings, message) }
	opts.OnError = func(error) { reported.Add(1) }
	a, counts, deleted := compatAdapter(t, sink, opts, 32, compatReply{501, "NotImplemented", ""}, nil)
	a.wg.Add(1)
	go func() { defer a.wg.Done(); _, _ = a.lookForFiles() }()
	defer func() { atomic.StoreUint32(&a.isStop, 1); sink.BeginDrain() }()
	for i := 0; i < 32; i++ {
		select {
		case <-received:
		case <-time.After(objectTestTimeout):
			t.Fatal("window did not fill")
		}
	}
	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	require.Eventually(t, func() bool { return atomic.LoadUint32(&a.isStop) == 1 }, objectTestTimeout, time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Close returned before final receipts: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.Zero(t, counts.conditional.Load())
	require.Zero(t, counts.plain.Load())
	ack(32)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(objectTestTimeout):
		t.Fatal("Close did not finish compatibility cleanup")
	}
	expectDeletedPrefix(t, deleted, 1, 32)
	require.Zero(t, reported.Load())
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0], "count=32 total=32")
	require.Equal(t, int32(32), counts.conditional.Load())
	require.Equal(t, int32(32), counts.plain.Load())
}
