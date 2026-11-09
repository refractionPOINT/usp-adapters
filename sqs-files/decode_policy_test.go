package usp_sqs_files

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

type policyClient struct {
	*receiptClient
	admitted chan struct{}
	once     sync.Once
}

func (c *policyClient) Ship(m *protocol.DataMessage, d time.Duration) error {
	err := c.receiptClient.Ship(m, d)
	if err == nil {
		c.once.Do(func() { close(c.admitted) })
	}
	return err
}

type decodePolicyFixture struct {
	mu            sync.Mutex
	deleted       int
	gets          map[string]int
	bucketDeletes atomic.Int32
	fail          atomic.Bool
	failure       string
	admitted      <-chan struct{}
	server        *httptest.Server
	session       *session.Session
}

func newDecodePolicyFixture(t *testing.T) *decodePolicyFixture {
	t.Helper()
	f := &decodePolicyFixture{gets: map[string]int{}}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			f.bucketDeletes.Add(1)
			t.Error("bucket object was deleted")
			w.WriteHeader(405)
			return
		}
		if r.Method == "GET" {
			key := strings.TrimPrefix(r.URL.Path, "/test/")
			f.mu.Lock()
			f.gets[key]++
			f.mu.Unlock()
			if key == "flaky.txt" && f.fail.Load() {
				select {
				case <-f.admitted:
				case <-r.Context().Done():
					return
				}
				switch f.failure {
				case "network":
					c, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					c.Close()
					return
				case "throttle":
					http.Error(w, "throttled", 429)
					return
				default:
					http.Error(w, "server error", 503)
					return
				}
			}
			if strings.HasSuffix(key, ".parquet") {
				fmt.Fprint(w, "undecodable synthetic file")
				return
			}
			fmt.Fprint(w, "synthetic event")
			return
		}
		if r.Header.Get("X-Amz-Target") != "AmazonSQS.DeleteMessageBatch" {
			t.Error("unexpected local operation")
			w.WriteHeader(400)
			return
		}
		var in struct{ Entries []struct{ Id string } }
		json.NewDecoder(r.Body).Decode(&in)
		var success []map[string]string
		for _, e := range in.Entries {
			success = append(success, map[string]string{"Id": e.Id})
		}
		f.mu.Lock()
		f.deleted += len(in.Entries)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		json.NewEncoder(w).Encode(map[string]interface{}{"Successful": success})
	}))
	t.Cleanup(f.server.Close)
	f.session = session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(f.server.URL), S3ForcePathStyle: aws.Bool(true), DisableComputeChecksums: aws.Bool(true), Credentials: credentials.NewStaticCredentials("test", "test", ""), MaxRetries: aws.Int(0)}))
	return f
}
func (f *decodePolicyFixture) adapter(t *testing.T, c *policyClient) (*SQSFilesAdapter, chan string, chan error) {
	t.Helper()
	a := &SQSFilesAdapter{uspClient: c, ctx: context.Background(), sqsClient: awssqs.New(f.session), chFiles: make(chan fileInfo)}
	a.conf.QueueURL = f.server.URL
	a.conf.BucketPath = "bucket"
	a.conf.FilePath = "files/path"
	a.conf.IsDecodeObjectKey = true
	a.bucketDownloaders = map[string]*s3manager.Downloader{"test": s3manager.NewDownloader(f.session)}
	warnings := make(chan string, 20)
	fatal := make(chan error, 20)
	a.conf.ClientOptions.OnWarning = func(s string) { warnings <- s }
	a.conf.ClientOptions.OnError = func(e error) { fatal <- e; a.stopCollection() }
	for i := 0; i < 2; i++ {
		a.wg.Add(1)
		go func() { defer a.wg.Done(); a.processFiles() }()
	}
	a.collectionContext()
	t.Cleanup(func() { a.stopCollection(); close(a.chFiles); a.wg.Wait() })
	return a, warnings, fatal
}
func policyMessage(body, receipt string) []*awssqs.Message {
	return []*awssqs.Message{{MessageId: aws.String(receipt), ReceiptHandle: aws.String(receipt), Body: aws.String(body)}}
}
func (f *decodePolicyFixture) deletedCount() int { f.mu.Lock(); defer f.mu.Unlock(); return f.deleted }
func TestDecodedBadFileSkippedAfterReadableReceipt(t *testing.T) {
	f := newDecodePolicyFixture(t)
	c := &policyClient{receiptClient: &receiptClient{observed: make(chan struct{}, 10)}, admitted: make(chan struct{})}
	a, warnings, fatal := f.adapter(t, c)
	done := make(chan error, 1)
	go func() {
		done <- a.handleBatch(a.collectionContext(), policyMessage(`{"bucket":"test","files":[{"path":"good.txt"},{"path":"bad%20file.parquet"}]}`, "mixed"), time.Now())
	}()
	waitReview(t, "mixed files not processed", func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.next == 1 && len(warnings) == 1 })
	select {
	case <-c.observed:
	case err := <-done:
		t.Fatalf("readable receipt was abandoned: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("receipt was not observed")
	}
	if f.deletedCount() != 0 {
		t.Fatal("notification deleted before readable file ACK")
	}
	if len(fatal) != 0 {
		t.Fatal("decode failure raised fatal callback before completion")
	}
	c.mu.Lock()
	c.pending = nil
	c.mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("confirmed notification did not finish")
	}
	if f.deletedCount() != 1 {
		t.Fatalf("deletes=%d want 1", f.deletedCount())
	}
	warning := <-warnings
	if !strings.Contains(warning, "skipped_undecodable_files=1") || !strings.Contains(warning, `object_key="bad file.parquet"`) {
		t.Fatalf("warning lacks count/customer key: %s", warning)
	}
	if f.bucketDeletes.Load() != 0 {
		t.Fatal("original object was deleted")
	}
}
func TestDecodeOnlyNotificationCompletesWithoutAdmission(t *testing.T) {
	f := newDecodePolicyFixture(t)
	c := &policyClient{receiptClient: &receiptClient{shipErr: errors.New("must not ship"), observeErr: errors.New("must not observe"), observed: make(chan struct{}, 10)}, admitted: make(chan struct{})}
	a, warnings, fatal := f.adapter(t, c)
	if err := a.handleBatch(a.collectionContext(), policyMessage(`{"bucket":"test","files":[{"path":"bad%20file.parquet"}]}`, "decode-only"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if f.deletedCount() != 1 || c.next != 0 || len(c.observed) != 0 {
		t.Fatalf("deletes=%d admissions=%d observations=%d", f.deletedCount(), c.next, len(c.observed))
	}
	if len(warnings) != 1 || len(fatal) != 0 {
		t.Fatalf("warnings=%d fatal=%d", len(warnings), len(fatal))
	}
	if warning := <-warnings; !strings.Contains(warning, "skipped_undecodable_files=1") || !strings.Contains(warning, `object_key="bad file.parquet"`) {
		t.Fatalf("warning=%s", warning)
	}
	if f.bucketDeletes.Load() != 0 {
		t.Fatal("original object was deleted")
	}
}
func TestTransientDownloadRetainsWholeNotificationAndReplaysReadable(t *testing.T) {
	for _, mode := range []string{"server", "throttle", "network"} {
		t.Run(mode, func(t *testing.T) {
			f := newDecodePolicyFixture(t)
			f.failure = mode
			f.fail.Store(true)
			first := &policyClient{receiptClient: &receiptClient{autoACK: true}, admitted: make(chan struct{})}
			f.admitted = first.admitted
			a, _, fatal := f.adapter(t, first)
			body := `{"bucket":"test","files":[{"path":"good.txt"},{"path":"flaky.txt"}]}`
			err := a.handleBatch(a.collectionContext(), policyMessage(body, "first"), time.Now())
			if err == nil {
				t.Fatal("transient failure did not retain notification")
			}
			a.conf.ClientOptions.OnError(err) // same fatal-owner path used by receiveBatches
			if len(fatal) != 1 || f.deletedCount() != 0 || first.next != 1 {
				t.Fatalf("fatal=%d deletes=%d admitted=%d", len(fatal), f.deletedCount(), first.next)
			}
			f.fail.Store(false)
			retry := &policyClient{receiptClient: &receiptClient{autoACK: true}, admitted: make(chan struct{})}
			b, _, retryFatal := f.adapter(t, retry)
			if err := b.handleBatch(b.collectionContext(), policyMessage(body, "redelivery"), time.Now()); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			goodGets := f.gets["good.txt"]
			f.mu.Unlock()
			if goodGets != 2 || retry.next != 2 || f.deletedCount() != 1 || len(retryFatal) != 0 {
				t.Fatalf("good downloads=%d retry admissions=%d deletes=%d fatal=%d", goodGets, retry.next, f.deletedCount(), len(retryFatal))
			}
			if f.bucketDeletes.Load() != 0 {
				t.Fatal("bucket object deleted")
			}
		})
	}
}
