package usp_sqs_files

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"
)

type reviewClient struct {
	*receiptClient
	draining chan struct{}
	once     sync.Once
}

func (c *reviewClient) Drain(time.Duration) error {
	c.mu.Lock()
	c.autoACK = true
	c.mu.Unlock()
	c.once.Do(func() { close(c.draining) })
	return nil
}

type reviewFixture struct {
	warnings        chan string
	downloadBarrier <-chan struct{}
	downloadStarted chan struct{}
	blockDelete     bool
	deleteStarted   chan struct{}
	deleteOnce      sync.Once
	a               *SQSFilesAdapter
	c               *receiptClient
	mu              sync.Mutex
	received        int
	deleted         map[string]bool
	errors          chan error
	cancel          context.CancelFunc
	done            chan error
}

func newReviewFixture(t *testing.T, total int, body func(int) string, drainACK bool, fatalOwner ...bool) *reviewFixture {
	t.Helper()
	f := &reviewFixture{warnings: make(chan string, 100), downloadStarted: make(chan struct{}, 100), deleteStarted: make(chan struct{}), c: &receiptClient{}, deleted: map[string]bool{}, errors: make(chan error, 100), done: make(chan error, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			f.mu.Lock()
			barrier := f.downloadBarrier
			f.mu.Unlock()
			f.downloadStarted <- struct{}{}
			if barrier != nil {
				select {
				case <-barrier:
				case <-r.Context().Done():
					return
				}
			}
			fmt.Fprint(w, strings.TrimSuffix(path.Base(r.URL.Path), ".txt"))
			return
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch r.Header.Get("X-Amz-Target") {
		case "AmazonSQS.ReceiveMessage":
			io.Copy(io.Discard, r.Body)
			f.mu.Lock()
			i := f.received
			if i < total {
				f.received++
			}
			f.mu.Unlock()
			if i >= total {
				<-r.Context().Done()
				return
			}
			raw := fmt.Sprintf(`{"bucket":"test","files":[{"path":"%d.txt"}]}`, i)
			if body != nil {
				raw = body(i)
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"Messages": []map[string]string{{"MessageId": fmt.Sprint(i), "ReceiptHandle": fmt.Sprint(i), "Body": raw}}})
		case "AmazonSQS.DeleteMessageBatch":
			var in struct {
				Entries []struct{ Id, ReceiptHandle string }
			}
			json.NewDecoder(r.Body).Decode(&in)
			f.mu.Lock()
			blocked := f.blockDelete
			f.mu.Unlock()
			if blocked {
				f.deleteOnce.Do(func() { close(f.deleteStarted) })
				<-r.Context().Done()
				return
			}
			var success []map[string]string
			f.mu.Lock()
			for _, e := range in.Entries {
				f.deleted[e.ReceiptHandle] = true
				success = append(success, map[string]string{"Id": e.Id})
			}
			f.mu.Unlock()
			json.NewEncoder(w).Encode(map[string]interface{}{"Successful": success})
		case "AmazonSQS.ChangeMessageVisibilityBatch":
			var in struct{ Entries []struct{ Id string } }
			json.NewDecoder(r.Body).Decode(&in)
			var success []map[string]string
			for _, e := range in.Entries {
				success = append(success, map[string]string{"Id": e.Id})
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"Successful": success})
		default:
			t.Errorf("unexpected %s", r.Header.Get("X-Amz-Target"))
			w.WriteHeader(400)
		}
	}))
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(source.URL), S3ForcePathStyle: aws.Bool(true), DisableComputeChecksums: aws.Bool(true), Credentials: credentials.NewStaticCredentials("test", "test", ""), MaxRetries: aws.Int(0)}))
	a := &SQSFilesAdapter{uspClient: f.c, ctx: ctx, sqsClient: awssqs.New(sess)}
	if drainACK {
		a.uspClient = &reviewClient{receiptClient: f.c, draining: make(chan struct{})}
	}
	a.conf.QueueURL = source.URL
	a.conf.ClientOptions.DebugLog = func(string) {}
	a.conf.ClientOptions.OnWarning = func(msg string) { f.warnings <- msg }
	a.conf.ClientOptions.OnError = func(err error) {
		f.errors <- err
		// Model the hosted owner: any fatal callback stops collection.
		if len(fatalOwner) > 0 && fatalOwner[0] {
			cancel()
		}
	}
	a.conf.BucketPath = "bucket"
	a.conf.FilePath = "files/path"
	a.bucketDownloaders = map[string]*s3manager.Downloader{"test": s3manager.NewDownloader(sess)}
	a.chFiles = make(chan fileInfo)
	a.conf.ParallelFetch = 2
	for i := 0; i < 2; i++ {
		a.wg.Add(1)
		go func() { defer a.wg.Done(); a.processFiles() }()
	}
	f.a = a
	stopped := make(chan struct{})
	a.receiveStopped = stopped
	go func() { defer close(stopped); f.done <- a.receiveEvents() }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			t.Error("receiver did not join")
		}
		a.wg.Wait()
		source.Close()
	})
	return f
}
func waitReview(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(what)
}
func TestReviewBoundedPipelineAndCumulativeReceipts(t *testing.T) {
	f := newReviewFixture(t, 9, nil, false)
	started := time.Now()
	waitReview(t, "eight batches did not admit behind a withheld receipt", func() bool { f.c.mu.Lock(); defer f.c.mu.Unlock(); return f.c.next == 8 })
	f.mu.Lock()
	received := f.received
	deleted := len(f.deleted)
	f.mu.Unlock()
	if received != 8 || deleted != 0 {
		t.Fatalf("before ACK: received=%d deleted=%d", received, deleted)
	}
	t.Logf("eight batches admitted with ACK withheld in %v", time.Since(started))
	f.c.mu.Lock()
	var retained []string
	for _, m := range f.c.pending[4:] {
		id := m.TextPayload
		if id == "" {
			id = string(m.BundlePayload)
		}
		retained = append(retained, id)
	}
	f.c.pending = f.c.pending[4:]
	f.c.mu.Unlock()
	waitReview(t, "first cumulative prefix did not clean up", func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(f.deleted) == 4 })
	f.mu.Lock()
	for _, id := range retained {
		if f.deleted[id] {
			t.Error("cleanup crossed pending receipt frontier")
		}
	}
	f.mu.Unlock()
	waitReview(t, "next batch did not reuse released capacity", func() bool { f.c.mu.Lock(); defer f.c.mu.Unlock(); return f.c.next == 9 })
	f.c.mu.Lock()
	f.c.pending = nil
	f.c.mu.Unlock()
	waitReview(t, "final cumulative receipt did not clean up", func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(f.deleted) == 9 })
}
func TestReviewGracefulCloseFlushesDeliveredBatch(t *testing.T) {
	f := newReviewFixture(t, 1, nil, true)
	waitReview(t, "initial batch not admitted", func() bool { f.c.mu.Lock(); defer f.c.mu.Unlock(); return f.c.next == 1 })
	closed := make(chan error, 1)
	go func() { closed <- f.a.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung")
	}
	f.mu.Lock()
	deleted := len(f.deleted)
	f.mu.Unlock()
	if deleted != 1 {
		t.Fatal("Close failed to delete the batch delivered during Drain")
	}
	if len(f.errors) != 0 {
		t.Fatalf("expected shutdown reported OnError: %v", <-f.errors)
	}
}

func TestReviewPoisonNotificationsRetainedWithoutBlockingValidWork(t *testing.T) {
	f := newReviewFixture(t, 5, func(i int) string {
		switch i {
		case 0:
			return `{"Event":"s3:TestEvent","Service":"Amazon S3"}`
		case 1:
			return `{"bucket":"test","files":[{"path":"bad.parquet"}]}`
		case 2:
			return `{"bucket":"test","files":[{"path":"good.txt"}]}`
		case 3:
			return `{"files":[{"path":"test.txt"}]}`
		default:
			return "{"
		}
	}, false, true)
	f.c.mu.Lock()
	f.c.autoACK = true
	f.c.mu.Unlock()
	waitReview(t, "valid work behind poison notifications was not delivered", func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.deleted["2"] })
	waitReview(t, "retained notification warnings not reported", func() bool { return len(f.warnings) == 4 })
	if len(f.errors) != 0 {
		t.Fatalf("permanent notifications raised fatal callback: %v", <-f.errors)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.deleted) != 1 {
		t.Fatalf("poison notifications deleted: %v", f.deleted)
	}
}

func TestReviewLatencyThroughput(t *testing.T) {
	const batches = 16
	const latency = 300 * time.Millisecond
	f := newReviewFixture(t, batches, nil, false)
	stopACKs := make(chan struct{})
	joined := make(chan struct{})
	defer func() { close(stopACKs); <-joined }()
	var peak int
	go func() {
		defer close(joined)
		ticker := time.NewTicker(latency)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				f.c.mu.Lock()
				if len(f.c.pending) > peak {
					peak = len(f.c.pending)
				}
				f.c.pending = nil
				f.c.mu.Unlock()
			case <-stopACKs:
				return
			}
		}
	}()
	started := time.Now()
	// A hang-only ceiling permits both baseline and pipelined implementations.
	deadline := time.Now().Add(15 * time.Second)
	for {
		f.mu.Lock()
		deleted := len(f.deleted)
		f.mu.Unlock()
		if deleted == batches {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("latency fixture hung")
		}
		time.Sleep(5 * time.Millisecond)
	}
	elapsed := time.Since(started)
	f.c.mu.Lock()
	peakPending := peak
	f.c.mu.Unlock()
	t.Logf("%d one-message batches; ACK interval=%v; elapsed=%v; batches/sec=%.2f; peak pending=%d", batches, latency, elapsed, float64(batches)/elapsed.Seconds(), peakPending)
}

func TestReviewMixedBatchRetainsOnlyBadNotifications(t *testing.T) {
	f := newReviewFixture(t, 0, nil, false, true)
	f.a.conf.IsDecodeObjectKey = true
	f.c.mu.Lock()
	f.c.autoACK = true
	f.c.mu.Unlock()
	bodies := []*string{
		aws.String(`{"Event":"s3:TestEvent","Service":"Amazon S3"}`),
		aws.String("{"),
		aws.String(`{"bucket":"test","files":[{"path":"one.txt"},{"path":"bad.parquet"}]}`),
		aws.String(`{"files":[{"path":"missing-bucket.txt"}]}`),
		nil,
		aws.String(`{"bucket":"test","files":[{"path":"bad%xx"}]}`),
		aws.String(`{"bucket":"test","files":[{"path":"valid.txt"}]}`),
	}
	var messages []*awssqs.Message
	for i, body := range bodies {
		messages = append(messages, &awssqs.Message{MessageId: aws.String(fmt.Sprint(i)), ReceiptHandle: aws.String(fmt.Sprint(i)), Body: body})
	}
	if err := f.a.handleBatch(f.a.collectionContext(), messages, time.Now()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.deleted) != 1 || !f.deleted["6"] {
		t.Fatalf("mixed notification disposal=%v", f.deleted)
	}
	if len(f.errors) != 0 || len(f.warnings) != 6 {
		t.Fatalf("per-notification fatal callbacks=%d warnings=%d", len(f.errors), len(f.warnings))
	}
}

func TestReviewCloseCancelsDeleteAfterGrace(t *testing.T) {
	old := leaseCloseBudget
	leaseCloseBudget = 200 * time.Millisecond
	defer func() { leaseCloseBudget = old }()
	f := newReviewFixture(t, 1, nil, true)
	waitReview(t, "batch not admitted", func() bool { f.c.mu.Lock(); defer f.c.mu.Unlock(); return f.c.next == 1 })
	f.mu.Lock()
	f.blockDelete = true
	f.mu.Unlock()
	closed := make(chan error, 1)
	go func() { closed <- f.a.Close() }()
	select {
	case <-f.deleteStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("delivered batch did not attempt cleanup during grace")
	}
	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("expired shutdown grace not reported")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung")
	}
	f.mu.Lock()
	deleted := len(f.deleted)
	f.mu.Unlock()
	if deleted != 0 {
		t.Fatal("canceled cleanup deleted source")
	}
	if len(f.errors) != 0 {
		t.Fatalf("expected canceled cleanup reported OnError: %v", <-f.errors)
	}
}

func TestReviewParallelFetchAcrossLeasedBatches(t *testing.T) {
	begin := make(chan struct{})
	release := make(chan struct{})
	f := newReviewFixture(t, 2, func(i int) string { <-begin; return fmt.Sprintf(`{"bucket":"test","files":[{"path":"%d.txt"}]}`, i) }, false)
	f.mu.Lock()
	f.downloadBarrier = release
	f.mu.Unlock()
	close(begin)
	for i := 0; i < 2; i++ {
		select {
		case <-f.downloadStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("ParallelFetch was serialized across leased batches")
		}
	}
	f.c.mu.Lock()
	f.c.autoACK = true
	f.c.mu.Unlock()
	close(release)
	waitReview(t, "parallel downloads did not finish source cleanup", func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(f.deleted) == 2 })
}
