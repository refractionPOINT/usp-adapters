package usp_sqs_files

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
)

type absenceFixture struct {
	a        *SQSFilesAdapter
	c        *receiptClient
	mu       sync.Mutex
	deleted  map[string]bool
	warnings chan string
	fatal    chan error
}

func newAbsenceFixture(t *testing.T, bodies []string, withhold, allAbsent bool, fixedBucket ...string) *absenceFixture {
	t.Helper()
	f := &absenceFixture{c: &receiptClient{autoACK: !withhold, observed: make(chan struct{}, 20)}, deleted: map[string]bool{}, warnings: make(chan string, 20), fatal: make(chan error, 20)}
	if allAbsent {
		f.c.shipErr = errors.New("absent files must not Ship")
		f.c.observeErr = errors.New("absent files must not require receipt")
	}
	ctx, cancel := context.WithCancel(context.Background())
	received := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		if r.Method == "HEAD" {
			if parts[0] == "missing-bucket" {
				w.WriteHeader(404)
				return
			}
			if parts[0] == "denied-bucket" {
				w.WriteHeader(403)
				return
			}
			if parts[0] == "unavailable-bucket" {
				w.WriteHeader(503)
				return
			}
			w.Header().Set("X-Amz-Bucket-Region", "us-east-1")
			return
		}
		if r.Method == "GET" {
			if parts[0] == "deleted-bucket" {
				w.WriteHeader(404)
				fmt.Fprint(w, "<Error><Code>NoSuchBucket</Code></Error>")
				return
			}
			if len(parts) == 2 && strings.HasPrefix(parts[1], "gone") {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(404)
				code := "NoSuchKey"
				if strings.Contains(parts[1], "notfound") {
					code = "NotFound"
				}
				fmt.Fprintf(w, "<Error><Code>%s</Code><Message>absent</Message></Error>", code)
				return
			}
			fmt.Fprint(w, "synthetic event")
			return
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch r.Header.Get("X-Amz-Target") {
		case "AmazonSQS.ReceiveMessage":
			io.Copy(io.Discard, r.Body)
			f.mu.Lock()
			i := received
			if i < len(bodies) {
				received++
			}
			f.mu.Unlock()
			if i >= len(bodies) {
				<-r.Context().Done()
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"Messages": []map[string]string{{"MessageId": fmt.Sprint(i), "ReceiptHandle": fmt.Sprint(i), "Body": bodies[i]}}})
		case "AmazonSQS.DeleteMessageBatch":
			var in struct {
				Entries []struct{ Id, ReceiptHandle string }
			}
			json.NewDecoder(r.Body).Decode(&in)
			var successful []map[string]string
			f.mu.Lock()
			for _, e := range in.Entries {
				f.deleted[e.ReceiptHandle] = true
				successful = append(successful, map[string]string{"Id": e.Id})
			}
			f.mu.Unlock()
			json.NewEncoder(w).Encode(map[string]interface{}{"Successful": successful})
		case "AmazonSQS.ChangeMessageVisibilityBatch":
			var in struct{ Entries []struct{ Id string } }
			json.NewDecoder(r.Body).Decode(&in)
			var successful []map[string]string
			for _, e := range in.Entries {
				successful = append(successful, map[string]string{"Id": e.Id})
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"Successful": successful})
		default:
			t.Error("unexpected local API operation")
			w.WriteHeader(400)
		}
	}))
	cfg := &aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(server.URL), S3ForcePathStyle: aws.Bool(true), DisableComputeChecksums: aws.Bool(true), Credentials: credentials.NewStaticCredentials("test", "test", ""), MaxRetries: aws.Int(0)}
	sess := session.Must(session.NewSession(cfg))
	a := &SQSFilesAdapter{uspClient: f.c, ctx: ctx, awsConfig: cfg, awsSession: sess, sqsClient: awssqs.New(sess), chFiles: make(chan fileInfo)}
	f.a = a
	a.conf.QueueURL = server.URL
	a.conf.BucketPath = "bucket"
	a.conf.FilePath = "files/path"
	if len(fixedBucket) > 0 {
		a.conf.Bucket = fixedBucket[0]
	}
	a.conf.ClientOptions.DebugLog = func(string) {}
	a.conf.ClientOptions.OnWarning = func(s string) { f.warnings <- s }
	a.conf.ClientOptions.OnError = func(err error) { f.fatal <- err; cancel() } // actual hosted owner policy
	for i := 0; i < 2; i++ {
		a.wg.Add(1)
		go func() { defer a.wg.Done(); a.processFiles() }()
	}
	stopped := make(chan struct{})
	a.receiveStopped = stopped
	go func() { defer close(stopped); a.receiveEvents() }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			t.Error("receiver did not join")
		}
		a.wg.Wait()
		server.Close()
	})
	return f
}
func (f *absenceFixture) deletedCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.deleted) }
func TestAbsentObjectThenValidNotificationContinues(t *testing.T) {
	f := newAbsenceFixture(t, []string{
		`{"bucket":"test","files":[{"path":"gone.txt"}]}`,
		`{"bucket":"test","files":[{"path":"valid.txt"}]}`,
	}, false, false)
	waitReview(t, "absent object stopped later valid work", func() bool { return f.deletedCount() == 2 || len(f.fatal) > 0 })
	if len(f.fatal) != 0 {
		t.Fatalf("absent object raised fatal callback: %v", <-f.fatal)
	}
	if f.deletedCount() != 2 || len(f.warnings) != 1 {
		t.Fatalf("deleted=%d warnings=%d", f.deletedCount(), len(f.warnings))
	}
}
func TestBucketFailureRetainsNotificationAndReportsError(t *testing.T) {
	for _, tc := range []struct {
		name, bucket string
		fixed        bool
	}{
		{"mistyped_fixed_bucket", "missing-bucket", true},
		{"wrong_bucket_path", "missing-bucket", false},
		{"fixed_access_denied", "denied-bucket", true},
		{"dynamic_access_denied", "denied-bucket", false},
		{"fixed_server_failure", "unavailable-bucket", true},
		{"dynamic_server_failure", "unavailable-bucket", false},
		{"bucket_deleted_after_lookup", "deleted-bucket", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var fixed []string
			if tc.fixed {
				fixed = []string{tc.bucket}
			}
			f := newAbsenceFixture(t, []string{fmt.Sprintf(`{"bucket":%q,"files":[{"path":"event.txt"}]}`, tc.bucket)}, false, true, fixed...)
			select {
			case err := <-f.fatal:
				if err == nil {
					t.Fatal("bucket failure did not report an error")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("bucket failure did not report visible error")
			}
			select {
			case <-f.a.receiveStopped:
			case <-time.After(10 * time.Second):
				t.Fatal("receiver did not join")
			}
			if got := f.deletedCount(); got != 0 {
				t.Fatalf("bucket failure deleted %d notifications", got)
			}
			f.c.mu.Lock()
			admitted := f.c.next
			f.c.mu.Unlock()
			if admitted != 0 || len(f.c.observed) != 0 {
				t.Fatal("bucket failure authorized admission or receipt observation")
			}
		})
	}
}
func TestMixedAbsentFileWaitsForRemainingReceipt(t *testing.T) {
	f := newAbsenceFixture(t, []string{`{"bucket":"test","files":[{"path":"gone.txt"},{"path":"valid.txt"}]}`}, true, false)
	waitReview(t, "mixed files did not process", func() bool {
		f.c.mu.Lock()
		defer f.c.mu.Unlock()
		return f.c.next == 1 && len(f.warnings) == 1 || len(f.fatal) > 0
	})
	if len(f.fatal) != 0 {
		t.Fatalf("absent file raised fatal callback: %v", <-f.fatal)
	}
	select {
	case <-f.c.observed:
	case <-time.After(5 * time.Second):
		t.Fatal("remaining delivery receipt was not observed")
	}
	if f.deletedCount() != 0 {
		t.Fatal("mixed notification deleted before valid file receipt")
	}
	f.c.mu.Lock()
	f.c.pending = nil
	f.c.mu.Unlock()
	waitReview(t, "confirmed mixed notification not deleted", func() bool { return f.deletedCount() == 1 })
}
func TestAllAbsentNotificationCompletesWithoutDependency(t *testing.T) {
	f := newAbsenceFixture(t, []string{`{"bucket":"test","files":[{"path":"gone-a.txt"},{"path":"gone-notfound.txt"}]}`}, false, true)
	waitReview(t, "all-absent notification did not complete", func() bool { return f.deletedCount() == 1 || len(f.fatal) > 0 })
	if len(f.fatal) != 0 {
		t.Fatalf("all-absent notification raised fatal callback: %v", <-f.fatal)
	}
	f.c.mu.Lock()
	admitted := f.c.next
	f.c.mu.Unlock()
	if f.deletedCount() != 1 || admitted != 0 || len(f.c.observed) != 0 || len(f.warnings) != 2 {
		t.Fatalf("deleted=%d admissions=%d observations=%d warnings=%d", f.deletedCount(), admitted, len(f.c.observed), len(f.warnings))
	}
}

func TestSourceAbsenceClassificationPreservesFatalErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"object_code", awserr.New("NoSuchKey", "absent", nil), true},
		{"bucket_code", awserr.New("NoSuchBucket", "absent", nil), false},
		{"not_found", awserr.New("NotFound", "absent", nil), true},
		{"missing_bucket_http_404", awserr.NewRequestFailure(awserr.New("NoSuchBucket", "absent", nil), 404, ""), false},
		{"http_404", awserr.NewRequestFailure(awserr.New("NotFound", "absent", nil), 404, ""), true},
		{"access_denied", awserr.NewRequestFailure(awserr.New("AccessDenied", "denied", nil), 403, ""), false},
		{"misleading_code_403", awserr.NewRequestFailure(awserr.New("NoSuchKey", "denied", nil), 403, ""), false},
		{"http_503", awserr.NewRequestFailure(awserr.New("InternalError", "failed", nil), 503, ""), false},
		{"misleading_code_500", awserr.NewRequestFailure(awserr.New("NoSuchBucket", "failed", nil), 500, ""), false},
		{"network", awserr.New("RequestError", "connection failed", io.ErrUnexpectedEOF), false},
		{"cancelled", context.Canceled, false},
		{"deadline", context.DeadlineExceeded, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sourceIsAbsent(tc.err, "NoSuchKey", "NotFound"); got != tc.want {
				t.Fatalf("absent=%v want %v", got, tc.want)
			}
		})
	}
	if sourceIsAbsent(awserr.New("NoSuchBucket", "absent", nil), "NoSuchKey", "NotFound") {
		t.Fatal("wrong operation's absent code accepted without 404")
	}
}
