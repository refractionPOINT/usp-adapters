package usp_sqs_files

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

// A real Ship attempt blocks only for the supplied SDK admission slice. The
// caller can retry until permission fallback or Close changes its context.
type slowCompatibilityClient struct {
	*compatibilityClient
	allow     <-chan struct{}
	attempted chan struct{}
	once      sync.Once
}

func (c *slowCompatibilityClient) Ship(m *protocol.DataMessage, d time.Duration) error {
	c.once.Do(func() { close(c.attempted) })
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-c.allow:
		return c.compatibilityClient.Ship(m, d)
	case <-timer.C:
		return uspclient.ErrorBufferFull
	}
}

func TestPermissionDisabledSlowWork(t *testing.T) {
	for _, work := range []string{"admission", "download"} {
		for _, outcome := range []string{"success", "stale_entry", "stale_top", "expired_parameter", "shutdown_work", "shutdown_wait", "delete403", "delete500", "delete_timeout", "shutdown_delete"} {
			t.Run(work+"/"+outcome, func(t *testing.T) {
				oldBudget, oldRPC, oldCadence, oldClose := sourceLeaseBudget, leaseRPCBudget, leaseRenewInterval, leaseCloseBudget
				sourceLeaseBudget, leaseRPCBudget = 300*time.Millisecond, time.Second
				leaseRenewInterval, leaseCloseBudget = 20*time.Millisecond, 100*time.Millisecond
				defer func() {
					sourceLeaseBudget, leaseRPCBudget, leaseRenewInterval, leaseCloseBudget = oldBudget, oldRPC, oldCadence, oldClose
				}()
				allowWork := make(chan struct{})
				var workOnce sync.Once
				releaseWork := func() { workOnce.Do(func() { close(allowWork) }) }
				defer releaseWork()
				c := &slowCompatibilityClient{compatibilityClient: &compatibilityClient{receiptClient: &receiptClient{}}, allow: allowWork, attempted: make(chan struct{})}
				if work != "admission" {
					releaseWork()
				}
				allowDownload := make(chan struct{})
				var downloadOnce sync.Once
				releaseDownload := func() { downloadOnce.Do(func() { close(allowDownload) }) }
				defer releaseDownload()
				if work != "download" {
					releaseDownload()
				}
				downloadStarted := make(chan struct{})
				var startedOnce sync.Once
				allowSecond := make(chan struct{})
				warnings, fatal := make(chan string, 20), make(chan error, 20)
				var receives, renewals, firstDeletes, secondDeletes atomic.Int32
				var acknowledged atomic.Bool
				deleteCanceled := make(chan struct{})
				source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" {
						if strings.HasSuffix(r.URL.Path, "/first.txt") {
							startedOnce.Do(func() { close(downloadStarted) })
							select {
							case <-allowDownload:
							case <-r.Context().Done():
								return
							}
						}
						fmt.Fprint(w, "synthetic event")
						return
					}
					w.Header().Set("Content-Type", "application/x-amz-json-1.0")
					switch r.Header.Get("X-Amz-Target") {
					case "AmazonSQS.ReceiveMessage":
						io.Copy(io.Discard, r.Body)
						n := receives.Add(1)
						if n > 3 {
							<-r.Context().Done()
							return
						}
						if n == 2 {
							select {
							case <-allowSecond:
							case <-r.Context().Done():
								return
							}
							// The held poll gets no packet; the next call starts
							// the second batch with a fresh pre-Receive timestamp.
							fmt.Fprint(w, `{"Messages":[]}`)
							return
						}
						handle := "first"
						if n == 3 {
							handle = "second"
						}
						body := fmt.Sprintf(`{"bucket":"test","files":[{"path":"%s.txt"}]}`, handle)
						json.NewEncoder(w).Encode(map[string]interface{}{"Messages": []map[string]string{{"MessageId": handle, "ReceiptHandle": handle, "Body": body}}})
					case "AmazonSQS.ChangeMessageVisibilityBatch":
						renewals.Add(1)
						w.WriteHeader(403)
						fmt.Fprint(w, `{"__type":"AccessDenied","message":"renewal denied"}`)
					case "AmazonSQS.DeleteMessageBatch":
						var req struct {
							Entries []struct{ Id, ReceiptHandle string }
						}
						json.NewDecoder(r.Body).Decode(&req)
						if len(req.Entries) != 1 {
							t.Error("unexpected delete entries")
							w.WriteHeader(400)
							return
						}
						if req.Entries[0].ReceiptHandle == "second" {
							secondDeletes.Add(1)
							fmt.Fprint(w, `{"Successful":[{"Id":"0"}]}`)
							return
						}
						if !acknowledged.Load() {
							t.Error("delete before first delivery receipt")
						}
						firstDeletes.Add(1)
						switch outcome {
						case "stale_entry":
							fmt.Fprint(w, `{"Failed":[{"Id":"0","Code":"ReceiptHandleIsInvalid","SenderFault":true}]}`)
						case "stale_top":
							w.WriteHeader(400)
							fmt.Fprint(w, `{"__type":"ReceiptHandleIsInvalid","message":"expired receipt handle"}`)
						case "expired_parameter":
							fmt.Fprint(w, `{"Failed":[{"Id":"0","Code":"InvalidParameterValue","Message":"The receipt handle has expired.","SenderFault":true}]}`)
						case "delete_timeout", "shutdown_delete":
							<-r.Context().Done()
							close(deleteCanceled)
						case "delete403":
							w.WriteHeader(403)
							fmt.Fprint(w, `{"__type":"AccessDenied","message":"delete denied"}`)
						case "delete500":
							w.WriteHeader(500)
							fmt.Fprint(w, `{"__type":"InternalError","message":"source failed"}`)
						default:
							fmt.Fprint(w, `{"Successful":[{"Id":"0"}]}`)
						}
					default:
						t.Error("unexpected local operation")
						w.WriteHeader(400)
					}
				}))
				defer source.Close()
				sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(source.URL), S3ForcePathStyle: aws.Bool(true), DisableComputeChecksums: aws.Bool(true), Credentials: credentials.NewStaticCredentials("test", "test", ""), MaxRetries: aws.Int(0)}))
				a := &SQSFilesAdapter{uspClient: c, ctx: context.Background(), sqsClient: awssqs.New(sess)}
				a.collectionContext()
				a.conf.QueueURL = source.URL
				a.conf.ClientOptions.DebugLog = func(string) {}
				a.conf.ClientOptions.OnWarning = func(s string) { warnings <- s }
				a.conf.ClientOptions.OnError = func(err error) { fatal <- err; a.stopCollection() }
				a.conf.BucketPath, a.conf.FilePath = "bucket", "files/path"
				a.bucketDownloaders = map[string]*s3manager.Downloader{"test": s3manager.NewDownloader(sess)}
				a.chFiles = make(chan fileInfo)
				a.wg.Add(1)
				go func() { defer a.wg.Done(); a.processFiles() }()
				stopped := make(chan struct{})
				a.receiveStopped = stopped
				go func() { defer close(stopped); a.receiveEvents() }()
				defer func() {
					a.stopCollection()
					select {
					case <-stopped:
					case <-time.After(10 * time.Second):
						t.Error("receiver did not join")
					}
					a.wg.Wait()
				}()
				started := c.attempted
				if work == "download" {
					started = downloadStarted
				}
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("work was not actually started")
				}
				waitReview(t, "permission warning missing", func() bool { return len(warnings) > 0 || len(fatal) > 0 })
				if len(fatal) > 0 {
					t.Fatalf("permission denial fatal: %v", <-fatal)
				}
				// Positive work/denial barriers precede the injected original
				// window. The fixed interval proves slow work crosses expiry.
				<-time.After(sourceLeaseBudget + 75*time.Millisecond)
				if firstDeletes.Load() != 0 {
					t.Fatal("delete while work still held")
				}
				closeAdapter := func() {
					done := make(chan error, 1)
					go func() { done <- a.Close() }()
					select {
					case <-done:
					case <-time.After(10 * time.Second):
						t.Fatal("Close hung")
					}
					if len(fatal) != 0 || firstDeletes.Load() != 0 {
						t.Fatalf("Close fatal=%d deletes=%d", len(fatal), firstDeletes.Load())
					}
				}
				if outcome == "shutdown_work" {
					closeAdapter()
					return
				}
				releaseWork()
				releaseDownload()
				waitReview(t, "slow work did not admit after original deadline", func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.next == 1 || len(fatal) > 0 })
				if len(fatal) > 0 {
					t.Fatalf("slow admission fatal: %v", <-fatal)
				}
				time.Sleep(75 * time.Millisecond) // admitted, receipt withheld
				if firstDeletes.Load() != 0 {
					t.Fatal("delete before slow-work receipt")
				}
				if outcome == "shutdown_wait" {
					closeAdapter()
					return
				}
				acknowledged.Store(true)
				c.mu.Lock()
				c.pending = nil
				c.mu.Unlock()
				waitReview(t, "no delete attempt after slow-work receipt", func() bool { return firstDeletes.Load() > 0 || len(fatal) > 0 })
				if outcome == "shutdown_delete" {
					done := make(chan error, 1)
					go func() { done <- a.Close() }()
					select {
					case <-done:
					case <-time.After(10 * time.Second):
						t.Fatal("Close hung during native cleanup")
					}
					select {
					case <-deleteCanceled:
					case <-time.After(5 * time.Second):
						t.Fatal("Close did not cancel cleanup RPC")
					}
					if len(fatal) != 0 {
						t.Fatalf("Close cleanup fatal=%v", <-fatal)
					}
					return
				}
				if outcome == "delete403" || outcome == "delete500" || outcome == "delete_timeout" {
					waitReview(t, "genuine delete failure was suppressed", func() bool { return len(fatal) > 0 })
					if outcome == "delete_timeout" {
						select {
						case <-deleteCanceled:
						case <-time.After(5 * time.Second):
							t.Fatal("cleanup RPC not bounded")
						}
					}
					return
				}
				waitReview(t, "completed batch did not release its slot", func() bool { return len(a.ensureDelivery().slots) == maxLeasedBatches-1 || len(fatal) > 0 })
				if len(fatal) > 0 {
					t.Fatalf("expired handle stopped collector: %v", <-fatal)
				}
				permissionWarnings, retainedWarnings := 0, 0
				for len(warnings) > 0 {
					warning := <-warnings
					if strings.Contains(warning, "renewal_disabled_count=1") {
						permissionWarnings++
					}
					if strings.Contains(warning, "delete_retained_count=1") {
						retainedWarnings++
					}
				}
				expectedRetained := 1
				if outcome == "success" {
					expectedRetained = 0
				}
				if permissionWarnings != 1 || retainedWarnings != expectedRetained {
					t.Fatalf("permission warnings=%d retained warnings=%d", permissionWarnings, retainedWarnings)
				}
				close(allowSecond)
				waitReview(t, "later batch not admitted by same collector", func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.next == 2 || len(fatal) > 0 })
				if len(fatal) > 0 {
					t.Fatal("later batch fatal")
				}
				c.mu.Lock()
				c.pending = nil
				c.mu.Unlock()
				waitReview(t, "later batch not deleted", func() bool { return secondDeletes.Load() == 1 || len(fatal) > 0 })
				if len(fatal) != 0 || renewals.Load() != 1 || firstDeletes.Load() != 1 || a.collectionContext().Err() != nil || c.stops.Load() != 0 {
					t.Fatalf("collector stopped/retried: fatal=%d renewals=%d first deletes=%d", len(fatal), renewals.Load(), firstDeletes.Load())
				}
			})
		}
	}
}

func TestStaleDeleteCompatibilityClassification(t *testing.T) {
	stale := func(id string) *awssqs.BatchResultErrorEntry {
		return &awssqs.BatchResultErrorEntry{Id: aws.String(id), Code: aws.String("ReceiptHandleIsInvalid")}
	}
	cases := []struct {
		name     string
		out      *awssqs.DeleteMessageBatchOutput
		err      error
		n, count int
		normal   bool
	}{
		{name: "top_stale", err: awserr.NewRequestFailure(awserr.New("ReceiptHandleIsInvalid", "expired", nil), 400, ""), n: 2, count: 2},
		{name: "deprecated_stale", err: awserr.New("InvalidIdFormat", "invalid", nil), n: 1, count: 1},
		{name: "expired_parameter", err: awserr.New("InvalidParameterValue", "ReceiptHandle has expired", nil), n: 1, count: 1},
		{name: "unrelated_parameter", err: awserr.New("InvalidParameterValue", "invalid queue URL", nil), n: 1},
		{name: "top403", err: awserr.NewRequestFailure(awserr.New("ReceiptHandleIsInvalid", "invalid", nil), 403, ""), n: 1},
		{name: "top500", err: awserr.NewRequestFailure(awserr.New("ReceiptHandleIsInvalid", "invalid", nil), 500, ""), n: 1},
		{name: "network", err: awserr.New("RequestError", "network", io.ErrUnexpectedEOF), n: 1},
		{name: "canceled", err: context.Canceled, n: 1},
		{name: "all_stale", out: &awssqs.DeleteMessageBatchOutput{Failed: []*awssqs.BatchResultErrorEntry{stale("0"), stale("1")}}, n: 2, count: 2},
		{name: "success_and_stale", out: &awssqs.DeleteMessageBatchOutput{Successful: []*awssqs.DeleteMessageBatchResultEntry{{Id: aws.String("0")}}, Failed: []*awssqs.BatchResultErrorEntry{stale("1")}}, n: 2, count: 1},
		{name: "stale_and_denied", out: &awssqs.DeleteMessageBatchOutput{Failed: []*awssqs.BatchResultErrorEntry{stale("0"), {Id: aws.String("1"), Code: aws.String("AccessDenied")}}}, n: 2},
		{name: "stale_and_source_failure", out: &awssqs.DeleteMessageBatchOutput{Failed: []*awssqs.BatchResultErrorEntry{stale("0"), {Id: aws.String("1"), Code: aws.String("InternalError")}}}, n: 2},
		{name: "missing_result", out: &awssqs.DeleteMessageBatchOutput{Failed: []*awssqs.BatchResultErrorEntry{stale("0")}}, n: 2},
		{name: "duplicate_result", out: &awssqs.DeleteMessageBatchOutput{Failed: []*awssqs.BatchResultErrorEntry{stale("0"), stale("0")}}, n: 2},
		{name: "unknown_result", out: &awssqs.DeleteMessageBatchOutput{Failed: []*awssqs.BatchResultErrorEntry{stale("9")}}, n: 1},
		{name: "nil_result", out: &awssqs.DeleteMessageBatchOutput{Failed: []*awssqs.BatchResultErrorEntry{nil}}, n: 1},
		{name: "normal_renewal_stays_strict", out: &awssqs.DeleteMessageBatchOutput{Failed: []*awssqs.BatchResultErrorEntry{stale("0")}}, n: 1, normal: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var warnings []string
			policy := newRenewalCompatibility(func(s string) { warnings = append(warnings, s) })
			if !tc.normal {
				policy.disable()
				warnings = nil
			}
			got := policy.retainStaleDelete(tc.out, tc.err, tc.n)
			if got != (tc.count > 0) {
				t.Fatalf("retained=%v expected count=%d", got, tc.count)
			}
			if tc.count > 0 {
				if len(warnings) != 1 || !strings.Contains(warnings[0], fmt.Sprintf("delete_retained_count=%d", tc.count)) {
					t.Fatalf("incorrect counted warnings: %v", warnings)
				}
			} else if len(warnings) != 0 {
				t.Fatalf("suppressed real error: %v", warnings)
			}
		})
	}
}
