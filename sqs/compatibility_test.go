package usp_sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type compatibilityClient struct {
	*receiptClient
	stops atomic.Int32
}

func (c *compatibilityClient) BeginDrain() { c.stops.Add(1) }
func (c *compatibilityClient) Close() ([]*protocol.DataMessage, error) {
	c.stops.Add(1)
	return c.GetUnsent()
}
func (c *compatibilityClient) Ship(m *protocol.DataMessage, d time.Duration) error {
	if c.stops.Load() != 0 {
		return uspclient.ErrorClosed
	}
	return c.receiptClient.Ship(m, d)
}
func TestRenewalPermissionCompatibility(t *testing.T) {
	for _, denial := range []string{"http403", "access_denied", "entry_denied", "entry_partial"} {
		for _, mode := range []string{"within_original", "shutdown"} {
			t.Run(denial+"/"+mode, func(t *testing.T) {
				oldBudget, oldRPC, oldCadence, oldClose := sourceLeaseBudget, leaseRPCBudget, leaseRenewInterval, leaseCloseBudget
				sourceLeaseBudget = 5 * time.Second
				leaseRPCBudget = time.Second
				leaseRenewInterval = 20 * time.Millisecond
				leaseCloseBudget = 100 * time.Millisecond
				defer func() {
					sourceLeaseBudget = oldBudget
					leaseRPCBudget = oldRPC
					leaseRenewInterval = oldCadence
					leaseCloseBudget = oldClose
				}()
				c := &compatibilityClient{receiptClient: &receiptClient{}}
				fatal := make(chan error, 20)
				warnings := make(chan string, 20)
				allowSecond := make(chan struct{})
				secondRequested := make(chan struct{})
				var secondOnce sync.Once
				var receives, renewals atomic.Int32
				var mu sync.Mutex
				deleted := map[string]bool{}
				firstCount := 1
				if denial == "entry_partial" {
					firstCount = 2
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

					w.Header().Set("Content-Type", "application/x-amz-json-1.0")
					switch r.Header.Get("X-Amz-Target") {
					case "AmazonSQS.ReceiveMessage":
						io.Copy(io.Discard, r.Body)
						n := receives.Add(1)
						if n > 2 {
							<-r.Context().Done()
							return
						}
						var messages []map[string]string
						if n == 1 {
							for i := 0; i < firstCount; i++ {
								messages = append(messages, map[string]string{"MessageId": fmt.Sprint(i), "ReceiptHandle": fmt.Sprint("first-", i), "Body": "event"})
							}
						} else {
							if n == 2 {
								secondOnce.Do(func() { close(secondRequested) })
								select {
								case <-allowSecond:
								case <-r.Context().Done():
									return
								}

							}
							messages = append(messages, map[string]string{"MessageId": "second", "ReceiptHandle": "second", "Body": "event"})
						}
						json.NewEncoder(w).Encode(map[string]interface{}{"Messages": messages})
					case "AmazonSQS.ChangeMessageVisibilityBatch":
						renewals.Add(1)
						var req struct{ Entries []struct{ Id string } }
						json.NewDecoder(r.Body).Decode(&req)
						switch denial {
						case "http403":
							w.WriteHeader(403)
							fmt.Fprint(w, `{"__type":"Forbidden","message":"renewal denied"}`)
						case "access_denied":
							w.WriteHeader(400)
							fmt.Fprint(w, `{"__type":"AccessDenied","message":"renewal denied"}`)
						case "entry_denied":
							fmt.Fprint(w, `{"Failed":[{"Id":"0","Code":"AccessDenied","SenderFault":true}]}`)
						case "entry_partial":
							fmt.Fprint(w, `{"Successful":[{"Id":"0"}],"Failed":[{"Id":"1","Code":"AccessDenied","SenderFault":true}]}`)
						}
					case "AmazonSQS.DeleteMessageBatch":
						var req struct {
							Entries []struct{ Id, ReceiptHandle string }
						}
						json.NewDecoder(r.Body).Decode(&req)
						var success []map[string]string
						mu.Lock()
						for _, e := range req.Entries {
							deleted[e.ReceiptHandle] = true
							success = append(success, map[string]string{"Id": e.Id})
						}
						mu.Unlock()
						json.NewEncoder(w).Encode(map[string]interface{}{"Successful": success})
					default:
						t.Error("unexpected local operation")
						w.WriteHeader(400)
					}
				}))
				defer server.Close()
				sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(server.URL), S3ForcePathStyle: aws.Bool(true), DisableComputeChecksums: aws.Bool(true), Credentials: credentials.NewStaticCredentials("test", "test", ""), MaxRetries: aws.Int(0)}))
				a := &SQSAdapter{uspClient: c, ctx: context.Background(), sqsClient: awssqs.New(sess)}
				a.collectionContext()
				a.conf.QueueURL = server.URL
				a.conf.ClientOptions.DebugLog = func(string) {}
				a.conf.ClientOptions.OnWarning = func(s string) { warnings <- s }
				a.conf.ClientOptions.OnError = func(err error) { fatal <- err; a.stopCollection() }

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
				}()
				waitReview(t, "denial did not produce compatibility warning", func() bool { return len(warnings) > 0 || len(fatal) > 0 })
				if len(fatal) != 0 {
					t.Fatalf("renewal permission denied caused fatal callback: %v", <-fatal)
				}
				c.mu.Lock()
				pending := len(c.pending)
				c.mu.Unlock()
				if pending != firstCount || c.stops.Load() != 0 {
					t.Fatalf("buffered records disrupted: pending=%d lifecycle stops=%d", pending, c.stops.Load())
				}
				select {
				case <-secondRequested:
				case <-time.After(5 * time.Second):
					t.Fatal("next receive not started")
				}
				if mode == "shutdown" {
					closed := make(chan error, 1)
					go func() { closed <- a.Close() }()
					select {
					case <-closed:
					case <-time.After(10 * time.Second):
						t.Fatal("compatibility shutdown hung")
					}
					mu.Lock()
					n := len(deleted)
					mu.Unlock()
					if n != 0 || len(fatal) != 0 {
						t.Fatalf("shutdown deleted=%d fatal=%d", n, len(fatal))
					}
					return
				}
				c.mu.Lock()
				c.pending = nil
				c.mu.Unlock()
				waitReview(t, "receipt within original lease did not delete", func() bool { mu.Lock(); defer mu.Unlock(); return len(deleted) == firstCount || len(fatal) > 0 })
				if len(fatal) != 0 {
					t.Fatalf("compatibility stopped collector: %v", <-fatal)
				}
				close(allowSecond)
				waitReview(t, "same collector did not admit later batch", func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.next == uint64(firstCount+1) || len(fatal) > 0 })
				time.Sleep(100 * time.Millisecond) // one positive admission barrier, then absence of further renewal calls
				if renewals.Load() != 1 || len(warnings) != 1 || len(fatal) != 0 {
					t.Fatalf("unbounded retries/warnings: renewals=%d warnings=%d fatal=%d", renewals.Load(), len(warnings), len(fatal))
				}
				warning := <-warnings
				if !strings.Contains(warning, "count=1") {
					t.Fatalf("warning is not counted: %s", warning)
				}
				if c.stops.Load() != 0 {
					t.Fatal("compatibility drained or closed the SDK")
				}
				c.mu.Lock()
				c.pending = nil
				c.mu.Unlock() // later valid receipt
				waitReview(t, "later valid batch not cleaned up", func() bool { mu.Lock(); defer mu.Unlock(); return deleted["second"] || len(fatal) > 0 })
				if len(fatal) != 0 || a.collectionContext().Err() != nil {
					t.Fatal("collector did not remain alive")
				}
				mu.Lock()
				n := len(deleted)
				mu.Unlock()
				expected := firstCount + 1
				if n != expected {
					t.Fatalf("deletes=%d want %d", n, expected)
				}
			})
		}
	}
}

func TestRenewalPermissionDenialClassification(t *testing.T) {
	denied := func(id string) *awssqs.BatchResultErrorEntry {
		return &awssqs.BatchResultErrorEntry{Id: aws.String(id), Code: aws.String("AccessDenied")}
	}
	cases := []struct {
		name string
		out  *awssqs.ChangeMessageVisibilityBatchOutput
		err  error
		n    int
		want bool
	}{
		{"http403", nil, awserr.NewRequestFailure(awserr.New("Forbidden", "denied", nil), 403, ""), 1, true},
		{"access_code", nil, awserr.New("AccessDeniedException", "denied", nil), 1, true},
		{"renewal500", nil, awserr.NewRequestFailure(awserr.New("AccessDenied", "server failed", nil), 500, ""), 1, false},
		{"network", nil, awserr.New("RequestError", "network", io.ErrUnexpectedEOF), 1, false},
		{"canceled", nil, context.Canceled, 1, false},
		{"entry_denial", &awssqs.ChangeMessageVisibilityBatchOutput{Failed: []*awssqs.BatchResultErrorEntry{denied("0")}}, nil, 1, true},
		{"missing_entry", &awssqs.ChangeMessageVisibilityBatchOutput{Failed: []*awssqs.BatchResultErrorEntry{denied("0")}}, nil, 2, false},
		{"duplicate_entry", &awssqs.ChangeMessageVisibilityBatchOutput{Failed: []*awssqs.BatchResultErrorEntry{denied("0"), denied("0")}}, nil, 2, false},
		{"unknown_entry", &awssqs.ChangeMessageVisibilityBatchOutput{Failed: []*awssqs.BatchResultErrorEntry{denied("9")}}, nil, 1, false},
		{"denial_and_real_failure", &awssqs.ChangeMessageVisibilityBatchOutput{Failed: []*awssqs.BatchResultErrorEntry{denied("0"), {Id: aws.String("1"), Code: aws.String("InternalError")}}}, nil, 2, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := renewalPermissionDenied(tc.out, tc.err, tc.n); got != tc.want {
				t.Fatalf("permission denial=%v want %v", got, tc.want)
			}
		})
	}
}
