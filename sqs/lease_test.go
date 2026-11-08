package usp_sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type leaseBlockedClient struct{ *receiptClient }

func (c *leaseBlockedClient) Ship(_ *protocol.DataMessage, timeout time.Duration) error {
	time.Sleep(timeout)
	return uspclient.ErrorBufferFull
}
func TestVisibilityRenewalLifecycle(t *testing.T) {
	for _, kind := range []string{"success", "failed", "lease_lost_ship", "lease_lost_download", "ambiguous", "partial", "omitted", "duplicate", "unknown", "expired_receive", "shutdown", "shutdown_rpc", "ack_during_renewal", "late_success", "max_lifetime"} {
		t.Run(kind, func(t *testing.T) {
			oldBudget, oldRPC, oldShip, oldClose, oldLifetime := sourceLeaseBudget, leaseRPCBudget, leaseShipSlice, leaseCloseBudget, leaseMaxLifetime
			sourceLeaseBudget = 10 * time.Second
			oldInterval := leaseRenewInterval
			leaseRenewInterval = 20 * time.Millisecond
			defer func() { leaseRenewInterval = oldInterval }()
			leaseRPCBudget = 2 * time.Second
			if kind == "ambiguous" || kind == "late_success" {
				leaseRPCBudget = 100 * time.Millisecond
			}
			leaseShipSlice = 5 * time.Millisecond
			leaseCloseBudget = 200 * time.Millisecond
			leaseMaxLifetime = 20 * time.Second
			if kind == "max_lifetime" {
				leaseMaxLifetime = 280 * time.Millisecond
			}
			defer func() {
				sourceLeaseBudget = oldBudget
				leaseRPCBudget = oldRPC
				leaseShipSlice = oldShip
				leaseCloseBudget = oldClose
				leaseMaxLifetime = oldLifetime
			}()
			c := &receiptClient{}
			var renewals, deletes, receives, activeRenew atomic.Int32
			twoRenewals := make(chan struct{})
			renewStarted := make(chan struct{})
			var once, renewOnce sync.Once
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

				w.Header().Set("Content-Type", "application/x-amz-json-1.0")
				switch r.Header.Get("X-Amz-Target") {
				case "AmazonSQS.ReceiveMessage":
					if receives.Add(1) > 1 {
						t.Error("unexpected second receive")
						w.WriteHeader(400)
						return
					}
					if kind == "expired_receive" {
						io.Copy(io.Discard, r.Body)
						<-r.Context().Done()
						return
					}
					body := "test"
					json.NewEncoder(w).Encode(map[string]interface{}{"Messages": []map[string]string{
						{"MessageId": "first", "ReceiptHandle": "receipt-first", "Body": body},
						{"MessageId": "second", "ReceiptHandle": "receipt-second", "Body": body}}})
				case "AmazonSQS.ChangeMessageVisibilityBatch":
					activeRenew.Add(1)
					defer activeRenew.Add(-1)
					n := renewals.Add(1)
					var req struct {
						Entries []struct {
							Id, ReceiptHandle string
							VisibilityTimeout int64
						}
					}
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					if len(req.Entries) != 2 {
						t.Errorf("renew entries=%d", len(req.Entries))
					}
					for i, e := range req.Entries {
						if e.Id != fmt.Sprint(i) || e.ReceiptHandle != []string{"receipt-first", "receipt-second"}[i] || e.VisibilityTimeout != 60 {
							t.Errorf("unexpected renewal entry %+v", e)
						}
					}
					renewOnce.Do(func() { close(renewStarted) })
					if kind == "late_success" {
						time.Sleep(200 * time.Millisecond)
					}
					if kind == "ambiguous" || kind == "shutdown_rpc" || kind == "ack_during_renewal" {
						<-r.Context().Done()
						return
					}
					if kind == "failed" || kind == "lease_lost_ship" || kind == "lease_lost_download" {
						w.WriteHeader(400)
						fmt.Fprint(w, `{"__type":"InvalidParameterValue","message":"renewal refused"}`)
						return
					}
					if kind == "partial" {
						fmt.Fprint(w, `{"Successful":[{"Id":"0"}],"Failed":[{"Id":"1","Code":"InternalError","SenderFault":false}]}`)
						return
					}
					if kind == "omitted" {
						fmt.Fprint(w, `{"Successful":[{"Id":"0"}]}`)
						return
					}
					if kind == "duplicate" {
						fmt.Fprint(w, `{"Successful":[{"Id":"0"},{"Id":"0"}]}`)
						return
					}
					if kind == "unknown" {
						fmt.Fprint(w, `{"Successful":[{"Id":"0"},{"Id":"unknown"}]}`)
						return
					}
					fmt.Fprint(w, `{"Successful":[{"Id":"0"},{"Id":"1"}]}`)
					if n >= 4 {
						once.Do(func() { close(twoRenewals) })
					}
				case "AmazonSQS.DeleteMessageBatch":
					deletes.Add(1)
					if activeRenew.Load() != 0 {
						t.Error("cleanup raced live renewal")
					}
					if kind != "success" {
						t.Errorf("cleanup after %s", kind)
					}
					fmt.Fprint(w, `{"Successful":[{"Id":"0"},{"Id":"1"}]}`)
				default:
					t.Errorf("unexpected request %s", r.Header.Get("X-Amz-Target"))
					w.WriteHeader(400)
				}
			}))
			defer source.Close()
			sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(source.URL), S3ForcePathStyle: aws.Bool(true), DisableComputeChecksums: aws.Bool(true), Credentials: credentials.NewStaticCredentials("test", "test", ""), MaxRetries: aws.Int(0)}))
			a := &SQSAdapter{uspClient: c, ctx: context.Background(), sqsClient: awssqs.New(sess)}
			if kind == "lease_lost_ship" {
				a.uspClient = &leaseBlockedClient{c}
			}
			a.collectionContext()
			defer a.stopCollection()
			a.conf.QueueURL = source.URL
			a.conf.ClientOptions.DebugLog = func(string) {}
			a.conf.ClientOptions.OnWarning = func(string) {}
			reported := make(chan error, 10)
			a.conf.ClientOptions.OnError = func(e error) { reported <- e }

			done := make(chan error, 1)
			stopped := make(chan struct{})
			a.receiveStopped = stopped
			messages := []*awssqs.Message{
				{MessageId: aws.String("first"), ReceiptHandle: aws.String("receipt-first"), Body: aws.String("test")},
				{MessageId: aws.String("second"), ReceiptHandle: aws.String("receipt-second"), Body: aws.String("test")}}
			receivedBefore := time.Now()
			if kind == "expired_receive" {
				receivedBefore = receivedBefore.Add(-sourceLeaseBudget - time.Second)
			}
			go func() { defer close(stopped); done <- a.handleBatch(a.collectionContext(), messages, receivedBefore) }()

			if kind == "success" {
				select {
				case <-twoRenewals:
				case <-time.After(5 * time.Second):
					t.Fatal("long work did not survive two renewals")
				}
				a.isStop.Store(true)
				c.mu.Lock()
				c.autoACK = true
				c.mu.Unlock()
			} else if kind == "ack_during_renewal" {
				select {
				case <-renewStarted:
				case <-time.After(5 * time.Second):
					t.Fatal("renewal not started")
				}
				a.isStop.Store(true)
				c.mu.Lock()
				c.autoACK = true
				c.mu.Unlock()
			} else if kind == "shutdown" || kind == "shutdown_rpc" {
				select {
				case <-renewStarted:
				case <-time.After(5 * time.Second):
					t.Fatal("renewal not started")
				}
				closed := make(chan error, 1)
				go func() { closed <- a.Close() }()
				select {
				case err := <-closed:
					if err != nil {
						t.Logf("close retains pending data: %v", err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("Close hung")
				}

			}
			select {
			case err := <-done:
				if kind == "success" && err != nil {
					t.Fatalf("long work failed: %v", err)
				}
				if kind != "success" && err == nil {
					t.Fatal("uncertain lease reported success")
				}
			case <-time.After(10 * time.Second):
				a.collectionContext()
				a.stopCollection()
				t.Fatal("receiver did not stop")
			}

			if kind == "success" {
				if renewals.Load() < 2 || deletes.Load() != 1 {
					t.Fatalf("renewals=%d deletes=%d", renewals.Load(), deletes.Load())
				}
			}
			if kind != "success" && deletes.Load() != 0 {
				t.Fatal("uncertain batch deleted")
			}
			if len(reported) != 0 {
				t.Fatalf("OnError reports=%d", len(reported))
			}
			// HTTP handler teardown can follow client-side RPC cancellation.
			settle := time.Now().Add(5 * time.Second)
			for activeRenew.Load() != 0 && time.Now().Before(settle) {
				time.Sleep(time.Millisecond)
			}
			if activeRenew.Load() != 0 {
				t.Fatal("renewal RPC not joined")
			}
			after := renewals.Load()
			time.Sleep(80 * time.Millisecond)
			if renewals.Load() != after {
				t.Fatal("renewal continued after receiver exit")
			}
		})
	}
}

func TestCleanupRefusesExpiredStoppedLease(t *testing.T) {
	l := newBatchLease(context.Background(), nil, "test", nil, time.Now())
	if err := l.stopAndJoin(); err != nil {
		t.Fatal(err)
	}
	defer l.abort()
	// Advance the stopped lease's known deadline deterministically. No tiny
	// happy-path RPC or scheduler budget is required to test expiry.
	l.mu.Lock()
	l.deadline = time.Now().Add(-time.Second)
	l.mu.Unlock()
	ctx, cancel, err := l.cleanupContext()
	if cancel != nil {
		cancel()
	}
	if err == nil || ctx != nil {
		t.Fatal("cleanup authorized after known lease expired")
	}
}

func TestCloseIsBoundedForStuckProducer(t *testing.T) {
	old := leaseCloseBudget
	leaseCloseBudget = 20 * time.Millisecond
	defer func() { leaseCloseBudget = old }()
	c := &receiptClient{}
	a := &SQSAdapter{uspClient: c}
	a.conf.ClientOptions.DebugLog = func(string) {}
	a.conf.ClientOptions.OnWarning = func(string) {}
	stopped := make(chan struct{})
	a.receiveStopped = stopped
	closed := make(chan error, 1)
	go func() { closed <- a.Close() }()
	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("stuck producer was not reported")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung")
	}

	if a.collectionContext().Err() == nil {
		t.Fatal("Close did not cancel work")
	}
	close(stopped)
}
