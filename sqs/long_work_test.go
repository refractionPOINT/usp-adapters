package usp_sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestLongWorkOutlivesInitialVisibilityBudget(t *testing.T) {
	old := sourceLeaseBudget
	sourceLeaseBudget = 2 * time.Second
	defer func() { sourceLeaseBudget = old }()
	c := &receiptClient{}
	var renewed, deleted atomic.Int32
	var received atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch r.Header.Get("X-Amz-Target") {
		case "AmazonSQS.ReceiveMessage":
			if received.Add(1) > 1 {
				io.Copy(io.Discard, r.Body)
				<-r.Context().Done()
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"Messages": []map[string]string{{"MessageId": "test", "ReceiptHandle": "test", "Body": "test"}}})
		case "AmazonSQS.ChangeMessageVisibilityBatch":
			renewed.Add(1)
			fmt.Fprint(w, `{"Successful":[{"Id":"0"}]}`)
		case "AmazonSQS.DeleteMessageBatch":
			deleted.Add(1)
			fmt.Fprint(w, `{"Successful":[{"Id":"0"}]}`)
		default:
			t.Error("unexpected request")
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(server.URL), S3ForcePathStyle: aws.Bool(true), DisableComputeChecksums: aws.Bool(true), Credentials: credentials.NewStaticCredentials("test", "test", ""), MaxRetries: aws.Int(0)}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &SQSAdapter{uspClient: c, ctx: ctx, sqsClient: awssqs.New(sess)}
	a.conf.QueueURL = server.URL
	a.conf.ClientOptions.DebugLog = func(string) {}
	a.conf.ClientOptions.OnError = func(error) {}

	done := make(chan error, 1)
	go func() { done <- a.receiveEvents() }()
	time.Sleep(2500 * time.Millisecond) // longer than initial two-second budget
	a.isStop.Store(true)
	a.collectionContext()
	a.stopIntake()
	c.mu.Lock()
	c.autoACK = true
	c.mu.Unlock()
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("legitimate long work failed: %v; renewals=%d deletions=%d", err, renewed.Load(), deleted.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("long work failed to finish")
	}

	if renewed.Load() < 2 || deleted.Load() != 1 {
		t.Fatalf("renewals=%d deletes=%d", renewed.Load(), deleted.Load())
	}
}
