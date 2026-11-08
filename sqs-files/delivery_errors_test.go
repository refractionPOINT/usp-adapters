package usp_sqs_files

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestReceiverFailuresReachOnError(t *testing.T) {
	for _, kind := range []string{"ship", "observe", "renewal_uncertain", "delete_rpc", "delete_partial", "parse", "download", "decode"} {
		t.Run(kind, func(t *testing.T) {
			old := sourceLeaseBudget
			sourceLeaseBudget = 150 * time.Millisecond
			defer func() { sourceLeaseBudget = old }()
			c := &receiptClient{autoACK: true}
			sentinel := errors.New("test failure")
			if kind == "ship" {
				c.shipErr = sentinel
			}
			if kind == "observe" {
				c.observeErr = sentinel
			}
			if kind == "renewal_uncertain" {
				c.autoACK = false
			}
			body := `{"bucket":"test","files":[{"path":"test.txt"}]}`
			if kind == "parse" {
				body = "{"
			}
			if kind == "decode" {
				body = `{"bucket":"test","files":[{"path":"test.parquet"}]}`
			}
			var deletes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					if kind == "download" {
						http.Error(w, "not found", http.StatusNotFound)
						return
					}
					fmt.Fprint(w, "not a parquet file")
					return
				}
				w.Header().Set("Content-Type", "application/x-amz-json-1.0")
				switch r.Header.Get("X-Amz-Target") {
				case "AmazonSQS.ReceiveMessage":
					json.NewEncoder(w).Encode(map[string]interface{}{"Messages": []map[string]string{{"MessageId": "test", "ReceiptHandle": "test", "Body": body}}})
				case "AmazonSQS.ChangeMessageVisibilityBatch":
					fmt.Fprint(w, `{"Successful":[]}`)
				case "AmazonSQS.DeleteMessageBatch":
					deletes.Add(1)
					if kind == "delete_rpc" {
						w.WriteHeader(400)
						fmt.Fprint(w, `{"__type":"InvalidParameterValue","message":"test delete failure"}`)
						return
					}
					if kind == "delete_partial" {
						fmt.Fprint(w, `{"Failed":[{"Id":"test","Code":"InternalError","SenderFault":false}]}`)
						return
					}
					t.Error("uncertain batch reached source delete")
					fmt.Fprint(w, `{"Failed":[{"Id":"test","Code":"InternalError"}]}`)
				default:
					t.Errorf("unexpected request %s", r.Header.Get("X-Amz-Target"))
					w.WriteHeader(400)
				}
			}))
			defer server.Close()
			sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(server.URL), S3ForcePathStyle: aws.Bool(true), DisableComputeChecksums: aws.Bool(true), Credentials: credentials.NewStaticCredentials("test", "test", ""), MaxRetries: aws.Int(0)}))
			a := &SQSFilesAdapter{uspClient: c, ctx: context.Background(), sqsClient: awssqs.New(sess)}
			a.conf.QueueURL = server.URL
			a.conf.ClientOptions.DebugLog = func(string) {}
			reported := make(chan error, 10)
			a.conf.ClientOptions.OnError = func(err error) { reported <- err }
			a.conf.BucketPath = "bucket"
			a.conf.FilePath = "files/path"
			a.isS3Inited = true
			a.awsDownloader = s3manager.NewDownloader(sess)
			a.chFiles = make(chan fileInfo)
			workerDone := make(chan struct{})
			go func() { defer close(workerDone); a.processFiles() }()
			defer func() { <-workerDone }()
			if err := a.receiveEvents(); err == nil {
				t.Fatal("source failure was hidden")
			}
			if len(reported) != 1 {
				t.Fatalf("OnError calls=%d want 1", len(reported))
			}
			if err := <-reported; err == nil {
				t.Fatal("empty OnError")
			}
			if kind == "delete_rpc" || kind == "delete_partial" {
				if deletes.Load() != 1 {
					t.Fatalf("delete calls=%d", deletes.Load())
				}
			} else if deletes.Load() != 0 {
				t.Fatal("failed processing deleted source")
			}
		})
	}
}
