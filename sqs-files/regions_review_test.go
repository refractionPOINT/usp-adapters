package usp_sqs_files

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDynamicNotificationBucketsUseTheirOwnRegions(t *testing.T) {
	regions := map[string]string{"east-bucket": "us-east-1", "west-bucket": "us-west-2"}
	var mu sync.Mutex
	heads := map[string]int{}
	gets := map[string]int{}
	deleted := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")[0]
		switch r.Method {
		case "HEAD":
			mu.Lock()
			heads[bucket]++
			mu.Unlock()
			w.Header().Set("X-Amz-Bucket-Region", regions[bucket])
		case "GET":
			if !strings.Contains(r.Header.Get("Authorization"), "/"+regions[bucket]+"/s3/aws4_request") {
				http.Error(w, "wrong region", 403)
				return
			}
			mu.Lock()
			gets[bucket]++
			mu.Unlock()
			fmt.Fprint(w, "synthetic event")
		default:
			if r.Header.Get("X-Amz-Target") != "AmazonSQS.DeleteMessageBatch" {
				t.Error("unexpected API operation")
				w.WriteHeader(400)
				return
			}
			var req struct{ Entries []struct{ Id string } }
			json.NewDecoder(r.Body).Decode(&req)
			var success []map[string]string
			for _, e := range req.Entries {
				success = append(success, map[string]string{"Id": e.Id})
			}
			mu.Lock()
			deleted += len(req.Entries)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/x-amz-json-1.0")
			json.NewEncoder(w).Encode(map[string]interface{}{"Successful": success})
		}
	}))
	defer server.Close()
	cfg := &aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(server.URL), S3ForcePathStyle: aws.Bool(true), DisableComputeChecksums: aws.Bool(true), Credentials: credentials.NewStaticCredentials("test", "test", ""), MaxRetries: aws.Int(0)}
	sess := session.Must(session.NewSession(cfg))
	c := &receiptClient{autoACK: true}
	a := &SQSFilesAdapter{uspClient: c, ctx: context.Background(), awsConfig: cfg, awsSession: sess, sqsClient: awssqs.New(sess), chFiles: make(chan fileInfo)}
	a.conf.QueueURL = server.URL
	a.conf.BucketPath = "Records/s3/bucket/name"
	a.conf.FilePath = "Records/s3/object/key"
	a.conf.ClientOptions.OnError = func(err error) { t.Errorf("notification failure: %v", err) }
	for i := 0; i < 2; i++ {
		a.wg.Add(1)
		go func() { defer a.wg.Done(); a.processFiles() }()
	}
	defer func() { close(a.chFiles); a.wg.Wait() }()
	var messages []*awssqs.Message
	for i, bucket := range []string{"east-bucket", "west-bucket", "east-bucket"} {
		body := fmt.Sprintf(`{"Records":[{"s3":{"bucket":{"name":"%s"},"object":{"key":"events.txt"}}}]}`, bucket)
		messages = append(messages, &awssqs.Message{MessageId: aws.String(fmt.Sprint(i)), ReceiptHandle: aws.String(fmt.Sprint(i)), Body: aws.String(body)})
	}
	if err := a.handleBatch(context.Background(), messages, time.Now()); err != nil {
		t.Fatalf("multi-region notification failed: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if heads["east-bucket"] != 1 || heads["west-bucket"] != 1 {
		t.Fatalf("bucket cache lookups=%v", heads)
	}
	if gets["east-bucket"] != 2 || gets["west-bucket"] != 1 || deleted != 3 {
		t.Fatalf("gets=%v deleted=%d", gets, deleted)
	}
}
