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
	"github.com/gorilla/websocket"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeliveryConfirmedSourceDelete(t *testing.T) {
	old := sourceLeaseBudget
	sourceLeaseBudget = 2300 * time.Millisecond
	defer func() { sourceLeaseBudget = old }()
	up := websocket.Upgrader{}
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := up.Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer c.Close()
		if c.ReadJSON(&protocol.ConnectionHeader{}) != nil {
			return
		}
		c.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageREADY})
		for {
			if _, _, e = c.ReadMessage(); e != nil {
				return
			}
		}
	}))
	defer sink.Close()
	opts := uspclient.ClientOptions{Identity: uspclient.Identity{Oid: "00000000-0000-4000-8000-000000000001", InstallationKey: "test"}, DestURL: "ws" + strings.TrimPrefix(sink.URL, "http"), Hostname: "test", Platform: "text", Architecture: "usp_adapter", DebugLog: func(string) {}, OnError: func(error) {}, OnWarning: func(string) {}}
	c, e := uspclient.NewClient(context.Background(), opts)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	var deleted atomic.Bool
	var receives atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, "test")
			return
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch r.Header.Get("X-Amz-Target") {
		case "AmazonSQS.ReceiveMessage":
			if receives.Add(1) > 1 {
				io.Copy(io.Discard, r.Body)
				<-r.Context().Done()
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"Messages": []map[string]string{{"MessageId": "test", "ReceiptHandle": "receipt", "Body": "{\"bucket\":\"test\",\"files\":[{\"path\":\"test\"}]}"}}})
		case "AmazonSQS.ChangeMessageVisibilityBatch":
			fmt.Fprint(w, `{"Successful":[{"Id":"0"}]}`)
		case "AmazonSQS.DeleteMessageBatch":
			deleted.Store(true)
			fmt.Fprint(w, `{"Successful":[{"Id":"test"}]}`)
		default:
			t.Errorf("unexpected request %s", r.Header.Get("X-Amz-Target"))
			http.Error(w, "unexpected", 400)
		}
	}))
	defer source.Close()
	sess := session.Must(session.NewSession(&aws.Config{S3ForcePathStyle: aws.Bool(true), DisableComputeChecksums: aws.Bool(true), Region: aws.String("us-east-1"), Endpoint: aws.String(source.URL), Credentials: credentials.NewStaticCredentials("test", "test", ""), MaxRetries: aws.Int(0)}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &SQSFilesAdapter{uspClient: c, ctx: ctx}
	a.conf.ClientOptions = opts
	a.conf.QueueURL = source.URL
	a.sqsClient = awssqs.New(sess)
	a.conf.BucketPath = "bucket"
	a.conf.FilePath = "files/path"
	a.bucketDownloaders = map[string]*s3manager.Downloader{"test": s3manager.NewDownloader(sess)}
	a.chFiles = make(chan fileInfo, 10)
	go a.processFiles()
	done := make(chan struct{})
	go func() { defer close(done); t.Logf("receive: %v", a.receiveEvents()) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("receiver cleanup did not join")
		}
	}()
	// Positive proof first: the source was received, downloaded and admitted
	// to the actual SDK. An idle receiver must not pass this negative-ACK test.
	deadline := time.Now().Add(5 * time.Second)
	for {
		pending, err := c.GetUnsent()
		if err != nil {
			t.Fatal(err)
		}
		if receives.Load() > 0 && len(pending) == 1 && string(pending[0].BundlePayload) == "test" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("source event was not received and admitted")
		}
		select {
		case <-done:
			t.Fatal("receiver stopped before source admission")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Only the absence-of-deletion check uses a fixed observation window.
	time.Sleep(150 * time.Millisecond)
	if deleted.Load() {
		t.Error("source deleted before delivery confirmation")
	}
	a.collectionContext()
	a.stopCollection()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("receiver did not stop")
	}
}

func TestSourceDeleteOnlyAfterReceipt(t *testing.T) {
	old := sourceLeaseBudget
	sourceLeaseBudget = 2300 * time.Millisecond
	defer func() { sourceLeaseBudget = old }()
	up := websocket.Upgrader{}
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := up.Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer c.Close()
		if c.ReadJSON(&protocol.ConnectionHeader{}) != nil {
			return
		}
		c.WriteJSON(protocol.ControlMessage{Verb: protocol.ControlMessageREADY})
		for {
			if _, _, e = c.ReadMessage(); e != nil {
				return
			}
		}
	}))
	defer sink.Close()
	opts := uspclient.ClientOptions{Identity: uspclient.Identity{Oid: "00000000-0000-4000-8000-000000000001", InstallationKey: "test"}, DestURL: "ws" + strings.TrimPrefix(sink.URL, "http"), Hostname: "test", Platform: "text", Architecture: "usp_adapter", DebugLog: func(string) {}, OnError: func(error) {}, OnWarning: func(string) {}}
	c := &receiptClient{}
	var deleted atomic.Bool
	var receives atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, "test")
			return
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch r.Header.Get("X-Amz-Target") {
		case "AmazonSQS.ReceiveMessage":
			if receives.Add(1) > 1 {
				io.Copy(io.Discard, r.Body)
				<-r.Context().Done()
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"Messages": []map[string]string{{"MessageId": "test", "ReceiptHandle": "receipt", "Body": "{\"bucket\":\"test\",\"files\":[{\"path\":\"test\"}]}"}}})
		case "AmazonSQS.ChangeMessageVisibilityBatch":
			fmt.Fprint(w, `{"Successful":[{"Id":"0"}]}`)
		case "AmazonSQS.DeleteMessageBatch":
			deleted.Store(true)
			fmt.Fprint(w, `{"Successful":[{"Id":"test"}]}`)
		default:
			t.Errorf("unexpected request %s", r.Header.Get("X-Amz-Target"))
			http.Error(w, "unexpected", 400)
		}
	}))
	defer source.Close()
	sess := session.Must(session.NewSession(&aws.Config{S3ForcePathStyle: aws.Bool(true), DisableComputeChecksums: aws.Bool(true), Region: aws.String("us-east-1"), Endpoint: aws.String(source.URL), Credentials: credentials.NewStaticCredentials("test", "test", ""), MaxRetries: aws.Int(0)}))
	a := &SQSFilesAdapter{uspClient: c, ctx: context.Background()}
	a.conf.ClientOptions = opts
	a.conf.QueueURL = source.URL
	a.sqsClient = awssqs.New(sess)
	a.conf.BucketPath = "bucket"
	a.conf.FilePath = "files/path"
	a.bucketDownloaders = map[string]*s3manager.Downloader{"test": s3manager.NewDownloader(sess)}
	a.chFiles = make(chan fileInfo, 10)
	go a.processFiles()
	done := make(chan struct{})
	go func() { defer close(done); t.Logf("receive: %v", a.receiveEvents()) }()

	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		admitted := c.next
		c.mu.Unlock()
		if admitted != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("message was not shipped")
		}
		time.Sleep(time.Millisecond)
	}
	if deleted.Load() {
		t.Fatal("source deleted before receipt")
	}
	a.isStop.Store(true)
	a.collectionContext()
	a.stopIntake()
	c.mu.Lock()
	c.pending = nil
	c.mu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("receiver did not finish")
	}
	if !deleted.Load() {
		t.Fatal("confirmed source was not deleted")
	}
}
