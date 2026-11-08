package usp_s3

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"
)

func TestListingPageFlushesBeforeNextPage(t *testing.T) {
	sink, ack, received, opts := objectSink(t)
	nextPage := make(chan struct{}, 1)
	var deletes atomic.Int32
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			if got := r.Header.Get("If-Match"); got != `"content"` {
				t.Errorf("delete condition = %q", got)
			}
			deletes.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.URL.Query().Get("list-type") == "2" {
			if r.URL.Query().Get("continuation-token") == "next" {
				nextPage <- struct{}{}
				fmt.Fprint(w, `<ListBucketResult><Contents><Key>second.log</Key><Size>12</Size></Contents></ListBucketResult>`)
			} else {
				fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>first.log</Key><Size>12</Size></Contents></ListBucketResult>`)
			}
			return
		}
		w.Header().Set("ETag", `"content"`)
		w.Header().Set("Content-Length", "12")
		w.Header().Set("Content-Range", "bytes 0-11/12")
		fmt.Fprint(w, "first\nother\n")
	}))
	defer store.Close()
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(store.URL), S3ForcePathStyle: aws.Bool(true), Credentials: credentials.AnonymousCredentials}))
	a := &S3Adapter{ctx: context.Background(), conf: S3Config{BucketName: "test", ParallelFetch: 1, ClientOptions: opts}, uspClient: sink, awsS3: s3.New(sess), awsDownloader: s3manager.NewDownloader(sess)}
	done := make(chan error, 1)
	go func() { _, err := a.lookForFiles(); done <- err }()
	defer func() {
		atomic.StoreUint32(&a.isStop, 1)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}()
	select {
	case seq := <-received:
		require.Equal(t, uint64(1), seq)
	case <-time.After(5 * time.Second):
		t.Fatal("first page not admitted")
	}
	select {
	case <-nextPage:
		t.Fatal("next page fetched before first page delivery")
	case <-time.After(75 * time.Millisecond):
	}
	require.Zero(t, deletes.Load())
	ack(1)
	select {
	case <-nextPage:
		require.Equal(t, int32(1), deletes.Load())
	case <-time.After(5 * time.Second):
		t.Fatal("next page not fetched after receipt")
	}
	select {
	case seq := <-received:
		require.Equal(t, uint64(2), seq)
	case <-time.After(5 * time.Second):
		t.Fatal("second page not admitted")
	}
	ack(2)
	select {
	case err := <-done:
		require.NoError(t, err)
		done <- err
	case <-time.After(5 * time.Second):
		t.Fatal("final page not flushed")
	}
	require.Equal(t, int32(2), deletes.Load())
}
