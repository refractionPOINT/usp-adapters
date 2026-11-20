package usp_sqs_files

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestNotificationObjectVersion(t *testing.T) {
	const native = `{"Records":[{"s3":{"bucket":{"name":"test"},"object":{"key":"events.txt","versionId":"old+/="}}}]}`
	const withoutVersion = `{"Records":[{"s3":{"bucket":{"name":"test"},"object":{"key":"events.txt"}}}]}`
	const mixed = `{"Records":[
		{"s3":{"bucket":{"name":"test"},"object":{"key":"events.txt","versionId":"old+/="}}},
		{"s3":{"bucket":{"name":"test"},"object":{"key":"events.txt"}}},
		{"s3":{"bucket":{"name":"test"},"object":{"key":"events.txt","versionId":"null"}}},
		{"s3":{"bucket":{"name":"test"},"object":{"key":"events.txt","versionId":null}}}
	]}`
	wrapped, err := json.Marshal(map[string]string{"Type": "Notification", "Message": native})
	if err != nil {
		t.Fatal(err)
	}

	type get struct {
		bucket, version string
	}
	for _, tc := range []struct {
		name, body            string
		filePath, bucketPath  string
		bucketOverride        string
		versionStatus         int
		versionError          string
		currentAccessDenied   bool
		currentIsDeleteMarker bool
		wantGets              []get
		wantPayloads          []string
		wantWarnings          int
		wantError             bool
	}{
		{
			name: "native_reads_notified_version", body: native,
			wantGets: []get{{"test", "old+/="}}, wantPayloads: []string{"old+/="},
		},
		{
			name: "absent_version_between_versioned_records", body: mixed,
			wantGets:     []get{{"test", "old+/="}, {"test", ""}, {"test", "null"}, {"test", ""}},
			wantPayloads: []string{"old+/=", "current", "null", "current"},
		},
		{
			name: "no_version_uses_current", body: withoutVersion,
			wantGets: []get{{"test", ""}}, wantPayloads: []string{"current"},
		},
		{
			name: "sns_wrapped", body: string(wrapped),
			filePath: "Message/Records/s3/object/key", bucketPath: "Message/Records/s3/bucket/name",
			wantGets: []get{{"test", "old+/="}}, wantPayloads: []string{"old+/="},
		},
		{
			name: "eventbridge", body: `{"source":"aws.s3","detail":{"bucket":{"name":"test"},"object":{"key":"events.txt","version-id":"old+/="}}}`,
			filePath: "detail/object/key", bucketPath: "detail/bucket/name",
			wantGets: []get{{"test", "old+/="}}, wantPayloads: []string{"old+/="},
		},
		{
			name: "indexed_native_path", body: mixed, filePath: "/Records/[2]/s3/object/key/",
			wantGets: []get{{"test", "null"}}, wantPayloads: []string{"null"},
		},
		{
			name: "current_delete_marker_does_not_hide_old_version", body: native, currentIsDeleteMarker: true,
			wantGets: []get{{"test", "old+/="}}, wantPayloads: []string{"old+/="},
		},
		{
			name: "permission_denied_falls_back_once", body: native,
			versionStatus: http.StatusForbidden, versionError: "AccessDenied",
			wantGets: []get{{"test", "old+/="}, {"test", ""}}, wantPayloads: []string{"current"}, wantWarnings: 1,
		},
		{
			name: "bare_forbidden_falls_back_once", body: native,
			versionStatus: http.StatusForbidden,
			wantGets:      []get{{"test", "old+/="}, {"test", ""}}, wantPayloads: []string{"current"}, wantWarnings: 1,
		},
		{
			name: "permission_denied_on_both_fetches", body: native,
			versionStatus: http.StatusForbidden, versionError: "AccessDenied", currentAccessDenied: true,
			wantGets: []get{{"test", "old+/="}, {"test", ""}}, wantWarnings: 1, wantError: true,
		},
		{
			name: "missing_version_does_not_fall_back", body: native,
			versionStatus: http.StatusNotFound, versionError: "NoSuchVersion",
			wantGets: []get{{"test", "old+/="}}, wantWarnings: 1,
		},
		{
			name: "signature_error_does_not_fall_back", body: native,
			versionStatus: http.StatusForbidden, versionError: "SignatureDoesNotMatch",
			wantGets: []get{{"test", "old+/="}}, wantError: true,
		},
		{
			name: "server_error_does_not_fall_back", body: native,
			versionStatus: http.StatusServiceUnavailable, versionError: "ServiceUnavailable",
			wantGets: []get{{"test", "old+/="}}, wantError: true,
		},
		{
			name: "fixed_bucket_does_not_use_another_buckets_version", body: native, bucketOverride: "other",
			wantGets: []get{{"other", ""}}, wantPayloads: []string{"current"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var gets []get
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				bucket := tc.wantGets[0].bucket
				if r.Method != http.MethodGet || r.URL.Path != "/"+bucket+"/events.txt" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				query := r.URL.Query()
				version := query.Get("versionId")
				if query.Has("versionId") && version == "" {
					t.Error("sent an empty versionId")
				}
				mu.Lock()
				gets = append(gets, get{bucket, version})
				mu.Unlock()
				switch {
				case version != "" && tc.versionStatus != 0:
					w.WriteHeader(tc.versionStatus)
					if tc.versionError != "" {
						fmt.Fprintf(w, "<Error><Code>%s</Code></Error>", tc.versionError)
					}
				case version != "":
					fmt.Fprint(w, version)
				case tc.currentAccessDenied:
					w.WriteHeader(http.StatusForbidden)
					fmt.Fprint(w, "<Error><Code>AccessDenied</Code></Error>")
				case tc.currentIsDeleteMarker:
					w.Header().Set("x-amz-delete-marker", "true")
					w.WriteHeader(http.StatusNotFound)
					fmt.Fprint(w, "<Error><Code>NoSuchKey</Code></Error>")
				default:
					fmt.Fprint(w, "current")
				}
			}))
			defer server.Close()

			cfg := aws.NewConfig().WithRegion("us-east-1").WithEndpoint(server.URL).
				WithCredentials(credentials.AnonymousCredentials).WithS3ForcePathStyle(true).WithMaxRetries(0)
			downloader := s3manager.NewDownloader(session.Must(session.NewSession(cfg)))
			client := &receiptClient{}
			a := &SQSFilesAdapter{
				uspClient:         client,
				chFiles:           make(chan fileInfo),
				bucketDownloaders: map[string]*s3manager.Downloader{"test": downloader, "other": downloader},
			}
			a.conf.FilePath = tc.filePath
			if a.conf.FilePath == "" {
				a.conf.FilePath = "Records/s3/object/key"
			}
			a.conf.BucketPath = tc.bucketPath
			if a.conf.BucketPath == "" {
				a.conf.BucketPath = "Records/s3/bucket/name"
			}
			a.conf.Bucket = tc.bucketOverride
			var warnings []string
			a.conf.ClientOptions.OnWarning = func(warning string) { warnings = append(warnings, warning) }
			workerDone := make(chan struct{})
			go func() {
				defer close(workerDone)
				a.processFiles()
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _, err := a.processNotification(ctx, &sqs.Message{Body: aws.String(tc.body)})
			close(a.chFiles)
			<-workerDone
			if (err != nil) != tc.wantError {
				t.Errorf("notification error = %v, want error = %t", err, tc.wantError)
			}

			mu.Lock()
			if !slices.Equal(gets, tc.wantGets) {
				t.Errorf("S3 requests = %v, want %v", gets, tc.wantGets)
			}
			mu.Unlock()
			var payloads []string
			for _, message := range client.pending {
				payloads = append(payloads, string(message.BundlePayload))
			}
			if !slices.Equal(payloads, tc.wantPayloads) {
				t.Errorf("admitted payloads = %q, want %q", payloads, tc.wantPayloads)
			}
			if len(warnings) != tc.wantWarnings {
				t.Errorf("warnings = %q, want %d", warnings, tc.wantWarnings)
			} else if tc.versionStatus == http.StatusForbidden && tc.wantWarnings == 1 &&
				!strings.Contains(warnings[0], "s3:GetObjectVersion") {
				t.Errorf("fallback warning does not explain the version permission: %q", warnings[0])
			}
		})
	}
}
