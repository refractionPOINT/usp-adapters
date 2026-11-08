package usp_s3

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/refractionPOINT/go-uspclient"
)

// Oversized objects exercise the listing-to-pipeline boundary without downloads
// or telemetry connections. Every listed key must reach the existing warning.
func TestListingPagination(t *testing.T) {
	for _, tc := range []struct {
		name       string
		fail       bool
		empty      bool
		continuous bool
		repeated   bool
	}{
		{name: "multiple_pages"}, {name: "empty_intermediate_page", empty: true}, {name: "continuation_failure", fail: true},
		{name: "continuous_continuation_failure", fail: true, continuous: true},
		{name: "repeated_token", repeated: true},
		{name: "continuous_repeated_token", repeated: true, continuous: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := []string{}
			warnings := []string{}
			errorsSeen := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				token := r.URL.Query().Get("continuation-token")
				requests = append(requests, token)
				if r.URL.Query().Get("prefix") != "logs/" {
					t.Errorf("prefix lost: %s", r.URL)
				}
				if token == "next+/=" && tc.fail {
					w.WriteHeader(503)
					fmt.Fprint(w, `<Error><Code>ServiceUnavailable</Code></Error>`)
					return
				}
				next := ""
				key := "first"
				truncated := false
				switch token {
				case "":
					next = "next+/="
					truncated = true
				case "next+/=":
					key = "second"
					if tc.repeated {
						next = token
						truncated = true
					}
					if tc.empty {
						key = ""
						next = "last"
						truncated = true
					}
				case "last":
					key = "third"
				default:
					t.Errorf("unexpected token: %q", token)
				}
				fmt.Fprintf(w, `<ListBucketResult><IsTruncated>%t</IsTruncated><NextContinuationToken>%s</NextContinuationToken>`, truncated, next)
				if key != "" {
					fmt.Fprintf(w, `<Contents><Key>%s</Key><Size>%d</Size></Contents>`, key, maxObjectSize+1)
				}
				fmt.Fprint(w, `</ListBucketResult>`)
			}))
			defer server.Close()
			sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(server.URL), S3ForcePathStyle: aws.Bool(true), Credentials: credentials.AnonymousCredentials, MaxRetries: aws.Int(0)}))
			a := &S3Adapter{awsS3: s3.New(sess), conf: S3Config{BucketName: "test-bucket", Prefix: "logs/", IsOneTimeLoad: !tc.continuous, ParallelFetch: 1, ClientOptions: uspclient.ClientOptions{OnError: func(error) { errorsSeen++ }, OnWarning: func(s string) { warnings = append(warnings, s) }}}}
			_, err := a.lookForFiles()
			wantRequests := 2
			if tc.empty {
				wantRequests = 3
			}
			if len(requests) != wantRequests {
				t.Fatalf("listing requests = %q, want %d", requests, wantRequests)
			}
			if tc.fail {
				if len(warnings) != 1 || !strings.Contains(warnings[0], "first") {
					t.Fatalf("first page not processed before continuation failure: %v", warnings)
				}
				if (err != nil) != !tc.continuous || errorsSeen != 1 {
					t.Fatalf("continuation failure hidden: err=%v callbacks=%d", err, errorsSeen)
				}
				return
			}
			if tc.repeated {
				if (err != nil) != !tc.continuous || errorsSeen != 1 || len(warnings) != 2 {
					t.Fatalf("repeated token handling: err=%v callbacks=%d warnings=%v", err, errorsSeen, warnings)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(warnings) != 2 || !strings.Contains(warnings[0], "first") {
				t.Fatalf("listed objects not processed: %v", warnings)
			}
		})
	}
}
