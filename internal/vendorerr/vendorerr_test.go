package vendorerr

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRedactRemovesSecretsAndKeepsVendorText(t *testing.T) {
	cases := []struct {
		name, in string
		secrets  []string
		hidden   []string
		shown    []string
	}{
		{"configured secret", "AADSTS7000215: invalid client secret hunter2-value for app", []string{"hunter2-value"}, []string{"hunter2-value"}, []string{"AADSTS7000215", "for app"}},
		{"query-escaped secret", "GET /x?client_secret=a%2Bb%2Fc failed", []string{"a+b/c"}, []string{"a%2Bb%2Fc"}, []string{"failed"}},
		{"json-escaped secret", `{"detail":"bad key p\"w\\d-9876"}`, []string{`p"w\d-9876`}, []string{`p\"w`, `d-9876`}, []string{"bad key"}},
		{"sas sig", "Put https://ns.servicebus.windows.net/hub?sr=x&sig=AbC123%2Bsig&se=99 denied", nil, []string{"AbC123"}, []string{"ns.servicebus.windows.net", "denied"}},
		{"shared access key", "Endpoint=sb://ns/;SharedAccessKeyName=root;SharedAccessKey=c2VjcmV0cw==;EntityPath=h", nil, []string{"c2VjcmV0cw"}, []string{"SharedAccessKeyName=root", "EntityPath=h"}},
		{"sas token", "token SharedAccessSignature sr=ns&sig=zzz&se=1&skn=root rejected", nil, []string{"zzz"}, []string{"rejected"}},
		{"authorization header", "request headers: Authorization: Bearer eyJhbGciOi.payload.sig123 end", nil, []string{"eyJhbGciOi"}, []string{"end"}},
		{"ssws", `sent "Authorization":"SSWS 00abcdefghijklmnopqrstuvwx" and failed`, nil, []string{"00abcdefghijklmnopqrstuvwx"}, []string{"failed"}},
		{"bare bearer", "invalid token Bearer abcdefghijklmnop0123456789 in request", nil, []string{"abcdefghijklmnop0123456789"}, []string{"in request"}},
		{"json token fields", `{"access_token":"tok-1234","client_secret":"cs-5678","error":"invalid_grant"}`, nil, []string{"tok-1234", "cs-5678"}, []string{"invalid_grant"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := Redact(c.in, c.secrets...)
			for _, h := range c.hidden {
				require.NotContains(t, out, h)
			}
			for _, s := range c.shown {
				require.Contains(t, out, s)
			}
			require.Contains(t, out, redacted)
		})
	}
	// Ordinary prose that merely mentions the words is left alone.
	prose := "Bearer authentication is required; the secret was not provided"
	require.Equal(t, prose, Redact(prose))
}

func TestHTTPTextReportsStatusAndBodyAndTruncatesAfterRedaction(t *testing.T) {
	resp := &http.Response{StatusCode: 403}
	require.Equal(t, `HTTP 403 Forbidden: {"error":{"code":"Forbidden", "message":"Insufficient privileges"}}`,
		HTTPText(resp, []byte("{\"error\":{\"code\":\"Forbidden\",\n \"message\":\"Insufficient privileges\"}}")))
	require.Equal(t, "HTTP 502 Bad Gateway", HTTPText(&http.Response{StatusCode: 502}, nil))

	// A secret straddling the truncation point must not leak a prefix.
	secret := strings.Repeat("S", 64)
	body := strings.Repeat("a", MaxBodyText-10) + secret + " tail"
	out := HTTPText(resp, []byte(body), secret)
	require.NotContains(t, out, "SSSS")
	require.Contains(t, out, "[truncated]")
	require.LessOrEqual(t, len(out), len("HTTP 403 Forbidden: ")+MaxBodyText+len(" ... [truncated]"))
}

func TestKindKeepsSentinelAndCauseForErrorsIs(t *testing.T) {
	kind := errors.New("temporarily unavailable")
	cause := context.DeadlineExceeded
	err := Kind(kind, "dial tcp: i/o timeout token=abc123456", cause, "abc123456")
	require.ErrorIs(t, err, kind)
	require.ErrorIs(t, err, cause)
	require.Equal(t, "temporarily unavailable: dial tcp: i/o timeout token=[REDACTED]", err.Error())

	wrapped := Wrap("GetRuntimeInformation()", cause)
	require.ErrorIs(t, wrapped, cause)
	require.Nil(t, Wrap("x", nil))
}

type streamingBody struct{ release chan struct{} }

func (b streamingBody) Read([]byte) (int, error) { <-b.release; return 0, io.EOF }
func (b streamingBody) Close() error             { return nil }

func TestReadBodyDoesNotBlockOnEndlessBody(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	start := time.Now()
	require.Nil(t, ReadBody(&http.Response{Body: streamingBody{release}}))
	require.Less(t, time.Since(start), 2*readTimeout)
}
