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

// stallingBody delivers its data, then blocks until released, like a vendor
// that sends part of an error body and never finishes it.
type stallingBody struct {
	data    []byte
	release chan struct{}
}

func (b *stallingBody) Read(p []byte) (int, error) {
	if len(b.data) > 0 {
		n := copy(p, b.data)
		b.data = b.data[n:]
		return n, nil
	}
	<-b.release
	return 0, io.EOF
}
func (b *stallingBody) Close() error { return nil }

func TestReadBodyReturnsBytesReadSoFarAtDeadline(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	resp := &http.Response{StatusCode: 400, Body: &stallingBody{data: []byte(`{"error":"partial vendor text`), release: release}}
	start := time.Now()
	got := readBody(resp, 50*time.Millisecond)
	elapsed := time.Since(start)
	require.Equal(t, `{"error":"partial vendor text`, string(got))
	require.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
	require.Less(t, elapsed, 5*time.Second)
}

func TestReadBodyReturnsImmediatelyAtEOFAndCapsAtReadLimit(t *testing.T) {
	start := time.Now()
	got := ReadBody(&http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader(`{"error":"busy"}`))})
	require.Equal(t, `{"error":"busy"}`, string(got))
	require.Less(t, time.Since(start), retryBodyTimeout)

	big := ReadBody(&http.Response{StatusCode: 400, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", 3*readLimit)))})
	require.Len(t, big, readLimit)
}

func TestHTTPTextDropsTokenSplitByReadLimit(t *testing.T) {
	resp := &http.Response{StatusCode: 400}
	secret := strings.Repeat("S", 64)
	// Whitespace compacts away, so the partial secret would be visible in the output.
	body := strings.Repeat(" ", readLimit-30) + "ok " + secret
	out := HTTPText(resp, []byte(body), secret)
	require.NotContains(t, out, "SSSS")
	require.Equal(t, "HTTP 400 Bad Request: ok ... [truncated]", out)

	// A secret that itself contains a delimiter character is cut mid-way.
	odd := "AAAA;BBBBBBBBBBBB"
	body = strings.Repeat(" ", readLimit-8) + "x " + odd
	out = HTTPText(resp, []byte(body), odd)
	require.NotContains(t, out, "AAAA")
	require.Contains(t, out, "[truncated]")

	// Input beyond readLimit is never examined, whatever the caller passes.
	body = "short " + strings.Repeat(" ", readLimit) + "tail-beyond-limit"
	out = HTTPText(resp, []byte(body))
	require.NotContains(t, out, "tail-beyond-limit")
}
