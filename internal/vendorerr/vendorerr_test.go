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
		HTTPText(resp, []byte("{\"error\":{\"code\":\"Forbidden\",\n \"message\":\"Insufficient privileges\"}}"), false))
	require.Equal(t, "HTTP 502 Bad Gateway", HTTPText(&http.Response{StatusCode: 502}, nil, false))

	// A secret straddling the truncation point must not leak a prefix.
	secret := strings.Repeat("S", 64)
	body := strings.Repeat("a", MaxBodyText-10) + secret + " tail"
	out := HTTPText(resp, []byte(body), false, secret)
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
	got, truncated := readBody(resp, 500*time.Millisecond)
	elapsed := time.Since(start)
	require.Equal(t, `{"error":"partial vendor text`, string(got))
	require.True(t, truncated)
	require.GreaterOrEqual(t, elapsed, 500*time.Millisecond)
	require.Less(t, elapsed, 5*time.Second)
}

func TestReadBodyReturnsAtEOFAndCapsAtReadLimit(t *testing.T) {
	// A long deadline: returning well before it proves EOF ends the wait, with
	// no dependence on how fast a loaded machine runs.
	start := time.Now()
	got, truncated := readBody(&http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader(`{"error":"busy"}`))}, 30*time.Second)
	require.Equal(t, `{"error":"busy"}`, string(got))
	require.False(t, truncated, "a body read to EOF is complete")
	require.Less(t, time.Since(start), 10*time.Second)

	start = time.Now()
	big, truncated := readBody(&http.Response{StatusCode: 400, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", 3*readLimit)))}, 30*time.Second)
	require.Len(t, big, readLimit)
	require.True(t, truncated, "a body cut at readLimit is reported as cut")
	require.Less(t, time.Since(start), 10*time.Second)
}

func TestHTTPTextDropsTokenSplitByReadLimit(t *testing.T) {
	resp := &http.Response{StatusCode: 400}
	secret := strings.Repeat("S", 64)
	// Whitespace compacts away, so the partial secret would be visible in the output.
	body := strings.Repeat(" ", readLimit-30) + "ok " + secret
	out := HTTPText(resp, []byte(body), false, secret)
	require.NotContains(t, out, "SSSS")
	require.Equal(t, "HTTP 400 Bad Request: ok ... [truncated]", out)

	// A secret that itself contains a delimiter character is cut mid-way.
	odd := "AAAA;BBBBBBBBBBBB"
	body = strings.Repeat(" ", readLimit-8) + "x " + odd
	out = HTTPText(resp, []byte(body), false, odd)
	require.NotContains(t, out, "AAAA")
	require.Contains(t, out, "[truncated]")

	// Input beyond readLimit is never examined, whatever the caller passes.
	body = "short " + strings.Repeat(" ", readLimit) + "tail-beyond-limit"
	out = HTTPText(resp, []byte(body), false)
	require.NotContains(t, out, "tail-beyond-limit")
}

func TestHTTPTextDropsTokenSplitByReadDeadline(t *testing.T) {
	resp := &http.Response{StatusCode: 400}
	release := make(chan struct{})
	defer close(release)
	// The vendor stalls after sending the first part of a credential, which is
	// far shorter than readLimit.
	secret := "abcdEFGHijklMNOPqrstUVWX0123456789"
	data := `{"error":"denied","hint":"` + secret[:20]
	got, truncated := readBody(&http.Response{StatusCode: 400, Body: &stallingBody{data: []byte(data), release: release}}, 500*time.Millisecond)
	require.True(t, truncated, "a body cut by the deadline is reported as cut")
	require.Equal(t, data, string(got))
	out := HTTPText(resp, got, truncated, secret)
	require.NotContains(t, out, secret[:8])
	require.Equal(t, `HTTP 400 Bad Request: {"error":"denied","hint":" ... [truncated]`, out)

	// A complete plain-text body without any delimiter is not emptied.
	require.Equal(t, "HTTP 400 Bad Request: nodelimiter", HTTPText(resp, []byte("nodelimiter"), false))
}

func TestHTTPTextDropsJSONEscapedSecretCutMidWay(t *testing.T) {
	resp := &http.Response{StatusCode: 400}
	// The vendor echoes the secret JSON-escaped (\"), so the cut leaves a prefix
	// of the escaped form that is not a prefix of the raw secret.
	secret := `abcd"efghijklmnop`
	data := `{"detail":"x abcd\"efgh`
	out := HTTPText(resp, []byte(data), true, secret)
	require.NotContains(t, out, "abcd")
	require.Equal(t, `HTTP 400 Bad Request: {"detail":"x ... [truncated]`, out)
}

func TestWrapAndKindClipLongTextAfterRedaction(t *testing.T) {
	// 2500 bytes of words: the clip at MaxBodyText lands inside a word.
	long := strings.Repeat("word ", 500)
	clipped := strings.Repeat("word ", MaxBodyText/5) + " ... [truncated]"

	cause := errors.New(long)
	wrapped := Wrap("Receive()", cause)
	require.ErrorIs(t, wrapped, cause, "clipping the text must not break unwrapping")
	require.Equal(t, "Receive(): "+clipped, wrapped.Error())

	kind := errors.New("temporarily unavailable")
	err := Kind(kind, long, cause)
	require.ErrorIs(t, err, kind)
	require.Equal(t, "temporarily unavailable: "+clipped, err.Error())

	// Redaction runs on the full text first: a secret that straddles the clip
	// point is replaced whole, never half-shown.
	secret := strings.Repeat("S", 64)
	text := strings.Repeat("a ", MaxBodyText/2-10) + secret + " tail"
	require.NotContains(t, Wrap("", errors.New(text), secret).Error(), "SSSS")

	// Short text is untouched.
	require.Equal(t, "op: short", Wrap("op", errors.New("short")).Error())
}

func TestClipRedactedNeverEndsInsideSecretPrefix(t *testing.T) {
	// Defence in depth: even text that was not redacted must not end with the
	// first part of a configured secret (raw or escaped) after the clip.
	for _, secret := range []string{"AAAA;BBBBBBBBBBBB", `AAAA"BBBBBBBBBBBB`} {
		form := secretForms(secret)[len(secretForms(secret))-1] // the escaped form when there is one
		text := strings.Repeat("a ", (MaxBodyText-8)/2) + form + " tail"
		out := clipRedacted(text, []string{secret})
		require.NotContains(t, out, "AAAA", secret)
		require.True(t, strings.HasSuffix(out, " ... [truncated]"))
	}
}

func TestRedactURLCredentials(t *testing.T) {
	cases := []struct {
		name, in string
		hidden   []string
		shown    []string
	}{
		{"query token", `next link rejected: https://example.test/api/v1/logs?after=abc&token=unknown-token-123456789&limit=5`, []string{"unknown-token-123456789"}, []string{"after=abc", "limit=5"}},
		{"userinfo", `rejected: http://alice:unknown-password-123@example.test/api/v1/logs?after=abc`, []string{"alice", "unknown-password-123"}, []string{"@example.test/api/v1/logs?after=abc"}},
		{"user only userinfo", `Get "https://tok123456@example.test/x": dial tcp: refused`, []string{"tok123456"}, []string{"dial tcp: refused"}},
		{"mixed case param", `<https://example.test/p?Access_Token=AbC987&Code=zzz-111>; rel="next"`, []string{"AbC987", "zzz-111"}, []string{`rel="next"`}},
		{"api key and signature", `https://example.test/p?apikey=k-1&api_key=k-2&signature=s-3&session=s-4&key=k-5&client_secret=c-6&assertion=a-7`, []string{"k-1", "k-2", "s-3", "s-4", "k-5", "c-6", "a-7"}, []string{"https://example.test/p?"}},
		{"json escaped ampersand", `{"next":"https:\/\/example.test\/p?$skiptoken=cursor\u0026token=unknown-token-123456789"}`, []string{"unknown-token-123456789"}, []string{"$skiptoken=cursor"}},
		{"percent encoded name", `rejected https://example.test/api/v1/logs?after=1&to%6ben=unknown-token-123456789&limit=5`, []string{"unknown-token-123456789"}, []string{"after=1", "limit=5"}},
		{"encoded separators", `bad link https://example.test/p?after=1%26token%3Dunknown-token-123456789&limit=5`, []string{"unknown-token-123456789"}, []string{"limit=5"}},
		{"encoded separator inside value", `bad link https://example.test/p?next=x%26access_token=unknown-token-123456789`, []string{"unknown-token-123456789"}, []string{"next="}},
		{"html ampersand", `<a href="https://example.test/p?after=1&amp;token=unknown-token-123456789">`, []string{"unknown-token-123456789"}, []string{"after=1"}},
		{"fragment token", `redirect https://example.test/cb#access_token=unknown-token-123456789&state=ok`, []string{"unknown-token-123456789"}, []string{"state=ok"}},
		{"json escaped userinfo", `{"url":"https:\/\/alice:unknown-password-123@example.test\/x"}`, []string{"alice", "unknown-password-123"}, []string{"@example.test"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := Redact(c.in)
			for _, h := range c.hidden {
				require.NotContains(t, out, h)
			}
			for _, s := range c.shown {
				require.Contains(t, out, s)
			}
			require.Contains(t, out, redacted)
		})
	}
	// Words that are only prose, or parameters that are not credentials, stay.
	for _, keep := range []string{
		"the error code was E0000001 for this key rotation",
		"https://example.test/p?after=abc&limit=10&filter=eventType",
		"see https://example.test/docs/page for the session policy",
	} {
		require.Equal(t, keep, Redact(keep))
	}
}
