package vendorerr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
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
	// first part of a configured secret after the clip.
	for _, secret := range []string{"AAAA;BBBBBBBBBBBB", `AAAA"BBBBBBBBBBBB`} {
		text := strings.Repeat("a ", (MaxBodyText-8)/2) + secret + " tail"
		out := clipRedacted(text, []string{secret})
		require.NotContains(t, out, "AAAA", secret)
		require.True(t, strings.HasSuffix(out, " ... [truncated]"))
	}
}

// A configured secret containing a tab or a newline is found when the vendor
// JSON-escaped it, and so is every JSON short escape in other text.
func TestRedactDecodesJSONShortEscapes(t *testing.T) {
	for _, secret := range []string{"pass\tword-9876", "pass\nword-9876", "pass\r\nword\f-9876\b"} {
		escaped := strings.NewReplacer("\t", `\t`, "\n", `\n`, "\r", `\r`, "\f", `\f`, "\b", `\b`).Replace(secret)
		out := Redact(`{"detail":"bad key `+escaped+` given"}`, secret)
		require.NotContains(t, out, "word")
		require.NotContains(t, out, "9876")
		require.Contains(t, out, "bad key [REDACTED] given")
	}
	require.Equal(t, "a\tb\nc", Redact(`a\tb\nc`))
}

// Whatever characters a credential holds and however its URL is encoded, no
// recognisable part of it is shown. Credentials the adapter was configured with
// are found in any spelling. Credentials it was not told about are found by
// shape, as long as the vendor spelled the delimiting characters in an escaped
// form: a raw "&", quote or space inside a value is indistinguishable from the
// end of it.
func TestRedactNeverShowsPartOfACredential(t *testing.T) {
	secrets := []string{"ab&cdefghij", "ab;cdefghij", `ab"cdefghij`, "ab'cdefghij", "ab cdefghij", "ab%41cdefghij", "ab+cdefghij", "ab/cdefghij", "ab=cdefghij", "ab\tcdefghij", `alpha"bravo-charlie`, "alpha+bravo/charlie=delta", oktaToken, sasKey}
	delimiting := strings.ContainsAny
	encodings := map[string]func(string) string{
		"raw":            func(s string) string { return s },
		"percent":        url.QueryEscape,
		"json":           func(s string) string { return jsonEscape(s) },
		"double encoded": func(s string) string { return url.QueryEscape(url.QueryEscape(s)) },
		"json of percent": func(s string) string {
			return jsonEscape(url.QueryEscape(s))
		},
		"html entities": func(s string) string {
			return strings.NewReplacer("+", "&#43;", "/", "&#47;", "=", "&#61;", `"`, "&quot;", "&", "&amp;", ";", "&#59;").Replace(s)
		},
		"numeric entities":         func(s string) string { return entityEncode(s, "&#%d;") },
		"hex entities":             func(s string) string { return entityEncode(s, "&#x%X;") },
		"json of numeric entities": func(s string) string { return jsonEscape(entityEncode(s, "&#%d;")) },
		"json of html entities": func(s string) string {
			return jsonEscape(strings.NewReplacer("+", "&#43;", "/", "&#47;", "=", "&#61;", `"`, "&quot;").Replace(s))
		},
	}
	// Shapes whose value ends at a delimiter that a secret may contain; only a
	// configured secret can be hidden whole there.
	configuredOnly := map[string]string{
		"json field":        `{"error":"invalid_client","client_secret":"%s","trace":"t-1"}`,
		"bearer":            "request failed. Authorization: Bearer %s trailing",
		"ssws":              "request failed. Authorization: SSWS %s trailing",
		"sas":               "denied SharedAccessSignature sr=hub&sig=%s&se=1 end",
		"connection string": "Endpoint=sb://hub.example.test/;SharedAccessKeyName=root;SharedAccessKey=%s;EntityPath=h end",
	}
	embeddings := map[string]string{
		"userinfo": "bad link https://alice:%s@example.test/p?after=1 end",
		"param":    "bad link https://example.test/p?after=1&token=%s&limit=5 end",
	}
	for k, v := range configuredOnly {
		embeddings[k] = v
	}
	for _, secret := range secrets {
		for encName, enc := range encodings {
			for embName, tmpl := range embeddings {
				in := strings.Replace(tmpl, "%s", enc(secret), 1)
				// A raw delimiter ends a value for any parser: only a configured
				// secret can be found there.
				ambiguous := delimiting(enc(secret), "&;' \t") || (encName == "raw" && delimiting(secret, `"`))
				for _, configured := range []bool{true, false} {
					if !configured && (ambiguous || configuredOnly[embName] != "") {
						continue
					}
					var cfg []string
					if configured {
						cfg = []string{secret}
					}
					out := Redact(in, cfg...)
					for i := 0; i+6 <= len(secret); i++ {
						require.NotContains(t, out, secret[i:i+6], "secret %q, %s, %s, configured=%v: %s", secret, encName, embName, configured, out)
					}
					require.Contains(t, out, redacted)
				}
			}
		}
	}
}

// A shape rule at an encoded layer used to cut a configured secret before the
// decoded layer could see all of it, leaving its tail in the output.
func TestRedactConfiguredSecretNeverLeavesTail(t *testing.T) {
	for _, c := range []struct {
		name, secret, in string
	}{
		{"json field with escaped quote", `alpha"bravo-charlie`, `{"client_secret":"alpha\"bravo-charlie"}`},
		{"entity encoded bearer", "alpha+bravo/charlie=delta", "Authorization: Bearer alpha&#43;bravo&#47;charlie&#61;delta"},
	} {
		t.Run(c.name, func(t *testing.T) {
			out := Redact(c.in, c.secret)
			for i := 0; i+6 <= len(c.secret); i++ {
				require.NotContains(t, out, c.secret[i:i+6], out)
			}
			require.Contains(t, out, redacted)
		})
	}
	// Vendor text next to the residue stays readable and fragments merge.
	require.Equal(t, `denied: [REDACTED] and Tenant not found`, Redact(`denied: aaaabbbbccccdd and Tenant not found`, "xaaaabbbbccccddx"))
}

// A shape rule that ends a value at an entity quote used to cut a configured
// secret containing a quote before decoding could match the whole of it, leaving
// a tail shorter than the residue sweep. Configured secrets match in any
// spelling of each character, mixed spellings included, as one secret.
func TestRedactConfiguredSecretMatchesAnySpellingOfEachCharacter(t *testing.T) {
	const secret = `Qn7Zp3Lm9Kx1",xy`
	for name, echo := range map[string]string{
		"raw":                "Qn7Zp3Lm9Kx1\",xy",
		"entity quote":       "Qn7Zp3Lm9Kx1&#34;,xy",
		"hex entity quote":   "Qn7Zp3Lm9Kx1&#x22;,xy",
		"padded entity":      "Qn7Zp3Lm9Kx1&#0034;,xy",
		"named entity quote": "Qn7Zp3Lm9Kx1&quot;,xy",
		"percent":            "Qn7Zp3Lm9Kx1%22%2Cxy",
		"percent lowercase":  "Qn7Zp3Lm9Kx1%22%2cxy",
		"json escapes":       `Qn7Zp3Lm9Kx1\"\u002cxy`,
		"json upper hex":     `Qn7Zp3Lm9Kx1\u0022\u002Cxy`,
		"percent letters":    "Qn7Zp3Lm9Kx1%22,%78%79",
		"entity letters":     "Qn7Zp3Lm9Kx1&#34;,&#x78;&#121;",
		"mixed":              "Qn7Zp3Lm9Kx1&#34;%2Cx&#x79;",
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, "?token=[REDACTED]&after=1", Redact("?token="+echo+"&after=1", secret))
			inJSON := echo // already one layer of escapes; only a raw quote needs the JSON one
			if name == "raw" {
				inJSON = strings.ReplaceAll(echo, `"`, `\"`)
			}
			require.Equal(t, `{"error":"bad","client_secret":"[REDACTED]","after":1}`, Redact(`{"error":"bad","client_secret":"`+inJSON+`","after":1}`, secret))
			require.Equal(t, "denied [REDACTED] end", Redact("denied "+echo+" end", secret))
		})
	}
	// Non-ASCII characters, and a non-BMP one as a JSON surrogate pair.
	const unicodeSecret = "pässwörd-\U0001F511-key"
	for _, echo := range []string{unicodeSecret, "p%C3%A4sswörd-\U0001F511-key", `p\u00e4sswörd-\ud83d\udd11-key`, "p&#228;sswörd-&#128273;-key", "p&#xE4;sswörd-&#x1f511;-key"} {
		require.Equal(t, "bad [REDACTED] here", Redact("bad "+echo+" here", unicodeSecret), echo)
	}
}

// A secret too long for a pattern (a bearer token passed as a secret) is still
// replaced, by its literal forms and the sweep, and does not slow redaction.
func TestRedactLongConfiguredSecretFallsBackToLiteralForms(t *testing.T) {
	var long strings.Builder
	for long.Len() < 2048 {
		long.WriteString("Zx9Qw4Er7Ty2Ui5Op8As1Df3Gh6Jk0Lm")
	}
	secret := long.String()
	require.Nil(t, secretPattern(secret))
	for name, echo := range map[string]string{"raw": secret, "percent": url.QueryEscape(secret), "json": jsonEscape(secret)} {
		out := Redact("denied: "+echo+" end of text", secret)
		require.Equal(t, "denied: [REDACTED] end of text", out, name)
	}
	start := time.Now()
	Redact(strings.Repeat("vendor text ", 1300)+secret, secret)
	require.Less(t, time.Since(start), 5*time.Second)
	// A secret at the pattern limit compiles.
	require.NotNil(t, secretPattern(secret[:maxSecretPatternLen]))
	require.Equal(t, "x [REDACTED] y", Redact("x "+url.QueryEscape(secret[:maxSecretPatternLen])+" y", secret[:maxSecretPatternLen]))
}

// Whatever the encoding or the shape the secret is in, no three-character piece
// of its tail from the first special character on survives (the sweep alone
// only hides six).
func TestRedactNeverLeavesSpecialCharacterTailOfConfiguredSecret(t *testing.T) {
	secrets := []string{`Qn7Zp3Lm9Kx1",xy`, `Hv4Rt8Bc2Wd6'ab`, "Ju5Ne1Gs9Mf3<qz>", "Ka2Pv7Xo4Ly&jw=", "Td8Ci3Sb6Uh;vk+", "Rm1Ez5Wq9Nx w3p", "Yc6Bo2Fl8Ga/mt%", `Ok3Dj7Vu1Ph\sn,`}
	encodings := map[string]func(string) string{
		"raw":              func(s string) string { return s },
		"percent":          url.QueryEscape,
		"json":             jsonEscape,
		"numeric entities": func(s string) string { return entityEncode(s, "&#%d;") },
		"hex entities":     func(s string) string { return entityEncode(s, "&#x%X;") },
		"named entities": func(s string) string {
			return strings.NewReplacer(`"`, "&quot;", "'", "&apos;", "<", "&lt;", ">", "&gt;", "&", "&amp;").Replace(s)
		},
		"json of percent":     func(s string) string { return jsonEscape(url.QueryEscape(s)) },
		"json of entities":    func(s string) string { return jsonEscape(entityEncode(s, "&#%d;")) },
		"percent of entities": func(s string) string { return url.QueryEscape(entityEncode(s, "&#x%x;")) },
	}
	templates := map[string]string{
		"param":    "bad link https://example.test/p?after=1&token=%s&limit=5 end",
		"json":     `{"error":"invalid_client","client_secret":"%s","trace":"t-1"}`,
		"userinfo": "bad link https://alice:%s@example.test/p?after=1 end",
		"bearer":   "request failed. Authorization: Bearer %s trailing",
		"sas":      "denied SharedAccessSignature sr=hub&sig=%s&se=1 end",
		"prose":    "the credential %s was rejected",
	}
	special := func(secret string) string {
		i := strings.IndexAny(secret, "\"'<>&,;+/=% \\")
		return secret[i:]
	}
	for _, secret := range secrets {
		tail := special(secret)
		for encName, enc := range encodings {
			for tmplName, tmpl := range templates {
				in := strings.Replace(tmpl, "%s", enc(secret), 1)
				out := Redact(in, secret)
				for i := 0; i+3 <= len(tail); i++ {
					piece := tail[i : i+3]
					if strings.Contains(tmpl, piece) {
						continue // vendor text of the template itself
					}
					require.NotContains(t, out, piece, "secret %q, %s, %s: %s", secret, encName, tmplName, out)
				}
				require.Contains(t, out, redacted)
			}
		}
	}
}

func TestRedactDoesNotSwallowFieldsAfterEscapedURLQuote(t *testing.T) {
	in := `{"error":"{\"url\":\"https://x.test/cb?code=abc\",\"message\":\"Tenant not found\"}"}`
	out := Redact(in)
	require.NotContains(t, out, "abc")
	require.Contains(t, out, "Tenant not found")
	// With an "@" later in the text the userinfo rule must not span the fields.
	in = `{"error":"{\"url\":\"https://x.test/cb\",\"message\":\"Tenant not found\",\"user\":\"bob@example.test\"}"}`
	out = Redact(in)
	require.Contains(t, out, "Tenant not found")
	require.Contains(t, out, "https://x.test/cb")
	require.Contains(t, out, "bob@example.test")
}

// The shape rules are case-insensitive, but entity names are not: "&amp;" must
// end a query value however the letters of the exclusion are folded.
func TestRedactAmpEntityEndsQueryValueNotFoldedByCase(t *testing.T) {
	for _, c := range []struct {
		in      string
		hidden  string
		visible []string
	}{
		{"?token=abc&amp;after=1", "abc", []string{"after=1"}},
		{"?token=AbC987654321&amp;error=invalid_client&amp;trace=t-1", "AbC987654321", []string{"error=invalid_client", "trace=t-1"}},
		{"denied https://hub.example.test/p?sr=hub&amp;sig=AbC987654321&amp;se=1 end", "AbC987654321", []string{"se=1", "end"}},
		{"error=denied&amp;client_secret=AbC987654321&amp;trace=t-1", "AbC987654321", []string{"error=denied", "trace=t-1"}},
		// Entities that merely start like "amp" or "a" are value characters.
		{"?token=ab&ampere;cd&Alpha;ef&aacute;gh&amp;after=1", "cd", []string{"after=1"}},
	} {
		out := Redact(c.in)
		require.NotContains(t, out, c.hidden, c.in)
		for _, v := range c.visible {
			require.Contains(t, out, v, c.in)
		}
	}
}

// A page that shows JSON with HTML-escaped quotes ends a value at the escaped
// quote like the JSON-escaped version does, unless more value follows it.
func TestRedactDoesNotSwallowFieldsAfterEntityQuote(t *testing.T) {
	for _, q := range []string{"&quot;", "&#34;", "&#x22;", "&#x0022;", "&#0034;", "&apos;", "&#39;", "&#x27;", "&lt;", "&#60;", "&gt;", "&#x3e;", "&#X3C;"} {
		in := "?code=abc" + q + ",&quot;message&quot;:&quot;Tenant not found&quot;"
		out := Redact(in)
		require.NotContains(t, out, "abc", in)
		require.Contains(t, out, "Tenant not found", in)
		require.Contains(t, out, redacted, in)
	}
	in := `<pre>{&quot;url&quot;:&quot;https://x.test/cb?code=abc&quot;,&quot;message&quot;:&quot;Tenant not found&quot;}</pre>`
	out := Redact(in)
	require.NotContains(t, out, "abc")
	require.Contains(t, out, "Tenant not found")
	// More value after the entity keeps it hidden: the quote is part of the value.
	out = Redact("?code=ab&quot;cdefgh&amp;after=1")
	require.NotContains(t, out, "cdefgh")
	require.Contains(t, out, "after=1")
	// A name that merely starts like a quote entity is a value character.
	out = Redact("?code=ab&quotient;cdefgh&amp;after=1")
	require.NotContains(t, out, "cdefgh")
	require.Contains(t, out, "after=1")
}

func TestWrapKindAndHTTPTextKeepErrorTextOnOneLine(t *testing.T) {
	// The vendor's own line breaks become spaces; escapes decoded from the text
	// (the tab and newline of a Windows path, BEL, ESC) are shown as visible
	// escapes, so the text stays on one line and the path reads as written.
	for _, c := range []struct{ name, raw, want string }{
		{"path as sent", `C:\temp\new path\u0007\u001b[31m` + "\n\n" + `done`, `C:\temp\new path\x07\x1b[31m done`},
		{"path doubly escaped", `C:\\temp\\new`, `C:\temp\new`},
		{"raw break in pretty JSON", "{\"a\":1,\r\n    \"b\":\"x\\ty\"}", `{"a":1, "b":"x\ty"}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, "op: "+c.want, Wrap("op", errors.New(c.raw)).Error())
			require.Equal(t, "kind: "+c.want, Kind(errors.New("kind"), c.raw, nil).Error())
			require.Equal(t, "HTTP 400 Bad Request: "+c.want, HTTPText(&http.Response{StatusCode: 400}, []byte(c.raw), false))
		})
	}
}

// oktaToken and sasKey are realistic credentials: a 42-character Okta API token
// and a 44-character base64 shared access key, both with characters a vendor
// page may spell as entities.
const (
	oktaToken = "00aV7Hq9NmJ2kQp6xB4RsY8Lv0tC3nE5dF6_wX7z9A"
	sasKey    = "Kd3Fv9Qm2Lx7Bn5Rt8Yc1Zp4Wj6Hs0Ga+Ue/Vo2Ni8E="
)

// valueEntityRe matches the entities that stand for one character of a value.
var valueEntityRe = regexp.MustCompile(`&(?:#[0-9]+|#[xX][0-9a-fA-F]+|quot);`)

// entityEncode spells every character that is not a letter or digit as an
// entity, with the given format of its code point.
func entityEncode(s, format string) string {
	var b strings.Builder
	for _, r := range s {
		if r < 0x80 && (r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z') {
			b.WriteRune(r)
		} else {
			fmt.Fprintf(&b, format, r)
		}
	}
	return b.String()
}

// Without being told the credential, the shape rules alone must hide a value
// whose characters are spelled as entities, whole: the ";" of an entity ends no
// value, and no tail of the value is left.
func TestRedactEntityEncodedShapesHideWholeValue(t *testing.T) {
	shapes := map[string]string{
		"ssws header":       "403 Forbidden. Authorization: SSWS %s end",
		"bearer header":     "403 Forbidden. Authorization: Bearer %s end",
		"authorization key": `{"Authorization":"%s","trace":"t-1"}`,
		"shared key":        "Endpoint=sb://hub.example.test/;SharedAccessKey=%s;EntityPath=h end",
		"shared signature":  "denied SharedAccessSignature %s end",
		"sig":               "denied https://hub.example.test/p?sr=hub&sig=%s&se=1 end",
		"query token":       "bad link https://example.test/p?after=1&token=%s&limit=5 end",
		"json field":        `{"error":"invalid_client","client_secret":"%s","trace":"t-1"}`,
		"form field":        "error=denied&client_secret=%s&trace=t-1",
	}
	for _, secret := range []string{oktaToken, sasKey} {
		for _, format := range []string{"&#%d;", "&#x%X;", "&#x%x;"} {
			for name, tmpl := range shapes {
				out := Redact(strings.Replace(tmpl, "%s", entityEncode(secret, format), 1))
				for i := 0; i+6 <= len(secret); i++ {
					require.NotContains(t, out, secret[i:i+6], "%s, %s: %s", name, format, out)
				}
				require.Contains(t, out, redacted, name)
			}
		}
	}
	// What follows the value is still the vendor's.
	require.Equal(t, "sig=[REDACTED]&se=1 expired", Redact("sig=Kd3Fv9&#43;Qm2Lx7B&#47;n5Rt8&#61;&se=1 expired"))
}

// A shape rule or a partial match may leave only the end of a configured
// secret; fragments of six characters or more are hidden.
func TestRedactHidesSixAndSevenCharacterSecretTails(t *testing.T) {
	secret := "Q7mK2xB9nR4vL8pT5wY3zA"
	forms := secretForms([]string{secret})
	require.Equal(t, "denied [REDACTED] end", sweepResidue("denied "+secret[len(secret)-6:]+" end", forms))
	require.Equal(t, "denied [REDACTED] end", sweepResidue("denied "+secret[len(secret)-7:]+" end", forms))
	require.Equal(t, "denied [REDACTED] end", sweepResidue("denied "+secret[:6]+" end", forms))
	require.Equal(t, "denied Q7mK2 end", sweepResidue("denied Q7mK2 end", forms), "five characters are not hidden")
}

func jsonEscape(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1])
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
		{"json escaped ampersand in userinfo", `{"url":"https:\/\/alice:unknown\u0026password@example.test\/p"}`, []string{"alice", "unknown", "password@"}, []string{"@example.test/p"}},
		{"userinfo with decoded slash", `bad https://alice:pa%2Fss%3Fword@example.test/p`, []string{"alice", "pa/ss", "word"}, []string{"@example.test/p"}},
		{"encoded nested fragment", `redirect https://example.test/cb%23access_token%3Dunknown-token-123456789%26state%3Dok`, []string{"unknown-token-123456789"}, []string{"state=ok"}},
		{"double encoded separators", `https://example.test/p?after=1%2526token%253Dunknown-token-123456789%2526limit%253D5`, []string{"unknown-token-123456789"}, []string{"after=1", "limit=5"}},
		// At the layer where "Token" is spelled out, "%26limit" is still escaped, so
		// it is part of the token's value and goes with it.
		{"mixed case escapes", `https://example.test/p?after=1%26Token%3dunknown-token-123456789%2526limit%3d5`, []string{"unknown-token-123456789", "limit=5"}, []string{"after=1"}},
		{"entity encoded fragment", `<a href="https://example.test/cb&#35;access_token&#61;unknown-token-123456789">`, []string{"unknown-token-123456789"}, []string{"https://example.test/cb"}},
		{"userinfo with encoded quote", `https://alice:strong%22unknown-password-123@example.test/`, []string{"strong", "unknown-password-123"}, []string{"@example.test/"}},
		{"userinfo with encoded space", `https://alice:strong%20unknown-password-123@example.test/`, []string{"strong", "unknown-password-123"}, []string{"@example.test/"}},
		{"userinfo with encoded apostrophe", `https://alice:strong%27unknown-password-123@example.test/`, []string{"strong", "unknown-password-123"}, []string{"@example.test/"}},
		{"userinfo with json escaped quote", `{"url":"https:\/\/alice:strong\"unknown-password-123@example.test\/"}`, []string{"strong", "unknown-password-123"}, []string{"@example.test"}},
		{"param value with encoded ampersand", `https://example.test/p?after=1&token=strong%26unknown-password-123`, []string{"strong", "unknown-password-123"}, []string{"after=1"}},
		{"param value with encoded semicolon", `https://example.test/p?after=1&token=strong%3Bunknown-password-123`, []string{"strong", "unknown-password-123"}, []string{"after=1"}},
		{"param value with double encoded ampersand", `https://example.test/p?after=1&token=strong%2526unknown-password-123`, []string{"strong", "unknown-password-123"}, []string{"after=1"}},
		{"surrogate pair and unicode escapes", `{"next":"https:\/\/example.test\/p?\ud83d\ude00=1\u0026\u0074oken=unknown-token-123456789"}`, []string{"unknown-token-123456789"}, []string{"=1"}},
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

func TestRedactConfiguredSecretInAnyEncoding(t *testing.T) {
	secret := "unknown+token/123=456"
	for name, echo := range map[string]string{
		"raw":               "unknown+token/123=456",
		"path escaped":      "unknown+token%2F123=456",
		"lowercase escapes": "unknown%2btoken%2f123%3d456",
		"query escaped":     "unknown%2Btoken%2F123%3D456",
		"double escaped":    "unknown%252Btoken%252F123%253D456",
		"json escaped":      `unknown+token\/123=456`,
		"json unicode":      `unknown\u002btoken\u002f123\u003d456`,
		"json in percent":   "unknown%5Cu002btoken%5Cu002f123=456",
		"html entities":     "unknown+token/123&#61;456",
	} {
		t.Run(name, func(t *testing.T) {
			out := Redact("denied: client_secret "+echo+" is invalid", secret)
			require.Equal(t, "denied: client_secret [REDACTED] is invalid", out)
		})
	}
	// A secret with spaces is also matched in its form-encoded spelling.
	require.Equal(t, "bad [REDACTED] here", Redact("bad correct+horse+battery+staple here", "correct horse battery staple"))
	// A secret that itself looks like an escape is matched as typed.
	require.Equal(t, "bad [REDACTED] here", Redact("bad p%41ss-w0rd here", "p%41ss-w0rd"))
	require.Equal(t, "bad [REDACTED] here", Redact("bad p%2541ss-w0rd here", "p%41ss-w0rd"))
}

func TestRedactFailsClosedOnDeeplyNestedEncoding(t *testing.T) {
	nested := func(levels int) string {
		s := "https://example.test/p?token=unknown-token-123456789"
		for range levels {
			s = url.QueryEscape(s)
		}
		return s
	}
	out := Redact("rejected " + nested(maxDecodePasses))
	require.NotContains(t, out, "unknown-token-123456789")
	require.Contains(t, out, "token=[REDACTED]")

	out = Redact("rejected "+nested(maxDecodePasses+1), "unknown-token-123456789")
	require.Equal(t, undecodable, out)
	require.Equal(t, undecodable, Redact(nested(maxDecodePasses+3)))
	err := Wrap("op", errors.New(nested(maxDecodePasses+1)))
	require.Equal(t, "op: "+undecodable, err.Error())
}

func TestHTTPTextDropsIncompleteEscapeAtCut(t *testing.T) {
	resp := &http.Response{StatusCode: 400}
	// The secret contains a space and arrives percent-encoded twice, cut inside
	// the second layer's escape.
	secret := "alpha bravo/charlie"
	for _, body := range []string{`{"hint":"x alpha bravo%2`, `{"hint":"x alpha bravo%25`, `{"hint":"x alpha bravo%252`, `{"hint":"x alpha bravo\u00`, `{"hint":"x alpha bravo&#x2`} {
		out := HTTPText(resp, []byte(body), true, secret)
		require.NotContains(t, out, "alpha", body)
		require.True(t, strings.HasSuffix(out, " ... [truncated]"), body)
	}
}

// The uppercase legacy aliases decode, so a secret echoed with one is hidden
// whole: no tail survives.
func TestRedactSecretWithUppercaseEntityAliases(t *testing.T) {
	cases := map[string]string{
		"QUOT":      `Qn7Zp3Lm9Kx1",xy`,
		"AMP":       `Qn7Zp3Lm9Kx1&,xy`,
		"LT":        `Qn7Zp3Lm9Kx1<,xy`,
		"GT":        `Qn7Zp3Lm9Kx1>,xy`,
		"QUOT long": `Qn7Zp3Lm9Kx1",xyAb5Cd8`,
	}
	names := map[string]string{"QUOT": "&QUOT;", "AMP": "&AMP;", "LT": "&LT;", "GT": "&GT;", "QUOT long": "&QUOT;"}
	for name, secret := range cases {
		for _, in := range []string{
			`{"client_secret":"` + strings.Replace(secret, secret[12:13], names[name], 1) + `","after":1}`,
			`{"detail":"bad ` + strings.Replace(secret, secret[12:13], names[name], 1) + ` end"}`,
			`?token=` + strings.Replace(secret, secret[12:13], url.QueryEscape(names[name]), 1) + `&after=1`,
			`{"detail":"bad ` + strings.Replace(secret, secret[12:13], names[name][:1]+fmt.Sprintf("%%%02x", names[name][1])+names[name][2:], 1) + ` end"}`,
		} {
			out := Redact(in, secret)
			require.NotContains(t, out, "xy", "%s: %s", name, in)
			require.NotContains(t, out, "Kx1", "%s: %s", name, in)
			require.Contains(t, out, redacted, "%s: %s", name, in)
		}
	}
	// A capitalization that is not an entity is not decoded.
	require.Equal(t, "a &Quot; b", Redact("a &Quot; b"))
	require.Equal(t, `a " b`, Redact("a &QUOT; b"))
}

// The zeros padding a numeric entity can be escaped like any other character.
func TestRedactSecretWithEscapedEntityPadding(t *testing.T) {
	secret := `Qn7Zp3Lm9Kx1",xy`
	for name, in := range map[string]string{
		"percent padding": `?token=Qn7Zp3Lm9Kx1&#%30%30%33%34;,xy&after=1`,
		"json padding":    `{"detail":"Qn7Zp3Lm9Kx1&#\u0030\u003034;,xy"}`,
		"hex padding":     `{"detail":"Qn7Zp3Lm9Kx1&#x%30%3022;,xy"}`,
		"json hex pad":    `{"detail":"Qn7Zp3Lm9Kx1&#x\u0030\u003022;,xy"}`,
	} {
		out := Redact(in, secret)
		require.NotContains(t, out, "xy", name)
		require.NotContains(t, out, ",", name+" "+out)
	}
}

// Hex entity digits and the "x" may be uppercase AND escaped. Regexp case
// folding cannot see through an escape, so each case must be spelled out.
func TestRedactSecretWithEscapedUppercaseHexEntity(t *testing.T) {
	secret := `Qn7Zp3Lm9Kx1",xy`
	for name, in := range map[string]string{
		"percent B":       `?token=Qn7Zp3Lm9%26%23x4%42%3Bx1&QUOT;,xy&after=1`,
		"json B":          `{"detail":"Qn7Zp3Lm9&#x4B;x1&QUOT;,xy"}`,
		"percent X":       `?token=Qn7Zp3Lm9Kx1&#%5822;,xy&after=1`,
		"json X":          `{"detail":"Qn7Zp3Lm9Kx1&#X22;,xy"}`,
		"percent lower b": `?token=Qn7Zp3Lm9%26%23x4%62%3Bx1&QUOT;,xy&after=1`,
	} {
		out := Redact(in, secret)
		require.NotContains(t, out, "xy", name+" "+out)
		require.NotContains(t, out, "Kx1", name+" "+out)
		require.Contains(t, out, redacted, name)
	}
}

func countingCache(max int) (*patternCache, *atomic.Int64) {
	var compiles atomic.Int64
	return newPatternCache(max, func(s string) *regexp.Regexp {
		compiles.Add(1)
		return secretPattern(s)
	}), &compiles
}

// Simultaneous misses for one secret compile once and take one slot.
func TestPatternCacheConcurrentMissesCompileOnce(t *testing.T) {
	c, compiles := countingCache(4)
	var wg sync.WaitGroup
	start := make(chan struct{})
	res := make([]*regexp.Regexp, 128)
	for i := range res {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res[i] = c.get("Qn7Zp3Lm9Kx1-rotating")
		}()
	}
	close(start)
	wg.Wait()
	require.EqualValues(t, 1, compiles.Load())
	require.Equal(t, 1, c.order.Len())
	require.Len(t, c.entries, 1)
	for _, re := range res {
		require.Same(t, res[0], re)
	}
}

// A rotating bearer token (longer than a pattern allows) never reaches the
// cache, so it cannot push the configured secrets out.
func TestPatternCacheIgnoresLongSecrets(t *testing.T) {
	c, compiles := countingCache(2)
	require.NotNil(t, c.get("Qn7Zp3Lm9Kx1-static"))
	for i := range 20 {
		require.Nil(t, c.get(strings.Repeat("T", maxSecretPatternLen)+fmt.Sprint(i)))
	}
	require.EqualValues(t, 1, compiles.Load())
	require.Equal(t, 1, c.order.Len())
	c.get("Qn7Zp3Lm9Kx1-static")
	require.EqualValues(t, 1, compiles.Load())
}

// A full cache evicts the least recently used secret, so new secrets are still
// cached, and nothing but compiled patterns is stored.
func TestPatternCacheEvictsLeastRecentlyUsedWhenFull(t *testing.T) {
	c, compiles := countingCache(3)
	for _, s := range []string{"secret-aaaa", "secret-bbbb", "secret-cccc"} {
		c.get(s)
	}
	c.get("secret-aaaa") // refresh: bbbb is now the oldest
	c.get("secret-dddd")
	require.Equal(t, 3, c.order.Len())
	require.EqualValues(t, 4, compiles.Load())
	c.get("secret-dddd") // new secret was cached
	c.get("secret-aaaa") // survivor was kept
	require.EqualValues(t, 4, compiles.Load())
	c.get("secret-bbbb") // evicted: compiled again
	require.EqualValues(t, 5, compiles.Load())
	for _, el := range c.entries {
		require.NotNil(t, el.Value.(*patternEntry).re)
	}
	require.Len(t, c.entries, 3)
}
