// Package vendorerr builds adapter errors that carry the end platform's own
// complaint (HTTP status and body, AMQP condition and description, SDK text)
// while removing actual secret material. Users need the vendor's words to fix
// their side (firewall rules, permissions, expired credentials), so nothing is
// summarised or classified away; only secrets are replaced.
package vendorerr

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// MaxBodyText bounds the vendor response body copied into an error.
	MaxBodyText = 2048
	// readLimit is the most body text redaction ever looks at. A body cut
	// here, or earlier by a read deadline, has its trailing partial token
	// dropped before redaction (see HTTPText), so a secret split by the cut
	// cannot escape the redaction patterns.
	readLimit    = 8 * MaxBodyText
	redacted     = "[REDACTED]"
	minSecretLen = 4
)

// How long ReadBody waits for an error body. A vendor error body is a short
// JSON or text document, so a body that is still arriving after this is
// returned as read so far. Throttling and server errors are retried anyway, so
// they get a shorter wait. Atomics so tests in other packages can raise them
// (SetBodyTimeoutsForTest) while adapter goroutines read them.
var (
	errorBodyTimeout atomic.Int64
	retryBodyTimeout atomic.Int64
)

func init() {
	errorBodyTimeout.Store(int64(500 * time.Millisecond))
	retryBodyTimeout.Store(int64(200 * time.Millisecond))
}

// SetBodyTimeoutsForTest overrides the ReadBody deadlines for the normal (4xx)
// and retried (429 and 5xx) cases and returns a function restoring the previous
// values. Tests asserting vendor body text raise them to several seconds so a
// slow race-detector machine cannot cut the body: an end-of-body still returns
// immediately, so the tests stay fast.
func SetBodyTimeoutsForTest(normal, retry time.Duration) (restore func()) {
	prevNormal, prevRetry := errorBodyTimeout.Swap(int64(normal)), retryBodyTimeout.Swap(int64(retry))
	return func() {
		errorBodyTimeout.Store(prevNormal)
		retryBodyTimeout.Store(prevRetry)
	}
}

var redactions = []struct {
	re   *regexp.Regexp
	with string
}{
	// SAS and signed URLs.
	{regexp.MustCompile(`(?i)(\bsig=)[^&\s"';,]+`), "${1}" + redacted},
	{regexp.MustCompile(`(?i)(SharedAccessKey\s*=)[^;\s"']+`), "${1}" + redacted},
	{regexp.MustCompile(`(?i)SharedAccessSignature\s+[^\s"']+`), "SharedAccessSignature " + redacted},
	// Authorization header values, whatever the scheme.
	{regexp.MustCompile(`(?i)(authorization["']?\s*[:=]\s*["']?)(?:(?:bearer|basic|ssws)\s+)?[^\s"',;}]+`), "${1}" + redacted},
	{regexp.MustCompile(`(?i)\b(bearer|ssws)\s+[A-Za-z0-9._~+/=-]{16,}`), "${1} " + redacted},
	// Token and secret fields echoed in JSON, form or query text.
	{regexp.MustCompile(`(?i)(["']?(?:access_token|refresh_token|id_token|client_secret|client_assertion|api_?key|password|secret)["']?\s*[:=]\s*["']?)[^"'&\s,;}]+`), "${1}" + redacted},
	// Credential-like query parameters in a URL-less fragment such as a
	// relative link. Whole URLs are handled by redactURLs, which decodes first.
	{regexp.MustCompile(`(?i)([?&](?:token|access_token|refresh_token|id_token|code|key|api_?key|secret|client_secret|client_assertion|assertion|password|passwd|pwd|sig|signature|session|session_?id|sid|auth|authorization|x-amz-signature)=)[^&\s"'#<>\\]+`), "${1}" + redacted},
}

// secretForms returns the raw secret plus the URL-escaped and JSON-escaped
// forms a vendor may echo it in (query strings, JSON bodies).
func secretForms(s string) []string {
	forms := []string{s}
	if q := url.QueryEscape(s); q != s {
		forms = append(forms, q)
	}
	if j, err := json.Marshal(s); err == nil {
		if e := string(j[1 : len(j)-1]); e != s {
			forms = append(forms, e)
		}
	}
	return forms
}

// Redact removes the given configured secret values and well-known credential
// shapes from text. Empty secrets are ignored.
func Redact(text string, secrets ...string) string {
	for _, s := range secrets {
		s = strings.TrimSpace(s)
		if len(s) < minSecretLen {
			continue // a real credential is never this short; replacing it would only mangle the text
		}
		for _, f := range secretForms(s) {
			text = strings.ReplaceAll(text, f, redacted)
		}
	}
	text = urlRe.ReplaceAllStringFunc(text, redactURL)
	for _, r := range redactions {
		text = r.re.ReplaceAllString(text, r.with)
	}
	return text
}

var (
	// urlRe finds URLs as they appear in plain text or inside a JSON string
	// (where "/" may be written "\/").
	urlRe = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*:(?:\\?/){2}[^\s"'<>]+`)
	// querySepRe matches the separators between query parameters, including
	// the spellings a JSON or HTML body uses for "&".
	querySepRe = regexp.MustCompile(`(?i)&amp;|\\u0026|[&;#]`)
	jsonUniRe  = regexp.MustCompile(`\\u([0-9a-fA-F]{4})`)
)

// credentialParams are the (lowercase, decoded) query parameter names whose
// value is a credential.
var credentialParams = map[string]bool{
	"token": true, "access_token": true, "refresh_token": true, "id_token": true,
	"code": true, "key": true, "apikey": true, "api_key": true, "secret": true,
	"client_secret": true, "client_assertion": true, "assertion": true,
	"password": true, "passwd": true, "pwd": true, "sig": true, "signature": true,
	"session": true, "sessionid": true, "session_id": true, "sid": true,
	"auth": true, "authorization": true, "x-amz-signature": true,
}

// decodeLenient undoes JSON and percent escaping (repeatedly, so double
// encoding does not help) and ignores malformed escapes instead of failing, so
// the result is what a lenient parser on the vendor side would see.
func decodeLenient(s string) string {
	for range 3 {
		d := jsonUniRe.ReplaceAllStringFunc(s, func(m string) string {
			n, _ := strconv.ParseUint(m[2:], 16, 32)
			return string(rune(n))
		})
		d = strings.ReplaceAll(d, `\/`, "/")
		d = strings.ReplaceAll(d, "&amp;", "&")
		d = percentDecode(d)
		if d == s {
			break
		}
		s = d
	}
	return s
}

func percentDecode(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
				b.WriteByte(byte(n))
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// redactURL hides the userinfo and the value of every credential-like query or
// fragment parameter of one URL. Parameter names are judged after decoding, so
// "to%6ben", "\u0026token" and "%26token%3D" do not hide a credential, while
// parameters that are not credentials stay readable.
func redactURL(u string) string {
	i := strings.Index(u, ":")
	j := i + 1
	for j < len(u) && (u[j] == '/' || u[j] == '\\') {
		j++
	}
	end := j
	for end < len(u) && !strings.ContainsRune(`/\?#`, rune(u[end])) {
		end++
	}
	if at := strings.LastIndex(u[j:end], "@"); at >= 0 {
		u = u[:j] + redacted + u[j+at:]
	}
	q := strings.IndexAny(u[j:], "?#")
	if q < 0 {
		return u
	}
	q += j + 1
	var b strings.Builder
	b.WriteString(u[:q])
	last := 0
	query := u[q:]
	for _, loc := range querySepRe.FindAllStringIndex(query, -1) {
		b.WriteString(redactParam(query[last:loc[0]]))
		b.WriteString(query[loc[0]:loc[1]])
		last = loc[1]
	}
	b.WriteString(redactParam(query[last:]))
	return b.String()
}

// redactParam redacts one raw "name=value" parameter when its decoded name, or
// any parameter smuggled into it through an encoded separator, is a credential.
func redactParam(seg string) string {
	name, _, hasEq := strings.Cut(seg, "=")
	if hasEq && credentialParams[strings.ToLower(strings.TrimSpace(decodeLenient(name)))] {
		return name + "=" + redacted
	}
	for _, part := range strings.FieldsFunc(decodeLenient(seg), func(r rune) bool { return r == '&' || r == ';' || r == '?' }) {
		if n, _, ok := strings.Cut(part, "="); ok && credentialParams[strings.ToLower(strings.TrimSpace(n))] {
			if hasEq && !strings.ContainsAny(name, `%\`) {
				return name + "=" + redacted
			}
			return redacted
		}
	}
	return seg
}

// Error is an error whose text is redacted but which still unwraps to the
// original cause and to an optional sentinel kind, so errors.Is/As keep driving
// control flow. Only Error() is redacted: Unwrap exposes the raw cause, so
// callers must not format it into user-visible output themselves.
type Error struct {
	text  string
	kind  error
	cause error
}

func (e *Error) Error() string { return e.text }
func (e *Error) Unwrap() []error {
	var out []error
	if e.kind != nil {
		out = append(out, e.kind)
	}
	if e.cause != nil {
		out = append(out, e.cause)
	}
	return out
}

// Wrap returns "op: <redacted cause text>" unwrapping to cause, with the cause
// text bounded to MaxBodyText. A nil cause returns nil.
func Wrap(op string, cause error, secrets ...string) error {
	if cause == nil {
		return nil
	}
	text := clipRedacted(Redact(cause.Error(), secrets...), secrets)
	if op != "" {
		text = op + ": " + text
	}
	return &Error{text: text, cause: cause}
}

// Kind returns "<kind text>: <redacted detail>" unwrapping to kind and, when
// not nil, cause. The sentinel kind keeps control-flow checks working while the
// detail carries what the vendor said, bounded to MaxBodyText.
func Kind(kind error, detail string, cause error, secrets ...string) error {
	text := kind.Error()
	if d := clipRedacted(Redact(detail, secrets...), secrets); d != "" {
		text += ": " + d
	}
	return &Error{text: text, kind: kind, cause: cause}
}

// ReadBody reads a bounded prefix of an HTTP error body. It waits at most a
// few hundred milliseconds (less for 429 and 5xx, which are retried anyway)
// and returns the bytes read so far, so a vendor that stalls or streams a body
// forever delays the caller's status handling by at most that deadline and
// still gets its partial text reported. truncated reports that the body was
// cut (deadline, readLimit or a read error) rather than read to its end; pass
// it to HTTPText. Closing the body, which callers do, releases the background
// reader.
func ReadBody(resp *http.Response) (body []byte, truncated bool) {
	if resp == nil || resp.Body == nil {
		return nil, false
	}
	timeout := time.Duration(errorBodyTimeout.Load())
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		timeout = time.Duration(retryBodyTimeout.Load())
	}
	return readBody(resp, timeout)
}

func readBody(resp *http.Response, timeout time.Duration) ([]byte, bool) {
	var (
		mu       sync.Mutex
		buf      []byte
		complete bool
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		chunk := make([]byte, 4096)
		for {
			mu.Lock()
			room := readLimit - len(buf)
			mu.Unlock()
			if room <= 0 {
				return
			}
			n, err := resp.Body.Read(chunk[:min(room, len(chunk))])
			mu.Lock()
			buf = append(buf, chunk[:n]...)
			complete = err == io.EOF
			mu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
	mu.Lock()
	defer mu.Unlock()
	return append([]byte(nil), buf...), !complete
}

// partialTokenDelimiters end a token that cannot belong to a credential value.
const partialTokenDelimiters = " \t\r\n\"'&,;{}[]"

// capBody prepares body for redaction. A complete body is returned as is. A
// cut body (truncated, or longer than readLimit) may end inside a secret that
// no redaction pattern can recognise, so its last token is dropped: everything
// after the last delimiter, and any trailing prefix of a configured secret (for
// secrets that themselves contain delimiter characters). The bool reports
// whether the body was cut.
func capBody(body []byte, truncated bool, secrets []string) (string, bool) {
	if len(body) > readLimit {
		body, truncated = body[:readLimit], true
	}
	if !truncated {
		return string(body), false
	}
	return trimCut(string(body), secrets), true
}

// trimCut drops the trailing partial token of text that was cut mid-stream:
// everything after the last delimiter, and any trailing prefix of a configured
// secret in its raw, URL-escaped or JSON-escaped form (for secrets that
// themselves contain delimiter characters).
func trimCut(text string, secrets []string) string {
	if i := strings.LastIndexAny(text, partialTokenDelimiters); i >= 0 {
		text = text[:i+1]
	} else {
		text = ""
	}
	text = strings.ToValidUTF8(text, "")
	for _, s := range secrets {
		s = strings.TrimSpace(s)
		if len(s) < minSecretLen {
			continue // Redact ignores it too
		}
		for _, form := range secretForms(s) {
			for k := min(len(form)-1, len(text)); k >= minSecretLen; k-- {
				if strings.HasSuffix(text, form[:k]) {
					text = text[:len(text)-k]
					break
				}
			}
		}
	}
	return text
}

// clipRedacted bounds already-redacted text to MaxBodyText with a truncation
// marker. The cut text gets the same trailing-partial-token treatment as a cut
// body, so a clip never ends inside a secret.
func clipRedacted(text string, secrets []string) string {
	if len(text) <= MaxBodyText {
		return text
	}
	return trimCut(text[:MaxBodyText], secrets) + " ... [truncated]"
}

// Clip bounds vendor-supplied text (a header, a link) to MaxBodyText bytes.
func Clip(text string) string {
	if len(text) <= MaxBodyText {
		return text
	}
	return strings.ToValidUTF8(text[:MaxBodyText], "") + " ... [truncated]"
}

// HTTPText describes a failed HTTP exchange: status line and the (redacted,
// whitespace-compacted, truncated) response body. It only ever examines the
// first readLimit bytes of body, whatever the caller passes. truncated says the
// body is a cut prefix (as ReadBody reports); its last token is then dropped
// before redaction. A body longer than readLimit is always treated as cut.
func HTTPText(resp *http.Response, body []byte, truncated bool, secrets ...string) string {
	status := fmt.Sprintf("HTTP %d", resp.StatusCode)
	if t := http.StatusText(resp.StatusCode); t != "" {
		status += " " + t
	}
	raw, capped := capBody(body, truncated, secrets)
	text := strings.Join(strings.Fields(Redact(strings.ToValidUTF8(raw, "?"), secrets...)), " ")
	suffix := ""
	if capped {
		suffix = " ... [truncated]"
	}
	if text == "" {
		return status + suffix
	}
	if len(text) > MaxBodyText {
		text = strings.ToValidUTF8(text[:MaxBodyText], "") // may split a rune
		suffix = " ... [truncated]"
	}
	return status + ": " + text + suffix
}
