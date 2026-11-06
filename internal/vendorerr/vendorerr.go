// Package vendorerr builds adapter errors that carry the end platform's own
// complaint (HTTP status and body, AMQP condition and description, SDK text)
// while removing actual secret material. Users need the vendor's words to fix
// their side (firewall rules, permissions, expired credentials), so nothing is
// summarised or classified away; only secrets are replaced.
package vendorerr

import (
	"fmt"
	"html"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf16"
)

const (
	// MaxBodyText bounds the vendor response body copied into an error.
	MaxBodyText = 2048
	// readLimit is the most body text redaction ever looks at. A body cut
	// here, or earlier by a read deadline, has its trailing partial token
	// dropped after redaction (see HTTPText), so a secret split by the cut
	// cannot be shown.
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

// Redaction runs at every layer of encoding. A vendor may wrap its message in
// JSON, HTML and percent escaping; decoding moves the boundaries of a value
// ("%26" becomes a "&" that ends a query parameter, "%22" a quote that ends a
// URL), so a value is redacted with the boundaries of the encoding it actually
// had: Redact runs every detector on the current layer, decodes one layer,
// and repeats until the text stops changing, so the last run is on plain text.
// A detector never has to guess how a decoded character used to be spelled.
const (
	maxDecodePasses = 4
	undecodable     = "[REDACTED: undecodable content]"
)

var redactions = []struct {
	re   *regexp.Regexp
	with string
}{
	// Userinfo of a URL: everything between "scheme://" and the last "@" of the
	// URL's token, so a password containing "/", "?" or "&" is hidden too. The
	// URL's own punctuation may itself be escaped at this layer (":", "/", "@"
	// as %XX or "\/"), and a JSON-escaped quote or backslash belongs to the
	// password.
	{regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*(?::|%3a)(?:\\?/|%2f){2})(?:\\.|[^\s"'<>])*(?:@|%40)`), "${1}" + redacted + "@"},
	// Credential-like parameters of a query or fragment, in a URL or not.
	{regexp.MustCompile(`(?i)([?&;#](?:token|access_token|refresh_token|id_token|code|key|api_?key|secret|client_secret|client_assertion|assertion|password|passwd|pwd|sig|signature|session|session_?id|sid|auth|authorization|x-amz-signature)=)(?:\\.|[^&;#\s"'<>])+`), "${1}" + redacted},
	// SAS and signed URLs.
	{regexp.MustCompile(`(?i)(\bsig=)[^&\s"';,]+`), "${1}" + redacted},
	{regexp.MustCompile(`(?i)(SharedAccessKey\s*=)[^;\s"']+`), "${1}" + redacted},
	{regexp.MustCompile(`(?i)SharedAccessSignature\s+[^\s"']+`), "SharedAccessSignature " + redacted},
	// Authorization header values, whatever the scheme.
	{regexp.MustCompile(`(?i)(authorization["']?\s*[:=]\s*["']?)(?:(?:bearer|basic|ssws)\s+)?[^\s"',;}]+`), "${1}" + redacted},
	{regexp.MustCompile(`(?i)\b(bearer|ssws)\s+[A-Za-z0-9._~+/=-]{16,}`), "${1} " + redacted},
	// Token and secret fields echoed in JSON, form or query text.
	{regexp.MustCompile(`(?i)(["']?(?:access_token|refresh_token|id_token|client_secret|client_assertion|api_?key|password|secret)["']?\s*[:=]\s*["']?)[^"'&\s,;}]+`), "${1}" + redacted},
}

var (
	// jsonEscapeRe matches a JSON string escape: \uXXXX (a surrogate pair is
	// matched whole by surrogatePairRe first), \/, \", \\, \b, \f, \n, \r and \t.
	jsonEscapeRe    = regexp.MustCompile(`\\(?:u([0-9a-fA-F]{4})|(["\\/bfnrt]))`)
	surrogatePairRe = regexp.MustCompile(`\\u([dD][89abAB][0-9a-fA-F]{2})\\u([dD][c-fC-F][0-9a-fA-F]{2})`)
	// htmlEntityRe matches the entities a vendor's HTML or XML error body uses
	// for markup and URL characters.
	htmlEntityRe = regexp.MustCompile(`&(?:amp|lt|gt|quot|apos|#[0-9]{1,7}|#[xX][0-9a-fA-F]{1,6});`)
	// partialEscapeRe matches an escape sequence left incomplete at the end of
	// text that was cut.
	partialEscapeRe = regexp.MustCompile(`(?:%[0-9a-fA-F]?|\\u?[0-9a-fA-F]{0,3}|&#?[a-zA-Z0-9]{0,8})$`)
)

var jsonShortEscapes = map[byte]string{'b': "\b", 'f': "\f", 'n': "\n", 'r': "\r", 't': "\t"}

// decodeOnce undoes one layer of JSON, HTML and percent escaping. Malformed
// escapes are left as they are. A "+" is not decoded: it is a space only in
// form encoding, so configured secrets are matched in that spelling too.
func decodeOnce(s string) string {
	s = surrogatePairRe.ReplaceAllStringFunc(s, func(m string) string {
		hi, _ := strconv.ParseUint(m[2:6], 16, 32)
		lo, _ := strconv.ParseUint(m[8:12], 16, 32)
		return string(utf16.DecodeRune(rune(hi), rune(lo)))
	})
	s = jsonEscapeRe.ReplaceAllStringFunc(s, func(m string) string {
		if m[1] != 'u' {
			if c, ok := jsonShortEscapes[m[1]]; ok {
				return c
			}
			return m[1:]
		}
		n, _ := strconv.ParseUint(m[2:], 16, 32)
		return string(rune(n))
	})
	s = htmlEntityRe.ReplaceAllStringFunc(s, html.UnescapeString)
	return strings.ToValidUTF8(percentDecode(s), "?")
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

// canonicalize decodes s until it stops changing, applying layer (when not nil)
// to the text of every layer just before it is decoded, including the final
// plain one. ok is false when it still changes after maxDecodePasses, which
// means deeper nesting than any real vendor produces.
func canonicalize(s string, layer func(string) string) (canonical string, ok bool) {
	s = strings.ToValidUTF8(s, "?")
	for range maxDecodePasses + 1 {
		if layer != nil {
			s = layer(s)
		}
		next := decodeOnce(s)
		if next == s {
			return s, true
		}
		s = next
	}
	return "", false
}

// secretForms returns the spellings of the configured secrets to look for in
// canonical text: the canonical secret, the raw one when it differs (a secret
// that itself looks like an escape), and the form-encoded spelling of a secret
// containing spaces. Secrets too short to be real credentials are skipped;
// replacing them would only mangle the text.
func secretForms(secrets []string) []string {
	var forms []string
	for _, s := range secrets {
		s = strings.TrimSpace(s)
		if len(s) < minSecretLen {
			continue
		}
		for _, f := range []string{s, canonicalOrRaw(s)} {
			forms = append(forms, f, strings.ReplaceAll(f, " ", "+"))
		}
	}
	slices.Sort(forms)
	forms = slices.Compact(forms)
	// Longest first, so a secret that contains another is replaced whole.
	slices.SortStableFunc(forms, func(a, b string) int { return len(b) - len(a) })
	return forms
}

func canonicalOrRaw(s string) string {
	if c, ok := canonicalize(s, nil); ok {
		return c
	}
	return s
}

// Redact returns text in plain form (JSON, HTML and percent escapes decoded)
// with the given configured secret values and well-known credential shapes
// removed, at every layer of encoding (see above). Text that cannot be fully
// decoded is replaced by a marker rather than risk leaking what a deeper layer
// hides. Empty secrets are ignored.
func Redact(text string, secrets ...string) string {
	forms := secretForms(secrets)
	text, ok := canonicalize(text, func(layer string) string {
		for _, f := range forms {
			layer = strings.ReplaceAll(layer, f, redacted)
		}
		for _, r := range redactions {
			layer = r.re.ReplaceAllString(layer, r.with)
		}
		return layer
	})
	if !ok {
		return undecodable
	}
	return text
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

// trimCut drops the trailing partial token of redacted text that was cut
// mid-stream or clipped: an incomplete escape sequence, everything after the
// last delimiter, and any trailing prefix of a configured secret (for secrets
// that themselves contain delimiter characters). A cut may land inside a
// secret that no pattern can recognise, so the end of cut text is never shown.
func trimCut(text string, secrets []string) string {
	text = partialEscapeRe.ReplaceAllString(text, "")
	if i := strings.LastIndexAny(text, partialTokenDelimiters); i >= 0 {
		text = text[:i+1]
	} else {
		text = ""
	}
	text = strings.ToValidUTF8(text, "")
	for _, form := range secretForms(secrets) {
		for k := min(len(form)-1, len(text)); k >= minSecretLen; k-- {
			if strings.HasSuffix(text, form[:k]) {
				text = text[:len(text)-k]
				break
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
// after redaction. A body longer than readLimit is always treated as cut.
func HTTPText(resp *http.Response, body []byte, truncated bool, secrets ...string) string {
	status := fmt.Sprintf("HTTP %d", resp.StatusCode)
	if t := http.StatusText(resp.StatusCode); t != "" {
		status += " " + t
	}
	suffix := ""
	if len(body) > readLimit {
		body, truncated = body[:readLimit], true
	}
	text := Redact(string(body), secrets...)
	if truncated {
		// Redact first, then drop the end: the cut may split a credential.
		text = trimCut(text, secrets)
		suffix = " ... [truncated]"
	}
	text = strings.Join(strings.Fields(text), " ")
	if text == "" {
		return status + suffix
	}
	if len(text) > MaxBodyText {
		text = strings.ToValidUTF8(text[:MaxBodyText], "") // may split a rune
		suffix = " ... [truncated]"
	}
	return status + ": " + text + suffix
}
