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
	"strings"
	"sync"
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
// they get a shorter wait. Variables so tests can shorten them.
var (
	errorBodyTimeout = 500 * time.Millisecond
	retryBodyTimeout = 200 * time.Millisecond
)

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
}

// Redact removes the given configured secret values and well-known credential
// shapes from text. Empty secrets are ignored.
func Redact(text string, secrets ...string) string {
	for _, s := range secrets {
		s = strings.TrimSpace(s)
		if len(s) < minSecretLen {
			continue // a real credential is never this short; replacing it would only mangle the text
		}
		text = strings.ReplaceAll(text, s, redacted)
		// Vendors echo values URL-encoded in query strings or JSON-escaped in bodies.
		if q := url.QueryEscape(s); q != s {
			text = strings.ReplaceAll(text, q, redacted)
		}
		if j, err := json.Marshal(s); err == nil {
			if e := string(j[1 : len(j)-1]); e != s {
				text = strings.ReplaceAll(text, e, redacted)
			}
		}
	}
	for _, r := range redactions {
		text = r.re.ReplaceAllString(text, r.with)
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

// Wrap returns "op: <redacted cause text>" unwrapping to cause. A nil cause
// returns nil.
func Wrap(op string, cause error, secrets ...string) error {
	if cause == nil {
		return nil
	}
	text := Redact(cause.Error(), secrets...)
	if op != "" {
		text = op + ": " + text
	}
	return &Error{text: text, cause: cause}
}

// Kind returns "<kind text>: <redacted detail>" unwrapping to kind and, when
// not nil, cause. The sentinel kind keeps control-flow checks working while the
// detail carries what the vendor said.
func Kind(kind error, detail string, cause error, secrets ...string) error {
	text := kind.Error()
	if d := Redact(detail, secrets...); d != "" {
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
	timeout := errorBodyTimeout
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		timeout = retryBodyTimeout
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
	text := string(body)
	if i := strings.LastIndexAny(text, partialTokenDelimiters); i >= 0 {
		text = text[:i+1]
	} else {
		text = ""
	}
	text = strings.ToValidUTF8(text, "")
	for _, s := range secrets {
		s = strings.TrimSpace(s)
		for k := min(len(s)-1, len(text)); k >= minSecretLen; k-- {
			if strings.HasSuffix(text, s[:k]) {
				text = text[:len(text)-k]
				break
			}
		}
	}
	return text, true
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
