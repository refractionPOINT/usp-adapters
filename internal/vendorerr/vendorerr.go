// Package vendorerr builds adapter errors that carry the end platform's own
// complaint (HTTP status and body, AMQP condition and description, SDK text)
// while removing actual secret material. Users need the vendor's words to fix
// their side (firewall rules, permissions, expired credentials), so nothing is
// summarised or classified away; only secrets are replaced.
package vendorerr

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	// MaxBodyText bounds the vendor response body copied into an error.
	MaxBodyText = 2048
	// readLimit is read before redaction so a secret straddling the
	// truncation point cannot leak as a partial value.
	readLimit   = 8 * MaxBodyText
	readTimeout = 2 * time.Second
	redacted    = "[REDACTED]"
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
		if s == "" {
			continue
		}
		text = strings.ReplaceAll(text, s, redacted)
		if q := url.QueryEscape(s); q != s {
			text = strings.ReplaceAll(text, q, redacted)
		}
	}
	for _, r := range redactions {
		text = r.re.ReplaceAllString(text, r.with)
	}
	return text
}

// Error is an error whose text is redacted but which still unwraps to the
// original cause and to an optional sentinel kind, so errors.Is/As keep driving
// control flow.
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

// ReadBody reads a bounded prefix of an HTTP error body. It never blocks past
// a short deadline, so a vendor that streams a body forever cannot delay the
// caller's own status handling; closing the body releases the reader.
func ReadBody(resp *http.Response) []byte {
	if resp == nil || resp.Body == nil {
		return nil
	}
	done := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, readLimit))
		done <- b
	}()
	select {
	case b := <-done:
		return b
	case <-time.After(readTimeout):
		return nil
	}
}

// HTTPText describes a failed HTTP exchange: status line and the (redacted,
// whitespace-compacted, truncated) response body.
func HTTPText(resp *http.Response, body []byte, secrets ...string) string {
	status := fmt.Sprintf("HTTP %d", resp.StatusCode)
	if t := http.StatusText(resp.StatusCode); t != "" {
		status += " " + t
	}
	text := strings.Join(strings.Fields(Redact(strings.ToValidUTF8(string(body), "?"), secrets...)), " ")
	if text == "" {
		return status
	}
	suffix := ""
	if len(body) >= readLimit {
		suffix = " ... [truncated]"
	}
	if len(text) > MaxBodyText {
		text = strings.ToValidUTF8(text[:MaxBodyText], "") // may split a rune
		suffix = " ... [truncated]"
	}
	return status + ": " + text + suffix
}
