// Package vendorerr builds adapter errors that carry the end platform's own
// complaint (HTTP status and body, AMQP condition and description, SDK text)
// while removing actual secret material. Users need the vendor's words to fix
// their side (firewall rules, permissions, expired credentials), so nothing is
// summarised or classified away; only secrets are replaced.
package vendorerr

import (
	"container/list"
	"crypto/sha256"
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
	"unicode"
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
	// residueLen is the length of the fragment of a configured secret that
	// Redact hides wherever it still appears in its output (see sweepResidue).
	residueLen = 6
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

// Value units of the shape rules below. A value may spell a character as an
// HTML entity, and the ";" inside it must not end the value, so a complete
// entity is one unit. entity matches any entity; entityNotAmp matches all but
// "&amp;", which in a query string or form body separates parameters.
// entityValue also leaves out the entities for quotes and angle brackets, which
// end a value like their plain spelling does (see entityValueAlt). The rules
// that use them are case-insensitive, so the entity part is wrapped in (?-i:):
// entity names are case-sensitive, and folding would make "[b-z]" match "a" and
// turn "&amp;" back into a value unit.
const (
	entity       = `&(?:#[0-9]+|#[xX][0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]*);`
	entityNotAmp = `&(?-i:#[0-9]+|#[xX][0-9a-fA-F]+|(?:[b-zA-Z]|a[a-ln-zA-Z]|am[a-oq-zA-Z]|amp[a-zA-Z0-9])[a-zA-Z0-9]*);`
	// Names other than amp, quot, apos, lt and gt. The numeric spellings of the
	// quote and bracket characters never reach the rules: normalizeQuoteEntities
	// rewrites them to the named ones first.
	entityValue = `&(?-i:#[0-9]+|#[xX][0-9a-fA-F]+|(?:[b-fh-km-pr-zA-Z]|a[0-9a-lnoq-zA-Z]|am[0-9a-oq-zA-Z]|amp[0-9a-zA-Z]|ap[0-9a-np-zA-Z]|apo[0-9a-rt-zA-Z]|apos[0-9a-zA-Z]|q[0-9a-tv-zA-Z]|qu[0-9a-np-zA-Z]|quo[0-9a-su-zA-Z]|quot[0-9a-zA-Z]|l[0-9a-su-zA-Z]|lt[0-9a-zA-Z]|g[0-9a-su-zA-Z]|gt[0-9a-zA-Z])[a-zA-Z0-9]*);`
	// The characters a value continues with after an escaped or entity quote:
	// anything but a JSON or URL delimiter.
	afterQuote = `[^&;#\s"'<>,:}\]\\]`
	// A quote, apostrophe or angle bracket, as a JSON escape or an entity, ends
	// the value unless more value follows it.
	entityValueAlt = `(?:\\"` + afterQuote + `|\\[^"]|` + entityValue + `|&(?:quot|apos|lt|gt);` + afterQuote + `|[^&;#\s"'<>])+`
)

// quoteEntityRe matches the numeric spellings of ", ', < and >, and the
// uppercase aliases of ", < and >.
var quoteEntityRe = regexp.MustCompile(`&#0*(?:34|39|60|62);|&#[xX]0*(?:22|27|3[cCeE]);|&(?:QUOT|LT|GT);`)

var quoteEntityNames = map[rune]string{'"': "&quot;", '\'': "&apos;", '<': "&lt;", '>': "&gt;"}

// normalizeQuoteEntities spells the numeric entities of ", ', < and > by name,
// which decodes to the same character, so the shape rules recognize every
// spelling by one rule.
func normalizeQuoteEntities(s string) string {
	return quoteEntityRe.ReplaceAllStringFunc(s, func(m string) string {
		if m[1] != '#' {
			return strings.ToLower(m)
		}
		digits, base := m[2:len(m)-1], 10
		if digits[0] == 'x' || digits[0] == 'X' {
			digits, base = digits[1:], 16
		}
		n, _ := strconv.ParseUint(digits, base, 32)
		return quoteEntityNames[rune(n)]
	})
}

var redactions = []struct {
	re   *regexp.Regexp
	with string
}{
	// Userinfo of a URL: everything between "scheme://" and the last "@" of the
	// URL's token, so a password containing "/", "?" or "&" is hidden too. The
	// URL's own punctuation may itself be escaped at this layer (":", "/", "@"
	// as %XX or "\/"), and a JSON-escaped backslash belongs to the password.
	// A JSON-escaped quote belongs to the password only when more
	// password follows it: before a JSON delimiter it ends the string the URL is
	// in, and the match must not run on into the next fields.
	{regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*(?::|%3a)(?:\\?/|%2f){2})(?:\\"[^\s"'<>,:}\]@\\]|\\[^"]|[^\s"'<>])*(?:\\")?(?:@|%40)`), "${1}" + redacted + "@"},
	// Credential-like parameters of a query or fragment, in a URL or not.
	{regexp.MustCompile(`(?i)([?&;#](?:token|access_token|refresh_token|id_token|code|key|api_?key|secret|client_secret|client_assertion|assertion|password|passwd|pwd|sig|signature|session|session_?id|sid|auth|authorization|x-amz-signature)=)` + entityValueAlt), "${1}" + redacted},
	// SAS and signed URLs.
	{regexp.MustCompile(`(?i)(\bsig=)(?:` + entityNotAmp + `|[^&\s"';,])+`), "${1}" + redacted},
	{regexp.MustCompile(`(?i)(SharedAccessKey\s*=)(?:` + entity + `|[^;\s"'])+`), "${1}" + redacted},
	{regexp.MustCompile(`(?i)SharedAccessSignature\s+(?:` + entity + `|[^\s"'])+`), "SharedAccessSignature " + redacted},
	// Authorization header values, whatever the scheme.
	{regexp.MustCompile(`(?i)(authorization["']?\s*[:=]\s*["']?)(?:(?:bearer|basic|ssws)\s+)?(?:` + entity + `|[^\s"',;}])+`), "${1}" + redacted},
	{regexp.MustCompile(`(?i)\b(bearer|ssws)\s+(?:` + entity + `|[A-Za-z0-9._~+/=-]){16,}`), "${1} " + redacted},
	// Token and secret fields echoed in JSON, form or query text.
	{regexp.MustCompile(`(?i)(["']?(?:access_token|refresh_token|id_token|client_secret|client_assertion|api_?key|password|secret)["']?\s*[:=]\s*["']?)(?:` + entityNotAmp + `|[^"'&\s,;}])+`), "${1}" + redacted},
}

var (
	// jsonEscapeRe matches a JSON string escape: \uXXXX (a surrogate pair is
	// matched whole by surrogatePairRe first), \/, \", \\, \b, \f, \n, \r and \t.
	jsonEscapeRe    = regexp.MustCompile(`\\(?:u([0-9a-fA-F]{4})|(["\\/bfnrt]))`)
	surrogatePairRe = regexp.MustCompile(`\\u([dD][89abAB][0-9a-fA-F]{2})\\u([dD][c-fC-F][0-9a-fA-F]{2})`)
	// htmlEntityRe matches the entities a vendor's HTML or XML error body uses
	// for markup and URL characters, including the uppercase legacy aliases
	// (&QUOT; &AMP; &LT; &GT;) that html.UnescapeString also decodes. Other
	// capitalizations are not entities and stay as they are.
	htmlEntityRe = regexp.MustCompile(`&(?:amp|lt|gt|quot|apos|AMP|LT|GT|QUOT|#[0-9]{1,7}|#[xX][0-9a-fA-F]{1,6});`)
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

// secretForms returns the literal spellings of the configured secrets: the
// canonical secret, the raw one when it differs (a secret that itself looks like
// an escape), and the form-encoded spelling of a secret containing spaces. They
// feed the residue sweep and the cut trimming, and the replacement of a secret
// too long for secretPattern. Secrets too short to be real credentials are
// skipped; replacing them would only mangle the text.
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

// maxSecretPatternLen bounds the secrets matched by pattern: the program grows
// by about a hundred instructions per character. A longer secret (a bearer
// token passed as a secret) is replaced by its literal forms and the sweep. Known
// limit: such a token echoed with a mixed-encoded entity suffix can leave a tail
// of under residueLen characters, which no literal form or fragment matches.
const maxSecretPatternLen = 256

var jsonShortEscapeLetters = map[rune]string{'\b': "b", '\f': "f", '\n': "n", '\r': "r", '\t': "t"}

var namedEntities = map[rune]string{'"': "quot", '\'': "apos", '<': "lt", '>': "gt", '&': "amp"}

// hexClass spells hex digits matching either letter case.
func hexClass(h string) string {
	var b strings.Builder
	for _, c := range strings.ToLower(h) {
		if c >= 'a' && c <= 'f' {
			b.WriteString("[" + string(c) + strings.ToUpper(string(c)) + "]")
		} else {
			b.WriteRune(c)
		}
	}
	return b.String()
}

// plainSpelling matches r spelled as itself, with percent escapes (and "+" for
// a space) or with JSON escapes.
func plainSpelling(r rune) string {
	alts := []string{regexp.QuoteMeta(string(r))}
	var percent strings.Builder
	for _, c := range []byte(string(r)) {
		percent.WriteString("%" + hexClass(fmt.Sprintf("%02x", c)))
	}
	alts = append(alts, percent.String())
	if r == ' ' {
		alts = append(alts, `\+`)
	}
	if r > 0xFFFF {
		hi, lo := utf16.EncodeRune(r)
		alts = append(alts, fmt.Sprintf(`\\u%s\\u%s`, hexClass(fmt.Sprintf("%04x", hi)), hexClass(fmt.Sprintf("%04x", lo))))
	} else {
		alts = append(alts, `\\u`+hexClass(fmt.Sprintf("%04x", r)))
	}
	if r == '"' || r == '\\' || r == '/' {
		alts = append(alts, `\\`+regexp.QuoteMeta(string(r)))
	}
	if e, ok := jsonShortEscapeLetters[r]; ok {
		alts = append(alts, `\\`+e)
	}
	return "(?:" + strings.Join(alts, "|") + ")"
}

// spelled matches text spelled character by character in plain spellings, so
// the "&", "#" and ";" of an entity may themselves be percent or JSON escaped
// (an entity inside JSON, or inside a URL). fold also accepts either letter
// case. It spells both cases explicitly: regexp case folding cannot reach a
// letter that is itself escaped, like %42 or B for "B".
func spelled(text string, fold bool) string {
	var b strings.Builder
	for _, r := range text {
		lower, upper := unicode.ToLower(r), unicode.ToUpper(r)
		if fold && lower != upper {
			b.WriteString("(?:" + plainSpelling(lower) + "|" + plainSpelling(upper) + ")")
		} else {
			b.WriteString(plainSpelling(r))
		}
	}
	return b.String()
}

// upperEntities are the uppercase legacy aliases of named entities, the only
// capitalizations other than lowercase that decode (html.UnescapeString).
var upperEntities = map[rune]string{'"': "QUOT", '&': "AMP", '<': "LT", '>': "GT"}

// charPattern matches any spelling of r in one layer: a plain spelling or an
// HTML entity (decimal, hex or named). The zeros that may pad a numeric entity
// are spelled in any plain spelling too, like every other character of it.
func charPattern(r rune) string {
	amp, hash, semi, zeros := spelled("&", false), spelled("#", false), spelled(";", false), "(?:"+plainSpelling('0')+")*"
	alts := []string{
		plainSpelling(r),
		amp + hash + zeros + spelled(strconv.Itoa(int(r)), false) + semi,
		amp + hash + spelled("x", true) + zeros + spelled(strconv.FormatInt(int64(r), 16), true) + semi,
	}
	if n, ok := namedEntities[r]; ok {
		alts = append(alts, amp+spelled(n, false)+semi)
	}
	if n, ok := upperEntities[r]; ok {
		alts = append(alts, amp+spelled(n, false)+semi)
	}
	return "(?:" + strings.Join(alts, "|") + ")"
}

// secretPattern matches the secret s with every character in any single-layer
// spelling, so "a&#34;,b", "a%22%2Cb" and a mix of them are one secret. nil when
// s is too long for a pattern or does not compile.
func secretPattern(s string) *regexp.Regexp {
	if len(s) > maxSecretPatternLen {
		return nil
	}
	var b strings.Builder
	for _, r := range s {
		b.WriteString(charPattern(r))
	}
	re, err := regexp.Compile(b.String())
	if err != nil {
		return nil
	}
	return re
}

// patternCache is a bounded LRU of compiled secret patterns, keyed by the
// SHA-256 of the secret so the secret itself is not a map key. The adapters pass
// the same few secrets (client secret, SAS key, API token) on every error, and a
// compile costs milliseconds. Only patterns that compiled are kept, secrets
// longer than maxSecretPatternLen never reach the cache (rotating bearer tokens
// would only churn it), and a full cache evicts its least recently used entry
// so new secrets keep being cached.
type patternCache struct {
	mu      sync.Mutex
	max     int
	entries map[[sha256.Size]byte]*list.Element // of *patternEntry
	order   *list.List                          // front = most recently used
	compile func(string) *regexp.Regexp
}

type patternEntry struct {
	key  [sha256.Size]byte
	once sync.Once
	re   *regexp.Regexp
}

func newPatternCache(max int, compile func(string) *regexp.Regexp) *patternCache {
	return &patternCache{max: max, entries: map[[sha256.Size]byte]*list.Element{}, order: list.New(), compile: compile}
}

// get returns the pattern for s, nil when s is too long or does not compile.
// Concurrent callers for one secret share a single entry and a single compile.
func (c *patternCache) get(s string) *regexp.Regexp {
	if len(s) > maxSecretPatternLen {
		return nil
	}
	key := sha256.Sum256([]byte(s))
	c.mu.Lock()
	el, ok := c.entries[key]
	if ok {
		c.order.MoveToFront(el)
	} else {
		if c.order.Len() >= c.max {
			oldest := c.order.Back()
			c.order.Remove(oldest)
			delete(c.entries, oldest.Value.(*patternEntry).key)
		}
		el = c.order.PushFront(&patternEntry{key: key})
		c.entries[key] = el
	}
	c.mu.Unlock()

	e := el.Value.(*patternEntry)
	e.once.Do(func() { e.re = c.compile(s) })
	if e.re == nil {
		c.mu.Lock()
		if cur, ok := c.entries[key]; ok && cur == el {
			c.order.Remove(el)
			delete(c.entries, key)
		}
		c.mu.Unlock()
	}
	return e.re
}

// maxCachedSecretPatterns bounds the cache: a handful of configured secrets per
// adapter, many adapters per process.
const maxCachedSecretPatterns = 64

var secretPatterns = newPatternCache(maxCachedSecretPatterns, secretPattern)

// secretMatchers returns what replaces the configured secrets in one layer of
// text: a pattern per secret, longest first, and the literal forms of the
// secrets that have none.
func secretMatchers(secrets []string) (res []*regexp.Regexp, literals []string) {
	secrets = slices.Clone(secrets)
	for i, s := range secrets {
		secrets[i] = strings.TrimSpace(s)
	}
	slices.SortStableFunc(secrets, func(a, b string) int { return len(b) - len(a) })
	seen := map[string]bool{}
	for _, s := range secrets {
		if len(s) < minSecretLen || seen[s] {
			continue
		}
		seen[s] = true
		for _, v := range slices.Compact([]string{s, canonicalOrRaw(s)}) {
			if re := secretPatterns.get(v); re != nil {
				res = append(res, re)
			} else {
				literals = append(literals, secretForms([]string{v})...)
			}
		}
	}
	return res, literals
}

// Redact returns text in plain form (JSON, HTML and percent escapes decoded)
// with the given configured secret values and well-known credential shapes
// removed, at every layer of encoding (see above). Text that cannot be fully
// decoded is replaced by a marker rather than risk leaking what a deeper layer
// hides. Empty secrets are ignored.
func Redact(text string, secrets ...string) string {
	return redact(text, false, secrets)
}

// rawBreakRe matches a run of whitespace that holds a line break or tab.
var rawBreakRe = regexp.MustCompile(`[ \t\r\n\v\f]*[\t\r\n\v\f][ \t\r\n\v\f]*`)

// redact is Redact; flatten also turns the line breaks and tabs the vendor
// itself sent (the first layer, once redacted) into single spaces. A tab or
// newline decoded from an escape later on is not the vendor's layout (it may be
// the "\t" of a Windows path), so oneLine shows those as visible escapes.
func redact(text string, flatten bool, secrets []string) string {
	forms := secretForms(secrets)
	patterns, literals := secretMatchers(secrets)
	first := true
	text, ok := canonicalize(text, func(layer string) string {
		// Configured secrets go first, before any shape rule can cut them.
		for _, re := range patterns {
			layer = re.ReplaceAllLiteralString(layer, redacted)
		}
		for _, f := range literals {
			layer = strings.ReplaceAll(layer, f, redacted)
		}
		layer = normalizeQuoteEntities(layer)
		for _, r := range redactions {
			layer = r.re.ReplaceAllString(layer, r.with)
		}
		if flatten && first {
			layer = rawBreakRe.ReplaceAllString(layer, " ")
		}
		first = false
		return layer
	})
	if !ok {
		return undecodable
	}
	return sweepResidue(text, forms)
}

// sweepResidue is the safety net for configured secrets: a shape rule that
// matched at an encoded layer (a field, header or URL whose delimiter was
// escaped) may have consumed only the start of a secret, and no later layer
// then sees the whole of it. Every occurrence in text of any residueLen-long
// fragment of a secret (the whole secret when shorter) is replaced, with
// overlapping and adjacent hits merged into one marker. Configured secrets are
// long random values, so vendor prose does not collide with their fragments.
func sweepResidue(text string, forms []string) string {
	type span struct{ lo, hi int }
	var spans []span
	for _, f := range forms {
		k := min(len(f), residueLen)
		for i := 0; i+k <= len(f); i++ {
			for from := 0; ; {
				j := strings.Index(text[from:], f[i:i+k])
				if j < 0 {
					break
				}
				spans = append(spans, span{from + j, from + j + k})
				from += j + 1
			}
		}
	}
	if len(spans) == 0 {
		return text
	}
	slices.SortFunc(spans, func(a, b span) int { return a.lo - b.lo })
	var b strings.Builder
	last, cur := 0, spans[0]
	for _, s := range append(spans[1:], span{len(text) + 1, len(text) + 1}) {
		if s.lo <= cur.hi {
			cur.hi = max(cur.hi, s.hi)
			continue
		}
		b.WriteString(text[last:cur.lo] + redacted)
		last, cur = cur.hi, s
	}
	b.WriteString(text[last:])
	return strings.ToValidUTF8(b.String(), "?")
}

// oneLine keeps vendor text on a single line. Redaction has already run, so
// this is presentation only. Runs of whitespace collapse to one space; control
// characters (which JSON escapes decode to, and which terminals interpret) are
// shown as visible escapes, so a decoded "C:\temp\new" still reads as written.
func oneLine(s string) string {
	var b strings.Builder
	space := true // drops leading spaces
	for _, r := range s {
		switch {
		case r == '\t':
			b.WriteString(`\t`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case unicode.IsControl(r):
			fmt.Fprintf(&b, `\x%02x`, r)
		case unicode.IsSpace(r):
			if !space {
				b.WriteByte(' ')
			}
			space = true
			continue
		default:
			b.WriteRune(r)
		}
		space = false
	}
	return strings.TrimSuffix(b.String(), " ")
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
	text := clipRedacted(oneLine(redact(cause.Error(), true, secrets)), secrets)
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
	if d := clipRedacted(oneLine(redact(detail, true, secrets)), secrets); d != "" {
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
	// A marker at the very end may stand for a secret the cut split.
	text = strings.TrimSuffix(text, redacted)
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
	text := redact(string(body), true, secrets)
	if truncated {
		// Redact first, then drop the end: the cut may split a credential.
		text = trimCut(text, secrets)
		suffix = " ... [truncated]"
	}
	text = oneLine(text)
	if text == "" {
		return status + suffix
	}
	if len(text) > MaxBodyText {
		text = strings.ToValidUTF8(text[:MaxBodyText], "") // may split a rune
		suffix = " ... [truncated]"
	}
	return status + ": " + text + suffix
}
