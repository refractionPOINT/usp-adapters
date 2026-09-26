// Package credguard validates a Google credential document before it is handed to the
// Google auth library.
//
// A Google credential document of type external_account (workload identity federation) or
// impersonated_service_account, or one carrying a top-level web/installed OAuth config,
// makes the auth library read a local resource (a file named in credential_source, the
// metadata server via a URL, or an executable) and/or exchange it against an endpoint the
// DOCUMENT names, using the library's own HTTP client. When an adapter runs somewhere the
// credential is not fully trusted (a hosted adapter configured by another party), that is
// a local-file/metadata exfiltration and SSRF surface.
//
// This validator accepts only a service_account key. It forbids credential_source, refuses
// the web/installed OAuth config shape (the library loads it ahead of `type`), and pins
// the token endpoint to a Google host. It is pure (no I/O).
package credguard

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"google.golang.org/api/option"
)

// ErrRefused is returned for a credential document that is not accepted.
var ErrRefused = errors.New("credguard: credential refused")

const maxDocumentBytes = 256 << 10
const maxDepth = 64

var googleTokenHosts = map[string]bool{
	"oauth2.googleapis.com":         true,
	"sts.googleapis.com":            true,
	"iamcredentials.googleapis.com": true,
	"accounts.google.com":           true,
}

func googleHost(host string) bool {
	h := strings.ToLower(strings.TrimSuffix(host, "."))
	if i := strings.LastIndexByte(h, ':'); i >= 0 {
		if h[i:] != ":443" {
			return false
		}
		h = h[:i]
	}
	return googleTokenHosts[h]
}

func pinnedGoogleURL(raw, field string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Opaque != "" {
		return fmt.Errorf("%w: %s must be an https Google endpoint", ErrRefused, field)
	}
	if !googleHost(u.Host) {
		return fmt.Errorf("%w: %s host %q is not a Google endpoint", ErrRefused, field, u.Host)
	}
	return nil
}

type document struct {
	Type             string          `json:"type"`
	TokenURI         string          `json:"token_uri"`
	CredentialSource json.RawMessage `json:"credential_source"`
	Web              json.RawMessage `json:"web"`
	Installed        json.RawMessage `json:"installed"`
}

// Validate reports whether raw is a credential document safe to load. Only a
// service_account key is accepted. It never performs I/O.
func Validate(raw []byte) error {
	if len(raw) == 0 {
		return fmt.Errorf("%w: empty document", ErrRefused)
	}
	if err := singleObjectDistinctKeys(raw); err != nil {
		return err
	}
	var doc document
	if json.Unmarshal(raw, &doc) != nil {
		return fmt.Errorf("%w: not a JSON object", ErrRefused)
	}
	if nonEmptyJSON(doc.Web) || nonEmptyJSON(doc.Installed) {
		return fmt.Errorf("%w: a credential must not carry a web/installed OAuth config", ErrRefused)
	}
	if doc.Type != "service_account" {
		if doc.Type == "" {
			return fmt.Errorf("%w: no credential type", ErrRefused)
		}
		return fmt.Errorf("%w: type %q is not accepted (only service_account)", ErrRefused, doc.Type)
	}
	if nonEmptyJSON(doc.CredentialSource) {
		return fmt.Errorf("%w: a service_account key must not carry credential_source", ErrRefused)
	}
	return pinnedGoogleURL(doc.TokenURI, "token_uri")
}

// Option validates raw and returns the client option that carries it, via the typed
// service_account loader so the auth library enforces the same type.
func Option(raw []byte) (option.ClientOption, error) {
	if err := Validate(raw); err != nil {
		return nil, err
	}
	return option.WithAuthCredentialsJSON(option.ServiceAccount, raw), nil
}

func nonEmptyJSON(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

func singleObjectDistinctKeys(raw []byte) error {
	if len(raw) > maxDocumentBytes {
		return fmt.Errorf("%w: document too large", ErrRefused)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	if err := walkDistinct(dec, 0); err != nil {
		return err
	}
	if dec.More() {
		return fmt.Errorf("%w: trailing data after the document", ErrRefused)
	}
	return nil
}

func walkDistinct(dec *json.Decoder, depth int) error {
	if depth > maxDepth {
		return fmt.Errorf("%w: nested too deep", ErrRefused)
	}
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("%w: not valid JSON", ErrRefused)
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return fmt.Errorf("%w: not valid JSON", ErrRefused)
			}
			name := key.(string)
			if seen[name] {
				return fmt.Errorf("%w: duplicate key %q", ErrRefused, name)
			}
			seen[name] = true
			if err := walkDistinct(dec, depth+1); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("%w: not valid JSON", ErrRefused)
		}
	case '[':
		for dec.More() {
			if err := walkDistinct(dec, depth+1); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("%w: not valid JSON", ErrRefused)
		}
	}
	return nil
}

// FileOption reads a credential file, validates it, and returns the client option. It is
// the safe replacement for option.WithCredentialsFile when the file path or its contents
// are not fully trusted: the file must be a service_account key, so the auth library never
// reads a credential_source or exchanges against a non-Google endpoint from it.
func FileOption(path string) (option.ClientOption, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Option(raw)
}
