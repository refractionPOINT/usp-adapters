package usp_okta

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
)

var (
	errInvalidContinuation = errors.New("okta continuation rejected")
	errVendorCredential    = errors.New("okta credentials rejected")
	errRequestRetry        = errors.New("okta request retry")
	errInvalidPage         = errors.New("okta page invalid")
)

type oktaPage struct {
	items []utils.Dict
	next  string
	delay time.Duration
}

func (a *OktaAdapter) validateContinuation(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.User != nil || u.Fragment != "" || !strings.EqualFold(u.Scheme, a.endpoint.Scheme) || !strings.EqualFold(u.Host, a.endpoint.Host) || u.EscapedPath() != logsURL || u.Opaque != "" {
		return errInvalidPage
	}
	// A vendor continuation is opaque, but must never carry credentials.
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return errInvalidPage
	}
	if q.Has("until") || (q.Get("sortOrder") != "" && q.Get("sortOrder") != "ASCENDING") {
		return errInvalidPage
	}
	for key := range q {
		switch strings.ToLower(key) {
		case "apikey", "api_key", "access_token", "token", "authorization":
			return errInvalidPage
		}
	}
	return nil
}

func (a *OktaAdapter) requestPage(ctx context.Context, raw string) (oktaPage, error) {
	var page oktaPage
	if err := a.validateContinuation(raw); err != nil {
		return page, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return page, errInvalidPage
	}
	req.Header.Set("Authorization", "SSWS "+a.conf.ApiKey)
	req.Header.Set("Accept", "application/json")
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return page, errRequestRetry
	} // url.Error can contain a cursor.
	defer resp.Body.Close()
	page.delay = rateDelay(resp.Header, time.Now())
	body, err := io.ReadAll(io.LimitReader(resp.Body, (16<<20)+1))
	if err != nil || len(body) > 16<<20 {
		return page, errRequestRetry
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return page, errVendorCredential
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		if d := retryAfter(resp.Header.Get("Retry-After"), time.Now()); d > page.delay {
			page.delay = d
		}
		return page, errRequestRetry
	}
	if resp.StatusCode == http.StatusBadRequest && req.URL.Query().Get("after") != "" {
		var envelope struct {
			Code string `json:"errorCode"`
		}
		if json.Unmarshal(body, &envelope) == nil && envelope.Code == "E0000001" {
			return page, errInvalidContinuation
		}
	}
	if resp.StatusCode != http.StatusOK {
		return page, errInvalidPage
	}
	if err := json.Unmarshal(body, &page.items); err != nil || page.items == nil || len(page.items) > 1000 {
		return page, errInvalidPage
	}
	for _, item := range page.items {
		if id, ok := item["uuid"].(string); !ok || id == "" {
			return oktaPage{}, errInvalidPage
		}
	}
	page.next, err = nextLink(resp.Header.Values("Link"))
	if err != nil || a.validateContinuation(page.next) != nil {
		return oktaPage{}, errInvalidPage
	}
	return page, nil
}

// nextLink splits only outside angle brackets and quoted parameter values.
// URL query spelling, escaping and ordering are retained byte-for-byte.
func nextLink(headers []string) (string, error) {
	var next string
	for _, header := range headers {
		start, angled, quoted, escaped := 0, false, false, false
		parts := []string{}
		for i, r := range header {
			if escaped {
				escaped = false
				continue
			}
			if quoted && r == '\\' {
				escaped = true
				continue
			}
			if !angled && r == '"' {
				quoted = !quoted
			}
			if !quoted {
				if r == '<' {
					angled = true
				}
				if r == '>' {
					angled = false
				}
			}
			if r == ',' && !angled && !quoted {
				parts = append(parts, header[start:i])
				start = i + 1
			}
		}
		if angled || quoted {
			return "", errInvalidPage
		}
		parts = append(parts, header[start:])
		for _, part := range parts {
			part = strings.TrimSpace(part)
			end := strings.IndexByte(part, '>')
			if !strings.HasPrefix(part, "<") || end < 1 {
				return "", errInvalidPage
			}
			_, params, err := mime.ParseMediaType("link" + part[end+1:])
			if err != nil {
				return "", errInvalidPage
			}
			for _, rel := range strings.Fields(params["rel"]) {
				if rel == "next" {
					if next != "" {
						return "", errInvalidPage
					}
					next = part[1:end]
				}
			}
		}
	}
	if next == "" {
		return "", errInvalidPage
	}
	return next, nil
}

func retryAfter(value string, now time.Time) time.Duration {
	if n, err := strconv.ParseInt(value, 10, 32); err == nil && n >= 0 {
		return time.Duration(n) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil && when.After(now) {
		return when.Sub(now)
	}
	return 0
}

func rateDelay(h http.Header, now time.Time) time.Duration {
	limit, e1 := strconv.ParseInt(h.Get("X-Rate-Limit-Limit"), 10, 64)
	remaining, e2 := strconv.ParseInt(h.Get("X-Rate-Limit-Remaining"), 10, 64)
	if e2 != nil {
		return 0
	}
	reserve := int64(1)
	if e1 == nil && limit/10 > reserve {
		reserve = limit / 10
	}
	if remaining > reserve {
		return 0
	}
	reset, err := strconv.ParseInt(h.Get("X-Rate-Limit-Reset"), 10, 64)
	if err == nil && reset > now.Unix() {
		return time.Unix(reset, 0).Sub(now)
	}
	return defaultPollInterval
}
