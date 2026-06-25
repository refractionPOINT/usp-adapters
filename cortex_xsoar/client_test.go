package usp_cortex_xsoar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	t.Run("requires api_key", func(t *testing.T) {
		c := XSOARConfig{ClientOptions: testClientOptions(t), URL: "https://x"}
		assert.Error(t, c.Validate())
	})

	t.Run("requires url", func(t *testing.T) {
		c := XSOARConfig{ClientOptions: testClientOptions(t), APIKey: "k"}
		assert.Error(t, c.Validate())
	})

	t.Run("applies defaults", func(t *testing.T) {
		c := XSOARConfig{ClientOptions: testClientOptions(t), APIKey: "k", URL: "https://x"}
		require.NoError(t, c.Validate())
		assert.Equal(t, "6", c.APIVersion)
		assert.Equal(t, defaultEventType, c.EventType)
		assert.Equal(t, defaultTimestampField, c.TimestampField)
		assert.Equal(t, defaultPageSize, c.PageSize)
		assert.Equal(t, defaultPollInterval, c.PollInterval)
		assert.Equal(t, defaultMaxPages, c.MaxPages)
		assert.Equal(t, defaultInitialLookback, c.InitialLookback)
		assert.Equal(t, defaultDedupeTTL, c.DedupeTTL)
		assert.Equal(t, defaultMaxRetryAttempts, c.MaxRetryAttempts)
	})

	t.Run("caps page size at the XSOAR maximum", func(t *testing.T) {
		c := XSOARConfig{ClientOptions: testClientOptions(t), APIKey: "k", URL: "https://x", PageSize: 5000}
		require.NoError(t, c.Validate())
		assert.Equal(t, maxPageSize, c.PageSize)
	})

	t.Run("rejects an unsupported api_version", func(t *testing.T) {
		c := XSOARConfig{ClientOptions: testClientOptions(t), APIKey: "k", URL: "https://x", APIVersion: "7"}
		assert.Error(t, c.Validate())
	})

	t.Run("api_version 8 requires api_key_id", func(t *testing.T) {
		c := XSOARConfig{ClientOptions: testClientOptions(t), APIKey: "k", URL: "https://x", APIVersion: "8"}
		assert.Error(t, c.Validate())

		c.APIKeyID = "42"
		assert.NoError(t, c.Validate())
	})

	t.Run("advanced key requires api_key_id", func(t *testing.T) {
		c := XSOARConfig{ClientOptions: testClientOptions(t), APIKey: "k", URL: "https://x", Advanced: true}
		assert.Error(t, c.Validate())

		c.APIKeyID = "42"
		assert.NoError(t, c.Validate())
	})

	t.Run("api_version 6 standard key needs no api_key_id", func(t *testing.T) {
		c := XSOARConfig{ClientOptions: testClientOptions(t), APIKey: "k", URL: "https://x", APIVersion: "6"}
		assert.NoError(t, c.Validate())
	})
}

func TestResolveBaseURL(t *testing.T) {
	t.Run("v6 has no path prefix", func(t *testing.T) {
		assert.Equal(t, "https://xsoar.example.com",
			resolveBaseURL(XSOARConfig{URL: "https://xsoar.example.com/", APIVersion: "6"}))
	})

	t.Run("v8 adds the public api prefix", func(t *testing.T) {
		assert.Equal(t, "https://api-tenant.example.com/xsoar/public/v1",
			resolveBaseURL(XSOARConfig{URL: "https://api-tenant.example.com", APIVersion: "8"}))
	})

	t.Run("base_path overrides the version default", func(t *testing.T) {
		assert.Equal(t, "https://xsoar.example.com/custom/api",
			resolveBaseURL(XSOARConfig{URL: "https://xsoar.example.com", APIVersion: "8", BasePath: "/custom/api/"}))
	})
}

func TestBuildQuery(t *testing.T) {
	cursor := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	t.Run("modified clause only when no user query", func(t *testing.T) {
		assert.Equal(t, `modified:>="2026-06-01T00:00:00Z"`, buildQuery("", cursor))
	})

	t.Run("user query is ANDed with the modified clause", func(t *testing.T) {
		assert.Equal(t,
			`(type:Phishing and severity:>=2) and modified:>="2026-06-01T00:00:00Z"`,
			buildQuery("type:Phishing and severity:>=2", cursor))
	})

	t.Run("cursor is rendered in UTC", func(t *testing.T) {
		loc := time.FixedZone("UTC+3", 3*60*60)
		assert.Equal(t, `modified:>="2026-06-01T00:00:00Z"`,
			buildQuery("", cursor.In(loc)))
	})
}

func TestBuildRequestBody(t *testing.T) {
	body := buildRequestBody(`modified:>="2026-06-01T00:00:00Z"`, 2, 100)
	filter, ok := body["filter"].(utils.Dict)
	require.True(t, ok, "body must carry a filter object")

	assert.Equal(t, `modified:>="2026-06-01T00:00:00Z"`, filter["query"])
	assert.Equal(t, 2, filter["page"], "page must be 0-indexed as passed")
	assert.Equal(t, 100, filter["size"])

	sort, ok := filter["sort"].([]utils.Dict)
	require.True(t, ok, "sort must be a list of order objects")
	require.Len(t, sort, 1)
	assert.Equal(t, "modified", sort[0]["field"])
	assert.Equal(t, true, sort[0]["asc"], "ascending sort keeps the cursor monotonic")
}

func TestExtractIncidents(t *testing.T) {
	t.Run("documented envelope with total", func(t *testing.T) {
		items, total, pageLen, err := extractIncidents([]byte(`{"data":[{"id":"1"},{"id":"2"}],"total":57}`))
		require.NoError(t, err)
		require.Len(t, items, 2)
		assert.Equal(t, 57, total)
		assert.Equal(t, 2, pageLen)
		assert.Equal(t, "1", items[0].FindOneString("id"))
	})

	t.Run("empty data array", func(t *testing.T) {
		items, total, pageLen, err := extractIncidents([]byte(`{"data":[],"total":0}`))
		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Equal(t, 0, total)
		assert.Equal(t, 0, pageLen)
	})

	t.Run("bare array fallback", func(t *testing.T) {
		items, total, pageLen, err := extractIncidents([]byte(`[{"id":"1"}]`))
		require.NoError(t, err)
		require.Len(t, items, 1)
		assert.Equal(t, 1, total)
		assert.Equal(t, 1, pageLen)
	})

	t.Run("empty body yields nothing", func(t *testing.T) {
		items, total, pageLen, err := extractIncidents([]byte("   "))
		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Equal(t, 0, total)
		assert.Equal(t, 0, pageLen)
	})

	t.Run("non-json errors", func(t *testing.T) {
		_, _, _, err := extractIncidents([]byte(`not json`))
		assert.Error(t, err)
	})

	t.Run("large integers keep precision", func(t *testing.T) {
		items, _, _, err := extractIncidents([]byte(`{"data":[{"autime":1601389784162034000}],"total":1}`))
		require.NoError(t, err)
		require.Len(t, items, 1)
		v, ok := items[0].GetInt("autime")
		require.True(t, ok)
		assert.Equal(t, uint64(1601389784162034000), v)
	})

	t.Run("non-object records are skipped but counted in pageLen", func(t *testing.T) {
		// pageLen reflects the raw record count (5), so a dropped record does not
		// make a full page look short and end pagination early; len(items) is the
		// kept count (2).
		items, total, pageLen, err := extractIncidents([]byte(`{"data":[{"id":"a"},"junk",123,null,{"id":"b"}],"total":5}`))
		require.NoError(t, err)
		require.Len(t, items, 2)
		assert.Equal(t, 5, pageLen, "pageLen must count raw records, not just the kept ones")
		assert.Equal(t, 5, total)
		assert.Equal(t, "a", items[0].FindOneString("id"))
		assert.Equal(t, "b", items[1].FindOneString("id"))
	})
}

func TestIncidentID(t *testing.T) {
	t.Run("uses the id field", func(t *testing.T) {
		assert.Equal(t, "978", incidentID(utils.Dict{"id": "978", "investigationId": "other"}))
	})

	t.Run("accepts a numeric id", func(t *testing.T) {
		assert.Equal(t, "978", incidentID(utils.Dict{"id": uint64(978)}))
	})

	t.Run("accepts a numeric id of zero", func(t *testing.T) {
		assert.Equal(t, "0", incidentID(utils.Dict{"id": uint64(0)}))
	})

	t.Run("falls back to investigationId", func(t *testing.T) {
		assert.Equal(t, "inv-1", incidentID(utils.Dict{"investigationId": "inv-1"}))
	})

	t.Run("content hash fallback is stable and distinct", func(t *testing.T) {
		a1 := incidentID(utils.Dict{"foo": "bar"})
		a2 := incidentID(utils.Dict{"foo": "bar"})
		b := incidentID(utils.Dict{"foo": "baz"})
		assert.Equal(t, a1, a2)
		assert.NotEqual(t, a1, b)
		assert.Contains(t, a1, "sha256:")
	})
}

func TestDedupeKey(t *testing.T) {
	a := &XSOARAdapter{}
	// Same incident, different modified -> different keys (an update re-ships).
	k1 := a.dedupeKey(utils.Dict{"id": "1", "modified": "2026-05-21T10:00:00Z"})
	k2 := a.dedupeKey(utils.Dict{"id": "1", "modified": "2026-05-21T11:00:00Z"})
	assert.NotEqual(t, k1, k2)
	// Same id and modified -> same key (a re-fetch is suppressed).
	k3 := a.dedupeKey(utils.Dict{"id": "1", "modified": "2026-05-21T10:00:00Z"})
	assert.Equal(t, k1, k3)
}

func TestEventTime(t *testing.T) {
	now := uint64(time.Now().UnixMilli())

	t.Run("uses the configured field", func(t *testing.T) {
		a := &XSOARAdapter{conf: XSOARConfig{TimestampField: "created"}}
		inc := utils.Dict{"created": "2026-05-21T13:45:30Z", "modified": "2026-05-21T14:00:00Z"}
		ts, _ := parseTimestamp("2026-05-21T13:45:30Z")
		assert.Equal(t, uint64(ts.UnixMilli()), a.eventTime(inc))
	})

	t.Run("falls back to modified when the field is absent", func(t *testing.T) {
		a := &XSOARAdapter{conf: XSOARConfig{TimestampField: "created"}}
		inc := utils.Dict{"modified": "2026-05-21T14:00:00Z"}
		ts, _ := parseTimestamp("2026-05-21T14:00:00Z")
		assert.Equal(t, uint64(ts.UnixMilli()), a.eventTime(inc))
	})

	t.Run("falls back to now when nothing parses", func(t *testing.T) {
		a := &XSOARAdapter{conf: XSOARConfig{TimestampField: "created"}}
		assert.GreaterOrEqual(t, a.eventTime(utils.Dict{"created": "not-a-date"}), now)
	})
}

func TestParseTimestamp(t *testing.T) {
	cases := []string{
		"2026-05-21T12:00:00Z",
		"2026-05-21T12:00:00.123456Z",      // microseconds + Z
		"2022-04-27T08:37:29.197107197Z",   // nanoseconds + Z (RFC3339Nano)
		"2020-09-29T17:29:44.162034+03:00", // microseconds + numeric offset
		"0001-01-01T00:00:00Z",             // Go zero-time sentinel
		"2026-05-21T12:00:00",              // offset-less fallback
	}
	for _, c := range cases {
		_, ok := parseTimestamp(c)
		assert.True(t, ok, "expected %q to parse", c)
	}
	_, ok := parseTimestamp("not a date")
	assert.False(t, ok)
	_, ok = parseTimestamp("")
	assert.False(t, ok)
}

func TestSignAdvanced(t *testing.T) {
	// The advanced signature is a plain SHA-256 hex of apiKey+nonce+timestamp.
	apiKey, nonce, ts := "secret", "abc123", "1700000000000"
	expected := sha256.Sum256([]byte(apiKey + nonce + ts))
	assert.Equal(t, hex.EncodeToString(expected[:]), signAdvanced(apiKey, nonce, ts))
}

func TestNewNonce(t *testing.T) {
	n, err := newNonce(64)
	require.NoError(t, err)
	assert.Len(t, n, 64)
	for _, r := range n {
		assert.True(t, strings.ContainsRune(nonceAlphabet, r), "nonce char %q must be alphanumeric", r)
	}
	// Two nonces must differ (cryptographically random).
	other, err := newNonce(64)
	require.NoError(t, err)
	assert.NotEqual(t, n, other)
}

func TestSetAuthHeaders(t *testing.T) {
	newReq := func() *http.Request {
		r, err := http.NewRequest(http.MethodPost, "https://x/incidents/search", nil)
		require.NoError(t, err)
		return r
	}

	t.Run("standard v6: only Authorization", func(t *testing.T) {
		c := NewXSOARClient("https://x", "the-key", "", false, false)
		r := newReq()
		require.NoError(t, c.setAuthHeaders(r))
		assert.Equal(t, "the-key", r.Header.Get("Authorization"))
		assert.Empty(t, r.Header.Get("x-xdr-auth-id"))
		assert.Empty(t, r.Header.Get("x-xdr-nonce"))
	})

	t.Run("standard v8: Authorization plus x-xdr-auth-id", func(t *testing.T) {
		c := NewXSOARClient("https://x", "the-key", "77", false, false)
		r := newReq()
		require.NoError(t, c.setAuthHeaders(r))
		assert.Equal(t, "the-key", r.Header.Get("Authorization"))
		assert.Equal(t, "77", r.Header.Get("x-xdr-auth-id"))
		assert.Empty(t, r.Header.Get("x-xdr-nonce"), "a standard key is not signed")
	})

	t.Run("advanced: signed headers verify against the scheme", func(t *testing.T) {
		c := NewXSOARClient("https://x", "the-key", "42", true, false)
		r := newReq()
		require.NoError(t, c.setAuthHeaders(r))

		ts := r.Header.Get("x-xdr-timestamp")
		nonce := r.Header.Get("x-xdr-nonce")
		assert.NotEmpty(t, ts)
		assert.Len(t, nonce, 64)
		assert.Equal(t, "42", r.Header.Get("x-xdr-auth-id"))
		assert.Equal(t, signAdvanced("the-key", nonce, ts), r.Header.Get("Authorization"),
			"Authorization must be SHA-256(apiKey+nonce+timestamp)")
	})
}

func TestIsTransientError(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		isTransient bool
	}{
		{"500", &HTTPError{StatusCode: 500}, true},
		{"503", &HTTPError{StatusCode: 503}, true},
		{"429", &HTTPError{StatusCode: 429}, true},
		{"401", &HTTPError{StatusCode: 401}, false},
		{"403", &HTTPError{StatusCode: 403}, false},
		{"404", &HTTPError{StatusCode: 404}, false},
		{"400", &HTTPError{StatusCode: 400}, false},
		{"network", fmt.Errorf("failed to execute request %q: connection refused", "x"), true},
		{"context canceled", context.Canceled, false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.isTransient, isTransientError(c.err))
		})
	}
}
