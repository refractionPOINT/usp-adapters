package usp_netskope

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClientOptions returns ClientOptions wired for a sink (no real LimaCharlie
// connection) with the logging callbacks pointed at the test log.
func testClientOptions(t *testing.T) uspclient.ClientOptions {
	t.Helper()
	return uspclient.ClientOptions{
		Identity: uspclient.Identity{
			Oid:             "11111111-1111-1111-1111-111111111111",
			InstallationKey: "test-installation-key",
		},
		Platform:      "json",
		SensorSeedKey: "netskope-test",
		TestSinkMode:  true,
		DebugLog:      func(msg string) { t.Logf("DBG: %s", msg) },
		OnWarning:     func(msg string) { t.Logf("WRN: %s", msg) },
		OnError:       func(err error) { t.Logf("ERR: %v", err) },
	}
}

// --- unit tests -------------------------------------------------------------

func TestParseIteratorResponse(t *testing.T) {
	t.Run("ok envelope with result and wait_time", func(t *testing.T) {
		items, wait, err := parseIteratorResponse([]byte(`{"ok":1,"result":[{"_id":"a"},{"_id":"b"}],"wait_time":12}`))
		require.NoError(t, err)
		require.Len(t, items, 2)
		assert.Equal(t, "a", items[0].FindOneString("_id"))
		assert.Equal(t, 12, wait)
	})

	t.Run("empty result", func(t *testing.T) {
		items, wait, err := parseIteratorResponse([]byte(`{"ok":1,"result":[],"wait_time":30}`))
		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Equal(t, 30, wait)
	})

	t.Run("ok=0 is an error and still surfaces wait_time", func(t *testing.T) {
		_, wait, err := parseIteratorResponse([]byte(`{"ok":0,"error":"bad index","wait_time":5}`))
		assert.Error(t, err)
		assert.Equal(t, 5, wait)
	})

	t.Run("empty body yields no items", func(t *testing.T) {
		items, _, err := parseIteratorResponse([]byte("   "))
		require.NoError(t, err)
		assert.Empty(t, items)
	})

	t.Run("invalid json errors", func(t *testing.T) {
		_, _, err := parseIteratorResponse([]byte(`not json`))
		assert.Error(t, err)
	})

	t.Run("large integers keep precision", func(t *testing.T) {
		items, _, err := parseIteratorResponse([]byte(`{"ok":1,"result":[{"timestamp":1748000000,"big":123456789012345678}]}`))
		require.NoError(t, err)
		require.Len(t, items, 1)
		v, ok := items[0].GetInt("big")
		require.True(t, ok)
		assert.Equal(t, uint64(123456789012345678), v)
	})

	t.Run("non-object records are skipped, valid ones kept", func(t *testing.T) {
		items, _, err := parseIteratorResponse([]byte(`{"ok":1,"result":[{"_id":"a"},"junk",123,null,{"_id":"b"}]}`))
		require.NoError(t, err)
		require.Len(t, items, 2)
		assert.Equal(t, "a", items[0].FindOneString("_id"))
		assert.Equal(t, "b", items[1].FindOneString("_id"))
	})
}

func TestRecordID(t *testing.T) {
	t.Run("uses configured id field", func(t *testing.T) {
		feed := NetskopeFeed{IDField: "alert_id"}
		assert.Equal(t, "abc", recordID(feed, utils.Dict{"alert_id": "abc", "_id": "other"}))
	})

	t.Run("falls back to _id then id", func(t *testing.T) {
		assert.Equal(t, "xyz", recordID(NetskopeFeed{}, utils.Dict{"_id": "xyz"}))
		assert.Equal(t, "q", recordID(NetskopeFeed{}, utils.Dict{"id": "q"}))
	})

	t.Run("accepts numeric ids", func(t *testing.T) {
		feed := NetskopeFeed{IDField: "n"}
		assert.Equal(t, "42", recordID(feed, utils.Dict{"n": uint64(42)}))
	})

	t.Run("content hash fallback is stable and distinct", func(t *testing.T) {
		a1 := recordID(NetskopeFeed{}, utils.Dict{"foo": "bar"})
		a2 := recordID(NetskopeFeed{}, utils.Dict{"foo": "bar"})
		b := recordID(NetskopeFeed{}, utils.Dict{"foo": "baz"})
		assert.Equal(t, a1, a2)
		assert.NotEqual(t, a1, b)
		assert.Contains(t, a1, "sha256:")
	})
}

func TestEventTime(t *testing.T) {
	a := &NetskopeAdapter{conf: NetskopeConfig{ClientOptions: testClientOptions(t)}}
	feed := NetskopeFeed{TimestampField: defaultTimestampField}

	t.Run("epoch seconds become milliseconds", func(t *testing.T) {
		assert.Equal(t, uint64(1748000000000), a.eventTime(feed, utils.Dict{"timestamp": uint64(1748000000)}))
	})

	t.Run("epoch seconds as float", func(t *testing.T) {
		assert.Equal(t, uint64(1748000000000), a.eventTime(feed, utils.Dict{"timestamp": float64(1748000000)}))
	})

	t.Run("iso-8601 string", func(t *testing.T) {
		ts, _ := time.Parse(time.RFC3339, "2026-05-21T13:45:30Z")
		assert.Equal(t, uint64(ts.UnixMilli()), a.eventTime(feed, utils.Dict{"timestamp": "2026-05-21T13:45:30Z"}))
	})

	t.Run("absent field falls back to now", func(t *testing.T) {
		before := uint64(time.Now().UnixMilli())
		got := a.eventTime(feed, utils.Dict{"user": "x"})
		assert.GreaterOrEqual(t, got, before)
	})
}

func TestResolveBaseURL(t *testing.T) {
	assert.Equal(t, "https://acme.goskope.com/api/v2",
		resolveBaseURL(NetskopeConfig{Tenant: "acme.goskope.com"}))
	// A scheme and trailing slash on the tenant are tolerated.
	assert.Equal(t, "https://acme.goskope.com/api/v2",
		resolveBaseURL(NetskopeConfig{Tenant: "https://acme.goskope.com/"}))
	// base_url wins and is used verbatim (minus a trailing slash).
	assert.Equal(t, "https://test.example/api/v2",
		resolveBaseURL(NetskopeConfig{Tenant: "acme.goskope.com", BaseURL: "https://test.example/api/v2/"}))
}

func TestParseStartTime(t *testing.T) {
	t.Run("empty means next", func(t *testing.T) {
		v, err := parseStartTime("")
		require.NoError(t, err)
		assert.Equal(t, int64(0), v)
	})
	t.Run("epoch seconds", func(t *testing.T) {
		v, err := parseStartTime("1748000000")
		require.NoError(t, err)
		assert.Equal(t, int64(1748000000), v)
	})
	t.Run("rfc3339", func(t *testing.T) {
		v, err := parseStartTime("2026-05-21T13:45:30Z")
		require.NoError(t, err)
		ts, _ := time.Parse(time.RFC3339, "2026-05-21T13:45:30Z")
		assert.Equal(t, ts.Unix(), v)
	})
	t.Run("relative duration", func(t *testing.T) {
		before := time.Now().Add(-24 * time.Hour).Unix()
		v, err := parseStartTime("24h")
		require.NoError(t, err)
		assert.InDelta(t, before, v, 5)
	})
	t.Run("invalid", func(t *testing.T) {
		_, err := parseStartTime("yesterday")
		assert.Error(t, err)
	})
	t.Run("non-positive epoch", func(t *testing.T) {
		_, err := parseStartTime("0")
		assert.Error(t, err)
	})
}

func TestSanitizeIndex(t *testing.T) {
	assert.Equal(t, "acme_alert_dlp", sanitizeIndex("acme_alert_dlp"))
	assert.Equal(t, "a_b_c-d", sanitizeIndex("a b/c-d"))
	assert.Equal(t, "seed_2", sanitizeIndex("seed.2"))
}

func TestFeedPath(t *testing.T) {
	assert.Equal(t, "events/dataexport/alerts/dlp", NetskopeFeed{Kind: "alerts", Type: "dlp"}.path())
	assert.Equal(t, "events/dataexport/events/page", NetskopeFeed{Kind: "events", Type: "page"}.path())
}

func TestWaitDuration(t *testing.T) {
	a := &NetskopeAdapter{conf: NetskopeConfig{
		PollInterval: 30 * time.Second,
		MaxWaitTime:  5 * time.Minute,
		minWaitTime:  1 * time.Second,
	}}

	t.Run("honours server wait_time", func(t *testing.T) {
		assert.Equal(t, 12*time.Second, a.waitDuration(12, 0, true))
	})
	t.Run("caps server wait_time at max", func(t *testing.T) {
		assert.Equal(t, 5*time.Minute, a.waitDuration(3600, 0, true))
	})
	t.Run("backlog with no hint drains at min", func(t *testing.T) {
		assert.Equal(t, 1*time.Second, a.waitDuration(0, 10000, true))
	})
	t.Run("idle with no hint waits poll_interval", func(t *testing.T) {
		assert.Equal(t, 30*time.Second, a.waitDuration(0, 0, true))
	})
	t.Run("failed poll with no hint waits poll_interval", func(t *testing.T) {
		assert.Equal(t, 30*time.Second, a.waitDuration(0, 0, false))
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

func TestRetryAfterHeader(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "7")
	assert.Equal(t, 7*time.Second, retryAfter(h))

	h2 := http.Header{}
	h2.Set("RateLimit-Reset", "3")
	assert.Equal(t, 3*time.Second, retryAfter(h2))

	assert.Equal(t, time.Duration(0), retryAfter(http.Header{}))
}

func TestValidate(t *testing.T) {
	t.Run("requires token", func(t *testing.T) {
		c := NetskopeConfig{ClientOptions: testClientOptions(t), Tenant: "acme.goskope.com"}
		assert.Error(t, c.Validate())
	})

	t.Run("requires tenant or base_url", func(t *testing.T) {
		c := NetskopeConfig{ClientOptions: testClientOptions(t), Token: "k"}
		assert.Error(t, c.Validate())
	})

	t.Run("applies defaults and the default feed set", func(t *testing.T) {
		c := NetskopeConfig{ClientOptions: testClientOptions(t), Token: "k", Tenant: "acme.goskope.com"}
		require.NoError(t, c.Validate())
		assert.Equal(t, defaultPollInterval, c.PollInterval)
		assert.Equal(t, defaultDedupeTTL, c.DedupeTTL)

		// Default = all alert types + the default event types (audit).
		names := map[string]NetskopeFeed{}
		for _, f := range c.Feeds {
			names[f.Name] = f
		}
		assert.Contains(t, names, "alert_dlp")
		assert.Contains(t, names, "alert_malware")
		assert.Contains(t, names, "event_audit")
		assert.Len(t, c.Feeds, len(defaultAlertTypes)+len(defaultEventTypes))

		// Each feed gets a unique, sanitized, prefix-namespaced index.
		assert.Equal(t, "netskope-test_alert_dlp", names["alert_dlp"].Index)
		assert.Equal(t, "alerts", names["alert_dlp"].Kind)
		assert.Equal(t, "dlp", names["alert_dlp"].Type)
		assert.Equal(t, defaultTimestampField, names["alert_dlp"].TimestampField)
	})

	t.Run("alert_types and event_types select feeds", func(t *testing.T) {
		c := NetskopeConfig{
			ClientOptions: testClientOptions(t), Token: "k", Tenant: "acme.goskope.com",
			AlertTypes: []string{"dlp", "malware"},
			EventTypes: []string{"page", "audit"},
		}
		require.NoError(t, c.Validate())
		got := map[string]bool{}
		for _, f := range c.Feeds {
			got[f.Name] = true
		}
		assert.Equal(t, map[string]bool{
			"alert_dlp": true, "alert_malware": true, "event_page": true, "event_audit": true,
		}, got)
	})

	t.Run("explicit empty alert_types disables alerts", func(t *testing.T) {
		c := NetskopeConfig{
			ClientOptions: testClientOptions(t), Token: "k", Tenant: "acme.goskope.com",
			AlertTypes: []string{},
			EventTypes: []string{"audit"},
		}
		require.NoError(t, c.Validate())
		assert.Len(t, c.Feeds, 1)
		assert.Equal(t, "event_audit", c.Feeds[0].Name)
	})

	t.Run("everything disabled is an error", func(t *testing.T) {
		c := NetskopeConfig{
			ClientOptions: testClientOptions(t), Token: "k", Tenant: "acme.goskope.com",
			AlertTypes: []string{}, EventTypes: []string{},
		}
		assert.Error(t, c.Validate())
	})

	t.Run("custom feeds replace defaults", func(t *testing.T) {
		c := NetskopeConfig{
			ClientOptions: testClientOptions(t), Token: "k", Tenant: "acme.goskope.com",
			AlertTypes: []string{}, EventTypes: []string{}, // ignored when Feeds set
			Feeds: []NetskopeFeed{{Kind: "alerts", Type: "dlp"}},
		}
		require.NoError(t, c.Validate())
		require.Len(t, c.Feeds, 1)
		assert.Equal(t, "alert_dlp", c.Feeds[0].Name)
	})

	t.Run("rejects invalid kind", func(t *testing.T) {
		c := NetskopeConfig{
			ClientOptions: testClientOptions(t), Token: "k", Tenant: "acme.goskope.com",
			Feeds: []NetskopeFeed{{Kind: "things", Type: "dlp"}},
		}
		assert.Error(t, c.Validate())
	})

	t.Run("rejects feed without type", func(t *testing.T) {
		c := NetskopeConfig{
			ClientOptions: testClientOptions(t), Token: "k", Tenant: "acme.goskope.com",
			Feeds: []NetskopeFeed{{Kind: "alerts"}},
		}
		assert.Error(t, c.Validate())
	})

	t.Run("rejects duplicate feed names", func(t *testing.T) {
		c := NetskopeConfig{
			ClientOptions: testClientOptions(t), Token: "k", Tenant: "acme.goskope.com",
			Feeds: []NetskopeFeed{
				{Name: "dup", Kind: "alerts", Type: "dlp"},
				{Name: "dup", Kind: "alerts", Type: "malware"},
			},
		}
		assert.Error(t, c.Validate())
	})

	t.Run("seeds feeds when start_time is set", func(t *testing.T) {
		c := NetskopeConfig{
			ClientOptions: testClientOptions(t), Token: "k", Tenant: "acme.goskope.com",
			StartTime:  "1748000000",
			AlertTypes: []string{"dlp"}, EventTypes: []string{},
		}
		require.NoError(t, c.Validate())
		require.Len(t, c.Feeds, 1)
		assert.Equal(t, "1748000000", c.Feeds[0].seedOp)
	})

	t.Run("index_prefix overrides the sensor seed key", func(t *testing.T) {
		c := NetskopeConfig{
			ClientOptions: testClientOptions(t), Token: "k", Tenant: "acme.goskope.com",
			IndexPrefix: "myprefix",
			AlertTypes:  []string{"dlp"}, EventTypes: []string{},
		}
		require.NoError(t, c.Validate())
		assert.Equal(t, "myprefix_alert_dlp", c.Feeds[0].Index)
	})
}
