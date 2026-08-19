package usp_sublime

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
		Platform:     "json",
		TestSinkMode: true,
		DebugLog:     func(msg string) { t.Logf("DBG: %s", msg) },
		OnWarning:    func(msg string) { t.Logf("WRN: %s", msg) },
		OnError:      func(err error) { t.Logf("ERR: %v", err) },
	}
}

// newDirectAdapter builds an adapter wired to a base URL without starting any
// goroutines -- for unit testing makeOneRequest in isolation.
func newDirectAdapter(t *testing.T, baseURL string) *SublimeAdapter {
	t.Helper()
	return &SublimeAdapter{
		conf: SublimeConfig{
			ClientOptions: testClientOptions(t),
			ApiKey:        "test-api-key",
			BaseURL:       baseURL,
		},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		doStop:     utils.NewEvent(),
		dedupe:     map[string]int64{},
	}
}

func TestValidate(t *testing.T) {
	t.Run("requires api_key", func(t *testing.T) {
		c := SublimeConfig{ClientOptions: testClientOptions(t)}
		assert.Error(t, c.Validate())
	})

	t.Run("applies defaults", func(t *testing.T) {
		c := SublimeConfig{ClientOptions: testClientOptions(t), ApiKey: "k"}
		require.NoError(t, c.Validate())
		assert.Equal(t, defaultBaseURL, c.BaseURL)
		assert.Equal(t, defaultPollInterval, c.PollInterval)
	})

	t.Run("keeps explicit values", func(t *testing.T) {
		c := SublimeConfig{
			ClientOptions: testClientOptions(t),
			ApiKey:        "k",
			BaseURL:       "https://sublime.example.com",
			PollInterval:  5 * time.Second,
		}
		require.NoError(t, c.Validate())
		assert.Equal(t, "https://sublime.example.com", c.BaseURL)
		assert.Equal(t, 5*time.Second, c.PollInterval)
	})

	t.Run("trims trailing slash from base_url", func(t *testing.T) {
		c := SublimeConfig{
			ClientOptions: testClientOptions(t),
			ApiKey:        "k",
			BaseURL:       "https://platform.sublime.security/",
		}
		require.NoError(t, c.Validate())
		assert.Equal(t, "https://platform.sublime.security", c.BaseURL)
	})
}

// TestNewSublimeAdapterValidates verifies the public constructor runs
// Validate(): the base URL default is applied when the config omits it (the
// bug where an empty base_url produced requests with no scheme/host), and a
// config with no api key is rejected.
func TestNewSublimeAdapterValidates(t *testing.T) {
	t.Run("applies defaults", func(t *testing.T) {
		a, chStopped, err := NewSublimeAdapter(context.Background(), SublimeConfig{
			ClientOptions: testClientOptions(t),
			ApiKey:        "k",
		})
		require.NoError(t, err)
		assert.Equal(t, defaultBaseURL, a.conf.BaseURL)
		assert.Equal(t, defaultPollInterval, a.conf.PollInterval)
		require.NoError(t, a.Close())
		<-chStopped
	})

	t.Run("rejects missing api key", func(t *testing.T) {
		_, _, err := NewSublimeAdapter(context.Background(), SublimeConfig{
			ClientOptions: testClientOptions(t),
		})
		assert.Error(t, err)
	})
}

// TestMakeOneRequestFiltersAndAdvancesSince verifies one poll: events at or
// before `since` are dropped, events with a missing or unparseable created_at
// are skipped, and the returned watermark advances to the newest created_at.
func TestMakeOneRequestFiltersAndAdvancesSince(t *testing.T) {
	now := time.Now().UTC()
	newest := now.Add(10 * time.Minute)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"events": [
			{"id": "old", "created_at": "` + now.Add(-time.Hour).Format(time.RFC3339Nano) + `"},
			{"id": "no-ts"},
			{"id": "bad-ts", "created_at": "not a timestamp"},
			{"id": "new-1", "created_at": "` + now.Add(5*time.Minute).Format(time.RFC3339Nano) + `"},
			{"id": "new-2", "created_at": "` + newest.Format(time.RFC3339Nano) + `"}
		], "count": 5, "total": 5}`))
	}))
	defer server.Close()

	a := newDirectAdapter(t, server.URL)
	items, newSince, err := a.makeOneRequest(now)
	require.NoError(t, err)

	ids := []string{}
	for _, item := range items {
		id, _ := item["id"].(string)
		ids = append(ids, id)
	}
	assert.ElementsMatch(t, []string{"new-1", "new-2"}, ids,
		"only events newer than since with a valid created_at are returned")
	assert.True(t, newSince.Equal(newest), "since must advance to the newest created_at, got %v want %v", newSince, newest)
}

// TestMakeOneRequestDedupes verifies an event id already seen in a previous
// poll is not returned again even when its created_at is inside the window.
func TestMakeOneRequestDedupes(t *testing.T) {
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"events": [
			{"id": "evt-1", "created_at": "` + now.Add(5*time.Minute).Format(time.RFC3339Nano) + `"}
		], "count": 1, "total": 1}`))
	}))
	defer server.Close()

	a := newDirectAdapter(t, server.URL)

	items, _, err := a.makeOneRequest(now)
	require.NoError(t, err)
	require.Len(t, items, 1)

	// Same window re-polled: the id is in the dedupe map, nothing returns.
	items, _, err = a.makeOneRequest(now)
	require.NoError(t, err)
	assert.Empty(t, items, "an already-seen event id must not be returned twice")
}

// TestMakeOneRequestInvalidJSON verifies a non-JSON body surfaces an error and
// returns no items, with the watermark unchanged.
func TestMakeOneRequestInvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not json`))
	}))
	defer server.Close()

	a := newDirectAdapter(t, server.URL)
	since := time.Now()
	items, newSince, err := a.makeOneRequest(since)
	assert.Error(t, err)
	assert.Nil(t, items)
	assert.True(t, newSince.Equal(since), "since must not advance on a bad response")
}

// TestMakeOneRequestSendsTimeFilter verifies each poll carries a
// created_at[gte] filter of the watermark minus the overlap period, so the
// API only returns new events instead of the entire audit log history.
func TestMakeOneRequestSendsTimeFilter(t *testing.T) {
	var gotGte string
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotGte = r.URL.Query().Get("created_at[gte]")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"events": [], "count": 0, "total": 0}`))
	}))
	defer server.Close()

	a := newDirectAdapter(t, server.URL)
	_, _, err := a.makeOneRequest(now)
	require.NoError(t, err)

	require.NotEmpty(t, gotGte, "poll requests must carry a created_at[gte] filter")
	gte, err := time.Parse(time.RFC3339, gotGte)
	require.NoError(t, err)
	want := now.Add(-overlapPeriod).Truncate(time.Second)
	assert.True(t, gte.Equal(want), "created_at[gte] must be since minus the overlap period, got %v want %v", gte, want)
}

// TestMakeOneRequestPaginates verifies a full page triggers a follow-up
// request at the next offset and the results are combined.
func TestMakeOneRequestPaginates(t *testing.T) {
	now := time.Now().UTC()
	eventTime := now.Add(time.Minute).Format(time.RFC3339Nano)
	var offsets []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offsets = append(offsets, r.URL.Query().Get("offset"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if r.URL.Query().Get("offset") == "0" {
			var b strings.Builder
			b.WriteString(`{"events": [`)
			for i := 0; i < pageLimit; i++ {
				if i > 0 {
					b.WriteString(",")
				}
				fmt.Fprintf(&b, `{"id": "evt-%d", "created_at": "%s"}`, i, eventTime)
			}
			fmt.Fprintf(&b, `], "count": %d, "total": %d}`, pageLimit, pageLimit+1)
			_, _ = w.Write([]byte(b.String()))
			return
		}
		_, _ = w.Write([]byte(`{"events": [{"id": "evt-last", "created_at": "` + eventTime + `"}], "count": 1, "total": 501}`))
	}))
	defer server.Close()

	a := newDirectAdapter(t, server.URL)
	items, _, err := a.makeOneRequest(now)
	require.NoError(t, err)
	assert.Len(t, items, pageLimit+1, "events from all pages must be combined")
	assert.Equal(t, []string{"0", "500"}, offsets, "a full page must trigger a request at the next offset")
}

// TestMakeOneRequestNon200 verifies a non-200 returns no items, preserves the
// watermark, and surfaces the failure both through OnError and as an error.
func TestMakeOneRequestNon200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"boom"}`))
	}))
	defer server.Close()

	errs := 0
	a := newDirectAdapter(t, server.URL)
	a.conf.ClientOptions.OnError = func(err error) { errs++; t.Logf("ERR: %v", err) }

	since := time.Now()
	items, newSince, err := a.makeOneRequest(since)
	assert.Nil(t, items)
	assert.True(t, newSince.Equal(since), "since must not advance on an error response")
	assert.Equal(t, 1, errs, "a non-200 must be reported via OnError")
	assert.Error(t, err, "a non-200 must also be returned as an error")
}
