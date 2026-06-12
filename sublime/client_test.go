package usp_sublime

import (
	"net/http"
	"net/http/httptest"
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

// TestMakeOneRequestNon200 pins the adapter's behavior on a non-200: no items,
// the watermark is preserved, and (a long-standing quirk) no error is returned
// -- the failure is only reported through OnError.
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
	// Pin the current behavior: the non-200 path returns a nil error (the
	// error variable it returns belongs to the preceding, successful, Do call).
	assert.NoError(t, err)
}
