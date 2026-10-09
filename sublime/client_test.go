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
// goroutines -- for unit testing poll in isolation. It started at `start` and
// its clock reads whatever *now holds.
func newDirectAdapter(t *testing.T, baseURL string, start time.Time, now *time.Time) *SublimeAdapter {
	t.Helper()
	return &SublimeAdapter{
		conf: SublimeConfig{
			ClientOptions: testClientOptions(t),
			ApiKey:        "test-api-key",
			BaseURL:       baseURL,
		},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		doStop:     utils.NewEvent(),
		now:        func() time.Time { return *now },
		start:      start,
		cursor:     start,
		dedupe:     map[string]time.Time{},
	}
}

// collect returns a ship callback that appends to *out.
func collect(out *[]utils.Dict) func([]utils.Dict) error {
	return func(items []utils.Dict) error {
		*out = append(*out, items...)
		return nil
	}
}

// setDate stamps a response with Sublime's clock reading now.
func setDate(w http.ResponseWriter, now time.Time) {
	w.Header().Set("Date", now.UTC().Format(http.TimeFormat))
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

	t.Run("trims trailing slash from base url", func(t *testing.T) {
		c := SublimeConfig{
			ClientOptions: testClientOptions(t),
			ApiKey:        "k",
			BaseURL:       "https://platform.sublime.security/",
		}
		require.NoError(t, c.Validate())
		assert.Equal(t, "https://platform.sublime.security", c.BaseURL,
			"a trailing slash must be trimmed so the request path is not //v0/...")
	})
}

// TestPollFiltersAndAdvancesCursor verifies one poll: events created before
// the adapter started are dropped, events with a missing or unparseable
// created_at are still shipped (the server placed them in the window) with a
// warning, the order is the reverse of the API's newest-first order, and the
// cursor advances to the poll time -- which is also sent as the exclusive
// upper bound of the requested window.
func TestPollFiltersAndAdvancesCursor(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	now := start.Add(10 * time.Minute)
	var gotGTE, gotLT string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotGTE = r.URL.Query().Get("created_at[gte]")
		gotLT = r.URL.Query().Get("created_at[lt]")
		setDate(w, now)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Newest-first, as the live API returns them.
		_, _ = w.Write([]byte(`{"events": [
			{"id": "new-2", "created_at": "` + start.Add(9*time.Minute).Format(time.RFC3339Nano) + `"},
			{"id": "new-1", "created_at": "` + start.Add(5*time.Minute).Format(time.RFC3339Nano) + `"},
			{"id": "bad-ts", "created_at": "not a timestamp"},
			{"id": "no-ts"},
			{"id": "pre-start", "created_at": "` + start.Add(-10*time.Second).Format(time.RFC3339Nano) + `"}
		], "count": 5, "total": 5}`))
	}))
	defer server.Close()

	a := newDirectAdapter(t, server.URL, start, &now)
	warnings := 0
	a.conf.ClientOptions.OnWarning = func(msg string) { warnings++; t.Logf("WRN: %s", msg) }
	var items []utils.Dict
	require.NoError(t, a.poll(collect(&items)))

	ids := []string{}
	for _, item := range items {
		id, _ := item["id"].(string)
		ids = append(ids, id)
	}
	assert.Equal(t, []string{"no-ts", "bad-ts", "new-1", "new-2"}, ids,
		"pre-start events are dropped, the rest ship oldest first")
	assert.Equal(t, 2, warnings, "each event with an unusable created_at must be reported")
	assert.True(t, a.cursor.Equal(now), "the cursor must advance to the poll time, got %v want %v", a.cursor, now)
	assert.Equal(t, start.Add(-overlapPeriod).Format(time.RFC3339Nano), gotGTE, "the window must start one overlap before the cursor")
	assert.Equal(t, now.Format(time.RFC3339Nano), gotLT, "the window must end at the poll time")
}

// TestPollDedupesOverlap verifies an event re-fetched in the next poll's
// overlap is not returned twice.
func TestPollDedupesOverlap(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	now := start.Add(time.Minute)
	evtAt := now.Add(-5 * time.Second) // inside the next poll's overlap
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDate(w, now)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"events": [
			{"id": "evt-1", "created_at": "` + evtAt.Format(time.RFC3339Nano) + `"}
		], "count": 1, "total": 1}`))
	}))
	defer server.Close()

	a := newDirectAdapter(t, server.URL, start, &now)
	var items []utils.Dict
	require.NoError(t, a.poll(collect(&items)))
	require.Len(t, items, 1)

	now = now.Add(time.Second)
	items = nil
	require.NoError(t, a.poll(collect(&items)))
	assert.Empty(t, items, "an already-shipped event id must not be returned twice")
}

// TestPollInvalidJSON verifies a non-JSON body surfaces an error, returns no
// items and leaves the cursor unchanged.
func TestPollInvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not json`))
	}))
	defer server.Close()

	start := time.Now().UTC().Truncate(time.Second)
	now := start.Add(time.Minute)
	a := newDirectAdapter(t, server.URL, start, &now)
	var items []utils.Dict
	err := a.poll(collect(&items))
	assert.Error(t, err)
	assert.Nil(t, items)
	assert.True(t, a.cursor.Equal(start), "the cursor must not advance on a bad response")
}

// TestPollNon200 pins the adapter's behavior on a non-200: no items, the
// cursor is preserved, the failure is reported through OnError, and an
// explicit error is returned to the caller.
func TestPollNon200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"boom"}`))
	}))
	defer server.Close()

	start := time.Now().UTC().Truncate(time.Second)
	now := start.Add(time.Minute)
	errs := 0
	a := newDirectAdapter(t, server.URL, start, &now)
	a.conf.ClientOptions.OnError = func(err error) { errs++; t.Logf("ERR: %v", err) }

	var items []utils.Dict
	err := a.poll(collect(&items))
	assert.Nil(t, items)
	assert.True(t, a.cursor.Equal(start), "the cursor must not advance on an error response")
	assert.Equal(t, 1, errs, "a non-200 must be reported via OnError")
	assert.Error(t, err, "a non-200 must surface an explicit error to the caller")
}

// TestPollEventsWithoutID verifies events lacking an id are deduplicated on
// their content: two distinct ones both ship, and neither ships again when the
// next poll re-reads the overlap.
func TestPollEventsWithoutID(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	now := start.Add(time.Minute)
	at := now.Add(-5 * time.Second).Format(time.RFC3339Nano)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDate(w, now)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"events": [
			{"type": "a", "created_at": "` + at + `"},
			{"type": "b", "created_at": "` + at + `"}
		], "count": 2, "total": 2}`))
	}))
	defer server.Close()

	a := newDirectAdapter(t, server.URL, start, &now)
	var items []utils.Dict
	require.NoError(t, a.poll(collect(&items)))
	assert.Len(t, items, 2, "distinct events without an id must both ship")

	now = now.Add(time.Second)
	items = nil
	require.NoError(t, a.poll(collect(&items)))
	assert.Empty(t, items, "events without an id must not re-ship from the overlap")
}
