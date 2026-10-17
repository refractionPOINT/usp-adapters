package usp_proofpoint_isolation

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
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

func TestValidate(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		c := ProofpointIsolationConfig{ClientOptions: testClientOptions(t), WebAPIKey: " k "}
		require.NoError(t, c.Validate())
		assert.Equal(t, "k", c.WebAPIKey)
		assert.Equal(t, defaultWebBaseURL, c.WebBaseURL)
		assert.Equal(t, defaultURLBaseURL, c.URLBaseURL)
		assert.Equal(t, defaultPageSize, c.PageSize)
		assert.Equal(t, defaultMaxPages, c.MaxPages)
		assert.Equal(t, defaultMaxRetryAttempts, c.MaxRetryAttempts)
		assert.Equal(t, 5*time.Minute, c.pollInterval)
		assert.Equal(t, time.Hour, c.initialLookback)
		assert.Equal(t, 15*time.Minute, c.overlap)
		assert.Equal(t, 5*time.Second, c.jobPollInterval)
		assert.Equal(t, 10*time.Minute, c.jobTimeout)
		assert.Equal(t, 24*time.Hour, c.dedupeTTL)

		feeds := c.feeds()
		require.Len(t, feeds, 1)
		assert.Equal(t, eventTypeWeb, feeds[0].eventType)
	})

	t.Run("both products", func(t *testing.T) {
		c := ProofpointIsolationConfig{ClientOptions: testClientOptions(t), WebAPIKey: "w", URLAPIKey: "u",
			WebBaseURL: "https://web.test/", URLBaseURL: "https://url.test/"}
		require.NoError(t, c.Validate())
		feeds := c.feeds()
		require.Len(t, feeds, 2)
		assert.Equal(t, isoFeed{eventType: eventTypeWeb, baseURL: "https://web.test", apiKey: "w"}, feeds[0])
		assert.Equal(t, isoFeed{eventType: eventTypeURL, baseURL: "https://url.test", apiKey: "u"}, feeds[1])
	})

	t.Run("explicit values", func(t *testing.T) {
		c := ProofpointIsolationConfig{ClientOptions: testClientOptions(t), URLAPIKey: "u",
			PageSize: 50000, PollIntervalSeconds: 60, InitialLookbackMinutes: 43200, OverlapMinutes: 5,
			JobPollIntervalSeconds: 1, JobTimeoutMinutes: 2, DedupeTTLHours: 1}
		require.NoError(t, c.Validate())
		assert.Equal(t, maxPageSize, c.PageSize)
		assert.Equal(t, time.Minute, c.pollInterval)
		assert.Equal(t, 30*24*time.Hour, c.initialLookback)
		assert.Equal(t, 5*time.Minute, c.overlap)
		assert.Equal(t, time.Second, c.jobPollInterval)
		assert.Equal(t, 2*time.Minute, c.jobTimeout)
		assert.Equal(t, time.Hour, c.dedupeTTL)
	})

	t.Run("missing keys", func(t *testing.T) {
		c := ProofpointIsolationConfig{ClientOptions: testClientOptions(t)}
		assert.ErrorContains(t, c.Validate(), "missing web_api_key")
	})

	t.Run("lookback beyond retention", func(t *testing.T) {
		c := ProofpointIsolationConfig{ClientOptions: testClientOptions(t), WebAPIKey: "k", InitialLookbackMinutes: 43201}
		assert.ErrorContains(t, c.Validate(), "30-day retention")
	})

	t.Run("dedupe ttl too short", func(t *testing.T) {
		c := ProofpointIsolationConfig{ClientOptions: testClientOptions(t), WebAPIKey: "k",
			DedupeTTLHours: 1, OverlapMinutes: 60}
		assert.ErrorContains(t, c.Validate(), "dedupe_ttl_hours")
	})

	t.Run("bad client options", func(t *testing.T) {
		c := ProofpointIsolationConfig{WebAPIKey: "k"}
		assert.ErrorContains(t, c.Validate(), "client_options")
	})
}

func TestParseTimestamp(t *testing.T) {
	want := time.Date(2025, 1, 1, 19, 44, 35, 0, time.UTC)
	for _, s := range []string{
		"2025-01-01T19:44:35.000+0000", // the API's format
		"2025-01-01T14:44:35.000-0500",
		"2025-01-01T19:44:35+0000",
		"2025-01-01T19:44:35.000Z",
		"2025-01-01T19:44:35Z",
		"2025-01-01T19:44:35.000",
		"2025-01-01 19:44:35",
	} {
		got, ok := parseTimestamp(s)
		assert.True(t, ok, s)
		assert.True(t, want.Equal(got), "%s => %v", s, got)
	}
	for _, s := range []string{"", "yesterday", "2025-13-01T00:00:00Z"} {
		_, ok := parseTimestamp(s)
		assert.False(t, ok, s)
	}
}

func TestParseUsageDataResponse(t *testing.T) {
	r, err := parseUsageDataResponse([]byte(`{"jobId":"j-1","status":"COMPLETED","pageToken":"p2","total":2,
		"data":[{"url":"a"},{"url":"b"}]}`))
	require.NoError(t, err)
	assert.Equal(t, "j-1", r.JobID)
	assert.Equal(t, statusCompleted, r.Status)
	assert.Equal(t, "p2", r.PageToken)
	assert.Len(t, r.Data, 2)

	// Opaque tokens sent as numbers are echoed back as their literal text.
	r, err = parseUsageDataResponse([]byte(`{"jobId":12345678901234567890,"status":"RUNNING","pageToken":null,"data":null}`))
	require.NoError(t, err)
	assert.Equal(t, "12345678901234567890", r.JobID)
	assert.Equal(t, "RUNNING", r.Status)
	assert.Equal(t, "", r.PageToken)
	assert.Empty(t, r.Data)

	_, err = parseUsageDataResponse([]byte(`[]`))
	assert.Error(t, err)
	_, err = parseUsageDataResponse([]byte(`{"data":{"not":"an array"}}`))
	assert.ErrorContains(t, err, "not an array")
}

func TestIsTransientError(t *testing.T) {
	assert.False(t, isTransientError(nil))
	assert.True(t, isTransientError(&HTTPError{StatusCode: 500}))
	assert.True(t, isTransientError(&HTTPError{StatusCode: 503}))
	assert.True(t, isTransientError(&HTTPError{StatusCode: 429}))
	assert.False(t, isTransientError(&HTTPError{StatusCode: 400}))
	assert.False(t, isTransientError(&HTTPError{StatusCode: 401}))
	assert.False(t, isTransientError(&HTTPError{StatusCode: 403}))
	assert.False(t, isTransientError(context.Canceled))
	assert.True(t, isTransientError(errors.New(`failed to execute request "x": dial tcp: connection refused`)))
	assert.False(t, isTransientError(errors.New("something else")))
}

func TestIsFailedStatus(t *testing.T) {
	for _, s := range []string{"FAILED", "failed", "ERROR", "CANCELLED", "ABORTED", "EXPIRED"} {
		assert.True(t, isFailedStatus(s), s)
	}
	for _, s := range []string{"RUNNING", "PENDING", "QUEUED", "IN_PROGRESS", "COMPLETED"} {
		assert.False(t, isFailedStatus(s), s)
	}
}

func TestDedupeKey(t *testing.T) {
	web := isoFeed{eventType: eventTypeWeb}
	url := isoFeed{eventType: eventTypeURL}
	a := []byte(`{"url":"https://a/","userName":"x","date":"2025-01-01T00:00:00.000+0000"}`)
	reordered := []byte(`{ "date":"2025-01-01T00:00:00.000+0000", "userName":"x", "url":"https://a/" }`)
	other := []byte(`{"url":"https://a/","userName":"y","date":"2025-01-01T00:00:00.000+0000"}`)

	assert.Equal(t, dedupeKey(web, a), dedupeKey(web, reordered), "key order and whitespace must not matter")
	assert.NotEqual(t, dedupeKey(web, a), dedupeKey(web, other))
	assert.NotEqual(t, dedupeKey(web, a), dedupeKey(url, a), "keys are namespaced per product")

	// Large integers beyond float64 precision must still distinguish records.
	big1 := []byte(`{"n":12345678901234567890}`)
	big2 := []byte(`{"n":12345678901234567891}`)
	assert.NotEqual(t, dedupeKey(web, big1), dedupeKey(web, big2))
}

// TestGetUsageDataRequest verifies the exact request the client sends: method,
// path, bearer auth, and the from/to/pageSize/jobId/pageToken parameters.
func TestGetUsageDataRequest(t *testing.T) {
	var gotMethod, gotPath, gotAuth string
	var gotQuery map[string][]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotAuth = r.Method, r.URL.Path, r.Header.Get("Authorization")
		gotQuery = r.URL.Query()
		_, _ = w.Write([]byte(`{"jobId":"j","status":"COMPLETED","data":[]}`))
	}))
	defer srv.Close()

	c := NewIsoClient(srv.URL+"/", "secret")
	defer c.Close()
	from := time.Date(2026, 10, 12, 9, 0, 0, 123_000_000, time.UTC)
	to := time.Date(2026, 10, 12, 10, 0, 0, 0, time.FixedZone("x", 3600))
	_, err := c.GetUsageData(context.Background(), usageDataRequest{
		From: from, To: to, PageSize: 500, JobID: "job-9", PageToken: "tok-2",
	})
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, gotMethod)
	assert.Equal(t, "/api/v2/reporting/usage-data", gotPath)
	assert.Equal(t, "Bearer secret", gotAuth)
	assert.Equal(t, []string{"2026-10-12T09:00:00.123"}, gotQuery["from"])
	assert.Equal(t, []string{"2026-10-12T09:00:00.000"}, gotQuery["to"], "to must be rendered in UTC")
	assert.Equal(t, []string{"500"}, gotQuery["pageSize"])
	assert.Equal(t, []string{"job-9"}, gotQuery["jobId"])
	assert.Equal(t, []string{"tok-2"}, gotQuery["pageToken"])
	assert.NotContains(t, gotQuery, "key", "the API key must not be sent in the URL")
}

func TestGetUsageDataHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"forbidden"}`))
	}))
	defer srv.Close()

	c := NewIsoClient(srv.URL, "k")
	defer c.Close()
	_, err := c.GetUsageData(context.Background(), usageDataRequest{From: time.Now(), To: time.Now(), PageSize: 1})
	var httpErr *HTTPError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, http.StatusForbidden, httpErr.StatusCode)
	assert.Contains(t, httpErr.Body, "forbidden")
}

// TestCloseIdempotent verifies Close can be called repeatedly and the stop
// channel closes.
func TestCloseIdempotent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobId":"j","status":"COMPLETED","data":[]}`))
	}))
	defer srv.Close()

	conf := fastConfig(t)
	conf.WebAPIKey = "k"
	conf.WebBaseURL = srv.URL
	a, chStopped, err := newProofpointIsolationAdapter(context.Background(), conf, &captureSink{})
	require.NoError(t, err)
	require.NoError(t, a.Close())
	require.NoError(t, a.Close())
	select {
	case <-chStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop channel did not close")
	}
}
