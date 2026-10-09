package usp_okta

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
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
		Platform:     "okta",
		TestSinkMode: true,
		DebugLog:     func(msg string) { t.Logf("DBG: %s", msg) },
		OnWarning:    func(msg string) { t.Logf("WRN: %s", msg) },
		OnError:      func(err error) { t.Logf("ERR: %v", err) },
	}
}

// newDirectAdapter builds an adapter wired to a server without starting any
// goroutines -- for unit testing makeOneRequest in isolation.
func newDirectAdapter(t *testing.T, baseURL, token string) *OktaAdapter {
	t.Helper()
	return &OktaAdapter{
		conf: OktaConfig{
			ClientOptions: testClientOptions(t),
			ApiKey:        token,
			URL:           baseURL,
		},
		httpClient:   &http.Client{Timeout: 5 * time.Second},
		pollInterval: time.Hour,
		doStop:       utils.NewEvent(),
		dedupe:       map[string]int64{},
		ctx:          context.Background(),
	}
}

// --- unit tests ---------------------------------------------------------------

func TestValidate(t *testing.T) {
	t.Run("requires url", func(t *testing.T) {
		c := OktaConfig{ClientOptions: testClientOptions(t), ApiKey: "k"}
		assert.Error(t, c.Validate())
	})

	t.Run("requires api key", func(t *testing.T) {
		c := OktaConfig{ClientOptions: testClientOptions(t), URL: "https://example.okta.com"}
		assert.Error(t, c.Validate())
	})

	t.Run("accepts a complete config", func(t *testing.T) {
		c := OktaConfig{ClientOptions: testClientOptions(t), ApiKey: "k", URL: "https://example.okta.com"}
		assert.NoError(t, c.Validate())
	})
}

func TestPollIntervalSeam(t *testing.T) {
	t.Run("defaults to 30s", func(t *testing.T) {
		sink := &captureSink{}
		conf := OktaConfig{
			ClientOptions: testClientOptions(t),
			ApiKey:        "k",
			URL:           "http://127.0.0.1:1", // never polled: the first poll waits pollInterval
		}
		adapter, _, err := newOktaAdapter(context.Background(), conf, sink)
		require.NoError(t, err)
		defer adapter.Close()
		assert.Equal(t, defaultPollInterval, adapter.pollInterval)
	})

	t.Run("test override applies", func(t *testing.T) {
		sink := &captureSink{}
		conf := OktaConfig{
			ClientOptions: testClientOptions(t),
			ApiKey:        "k",
			URL:           "http://127.0.0.1:1",
			PollInterval:  time.Hour,
		}
		adapter, _, err := newOktaAdapter(context.Background(), conf, sink)
		require.NoError(t, err)
		defer adapter.Close()
		assert.Equal(t, time.Hour, adapter.pollInterval)
	})
}

// TestMakeOneRequestDedupe verifies a second poll over the same (overlapping)
// window returns no events: the per-uuid deduper suppresses everything already
// reported.
func TestMakeOneRequestDedupe(t *testing.T) {
	const token = "tok-dedupe"

	mock := newMockOktaSystemLog(token)
	published := time.Now().UTC().Add(-5 * time.Minute)
	mock.addEvent(systemLogEvent("11111111-aaaa-1111-1111-111111111111", "user.session.start", published))
	mock.addEvent(systemLogEvent("22222222-aaaa-1111-1111-111111111111", "user.session.end", published))

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	a := newDirectAdapter(t, server.URL, token)
	notBefore := time.Now().Add(-10 * time.Minute)

	first := a.makeOneRequest(logsURL, notBefore)
	require.Len(t, first, 2, "first poll must return both events")

	second := a.makeOneRequest(logsURL, notBefore)
	assert.Empty(t, second, "re-polling the same window must not return already-seen events")
	assert.Equal(t, 2, mock.requestCount())
}

// TestMakeOneRequestNon200 verifies a non-200 response yields no items and is
// reported via OnError (the adapter keeps running and retries next poll).
func TestMakeOneRequestNon200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"errorCode":"E0000009","errorSummary":"Internal Server Error"}`))
	}))
	defer server.Close()

	var mu sync.Mutex
	var errCount int
	a := newDirectAdapter(t, server.URL, "tok")
	a.conf.ClientOptions.OnError = func(err error) {
		t.Logf("ERR: %v", err)
		mu.Lock()
		errCount++
		mu.Unlock()
	}

	items := a.makeOneRequest(logsURL, time.Now().Add(-10*time.Minute))
	assert.Nil(t, items)
	mu.Lock()
	assert.Equal(t, 1, errCount, "a non-200 must be reported via OnError")
	mu.Unlock()
	assert.False(t, a.doStop.IsSet(), "a non-200 must not stop the adapter")
}

// TestMakeOneRequestInvalidJSON verifies a malformed body yields no items and
// is reported via OnError.
func TestMakeOneRequestInvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`this is not json`))
	}))
	defer server.Close()

	var mu sync.Mutex
	var errCount int
	a := newDirectAdapter(t, server.URL, "tok")
	a.conf.ClientOptions.OnError = func(err error) {
		t.Logf("ERR: %v", err)
		mu.Lock()
		errCount++
		mu.Unlock()
	}

	items := a.makeOneRequest(logsURL, time.Now().Add(-10*time.Minute))
	assert.Nil(t, items)
	mu.Lock()
	assert.Equal(t, 1, errCount, "invalid JSON must be reported via OnError")
	mu.Unlock()
	assert.False(t, a.doStop.IsSet(), "invalid JSON must not stop the adapter")
}
