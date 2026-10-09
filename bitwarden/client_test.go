package usp_bitwarden

import (
	"context"
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
		Platform:     "json",
		TestSinkMode: true,
		DebugLog:     func(msg string) { t.Logf("DBG: %s", msg) },
		OnWarning:    func(msg string) { t.Logf("WRN: %s", msg) },
		OnError:      func(err error) { t.Logf("ERR: %v", err) },
	}
}

// startTestAdapter builds an adapter against the mock with an injected capture
// sink and a short poll interval, then starts the fetch goroutine exactly as
// the real constructor does. The production poll interval is 30s (longer than
// the whole test package is allowed to run), so tests wire the struct directly
// the way 1password's tests do.
func startTestAdapter(t *testing.T, mockURL string, sink uspSink, pollInterval time.Duration, clientID, clientSecret string) *BitwardenAdapter {
	t.Helper()
	conf := BitwardenConfig{
		ClientOptions:    testClientOptions(t),
		ClientID:         clientID,
		ClientSecret:     clientSecret,
		TokenEndpointURL: mockURL + "/connect/token",
		EventsBaseURL:    mockURL,
	}
	require.NoError(t, conf.Validate())

	a := &BitwardenAdapter{
		conf:          conf,
		ctx:           context.Background(),
		doStop:        utils.NewEvent(),
		chStopped:     make(chan struct{}),
		uspClient:     sink,
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		tokenEndpoint: conf.TokenEndpointURL,
		eventsBaseURL: conf.EventsBaseURL,
		pollInterval:  pollInterval,
	}

	a.wgSenders.Add(1)
	go a.fetchEvents()
	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()
	return a
}

// stop tears a test adapter down without the production Close()'s 1-minute
// drain budget (the capture sink drains instantly anyway).
func (a *BitwardenAdapter) stop() {
	a.doStop.Set()
	a.wgSenders.Wait()
	a.httpClient.CloseIdleConnections()
}

// --- unit tests ---------------------------------------------------------------

func TestValidate(t *testing.T) {
	t.Run("requires client_id", func(t *testing.T) {
		c := BitwardenConfig{ClientOptions: testClientOptions(t), ClientSecret: "s"}
		assert.Error(t, c.Validate())
	})

	t.Run("requires client_secret", func(t *testing.T) {
		c := BitwardenConfig{ClientOptions: testClientOptions(t), ClientID: "i"}
		assert.Error(t, c.Validate())
	})

	t.Run("region defaults to us", func(t *testing.T) {
		c := BitwardenConfig{ClientOptions: testClientOptions(t), ClientID: "i", ClientSecret: "s"}
		require.NoError(t, c.Validate())
		assert.Equal(t, "us", c.Region)
	})

	t.Run("accepts eu region", func(t *testing.T) {
		c := BitwardenConfig{ClientOptions: testClientOptions(t), ClientID: "i", ClientSecret: "s", Region: "eu"}
		assert.NoError(t, c.Validate())
	})

	t.Run("rejects unknown region", func(t *testing.T) {
		c := BitwardenConfig{ClientOptions: testClientOptions(t), ClientID: "i", ClientSecret: "s", Region: "apac"}
		assert.Error(t, c.Validate())
	})

	t.Run("custom token endpoint requires events base url", func(t *testing.T) {
		c := BitwardenConfig{
			ClientOptions: testClientOptions(t), ClientID: "i", ClientSecret: "s",
			TokenEndpointURL: "https://bitwarden.example.com/connect/token",
		}
		assert.Error(t, c.Validate())
	})

	t.Run("custom events base url requires token endpoint", func(t *testing.T) {
		c := BitwardenConfig{
			ClientOptions: testClientOptions(t), ClientID: "i", ClientSecret: "s",
			EventsBaseURL: "https://bitwarden.example.com",
		}
		assert.Error(t, c.Validate())
	})

	t.Run("custom urls conflict with region", func(t *testing.T) {
		c := BitwardenConfig{
			ClientOptions: testClientOptions(t), ClientID: "i", ClientSecret: "s",
			Region:           "us",
			TokenEndpointURL: "https://bitwarden.example.com/connect/token",
			EventsBaseURL:    "https://bitwarden.example.com",
		}
		assert.Error(t, c.Validate())
	})

	t.Run("both custom urls accepted", func(t *testing.T) {
		c := BitwardenConfig{
			ClientOptions: testClientOptions(t), ClientID: "i", ClientSecret: "s",
			TokenEndpointURL: "https://bitwarden.example.com/connect/token",
			EventsBaseURL:    "https://bitwarden.example.com",
		}
		assert.NoError(t, c.Validate())
		assert.Empty(t, c.Region)
	})
}

// TestConstructorURLResolution verifies the constructor seam resolves the
// token/events endpoints from the config exactly as production does, and that
// the adapter shuts down cleanly through Close().
func TestConstructorURLResolution(t *testing.T) {
	ctx := context.Background()

	t.Run("defaults to US cloud", func(t *testing.T) {
		conf := BitwardenConfig{ClientOptions: testClientOptions(t), ClientID: "i", ClientSecret: "s"}
		a, chStopped, err := newBitwardenAdapter(ctx, conf, &captureSink{})
		require.NoError(t, err)
		assert.Equal(t, "https://identity.bitwarden.com/connect/token", a.tokenEndpoint)
		assert.Equal(t, "https://api.bitwarden.com", a.eventsBaseURL)
		assert.Equal(t, defaultPollInterval, a.pollInterval)
		require.NoError(t, a.Close())
		select {
		case <-chStopped:
		case <-time.After(5 * time.Second):
			t.Fatal("adapter did not stop after Close")
		}
	})

	t.Run("eu region uses EU cloud", func(t *testing.T) {
		conf := BitwardenConfig{ClientOptions: testClientOptions(t), ClientID: "i", ClientSecret: "s", Region: "eu"}
		a, _, err := newBitwardenAdapter(ctx, conf, &captureSink{})
		require.NoError(t, err)
		defer a.Close()
		assert.Equal(t, "https://identity.bitwarden.eu/connect/token", a.tokenEndpoint)
		assert.Equal(t, "https://api.bitwarden.eu", a.eventsBaseURL)
	})

	t.Run("custom urls win", func(t *testing.T) {
		conf := BitwardenConfig{
			ClientOptions: testClientOptions(t), ClientID: "i", ClientSecret: "s",
			TokenEndpointURL: "https://bitwarden.example.com/connect/token",
			EventsBaseURL:    "https://bitwarden.example.com",
		}
		a, _, err := newBitwardenAdapter(ctx, conf, &captureSink{})
		require.NoError(t, err)
		defer a.Close()
		assert.Equal(t, "https://bitwarden.example.com/connect/token", a.tokenEndpoint)
		assert.Equal(t, "https://bitwarden.example.com", a.eventsBaseURL)
	})

	t.Run("invalid config rejected", func(t *testing.T) {
		conf := BitwardenConfig{ClientOptions: testClientOptions(t)}
		_, _, err := newBitwardenAdapter(ctx, conf, &captureSink{})
		assert.Error(t, err)
	})
}
