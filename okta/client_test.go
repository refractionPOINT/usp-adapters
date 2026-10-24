package usp_okta

import (
	"context"
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
		Platform:     "okta",
		TestSinkMode: true,
		DebugLog:     func(msg string) { t.Logf("DBG: %s", msg) },
		OnWarning:    func(msg string) { t.Logf("WRN: %s", msg) },
		OnError:      func(err error) { t.Logf("ERR: %v", err) },
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
