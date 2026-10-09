package usp_pandadoc

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
		Platform:     "json",
		TestSinkMode: true,
		DebugLog:     func(msg string) { t.Logf("DBG: %s", msg) },
		OnWarning:    func(msg string) { t.Logf("WRN: %s", msg) },
		OnError:      func(err error) { t.Logf("ERR: %v", err) },
	}
}

func TestValidate(t *testing.T) {
	t.Run("requires api_key", func(t *testing.T) {
		c := PandaDocConfig{ClientOptions: testClientOptions(t)}
		assert.Error(t, c.Validate())
	})

	t.Run("accepts a complete config", func(t *testing.T) {
		c := PandaDocConfig{ClientOptions: testClientOptions(t), ApiKey: "fake-test-api-key"}
		assert.NoError(t, c.Validate())
	})
}

// TestConstructorDefaults verifies the URL and PollInterval seams default to
// production values when left empty, and are honored when set.
func TestConstructorDefaults(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		sink := &captureSink{}
		conf := PandaDocConfig{ClientOptions: testClientOptions(t), ApiKey: "fake-test-api-key"}
		adapter, _, err := newPandaDocAdapter(context.Background(), conf, sink)
		require.NoError(t, err)
		defer adapter.Close()

		assert.Equal(t, defaultLogsEndpoint, adapter.logsURL)
		assert.Equal(t, defaultPollInterval, adapter.pollInterval)
	})

	t.Run("overrides", func(t *testing.T) {
		sink := &captureSink{}
		conf := PandaDocConfig{
			ClientOptions: testClientOptions(t),
			ApiKey:        "fake-test-api-key",
			URL:           "https://pandadoc.example.com/public/v1/logs",
			PollInterval:  time.Hour,
		}
		adapter, _, err := newPandaDocAdapter(context.Background(), conf, sink)
		require.NoError(t, err)
		defer adapter.Close()

		assert.Equal(t, "https://pandadoc.example.com/public/v1/logs", adapter.logsURL)
		assert.Equal(t, time.Hour, adapter.pollInterval)
	})
}
