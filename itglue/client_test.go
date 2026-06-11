package usp_itglue

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
	t.Run("requires token", func(t *testing.T) {
		c := ITGlueConfig{ClientOptions: testClientOptions(t)}
		assert.Error(t, c.Validate())
	})

	t.Run("requires valid client options", func(t *testing.T) {
		c := ITGlueConfig{Token: "itg.fake-api-key-1111"}
		assert.Error(t, c.Validate())
	})

	t.Run("valid config passes", func(t *testing.T) {
		c := ITGlueConfig{ClientOptions: testClientOptions(t), Token: "itg.fake-api-key-1111"}
		assert.NoError(t, c.Validate())
	})
}

// TestConstructorDefaults verifies the adapter falls back to the public IT Glue
// API root and the production 30s poll interval when neither is overridden, and
// honours overrides when they are set.
func TestConstructorDefaults(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		sink := &captureSink{}
		conf := ITGlueConfig{ClientOptions: testClientOptions(t), Token: "itg.fake-api-key-1111"}
		adapter, _, err := newITGlueAdapter(context.Background(), conf, sink)
		require.NoError(t, err)
		defer adapter.Close()

		assert.Equal(t, URL, adapter.baseURL)
		assert.Equal(t, defaultPollInterval, adapter.pollInterval)
	})

	t.Run("overrides", func(t *testing.T) {
		sink := &captureSink{}
		conf := ITGlueConfig{
			ClientOptions: testClientOptions(t),
			Token:         "itg.fake-api-key-1111",
			BaseURL:       "https://itglue.example.com",
			// A long interval so the poller never fires a request at the fake
			// host during the test; only the stored value is asserted.
			PollInterval: time.Hour,
		}
		adapter, _, err := newITGlueAdapter(context.Background(), conf, sink)
		require.NoError(t, err)
		defer adapter.Close()

		assert.Equal(t, "https://itglue.example.com", adapter.baseURL)
		assert.Equal(t, time.Hour, adapter.pollInterval)
	})
}
