package usp_sophos

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

// testConfig returns a complete valid config pointed at nothing in particular;
// tests override URL/AuthURL to aim it at their mock server.
func testConfig(t *testing.T) SophosConfig {
	t.Helper()
	return SophosConfig{
		ClientOptions: testClientOptions(t),
		ClientId:      "11111111-2222-3333-4444-555555555555",
		ClientSecret:  "fake-client-secret-for-tests",
		TenantId:      "11111111-1111-1111-1111-aaaaaaaaaaaa",
		URL:           "https://api-us03.central.example.com",
	}
}

func TestConfigValidate(t *testing.T) {
	t.Run("complete config passes", func(t *testing.T) {
		c := testConfig(t)
		assert.NoError(t, c.Validate())
	})

	t.Run("requires url", func(t *testing.T) {
		c := testConfig(t)
		c.URL = ""
		assert.Error(t, c.Validate())
	})

	t.Run("requires client id", func(t *testing.T) {
		c := testConfig(t)
		c.ClientId = ""
		assert.Error(t, c.Validate())
	})

	t.Run("requires client secret", func(t *testing.T) {
		c := testConfig(t)
		c.ClientSecret = ""
		assert.Error(t, c.Validate())
	})

	t.Run("requires tenant id", func(t *testing.T) {
		c := testConfig(t)
		c.TenantId = ""
		assert.Error(t, c.Validate())
	})
}

// TestSeamDefaults verifies the test seams default to production values when
// unset, and apply overrides when set.
func TestSeamDefaults(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		conf := testConfig(t)
		adapter, _, err := newSophosAdapter(context.Background(), conf, &captureSink{})
		require.NoError(t, err)
		defer adapter.Close()

		assert.Equal(t, defaultAuthURL, adapter.authURL,
			"empty auth_url must fall back to the real Sophos OAuth endpoint")
		assert.Equal(t, defaultPollInterval, adapter.pollInterval)
	})

	t.Run("overrides", func(t *testing.T) {
		conf := testConfig(t)
		conf.AuthURL = "https://auth.example.com/api/v2/oauth2/token"
		conf.PollInterval = 1 * time.Hour
		adapter, _, err := newSophosAdapter(context.Background(), conf, &captureSink{})
		require.NoError(t, err)
		defer adapter.Close()

		assert.Equal(t, "https://auth.example.com/api/v2/oauth2/token", adapter.authURL)
		assert.Equal(t, 1*time.Hour, adapter.pollInterval)
	})
}
