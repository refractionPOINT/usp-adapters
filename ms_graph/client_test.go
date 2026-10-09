package usp_ms_graph

import (
	"context"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClientOptions returns ClientOptions wired for a sink (no real
// LimaCharlie connection) with the logging callbacks pointed at the test log.
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

func validTestConfig(t *testing.T) MsGraphConfig {
	t.Helper()
	return MsGraphConfig{
		ClientOptions: testClientOptions(t),
		TenantID:      testTenantID,
		ClientID:      testClientID,
		ClientSecret:  testClientSecret,
		URL:           testResource,
	}
}

func TestValidate(t *testing.T) {
	t.Run("complete config passes", func(t *testing.T) {
		c := validTestConfig(t)
		assert.NoError(t, c.Validate())
	})

	t.Run("requires tenant_id", func(t *testing.T) {
		c := validTestConfig(t)
		c.TenantID = ""
		assert.Error(t, c.Validate())
	})

	t.Run("requires client_id", func(t *testing.T) {
		c := validTestConfig(t)
		c.ClientID = ""
		assert.Error(t, c.Validate())
	})

	t.Run("requires client_secret", func(t *testing.T) {
		c := validTestConfig(t)
		c.ClientSecret = ""
		assert.Error(t, c.Validate())
	})

	t.Run("requires url", func(t *testing.T) {
		c := validTestConfig(t)
		c.URL = ""
		assert.Error(t, c.Validate())
	})
}

// TestConstructorDefaults verifies the endpoint and poll-interval overrides
// default to the production Microsoft endpoints and 30s cadence when unset.
// The default poll interval also means the fetch goroutine never issues a
// request before Close() stops it, so no network is touched here.
func TestConstructorDefaults(t *testing.T) {
	t.Run("empty overrides use production defaults", func(t *testing.T) {
		sink := &captureSink{}
		adapter, _, err := newMsGraphAdapter(context.Background(), validTestConfig(t), sink)
		require.NoError(t, err)
		defer adapter.Close()

		assert.Equal(t, defaultLoginEndpoint, adapter.loginEndpoint)
		assert.Equal(t, defaultPollInterval, adapter.pollInterval)
	})

	t.Run("overrides are honored and normalized", func(t *testing.T) {
		conf := validTestConfig(t)
		conf.LoginEndpoint = "https://login.example.com/"
		conf.PollInterval = defaultPollInterval + time.Minute

		sink := &captureSink{}
		adapter, _, err := newMsGraphAdapter(context.Background(), conf, sink)
		require.NoError(t, err)
		defer adapter.Close()

		assert.Equal(t, "https://login.example.com", adapter.loginEndpoint,
			"trailing slash should be trimmed")
		assert.Equal(t, defaultPollInterval+time.Minute, adapter.pollInterval)
	})
}
