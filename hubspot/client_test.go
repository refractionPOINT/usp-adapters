package usp_hubspot

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

func TestConfigValidate(t *testing.T) {
	t.Run("requires access_token", func(t *testing.T) {
		c := HubSpotConfig{ClientOptions: testClientOptions(t)}
		assert.Error(t, c.Validate())
	})

	t.Run("valid", func(t *testing.T) {
		c := HubSpotConfig{
			ClientOptions: testClientOptions(t),
			AccessToken:   "fake-hubspot-token-1111",
		}
		assert.NoError(t, c.Validate())
	})
}

// TestDefaults verifies an empty URL/PollInterval resolve to the real HubSpot
// endpoint and the production poll interval.
func TestDefaults(t *testing.T) {
	sink := &captureSink{}
	conf := HubSpotConfig{
		ClientOptions: testClientOptions(t),
		AccessToken:   "fake-hubspot-token-1111",
	}
	adapter, chStopped, err := newHubSpotAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	require.NotNil(t, chStopped)

	assert.Equal(t, logsURL, adapter.apiURL)
	assert.Equal(t, defaultPollInterval, adapter.pollInterval)

	// With the default 30s poll interval the fetch goroutine is still idling;
	// nothing was requested or shipped, and Close unblocks it promptly.
	require.NoError(t, adapter.Close())
	assert.Equal(t, 0, sink.count())
}

// TestURLOverride verifies the config override replaces the endpoint queried.
func TestURLOverride(t *testing.T) {
	sink := &captureSink{}
	conf := HubSpotConfig{
		ClientOptions: testClientOptions(t),
		AccessToken:   "fake-hubspot-token-1111",
		URL:           "https://hubspot-proxy.example.com/audit-logs",
		PollInterval:  time.Hour, // never polls during the test
	}
	adapter, _, err := newHubSpotAdapter(context.Background(), conf, sink)
	require.NoError(t, err)

	assert.Equal(t, "https://hubspot-proxy.example.com/audit-logs", adapter.apiURL)
	assert.Equal(t, time.Hour, adapter.pollInterval)
	require.NoError(t, adapter.Close())
}
