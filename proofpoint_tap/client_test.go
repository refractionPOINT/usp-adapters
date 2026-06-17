package usp_proofpoint_tap

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
	t.Run("requires principal", func(t *testing.T) {
		c := ProofpointTapConfig{ClientOptions: testClientOptions(t), Secret: "s"}
		assert.Error(t, c.Validate())
	})

	t.Run("requires secret", func(t *testing.T) {
		c := ProofpointTapConfig{ClientOptions: testClientOptions(t), Principal: "p"}
		assert.Error(t, c.Validate())
	})

	t.Run("valid", func(t *testing.T) {
		c := ProofpointTapConfig{
			ClientOptions: testClientOptions(t),
			Principal:     testPrincipal,
			Secret:        testSecret,
		}
		assert.NoError(t, c.Validate())
	})
}

// TestDefaults verifies an empty URL/PollInterval resolve to the real
// Proofpoint TAP endpoint and the production poll interval.
func TestDefaults(t *testing.T) {
	sink := &captureSink{}
	conf := ProofpointTapConfig{
		ClientOptions: testClientOptions(t),
		Principal:     testPrincipal,
		Secret:        testSecret,
	}
	adapter, chStopped, err := newProofpointTapAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	require.NotNil(t, chStopped)

	assert.Equal(t, logsEndpoint, adapter.apiURL)
	assert.Equal(t, queryInterval*time.Second, adapter.pollInterval)

	// With the default 60s poll interval the fetch goroutine is still idling;
	// nothing was requested or shipped, and Close unblocks it promptly.
	require.NoError(t, adapter.Close())
	assert.Equal(t, 0, sink.count())
}

// TestURLOverride verifies the config override replaces the endpoint queried.
func TestURLOverride(t *testing.T) {
	sink := &captureSink{}
	conf := ProofpointTapConfig{
		ClientOptions: testClientOptions(t),
		Principal:     testPrincipal,
		Secret:        testSecret,
		URL:           "https://tap-proxy.example.com/v2/siem/all",
		PollInterval:  time.Hour, // never polls during the test
	}
	adapter, _, err := newProofpointTapAdapter(context.Background(), conf, sink)
	require.NoError(t, err)

	assert.Equal(t, "https://tap-proxy.example.com/v2/siem/all", adapter.apiURL)
	assert.Equal(t, time.Hour, adapter.pollInterval)
	require.NoError(t, adapter.Close())
}
