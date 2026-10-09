package usp_slack

import (
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
		c := SlackConfig{ClientOptions: testClientOptions(t)}
		assert.Error(t, c.Validate())
	})

	t.Run("requires valid client options", func(t *testing.T) {
		c := SlackConfig{Token: "xoxp-1111111111-fake-test-token"}
		assert.Error(t, c.Validate())
	})

	t.Run("accepts a complete config", func(t *testing.T) {
		c := SlackConfig{
			ClientOptions: testClientOptions(t),
			Token:         "xoxp-1111111111-fake-test-token",
		}
		assert.NoError(t, c.Validate())
	})
}

// TestDefaults verifies an empty ApiURL/PollInterval fall back to the public
// Slack endpoint and the production poll interval, and that overrides stick.
func TestDefaults(t *testing.T) {
	t.Run("defaults applied", func(t *testing.T) {
		sink := &captureSink{}
		a, _, err := newSlackAdapter(t.Context(), SlackConfig{
			ClientOptions: testClientOptions(t),
			Token:         "xoxp-1111111111-fake-test-token",
		}, sink)
		require.NoError(t, err)
		// Close before the first poll interval elapses: no request is ever
		// made to the (real) default endpoint.
		defer a.Close()

		assert.Equal(t, "https://api.slack.com/audit/v1/logs", a.apiURL)
		assert.Equal(t, 5*time.Second, a.pollInterval)
	})

	t.Run("overrides applied", func(t *testing.T) {
		sink := &captureSink{}
		a, _, err := newSlackAdapter(t.Context(), SlackConfig{
			ClientOptions: testClientOptions(t),
			Token:         "xoxp-1111111111-fake-test-token",
			ApiURL:        "https://slack.example.com/audit/v1/logs",
			PollInterval:  123 * time.Millisecond,
		}, sink)
		require.NoError(t, err)
		defer a.Close()

		assert.Equal(t, "https://slack.example.com/audit/v1/logs", a.apiURL)
		assert.Equal(t, 123*time.Millisecond, a.pollInterval)
	})
}
