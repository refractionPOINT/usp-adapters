package usp_cloudflare_access

import (
	"context"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testContext returns a context bounded generously enough for the mock-backed
// end-to-end tests, and its cancel func.
func testContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 15*time.Second)
}

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

func validConfig(t *testing.T) CloudflareAccessConfig {
	return CloudflareAccessConfig{
		ClientOptions: testClientOptions(t),
		APIToken:      "test-token",
		AccountID:     "acct-123",
	}
}

func TestValidateRequiresAPIToken(t *testing.T) {
	conf := validConfig(t)
	conf.APIToken = ""
	require.Error(t, conf.Validate())
}

func TestValidateRequiresAccountID(t *testing.T) {
	conf := validConfig(t)
	conf.AccountID = ""
	require.Error(t, conf.Validate())
}

func TestValidateFillsDefaults(t *testing.T) {
	conf := validConfig(t)
	require.NoError(t, conf.Validate())

	assert.Equal(t, defaultBaseURL, conf.BaseURL)
	assert.Equal(t, defaultPollInterval, conf.PollInterval)
	assert.Equal(t, defaultInitialLookback, conf.InitialLookback)
	assert.Equal(t, defaultLimit, conf.Limit)
	assert.Equal(t, defaultMaxPages, conf.MaxPages)
	assert.Equal(t, defaultDedupeTTL, conf.DedupeTTL)
	assert.Equal(t, defaultRetryBaseDelay, conf.RetryBaseDelay)
	assert.Equal(t, defaultMaxRetryDelay, conf.MaxRetryDelay)
	assert.Equal(t, defaultMaxRetryAttempts, conf.MaxRetryAttempts)
}

func TestValidateClampsLimitToMax(t *testing.T) {
	conf := validConfig(t)
	conf.Limit = 5000
	require.NoError(t, conf.Validate())
	assert.Equal(t, maxLimit, conf.Limit)
}

func TestValidateTrimsBaseURL(t *testing.T) {
	conf := validConfig(t)
	conf.BaseURL = "https://example.test/client/v4/"
	require.NoError(t, conf.Validate())
	assert.Equal(t, "https://example.test/client/v4", conf.BaseURL)
}

func TestParseTimestampAcceptsDocumentedFormat(t *testing.T) {
	ts, ok := parseTimestamp("2026-07-02T15:03:00Z")
	require.True(t, ok)
	assert.Equal(t, 2026, ts.Year())
	assert.Equal(t, time.July, ts.Month())
}

func TestParseTimestampRejectsGarbage(t *testing.T) {
	_, ok := parseTimestamp("not-a-timestamp")
	assert.False(t, ok)
}

func TestDedupeKeyPrefersRayID(t *testing.T) {
	item := map[string]interface{}{"ray_id": "a1cbcd95da9aa9e9", "user_email": "user@example.com"}
	assert.Equal(t, "a1cbcd95da9aa9e9", dedupeKey(item))
}

func TestDedupeKeyFallsBackToContentHash(t *testing.T) {
	item := map[string]interface{}{"user_email": "user@example.com"}
	key := dedupeKey(item)
	assert.NotEmpty(t, key)
	assert.NotEqual(t, "a1cbcd95da9aa9e9", key)
}
