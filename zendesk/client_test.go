package usp_zendesk

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
	valid := func() ZendeskConfig {
		return ZendeskConfig{
			ClientOptions: testClientOptions(t),
			ApiToken:      "test-api-token-0000000000",
			ZendeskDomain: "example.zendesk.com",
			ZendeskEmail:  "jdoe@example.com",
		}
	}

	t.Run("valid config passes", func(t *testing.T) {
		c := valid()
		assert.NoError(t, c.Validate())
	})

	t.Run("requires api_token", func(t *testing.T) {
		c := valid()
		c.ApiToken = ""
		assert.Error(t, c.Validate())
	})

	t.Run("requires zendesk_domain", func(t *testing.T) {
		c := valid()
		c.ZendeskDomain = ""
		assert.Error(t, c.Validate())
	})

	t.Run("requires zendesk_email", func(t *testing.T) {
		c := valid()
		c.ZendeskEmail = ""
		assert.Error(t, c.Validate())
	})
}

// TestBaseURLAndPollIntervalDefaults verifies the adapter derives its base URL
// from the configured domain when base_url is unset, honors an explicit
// base_url override (trailing slash trimmed), and applies the default poll
// interval unless the test seam overrides it.
func TestBaseURLAndPollIntervalDefaults(t *testing.T) {
	ctx := context.Background()

	t.Run("defaults from domain", func(t *testing.T) {
		conf := ZendeskConfig{
			ClientOptions: testClientOptions(t),
			ApiToken:      "tok",
			ZendeskDomain: "example.zendesk.com",
			ZendeskEmail:  "jdoe@example.com",
		}
		a, _, err := newZendeskAdapter(ctx, conf, &captureSink{})
		require.NoError(t, err)
		defer a.Close()
		assert.Equal(t, "https://example.zendesk.com", a.baseURL)
		assert.Equal(t, defaultPollInterval, a.pollInterval)
	})

	t.Run("base_url override wins and is trimmed", func(t *testing.T) {
		conf := ZendeskConfig{
			ClientOptions: testClientOptions(t),
			ApiToken:      "tok",
			ZendeskDomain: "example.zendesk.com",
			ZendeskEmail:  "jdoe@example.com",
			BaseURL:       "http://127.0.0.1:1/",
			PollInterval:  time.Hour,
		}
		a, _, err := newZendeskAdapter(ctx, conf, &captureSink{})
		require.NoError(t, err)
		defer a.Close()
		assert.Equal(t, "http://127.0.0.1:1", a.baseURL)
		assert.Equal(t, time.Hour, a.pollInterval)
	})
}
