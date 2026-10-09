package usp_defender

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

func TestValidate(t *testing.T) {
	valid := func() DefenderConfig {
		return DefenderConfig{
			ClientOptions: testClientOptions(t),
			TenantID:      "11111111-1111-1111-1111-111111111111",
			ClientID:      "22222222-2222-2222-2222-222222222222",
			ClientSecret:  "fake-secret",
		}
	}

	t.Run("valid config passes", func(t *testing.T) {
		c := valid()
		assert.NoError(t, c.Validate())
	})

	t.Run("requires tenant_id", func(t *testing.T) {
		c := valid()
		c.TenantID = ""
		assert.Error(t, c.Validate())
	})

	t.Run("requires client_id", func(t *testing.T) {
		c := valid()
		c.ClientID = ""
		assert.Error(t, c.Validate())
	})

	t.Run("requires client_secret", func(t *testing.T) {
		c := valid()
		c.ClientSecret = ""
		assert.Error(t, c.Validate())
	})

	t.Run("empty endpoint is valid (defaults to enterprise)", func(t *testing.T) {
		c := valid()
		c.Endpoint = ""
		assert.NoError(t, c.Validate())
	})

	t.Run("known endpoints are valid", func(t *testing.T) {
		for _, ep := range []string{"enterprise", "gcc-gov", "gcc-high-gov", "dod-gov"} {
			c := valid()
			c.Endpoint = ep
			assert.NoErrorf(t, c.Validate(), "endpoint %q should be valid", ep)
		}
	})

	t.Run("rejects an unknown endpoint", func(t *testing.T) {
		c := valid()
		c.Endpoint = "china"
		assert.Error(t, c.Validate())
	})
}

func TestEndpointDefaults(t *testing.T) {
	t.Run("token endpoint defaults to the Microsoft identity platform", func(t *testing.T) {
		c := DefenderConfig{TenantID: "11111111-1111-1111-1111-111111111111"}
		assert.Equal(t,
			"https://login.microsoftonline.com/11111111-1111-1111-1111-111111111111/oauth2/v2.0/token",
			c.tokenURL())
	})

	t.Run("token endpoint override wins", func(t *testing.T) {
		c := DefenderConfig{
			TenantID: "11111111-1111-1111-1111-111111111111",
			TokenURL: "https://mock.example.com/token",
		}
		assert.Equal(t, "https://mock.example.com/token", c.tokenURL())
	})

	t.Run("alerts endpoint defaults to MS Graph alerts_v2", func(t *testing.T) {
		c := DefenderConfig{}
		assert.Equal(t, "https://graph.microsoft.com/v1.0/security/alerts_v2", c.alertsURL())
	})

	t.Run("alerts endpoint override wins", func(t *testing.T) {
		c := DefenderConfig{AlertsURL: "https://mock.example.com/v1.0/security/alerts_v2"}
		assert.Equal(t, "https://mock.example.com/v1.0/security/alerts_v2", c.alertsURL())
	})

	t.Run("scope defaults to the commercial MS Graph root", func(t *testing.T) {
		c := DefenderConfig{}
		assert.Equal(t, "https://graph.microsoft.com/.default", c.scope())
	})

	t.Run("empty endpoint resolves to the commercial cloud", func(t *testing.T) {
		c := DefenderConfig{TenantID: "11111111-1111-1111-1111-111111111111"}
		assert.Equal(t, "https://graph.microsoft.com/v1.0/security/alerts_v2", c.alertsURL())
		assert.Equal(t,
			"https://login.microsoftonline.com/11111111-1111-1111-1111-111111111111/oauth2/v2.0/token",
			c.tokenURL())
		assert.Equal(t, "https://graph.microsoft.com/.default", c.scope())
	})
}

// TestEndpointEnvironments pins the alerts URL, token URL and OAuth2 scope
// each named Microsoft national cloud deployment resolves to. Sources are the
// MS Graph deployments doc (https://learn.microsoft.com/en-us/graph/deployments):
// gcc-gov shares the worldwide endpoints with enterprise; gcc-high-gov (L4)
// uses graph.microsoft.us; dod-gov (L5) uses dod-graph.microsoft.us; both L4
// and L5 authenticate against login.microsoftonline.us. The scope always
// matches the Graph service root.
func TestEndpointEnvironments(t *testing.T) {
	const tenant = "11111111-1111-1111-1111-111111111111"
	cases := []struct {
		endpoint  string
		alertsURL string
		tokenURL  string
		scope     string
	}{
		{
			endpoint:  "enterprise",
			alertsURL: "https://graph.microsoft.com/v1.0/security/alerts_v2",
			tokenURL:  "https://login.microsoftonline.com/" + tenant + "/oauth2/v2.0/token",
			scope:     "https://graph.microsoft.com/.default",
		},
		{
			endpoint:  "gcc-gov",
			alertsURL: "https://graph.microsoft.com/v1.0/security/alerts_v2",
			tokenURL:  "https://login.microsoftonline.com/" + tenant + "/oauth2/v2.0/token",
			scope:     "https://graph.microsoft.com/.default",
		},
		{
			endpoint:  "gcc-high-gov",
			alertsURL: "https://graph.microsoft.us/v1.0/security/alerts_v2",
			tokenURL:  "https://login.microsoftonline.us/" + tenant + "/oauth2/v2.0/token",
			scope:     "https://graph.microsoft.us/.default",
		},
		{
			endpoint:  "dod-gov",
			alertsURL: "https://dod-graph.microsoft.us/v1.0/security/alerts_v2",
			tokenURL:  "https://login.microsoftonline.us/" + tenant + "/oauth2/v2.0/token",
			scope:     "https://dod-graph.microsoft.us/.default",
		},
	}
	for _, tc := range cases {
		t.Run(tc.endpoint, func(t *testing.T) {
			c := DefenderConfig{TenantID: tenant, Endpoint: tc.endpoint}
			assert.Equal(t, tc.alertsURL, c.alertsURL(), "alerts URL")
			assert.Equal(t, tc.tokenURL, c.tokenURL(), "token URL")
			assert.Equal(t, tc.scope, c.scope(), "scope")
		})
	}
}

// TestPollIntervalDefault verifies an unset poll_interval falls back to the
// historical 30s default.
func TestPollIntervalDefault(t *testing.T) {
	sink := &captureSink{}
	conf := DefenderConfig{
		ClientOptions: testClientOptions(t),
		TenantID:      "11111111-1111-1111-1111-111111111111",
		ClientID:      "22222222-2222-2222-2222-222222222222",
		ClientSecret:  "fake-secret",
	}
	adapter, _, err := newDefenderAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	// Close() before the first 30s poll tick fires, so no network call is made.
	defer adapter.Close()
	assert.Equal(t, 30*time.Second, adapter.pollInterval)
}
