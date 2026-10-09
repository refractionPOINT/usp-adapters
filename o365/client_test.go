package usp_o365

import (
	"testing"

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

// validConfig returns a config that passes Validate; individual tests blank out
// fields to assert each one is required.
func validConfig(t *testing.T) Office365Config {
	t.Helper()
	return Office365Config{
		ClientOptions: testClientOptions(t),
		Domain:        testDomain,
		TenantID:      testTenantID,
		PublisherID:   testPublisherID,
		ClientID:      testClientID,
		ClientSecret:  testClientSecret,
		Endpoint:      "enterprise",
	}
}

func TestValidate(t *testing.T) {
	t.Run("valid named endpoint passes", func(t *testing.T) {
		c := validConfig(t)
		assert.NoError(t, c.Validate())
	})

	t.Run("every named endpoint passes", func(t *testing.T) {
		for name := range URL {
			c := validConfig(t)
			c.Endpoint = name
			assert.NoError(t, c.Validate(), "endpoint %q should be accepted", name)
		}
	})

	t.Run("custom https endpoint passes", func(t *testing.T) {
		c := validConfig(t)
		c.Endpoint = "https://manage.contoso.example.com/api/v1.0/"
		assert.NoError(t, c.Validate())
	})

	t.Run("invalid endpoint rejected", func(t *testing.T) {
		c := validConfig(t)
		c.Endpoint = "not-a-real-endpoint"
		assert.Error(t, c.Validate())
	})

	t.Run("required fields", func(t *testing.T) {
		for name, blank := range map[string]func(*Office365Config){
			"domain":        func(c *Office365Config) { c.Domain = "" },
			"tenant_id":     func(c *Office365Config) { c.TenantID = "" },
			"publisher_id":  func(c *Office365Config) { c.PublisherID = "" },
			"client_id":     func(c *Office365Config) { c.ClientID = "" },
			"client_secret": func(c *Office365Config) { c.ClientSecret = "" },
			"endpoint":      func(c *Office365Config) { c.Endpoint = "" },
		} {
			t.Run(name, func(t *testing.T) {
				c := validConfig(t)
				blank(&c)
				assert.Error(t, c.Validate(), "missing %s must fail validation", name)
			})
		}
	})

	t.Run("invalid client_options rejected", func(t *testing.T) {
		c := validConfig(t)
		c.ClientOptions = uspclient.ClientOptions{}
		assert.Error(t, c.Validate())
	})
}

// TestNamedEndpointTables verifies every named endpoint has a matching token
// URL and resource scope -- the constructor and updateBearerToken index these
// maps by the same key.
func TestNamedEndpointTables(t *testing.T) {
	for name := range URL {
		_, hasToken := TokenURL[name]
		require.True(t, hasToken, "endpoint %q has no token URL", name)
		_, hasScope := ResourceScope[name]
		require.True(t, hasScope, "endpoint %q has no resource scope", name)
	}
}
