package usp_entraid

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	valid := func(t *testing.T) EntraIDConfig {
		return EntraIDConfig{
			ClientOptions: testClientOptions(t),
			TenantID:      testTenantID,
			ClientID:      testClientID,
			ClientSecret:  testClientSecret,
		}
	}

	t.Run("valid config passes", func(t *testing.T) {
		c := valid(t)
		require.NoError(t, c.Validate())
	})

	t.Run("requires tenant_id", func(t *testing.T) {
		c := valid(t)
		c.TenantID = ""
		assert.Error(t, c.Validate())
	})

	t.Run("requires client_id", func(t *testing.T) {
		c := valid(t)
		c.ClientID = ""
		assert.Error(t, c.Validate())
	})

	t.Run("requires client_secret", func(t *testing.T) {
		c := valid(t)
		c.ClientSecret = ""
		assert.Error(t, c.Validate())
	})
}

func TestEndpointResolution(t *testing.T) {
	t.Run("defaults match the public Microsoft endpoints", func(t *testing.T) {
		c := EntraIDConfig{TenantID: testTenantID}
		assert.Equal(t,
			"https://login.microsoftonline.com/"+testTenantID+"/oauth2/v2.0/token",
			c.tokenURL())
		assert.Equal(t,
			"https://graph.microsoft.com/v1.0/identityProtection/riskDetections",
			c.riskDetectionsURL())
		// The default Graph URL must stay in sync with the historical
		// hardcoded value.
		assert.Equal(t, URL["get_alerts"], c.riskDetectionsURL())
	})

	t.Run("overrides are honored and trailing slashes trimmed", func(t *testing.T) {
		c := EntraIDConfig{
			TenantID:      testTenantID,
			LoginEndpoint: "http://127.0.0.1:8080/",
			GraphEndpoint: "http://127.0.0.1:9090/",
		}
		assert.Equal(t,
			"http://127.0.0.1:8080/"+testTenantID+"/oauth2/v2.0/token",
			c.tokenURL())
		assert.Equal(t,
			"http://127.0.0.1:9090/v1.0/identityProtection/riskDetections",
			c.riskDetectionsURL())
	})
}
