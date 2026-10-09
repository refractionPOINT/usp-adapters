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

	t.Run("accepts known streams", func(t *testing.T) {
		c := valid(t)
		c.Streams = "risk_detections, sign_ins,audit_logs"
		require.NoError(t, c.Validate())
	})

	t.Run("rejects unknown streams", func(t *testing.T) {
		c := valid(t)
		c.Streams = "sign_ins,signin_logs"
		assert.Error(t, c.Validate())
	})

	t.Run("accepts known endpoints", func(t *testing.T) {
		for _, name := range []string{"", "enterprise", "gcc-gov", "gcc-high-gov", "dod-gov"} {
			c := valid(t)
			c.Endpoint = name
			assert.NoErrorf(t, c.Validate(), "endpoint %q", name)
		}
	})

	t.Run("rejects unknown endpoint", func(t *testing.T) {
		c := valid(t)
		c.Endpoint = "gcch"
		assert.Error(t, c.Validate())
	})
}

func TestStreamSelection(t *testing.T) {
	t.Run("defaults to risk detections only", func(t *testing.T) {
		c := EntraIDConfig{}
		streams, err := c.streams()
		require.NoError(t, err)
		require.Len(t, streams, 1)
		assert.Equal(t, "risk_detections", streams[0].name)
		assert.Equal(t, "/v1.0/identityProtection/riskDetections", streams[0].path)
		assert.Equal(t, "activityDateTime", streams[0].tsField)
	})

	t.Run("resolves paths and timestamp fields", func(t *testing.T) {
		c := EntraIDConfig{Streams: "sign_ins, audit_logs"}
		streams, err := c.streams()
		require.NoError(t, err)
		require.Len(t, streams, 2)
		assert.Equal(t, "/v1.0/auditLogs/signIns", streams[0].path)
		assert.Equal(t, "createdDateTime", streams[0].tsField)
		assert.Equal(t, "/v1.0/auditLogs/directoryAudits", streams[1].path)
		assert.Equal(t, "activityDateTime", streams[1].tsField)
	})

	t.Run("collapses duplicates and normalizes case", func(t *testing.T) {
		c := EntraIDConfig{Streams: "Sign_Ins,sign_ins"}
		streams, err := c.streams()
		require.NoError(t, err)
		require.Len(t, streams, 1)
		assert.Equal(t, "sign_ins", streams[0].name)
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

	t.Run("national clouds resolve hosts and scope together", func(t *testing.T) {
		// A token acquired for one deployment is not valid against another, so
		// the scope must track the Graph service root of the same environment.
		for _, tc := range []struct {
			endpoint string
			login    string
			graph    string
			scope    string
		}{
			{"", "https://login.microsoftonline.com", "https://graph.microsoft.com", "https://graph.microsoft.com/.default"},
			{"enterprise", "https://login.microsoftonline.com", "https://graph.microsoft.com", "https://graph.microsoft.com/.default"},
			{"gcc-gov", "https://login.microsoftonline.com", "https://graph.microsoft.com", "https://graph.microsoft.com/.default"},
			{"gcc-high-gov", "https://login.microsoftonline.us", "https://graph.microsoft.us", "https://graph.microsoft.us/.default"},
			{"dod-gov", "https://login.microsoftonline.us", "https://dod-graph.microsoft.us", "https://dod-graph.microsoft.us/.default"},
		} {
			c := EntraIDConfig{TenantID: testTenantID, Endpoint: tc.endpoint}
			assert.Equalf(t, tc.login+"/"+testTenantID+"/oauth2/v2.0/token", c.tokenURL(), "endpoint %q", tc.endpoint)
			assert.Equalf(t, tc.graph+"/v1.0/identityProtection/riskDetections", c.riskDetectionsURL(), "endpoint %q", tc.endpoint)
			assert.Equalf(t, tc.scope, c.scope(), "endpoint %q", tc.endpoint)
		}
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
