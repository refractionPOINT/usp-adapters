package usp_cato

import (
	"encoding/json"
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

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func TestCatoConfigValidate(t *testing.T) {
	t.Run("requires apikey", func(t *testing.T) {
		c := CatoConfig{ClientOptions: testClientOptions(t), AccountId: testAccountID}
		assert.Error(t, c.Validate())
	})

	t.Run("requires accountid", func(t *testing.T) {
		c := CatoConfig{ClientOptions: testClientOptions(t), ApiKey: testAPIKey}
		assert.Error(t, c.Validate())
	})

	t.Run("accepts a complete config, url optional", func(t *testing.T) {
		c := CatoConfig{ClientOptions: testClientOptions(t), ApiKey: testAPIKey, AccountId: testAccountID}
		assert.NoError(t, c.Validate())
		c.Url = "https://cato-mock.example.com/api/v1/graphql2"
		assert.NoError(t, c.Validate())
	})
}

// TestGraphqlURL guards the production endpoint and the config override.
func TestGraphqlURL(t *testing.T) {
	a := &CatoAdapter{}
	assert.Equal(t, "https://api.catonetworks.com/api/v1/graphql2", a.graphqlURL(),
		"an empty config must target the production Cato endpoint")

	a.conf.Url = "https://cato-mock.example.com/api/v1/graphql2"
	assert.Equal(t, "https://cato-mock.example.com/api/v1/graphql2", a.graphqlURL())
}
