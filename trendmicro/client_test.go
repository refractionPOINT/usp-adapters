package usp_trendmicro

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
	t.Run("requires api_token", func(t *testing.T) {
		c := TrendMicroConfig{ClientOptions: testClientOptions(t)}
		assert.Error(t, c.Validate())
	})

	t.Run("rejects an unknown region", func(t *testing.T) {
		c := TrendMicroConfig{ClientOptions: testClientOptions(t), APIToken: "k", Region: "mars"}
		assert.Error(t, c.Validate())
	})

	t.Run("applies defaults", func(t *testing.T) {
		c := TrendMicroConfig{ClientOptions: testClientOptions(t), APIToken: "k"}
		require.NoError(t, c.Validate())
		assert.Equal(t, "us", c.Region, "region must default to us")
		assert.Equal(t, defaultPollInterval, c.PollInterval,
			"poll interval must default to the historical 60s")
	})

	t.Run("keeps explicit values", func(t *testing.T) {
		c := TrendMicroConfig{
			ClientOptions: testClientOptions(t),
			APIToken:      "k",
			Region:        "eu",
			PollInterval:  5 * time.Second,
		}
		require.NoError(t, c.Validate())
		assert.Equal(t, "eu", c.Region)
		assert.Equal(t, 5*time.Second, c.PollInterval)
	})

	t.Run("accepts a url override", func(t *testing.T) {
		c := TrendMicroConfig{
			ClientOptions: testClientOptions(t),
			APIToken:      "k",
			URL:           "https://visionone.example.com",
		}
		require.NoError(t, c.Validate())
		assert.Equal(t, "https://visionone.example.com", c.URL)
		assert.Equal(t, "us", c.Region, "region still defaults even when url is overridden")
	})

	t.Run("every documented region maps to a Trend Micro domain", func(t *testing.T) {
		for _, region := range []string{"us", "eu", "sg", "jp", "in", "au"} {
			c := TrendMicroConfig{ClientOptions: testClientOptions(t), APIToken: "k", Region: region}
			require.NoError(t, c.Validate(), "region %s", region)
			assert.NotEmpty(t, regionalDomains[region], "region %s", region)
		}
	})
}
