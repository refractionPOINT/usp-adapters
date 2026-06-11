package usp_wiz

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

// validTestConfig returns a config that passes Validate(). The URLs point at a
// reserved example domain; nothing in these unit tests performs network I/O.
func validTestConfig(t *testing.T) WizConfig {
	t.Helper()
	return WizConfig{
		ClientOptions: testClientOptions(t),
		ClientID:      "test-client-id",
		ClientSecret:  "test-client-secret",
		URL:           "https://api.wiz.example.com/graphql",
		Query:         testIssuesQuery,
		TimeField:     "createdAt",
		DataPath:      []string{"data", "issues", "nodes"},
		IDField:       "id",
	}
}

func TestValidate(t *testing.T) {
	t.Run("valid config passes", func(t *testing.T) {
		c := validTestConfig(t)
		assert.NoError(t, c.Validate())
	})

	mutations := map[string]func(*WizConfig){
		"missing client_id":     func(c *WizConfig) { c.ClientID = "" },
		"missing client_secret": func(c *WizConfig) { c.ClientSecret = "" },
		"missing url":           func(c *WizConfig) { c.URL = "" },
		"missing query":         func(c *WizConfig) { c.Query = "" },
		"missing time_field":    func(c *WizConfig) { c.TimeField = "" },
		"missing data_path":     func(c *WizConfig) { c.DataPath = nil },
		"missing id_field":      func(c *WizConfig) { c.IDField = "" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			c := validTestConfig(t)
			mutate(&c)
			assert.Error(t, c.Validate())
		})
	}
}

// TestConstructorDefaults verifies the constructor fills the token URL and
// poll interval defaults (production values) when the config leaves them
// empty, and that explicit values are respected.
func TestConstructorDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("defaults applied", func(t *testing.T) {
		sink := &captureSink{}
		// The default 30s poll interval means no request is made before Close.
		adapter, _, err := newWizAdapter(ctx, validTestConfig(t), sink)
		require.NoError(t, err)
		defer adapter.Close()

		assert.Equal(t, "https://auth.app.wiz.io/oauth/token", adapter.conf.TokenURL,
			"an empty token_url must default to the Wiz commercial cloud endpoint")
		assert.Equal(t, 30*time.Second, adapter.conf.PollInterval,
			"the default poll interval must stay the historical 30 seconds")
	})

	t.Run("overrides respected", func(t *testing.T) {
		sink := &captureSink{}
		conf := validTestConfig(t)
		conf.TokenURL = "https://auth.gov.wiz.example.com/oauth/token"
		conf.PollInterval = 42 * time.Minute
		adapter, _, err := newWizAdapter(ctx, conf, sink)
		require.NoError(t, err)
		defer adapter.Close()

		assert.Equal(t, conf.TokenURL, adapter.conf.TokenURL)
		assert.Equal(t, 42*time.Minute, adapter.conf.PollInterval)
	})
}
