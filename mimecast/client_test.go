package usp_mimecast

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
	t.Run("requires client_id", func(t *testing.T) {
		c := MimecastConfig{ClientOptions: testClientOptions(t), ClientSecret: "s"}
		assert.Error(t, c.Validate())
	})

	t.Run("requires client_secret", func(t *testing.T) {
		c := MimecastConfig{ClientOptions: testClientOptions(t), ClientId: "i"}
		assert.Error(t, c.Validate())
	})

	t.Run("accepts a complete config", func(t *testing.T) {
		c := MimecastConfig{ClientOptions: testClientOptions(t), ClientId: "i", ClientSecret: "s"}
		assert.NoError(t, c.Validate())
	})
}

func TestConstructorDefaultsAndOverrides(t *testing.T) {
	t.Run("defaults to the global API root and a 30s poll", func(t *testing.T) {
		conf := MimecastConfig{
			ClientOptions: testClientOptions(t),
			ClientId:      "i",
			ClientSecret:  "s",
		}
		a, chStopped, err := newMimecastAdapter(context.Background(), conf, &captureSink{})
		require.NoError(t, err)
		require.NotNil(t, chStopped)
		defer a.Close()

		assert.Equal(t, "https://api.services.mimecast.com", a.baseURL)
		assert.Equal(t, defaultPollInterval, a.pollInterval)
	})

	t.Run("honors base_url and poll interval overrides", func(t *testing.T) {
		conf := MimecastConfig{
			ClientOptions: testClientOptions(t),
			ClientId:      "i",
			ClientSecret:  "s",
			BaseURL:       "https://mimecast.example.com/",
			PollInterval:  5 * time.Second,
		}
		a, _, err := newMimecastAdapter(context.Background(), conf, &captureSink{})
		require.NoError(t, err)
		defer a.Close()

		assert.Equal(t, "https://mimecast.example.com", a.baseURL,
			"a trailing slash must be trimmed so path joins stay valid")
		assert.Equal(t, 5*time.Second, a.pollInterval)
	})
}
