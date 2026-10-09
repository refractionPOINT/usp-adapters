package usp_box

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

func TestBoxConfigValidate(t *testing.T) {
	t.Run("complete config validates", func(t *testing.T) {
		c := BoxConfig{
			ClientOptions: testClientOptions(t),
			ClientID:      "test-client-id",
			ClientSecret:  "test-client-secret",
			SubjectID:     "1111111111",
		}
		require.NoError(t, c.Validate())
	})

	t.Run("missing client_id errors", func(t *testing.T) {
		c := BoxConfig{
			ClientOptions: testClientOptions(t),
			ClientSecret:  "test-client-secret",
			SubjectID:     "1111111111",
		}
		assert.Error(t, c.Validate())
	})

	t.Run("missing client_secret errors", func(t *testing.T) {
		c := BoxConfig{
			ClientOptions: testClientOptions(t),
			ClientID:      "test-client-id",
			SubjectID:     "1111111111",
		}
		assert.Error(t, c.Validate())
	})

	t.Run("missing subject_id errors", func(t *testing.T) {
		c := BoxConfig{
			ClientOptions: testClientOptions(t),
			ClientID:      "test-client-id",
			ClientSecret:  "test-client-secret",
		}
		assert.Error(t, c.Validate())
	})

	t.Run("url overrides are optional", func(t *testing.T) {
		c := BoxConfig{
			ClientOptions: testClientOptions(t),
			ClientID:      "test-client-id",
			ClientSecret:  "test-client-secret",
			SubjectID:     "1111111111",
			EventsURL:     "http://127.0.0.1:1/2.0/events",
			TokenURL:      "http://127.0.0.1:1/oauth2/token",
		}
		require.NoError(t, c.Validate())
	})
}
