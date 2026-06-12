package usp_cylance

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLog collects adapter errors for assertions and forwards logs to the test
// log. The adapter's Close() does not wait for its polling goroutine to exit,
// so the goroutine can outlive the test function; testLog goes quiet once the
// test is done instead of calling t.Logf after the test has completed.
type testLog struct {
	t    *testing.T
	mu   sync.Mutex
	done bool
	errs []string
}

func newTestLog(t *testing.T) *testLog {
	l := &testLog{t: t}
	t.Cleanup(func() {
		l.mu.Lock()
		l.done = true
		l.mu.Unlock()
	})
	return l
}

func (l *testLog) logf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done {
		return
	}
	l.t.Logf(format, args...)
}

func (l *testLog) onError(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done {
		return
	}
	l.errs = append(l.errs, err.Error())
	l.t.Logf("ERR: %v", err)
}

func (l *testLog) hasErrorContaining(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.errs {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

// testClientOptions returns ClientOptions wired for a sink (no real
// LimaCharlie connection) with logging routed through a goroutine-safe
// testLog, which is also returned for error assertions.
func testClientOptions(t *testing.T) (uspclient.ClientOptions, *testLog) {
	t.Helper()
	l := newTestLog(t)
	return uspclient.ClientOptions{
		Identity: uspclient.Identity{
			Oid:             "11111111-1111-1111-1111-111111111111",
			InstallationKey: "test-installation-key",
		},
		Platform:     "json",
		TestSinkMode: true,
		DebugLog:     func(msg string) { l.logf("DBG: %s", msg) },
		OnWarning:    func(msg string) { l.logf("WRN: %s", msg) },
		OnError:      l.onError,
	}, l
}

// --- unit tests ---------------------------------------------------------------

func TestValidate(t *testing.T) {
	opts, _ := testClientOptions(t)

	t.Run("requires tenant id", func(t *testing.T) {
		c := CylanceConfig{ClientOptions: opts, AppID: "a", AppSecret: "s"}
		assert.ErrorContains(t, c.Validate(), "tenant")
	})

	t.Run("requires app id", func(t *testing.T) {
		c := CylanceConfig{ClientOptions: opts, TenantID: "t", AppSecret: "s"}
		assert.ErrorContains(t, c.Validate(), "app id")
	})

	t.Run("requires app secret", func(t *testing.T) {
		c := CylanceConfig{ClientOptions: opts, TenantID: "t", AppID: "a"}
		assert.ErrorContains(t, c.Validate(), "app secret")
	})

	t.Run("applies defaults", func(t *testing.T) {
		c := CylanceConfig{ClientOptions: opts, TenantID: "t", AppID: "a", AppSecret: "s"}
		require.NoError(t, c.Validate())
		assert.Equal(t, defaultLoggingBaseURL, c.LoggingBaseURL)
		assert.Equal(t, queryInterval*time.Second, c.PollInterval,
			"production poll interval must stay the historical 60 seconds")
	})

	t.Run("keeps an explicit base url override", func(t *testing.T) {
		c := CylanceConfig{
			ClientOptions: opts, TenantID: "t", AppID: "a", AppSecret: "s",
			LoggingBaseURL: "https://protect.example.com",
		}
		require.NoError(t, c.Validate())
		assert.Equal(t, "https://protect.example.com", c.LoggingBaseURL)
	})
}

func TestGetJWTExpiration(t *testing.T) {
	t.Run("reads exp from a token", func(t *testing.T) {
		exp := time.Now().Add(42 * time.Minute).Truncate(time.Second).UTC()
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"exp": exp.Unix()})
		signed, err := tok.SignedString([]byte("not-a-real-secret"))
		require.NoError(t, err)

		got, err := getJWTExpiration(signed)
		require.NoError(t, err)
		assert.Equal(t, exp, got)
	})

	t.Run("rejects a malformed token", func(t *testing.T) {
		_, err := getJWTExpiration("not.a.jwt")
		assert.Error(t, err)
	})

	t.Run("rejects a token without exp", func(t *testing.T) {
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": "x"})
		signed, err := tok.SignedString([]byte("not-a-real-secret"))
		require.NoError(t, err)

		_, err = getJWTExpiration(signed)
		assert.ErrorContains(t, err, "exp claim")
	})
}

func TestEventsResponsePagination(t *testing.T) {
	assert.True(t, CylanceEventsResponse{PageNumber: 1, TotalPages: 3}.HasNextPage())
	assert.True(t, CylanceEventsResponse{PageNumber: 2, TotalPages: 3}.HasNextPage())
	assert.False(t, CylanceEventsResponse{PageNumber: 3, TotalPages: 3}.HasNextPage())
	// An empty result set (total_pages 0 on page 1) has no next page.
	assert.False(t, CylanceEventsResponse{PageNumber: 1, TotalPages: 0}.HasNextPage())
	// Detail responses are single objects and never paginate.
	assert.False(t, CylanceEventResponse{}.HasNextPage())
}

func TestSetMeta(t *testing.T) {
	t.Run("events response tags items but keeps existing meta", func(t *testing.T) {
		r := &CylanceEventsResponse{PageItems: []utils.Dict{
			{"Id": "a"},
			{"Id": "b", "ObjectSource": "Custom", "ObjectType": "Existing"},
		}}
		r.SetMeta("Cylance", "Detection")

		assert.Equal(t, "Cylance", r.PageItems[0]["ObjectSource"])
		assert.Equal(t, "Detection", r.PageItems[0]["ObjectType"])
		assert.Equal(t, "Custom", r.PageItems[1]["ObjectSource"], "existing meta must not be overwritten")
		assert.Equal(t, "Existing", r.PageItems[1]["ObjectType"])
	})

	t.Run("event response tags the single document", func(t *testing.T) {
		r := &CylanceEventResponse{Event: []utils.Dict{{"Id": "a", "ObjectType": nil}}}
		r.SetMeta("Cylance", "Threat")
		assert.Equal(t, "Cylance", r.Event[0]["ObjectSource"])
		assert.Equal(t, "Threat", r.Event[0]["ObjectType"], "a nil ObjectType counts as absent")
	})
}
