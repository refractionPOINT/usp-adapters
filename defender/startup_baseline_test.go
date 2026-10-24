package usp_defender

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This constructor/request oracle also runs unchanged on the ordinary master
// collector. It compares real first vendor requests, without a stored baseline.
func TestOrdinaryFirstRequestRetainsStartupFloor(t *testing.T) {
	// Compare the ordinary startup floor independently of the separately tested
	// local-time-with-literal-Z defect in the original Defender formatter.
	previousLocal := time.Local
	time.Local = time.UTC
	t.Cleanup(func() { time.Local = previousLocal })
	requests := make(chan string, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			fmt.Fprint(w, `{"access_token":"fixture"}`)
			return
		}
		raw := r.URL.Query().Get("$filter")
		if r.URL.Path == "/api/v1/logs" {
			raw = r.URL.Query().Get("since")
			w.Header().Set("Link", fmt.Sprintf(`<http://%s%s>; rel="next"`, r.Host, r.URL.RequestURI()))
			fmt.Fprint(w, `[]`)
		} else {
			fmt.Fprint(w, `{"value":[]}`)
		}
		select {
		case requests <- raw:
		default:
		}
	}))
	defer server.Close()
	started := time.Now().UTC()
	a, _, err := newDefenderAdapter(context.Background(), DefenderConfig{ClientOptions: testClientOptions(t), TenantID: mockTenantID, ClientID: mockClientID, ClientSecret: mockClientSecret, TokenURL: server.URL + tokenPath, AlertsURL: server.URL + alertsPath, PollInterval: time.Millisecond}, &captureSink{})
	require.NoError(t, err)
	defer a.Close()
	select {
	case filter := <-requests:
		parts := strings.Fields(filter)
		value := filter
		if len(parts) >= 3 {
			value = parts[2]
		}
		boundary, err := time.Parse(time.RFC3339Nano, value)
		require.NoError(t, err)
		require.False(t, boundary.Before(started.Add(-time.Second)), "ordinary first request must not expand startup history")
		require.False(t, boundary.After(started.Add(time.Second)), "first baseline must stay at startup, not move to a later poll")
		t.Logf("first request baseline delta from constructor: %s", boundary.Sub(started))
	case <-time.After(35 * time.Second):
		t.Fatal("ordinary adapter never requested its first source window")
	}
}
