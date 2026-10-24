package usp_okta

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRealSDKTestSinkKeepsPollingWithoutInternalErrors(t *testing.T) {
	var calls, errors, trackingFailures atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=%d>; rel="next"`, r.Host, logsURL, n))
		fmt.Fprintf(w, `[{"uuid":%q,"published":%q}]`, fmt.Sprint(n), time.Now().UTC().Format(time.RFC3339Nano))
	}))
	defer server.Close()
	opts := testClientOptions(t)
	opts.TestSinkMode = true
	opts.OnError = func(error) { errors.Add(1) }
	opts.DebugLog = func(s string) {
		if strings.Contains(s, "admission_tracking_failed") {
			trackingFailures.Add(1)
		}
	}
	a, stopped, err := NewOktaAdapter(context.Background(), OktaConfig{ClientOptions: opts, ApiKey: "fixture", URL: server.URL, PollInterval: time.Millisecond})
	require.NoError(t, err)
	defer a.Close()
	require.Eventually(t, func() bool {
		select {
		case <-stopped:
			return true
		default:
		}
		return calls.Load() >= 4
	}, 5*time.Second, time.Millisecond)
	select {
	case <-stopped:
		t.Fatal("real SDK TestSinkMode stopped collector")
	default:
	}
	require.GreaterOrEqual(t, calls.Load(), int32(4), "three admissions must succeed before the next vendor request")
	require.NoError(t, a.Close())
	require.Zero(t, errors.Load())
	require.Zero(t, trackingFailures.Load())
}
