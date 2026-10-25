package usp_okta

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIndependentReviewRealSDKTestSinkKeepsPolling(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=next>; rel="next"`, r.Host, logsURL))
		fmt.Fprintf(w, `[{"uuid":"one","published":%q},{"uuid":"two","published":%q},{"uuid":"three","published":%q}]`, time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
	}))
	defer server.Close()
	opts := testClientOptions(t)
	opts.TestSinkMode = true
	a, stopped, err := NewOktaAdapter(context.Background(), OktaConfig{ClientOptions: opts, ApiKey: "fixture", URL: server.URL, PollInterval: time.Millisecond})
	require.NoError(t, err)
	defer a.Close()
	require.Eventually(t, func() bool {
		select {
		case <-stopped:
			return true
		default:
		}
		return calls.Load() >= 3
	}, 5*time.Second, time.Millisecond)
	select {
	case <-stopped:
		t.Fatalf("supported real SDK TestSinkMode stopped collector after %d vendor requests; first three-item page should drain and polling should continue", calls.Load())
	default:
	}
	require.GreaterOrEqual(t, calls.Load(), int32(3))
}
