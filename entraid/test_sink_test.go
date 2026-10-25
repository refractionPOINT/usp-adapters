package usp_entraid

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Use the public constructor, real pinned SDK and ordinary source timing. The
// loopback server substitutes only the vendor, including opaque pagination.
// TestSinkMode discards records: this establishes compatibility, not real ACKs.
func TestRealSDKTestSinkKeepsPolling(t *testing.T) {
	var calls, errors, trackingFailures atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		start, end := bounds(t, r)
		next := ""
		if n <= 3 {
			u := *r.URL
			u.Scheme = "http"
			u.Host = r.Host
			q := u.Query()
			q.Set("$skiptoken", fmt.Sprint(n))
			u.RawQuery = q.Encode()
			next = u.String()
		}
		items := []map[string]interface{}{}
		if n <= 3 {
			for i := range 3 {
				items = append(items, item(fmt.Sprintf("%d-%d", n, i), start.Add(end.Sub(start)/2)))
			}
		}
		sendPage(w, items, next)
	})
	opts := testClientOptions(t)
	opts.TestSinkMode = true
	opts.OnError = func(error) { errors.Add(1) }
	opts.DebugLog = func(s string) {
		if strings.Contains(s, "admission_tracking_failed") || strings.Contains(s, "collection_stalled") {
			trackingFailures.Add(1)
		}
	}
	a, stopped, err := NewEntraIDAdapter(context.Background(), EntraIDConfig{ClientOptions: opts, TenantID: testTenantID, ClientID: testClientID, ClientSecret: "fixture&secret=encoded", GraphEndpoint: server.URL, LoginEndpoint: server.URL, PollInterval: time.Millisecond})
	require.NoError(t, err)
	defer a.Close()
	require.Eventually(t, func() bool {
		select {
		case <-stopped:
			return true
		default:
		}
		return calls.Load() >= 4
	}, 50*time.Second, time.Millisecond)
	select {
	case <-stopped:
		t.Fatalf("real SDK TestSinkMode stopped after %d requests", calls.Load())
	default:
	}
	require.GreaterOrEqual(t, calls.Load(), int32(4), "three distinct three-item pages must drain and polling must continue")
	require.NoError(t, a.Close())
	require.Len(t, a.generation.live, 9, "all three distinct pages must be admitted")
	require.Zero(t, errors.Load(), "testing sink compatibility must not emit OnError")
	require.Zero(t, trackingFailures.Load(), "second admission must not break the observer")
}
