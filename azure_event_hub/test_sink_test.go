package usp_azure_event_hub

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/stretchr/testify/require"
)

// Only the vendor Hub is fake. This is the SDK-constructing constructor called
// by NewEventHubAdapter, with the real pinned USP SDK and existing testing mode.
func TestRealSDKTestSinkKeepsReceiving(t *testing.T) {
	var errors, trackingFailures atomic.Int32
	opts := uspclient.ClientOptions{TestSinkMode: true,
		OnError: func(error) { errors.Add(1) }, OnWarning: func(string) {},
		DebugLog: func(s string) {
			if strings.Contains(s, "internal_stall") {
				trackingFailures.Add(1)
			}
		},
	}
	hub := &testHub{ids: []string{"0"}}
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	a, stopped, err := newEventHubAdapter(context.Background(), EventHubConfig{ClientOptions: opts, ConnectionString: testConnection}, hub, source)
	require.NoError(t, err)
	defer a.Close()
	require.Eventually(t, func() bool { return len(hub.snapshot()) == 1 }, 5*time.Second, time.Millisecond)
	call := hub.snapshot()[0]
	require.Empty(t, call.start.Offset, "ordinary initial latest is unchanged")
	for i := int64(0); i < 3; i++ {
		require.NoError(t, call.handler(context.Background(), fixtureEvent(100+i, i)), "real SDK testing sink must accept the second and third record")
	}
	select {
	case <-stopped:
		t.Fatal("real SDK TestSinkMode stopped receiver")
	default:
	}
	require.False(t, a.stalled.Load())
	require.NoError(t, a.Close())
	require.Zero(t, errors.Load())
	require.Zero(t, trackingFailures.Load())
	require.NoError(t, a.confirm(a.final))
	require.NoError(t, a.confirm(a.final))
	require.Equal(t, int64(2), a.final.confirmed["0"], "immediate testing sink acceptance is local only")
}
