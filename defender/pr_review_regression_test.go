package usp_defender

import (
	"context"
	"net/http"
	"testing"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
)

type prWarningSink struct {
	captureSink
	attempts        int
	attemptTimeouts []time.Duration
}

func (s *prWarningSink) Ship(m *protocol.DataMessage, d time.Duration) error {
	s.attempts++
	s.attemptTimeouts = append(s.attemptTimeouts, d)
	if s.attempts%4 != 0 {
		return uspclient.ErrorBufferFull
	}
	return s.captureSink.Ship(m, d)
}
func TestDefenderBackpressureWarningEndsWithRecovery(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	sink := &prWarningSink{}
	a, g := harness(t, server, sink)
	warnings := 0
	a.conf.ClientOptions.OnWarning = func(string) { warnings++ }
	for i := 0; i < 2; i++ {
		stamp := time.Unix(0, g.position.Boundary).Add(time.Second)
		require.NoError(t, a.shipItem(context.Background(), g, realisticAlert(string(rune('a'+i)), stamp), g.position.Boundary, g.position.WindowEnd))
		require.Equal(t, i+1, warnings, "one warning per continuous buffer-full episode, and a new warning after recovery")
		require.Equal(t, uint64(i+1), g.admitted, "failed attempts cannot create admissions")
		require.Len(t, g.live, i+1)
	}
	require.Equal(t, 8, sink.attempts)
	for _, d := range sink.attemptTimeouts {
		require.LessOrEqual(t, d, time.Second, "each attempt remains cancellation-bounded")
	}
	require.NoError(t, a.confirm(g))
	require.Zero(t, g.confirmed.Load(), "admission-only progress is not a complete source interval")
}
func TestDefenderSourceIdentityHotPathDoesNotAllocate(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	a, _ := harness(t, server, &captureSink{})
	expected := a.sourceFingerprint()
	allocations := testing.AllocsPerRun(1000, func() {
		if a.sourceFingerprint() != expected {
			panic("source identity changed")
		}
	})
	require.Zero(t, allocations, "constant source identity must be cached off the item hot path")
}
