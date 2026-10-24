package usp_okta

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
)

func TestPageAdmissionRetainsExactLiveBound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=next>; rel="next"`, r.Host, logsURL))
		fmt.Fprint(w, `[{"uuid":"one"},{"uuid":"two"}]`)
	}))
	defer server.Close()
	sink := &ackSink{}
	a, g := localHarness(t, server.URL, sink)
	// Seed real successful, still-pending SDK admissions, never a fake saved ID set.
	for i := 0; i < liveIDLimit-1; i++ {
		id := fmt.Sprint("prior-", i)
		dep, err := a.tracker.Ship(&protocol.DataMessage{JsonPayload: map[string]interface{}{"uuid": id}}, time.Second)
		require.NoError(t, err)
		g.live[eventDigest(id)] = liveID{dependency: dep, page: 1, expiry: time.Now().Add(overlapPeriod).Unix()}
	}
	var pressure atomic.Bool
	a.conf.ClientOptions.DebugLog = func(s string) {
		if s == "okta collection: live_history_pressure" {
			pressure.Store(true)
		}
	}
	stop := runCollector(t, a, g)
	require.Eventually(t, pressure.Load, 5*time.Second, time.Millisecond)
	stop()
	require.Equal(t, liveIDLimit, sink.count(), "the second record of a page must wait at capacity")
	require.Len(t, g.live, liveIDLimit)
	require.NotContains(t, g.live, eventDigest("two"))
	require.NoError(t, a.confirm(g))
	require.Zero(t, g.confirmed.Load(), "an incomplete pressured page cannot certify source progress")
	require.NotContains(t, g.position.Next, "after=next", "retain the current opaque request until every record was admitted")
}

type invalidSequenceSink struct{ ackSink }

func (s *invalidSequenceSink) Ship(m *protocol.DataMessage, d time.Duration) error {
	err := s.ackSink.Ship(m, d)
	if s.count() == 2 {
		m.SeqNum++
	}
	return err
}
func TestInternalAdmissionTrackingFailureDoesNotReportVendorError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=next>; rel="next"`, r.Host, logsURL))
		fmt.Fprint(w, `[{"uuid":"one"},{"uuid":"two"}]`)
	}))
	defer server.Close()
	sink := &invalidSequenceSink{}
	a, g := localHarness(t, server.URL, sink)
	var failures atomic.Int32
	a.conf.ClientOptions.OnError = func(error) { failures.Add(1) }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a.collect(ctx, g)
	require.Equal(t, 2, sink.count())
	require.Zero(t, failures.Load(), "an internal ACK tracker failure is not a vendor failure")
	require.Zero(t, g.confirmed.Load())
	require.NotContains(t, g.live, eventDigest("two"))
}
