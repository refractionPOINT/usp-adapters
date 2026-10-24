package usp_okta

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"github.com/stretchr/testify/require"
)

type ackSink struct {
	mu      sync.Mutex
	next    uint64
	pending []*protocol.DataMessage
	total   int
	fail    error
	readErr error
}

func (s *ackSink) Ship(m *protocol.DataMessage, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	s.next++
	m.SeqNum = s.next
	s.pending = append(s.pending, m)
	s.total++
	return nil
}
func (s *ackSink) GetUnsent() ([]*protocol.DataMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*protocol.DataMessage(nil), s.pending...), s.readErr
}
func (s *ackSink) Drain(time.Duration) error               { return nil } // Deliberately does not ACK.
func (s *ackSink) Close() ([]*protocol.DataMessage, error) { return s.GetUnsent() }
func (s *ackSink) count() int                              { s.mu.Lock(); defer s.mu.Unlock(); return s.total }
func (s *ackSink) ack(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.pending) > 0 && s.pending[0].SeqNum <= seq {
		s.pending = s.pending[1:]
	}
}

func localHarness(t *testing.T, serverURL string, sink uspSink) (*OktaAdapter, *oktaGeneration) {
	t.Helper()
	a := directAdapter(t, serverURL)
	a.uspClient = sink
	a.pollInterval = time.Millisecond
	tracker, err := acktrack.NewACKTracker(sink, 1, 16)
	require.NoError(t, err)
	a.tracker = tracker
	return a, newOktaGeneration(1, a.initialPosition())
}

func runCollector(t *testing.T, a *OktaAdapter, g *oktaGeneration) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.collect(ctx, g) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("collector did not join")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestProgressPendingDuplicateAndEmptyPageDependOnACK(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s?after=%d>; rel=\"next\"", r.Host, logsURL, n))
		if n <= 2 {
			_, _ = w.Write([]byte(`[{"uuid":"same"}]`))
		} else {
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer server.Close()
	sink := &ackSink{}
	a, g := localHarness(t, server.URL, sink)
	stop := runCollector(t, a, g)
	require.Eventually(t, func() bool { return calls.Load() >= 4 }, time.Second, time.Millisecond)
	stop()
	require.Equal(t, 1, sink.count(), "pending duplicate must not be shipped twice")
	before := g.confirmed.Load()
	require.NoError(t, a.confirm(g))
	require.Equal(t, before, g.confirmed.Load())
	// A successful Drain must not certify a pending event or subsequent empties.
	require.NoError(t, sink.Drain(time.Second))
	require.NoError(t, a.confirm(g))
	require.Equal(t, before, g.confirmed.Load())
	sink.ack(1)
	require.NoError(t, a.confirm(g))
	require.Equal(t, g.page, g.confirmed.Load())
	require.Len(t, g.live, 1)
}

func TestProgressContinuousUnackedTailKeepsFrozenCompleteProbe(t *testing.T) {
	sink := &ackSink{}
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	a, g := localHarness(t, server.URL, sink)
	for i := uint64(1); i <= 1000; i++ {
		dep, err := a.tracker.Ship(&protocol.DataMessage{TextPayload: "tail"}, time.Second)
		require.NoError(t, err)
		require.NoError(t, a.tracker.Candidate("logs", candidateBytes(g.id, i), dep))
	}
	sink.ack(1)
	require.NoError(t, a.confirm(g))
	require.Equal(t, uint64(1), g.confirmed.Load())
	sink.ack(1000)
	require.NoError(t, a.confirm(g))
	require.Equal(t, uint64(1000), g.confirmed.Load())
}

func TestProgressOversizePageNeverLosesItsAdmissionDependency(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		after := "complete"
		if n == 1 {
			after = strings.Repeat("z", 9*1024)
		}
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s?after=%s>; rel=\"next\"", r.Host, logsURL, after))
		if n == 1 {
			_, _ = w.Write([]byte(`[{"uuid":"pending-oversize-page"}]`))
		} else {
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer server.Close()
	sink := &ackSink{}
	a, g := localHarness(t, server.URL, sink)
	stop := runCollector(t, a, g)
	require.Eventually(t, func() bool { return calls.Load() >= 3 }, time.Second, time.Millisecond)
	stop()
	before := g.confirmed.Load()
	require.NoError(t, a.confirm(g))
	require.Equal(t, before, g.confirmed.Load(), "a later small empty page must retain the oversize page's dependency")
	sink.ack(1)
	require.NoError(t, a.confirm(g))
	require.Equal(t, g.page, g.confirmed.Load())
	require.Contains(t, g.position.Next, "after=complete")
}

func TestProgressFailedAdmissionCannotAdvancePage(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s?after=never>; rel=\"next\"", r.Host, logsURL))
		_, _ = w.Write([]byte(`[{"uuid":"failed"}]`))
	}))
	defer server.Close()
	sink := &ackSink{fail: errors.New("admission failed")}
	a, g := localHarness(t, server.URL, sink)
	before := g.confirmed.Load()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a.collect(ctx, g)
	require.Equal(t, int32(1), calls.Load())
	require.Empty(t, g.live)
	require.NoError(t, a.confirm(g))
	require.Equal(t, before, g.confirmed.Load())
}

func TestProgressExpiredContinuationStallsWithoutReplayingUncoveredHistory(t *testing.T) {
	var initial, retries, vendorErrors atomic.Int32
	var mu sync.Mutex
	var urls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after") == "expired /+" {
			retries.Add(1)
			mu.Lock()
			urls = append(urls, r.URL.RequestURI())
			mu.Unlock()
			w.WriteHeader(400)
			fmt.Fprint(w, `{"errorCode":"E0000001"}`)
			return
		}
		initial.Add(1)
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=expired+%%2f%%2b&x=1>; rel="next"`, r.Host, logsURL))
		fmt.Fprint(w, `[{"uuid":"already-admitted"}]`)
	}))
	defer server.Close()
	sink := &captureSink{}
	opts := testClientOptions(t)
	opts.OnError = func(error) { vendorErrors.Add(1) }
	a, stopped, err := newOktaAdapter(context.Background(), OktaConfig{URL: server.URL, ApiKey: "dummy", ClientOptions: opts, PollInterval: time.Millisecond}, sink)
	require.NoError(t, err)
	defer a.Close()
	require.Eventually(t, func() bool { return retries.Load() >= 2 }, 3*time.Second, time.Millisecond)
	started := time.Now()
	require.NoError(t, a.Close())
	require.Less(t, time.Since(started), time.Second)
	<-stopped
	require.Equal(t, int32(1), initial.Load(), "expired continuation must never rewind to the first request")
	mu.Lock()
	defer mu.Unlock()
	for _, u := range urls {
		require.Equal(t, logsURL+"?after=expired+%2f%2b&x=1", u)
	}
	require.Equal(t, 1, sink.count())
	require.Zero(t, vendorErrors.Load())
}

type blockedShutdownSink struct {
	ackSink
	entered chan struct{}
	release chan struct{}
	begin   sync.Once
}

func (s *blockedShutdownSink) Ship(m *protocol.DataMessage, timeout time.Duration) error {
	if s.count() == 0 {
		return s.ackSink.Ship(m, timeout)
	}
	close(s.entered)
	<-s.release
	return uspclient.ErrorClosed
}
func (s *blockedShutdownSink) BeginDrain()               { s.begin.Do(func() { close(s.release) }) }
func (s *blockedShutdownSink) Drain(time.Duration) error { s.ack(^uint64(0)); return nil }
func (s *blockedShutdownSink) Close() ([]*protocol.DataMessage, error) {
	return s.GetUnsent()
}

func TestProgressShutdownWakesBlockedProducerBeforeJoin(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s?after=partial-page>; rel=\"next\"", r.Host, logsURL))
		fmt.Fprint(w, `[{"uuid":"accepted"},{"uuid":"blocked"}]`)
	}))
	defer server.Close()
	sink := &blockedShutdownSink{entered: make(chan struct{}), release: make(chan struct{})}
	a, _, err := newOktaAdapter(context.Background(), OktaConfig{URL: server.URL, ApiKey: "dummy", ClientOptions: testClientOptions(t), PollInterval: time.Millisecond}, sink)
	require.NoError(t, err)
	defer a.Close()
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("producer did not block")
	}
	initial := uint64(0)
	closed := make(chan error, 1)
	go func() { closed <- a.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		sink.BeginDrain()
		<-closed
		t.Fatal("shutdown did not wake producer before joining")
	}
	require.Equal(t, initial, a.generation.confirmed.Load(), "an interrupted page cannot advance, even after accepted data ACKs")
}
