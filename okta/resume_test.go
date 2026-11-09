package usp_okta

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"github.com/refractionPOINT/usp-adapters/resume"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

type memoryProgress struct {
	mu                 sync.Mutex
	durable, confirmed []byte
	calls              int
	fail               bool
}

func (m *memoryProgress) Restore(_ context.Context, initial []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.durable == nil {
		m.durable = append([]byte(nil), initial...)
	}
	if m.fail {
		return nil, errors.New("storage unavailable")
	}
	return append([]byte(nil), m.durable...), nil
}
func (m *memoryProgress) Confirm(p []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.confirmed = append([]byte(nil), p...)
}
func (m *memoryProgress) snapshot() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.confirmed...)
}
func (m *memoryProgress) saved() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.durable...)
}
func progressBytes(t *testing.T, q string, when time.Time) []byte {
	t.Helper()
	raw, err := json.Marshal(savedPosition{1, q, when, when})
	require.NoError(t, err)
	return raw
}
func resumeHarness(t *testing.T, endpoint string, state *memoryProgress, sink uspSink) (*OktaAdapter, *oktaGeneration) {
	t.Helper()
	a, g := localHarness(t, endpoint, sink)
	a.conf.ResumeState = state
	require.NoError(t, a.conf.Prepare(context.Background()))
	g.position = a.initialPosition()
	var err error
	a.tracker, err = acktrack.NewACKTracker(sink, 1, 16+maxResumeBytes)
	require.NoError(t, err)
	return a, g
}

func TestResumePrepareDurableFixedBaselineAndNoConstructorRead(t *testing.T) {
	state := &memoryProgress{fail: true}
	conf := OktaConfig{URL: "https://example.okta.com", ApiKey: "fixture", ResumeState: state, ClientOptions: testClientOptions(t)}
	_, _, err := newOktaAdapter(context.Background(), conf, &ackSink{})
	require.Error(t, err)
	require.Error(t, conf.Prepare(context.Background()))
	first := state.saved()
	require.NotEmpty(t, first)
	require.False(t, conf.prepared.ready)
	state.fail = false
	require.NoError(t, conf.Prepare(context.Background()))
	require.Equal(t, first, conf.prepared.initial)
	require.NoError(t, conf.Prepare(context.Background()))
	require.Equal(t, 2, state.calls)
	// Cancel before construction so no real vendor request is possible.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a, _, err := newOktaAdapter(ctx, conf, &ackSink{})
	require.NoError(t, err)
	require.NoError(t, a.Close())
	require.Equal(t, 2, state.calls)
}

func TestResumeRestartGapSameTimestampAndLateEmptyPages(t *testing.T) {
	// The saved opaque cursor must survive byte-for-byte; published timestamps
	// deliberately arrive out of order and are not a completeness watermark.
	var calls atomic.Int32
	var firstQuery atomic.Value
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			firstQuery.Store(r.URL.RawQuery)
		}
		next := "late"
		if n >= 3 {
			next = "tail"
		}
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=%s>; rel="next"`, r.Host, logsURL, next))
		switch n {
		case 1:
			fmt.Fprint(w, `[{"uuid":"gap-a","published":"2026-01-01T01:00:00Z"},{"uuid":"gap-b","published":"2026-01-01T01:00:00Z"}]`)
		case 3:
			fmt.Fprint(w, `[{"uuid":"late","published":"2026-01-01T00:59:00Z"}]`)
		default:
			fmt.Fprint(w, `[]`)
		}
	}))
	defer vendor.Close()
	query := "after=saved+%2f%2b&sortOrder=ASCENDING"
	state := &memoryProgress{durable: progressBytes(t, query, time.Now().Add(-time.Hour))}
	sink := &ackSink{}
	a, g := resumeHarness(t, vendor.URL, state, sink)
	stop := runCollector(t, a, g)
	require.Eventually(t, func() bool { return calls.Load() >= 5 }, 5*time.Second, time.Millisecond)
	stop()
	require.Equal(t, query, firstQuery.Load())
	require.Equal(t, 3, sink.count())
	require.Empty(t, state.snapshot(), "empty pages may not cross unconfirmed admissions")
	sink.ack(3)
	require.NoError(t, a.confirm(g))
	p, err := decodePosition(state.snapshot())
	require.NoError(t, err)
	require.Equal(t, "after=tail", p.Query)
	// A failed periodic/final durable save leaves the prior restart cursor.
	require.Equal(t, query, func() string { p, e := decodePosition(state.saved()); require.NoError(t, e); return p.Query }())
	state.durable = state.snapshot()
	conf := a.conf
	conf.prepared = nil
	require.NoError(t, conf.Prepare(context.Background()))
	u, err := url.Parse(conf.prepared.position.Next)
	require.NoError(t, err)
	require.Equal(t, "after=tail", u.RawQuery)
}

func TestResumeFailedPageCannotPublishItsCursor(t *testing.T) {
	var calls atomic.Int32
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) > 1 {
			w.WriteHeader(500)
			fmt.Fprint(w, `{"error":"fixture"}`)
			return
		}
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=complete>; rel="next"`, r.Host, logsURL))
		fmt.Fprint(w, `[]`)
	}))
	defer vendor.Close()
	state := &memoryProgress{}
	a, g := resumeHarness(t, vendor.URL, state, &ackSink{})
	stop := runCollector(t, a, g)
	require.Eventually(t, func() bool { return calls.Load() >= 2 }, 5*time.Second, time.Millisecond)
	stop()
	require.NoError(t, a.confirm(g))
	p, err := decodePosition(state.snapshot())
	require.NoError(t, err)
	require.Equal(t, "after=complete", p.Query)
}

type memoryDedup struct {
	mu       sync.Mutex
	seen     map[string]bool
	adds     int
	attempts int
	fail     bool
}

func validMarkerBatch(ns string, keys []string) error {
	if len(ns) != 64 {
		return errors.New("namespace must be 64 hex characters")
	}
	if _, err := hex.DecodeString(ns); err != nil {
		return err
	}
	for _, key := range keys {
		if len(key) != 64 {
			return errors.New("key must be 64 hex characters")
		}
		if _, err := hex.DecodeString(key); err != nil {
			return err
		}
	}
	return nil
}
func (d *memoryDedup) Check(_ context.Context, ns string, keys []string) ([]bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := validMarkerBatch(ns, keys); err != nil {
		return nil, err
	}
	if d.fail {
		return nil, errors.New("unavailable")
	}
	out := make([]bool, len(keys))
	for i, k := range keys {
		out[i] = d.seen[ns+k]
	}
	return out, nil
}
func (d *memoryDedup) Add(_ context.Context, ns string, keys []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.attempts++
	if err := validMarkerBatch(ns, keys); err != nil {
		return err
	}
	if d.fail {
		return errors.New("unavailable")
	}
	for _, k := range keys {
		d.seen[ns+k] = true
		d.adds++
	}
	return nil
}

func TestResumeSharedDedupOnlyAfterValidatedACK(t *testing.T) {
	var calls atomic.Int32
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=tail>; rel="next"`, r.Host, logsURL))
		fmt.Fprint(w, `[{"uuid":"same"}]`)
	}))
	defer vendor.Close()
	state := &memoryProgress{}
	sink := &ackSink{}
	a, g := resumeHarness(t, vendor.URL, state, sink)
	d := &memoryDedup{seen: map[string]bool{}}
	a.conf.ResumeDeduper = d
	a.conf.ResumeNamespace = strings.Repeat("a", 64)
	stop := runCollector(t, a, g)
	require.Eventually(t, func() bool { return calls.Load() >= 4 }, 5*time.Second, time.Millisecond)
	d.mu.Lock()
	d.seen[a.conf.ResumeNamespace+dedupKey(sharedEventDigest("same"))] = true
	d.mu.Unlock()
	require.Eventually(t, func() bool { return calls.Load() >= 8 }, 5*time.Second, time.Millisecond)
	stop()
	require.Empty(t, state.snapshot(), "a shared marker cannot erase a pending local dependency")
	require.Equal(t, 1, sink.count())
	require.Zero(t, d.adds)
	sink.ack(1)
	require.NoError(t, a.confirm(g))
	a.flushDedup(context.Background(), g)
	require.Equal(t, 1, d.adds)
	seen := a.sharedDuplicates(context.Background(), []utils.Dict{{"uuid": "same"}})
	require.True(t, seen[eventDigest("same")])
	a.conf.ResumeNamespace = strings.Repeat("b", 64)
	require.Empty(t, a.sharedDuplicates(context.Background(), []utils.Dict{{"uuid": "same"}}))
	d.fail = true
	a.conf.ResumeNamespace = strings.Repeat("a", 64)
	require.Empty(t, a.sharedDuplicates(context.Background(), []utils.Dict{{"uuid": "same"}}), "dedup outage must replay")
}

func TestResumeRecoveryCapAndExpiredCursorKeepCollecting(t *testing.T) {
	for _, age := range []time.Duration{time.Hour, 4 * time.Hour} {
		t.Run(age.String(), func(t *testing.T) {
			var fallback atomic.Bool
			vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("after") == "expired" {
					w.WriteHeader(400)
					fmt.Fprint(w, `{"errorCode":"E0000001"}`)
					return
				}
				fallback.Store(true)
				w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=forward>; rel="next"`, r.Host, logsURL))
				fmt.Fprint(w, `[]`)
			}))
			defer vendor.Close()
			state := &memoryProgress{durable: progressBytes(t, "after=expired", time.Now().Add(-age))}
			a := directAdapter(t, vendor.URL)
			var skipped, unknown atomic.Uint64
			a.conf.ResumeState = state
			a.conf.ResumeStats = func(class string, n uint64) {
				switch class {
				case "skipped":
					skipped.Add(n)
				case "unknown":
					unknown.Add(n)
				}
			}
			require.NoError(t, a.conf.Prepare(context.Background()))
			g := newOktaGeneration(1, a.initialPosition())
			a.uspClient = &ackSink{}
			a.pollInterval = time.Millisecond
			var err error
			a.tracker, err = acktrack.NewACKTracker(a.uspClient, 1, 16+maxResumeBytes)
			require.NoError(t, err)
			stop := runCollector(t, a, g)
			require.Eventually(t, func() bool { return fallback.Load() && len(state.snapshot()) > 0 }, 5*time.Second, time.Millisecond)
			stop()
			require.NoError(t, a.confirm(g))
			require.NotEmpty(t, state.snapshot())
			if age > recoveryHorizon {
				require.Greater(t, skipped.Load(), uint64(55))
			} else {
				require.Equal(t, uint64(1), unknown.Load())
			}
		})
	}
}

func TestResumeNilStateDoesNotInvokeHooks(t *testing.T) {
	conf := OktaConfig{URL: "https://example.okta.com", ResumeDeduper: &memoryDedup{fail: true}, ResumeStats: func(string, uint64) { t.Fatal("unexpected state statistic") }}
	require.NoError(t, conf.Prepare(context.Background()))
	require.Nil(t, conf.prepared)
}

type drainACKSink struct {
	ackSink
	drainError error
}

func (s *drainACKSink) Drain(time.Duration) error { s.ack(^uint64(0)); return s.drainError }

func TestResumeClosePublishesFinalACKOnDrainError(t *testing.T) {
	for _, drainErr := range []error{nil, errors.New("drain interrupted")} {
		t.Run(fmt.Sprint(drainErr), func(t *testing.T) {
			var calls atomic.Int32
			vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=final>; rel="next"`, r.Host, logsURL))
				fmt.Fprint(w, `[{"uuid":"final"}]`)
			}))
			defer vendor.Close()
			state := &memoryProgress{}
			conf := OktaConfig{URL: vendor.URL, ApiKey: "fixture", ResumeState: state, ClientOptions: testClientOptions(t), PollInterval: time.Millisecond}
			require.NoError(t, conf.Prepare(context.Background()))
			sink := &drainACKSink{drainError: drainErr}
			a, _, err := newOktaAdapter(context.Background(), conf, sink)
			require.NoError(t, err)
			require.Eventually(t, func() bool { return calls.Load() >= 3 }, 5*time.Second, time.Millisecond)
			require.Empty(t, state.snapshot())
			err = a.Close()
			if drainErr != nil {
				require.ErrorIs(t, err, drainErr)
			} else {
				require.NoError(t, err)
			}
			p, err := decodePosition(state.snapshot())
			require.NoError(t, err)
			require.Equal(t, "after=final", p.Query)
		})
	}
}

func TestResumeRejectsInvalidOrOversizedStateAndRequiresDedupNamespace(t *testing.T) {
	for _, raw := range [][]byte{[]byte(`{}`), make([]byte, maxResumeBytes+1), progressBytes(t, "api_key=forbidden", time.Now())} {
		conf := OktaConfig{URL: "https://example.okta.com", ResumeState: &memoryProgress{durable: raw}}
		require.Error(t, conf.Prepare(context.Background()))
	}
	conf := OktaConfig{URL: "https://example.okta.com", ResumeState: &memoryProgress{}, ResumeDeduper: &memoryDedup{}}
	require.Error(t, conf.Prepare(context.Background()))
}

func TestResumeExpiredCursorWithoutTimestampUsesBoundedOverlap(t *testing.T) {
	firstFallback := make(chan string, 1)
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after") == "expired" {
			w.WriteHeader(400)
			fmt.Fprint(w, `{"errorCode":"E0000001"}`)
			return
		}
		select {
		case firstFallback <- r.URL.Query().Get("since"):
		default:
		}
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=forward>; rel="next"`, r.Host, logsURL))
		fmt.Fprint(w, `[]`)
	}))
	defer vendor.Close()
	raw, err := json.Marshal(savedPosition{Version: 1, Query: "after=expired", Observed: time.Now()})
	require.NoError(t, err)
	state := &memoryProgress{durable: raw}
	a, g := resumeHarness(t, vendor.URL, state, &ackSink{})
	var unknown atomic.Uint64
	a.conf.ResumeStats = func(class string, n uint64) {
		if class == "unknown" {
			unknown.Add(n)
		}
	}
	observed := g.position.Observed.UTC()
	stop := runCollector(t, a, g)
	select {
	case rawSince := <-firstFallback:
		since, err := time.Parse(time.RFC3339Nano, rawSince)
		require.NoError(t, err)
		require.Equal(t, observed.Add(-overlapPeriod), since)
	case <-time.After(5 * time.Second):
		t.Fatal("missing timestamp parked collection")
	}
	stop()
	require.Equal(t, uint64(1), unknown.Load())
}

func TestResumeSharedMarkerFullDigestAndOneAttemptPerConfirmation(t *testing.T) {
	require.Equal(t, "dba4f47042805b4b03c9ecde4daaa26d20557f0aa319cb2f0d74eccb29ba1ae7", dedupKey(sharedEventDigest("same")))
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			vendor := httptest.NewServer(http.NotFoundHandler())
			defer vendor.Close()
			sink := &ackSink{}
			a, g := resumeHarness(t, vendor.URL, &memoryProgress{}, sink)
			d := &memoryDedup{seen: map[string]bool{}, fail: fail}
			a.conf.ResumeDeduper = d
			a.conf.ResumeNamespace = strings.Repeat("a", 64)
			dep, err := a.admit(context.Background(), &protocol.DataMessage{TextPayload: "fixture"})
			require.NoError(t, err)
			g.page = 1
			digest := eventDigest("same")
			g.live[digest] = liveID{dependency: dep, page: 1, sharedDigest: sharedEventDigest("same")}
			marker, err := a.resumeCandidate(g)
			require.NoError(t, err)
			require.NoError(t, a.tracker.Candidate("logs", marker, dep))
			a.flushDedup(context.Background(), g)
			require.Zero(t, d.attempts, "pending admission is never shared delivery evidence")
			sink.ack(1)
			require.NoError(t, a.confirm(g))
			a.flushDedup(context.Background(), g)
			require.Equal(t, 1, d.attempts)
			d.fail = false
			for i := 0; i < 10; i++ {
				g.confirmed.Store(uint64(i + 2)) // New confirmed empty pages must not renew old markers.
				require.NoError(t, a.confirm(g))
				a.flushDedup(context.Background(), g)
			}
			require.Equal(t, 1, d.attempts, "confirmed IDs must not renew TTL or retry failed Add")
			if fail {
				require.Empty(t, d.seen, "failed Add permits replay")
			} else {
				require.Len(t, d.seen, 1)
			}
		})
	}
}

func TestResumeLongRunningCursorExpiryDoesNotReplayStartupHistory(t *testing.T) {
	var skipped, unknown atomic.Uint64
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		after := r.URL.Query().Get("after")
		if after == "expired" {
			w.WriteHeader(400)
			fmt.Fprint(w, `{"errorCode":"E0000001"}`)
			return
		}
		next := "expired"
		body := `[{"uuid":"already-collected","published":"2099-01-01T00:00:00Z"}]`
		if after != "start" {
			next = "forward"
			body = `[]`
		}
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=%s>; rel="next"`, r.Host, logsURL, next))
		fmt.Fprint(w, body)
	}))
	defer vendor.Close()
	now := time.Now()
	raw, err := json.Marshal(savedPosition{Version: 1, Query: "after=start", Since: now.Add(-48 * time.Hour), Observed: now})
	require.NoError(t, err)
	state := &memoryProgress{durable: raw}
	sink := &captureSink{}
	a, g := resumeHarness(t, vendor.URL, state, sink)
	a.conf.ResumeStats = func(class string, n uint64) {
		switch class {
		case "skipped":
			skipped.Add(n)
		case "unknown":
			unknown.Add(n)
		}
	}
	stop := runCollector(t, a, g)
	require.Eventually(t, func() bool { p, e := decodePosition(state.snapshot()); return e == nil && p.Query == "after=forward" }, 5*time.Second, time.Millisecond)
	stop()
	p, err := decodePosition(state.snapshot())
	require.NoError(t, err)
	require.False(t, p.Since.Before(now.Add(-overlapPeriod)))
	require.False(t, p.Since.After(time.Now().Add(-overlapPeriod)))
	require.Zero(t, skipped.Load(), "previously collected startup history is not skipped history")
	require.Equal(t, uint64(1), unknown.Load())
	require.Equal(t, 1, sink.count())
}

type slowMarkerBackend struct{ checks, adds atomic.Int32 }

func (d *slowMarkerBackend) Check(ctx context.Context, _ string, _ []string) ([]bool, error) {
	d.checks.Add(1)
	<-ctx.Done()
	return nil, ctx.Err()
}
func (d *slowMarkerBackend) Add(ctx context.Context, _ string, _ []string) error {
	d.adds.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func TestResumeSlowDedupBacksOffWithoutHoldingCollection(t *testing.T) {
	vendor := httptest.NewServer(http.NotFoundHandler())
	defer vendor.Close()
	a, g := resumeHarness(t, vendor.URL, &memoryProgress{}, &ackSink{})
	backend := &slowMarkerBackend{}
	a.conf.ResumeDeduper = backend
	a.conf.ResumeNamespace = strings.Repeat("a", 64)
	require.Empty(t, a.sharedDuplicates(context.Background(), []utils.Dict{{"uuid": "slow"}}))
	require.Equal(t, int32(1), backend.checks.Load())
	for i := 0; i < 10; i++ {
		require.Empty(t, a.sharedDuplicates(context.Background(), []utils.Dict{{"uuid": "slow"}}))
	}
	require.Equal(t, int32(1), backend.checks.Load(), "failed checks enter cooldown")
	g.live[eventDigest("slow")] = liveID{page: 1, sharedDigest: sharedEventDigest("slow")}
	g.confirmed.Store(1)
	a.flushDedup(context.Background(), g)
	require.Zero(t, backend.adds.Load(), "the shared backend cooldown also skips Add")
	a.dedupRetryAfter = time.Time{}
	g.live[eventDigest("next")] = liveID{page: 2, sharedDigest: sharedEventDigest("next")}
	g.confirmed.Store(2)
	a.flushDedup(context.Background(), g)
	require.Equal(t, int32(1), backend.adds.Load())
	a.flushDedup(context.Background(), g)
	require.Equal(t, int32(1), backend.adds.Load(), "unchanged confirmed frontier neither scans nor retries Add")
}

func TestResumeOversizedCursorFallsBackWithoutCrossingPendingPage(t *testing.T) {
	var calls atomic.Int32
	var second atomic.Bool
	var unknown atomic.Uint64
	bigA, bigB := strings.Repeat("x", 5*1024), strings.Repeat("y", 5*1024)
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		next, id := bigA, "first-page"
		switch r.URL.Query().Get("after") {
		case bigA:
			second.Store(true)
			next, id = bigB, "second-page"
		case bigB:
			next, id = bigB, "third-page"
		}
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=%s>; rel="next"`, r.Host, logsURL, next))
		fmt.Fprintf(w, `[{"uuid":%q}]`, id)
	}))
	defer vendor.Close()
	state := &memoryProgress{}
	sink := &ackSink{}
	a, g := resumeHarness(t, vendor.URL, state, sink)
	a.pollInterval = 20 * time.Millisecond
	initial := append([]byte(nil), a.conf.prepared.positionBytes...)
	a.conf.ResumeStats = func(class string, n uint64) {
		if class == "unknown" {
			unknown.Add(n)
		}
	}
	started := time.Now()
	stop := runCollector(t, a, g)
	require.Eventually(t, func() bool { return second.Load() && calls.Load() >= 4 }, 5*time.Second, time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	stop()
	require.LessOrEqual(t, int(calls.Load()), 3+int(time.Since(started)/a.pollInterval)+2, "a repeated nonempty cursor must be paced, not a hot vendor loop")
	require.Equal(t, 3, sink.count(), "real oversized continuations must reach later pages")
	require.Contains(t, g.position.Next, "after="+bigB, "live cursor must remain intact")
	require.Equal(t, uint64(1), unknown.Load(), "one unknown count per consecutive oversized episode")
	require.Empty(t, state.snapshot(), "all fallback candidates retain the pending page dependencies")
	sink.ack(3)
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, state.snapshot(), "only the durable candidate uses the unchanged prior bounded position")
	require.LessOrEqual(t, len(state.snapshot()), 4096)
	envelope, err := json.Marshal(struct {
		Position []byte
		Metadata string
	}{make([]byte, maxResumeBytes), strings.Repeat("m", 2048)})
	require.NoError(t, err)
	require.LessOrEqual(t, len(envelope), 8192)
}

func TestResumeRestoreOversizedCursorUsesBoundedUnconfirmedFallback(t *testing.T) {
	now := time.Now().UTC()
	raw := progressBytes(t, "after="+strings.Repeat("x", 5*1024), now)
	state := &recoverProgress{memoryProgress: &memoryProgress{durable: raw}}
	var unknown uint64
	conf := OktaConfig{URL: "https://example.okta.com", ResumeState: state, ResumeStats: func(class string, n uint64) {
		if class == "unknown" {
			unknown += n
		}
	}}
	require.NoError(t, conf.Prepare(context.Background()))
	u, err := url.Parse(conf.prepared.position.Next)
	require.NoError(t, err)
	require.Empty(t, u.Query().Get("after"))
	since, err := time.Parse(time.RFC3339Nano, u.Query().Get("since"))
	require.NoError(t, err)
	require.False(t, since.Before(now.Add(-overlapPeriod)))
	require.False(t, since.After(time.Now().Add(-overlapPeriod)))
	require.Equal(t, uint64(1), unknown)
	require.Empty(t, state.snapshot(), "recovery is durable initialization, not a confirmed fetched frontier")
	require.Equal(t, 1, state.recoverCalls)
	require.LessOrEqual(t, len(state.saved()), 4096)
	require.NotEqual(t, raw, state.saved(), "recovery must durably establish a valid baseline before polling")
}

func TestResumeExpiredCursorPreservesOutageBeforeOverlap(t *testing.T) {
	now := time.Now().UTC()
	observed := now.Add(-2 * time.Hour)
	requested := make(chan string, 1)
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after") == "expired" {
			w.WriteHeader(400)
			fmt.Fprint(w, `{"errorCode":"E0000001"}`)
			return
		}
		select {
		case requested <- r.URL.Query().Get("since"):
		default:
		}
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=forward>; rel="next"`, r.Host, logsURL))
		fmt.Fprint(w, `[]`)
	}))
	defer vendor.Close()
	raw, err := json.Marshal(savedPosition{Version: 1, Query: "after=expired", Since: now.Add(-48 * time.Hour), Observed: observed})
	require.NoError(t, err)
	a, g := resumeHarness(t, vendor.URL, &memoryProgress{durable: raw}, &ackSink{})
	var skipped, unknown atomic.Uint64
	a.conf.ResumeStats = func(class string, n uint64) {
		switch class {
		case "skipped":
			skipped.Add(n)
		case "unknown":
			unknown.Add(n)
		}
	}
	stop := runCollector(t, a, g)
	select {
	case rawSince := <-requested:
		since, err := time.Parse(time.RFC3339Nano, rawSince)
		require.NoError(t, err)
		require.Equal(t, observed.Add(-overlapPeriod), since)
	case <-time.After(5 * time.Second):
		t.Fatal("expired cursor did not fall back")
	}
	stop()
	require.Zero(t, skipped.Load())
	require.Equal(t, uint64(1), unknown.Load())
}

type recoverProgress struct {
	*memoryProgress
	recoverCalls    int
	baselines       [][]byte
	recoverFail     bool
	restoreError    error
	invalidRecovery bool
}

func (m *recoverProgress) Restore(ctx context.Context, initial []byte) ([]byte, error) {
	raw, err := m.memoryProgress.Restore(ctx, initial)
	if err != nil {
		return nil, err
	}
	if m.restoreError != nil {
		return nil, m.restoreError
	}
	return raw, nil
}
func (m *recoverProgress) Recover(_ context.Context, baseline []byte) ([]byte, error) {
	m.recoverCalls++
	m.baselines = append(m.baselines, append([]byte(nil), baseline...))
	if m.recoverFail {
		return nil, errors.New("recovery storage unavailable")
	}
	if m.invalidRecovery {
		return []byte(`{}`), nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.durable = append([]byte(nil), baseline...)
	return append([]byte(nil), m.durable...), nil
}

func TestResumeInvalidStateRequiresDurableRecoveryBeforePolling(t *testing.T) {
	now := time.Now().UTC()
	unsupported := progressBytes(t, "after=old", now)
	unsupported = []byte(strings.Replace(string(unsupported), `"v":1`, `"v":2`, 1))
	rejected := progressBytes(t, "until=unsupported", now.Add(-4*time.Hour))
	canonicalOversize := []byte(fmt.Sprintf(`{"v":1,"q":%q,"since":%q,"observed":%q}`, "after=x"+strings.Repeat("&x=1", 600), now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)))
	require.Less(t, len(canonicalOversize), maxResumeBytes)
	for name, raw := range map[string][]byte{"malformed": []byte(`{`), "unsupported": unsupported, "rejected": rejected, "oversized": make([]byte, 10*1024), "canonical-size": canonicalOversize} {
		t.Run(name, func(t *testing.T) {
			store := &recoverProgress{memoryProgress: &memoryProgress{durable: raw}, recoverFail: true}
			var unknown, skipped uint64
			conf := OktaConfig{URL: "https://example.okta.com", ResumeState: store, ResumeStats: func(class string, n uint64) {
				switch class {
				case "unknown":
					unknown += n
				case "skipped":
					skipped += n
				}
			}}
			require.Error(t, conf.Prepare(context.Background()))
			require.False(t, conf.prepared.ready)
			require.Zero(t, unknown)
			require.Zero(t, skipped)
			require.Equal(t, raw, store.saved())
			_, _, err := newOktaAdapter(context.Background(), conf, &ackSink{})
			require.Error(t, err)
			require.Error(t, conf.Prepare(context.Background()))
			require.Equal(t, 1, store.calls, "a recovery retry reuses the known read/revision")
			require.Len(t, store.baselines, 2)
			require.Equal(t, store.baselines[0], store.baselines[1], "recovery baseline is fixed across retries")
			store.recoverFail = false
			require.NoError(t, conf.Prepare(context.Background()))
			require.Equal(t, store.baselines[0], store.saved())
			require.Equal(t, uint64(1), unknown)
			require.Zero(t, skipped)
			require.NoError(t, conf.Prepare(context.Background()))
			require.Equal(t, uint64(1), unknown)
			require.Equal(t, 3, store.recoverCalls)
		})
	}
}

func TestResumeStorageErrorsNeverTriggerRecovery(t *testing.T) {
	for _, readError := range []error{errors.New("storage read failed"), context.Canceled, context.DeadlineExceeded} {
		store := &recoverProgress{memoryProgress: &memoryProgress{}, restoreError: readError}
		conf := OktaConfig{URL: "https://example.okta.com", ResumeState: store}
		require.ErrorIs(t, conf.Prepare(context.Background()), readError)
		require.Zero(t, store.recoverCalls)
		require.Nil(t, conf.prepared.recovery)
	}
	store := &recoverProgress{memoryProgress: &memoryProgress{}, restoreError: fmt.Errorf("known invalid stored envelope: %w", resume.ErrInvalidPosition)}
	conf := OktaConfig{URL: "https://example.okta.com", ResumeState: store}
	require.NoError(t, conf.Prepare(context.Background()))
	require.Equal(t, 1, store.recoverCalls)
}

func TestResumeRecoveryRejectsInvalidRecoveryReply(t *testing.T) {
	store := &recoverProgress{memoryProgress: &memoryProgress{durable: []byte(`{}`)}, invalidRecovery: true}
	var unknown uint64
	conf := OktaConfig{URL: "https://example.okta.com", ResumeState: store, ResumeStats: func(class string, n uint64) { unknown += n }}
	require.ErrorIs(t, conf.Prepare(context.Background()), resume.ErrInvalidPosition)
	require.False(t, conf.prepared.ready)
	require.Zero(t, unknown)
	store.invalidRecovery = false
	require.NoError(t, conf.Prepare(context.Background()))
	require.Equal(t, uint64(1), unknown)
}

func TestResumeNamespaceAndPreparationCounters(t *testing.T) {
	for _, ns := range []string{"raw identity", strings.Repeat("A", 64), strings.Repeat("a", 63), strings.Repeat("g", 64)} {
		store := &memoryProgress{}
		conf := OktaConfig{URL: "https://example.okta.com", ResumeState: store, ResumeNamespace: ns}
		require.Error(t, conf.Prepare(context.Background()))
		require.Zero(t, store.calls)
	}
	store := &memoryProgress{durable: progressBytes(t, "after=old", time.Now().Add(-4*time.Hour))}
	var skipped uint64
	conf := OktaConfig{URL: "https://example.okta.com", ResumeState: store, ResumeNamespace: strings.Repeat("a", 64), ResumeStats: func(class string, n uint64) {
		if class == "skipped" {
			skipped += n
		}
	}}
	require.NoError(t, conf.Prepare(context.Background()))
	first := skipped
	require.Greater(t, first, uint64(55))
	require.NoError(t, conf.Prepare(context.Background()))
	require.Equal(t, first, skipped)
	// The optional API produces a typed error when durable recovery is missing.
	bad := OktaConfig{URL: "https://example.okta.com", ResumeState: &memoryProgress{durable: []byte(`{}`)}}
	require.ErrorIs(t, bad.Prepare(context.Background()), resume.ErrInvalidPosition)
}

func TestResumeRecoveredInvalidStateCollectsForward(t *testing.T) {
	sinceRequest := make(chan string, 1)
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case sinceRequest <- r.URL.Query().Get("since"):
		default:
		}
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=tail>; rel="next"`, r.Host, logsURL))
		fmt.Fprint(w, `[{"uuid":"after-recovery"}]`)
	}))
	defer vendor.Close()
	store := &recoverProgress{memoryProgress: &memoryProgress{durable: []byte(`{"v":999}`)}}
	var unknown atomic.Uint64
	conf := OktaConfig{URL: vendor.URL, ApiKey: "fixture", ClientOptions: testClientOptions(t), PollInterval: time.Millisecond, ResumeState: store, ResumeStats: func(class string, n uint64) {
		if class == "unknown" {
			unknown.Add(n)
		}
	}}
	started := time.Now()
	require.NoError(t, conf.Prepare(context.Background()))
	sink := &captureSink{}
	a, _, err := newOktaAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { a.Close() })
	select {
	case raw := <-sinceRequest:
		since, err := time.Parse(time.RFC3339Nano, raw)
		require.NoError(t, err)
		require.False(t, since.Before(started.Add(-overlapPeriod)))
		require.False(t, since.After(time.Now().Add(-overlapPeriod)))
	case <-time.After(5 * time.Second):
		t.Fatal("recovery never started collection")
	}
	require.Eventually(t, func() bool { return sink.count() == 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, a.Close())
	require.Equal(t, uint64(1), unknown.Load())
	require.NotEmpty(t, store.snapshot())
}

func TestResumeOversizedDurableCandidateReusesLastBoundedPosition(t *testing.T) {
	vendor := httptest.NewServer(http.NotFoundHandler())
	defer vendor.Close()
	a, g := resumeHarness(t, vendor.URL, &memoryProgress{}, &ackSink{})
	var unknown uint64
	a.conf.ResumeStats = func(class string, n uint64) {
		if class == "unknown" {
			unknown += n
		}
	}
	u := *a.endpoint
	u.RawQuery = "after=last-bounded"
	g.position.Next = u.String()
	g.page = 1
	bounded, err := a.resumeCandidate(g)
	require.NoError(t, err)
	u.RawQuery = "after=" + strings.Repeat("x", 5*1024)
	g.position.Next = u.String()
	live := g.position
	for i := 0; i < 10; i++ {
		g.page++
		candidate, err := a.resumeCandidate(g)
		require.NoError(t, err)
		require.Equal(t, bounded[16:], candidate[16:], "only local page markers change while durable source position stays fixed")
		require.Equal(t, live, g.position, "durable fallback cannot rewrite live pagination")
	}
	require.Equal(t, uint64(1), unknown)
	u.RawQuery = "after=next-bounded"
	g.position.Next = u.String()
	bounded, err = a.resumeCandidate(g)
	require.NoError(t, err)
	p, err := decodePosition(bounded[16:])
	require.NoError(t, err)
	require.Equal(t, "after=next-bounded", p.Query)
	u.RawQuery = "after=" + strings.Repeat("y", 5*1024)
	g.position.Next = u.String()
	candidate, err := a.resumeCandidate(g)
	require.NoError(t, err)
	require.Equal(t, bounded[16:], candidate[16:])
	require.Equal(t, uint64(2), unknown, "a new oversized episode begins only after a bounded continuation")
}
