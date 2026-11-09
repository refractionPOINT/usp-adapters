package usp_okta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
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
	require.Eventually(t, func() bool { return calls.Load() >= 5 }, time.Second, time.Millisecond)
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
	require.Eventually(t, func() bool { return calls.Load() >= 2 }, time.Second, time.Millisecond)
	stop()
	require.NoError(t, a.confirm(g))
	p, err := decodePosition(state.snapshot())
	require.NoError(t, err)
	require.Equal(t, "after=complete", p.Query)
}

type memoryDedup struct {
	mu   sync.Mutex
	seen map[string]bool
	adds int
	fail bool
}

func (d *memoryDedup) Check(_ context.Context, ns string, keys []string) ([]bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
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
	a.conf.ResumeNamespace = "hashed-source-a"
	stop := runCollector(t, a, g)
	require.Eventually(t, func() bool { return calls.Load() >= 4 }, time.Second, time.Millisecond)
	stop()
	require.Equal(t, 1, sink.count())
	require.Zero(t, d.adds)
	sink.ack(1)
	require.NoError(t, a.confirm(g))
	a.flushDedup(context.Background(), g)
	require.Equal(t, 1, d.adds)
	seen := a.sharedDuplicates(context.Background(), []utils.Dict{{"uuid": "same"}})
	require.True(t, seen[eventDigest("same")])
	a.conf.ResumeNamespace = "hashed-source-b"
	require.Empty(t, a.sharedDuplicates(context.Background(), []utils.Dict{{"uuid": "same"}}))
	d.fail = true
	a.conf.ResumeNamespace = "hashed-source-a"
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
			require.Eventually(t, func() bool { return fallback.Load() && len(state.snapshot()) > 0 }, time.Second, time.Millisecond)
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
			require.Eventually(t, func() bool { return calls.Load() >= 3 }, time.Second, time.Millisecond)
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
	started := time.Now()
	stop := runCollector(t, a, g)
	select {
	case rawSince := <-firstFallback:
		since, err := time.Parse(time.RFC3339Nano, rawSince)
		require.NoError(t, err)
		require.WithinDuration(t, started.Add(-overlapPeriod), since, time.Second)
	case <-time.After(time.Second):
		t.Fatal("missing timestamp parked collection")
	}
	stop()
	require.Equal(t, uint64(1), unknown.Load())
}
