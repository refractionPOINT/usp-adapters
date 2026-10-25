package usp_entraid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

func fixtureRuntime() entraRuntime {
	rt := defaultRuntime()
	elapsedStart := time.Now()
	rt.now = func() time.Time { return fixtureBaseTime().Add(time.Hour).Add(time.Since(elapsedStart)) }
	rt.start = fixtureBaseTime().Add(-time.Minute).UnixNano()
	rt.window = 61 * time.Minute
	rt.reconcileInterval = 50 * time.Millisecond
	rt.liveSpacing = 0
	rt.replaySpacing = 0
	rt.globalSpacing = 0
	rt.liveBudget = 100000
	rt.replayBudget = 100000
	return rt
}
func vendorServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			require.NoError(t, r.ParseForm())
			require.Equal(t, "fixture&secret=encoded", r.Form.Get("client_secret"))
			fmt.Fprint(w, `{"access_token":"fixture-bearer"}`)
			return
		}
		require.Equal(t, "Bearer fixture-bearer", r.Header.Get("Authorization"))
		handler(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}
func harness(t *testing.T, server *httptest.Server, sink uspSink) (*EntraIDAdapter, *entraGeneration) {
	t.Helper()
	rt := fixtureRuntime()
	rt.window = 5 * time.Minute
	rt.ackInterval = func() time.Duration { return 10 * time.Millisecond }
	conf := EntraIDConfig{ClientOptions: testClientOptions(t), TenantID: testTenantID, ClientID: testClientID, ClientSecret: "fixture&secret=encoded", LoginEndpoint: server.URL, GraphEndpoint: server.URL, Streams: "risk_detections,sign_ins,audit_logs"}
	streams, err := conf.streams()
	require.NoError(t, err)
	a := &EntraIDAdapter{conf: conf, streams: streams, httpClient: server.Client(), uspClient: sink, runtime: rt, startedAt: fixtureBaseTime().UnixNano(), pollInterval: time.Millisecond}
	a.tracker, err = acktrack.NewACKTracker(sink, 1, 16)
	require.NoError(t, err)
	return a, newGeneration(1, a.initialPosition())
}
func bounds(t *testing.T, r *http.Request) (time.Time, time.Time) {
	t.Helper()
	field := "activityDateTime"
	if strings.HasSuffix(r.URL.Path, "signIns") {
		field = "createdDateTime"
	}
	start, ok := parseTimestampFilter(r.URL.Query().Get("$filter"), field)
	require.True(t, ok)
	end, err := parseUntilFilter(r.URL.Query().Get("$filter"), field)
	require.NoError(t, err)
	return start, end
}
func sendPage(w http.ResponseWriter, items []map[string]interface{}, next string) {
	if items == nil {
		items = []map[string]interface{}{}
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"value": items, "@odata.nextLink": next})
}
func item(id string, stamp time.Time) map[string]interface{} {
	return map[string]interface{}{"id": id, "activityDateTime": stamp.Format(time.RFC3339Nano), "createdDateTime": stamp.Format(time.RFC3339Nano)}
}
func scan(t *testing.T, a *EntraIDAdapter, g *entraGeneration, stream entraStream) int64 {
	t.Helper()
	s := g.position[stream.name]
	for range 200 {
		end, err := a.scanWindow(context.Background(), g, stream, s.Boundary, s.WindowEnd, false, &graphRequests{})
		if errors.Is(err, errYield) {
			continue
		}
		require.NoError(t, err)
		require.NoError(t, a.complete(g, stream, end, false))
		return end
	}
	t.Fatal("scan did not complete")
	return 0
}

func TestEntraFixedWindowsPaginationACKAndPendingDuplicates(t *testing.T) {
	var pages atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := bounds(t, r)
		require.Equal(t, 5*time.Minute, end.Sub(start))
		pages.Add(1)
		if r.URL.Query().Get("$skiptoken") == "second" {
			sendPage(w, []map[string]interface{}{item("old-tie-a", start), item("old-tie-b", start)}, "")
			return
		}
		u := *r.URL
		u.Scheme = "http"
		u.Host = r.Host
		q := u.Query()
		q.Set("$skiptoken", "second")
		u.RawQuery = q.Encode()
		sendPage(w, []map[string]interface{}{item("newest", end.Add(-time.Nanosecond)), item("excluded-upper", end)}, u.String())
	})
	sink := &captureSink{withheld: true}
	a, g := harness(t, server, sink)
	initial := g.confirmed.Load()
	end := scan(t, a, g, a.streams[0])
	firstSerial := g.serial
	require.Equal(t, 3, sink.count())
	require.Equal(t, int32(2), pages.Load())
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load())
	// Rechecking this SAME stream suppresses pending duplicates and must retain
	// their original ACK dependency. Another collection owns distinct IDs.
	first := g.position[a.streams[0].name]
	for range 2 {
		_, err := a.scanWindow(context.Background(), g, a.streams[0], first.Coverage, end, true, &graphRequests{})
		if err != nil {
			require.ErrorIs(t, err, errYield)
		}
	}
	require.Equal(t, 3, sink.count())
	require.NoError(t, a.candidate(g))
	scan(t, a, g, a.streams[1])
	require.Equal(t, 6, sink.count())
	sink.ack(2)
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load())
	sink.ack(3)
	require.NoError(t, a.confirm(g))
	require.Equal(t, firstSerial, g.confirmed.Load(), "partial ACK confirms only the first complete stream window")
	sink.ack(6)
	require.NoError(t, a.confirm(g))
	require.Equal(t, g.serial, g.confirmed.Load())
	require.Equal(t, end, g.position[a.streams[1].name].Boundary)
	// An empty later interval still inherits its preceding pending admission.
	sink.mu.Lock()
	sink.withheld = true
	sink.mu.Unlock()
	s := g.position[a.streams[0].name]
	require.NoError(t, a.shipItem(context.Background(), g, a.streams[0], item("pending", time.Unix(0, s.Boundary)), s.Boundary, s.WindowEnd))
	require.NoError(t, a.complete(g, a.streams[0], s.WindowEnd, false))
	saved := g.confirmed.Load()
	require.NoError(t, a.complete(g, a.streams[0], s.WindowEnd+int64(a.runtime.window), false))
	require.NoError(t, a.confirm(g))
	require.Equal(t, saved, g.confirmed.Load())
}

func TestEntraTransientRetryClockResetAndSanitization(t *testing.T) {
	var status atomic.Int32
	status.Store(503)
	var calls atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if status.Load() != 200 {
			w.WriteHeader(int(status.Load()))
			fmt.Fprint(w, `{"secret":"must-not-escape"}`)
			return
		}
		sendPage(w, nil, "")
	})
	a, _ := harness(t, server, &captureSink{})
	clock := fixtureBaseTime()
	a.runtime.now = func() time.Time { return clock }
	var errs []string
	a.conf.ClientOptions.OnError = func(err error) { errs = append(errs, err.Error()) }
	state := &graphRequests{}
	failure := &requestFailure{}
	stream := a.streams[0]
	raw := a.windowURL(stream, clock.UnixNano(), clock.Add(time.Minute).UnixNano())
	request := func(want error) {
		t.Helper()
		_, err := a.requestPage(context.Background(), stream, raw, false, state, failure)
		if want == nil {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, want)
		}
	}
	request(errRetry)
	require.Empty(t, errs)
	clock = clock.Add(59 * time.Minute)
	request(errRetry)
	require.Empty(t, errs)
	clock = clock.Add(time.Minute)
	request(errRetry)
	require.Len(t, errs, 1)
	clock = clock.Add(time.Minute)
	status.Store(200)
	request(nil)
	require.True(t, state.episode(stream.name).since.IsZero())
	require.False(t, state.episode(stream.name).reported)
	status.Store(503)
	clock = clock.Add(time.Minute)
	request(errRetry)
	clock = clock.Add(59 * time.Minute)
	request(errRetry)
	require.Len(t, errs, 1)
	status.Store(403)
	clock = clock.Add(time.Minute)
	request(errVendor)
	require.Len(t, errs, 2)
	for _, e := range errs {
		require.NotContains(t, e, "must-not-escape")
		require.NotContains(t, e, server.URL)
		require.NotContains(t, e, a.conf.ClientSecret)
	}
	require.Equal(t, int32(7), calls.Load())
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type unreadableBody struct{ reads *atomic.Int32 }

func (b unreadableBody) Read([]byte) (int, error) { b.reads.Add(1); return 0, io.ErrUnexpectedEOF }
func (b unreadableBody) Close() error             { return nil }
func TestEntraRetryAfterBeforeBodyAndHTTPGuards(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	a, _ := harness(t, server, &captureSink{})
	now := a.runtime.now().Truncate(time.Second)
	for _, status := range []int{429, 503} {
		var reads atomic.Int32
		a.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{"7200"}}, Body: unreadableBody{&reads}}, nil
		})}
		delay, err := a.doJSON(context.Background(), http.MethodGet, server.URL, "", "token", &graphPage{})
		require.ErrorIs(t, err, errRetry)
		require.Equal(t, 2*time.Hour, delay)
		require.Zero(t, reads.Load())
	}
	require.Equal(t, 2*time.Hour, retryAfter(now.Add(2*time.Hour).Format(http.TimeFormat), now))
	// A truncated 200 body still preserves a vendor delay.
	a.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Retry-After": []string{"120"}}, Body: unreadableBody{&atomic.Int32{}}}, nil
	})}
	delay, err := a.doJSON(context.Background(), http.MethodGet, server.URL, "", "token", &graphPage{})
	require.ErrorIs(t, err, errRetry)
	require.Equal(t, 2*time.Minute, delay)
	// Request contexts impose deadlines even if an injected client has no Timeout.
	a.runtime.requestTimeout = 20 * time.Millisecond
	a.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() })}
	before := time.Now()
	_, err = a.doJSON(context.Background(), http.MethodGet, server.URL, "", "token", &graphPage{})
	require.ErrorIs(t, err, errRetry)
	require.Less(t, time.Since(before), time.Second)
	var leaked atomic.Int32
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1) }))
	defer evil.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, evil.URL, http.StatusFound) }))
	defer redirect.Close()
	a.httpClient = redirect.Client()
	a.runtime.requestTimeout = time.Second
	_, err = a.doJSON(context.Background(), http.MethodGet, redirect.URL, "", "token", &graphPage{})
	require.ErrorIs(t, err, errPage)
	require.Zero(t, leaked.Load())
	for _, raw := range []string{evil.URL + a.streams[0].path, server.URL + a.streams[1].path, server.URL + a.streams[0].path + "?access_token=hidden", server.URL + a.streams[0].path + "#fragment"} {
		require.Error(t, a.validateContinuation(raw, a.streams[0]))
	}
}

func TestEntraSlidingRequestBudgetsAndRetryAfter(t *testing.T) {
	rt := defaultRuntime()
	clock := fixtureBaseTime()
	rt.now = func() time.Time { return clock }
	a := &EntraIDAdapter{runtime: rt}
	state := &graphRequests{}
	var live, replay int
	for n := range 600 {
		if a.reserveRequest(n%5 == 0, state) {
			if n%5 == 0 {
				replay++
			} else {
				live++
			}
		}
		clock = clock.Add(100 * time.Millisecond)
	}
	require.LessOrEqual(t, live, 24)
	require.LessOrEqual(t, replay, 6)
	require.Greater(t, live, 10)
	require.Greater(t, replay, 2)
	require.LessOrEqual(t, live+replay, 30)
	// Bucket caps still hold if spacing is bypassed; no burst across rolling boundaries.
	a.runtime.liveSpacing = 0
	a.runtime.replaySpacing = 0
	a.runtime.globalSpacing = 0
	state = &graphRequests{}
	for range 24 {
		require.True(t, a.reserveRequest(false, state))
	}
	require.False(t, a.reserveRequest(false, state))
	for range 6 {
		require.True(t, a.reserveRequest(true, state))
	}
	require.False(t, a.reserveRequest(true, state))
	clock = clock.Add(59 * time.Second)
	require.False(t, a.reserveRequest(false, state))
	clock = clock.Add(time.Second)
	require.True(t, a.reserveRequest(false, state))
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7200")
		w.WriteHeader(429)
		fmt.Fprint(w, strings.Repeat("x", 1024))
	})
	b, _ := harness(t, server, &captureSink{})
	b.runtime.now = func() time.Time { return clock }
	requests := &graphRequests{}
	failure := &requestFailure{}
	stream := b.streams[0]
	raw := b.windowURL(stream, clock.UnixNano(), clock.Add(time.Minute).UnixNano())
	_, err := b.requestPage(context.Background(), stream, raw, true, requests, failure)
	require.ErrorIs(t, err, errRetry)
	require.GreaterOrEqual(t, failure.retryAt.Sub(clock), 2*time.Hour)
	clock = clock.Add(time.Hour)
	_, err = b.requestPage(context.Background(), b.streams[1], b.windowURL(b.streams[1], clock.UnixNano(), clock.Add(time.Minute).UnixNano()), false, requests, &requestFailure{})
	require.ErrorIs(t, err, errYield)
}

func TestEntraLateVisibilityReconciliationAndPageYield(t *testing.T) {
	var late atomic.Bool
	var pages atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		pages.Add(1)
		start, _ := bounds(t, r)
		if r.URL.Query().Get("$skiptoken") == "next" {
			sendPage(w, nil, "")
			return
		}
		u := *r.URL
		u.Scheme = "http"
		u.Host = r.Host
		q := u.Query()
		q.Set("$skiptoken", "next")
		u.RawQuery = q.Encode()
		var items []map[string]interface{}
		if late.Load() {
			items = []map[string]interface{}{item("late-risk", start.Add(time.Second))}
		}
		sendPage(w, items, u.String())
	})
	a, g := harness(t, server, &captureSink{})
	stream := a.streams[0]
	initial := g.position[stream.name]
	start, end := initial.Boundary, initial.WindowEnd
	requests := &graphRequests{}
	got, err := a.scanWindow(context.Background(), g, stream, start, end, false, requests)
	require.ErrorIs(t, err, errYield)
	require.Equal(t, start, got)
	require.Equal(t, int32(1), pages.Load())
	require.Equal(t, start, g.position[stream.name].Boundary)
	got, err = a.scanWindow(context.Background(), g, stream, start, end, false, requests)
	require.NoError(t, err)
	require.NoError(t, a.complete(g, stream, got, false))
	late.Store(true)
	// Published well after the old five-minute window, but inside retained coverage.
	a.runtime.now = func() time.Time { return time.Unix(0, end).Add(48 * time.Hour) }
	for range 2 {
		_, err = a.scanWindow(context.Background(), g, stream, start, end, true, requests)
	}
	require.NoError(t, err)
	require.Equal(t, 1, a.uspClient.(*captureSink).count())
	// Resume an unfinished traversal from its original bounds, never a stale nextLink.
	g.scans = make(map[string]*graphScan)
	_, err = a.scanWindow(context.Background(), g, stream, start, end, true, &graphRequests{})
	require.ErrorIs(t, err, errYield)
	key := stream.name + "/replay"
	require.Contains(t, g.scans[key].next, "skiptoken")
	p := g.position[stream.name]
	p.Replay, p.ReplayEnd = start, end
	g.position[stream.name] = p
	require.NoError(t, a.candidate(g))
	require.NoError(t, a.confirm(g))
	require.Equal(t, g.serial, g.confirmed.Load())
	require.Equal(t, start, g.position[stream.name].Replay)
	require.Equal(t, end, g.position[stream.name].ReplayEnd)
}

func TestEntraContinuousLoadConfirmsFrozenProbe(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := bounds(t, r)
		sendPage(w, []map[string]interface{}{item(start.String(), start)}, "")
	})
	sink := &captureSink{withheld: true}
	a, g := harness(t, server, sink)
	stream := a.streams[0]
	scan(t, a, g, stream)
	first := g.serial
	for range 12 {
		scan(t, a, g, stream)
	}
	sink.ack(1)
	require.NoError(t, a.confirm(g))
	require.Equal(t, first, g.confirmed.Load())
	// Producers keep submitting while the independent confirmer samples ACKs.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 50 {
			scan(t, a, g, stream)
		}
	}()
	for range 50 {
		sink.ack(10000)
		require.NoError(t, a.confirm(g))
	}
	wg.Wait()
	sink.ack(10000)
	require.NoError(t, a.confirm(g))
	require.Greater(t, g.confirmed.Load(), first)
}

type blockedSink struct {
	captureSink
	attempts          atomic.Int32
	active            atomic.Int32
	begin             atomic.Int32
	beginDuringActive atomic.Bool
	fail              bool
}

func (s *blockedSink) Ship(m *protocol.DataMessage, timeout time.Duration) error {
	n := s.attempts.Add(1)
	if n == 1 && !s.fail {
		return s.captureSink.Ship(m, timeout)
	}
	s.active.Add(1)
	defer s.active.Add(-1)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	<-timer.C
	return uspclient.ErrorBufferFull
}
func (s *blockedSink) BeginDrain() { s.beginDuringActive.Store(s.active.Load() > 0); s.begin.Add(1) }

func TestEntraAllCloudScopesAndStreamFlagsHTTP(t *testing.T) {
	for _, cloud := range []string{"enterprise", "gcc-gov", "gcc-high-gov", "dod-gov"} {
		for _, selection := range []string{"", "risk_detections", "sign_ins", "audit_logs", "risk_detections,sign_ins,audit_logs"} {
			t.Run(cloud+"/"+selection, func(t *testing.T) {
				var mu sync.Mutex
				paths := make(map[string]int)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodPost {
						require.NoError(t, r.ParseForm())
						require.Equal(t, environments[cloud].scope, r.Form.Get("scope"))
						require.Equal(t, testClientSecret, r.Form.Get("client_secret"))
						fmt.Fprint(w, `{"access_token":"fixture-bearer"}`)
						return
					}
					require.Equal(t, "Bearer fixture-bearer", r.Header.Get("Authorization"))
					bounds(t, r)
					mu.Lock()
					paths[r.URL.Path]++
					mu.Unlock()
					sendPage(w, []map[string]interface{}{item(r.URL.Path, fixtureBaseTime())}, "")
				}))
				defer server.Close()
				conf := testConfig(t, server.URL)
				conf.Endpoint = cloud
				conf.Streams = selection
				streams, err := conf.streams()
				require.NoError(t, err)
				sink := &captureSink{}
				a, _, err := newFixtureEntraIDAdapter(context.Background(), conf, sink)
				require.NoError(t, err)
				defer a.Close()
				require.Eventually(t, func() bool { return sink.count() == len(streams) }, 5*time.Second, time.Millisecond)
				mu.Lock()
				defer mu.Unlock()
				require.Len(t, paths, len(streams))
				for _, stream := range streams {
					require.Positive(t, paths[stream.path])
				}
			})
		}
	}
}

func TestEntraReconciliationYieldsToLiveAndHonorsCoverage(t *testing.T) {
	var livePages, replayPages atomic.Int32
	var clock, maxLiveEnd atomic.Int64
	clock.Store(fixtureBaseTime().UnixNano())
	base := fixtureBaseTime()
	replayFloor := base.Add(-72 * time.Hour).UnixNano()
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := bounds(t, r)
		clock.Add(int64(10 * time.Second))
		if start.Before(base) {
			replayPages.Add(1)
			u := *r.URL
			u.Scheme = "http"
			u.Host = r.Host
			q := u.Query()
			q.Set("page", fmt.Sprint(replayPages.Load()))
			u.RawQuery = q.Encode()
			sendPage(w, nil, u.String())
			return
		}
		livePages.Add(1)
		for old := maxLiveEnd.Load(); end.UnixNano() > old; old = maxLiveEnd.Load() {
			if maxLiveEnd.CompareAndSwap(old, end.UnixNano()) {
				break
			}
		}
		sendPage(w, nil, "")
	})
	a, g := harness(t, server, &captureSink{})
	a.streams = a.streams[:1]
	for name := range g.position {
		if name != a.streams[0].name {
			delete(g.position, name)
		}
	}
	a.runtime.now = func() time.Time { return time.Unix(0, clock.Load()) }
	a.runtime.reconcileInterval = 0
	a.runtime.window = time.Second
	a.runtime.replayWindow = time.Hour
	s := g.position[a.streams[0].name]
	s.Coverage = replayFloor
	s.WindowEnd = s.Boundary + int64(time.Second)
	g.position[a.streams[0].name] = s
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.collect(ctx, g) }()
	defer func() { cancel(); <-done }()
	require.Eventually(t, func() bool { return maxLiveEnd.Load() >= base.Add(5*time.Second).UnixNano() && replayPages.Load() >= 4 }, 5*time.Second, time.Millisecond)
	cancel()
	<-done
	require.GreaterOrEqual(t, g.position[a.streams[0].name].Boundary, base.Add(4*time.Second).UnixNano())
	require.Equal(t, replayFloor, g.position[a.streams[0].name].Replay)
	require.Equal(t, replayFloor, g.scans[a.streams[0].name+"/replay"].start)
	// Locally narrowed coverage does not permit broadening back to 72h.
	s = g.position[a.streams[0].name]
	s.Coverage = base.Add(-time.Minute).UnixNano()
	s.Replay, s.ReplayEnd = 0, 0
	g.position[a.streams[0].name] = s
	g.scans = make(map[string]*graphScan)
	a.runtime.now = func() time.Time { return base.Add(time.Minute) }
	priorReplay := replayPages.Load()
	ctx, cancel = context.WithCancel(context.Background())
	a.pollInterval = time.Millisecond
	done = make(chan struct{})
	go func() { defer close(done); a.collect(ctx, g) }()
	require.Eventually(t, func() bool { return replayPages.Load() > priorReplay }, 5*time.Second, time.Millisecond)
	cancel()
	<-done
	require.GreaterOrEqual(t, g.position[a.streams[0].name].Replay, s.Coverage)
}

func TestEntraLocalReplayClampsToCompleteIDCoverage(t *testing.T) {
	for _, age := range []time.Duration{time.Hour} {
		t.Run(age.String(), func(t *testing.T) {
			var called atomic.Bool
			base := fixtureBaseTime()
			coverage := base.Add(-age).UnixNano()
			expected := coverage
			if floor := base.Add(-72 * time.Hour).UnixNano(); floor > expected {
				expected = floor
			}
			server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
				start, _ := bounds(t, r)
				require.GreaterOrEqual(t, start.UnixNano(), expected, "local replay cannot escape complete unexpired coverage")
				called.Store(true)
				sendPage(w, nil, "")
			})
			a, g := harness(t, server, &captureSink{})
			a.streams = a.streams[:1]
			name := a.streams[0].name
			s := g.position[name]
			s.Coverage = coverage
			s.Replay = base.Add(-74 * time.Hour).UnixNano()
			s.ReplayEnd = s.Boundary
			g.position = entraPosition{name: s}
			a.runtime.reconcileInterval = 0
			a.runtime.now = func() time.Time { return base }
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); a.collect(ctx, g) }()
			require.Eventually(t, called.Load, 5*time.Second, time.Millisecond)
			cancel()
			<-done
			require.GreaterOrEqual(t, g.position[name].Replay, expected)
			require.GreaterOrEqual(t, g.position[name].Coverage, expected)
		})
	}
}

func TestEntraIncompleteOrInvalidPageNeverCommits(t *testing.T) {
	for _, kind := range []string{"full-terminal", "invalid-time", "off-origin"} {
		t.Run(kind, func(t *testing.T) {
			server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
				start, _ := bounds(t, r)
				switch kind {
				case "full-terminal":
					items := make([]map[string]interface{}, 1000)
					for i := range items {
						items[i] = item(fmt.Sprint(i), start)
					}
					sendPage(w, items, "")
				case "invalid-time":
					sendPage(w, []map[string]interface{}{{"id": "bad", "activityDateTime": "invalid"}}, "")
				case "off-origin":
					sendPage(w, []map[string]interface{}{item("first", start)}, "https://graph.microsoft.com.invalid/v1.0/identityProtection/riskDetections")
				}
			})
			a, g := harness(t, server, &captureSink{})
			stream := a.streams[0]
			p := g.position[stream.name]
			initial := g.confirmed.Load()
			end, err := a.scanWindow(context.Background(), g, stream, p.Boundary, p.WindowEnd, false, &graphRequests{})
			require.Error(t, err)
			require.Equal(t, p.Boundary, end)
			require.NoError(t, a.confirm(g))
			require.Equal(t, initial, g.confirmed.Load())
		})
	}
}

// Cancel blocked admission, join the collector and preserve the accepted SDK tail.
func TestEntraCanceledAdmissionAndCloseJoinWithoutReset(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := bounds(t, r)
		sendPage(w, []map[string]interface{}{item(start.String(), start)}, "")
	})
	rt := fixtureRuntime()
	rt.window = time.Minute
	rt.ackInterval = func() time.Duration { return 10 * time.Millisecond }
	sink := &blockedSink{}
	var internalErrors atomic.Int32
	conf := EntraIDConfig{ClientOptions: testClientOptions(t), TenantID: testTenantID, ClientID: testClientID, ClientSecret: "fixture&secret=encoded", GraphEndpoint: server.URL, LoginEndpoint: server.URL, PollInterval: time.Millisecond}
	conf.ClientOptions.OnError = func(error) { internalErrors.Add(1) }
	a, done, err := newEntraIDAdapterWithRuntime(context.Background(), conf, sink, rt)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sink.attempts.Load() > 1 && sink.active.Load() > 0 }, 5*time.Second, time.Millisecond)
	require.Zero(t, sink.begin.Load(), "collection cannot seal the client under pressure")
	before := time.Now()
	require.NoError(t, a.Close())
	require.Less(t, time.Since(before), time.Second)
	require.Equal(t, int32(1), sink.begin.Load())
	require.True(t, sink.beginDuringActive.Load(), "BeginDrain must precede joining the blocked producer")
	require.Zero(t, sink.active.Load())
	<-done
	require.Equal(t, 1, sink.count(), "accepted SDK history remains on the original client")
	require.Same(t, sink, a.uspClient)
	require.Zero(t, internalErrors.Load())
	b, g := harness(t, server, &blockedSink{fail: true})
	state := g.position[b.streams[0].name]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err = b.shipItem(ctx, g, b.streams[0], item("failed", time.Unix(0, state.Boundary)), state.Boundary, state.WindowEnd)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, g.live)
	require.NoError(t, b.confirm(g))
	require.Zero(t, g.confirmed.Load())
}
