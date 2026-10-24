package usp_entraid

import (
	"bytes"
	"context"
	"encoding/binary"
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
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type memoryStore struct {
	mu                sync.Mutex
	value             utils.Checkpoint
	found             bool
	loadErr, saveErr  error
	refuse, loseReply bool
	attempts          []utils.Checkpoint
}

func (s *memoryStore) Load(ctx context.Context) (utils.Checkpoint, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return utils.Checkpoint{}, false, ctx.Err()
	}
	return s.value.Clone(), s.found, s.loadErr
}
func (s *memoryStore) CompareAndSwap(ctx context.Context, f utils.CheckpointFence, n utils.Checkpoint) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	s.attempts = append(s.attempts, n.Clone())
	if s.refuse && !s.found {
		return false, utils.ErrCheckpointAllocationRefused
	}
	if s.saveErr != nil {
		return false, s.saveErr
	}
	if s.found && n.CheckpointFence == s.value.CheckpointFence && n.Attempt == s.value.Attempt && bytes.Equal(n.Position, s.value.Position) {
		return true, nil
	}
	if (!s.found && f != (utils.CheckpointFence{})) || (s.found && f != s.value.CheckpointFence) {
		return false, nil
	}
	s.value = n.Clone()
	s.found = true
	if s.loseReply {
		return false, errors.New("fixture store reply lost")
	}
	return true, nil
}
func (s *memoryStore) snapshot() utils.Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value.Clone()
}
func (s *memoryStore) setErrors(load, save error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadErr, s.saveErr = load, save
}

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
func harness(t *testing.T, server *httptest.Server, sink uspSink) (*EntraIDAdapter, *entraGeneration, *memoryStore) {
	t.Helper()
	rt := fixtureRuntime()
	rt.window = 5 * time.Minute
	rt.saveInterval = func() time.Duration { return 10 * time.Millisecond }
	conf := EntraIDConfig{ClientOptions: testClientOptions(t), TenantID: testTenantID, ClientID: testClientID, ClientSecret: "fixture&secret=encoded", LoginEndpoint: server.URL, GraphEndpoint: server.URL, Streams: "risk_detections,sign_ins,audit_logs"}
	streams, err := conf.streams()
	require.NoError(t, err)
	a := &EntraIDAdapter{conf: conf, streams: streams, httpClient: server.Client(), uspClient: sink, runtime: rt, startedAt: fixtureBaseTime().UnixNano(), pollInterval: time.Millisecond}
	a.tracker, err = utils.NewACKTracker(sink, 1, checkpointPositionLimit+16)
	require.NoError(t, err)
	store := &memoryStore{}
	session, raw, err := utils.OpenCheckpoint(context.Background(), store, 1, a.sourceFingerprint(), a.initialPosition, a.validatePosition, nil)
	require.NoError(t, err)
	p, err := decodePosition(raw)
	require.NoError(t, err)
	return a, newGeneration(1, session, p), store
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
	a, g, store := harness(t, server, sink)
	initial := store.snapshot().Position
	end := scan(t, a, g, a.streams[0])
	require.Equal(t, 3, sink.count())
	require.Equal(t, int32(2), pages.Load())
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, initial, store.snapshot().Position)
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
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, initial, store.snapshot().Position)
	sink.ack(3)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err := decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, end, p[a.streams[0].name].Boundary)
	require.Equal(t, first.Coverage, p[a.streams[1].name].Boundary)
	sink.ack(6)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err = decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, end, p[a.streams[1].name].Boundary)
	// An empty later interval still inherits its preceding pending admission.
	sink.mu.Lock()
	sink.withheld = true
	sink.mu.Unlock()
	s := g.position[a.streams[0].name]
	require.NoError(t, a.shipItem(context.Background(), g, a.streams[0], item("pending", time.Unix(0, s.Boundary)), s.Boundary, s.WindowEnd))
	require.NoError(t, a.complete(g, a.streams[0], s.WindowEnd, false))
	saved := store.snapshot().Position
	require.NoError(t, a.complete(g, a.streams[0], s.WindowEnd+int64(a.runtime.window), false))
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, saved, store.snapshot().Position)
}

func TestEntraReleaseOneBootstrapResumeSourceAndCredentials(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	a, g, store := harness(t, server, &captureSink{})
	initial := g.position[a.streams[0].name].Boundary
	require.Equal(t, a.startedAt, initial)
	scan(t, a, g, a.streams[0])
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	saved := store.snapshot()
	fingerprint := a.sourceFingerprint()
	a.conf.ClientID = "rotated-application"
	a.conf.ClientSecret = "rotated-secret"
	require.Equal(t, fingerprint, a.sourceFingerprint())
	session, raw, err := utils.OpenCheckpoint(context.Background(), store, 1, a.sourceFingerprint(), func() ([]byte, error) { t.Fatal("valid checkpoint must not bootstrap"); return nil, nil }, a.validatePosition, nil)
	require.NoError(t, err)
	require.NotNil(t, session)
	require.Equal(t, saved.Position, raw)
	a.conf.TenantID = "different-tenant"
	require.NotEqual(t, fingerprint, a.sourceFingerprint())
	initialRaw, err := a.initialPosition()
	require.NoError(t, err)
	_, replaced, err := utils.OpenCheckpoint(context.Background(), store, 1, a.sourceFingerprint(), a.initialPosition, a.validatePosition, nil)
	require.NoError(t, err)
	require.Equal(t, initialRaw, replaced, "a new source must replace the slot, not inherit the old source boundary")
	a.conf.TenantID = testTenantID
	a.conf.GraphEndpoint = server.URL + "/other"
	require.NotEqual(t, fingerprint, a.sourceFingerprint())
	a.conf.GraphEndpoint = server.URL
	a.streams = a.streams[:1]
	require.NotEqual(t, fingerprint, a.sourceFingerprint())
	// Go-only injection cannot become customer config, even with a live store.
	a.conf.CheckpointStore = store
	a.conf.CheckpointIssue = func(string) {}
	b, err := json.Marshal(a.conf)
	require.NoError(t, err)
	require.NotContains(t, string(b), "Checkpoint")
	b, err = yaml.Marshal(a.conf)
	require.NoError(t, err)
	require.NotContains(t, string(b), "checkpoint")
}

func TestEntraUnavailableBootstrapAndAllocationRefusal(t *testing.T) {
	for _, mode := range []string{"load", "claim", "corrupt", "refuse"} {
		t.Run(mode, func(t *testing.T) {
			refuse := mode == "refuse"
			mock := newMockMicrosoft()
			mock.addDetection(item("first", fixtureBaseTime()))
			server := mock.start(t)
			conf := testConfig(t, server.URL)
			store := &memoryStore{refuse: refuse}
			switch mode {
			case "load":
				store.loadErr = errors.New("fixture unavailable")
			case "claim":
				store.saveErr = errors.New("fixture unavailable")
			case "corrupt":
				streams, err := conf.streams()
				require.NoError(t, err)
				source := (&EntraIDAdapter{conf: conf, streams: streams}).sourceFingerprint()
				store.found = true
				store.value = utils.Checkpoint{SchemaVersion: 999, SourceFingerprint: source, Position: []byte{0}, CheckpointFence: utils.CheckpointFence{Revision: "old", Nonce: "old"}}
			}
			conf.CheckpointStore = store
			var issues, errorsSeen, warnings atomic.Int32
			conf.CheckpointIssue = func(string) { issues.Add(1) }
			conf.ClientOptions.OnError = func(error) { errorsSeen.Add(1) }
			conf.ClientOptions.OnWarning = func(string) { warnings.Add(1) }
			sink := &captureSink{}
			before := time.Now()
			a, done, err := newEntraIDAdapter(context.Background(), conf, sink)
			require.NoError(t, err)
			require.Less(t, time.Since(before), time.Second)
			require.Eventually(t, func() bool { return issues.Load() > 0 }, time.Second, time.Millisecond)
			if refuse {
				require.Eventually(t, func() bool { return sink.count() == 1 }, time.Second, time.Millisecond)
			} else {
				require.Zero(t, mock.tokenRequestCount())
				require.Zero(t, mock.graphRequestCount())
				require.Zero(t, sink.count())
			}
			require.NoError(t, a.Close())
			select {
			case <-done:
			default:
				t.Fatal("bootstrap did not join")
			}
			require.Zero(t, errorsSeen.Load())
			require.Zero(t, warnings.Load())
		})
	}
}

func TestEntraCompactSharedNewestSummaryAndLiveCache(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	sink := &captureSink{}
	a, g, store := harness(t, server, sink)
	for _, stream := range a.streams {
		s := g.position[stream.name]
		s.Boundary += int64(2 * time.Hour)
		s.WindowEnd = s.Boundary + int64(a.runtime.window)
		g.position[stream.name] = s
		for i := range 1100 {
			stamp := time.Unix(0, s.Coverage).Add(time.Duration(i) * time.Second)
			require.NoError(t, a.shipItem(context.Background(), g, stream, item(fmt.Sprintf("%s/%d", stream.name, i), stamp), s.Coverage, s.Boundary))
		}
	}
	require.Len(t, g.live, 3300)
	require.NoError(t, a.candidate(g))
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	raw := store.snapshot().Position
	require.LessOrEqual(t, len(raw)+37, 32*1024)
	p, err := decodePosition(raw)
	require.NoError(t, err)
	total := 0
	for _, stream := range a.streams {
		s := p[stream.name]
		total += len(s.IDs)
		require.Len(t, s.IDs, 341)
		require.Greater(t, s.Coverage, g.position[stream.name].Coverage)
		require.Equal(t, g.position[stream.name].Coverage, s.Unverified)
		require.Equal(t, eventDigest(a.sourceFingerprint(), stream.name, fmt.Sprintf("%s/%d", stream.name, 1099)), s.IDs[0].digest)
	}
	require.LessOrEqual(t, total, 1024)
	// Saved-summary eviction never evicts the running cache or causes poll replays.
	prior := sink.count()
	stream := a.streams[0]
	s := g.position[stream.name]
	require.NoError(t, a.shipItem(context.Background(), g, stream, item(stream.name+"/0", time.Unix(0, s.Coverage)), s.Coverage, s.Boundary))
	require.Equal(t, prior, sink.count())
	for n := range len(raw) {
		_, err := decodePosition(raw[:n])
		require.Error(t, err)
	}
	bad := append([]byte(nil), raw...)
	binary.BigEndian.PutUint64(bad[3:], 0)
	_, err = decodePosition(bad)
	require.Error(t, err)
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
	a, _, _ := harness(t, server, &captureSink{})
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
	a, _, _ := harness(t, server, &captureSink{})
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
	b, _, _ := harness(t, server, &captureSink{})
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
	a, g, _ := harness(t, server, &captureSink{})
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
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.NotContains(t, string(g.latest), "skiptoken")
	restored, err := decodePosition(g.latest)
	require.NoError(t, err)
	require.Equal(t, start, restored[stream.name].Replay)
	require.Equal(t, end, restored[stream.name].ReplayEnd)
}

func TestEntraUncertainSaveRetainsNewestConfirmed(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	a, g, store := harness(t, server, &captureSink{})
	stream := a.streams[0]
	first := scan(t, a, g, stream)
	store.setErrors(errors.New("unknown read"), errors.New("unknown CAS"))
	require.Error(t, a.saveConfirmed(context.Background(), g))
	frozen := append([]byte(nil), g.latest...)
	second := scan(t, a, g, stream)
	require.Greater(t, second, first)
	require.Error(t, a.saveConfirmed(context.Background(), g))
	latest := append([]byte(nil), g.latest...)
	require.NotEqual(t, frozen, latest)
	store.setErrors(nil, nil)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, frozen, store.snapshot().Position)
	require.Equal(t, latest, g.latest)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, latest, store.snapshot().Position)
	// Lost response after an applied write is resolved by exact readback.
	scan(t, a, g, stream)
	store.mu.Lock()
	store.loseReply = true
	store.mu.Unlock()
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, g.latest, store.snapshot().Position)
	// Applied CAS with both response and readback unavailable remains frozen.
	scan(t, a, g, stream)
	store.setErrors(errors.New("readback unavailable"), nil)
	require.Error(t, a.saveConfirmed(context.Background(), g))
	applied := store.snapshot()
	require.Equal(t, g.latest, applied.Position)
	scan(t, a, g, stream)
	store.setErrors(errors.New("readback unavailable"), errors.New("CAS unavailable"))
	require.Error(t, a.saveConfirmed(context.Background(), g))
	require.NotEqual(t, g.latest, applied.Position)
	store.setErrors(nil, nil)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, applied.Position, store.snapshot().Position)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, g.latest, store.snapshot().Position)
}

func TestEntraContinuousLoadConfirmsFrozenProbe(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := bounds(t, r)
		sendPage(w, []map[string]interface{}{item(start.String(), start)}, "")
	})
	sink := &captureSink{withheld: true}
	a, g, store := harness(t, server, sink)
	stream := a.streams[0]
	first := scan(t, a, g, stream)
	for range 12 {
		scan(t, a, g, stream)
	}
	sink.ack(1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err := decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, first, p[stream.name].Boundary)
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
		require.NoError(t, a.saveConfirmed(context.Background(), g))
	}
	wg.Wait()
	sink.ack(10000)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err = decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Greater(t, p[stream.name].Boundary, first)
}

type blockedSink struct {
	captureSink
	attempts atomic.Int32
	active   atomic.Int32
	begin    atomic.Int32
	fail     bool
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
func (s *blockedSink) BeginDrain() { s.begin.Add(1) }

type checkingStore struct {
	memoryStore
	sink     *blockedSink
	conflict atomic.Bool
	badJoin  atomic.Bool
	loads    atomic.Int32
}

func (s *checkingStore) Load(ctx context.Context) (utils.Checkpoint, bool, error) {
	if s.loads.Add(1) > 1 && s.sink.active.Load() != 0 {
		s.badJoin.Store(true)
	}
	return s.memoryStore.Load(ctx)
}
func (s *checkingStore) CompareAndSwap(ctx context.Context, f utils.CheckpointFence, n utils.Checkpoint) (bool, error) {
	if f != (utils.CheckpointFence{}) && s.sink.attempts.Load() > 1 && !s.conflict.Swap(true) {
		return false, nil
	}
	return s.memoryStore.CompareAndSwap(ctx, f, n)
}
func TestEntraConflictCancelsBlockedShipAndShutdownJoins(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := bounds(t, r)
		sendPage(w, []map[string]interface{}{item(start.String(), start)}, "")
	})
	rt := fixtureRuntime()
	rt.window = time.Minute
	rt.saveInterval = func() time.Duration { return 10 * time.Millisecond }
	sink := &blockedSink{}
	store := &checkingStore{sink: sink}
	conf := EntraIDConfig{ClientOptions: testClientOptions(t), TenantID: testTenantID, ClientID: testClientID, ClientSecret: "fixture&secret=encoded", GraphEndpoint: server.URL, LoginEndpoint: server.URL, PollInterval: time.Millisecond, CheckpointStore: store, runtime: &rt}
	var internalErrors atomic.Int32
	conf.ClientOptions.OnError = func(error) { internalErrors.Add(1) }
	a, done, err := newEntraIDAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return store.conflict.Load() }, 2*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return store.loads.Load() > 1 }, 2*time.Second, time.Millisecond)
	require.False(t, store.badJoin.Load())
	require.Zero(t, sink.begin.Load(), "ordinary ownership conflicts must not seal the SDK")
	before := time.Now()
	require.NoError(t, a.Close())
	require.Less(t, time.Since(before), time.Second)
	require.Equal(t, int32(1), sink.begin.Load())
	require.Zero(t, sink.active.Load())
	select {
	case <-done:
	default:
		t.Fatal("worker not joined")
	}
	require.Zero(t, internalErrors.Load())
	// A failed admission never enters live dedupe or certifies a window.
	b, g, _ := harness(t, server, &blockedSink{fail: true})
	s := g.position[b.streams[0].name]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err = b.shipItem(ctx, g, b.streams[0], item("failed", time.Unix(0, s.Boundary)), s.Boundary, s.WindowEnd)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, g.live)
}

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
				a, _, err := newEntraIDAdapter(context.Background(), conf, sink)
				require.NoError(t, err)
				defer a.Close()
				require.Eventually(t, func() bool { return sink.count() == len(streams) }, time.Second, time.Millisecond)
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

func TestEntraDefaultMissingAndNilStartAtConstructorBoundary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Graph must not run before its first fixed window closes")
	}))
	defer server.Close()
	conf := EntraIDConfig{ClientOptions: testClientOptions(t), TenantID: testTenantID, ClientID: testClientID, ClientSecret: testClientSecret, GraphEndpoint: server.URL, LoginEndpoint: server.URL, PollInterval: time.Millisecond}
	for _, withStore := range []bool{true, false} {
		t.Run(fmt.Sprint(withStore), func(t *testing.T) {
			store := &memoryStore{}
			if withStore {
				conf.CheckpointStore = store
			} else {
				conf.CheckpointStore = nil
			}
			before := time.Now().UnixNano()
			a, _, err := newEntraIDAdapter(context.Background(), conf, &captureSink{})
			require.NoError(t, err)
			after := time.Now().UnixNano()
			// Observe constructor's exact start field; for nil persistence initialPosition
			// uses precisely the same boundary rather than enabling historical bootstrap.
			raw, err := a.initialPosition()
			require.NoError(t, err)
			p, err := decodePosition(raw)
			require.NoError(t, err)
			boundary := p["risk_detections"].Boundary
			require.GreaterOrEqual(t, boundary, before-int64(graphTimeQuantum))
			require.LessOrEqual(t, boundary, after)
			require.Equal(t, int64(30*time.Second), p["risk_detections"].WindowEnd-boundary)
			if withStore {
				require.Eventually(t, func() bool { return store.snapshot().Revision != "" }, time.Second, time.Millisecond)
				require.Equal(t, raw, store.snapshot().Position)
			}
			require.NoError(t, a.Close())
		})
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
	a, g, _ := harness(t, server, &captureSink{})
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
	require.Eventually(t, func() bool { return maxLiveEnd.Load() >= base.Add(5*time.Second).UnixNano() && replayPages.Load() >= 4 }, time.Second, time.Millisecond)
	cancel()
	<-done
	require.GreaterOrEqual(t, g.position[a.streams[0].name].Boundary, base.Add(4*time.Second).UnixNano())
	require.Equal(t, replayFloor, g.position[a.streams[0].name].Replay)
	require.Equal(t, replayFloor, g.scans[a.streams[0].name+"/replay"].start)
	// A restored narrow summary does not permit broadening back to 72h.
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
	require.Eventually(t, func() bool { return replayPages.Load() > priorReplay }, time.Second, time.Millisecond)
	cancel()
	<-done
	require.GreaterOrEqual(t, g.position[a.streams[0].name].Replay, s.Coverage)
}

func TestEntraRestoredReplayClampsToCompleteIDCoverage(t *testing.T) {
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
				require.GreaterOrEqual(t, start.UnixNano(), expected, "restored replay cannot escape complete unexpired coverage")
				called.Store(true)
				sendPage(w, nil, "")
			})
			a, g, _ := harness(t, server, &captureSink{})
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
			require.Eventually(t, called.Load, time.Second, time.Millisecond)
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
			a, g, store := harness(t, server, &captureSink{})
			stream := a.streams[0]
			p := g.position[stream.name]
			initial := store.snapshot().Position
			end, err := a.scanWindow(context.Background(), g, stream, p.Boundary, p.WindowEnd, false, &graphRequests{})
			require.Error(t, err)
			require.Equal(t, p.Boundary, end)
			require.NoError(t, a.saveConfirmed(context.Background(), g))
			require.Equal(t, initial, store.snapshot().Position)
		})
	}
}

func TestEntraSummaryCoverageUsesRepresentableWindowBounds(t *testing.T) {
	base := fixtureBaseTime().UnixNano()
	s := streamPosition{Boundary: base + int64(time.Hour), WindowEnd: base + int64(2*time.Hour), Coverage: base}
	for i := range 1025 {
		var digest [16]byte
		binary.BigEndian.PutUint64(digest[:8], uint64(i))
		s.IDs = append(s.IDs, savedID{digest: digest, expiry: base + int64(i)*int64(graphTimeQuantum) + 1 + int64(historyHorizon)})
	}
	raw, err := encodePosition(entraPosition{"risk_detections": s})
	require.NoError(t, err)
	p, err := decodePosition(raw)
	require.NoError(t, err)
	require.Equal(t, base+int64(graphTimeQuantum), p["risk_detections"].Coverage)
	require.Len(t, p["risk_detections"].IDs, 1024)
	s.Coverage = base + 1
	_, err = encodePosition(entraPosition{"risk_detections": s})
	require.ErrorIs(t, err, utils.ErrCheckpointInvalid)
}

func TestEntraShutdownDrainIsNotACK(t *testing.T) {
	mock := newMockMicrosoft()
	mock.addDetection(item("unacknowledged", fixtureBaseTime()))
	server := mock.start(t)
	conf := testConfig(t, server.URL)
	store := &memoryStore{}
	conf.CheckpointStore = store
	var onErrors atomic.Int32
	conf.ClientOptions.OnError = func(error) { onErrors.Add(1) }
	sink := &captureSink{withheld: true}
	a, done, err := newEntraIDAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sink.count() == 1 }, time.Second, time.Millisecond)
	prior := store.snapshot().Position
	err = a.Close()
	var uncertain *utils.UnackedMessagesError
	require.ErrorAs(t, err, &uncertain)
	require.Equal(t, 1, uncertain.Count)
	require.Equal(t, prior, store.snapshot().Position, "successful Drain cannot certify an unACKed window")
	select {
	case <-done:
	default:
		t.Fatal("close returned before collection joined")
	}
	require.Zero(t, onErrors.Load())
}
