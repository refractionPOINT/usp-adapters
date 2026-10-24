package usp_defender

import (
	"bytes"
	"context"
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

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
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
		return false, errors.New("raw-private-store-error")
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

func fastRuntime(now time.Time) defenderRuntime {
	rt := defaultRuntime()
	rt.now = func() time.Time { return now }
	rt.liveSpacing = 0
	rt.replaySpacing = 0
	return rt
}
func vendorServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			fmt.Fprint(w, `{"access_token":"fixture-bearer"}`)
			return
		}
		require.Equal(t, "Bearer fixture-bearer", r.Header.Get("Authorization"))
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	return server
}
func harness(t *testing.T, server *httptest.Server, sink uspSink) (*DefenderAdapter, *defenderGeneration, *memoryStore) {
	t.Helper()
	start := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	endpoint, err := collectionEndpoint(server.URL + alertsPath)
	require.NoError(t, err)
	a := &DefenderAdapter{conf: DefenderConfig{ClientOptions: testClientOptions(t), TenantID: mockTenantID, ClientID: mockClientID, ClientSecret: mockClientSecret, TokenURL: server.URL + tokenPath}, collectionURL: endpoint, httpClient: server.Client(), uspClient: sink, runtime: fastRuntime(start), pollInterval: time.Millisecond}
	a.tracker, err = utils.NewACKTracker(sink, 1, checkpointPositionLimit+16)
	require.NoError(t, err)
	store := &memoryStore{}
	session, raw, err := utils.OpenCheckpoint(context.Background(), store, 1, a.sourceFingerprint(), a.initialPosition, func(b []byte) error { _, err := decodePosition(b); return err }, nil)
	require.NoError(t, err)
	p, err := decodePosition(raw)
	require.NoError(t, err)
	return a, newGeneration(1, session, p), store
}
func windowBounds(t *testing.T, r *http.Request) (time.Time, time.Time) {
	t.Helper()
	parts := strings.Split(strings.TrimPrefix(r.URL.Query().Get("$filter"), "createdDateTime ge "), " and createdDateTime lt ")
	require.Len(t, parts, 2)
	start, err := time.Parse(time.RFC3339Nano, parts[0])
	require.NoError(t, err)
	end, err := time.Parse(time.RFC3339Nano, parts[1])
	require.NoError(t, err)
	return start, end
}
func sendPage(w http.ResponseWriter, items []map[string]interface{}, next string) {
	if items == nil {
		items = []map[string]interface{}{}
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"value": items, "@odata.nextLink": next})
}
func scan(t *testing.T, a *DefenderAdapter, g *defenderGeneration) int64 {
	t.Helper()
	end, err := a.scanWindow(context.Background(), g, g.position.Boundary, g.position.WindowEnd, false, &graphRequests{})
	require.NoError(t, err)
	require.NoError(t, a.complete(g, end, false))
	return end
}

func TestDefenderNewestFirstTiesPaginationAndWholeWindowACK(t *testing.T) {
	var calls atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		start, end := windowBounds(t, r)
		if r.URL.Query().Get("$skiptoken") == "second" {
			sendPage(w, []map[string]interface{}{realisticAlert("older-tie-b", start.Add(time.Second)), realisticAlert("older-tie-c", start.Add(time.Second))}, "")
			return
		}
		next := *r.URL
		next.Scheme = "http"
		next.Host = r.Host
		q := next.Query()
		q.Set("$skiptoken", "second")
		next.RawQuery = q.Encode()
		require.Equal(t, int32(1), n)
		sendPage(w, []map[string]interface{}{realisticAlert("newest", end.Add(-time.Second)), realisticAlert("older-tie-a", start.Add(time.Second))}, next.String())
	})
	sink := &captureSink{withheld: true}
	a, g, store := harness(t, server, sink)
	initial := store.snapshot().Position
	end := scan(t, a, g)
	require.Equal(t, 2, int(calls.Load()))
	require.Equal(t, 4, sink.count())
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, initial, store.snapshot().Position)
	sink.ack(3)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, initial, store.snapshot().Position, "partial page ACK cannot complete an interval")
	sink.ack(4)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err := decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, end, p.Boundary)
	require.Len(t, p.IDs, 4)
}

func TestDefenderFullTerminalPageSplitsWithoutSkippingTies(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := windowBounds(t, r)
		base := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
		var items []map[string]interface{}
		for i := 1199; i >= 0; i-- {
			created := base.Add(time.Minute)
			if i >= 600 {
				created = base.Add(4 * time.Minute)
			}
			if !created.Before(start) && created.Before(end) {
				items = append(items, realisticAlert(fmt.Sprint(i), created))
			}
		}
		if len(items) > 1000 {
			items = items[:1000]
		}
		sendPage(w, items, "")
	})
	sink := &captureSink{}
	a, g, store := harness(t, server, sink)
	originalEnd := g.position.WindowEnd
	end := scan(t, a, g)
	require.Less(t, end, originalEnd)
	require.Equal(t, originalEnd, g.position.WindowEnd, "retain fixed incomplete upper bound")
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err := decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, originalEnd, p.WindowEnd)
	scan(t, a, g)
	require.Equal(t, 1200, sink.count())
	require.Equal(t, originalEnd, g.position.Boundary)
}
func TestDefenderIndivisibleFullTieStallsWithoutCandidate(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := windowBounds(t, r)
		items := make([]map[string]interface{}, 1000)
		for i := range items {
			items[i] = realisticAlert(fmt.Sprint(i), start)
		}
		sendPage(w, items, "")
	})
	sink := &captureSink{}
	a, g, store := harness(t, server, sink)
	start := g.position.Boundary
	initial := store.snapshot().Position
	end, err := a.scanWindow(context.Background(), g, start, start+1, false, &graphRequests{})
	require.ErrorIs(t, err, errIncomplete)
	require.Equal(t, start, end)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, initial, store.snapshot().Position)
}
func TestDefenderUTCHalfOpenAndNoUpdateOnlyEmission(t *testing.T) {
	original := time.Local
	time.Local = time.FixedZone("fixture-west", -7*3600)
	t.Cleanup(func() { time.Local = original })
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := windowBounds(t, r)
		require.Equal(t, "Z", string(r.URL.Query().Get("$filter")[len(r.URL.Query().Get("$filter"))-1]))
		old := realisticAlert("only-updated", start.Add(-time.Hour))
		old["lastUpdateDateTime"] = start.Add(time.Second).Format(time.RFC3339Nano)
		sendPage(w, []map[string]interface{}{old, realisticAlert("lower-inclusive", start), realisticAlert("upper-exclusive", end)}, "")
	})
	sink := &captureSink{}
	a, g, _ := harness(t, server, sink)
	local := time.Unix(0, g.position.Boundary).In(time.Local)
	a.runtime.now = func() time.Time { return local }
	raw, err := a.initialPosition()
	require.NoError(t, err)
	p, err := decodePosition(raw)
	require.NoError(t, err)
	require.Equal(t, local.UnixNano(), p.Boundary)
	require.Contains(t, a.windowURL(g.position.Boundary, g.position.WindowEnd), url.QueryEscape(time.Unix(0, g.position.Boundary).UTC().Format(graphTimeLayout)))
	scan(t, a, g)
	require.Equal(t, 1, sink.count())
	require.Equal(t, "lower-inclusive", sink.snapshot()[0].JsonPayload["id"])
}
func TestDefenderPendingDedupEmptyWindowsAndContinuousTail(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	sink := &captureSink{withheld: true}
	a, g, store := harness(t, server, sink)
	base := g.position.Boundary
	first := realisticAlert("first", time.Unix(0, base+1))
	require.NoError(t, a.shipItem(context.Background(), g, first, base, base+10))
	require.NoError(t, a.shipItem(context.Background(), g, first, base, base+10))
	require.Equal(t, 1, sink.count())
	require.NoError(t, a.complete(g, base+10, false))
	firstBoundary := g.position.Boundary
	require.NoError(t, a.complete(g, base+20, false)) // empty inherits previous admission
	second := realisticAlert("second", time.Unix(0, base+21))
	require.NoError(t, a.shipItem(context.Background(), g, second, base+20, base+30))
	require.NoError(t, a.complete(g, base+30, false))
	sink.ack(1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err := decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, firstBoundary, p.Boundary, "frozen probe progresses with an unACKed continuous tail")
	sink.ack(2)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err = decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, base+30, p.Boundary)
}

func TestDefenderSaveFailureRetainsConfirmedAndFrozenAttempt(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	a, g, store := harness(t, server, &captureSink{})
	var onError atomic.Int32
	a.conf.ClientOptions.OnError = func(error) { onError.Add(1) }
	first := scan(t, a, g)
	initial := store.snapshot().Position
	store.setErrors(errors.New("sensitive-read"), errors.New("sensitive-write"))
	require.Error(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, initial, store.snapshot().Position)
	second := scan(t, a, g)
	require.Greater(t, second, first)
	store.setErrors(nil, nil)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err := decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, first, p.Boundary, "resolve exact frozen older write first")
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err = decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, second, p.Boundary)
	require.Zero(t, onError.Load())
	store.mu.Lock()
	store.loseReply = true
	store.mu.Unlock()
	scan(t, a, g)
	require.NoError(t, a.saveConfirmed(context.Background(), g), "exact readback resolves lost reply")
}
func TestDefenderConflictDiscardsOldCandidatesKeepsSequenceHistory(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	sink := &captureSink{withheld: true}
	a, old, store := harness(t, server, sink)
	initial := store.snapshot().Position
	scan(t, a, old)
	successor, raw, err := utils.OpenCheckpoint(context.Background(), store, 1, a.sourceFingerprint(), a.initialPosition, func([]byte) error { return nil }, nil)
	require.NoError(t, err)
	require.ErrorIs(t, a.saveConfirmed(context.Background(), old), utils.ErrCheckpointConflict)
	p, err := decodePosition(raw)
	require.NoError(t, err)
	g := newGeneration(2, successor, p)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, initial, store.snapshot().Position, "consumed stale confirmation cannot rebase")
	base := g.position.Boundary
	require.NoError(t, a.shipItem(context.Background(), g, realisticAlert("after-conflict", time.Unix(0, base+1)), base, base+2))
	require.NoError(t, a.complete(g, base+2, false))
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, initial, store.snapshot().Position)
	sink.ack(1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err = decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, base+2, p.Boundary)
}

func TestDefenderEncodedCapNewestSummaryAndLiveIndependence(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	a, g, _ := harness(t, server, &captureSink{withheld: true})
	base := g.position.Boundary
	for i := 0; i < 300; i++ {
		require.NoError(t, a.shipItem(context.Background(), g, realisticAlert(fmt.Sprint(i), time.Unix(0, base+int64(i)+1)), base, base+1000))
	}
	g.position.Boundary = base + 1000
	g.position.IDs = g.recent
	raw, err := encodePosition(g.position)
	require.NoError(t, err)
	require.LessOrEqual(t, len(raw), 8128)
	require.LessOrEqual(t, len(raw)+37, 8192)
	p, err := decodePosition(raw)
	require.NoError(t, err)
	require.Len(t, p.IDs, 256)
	require.Len(t, g.live, 300)
	require.Equal(t, base+300+int64(historyHorizon), p.IDs[0].expiry)
	require.Equal(t, base+45, p.Coverage, "complete suffix excludes oldest omitted timestamp")
	for i := 0; i < 300; i++ {
		require.NoError(t, a.shipItem(context.Background(), g, realisticAlert(fmt.Sprint(i), time.Unix(0, base+int64(i)+1)), base, base+1000))
	}
	require.Equal(t, 300, a.uspClient.(*captureSink).count(), "saved cap must not evict pending/live IDs")
	_, err = decodePosition(make([]byte, 8129))
	require.Error(t, err)
}

func TestDefenderBootstrapR1MissingNilValidUnavailableAndGenerations(t *testing.T) {
	now := time.Date(2026, 2, 3, 4, 0, 0, 0, time.UTC)
	var requests atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { requests.Add(1); sendPage(w, nil, "") })
	config := func() DefenderConfig {
		return DefenderConfig{ClientOptions: testClientOptions(t), TenantID: mockTenantID, ClientID: mockClientID, ClientSecret: mockClientSecret, TokenURL: server.URL + tokenPath, AlertsURL: server.URL + alertsPath, PollInterval: time.Millisecond}
	}
	sourceAdapter, _, _ := harness(t, server, &captureSink{})
	source := sourceAdapter.sourceFingerprint()
	for _, mode := range []string{"nil", "missing", "valid", "unavailable", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			conf := config()
			store := &memoryStore{}
			old := now.Add(-time.Hour).UnixNano()
			if mode == "valid" || mode == "corrupt" {
				raw, err := encodePosition(defenderPosition{Boundary: old, WindowEnd: old + int64(5*time.Minute), Coverage: old})
				require.NoError(t, err)
				if mode == "corrupt" {
					raw = []byte("invalid")
				}
				store.found = true
				store.value = utils.Checkpoint{SchemaVersion: 1, SourceFingerprint: source, Position: raw, CheckpointFence: utils.CheckpointFence{Nonce: "old", Revision: "old"}}
			}
			if mode == "unavailable" {
				store.loadErr = errors.New("private-source-error")
			}
			if mode != "nil" {
				conf.CheckpointStore = store
			}
			var onError atomic.Int32
			conf.ClientOptions.OnError = func(error) { onError.Add(1) }
			before := requests.Load()
			started := time.Now()
			a, _, err := newDefenderAdapterWithRuntime(context.Background(), conf, &captureSink{}, fastRuntime(now))
			require.NoError(t, err)
			require.Less(t, time.Since(started), 100*time.Millisecond)
			if mode == "valid" {
				require.Eventually(t, func() bool { return requests.Load() > before }, time.Second, time.Millisecond)
			} else {
				time.Sleep(30 * time.Millisecond)
				require.Equal(t, before, requests.Load(), "no fallback collection on unavailable or fresh state")
			}
			require.NoError(t, a.Close())
			require.Zero(t, onError.Load())
			if mode == "missing" {
				p, err := decodePosition(store.snapshot().Position)
				require.NoError(t, err)
				require.Equal(t, now.Truncate(graphTimeQuantum).UnixNano(), p.Boundary)
			}
			if mode == "unavailable" {
				require.Empty(t, store.snapshot().Position)
			}
		})
	}
	first := sourceAdapter.sourceFingerprint()
	sourceAdapter.conf.ClientID = "rotated-client"
	sourceAdapter.conf.ClientSecret = "rotated-secret"
	sourceAdapter.conf.TokenURL = "https://login.microsoftonline.us/fixture/token"
	require.Equal(t, first, sourceAdapter.sourceFingerprint())
	sourceAdapter.conf.TenantID = "different-tenant"
	require.NotEqual(t, first, sourceAdapter.sourceFingerprint())
	sourceAdapter.conf.TenantID = mockTenantID
	sourceAdapter.collectionURL, _ = collectionEndpoint("https://graph.microsoft.us/v1.0/security/alerts_v2")
	require.NotEqual(t, first, sourceAdapter.sourceFingerprint())
	// Replacing source inside the stable store slot starts R1 at now, without
	// retaining separate source-key generations that a switch-back resurrects.
	store := &memoryStore{}
	open := func() []byte {
		_, raw, err := utils.OpenCheckpoint(context.Background(), store, 1, sourceAdapter.sourceFingerprint(), sourceAdapter.initialPosition, func(b []byte) error { _, e := decodePosition(b); return e }, nil)
		require.NoError(t, err)
		return raw
	}
	_ = open()
	sourceAdapter.conf.TenantID = "another-source"
	raw := open()
	p, err := decodePosition(raw)
	require.NoError(t, err)
	require.Equal(t, sourceAdapter.runtime.now().Truncate(graphTimeQuantum).UnixNano(), p.Boundary)
}

func TestDefenderAllocationRefusalOnlyMetricAndGoOnlyConfig(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := windowBounds(t, r)
		sendPage(w, []map[string]interface{}{realisticAlert("new", start.Add(time.Second))}, "")
	})
	var calls atomic.Int32
	now := time.Now()
	rt := fastRuntime(now)
	rt.now = func() time.Time {
		if calls.Add(1) == 1 {
			return now
		}
		return now.Add(10 * time.Minute)
	}
	store := &memoryStore{refuse: true}
	var warnings, errorsSeen, refusals atomic.Int32
	conf := DefenderConfig{ClientOptions: testClientOptions(t), TenantID: mockTenantID, ClientID: mockClientID, ClientSecret: mockClientSecret, TokenURL: server.URL + tokenPath, AlertsURL: server.URL + alertsPath, PollInterval: time.Millisecond, CheckpointStore: store, CheckpointIssue: func(code string) {
		if code == "allocation_refused" {
			refusals.Add(1)
		}
	}}
	conf.ClientOptions.OnWarning = func(string) { warnings.Add(1) }
	conf.ClientOptions.OnError = func(error) { errorsSeen.Add(1) }
	encoded, err := json.Marshal(conf)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "Checkpoint")
	require.NotContains(t, string(encoded), "checkpoint")
	sink := &captureSink{}
	a, _, err := newDefenderAdapterWithRuntime(context.Background(), conf, sink, rt)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sink.count() > 0 }, time.Second, time.Millisecond)
	require.NoError(t, a.Close())
	require.Equal(t, int32(1), refusals.Load())
	require.Zero(t, warnings.Load())
	require.Zero(t, errorsSeen.Load())
	require.False(t, store.found)
}

func TestDefenderRedirectContinuationAndErrorSanitization(t *testing.T) {
	var leaked atomic.Int32
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1); fmt.Fprint(w, `{"value":[]}`) }))
	defer evil.Close()
	for _, mode := range []string{"link", "redirect", "token-redirect", "credential"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					if mode == "token-redirect" {
						http.Redirect(w, r, evil.URL, 302)
					} else {
						fmt.Fprint(w, `{"access_token":"never-leak"}`)
					}
					return
				}
				if mode == "link" {
					sendPage(w, nil, evil.URL+alertsPath)
				} else if mode == "redirect" {
					http.Redirect(w, r, evil.URL, 302)
				} else {
					w.WriteHeader(403)
					fmt.Fprint(w, `{"error":"secret-response-payload"}`)
				}
			}))
			defer server.Close()
			a, g, _ := harness(t, server, &captureSink{})
			var messages []string
			a.conf.ClientOptions.OnError = func(e error) { messages = append(messages, e.Error()) }
			a.conf.ClientOptions.OnWarning = func(s string) { messages = append(messages, s) }
			_, err := a.scanWindow(context.Background(), g, g.position.Boundary, g.position.WindowEnd, false, &graphRequests{})
			require.Error(t, err)
			for _, message := range messages {
				require.NotContains(t, message, "http")
				require.NotContains(t, message, "never-leak")
				require.NotContains(t, message, "secret-response-payload")
			}
		})
	}
	require.Zero(t, leaked.Load())
	// Opaque valid continuation spelling is retained, including test overrides.
	a, _, _ := harness(t, vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") }), &captureSink{})
	require.NoError(t, a.validateContinuation(a.collectionURL.String()+"?$skiptoken=a%2Bb"))
	require.Error(t, a.validateContinuation(a.collectionURL.String()+"/other?$skiptoken=x"))
}

func TestDefenderRetryAfterAndCancellationWithoutOnError(t *testing.T) {
	requested := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(429)
		select {
		case requested <- struct{}{}:
		default:
		}
	}))
	defer server.Close()
	a, g, _ := harness(t, server, &captureSink{})
	// Exercise cancellation during a substantial scheduler cooldown, rather
	// than a millisecond poll that would hide a non-cancellable wait.
	a.pollInterval = 2 * time.Second
	var onError atomic.Int32
	a.conf.ClientOptions.OnError = func(error) { onError.Add(1) }
	retrySeen := make(chan struct{}, 1)
	a.conf.CheckpointIssue = func(code string) {
		if code == "vendor_retry" {
			select {
			case retrySeen <- struct{}{}:
			default:
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	base := time.Unix(0, g.position.Boundary)
	a.runtime.now = func() time.Time { return base.Add(6 * time.Minute) }
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.collect(ctx, g)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
	}()
	<-requested
	<-retrySeen
	// Let the yielded request reach its rate/backoff scheduler wait.
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retry wait ignored cancellation")
	}
	require.Zero(t, onError.Load())
	require.Equal(t, 120*time.Second, retryAfter("120", time.Now()))
	now := time.Now().UTC().Truncate(time.Second)
	require.Equal(t, 2*time.Minute, retryAfter(now.Add(2*time.Minute).Format(http.TimeFormat), now))
}
func TestDefenderTransientEscalatesOnlyContinuousFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	a, g, _ := harness(t, server, &captureSink{})
	var onError atomic.Int32
	a.conf.ClientOptions.OnError = func(error) { onError.Add(1) }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	requests := &graphRequests{failureStart: time.Now().Add(-61 * time.Minute)}
	_, err := a.scanWindow(ctx, g, g.position.Boundary, g.position.WindowEnd, false, requests)
	require.Error(t, err)
	require.Equal(t, int32(1), onError.Load())
}

type blockedSink struct {
	captureSink
	entered  chan struct{}
	released chan struct{}
	once     sync.Once
}

func (s *blockedSink) Ship(*protocol.DataMessage, time.Duration) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.released
	return errors.New("closed admission")
}
func (s *blockedSink) BeginDrain() { s.once.Do(func() { close(s.released) }) }
func TestDefenderShutdownBeginDrainBeforeJoiningBlockedProducer(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := windowBounds(t, r)
		sendPage(w, []map[string]interface{}{realisticAlert("blocked", start.Add(time.Second))}, "")
	})
	now := time.Now()
	var clockCalls atomic.Int32
	rt := fastRuntime(now)
	rt.now = func() time.Time {
		if clockCalls.Add(1) == 1 {
			return now
		}
		return now.Add(10 * time.Minute)
	}
	store := &memoryStore{}
	sink := &blockedSink{entered: make(chan struct{}, 1), released: make(chan struct{})}
	a, _, err := newDefenderAdapterWithRuntime(context.Background(), DefenderConfig{ClientOptions: testClientOptions(t), TenantID: mockTenantID, ClientID: mockClientID, ClientSecret: mockClientSecret, TokenURL: server.URL + tokenPath, AlertsURL: server.URL + alertsPath, PollInterval: time.Millisecond, CheckpointStore: store}, sink, rt)
	require.NoError(t, err)
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("producer never entered Ship")
	}
	initial := store.snapshot().Position
	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close joined a blocked producer before BeginDrain")
	}
	require.Equal(t, initial, store.snapshot().Position, "failed Ship is not delivered")
}

type drainACKSink struct {
	captureSink
	store   *memoryStore
	atClose utils.Checkpoint
}

func (s *drainACKSink) BeginDrain()               {}
func (s *drainACKSink) Drain(time.Duration) error { s.ack(^uint64(0)); return nil }
func (s *drainACKSink) Close() ([]*protocol.DataMessage, error) {
	s.atClose = s.store.snapshot()
	return nil, nil
}
func TestDefenderShutdownFinalConfirmAfterDrainBeforeClose(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := windowBounds(t, r)
		sendPage(w, []map[string]interface{}{realisticAlert("ack-on-drain", start.Add(time.Second))}, "")
	})
	now := time.Now()
	var calls atomic.Int32
	rt := fastRuntime(now)
	rt.now = func() time.Time {
		if calls.Add(1) == 1 {
			return now
		}
		return now.Add(10 * time.Minute)
	}
	store := &memoryStore{}
	sink := &drainACKSink{captureSink: captureSink{withheld: true}, store: store}
	a, _, err := newDefenderAdapterWithRuntime(context.Background(), DefenderConfig{ClientOptions: testClientOptions(t), TenantID: mockTenantID, ClientID: mockClientID, ClientSecret: mockClientSecret, TokenURL: server.URL + tokenPath, AlertsURL: server.URL + alertsPath, PollInterval: time.Millisecond, CheckpointStore: store}, sink, rt)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sink.count() == 1 }, time.Second, time.Millisecond)
	// Ensure the collector returned the page and installed its candidate.
	require.Eventually(t, func() bool { c, err := a.tracker.Confirm(); return err == nil && len(c) == 0 }, time.Second, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, a.Close())
	p, err := decodePosition(sink.atClose.Position)
	require.NoError(t, err)
	require.Equal(t, now.Truncate(graphTimeQuantum).Add(5*time.Minute).UnixNano(), p.Boundary)
}

type gatedAdmissionSink struct {
	captureSink
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
	fail    atomic.Bool
}

func (s *gatedAdmissionSink) Ship(m *protocol.DataMessage, d time.Duration) error {
	s.calls.Add(1)
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release
	if s.fail.Load() {
		return errors.New("fixture admission refused")
	}
	return s.captureSink.Ship(m, d)
}
func TestDefenderPendingAdmissionWaitAndFailedShipNotSuppressed(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := windowBounds(t, r)
		item := realisticAlert("same-id", start.Add(time.Second))
		sendPage(w, []map[string]interface{}{item, item}, "")
	})
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			sink := &gatedAdmissionSink{captureSink: captureSink{withheld: true}, entered: make(chan struct{}, 2), release: make(chan struct{})}
			sink.fail.Store(fail)
			a, g, store := harness(t, server, sink)
			initial := store.snapshot().Position
			done := make(chan error, 1)
			go func() {
				end, err := a.scanWindow(context.Background(), g, g.position.Boundary, g.position.WindowEnd, false, &graphRequests{})
				if err == nil {
					err = a.complete(g, end, false)
				}
				done <- err
			}()
			<-sink.entered
			require.Equal(t, int32(1), sink.calls.Load())
			require.NoError(t, a.saveConfirmed(context.Background(), g))
			require.Equal(t, initial, store.snapshot().Position)
			close(sink.release)
			err := <-done
			if fail {
				require.Error(t, err)
				require.Empty(t, g.live)
				require.NoError(t, a.saveConfirmed(context.Background(), g))
				require.Equal(t, initial, store.snapshot().Position)
				sink.fail.Store(false)
				scan(t, a, g)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, sink.count(), "failed or pending admission cannot mark the ID delivered")
			require.NoError(t, a.saveConfirmed(context.Background(), g))
			require.Equal(t, initial, store.snapshot().Position)
			sink.ack(1)
			require.NoError(t, a.saveConfirmed(context.Background(), g))
			require.NotEqual(t, initial, store.snapshot().Position)
		})
	}
}

func TestDefenderCountShortfallAndIncompleteRestartKeepBounds(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := windowBounds(t, r)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"value": []map[string]interface{}{realisticAlert("one", start)}, "@odata.count": 2})
	})
	a, g, store := harness(t, server, &captureSink{})
	start := g.position.Boundary
	initial := store.snapshot().Position
	_, err := a.scanWindow(context.Background(), g, start, start+1, false, &graphRequests{})
	require.ErrorIs(t, err, errIncomplete)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, initial, store.snapshot().Position)
	p, err := decodePosition(initial)
	require.NoError(t, err)
	require.Equal(t, g.position.WindowEnd, p.WindowEnd)
}

func TestDefenderReconciliationRestoresOnlyCompleteSuffixAndKeepsLag(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	a, g, _ := harness(t, server, &captureSink{})
	base := g.position.Boundary
	g.position.Boundary += int64(time.Hour)
	g.position.WindowEnd += int64(time.Hour)
	g.position.Replay = base
	g.position.ReplayEnd = g.position.Boundary
	for i := 0; i < 300; i++ {
		g.remember(savedID{digest: eventDigest(a.sourceFingerprint(), fmt.Sprint(i)), expiry: base + int64(i+1) + int64(historyHorizon)})
	}
	g.position.IDs = g.recent
	raw, err := encodePosition(g.position)
	require.NoError(t, err)
	p, err := decodePosition(raw)
	require.NoError(t, err)
	require.Equal(t, base, p.Replay, "necessary requested bounds are never truncated to fit IDs")
	require.Equal(t, base, p.Unverified)
	require.Greater(t, p.Coverage, p.Replay)
	restored := newGeneration(2, nil, p)
	a.runtime.now = func() time.Time { return time.Unix(0, p.Boundary) }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.collect(ctx, restored) }()
	require.Eventually(t, func() bool { confirmed, err := a.tracker.Confirm(); return err == nil && len(confirmed) > 0 }, time.Second, time.Millisecond)
	cancel()
	<-done
	require.Equal(t, base, restored.position.Unverified, "older unverified coverage remains explicit after the safe suffix scan")
}

func TestDefenderSaveRetryWithoutNewCandidate(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	a, g, store := harness(t, server, &captureSink{})
	boundary := scan(t, a, g)
	store.setErrors(errors.New("read unavailable"), errors.New("save unavailable"))
	require.Error(t, a.saveConfirmed(context.Background(), g))
	store.setErrors(nil, nil)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	p, err := decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, boundary, p.Boundary, "consumed confirmation remains dirty without another source candidate")
}

func TestDefenderConflictRejectsDelayedOldGenerationConfirmation(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	sink := &captureSink{withheld: true}
	a, old, store := harness(t, server, sink)
	initial := store.snapshot().Position
	base := old.position.Boundary
	for i := 0; i < 2; i++ {
		start := base + int64(i)*10
		require.NoError(t, a.shipItem(context.Background(), old, realisticAlert(fmt.Sprint(i), time.Unix(0, start+1)), start, start+10))
		require.NoError(t, a.complete(old, start+10, false))
	}
	session, raw, err := utils.OpenCheckpoint(context.Background(), store, 1, a.sourceFingerprint(), a.initialPosition, func([]byte) error { return nil }, nil)
	require.NoError(t, err)
	sink.ack(1)
	require.ErrorIs(t, a.saveConfirmed(context.Background(), old), utils.ErrCheckpointConflict)
	p, err := decodePosition(raw)
	require.NoError(t, err)
	fresh := newGeneration(2, session, p)
	sink.ack(2)
	require.NoError(t, a.saveConfirmed(context.Background(), fresh))
	require.Equal(t, initial, store.snapshot().Position, "old probe confirmed after takeover must be discarded")
	require.NoError(t, a.shipItem(context.Background(), fresh, realisticAlert("fresh", time.Unix(0, base+1)), base, base+10))
	require.NoError(t, a.complete(fresh, base+10, false))
	require.NoError(t, a.saveConfirmed(context.Background(), fresh))
	require.Equal(t, initial, store.snapshot().Position)
	sink.ack(3)
	require.NoError(t, a.saveConfirmed(context.Background(), fresh))
	p, err = decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, base+10, p.Boundary)
}

func TestDefenderTimestampPrecisionRoundTripsAndSplitIsRepresentable(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	alertTime := base.Add(2 * graphTimeQuantum)
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, end := windowBounds(t, r)
		var items []map[string]interface{}
		if !alertTime.Before(start) && alertTime.Before(end) {
			for i := 0; i < 1000; i++ {
				items = append(items, realisticAlert(fmt.Sprint(i), alertTime))
			}
		}
		sendPage(w, items, "")
	})
	a, g, _ := harness(t, server, &captureSink{})
	a.startedAt = base.Add(123456789 * time.Nanosecond).UnixNano()
	raw, err := a.initialPosition()
	require.NoError(t, err)
	p, err := decodePosition(raw)
	require.NoError(t, err)
	start, end := windowBounds(t, httptest.NewRequest(http.MethodGet, a.windowURL(p.Boundary, p.WindowEnd), nil))
	require.Equal(t, p.Boundary, start.UnixNano())
	require.Equal(t, p.WindowEnd, end.UnixNano())
	split, err := a.scanWindow(context.Background(), g, base.UnixNano(), base.Add(3*graphTimeQuantum).UnixNano(), false, &graphRequests{})
	require.NoError(t, err)
	require.Equal(t, base.Add(graphTimeQuantum).UnixNano(), split, "saved split must equal the query's exact representable bound")
}

func TestDefenderPageBudgetYieldsAndResumesEveryOpaqueNextLink(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(fmt.Sprint(replay), func(t *testing.T) {
			budget := 24
			if replay {
				budget = 6
			}
			total := budget + 3
			var calls atomic.Int32
			server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				start, _ := windowBounds(t, r)
				n := 0
				fmt.Sscan(r.URL.Query().Get("$skiptoken"), &n)
				next := ""
				if n+1 < total {
					u := *r.URL
					u.Scheme = "http"
					u.Host = r.Host
					q := u.Query()
					q.Set("$skiptoken", fmt.Sprint(n+1))
					u.RawQuery = q.Encode()
					next = u.String()
				}
				sendPage(w, []map[string]interface{}{realisticAlert(fmt.Sprint(n), start.Add(time.Second))}, next)
			})
			sink := &captureSink{}
			a, g, store := harness(t, server, sink)
			initial := store.snapshot().Position
			start, end := g.position.Boundary, g.position.WindowEnd
			_, err := a.scanWindow(context.Background(), g, start, end, replay, &graphRequests{})
			require.ErrorIs(t, err, errYield)
			require.Equal(t, int32(budget), calls.Load())
			require.NoError(t, a.saveConfirmed(context.Background(), g))
			require.Equal(t, initial, store.snapshot().Position)
			work := g.liveScan
			if replay {
				work = g.replayScan
			}
			require.NotNil(t, work)
			require.Equal(t, end, work.targetEnd)
			u, err := url.Parse(work.next)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprint(budget), u.Query().Get("$skiptoken"))
			complete, err := a.scanWindow(context.Background(), g, start, end, replay, &graphRequests{})
			require.NoError(t, err)
			require.Equal(t, end, complete)
			require.Equal(t, int32(total), calls.Load())
			require.Equal(t, total, sink.count(), "budget yields retain continuation rather than re-fetching pages or skipping ties")
			require.NoError(t, a.complete(g, complete, false))
			require.NoError(t, a.saveConfirmed(context.Background(), g))
			p, err := decodePosition(store.snapshot().Position)
			require.NoError(t, err)
			require.Equal(t, end, p.Boundary)
		})
	}
}

type timeoutAdmissionSink struct {
	captureSink
	retry   bool
	calls   atomic.Int32
	drains  atomic.Int32
	entered chan time.Duration
	release chan struct{}
}

func (s *timeoutAdmissionSink) Ship(_ *protocol.DataMessage, timeout time.Duration) error {
	attempt := s.calls.Add(1)
	if s.retry && attempt == 1 {
		return uspclient.ErrorBufferFull
	}
	s.entered <- timeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-s.release:
	}
	return uspclient.ErrorBufferFull
}
func (s *timeoutAdmissionSink) BeginDrain() { s.drains.Add(1) }

func TestDefenderGenerationCancellationBoundsEveryAdmissionAttempt(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprint(retry), func(t *testing.T) {
			server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
			sink := &timeoutAdmissionSink{retry: retry, entered: make(chan time.Duration, 2), release: make(chan struct{})}
			a, g, store := harness(t, server, sink)
			initial := store.snapshot().Position
			var vendorErrors atomic.Int32
			a.conf.ClientOptions.OnError = func(error) { vendorErrors.Add(1) }
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			t.Cleanup(func() { cancel(); close(sink.release) })
			start := g.position.Boundary
			go func() {
				done <- a.shipItem(ctx, g, realisticAlert("unadmitted", time.Unix(0, start).Add(time.Second)), start, g.position.WindowEnd)
			}()
			var timeout time.Duration
			select {
			case timeout = <-sink.entered:
			case <-time.After(time.Second):
				t.Fatal("Ship did not start")
			}
			cancel()
			require.LessOrEqual(t, timeout, time.Second, "both the first Ship and backpressure retries must have bounded timeouts")
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(1500 * time.Millisecond):
				t.Fatal("generation cancellation could not join admission within one bounded Ship attempt")
			}
			require.Zero(t, sink.drains.Load(), "a generation conflict must preserve the live SDK and tracker")
			require.Empty(t, g.live, "failed admission must not mark dedupe state")
			require.Empty(t, g.recent)
			require.False(t, g.hasDependency)
			require.NoError(t, a.saveConfirmed(context.Background(), g))
			require.Equal(t, initial, store.snapshot().Position)
			require.Zero(t, vendorErrors.Load(), "generation cancellation is not a vendor error")
		})
	}
}
