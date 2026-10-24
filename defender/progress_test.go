package usp_defender

import (
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
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"github.com/stretchr/testify/require"
)

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
func harness(t *testing.T, server *httptest.Server, sink uspSink) (*DefenderAdapter, *defenderGeneration) {
	t.Helper()
	start := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	endpoint, err := collectionEndpoint(server.URL + alertsPath)
	require.NoError(t, err)
	a := &DefenderAdapter{conf: DefenderConfig{ClientOptions: testClientOptions(t), TenantID: mockTenantID, ClientID: mockClientID, ClientSecret: mockClientSecret, TokenURL: server.URL + tokenPath}, collectionURL: endpoint, httpClient: server.Client(), uspClient: sink, runtime: fastRuntime(start), pollInterval: time.Millisecond}
	a.tracker, err = acktrack.NewACKTracker(sink, 2, 16)
	require.NoError(t, err)
	return a, newGeneration(1, a.initialPosition())
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
	a, g := harness(t, server, sink)
	initial := g.confirmed.Load()
	end := scan(t, a, g)
	require.Equal(t, 2, int(calls.Load()))
	require.Equal(t, 4, sink.count())
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load())
	sink.ack(3)
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load(), "partial page ACK cannot complete an interval")
	sink.ack(4)
	require.NoError(t, a.confirm(g))
	require.Equal(t, g.serial, g.confirmed.Load(), "complete source marker awaits cumulative ACK")

	p := g.position
	require.Equal(t, end, p.Boundary)
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
	a, g := harness(t, server, sink)
	originalEnd := g.position.WindowEnd
	end := scan(t, a, g)
	require.Less(t, end, originalEnd)
	require.Equal(t, originalEnd, g.position.WindowEnd, "retain fixed incomplete upper bound")
	require.NoError(t, a.confirm(g))
	require.Equal(t, g.serial, g.confirmed.Load(), "complete source marker awaits cumulative ACK")

	p := g.position
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
	a, g := harness(t, server, sink)
	start := g.position.Boundary
	initial := g.confirmed.Load()
	end, err := a.scanWindow(context.Background(), g, start, start+1, false, &graphRequests{})
	require.ErrorIs(t, err, errIncomplete)
	require.Equal(t, start, end)
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load())
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
	a, g := harness(t, server, sink)
	local := time.Unix(0, g.position.Boundary).In(time.Local)
	a.runtime.now = func() time.Time { return local }
	p := a.initialPosition()
	require.Equal(t, local.UnixNano(), p.Boundary)
	require.Contains(t, a.windowURL(g.position.Boundary, g.position.WindowEnd), url.QueryEscape(time.Unix(0, g.position.Boundary).UTC().Format(graphTimeLayout)))
	scan(t, a, g)
	require.Equal(t, 1, sink.count())
	require.Equal(t, "lower-inclusive", sink.snapshot()[0].JsonPayload["id"])
}
func TestDefenderPendingDedupEmptyWindowsAndContinuousTail(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	sink := &captureSink{withheld: true}
	a, g := harness(t, server, sink)
	base := g.position.Boundary
	first := realisticAlert("first", time.Unix(0, base+1))
	require.NoError(t, a.shipItem(context.Background(), g, first, base, base+10))
	require.NoError(t, a.shipItem(context.Background(), g, first, base, base+10))
	require.Equal(t, 1, sink.count())
	require.NoError(t, a.complete(g, base+10, false))
	firstSerial := g.serial
	require.NoError(t, a.complete(g, base+20, false)) // empty inherits previous admission
	second := realisticAlert("second", time.Unix(0, base+21))
	require.NoError(t, a.shipItem(context.Background(), g, second, base+20, base+30))
	require.NoError(t, a.complete(g, base+30, false))
	sink.ack(1)
	require.NoError(t, a.confirm(g))
	require.Equal(t, firstSerial, g.confirmed.Load(), "frozen probe progresses with an unACKed continuous tail")
	sink.ack(2)
	require.NoError(t, a.confirm(g))
	require.Equal(t, g.serial, g.confirmed.Load())
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
			a, g := harness(t, server, &captureSink{})
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
	a, _ := harness(t, vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") }), &captureSink{})
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
	a, g := harness(t, server, &captureSink{})
	// Exercise cancellation during a substantial scheduler cooldown, rather
	// than a millisecond poll that would hide a non-cancellable wait.
	a.pollInterval = 2 * time.Second
	var onError atomic.Int32
	a.conf.ClientOptions.OnError = func(error) { onError.Add(1) }
	retrySeen := make(chan struct{}, 1)
	a.conf.ClientOptions.DebugLog = func(code string) {
		code = strings.TrimPrefix(code, "defender collection: ")
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
	a, g := harness(t, server, &captureSink{})
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
	sink := &blockedSink{entered: make(chan struct{}, 1), released: make(chan struct{})}
	a, _, err := newDefenderAdapterWithRuntime(context.Background(), DefenderConfig{ClientOptions: testClientOptions(t), TenantID: mockTenantID, ClientID: mockClientID, ClientSecret: mockClientSecret, TokenURL: server.URL + tokenPath, AlertsURL: server.URL + alertsPath, PollInterval: time.Millisecond}, sink, rt)
	require.NoError(t, err)
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("producer never entered Ship")
	}

	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close joined a blocked producer before BeginDrain")
	}
	require.Zero(t, a.generation.confirmed.Load(), "failed Ship is not delivered")
	require.Empty(t, a.generation.live)
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
			a, g := harness(t, server, sink)
			initial := g.confirmed.Load()
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
			require.NoError(t, a.confirm(g))
			require.Equal(t, initial, g.confirmed.Load())
			close(sink.release)
			err := <-done
			if fail {
				require.Error(t, err)
				require.Empty(t, g.live)
				require.NoError(t, a.confirm(g))
				require.Equal(t, initial, g.confirmed.Load())
				sink.fail.Store(false)
				scan(t, a, g)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, sink.count(), "failed or pending admission cannot mark the ID delivered")
			require.NoError(t, a.confirm(g))
			require.Equal(t, initial, g.confirmed.Load())
			sink.ack(1)
			require.NoError(t, a.confirm(g))
			require.NotEqual(t, initial, g.confirmed.Load())
		})
	}
}

func TestDefenderCountShortfallAndIncompleteRestartKeepBounds(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := windowBounds(t, r)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"value": []map[string]interface{}{realisticAlert("one", start)}, "@odata.count": 2})
	})
	a, g := harness(t, server, &captureSink{})
	start := g.position.Boundary
	initial := g.confirmed.Load()
	_, err := a.scanWindow(context.Background(), g, start, start+1, false, &graphRequests{})
	require.ErrorIs(t, err, errIncomplete)
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load())
	require.Zero(t, g.serial, "incomplete count must not create source progress")
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
	a, g := harness(t, server, &captureSink{})
	a.startedAt = base.Add(123456789 * time.Nanosecond).UnixNano()
	p := a.initialPosition()
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
			a, g := harness(t, server, sink)
			initial := g.confirmed.Load()
			start, end := g.position.Boundary, g.position.WindowEnd
			_, err := a.scanWindow(context.Background(), g, start, end, replay, &graphRequests{})
			require.ErrorIs(t, err, errYield)
			require.Equal(t, int32(budget), calls.Load())
			require.NoError(t, a.confirm(g))
			require.Equal(t, initial, g.confirmed.Load())
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
			require.NoError(t, a.confirm(g))
			require.Equal(t, g.serial, g.confirmed.Load(), "complete source marker awaits cumulative ACK")

			p := g.position
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
			a, g := harness(t, server, sink)
			initial := g.confirmed.Load()
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
			require.False(t, g.hasDependency)
			require.NoError(t, a.confirm(g))
			require.Equal(t, initial, g.confirmed.Load())
			require.Zero(t, vendorErrors.Load(), "generation cancellation is not a vendor error")
		})
	}
}

// Pressure fixtures keep payloads small while retaining real SDK admissions,
// sequence numbers and HTTP pagination at the production cache limit.
func pressureAlert(id string, created time.Time) map[string]interface{} {
	return map[string]interface{}{"id": id, "createdDateTime": created.UTC().Format(time.RFC3339Nano)}
}

// Admission ACKs must be observable even while no complete source candidate
// exists. Reclamation still requires expiry, and may never erase pending IDs.
func TestDefenderPressureAdmissionACKsIndependentOfSourceCompletion(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	sink := &captureSink{withheld: true}
	a, g := harness(t, server, sink)
	initial := g.confirmed.Load()
	stamp := time.Unix(0, g.position.Boundary).Add(time.Second)
	for i := 0; i < liveIDLimit; i++ {
		require.NoError(t, a.shipItem(context.Background(), g, pressureAlert(fmt.Sprint("pressure-", i), stamp), g.position.Boundary, g.position.WindowEnd))
	}
	require.NoError(t, a.confirm(g))
	extra := pressureAlert("pressure-next", stamp.Add(time.Second))
	require.ErrorIs(t, a.shipItem(context.Background(), g, extra, g.position.Boundary, g.position.WindowEnd), errPressure, "unACKed live IDs remain protected")
	a.runtime.now = func() time.Time { return stamp.Add(80 * time.Hour) }
	require.NoError(t, a.confirm(g))
	require.ErrorIs(t, a.shipItem(context.Background(), g, extra, g.position.Boundary, g.position.WindowEnd), errPressure, "expiry alone must not evict unACKed admissions")
	sink.ack(1)
	require.NoError(t, a.confirm(g))
	a.runtime.now = func() time.Time { return stamp }
	require.ErrorIs(t, a.shipItem(context.Background(), g, extra, g.position.Boundary, g.position.WindowEnd), errPressure, "ACK alone must not shorten live dedupe coverage")
	a.runtime.now = func() time.Time { return stamp.Add(80 * time.Hour) }
	require.NoError(t, a.shipItem(context.Background(), g, extra, g.position.Boundary, g.position.WindowEnd), "ACKed expired admissions must reclaim without completing the source interval")
	require.Equal(t, liveIDLimit, len(g.live))
	require.NoError(t, a.shipItem(context.Background(), g, pressureAlert("pressure-1000", stamp), g.position.Boundary, g.position.WindowEnd))
	require.Equal(t, liveIDLimit+1, sink.count(), "suppression of pending IDs survives partial reclamation")
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load(), "admission confirmations cannot certify an incomplete frontier")
}

// A real opaque-page producer hits pressure halfway through page 66. Recover
// that exact page offset without re-fetching forgotten IDs, retaining at most
// one page per lane. A concurrent manager exercises the ACK snapshot boundary.
func TestDefenderPressureProducerResumesPartialPageWithoutReplay(t *testing.T) {
	for _, terminal := range []string{"complete", "full", "count", "malformed"} {
		t.Run(terminal, func(t *testing.T) {
			const total = 66001
			var calls atomic.Int32
			var invalid atomic.Bool
			var retiredID atomic.Value
			retiredID.Store("producer-0")
			stamp := time.Date(2026, 1, 2, 3, 0, 1, 0, time.UTC)
			server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Query().Get("$filter") != "" {
					start, _ := windowBounds(t, r)
					if start.After(stamp.Add(time.Hour)) {
						sendPage(w, []map[string]interface{}{pressureAlert("fresh-after-pressure", stamp.Add(80*time.Hour-time.Minute))}, "")
						return
					}
				}
				offset := 0
				if raw := r.URL.Query().Get("$skiptoken"); raw != "" {
					_, err := fmt.Sscan(raw, &offset)
					require.NoError(t, err)
				}
				end := offset + 1000
				if end > total {
					end = total
				}
				items := make([]map[string]interface{}, 0, end-offset)
				for i := offset; i < end; i++ {
					items = append(items, pressureAlert(fmt.Sprint("producer-", i), stamp))
				}
				if offset == 66000 {
					// A duplicate from an ACKed/expired earlier page must remain suppressed.
					items = append(items, pressureAlert(retiredID.Load().(string), stamp))
					if terminal == "malformed" && !invalid.Swap(true) {
						items = append(items, map[string]interface{}{"id": "producer-66000"})
					}
					if terminal == "full" {
						for len(items) < 1000 {
							items = append(items, pressureAlert(retiredID.Load().(string), stamp))
						}
					}
				}
				next := ""
				if end < total {
					next = fmt.Sprintf("http://%s%s?$skiptoken=%d", r.Host, alertsPath, end)
				}
				if terminal == "count" {
					_ = json.NewEncoder(w).Encode(map[string]interface{}{"value": items, "@odata.nextLink": next, "@odata.count": total})
					return
				}
				sendPage(w, items, next)
			})
			release := make(chan struct{})
			close(release)
			sink := &gatedAdmissionSink{captureSink: captureSink{withheld: true}, entered: make(chan struct{}, 1), release: release}
			a, g := harness(t, server, sink)
			initial := g.confirmed.Load()
			// This second overlapping lane has not consumed the first lane's IDs.
			g.replayScan = a.newScan(g.position.Boundary, g.position.WindowEnd, g.position.WindowEnd)
			var clock atomic.Int64
			clock.Store(stamp.UnixNano())
			a.runtime.now = func() time.Time { return time.Unix(0, clock.Load()) }
			requests := &graphRequests{}
			for i := 0; i < 65; i++ {
				_, err := a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
				require.ErrorIs(t, err, errYield)
			}
			_, err := a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
			require.ErrorIs(t, err, errPressure)
			require.Equal(t, liveIDLimit, sink.count())
			require.Equal(t, int32(66), calls.Load())
			clock.Store(stamp.Add(80 * time.Hour).UnixNano())
			require.NoError(t, a.confirm(g))
			_, err = a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
			require.ErrorIs(t, err, errPressure, "pending IDs must remain protected under producer pressure")
			canceled, stopCanceled := context.WithCancel(context.Background())
			stopCanceled()
			_, err = a.scanPages(canceled, g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
			require.ErrorIs(t, err, context.Canceled)
			require.Len(t, g.live, liveIDLimit, "cancellation must not reclaim or certify source progress")
			sink.ack(liveIDLimit)
			ctx, cancel := context.WithCancel(context.Background())
			managerDone := make(chan struct{})
			observed := make(chan struct{})
			go func() {
				defer close(managerDone)
				first := true
				for ctx.Err() == nil {
					_ = a.confirm(g)
					if first {
						close(observed)
						first = false
					}
					time.Sleep(time.Millisecond)
				}
			}()
			defer func() { cancel(); <-managerDone }()
			// Synchronize with an ACK observation without reading collector-owned maps.
			select {
			case <-observed:
			case <-time.After(time.Second):
				t.Fatal("manager did not observe ACKs")
			}
			sink.fail.Store(true)
			_, err = a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
			require.Error(t, err, "a failed admission after reclamation cannot become delivered")
			require.Equal(t, liveIDLimit, sink.count())
			_, marked := g.live[eventDigest(a.sourceFingerprint(), "producer-65536")]
			require.False(t, marked, "failed Ship must not suppress the first unadmitted item")
			require.Equal(t, initial, g.confirmed.Load())
			for i := 0; i < liveIDLimit; i++ {
				id := fmt.Sprint("producer-", i)
				if _, exists := g.live[eventDigest(a.sourceFingerprint(), id)]; !exists {
					retiredID.Store(id)
					break
				}
			}
			_, stillLive := g.live[eventDigest(a.sourceFingerprint(), retiredID.Load().(string))]
			require.False(t, stillLive, "fixture duplicate must be an actually retired ID, independent of map iteration")
			sink.fail.Store(false)
			_, err = a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
			require.ErrorIs(t, err, errYield, "retained partial page must resume after expired ACKs release pressure")
			require.Equal(t, int32(66), calls.Load(), "a pressure retry must not refetch its partially consumed page")
			end, err := a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
			expectedCalls := int32(67)
			if terminal == "malformed" {
				require.ErrorIs(t, err, errPage)
				require.Nil(t, g.liveScan.page, "malformed item after retirement must release its page")
				end, err = a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
				expectedCalls++
			}
			if terminal == "complete" || terminal == "malformed" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, errIncomplete, "ambiguous terminal pages cannot restart across reclaimed history")
			}
			require.Equal(t, total, sink.count(), "every source ID is admitted once through actual page traversal")
			require.Equal(t, expectedCalls, calls.Load())
			cancel()
			<-managerDone
			require.Equal(t, initial, g.confirmed.Load())
			_, err = a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanReplay, 1, requests)
			require.ErrorIs(t, err, errIncomplete, "an overlapping traversal cannot revisit reclaimed IDs")
			// Slot restoration must use carried proof; an unresolved scan stays blocked.
			work := g.liveScan
			g.liveScan = nil
			_, err = a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
			if terminal == "complete" || terminal == "malformed" {
				require.NoError(t, err, "the completed source proof survives a cleared traversal slot without replay")
				require.Equal(t, total, sink.count(), "carried ACK proofs prevent re-emission on slot restoration")
			} else {
				require.ErrorIs(t, err, errIncomplete, "an unresolved forgotten interval remains stalled")
			}
			g.liveScan = work
			require.Equal(t, expectedCalls, calls.Load())
			if terminal != "complete" && terminal != "malformed" {
				for i := 0; i < 2; i++ {
					_, err = a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
					require.ErrorIs(t, err, errIncomplete)
				}
				require.Equal(t, expectedCalls, calls.Load(), "unresolved old traversal must stall without repeated old emission")
				freshCtx, stop := context.WithCancel(context.Background())
				freshDone := make(chan struct{})
				go func() { defer close(freshDone); a.collect(freshCtx, g) }()
				require.Eventually(t, func() bool { return sink.count() == total+1 }, time.Second, time.Millisecond, "unresolved old pressure must leave fresh-tail ingestion available")
				stop()
				<-freshDone
				require.NoError(t, a.confirm(g))
				require.Equal(t, initial, g.confirmed.Load(), "stalled/abandoned work never advances the source frontier")
				return
			}
			require.NoError(t, a.complete(g, end, false))
			require.NoError(t, a.confirm(g))
			require.Equal(t, initial, g.confirmed.Load(), "last page still awaits ACK; reclamation never certifies source progress")
			sink.ack(total)
			require.NoError(t, a.confirm(g))
			require.Equal(t, g.serial, g.confirmed.Load(), "complete source marker awaits cumulative ACK")

			p := g.position
			require.Equal(t, end, p.Boundary)
			require.NotZero(t, p.Unverified, "expired exact history must remain explicitly unverified")
			require.LessOrEqual(t, len(g.live), liveIDLimit)
		})
	}
}

func TestDefenderPressureExpiredReplayKeepsUnverifiedGap(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	oldStart := base.Add(-time.Hour)
	oldEnd := oldStart.Add(5 * time.Minute)
	stamp := oldEnd.Add(-graphTimeQuantum)
	var oldQueries atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := windowBounds(t, r)
		if start.Before(base) {
			oldQueries.Add(1)
			w.WriteHeader(503)
			return
		}
		if start.Before(base.Add(time.Hour)) {
			w.WriteHeader(503)
			return
		}
		sendPage(w, []map[string]interface{}{pressureAlert("fresh-after-expired-replay", base.Add(80*time.Hour-time.Minute))}, "")
	})
	sink := &captureSink{}
	a, g := harness(t, server, sink)
	initial := g.confirmed.Load()
	g.position.Coverage = oldStart.UnixNano()
	g.position.Replay = oldStart.UnixNano()
	g.position.ReplayEnd = oldEnd.UnixNano()
	g.replayScan = a.newScan(g.position.Replay, g.position.ReplayEnd, g.position.ReplayEnd)
	for i := 0; i < liveIDLimit; i++ {
		require.NoError(t, a.shipItem(context.Background(), g, pressureAlert(fmt.Sprint("old-replay-", i), stamp), g.position.Replay, g.position.ReplayEnd))
	}
	require.NoError(t, a.confirm(g))
	a.runtime.now = func() time.Time { return base.Add(80 * time.Hour) }
	require.NoError(t, a.shipItem(context.Background(), g, pressureAlert("new-admission", base.Add(time.Second)), g.position.Boundary, g.position.WindowEnd), "an expired ACKed replay must not pin fresh admission forever")
	require.Equal(t, oldEnd.UnixNano(), g.position.Coverage)
	require.Equal(t, oldStart.UnixNano(), g.position.Unverified)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var turns int
	a.runtime.now = func() time.Time {
		turns++
		if turns > 1 {
			cancel()
		}
		return base.Add(80 * time.Hour)
	}
	a.collect(ctx, g) // cancel at the next source-clock turn, after replay housekeeping
	require.Equal(t, liveIDLimit+2, sink.count())
	require.Zero(t, oldQueries.Load(), "an expired replay prefix must be abandoned as unverified, not queried again after its IDs were forgotten")
	require.Zero(t, g.position.Replay)
	require.Zero(t, g.position.ReplayEnd)
	require.Equal(t, oldStart.UnixNano(), g.position.Unverified)
	require.Equal(t, base.UnixNano(), g.position.Boundary, "abandoning expired history never fabricates a completed live frontier")
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load(), "admission-only ACKs cannot write history metadata as a source completion")
}

// Both direct admissions and a real 65-page history producer fill the cache.
// Source-clock lag is synthetic: the unexpired case advances it by one hour;
// it does not simulate that hour in wall time or claim a customer event rate.
func TestDefenderReclaimPreservesHealthyCatchUpAndAheadIDs(t *testing.T) {
	for _, producer := range []bool{false, true} {
		for _, aheadCount := range []int{0, 5} {
			for _, lag := range []time.Duration{time.Hour, 48 * time.Hour} {
				t.Run(fmt.Sprintf("producer=%v/ahead=%d/lag=%s", producer, aheadCount, lag), func(t *testing.T) {
					boundary := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
					now := boundary.Add(lag)
					oldStamp := boundary.Add(-time.Hour)
					oldCount := liveIDLimit - 1000 - aheadCount
					var oldCalls atomic.Int32
					server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
						token := r.URL.Query().Get("$skiptoken")
						if strings.HasPrefix(token, "history-") || (token == "" && strings.HasPrefix(r.URL.Query().Get("$filter"), "createdDateTime ge "+boundary.Add(-2*time.Hour).Format("2006-01-02T15:04:05"))) {
							offset := 0
							if token != "" {
								_, err := fmt.Sscanf(token, "history-%d", &offset)
								require.NoError(t, err)
							}
							end := offset + 1000
							if end > oldCount {
								end = oldCount
							}
							items := make([]map[string]interface{}, 0, end-offset)
							for i := offset; i < end; i++ {
								items = append(items, pressureAlert(fmt.Sprint("history-", i), oldStamp))
							}
							next := ""
							if end < oldCount {
								next = fmt.Sprintf("http://%s%s?$skiptoken=history-%d", r.Host, alertsPath, end)
							}
							oldCalls.Add(1)
							sendPage(w, items, next)
							return
						}
						if strings.HasPrefix(token, "more-") || (token == "" && strings.HasPrefix(r.URL.Query().Get("$filter"), "createdDateTime ge "+boundary.Add(10*time.Minute).Format("2006-01-02T15:04:05"))) {
							offset := 0
							if token != "" {
								_, err := fmt.Sscanf(token, "more-%d", &offset)
								require.NoError(t, err)
							}
							end := offset + 1000
							if end > 9001 {
								end = 9001
							}
							items := make([]map[string]interface{}, 0, end-offset)
							for i := offset; i < end; i++ {
								items = append(items, pressureAlert(fmt.Sprint("unexpired-more-", i), boundary.Add(11*time.Minute)))
							}
							next := ""
							if end < 9001 {
								next = fmt.Sprintf("http://%s%s?$skiptoken=more-%d", r.Host, alertsPath, end)
							}
							sendPage(w, items, next)
							return
						}
						if token == "settled-next" {
							items := []map[string]interface{}{pressureAlert("settled-0", boundary.Add(time.Second))}
							for i := 0; i < 10; i++ {
								items = append(items, pressureAlert(fmt.Sprint("later-", i), boundary.Add(2*time.Second)))
							}
							sendPage(w, items, "")
							return
						}
						start, _ := windowBounds(t, r)
						if start.Equal(boundary) {
							items := make([]map[string]interface{}, 0, 1000)
							for i := 0; i < 1000; i++ {
								items = append(items, pressureAlert(fmt.Sprint("settled-", i), boundary.Add(time.Second)))
							}
							sendPage(w, items, fmt.Sprintf("http://%s%s?$skiptoken=settled-next", r.Host, alertsPath))
							return
						}
						if start.Equal(boundary.Add(5 * time.Minute)) {
							items := []map[string]interface{}{}
							for i := 0; i < aheadCount; i++ {
								items = append(items, pressureAlert(fmt.Sprint("ahead-", i), boundary.Add(6*time.Minute)))
							}
							sendPage(w, items, "")
							return
						}
						sendPage(w, []map[string]interface{}{pressureAlert("fresh-trigger", now.Add(-time.Minute))}, "")
					})
					sink := &captureSink{withheld: true}
					a, g := harness(t, server, sink)
					a.runtime.now = func() time.Time { return now }
					ctx := context.Background()
					requests := &graphRequests{}
					if aheadCount > 0 {
						_, err := a.scanPages(ctx, g, boundary.Add(5*time.Minute).UnixNano(), boundary.Add(10*time.Minute).UnixNano(), scanTail, 1, requests)
						require.NoError(t, err)
					}
					if producer {
						g.position.Boundary = boundary.Add(-2 * time.Hour).UnixNano()
						g.position.WindowEnd = boundary.UnixNano()
						g.position.Coverage = g.position.Boundary
						for {
							end, err := a.scanPages(ctx, g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
							if errors.Is(err, errYield) {
								continue
							}
							require.NoError(t, err)
							require.NoError(t, a.complete(g, end, false))
							break
						}
						require.Equal(t, int32((oldCount+999)/1000), oldCalls.Load())
					} else {
						for i := 0; i < oldCount; i++ {
							require.NoError(t, a.shipItem(ctx, g, pressureAlert(fmt.Sprint("history-", i), oldStamp), boundary.Add(-2*time.Hour).UnixNano(), boundary.UnixNano()))
						}
						require.NoError(t, a.complete(g, boundary.UnixNano(), false))
					}
					sink.ack(^uint64(0))
					require.NoError(t, a.confirm(g))
					initial := g.confirmed.Load()
					_, err := a.scanPages(ctx, g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
					require.ErrorIs(t, err, errYield)
					require.Len(t, g.live, liveIDLimit)
					sink.ack(^uint64(0))
					require.NoError(t, a.confirm(g))
					_, err = a.scanPages(ctx, g, now.Add(-10*time.Minute).UnixNano(), now.UnixNano(), scanTail, 1, requests)
					require.NoError(t, err, "ACKed optional history behind the frontier must release capacity, including before 24h expiry")
					if lag == time.Hour {
						require.True(t, oldStamp.Add(historyHorizon).After(now), "this producer's retired history is UNEXPIRED")
					}
					for i := 0; i < aheadCount; i++ {
						_, retained := g.live[eventDigest(a.sourceFingerprint(), fmt.Sprint("ahead-", i))]
						require.True(t, retained, "ahead prompt IDs remain protected")
					}
					end, err := a.scanPages(ctx, g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
					require.NoError(t, err, "healthy consumed settled prefix must survive another lane's reclaim")
					require.NoError(t, a.complete(g, end, false))
					require.NoError(t, a.confirm(g))
					require.Equal(t, initial, g.confirmed.Load(), "source candidates still wait for pending tail and own-page dependencies")
					sink.ack(^uint64(0))
					require.NoError(t, a.confirm(g))
					end, err = a.scanPages(ctx, g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
					require.NoError(t, err)
					require.NoError(t, a.complete(g, end, false))
					require.NoError(t, a.confirm(g))
					require.Equal(t, g.serial, g.confirmed.Load(), "complete source marker awaits cumulative ACK")

					p := g.position
					require.Equal(t, boundary.Add(10*time.Minute).UnixNano(), p.Boundary)
					require.Equal(t, liveIDLimit+11, sink.count(), "all source IDs, including previously prompted next-window IDs, ship exactly once")
					require.LessOrEqual(t, len(g.live), liveIDLimit)
					if producer {
						require.NotZero(t, g.position.Unverified, "discarded optional coverage stays explicitly unverified")
					} else {
						require.Zero(t, g.position.Unverified, "discarding IDs older than Coverage must not fabricate a gap")
					}
					if producer && lag == time.Hour {
						initial = g.confirmed.Load()
						for {
							end, err = a.scanPages(ctx, g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
							if errors.Is(err, errYield) {
								continue
							}
							require.NoError(t, err, "multiple productive prune batches cannot impose a new intake ceiling")
							require.NoError(t, a.complete(g, end, false))
							break
						}
						require.Equal(t, liveIDLimit+11+9001, sink.count(), "real unexpired producer crosses capacity through multiple bounded batches")
						require.LessOrEqual(t, len(g.live), liveIDLimit)
						require.NoError(t, a.confirm(g))
						require.Equal(t, initial, g.confirmed.Load(), "new-page ACKs remain mandatory after optional-history pruning")
						sink.ack(^uint64(0))
						require.NoError(t, a.confirm(g))
						require.Equal(t, g.serial, g.confirmed.Load(), "complete source marker awaits cumulative ACK")

						saved := g.position
						require.Equal(t, boundary.Add(15*time.Minute).UnixNano(), saved.Boundary)
					}

				})
			}
		}
	}
}

func TestDefenderMalformedItemRefetchPreservesPrefixACK(t *testing.T) {
	var calls atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := windowBounds(t, r)
		second := pressureAlert("fixed", start.Add(2*time.Second))
		if calls.Add(1) == 1 {
			second = map[string]interface{}{"id": "fixed"}
		}
		sendPage(w, []map[string]interface{}{pressureAlert("prefix", start.Add(time.Second)), second}, "")
	})
	sink := &captureSink{withheld: true}
	a, g := harness(t, server, sink)
	initial := g.confirmed.Load()
	requests := &graphRequests{}
	_, err := a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
	require.ErrorIs(t, err, errPage)
	require.Nil(t, g.liveScan.page, "a transient invalid item must not pin its decoded page")
	end, err := a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
	require.NoError(t, err, "retry refetches corrected source page")
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, 2, sink.count(), "successful prefix is not admitted again")
	require.NoError(t, a.complete(g, end, false))
	sink.ack(1)
	require.NoError(t, a.confirm(g))
	require.Equal(t, initial, g.confirmed.Load())
	sink.ack(2)
	require.NoError(t, a.confirm(g))
	require.NotEqual(t, initial, g.confirmed.Load())
}

func TestDefenderRetirementBlocksOnlyUnsafeOtherScanAndReleasesPage(t *testing.T) {
	boundary := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	stamp := boundary.Add(-time.Hour)
	var deletedID atomic.Value
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		sendPage(w, []map[string]interface{}{pressureAlert(deletedID.Load().(string), stamp)}, "")
	})
	sink := &captureSink{}
	a, g := harness(t, server, sink)
	a.runtime.now = func() time.Time { return boundary.Add(48 * time.Hour) }
	for i := 0; i < liveIDLimit; i++ {
		require.NoError(t, a.shipItem(context.Background(), g, pressureAlert(fmt.Sprint("unsafe-", i), stamp), boundary.Add(-2*time.Hour).UnixNano(), boundary.UnixNano()))
	}
	require.NoError(t, a.confirm(g))
	old := a.newScan(boundary.Add(-2*time.Hour).UnixNano(), boundary.UnixNano(), boundary.UnixNano())
	old.page = &graphPage{Items: []map[string]interface{}{pressureAlert("retained-payload", stamp)}}
	g.replayScan = old
	require.NoError(t, a.shipItem(context.Background(), g, pressureAlert("trigger", boundary.Add(time.Second)), g.position.Boundary, g.position.WindowEnd))
	for i := 0; i < liveIDLimit; i++ {
		id := fmt.Sprint("unsafe-", i)
		if _, exists := g.live[eventDigest(a.sourceFingerprint(), id)]; !exists {
			deletedID.Store(id)
			break
		}
	}
	require.NotNil(t, deletedID.Load())
	require.Nil(t, old.page, "a scan which lost an unconsumed ID cannot resume and must release its payload")
	before := sink.count()
	_, err := a.scanPages(context.Background(), g, old.start, old.targetEnd, scanReplay, 1, &graphRequests{})
	require.Equal(t, before, sink.count(), "removing the unsafe-other-scan guard actually re-emits a forgotten ID")
	require.ErrorIs(t, err, errIncomplete)
}

func TestDefenderReclaimNeverForgetsCurrentUnconsumedID(t *testing.T) {
	boundary := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("$skiptoken") {
		case "z":
			sendPage(w, []map[string]interface{}{pressureAlert("z", boundary.Add(-40*time.Second))}, fmt.Sprintf("http://%s%s?$skiptoken=x", r.Host, alertsPath))
		case "x":
			sendPage(w, []map[string]interface{}{pressureAlert("x", boundary.Add(-time.Minute))}, "")
		default:
			sendPage(w, []map[string]interface{}{pressureAlert("y", boundary.Add(-50*time.Second))}, fmt.Sprintf("http://%s%s?$skiptoken=z", r.Host, alertsPath))
		}
	})
	sink := &captureSink{withheld: true}
	a, g := harness(t, server, sink)
	a.runtime.now = func() time.Time { return boundary.Add(48 * time.Hour) }
	require.NoError(t, a.shipItem(context.Background(), g, pressureAlert("x", boundary.Add(-time.Minute)), boundary.Add(-2*time.Minute).UnixNano(), boundary.UnixNano()))
	for i := 0; i < liveIDLimit-2; i++ {
		require.NoError(t, a.shipItem(context.Background(), g, pressureAlert(fmt.Sprint("other-tail-", i), boundary.Add(6*time.Minute)), boundary.Add(5*time.Minute).UnixNano(), boundary.Add(10*time.Minute).UnixNano()))
	}
	requests := &graphRequests{}
	_, err := a.scanPages(context.Background(), g, boundary.Add(-2*time.Minute).UnixNano(), boundary.UnixNano(), scanTail, 1, requests)
	require.ErrorIs(t, err, errYield)
	sink.ack(1)
	require.NoError(t, a.confirm(g))
	_, err = a.scanPages(context.Background(), g, boundary.Add(-2*time.Minute).UnixNano(), boundary.UnixNano(), scanTail, 1, requests)
	// The current scan overlaps optional history behind the frontier.
	// The intact guard waits; its mutant retires x and admits z immediately.
	// Continue both variants so the mutant demonstrates an actual duplicate.
	if err != nil && !errors.Is(err, errYield) {
		require.ErrorIs(t, err, errPressure)
	}
	sink.ack(^uint64(0))
	require.NoError(t, a.confirm(g))
	if errors.Is(err, errPressure) {
		_, err = a.scanPages(context.Background(), g, boundary.Add(-2*time.Minute).UnixNano(), boundary.UnixNano(), scanTail, 1, requests)
		require.ErrorIs(t, err, errYield)
	}
	_, err = a.scanPages(context.Background(), g, boundary.Add(-2*time.Minute).UnixNano(), boundary.UnixNano(), scanTail, 1, requests)
	require.NoError(t, err)
	require.Equal(t, liveIDLimit+1, sink.count(), "the work exemption prevents x being evicted, then actually admitted twice")
}

func TestDefenderOptionalHistoryRetirementPreservesPendingIDs(t *testing.T) {
	boundary := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	sink := &captureSink{withheld: true}
	a, g := harness(t, server, sink)
	a.runtime.now = func() time.Time { return boundary.Add(6 * time.Minute) }
	for i := 0; i < liveIDLimit; i++ {
		require.NoError(t, a.shipItem(context.Background(), g, pressureAlert(fmt.Sprint("optional-", i), boundary.Add(-time.Hour)), boundary.Add(-2*time.Hour).UnixNano(), boundary.UnixNano()))
	}
	sink.ack(1)
	require.NoError(t, a.confirm(g))
	require.NoError(t, a.shipItem(context.Background(), g, pressureAlert("fresh-one", boundary.Add(time.Second)), g.position.Boundary, g.position.WindowEnd))
	require.Len(t, g.live, liveIDLimit, "only the ACKed optional prefix can retire; expiry is not required")
	require.Equal(t, liveIDLimit+1, sink.count())
	_, pending := g.live[eventDigest(a.sourceFingerprint(), "optional-1")]
	require.True(t, pending, "unACKed optional history remains mandatory")
	sink.ack(^uint64(0))
	require.NoError(t, a.confirm(g))
	require.NoError(t, a.shipItem(context.Background(), g, pressureAlert("fresh-two", boundary.Add(2*time.Second)), g.position.Boundary, g.position.WindowEnd))
	require.Less(t, len(g.live), liveIDLimit, "a bounded batch releases ACKed unexpired history")
}

func TestDefenderRecentPromptOverlapRemainsProtectedAtCapacity(t *testing.T) {
	boundary := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	sink := &captureSink{}
	a, g := harness(t, server, sink)
	a.runtime.now = func() time.Time { return boundary.Add(6 * time.Minute) }
	for i := 0; i < liveIDLimit; i++ {
		require.NoError(t, a.shipItem(context.Background(), g, pressureAlert(fmt.Sprint("recent-", i), boundary.Add(-time.Minute)), boundary.Add(-2*time.Minute).UnixNano(), boundary.UnixNano()))
	}
	require.NoError(t, a.confirm(g))
	err := a.shipItem(context.Background(), g, pressureAlert("overflow", boundary.Add(time.Second)), g.position.Boundary, g.position.WindowEnd)
	require.ErrorIs(t, err, errPressure, "ACKed records inside the recent prompt overlap are still protected")
	require.Len(t, g.live, liveIDLimit)
	require.Equal(t, liveIDLimit, sink.count())
	require.Zero(t, g.position.Unverified, "refusing capacity discards no coverage")
}

func TestDefenderRetirementRecordsOnlyActualCoverageGap(t *testing.T) {
	boundary := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	sink := &captureSink{}
	a, g := harness(t, server, sink)
	now := boundary.Add(48 * time.Hour)
	a.runtime.now = func() time.Time { return now }
	g.position.Coverage = boundary.Add(-time.Hour).UnixNano()
	require.NoError(t, a.shipItem(context.Background(), g, pressureAlert("older-than-coverage", boundary.Add(-2*time.Hour)), boundary.Add(-3*time.Hour).UnixNano(), boundary.UnixNano()))
	require.NoError(t, a.shipItem(context.Background(), g, pressureAlert("future-prompt", boundary.Add(6*time.Minute)), boundary.Add(5*time.Minute).UnixNano(), boundary.Add(10*time.Minute).UnixNano()))
	require.NoError(t, a.confirm(g))
	a.reclaim(g, nil) // direct metadata unit: first tier deletes only older history
	require.Len(t, g.live, 1)
	require.Zero(t, g.position.Unverified, "retiring history already outside coverage creates no new gap")
	now = now.Add(5 * time.Minute)
	a.reclaim(g, nil) // expired future prompt admission is the only remaining ID
	require.Empty(t, g.live)
	require.Zero(t, g.position.Unverified, "an omitted future ID has not yet broken completed history")
	require.NoError(t, a.complete(g, boundary.Add(5*time.Minute).UnixNano(), false))
	require.Zero(t, g.position.Unverified, "later completion before the actual retired timestamp does not invent a gap")
	require.NoError(t, a.complete(g, boundary.Add(10*time.Minute).UnixNano(), false))
	require.Equal(t, boundary.Add(-time.Hour).UnixNano(), g.position.Unverified, "crossing a genuinely retired ID makes that history explicitly unverified")
}

func TestDefenderForeignRetirementKeepsConsumedSettledProof(t *testing.T) {
	boundary := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("$skiptoken") == "done" {
			sendPage(w, []map[string]interface{}{pressureAlert("consumed-0", boundary.Add(time.Second)), pressureAlert("last", boundary.Add(2*time.Second))}, "")
			return
		}
		items := make([]map[string]interface{}, 0, 1000)
		for i := 0; i < 1000; i++ {
			items = append(items, pressureAlert(fmt.Sprint("consumed-", i), boundary.Add(time.Second)))
		}
		sendPage(w, items, fmt.Sprintf("http://%s%s?$skiptoken=done", r.Host, alertsPath))
	})
	sink := &captureSink{}
	a, g := harness(t, server, sink)
	a.runtime.now = func() time.Time { return boundary.Add(48 * time.Hour) }
	for i := 0; i < liveIDLimit; i++ {
		require.NoError(t, a.shipItem(context.Background(), g, pressureAlert(fmt.Sprint("consumed-", i), boundary.Add(time.Second)), g.position.Boundary, g.position.WindowEnd))
	}
	require.NoError(t, a.confirm(g))
	requests := &graphRequests{}
	_, err := a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
	require.ErrorIs(t, err, errYield)
	require.NoError(t, a.shipItem(context.Background(), g, pressureAlert("foreign-trigger", boundary.Add(48*time.Hour-time.Minute)), boundary.Add(47*time.Hour).UnixNano(), boundary.Add(48*time.Hour).UnixNano()))
	require.Len(t, g.live, liveIDLimit-1000+1, "only the consumed settled prefix is harmless when no behind-frontier history exists")
	end, err := a.scanPages(context.Background(), g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
	require.NoError(t, err, "an exact settled continuation survives another lane retiring its CONSUMED IDs")
	require.Equal(t, liveIDLimit+2, sink.count(), "the retired consumed ID stays suppressed by the active traversal proof")
	require.NoError(t, a.complete(g, end, false))
	require.NoError(t, a.confirm(g))
	require.Equal(t, g.serial, g.confirmed.Load(), "complete source marker awaits cumulative ACK")

	saved := g.position
	require.Equal(t, end, saved.Boundary)
}

// Finite prompt burst; no excessive count, timestamp tie, pending ACK or
// oversized continuation chain. Settled source is briefly unavailable, then
// healthy. After all identities expire, both prompt and settled must recover.
func TestDefenderFiniteACKedPromptBurstRecoversAcrossSettledWindows(t *testing.T) {
	for _, trigger := range []string{"fresh-tail-reclaim", "settled-unconsumed", "missing-count", "pending-then-ACK", "split-with-retained-proof"} {
		t.Run(trigger, func(t *testing.T) {
			b := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
			now := b.Add(12 * time.Minute)
			stamp := b.Add(6 * time.Minute)
			if trigger == "settled-unconsumed" {
				stamp = b.Add(2 * time.Minute)
			}
			phase := "seed"
			var pageGets int
			server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
				start, end := windowBounds(t, r)
				if phase == "outage" {
					w.WriteHeader(503)
					return
				}
				if phase == "healthy" && start.After(b.Add(time.Hour)) {
					sendPage(w, []map[string]interface{}{pressureAlert("fresh-after-outage", now.Add(-time.Minute))}, "")
					return
				}
				if phase == "healthy" && (!end.After(stamp) || start.After(stamp.Add(time.Duration(liveIDLimit-1)*time.Millisecond))) {
					if trigger == "missing-count" {
						json.NewEncoder(w).Encode(map[string]interface{}{"value": []map[string]interface{}{}, "@odata.count": 1})
					} else {
						sendPage(w, nil, "")
					}
					return
				}
				if phase == "healthy" && trigger == "split-with-retained-proof" {
					items := []map[string]interface{}{}
					for i := 0; i < liveIDLimit && len(items) < 1000; i++ {
						created := stamp.Add(time.Duration(i) * time.Millisecond)
						if !created.Before(start) && created.Before(end) {
							items = append(items, pressureAlert(fmt.Sprint("burst-", i), created))
						}
					}
					// Graph omits continuation on an ambiguous full page. The fixed
					// range must split without dropping carried identities in its suffix.
					sendPage(w, items, "")
					return
				}
				page := 0
				if token := r.URL.Query().Get("$skiptoken"); token != "" {
					_, err := fmt.Sscanf(token, "page-%d", &page)
					require.NoError(t, err)
				}
				pageGets++
				items := []map[string]interface{}{}
				// A new late-visible record precedes the known burst in the healthy
				// settled response, as unordered Graph pages allow.
				if phase == "healthy" && trigger == "settled-unconsumed" && page == 0 {
					items = append(items, pressureAlert("late-visible", b.Add(time.Minute)))
				}
				for i := page * 999; i < (page+1)*999 && i < liveIDLimit; i++ {
					created := stamp.Add(time.Duration(i) * time.Millisecond)
					if !created.Before(start) && created.Before(end) {
						items = append(items, pressureAlert(fmt.Sprint("burst-", i), created))
					}
				}
				next := ""
				if (page+1)*999 < liveIDLimit {
					u := *r.URL
					u.Scheme = "http"
					u.Host = r.Host
					q := u.Query()
					q.Set("$skiptoken", fmt.Sprint("page-", page+1))
					u.RawQuery = q.Encode()
					next = u.String()
				}
				sendPage(w, items, next)
			})
			sink := &captureSink{withheld: trigger == "pending-then-ACK"}
			a, g := harness(t, server, sink)
			a.runtime.now = func() time.Time { return now }
			a.pollInterval = 30 * time.Second
			ctx := context.Background()
			req := &graphRequests{}
			// The real producer drains 66 HTTP pages; all IDs are real successful
			// admissions. No direct live-map or ACK-watermark seeding.
			start, end := now.Add(-10*time.Minute).UnixNano(), now.UnixNano()
			for turn := 0; turn < 100; turn++ {
				_, err := a.scanPages(ctx, g, start, end, scanTail, 1, req)
				require.NoError(t, a.confirm(g))
				if err == nil {
					break
				}
				require.ErrorIs(t, err, errYield)
			}
			require.Equal(t, liveIDLimit, sink.count())
			require.Len(t, g.live, liveIDLimit)
			if trigger == "pending-then-ACK" {
				require.Zero(t, g.acked.Load())
			} else {
				require.Equal(t, uint64(liveIDLimit), g.acked.Load())
			}
			require.Nil(t, g.tailScan)
			require.Equal(t, 66, pageGets)
			require.Equal(t, b.UnixNano(), g.position.Boundary)
			// An actual 503 retains the first settled scan before recovery.
			phase = "outage"
			_, err := a.scanPages(ctx, g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, req)
			require.ErrorIs(t, err, errYield)
			require.NotNil(t, g.liveScan)
			time.Sleep(1150 * time.Millisecond)
			phase = "healthy"
			now = b.Add(80 * time.Hour)
			if trigger != "settled-unconsumed" {
				if trigger == "pending-then-ACK" {
					sink.ack(1)
					require.NoError(t, a.confirm(g))
					_, err = a.scanPages(ctx, g, now.Add(-10*time.Minute).UnixNano(), now.UnixNano(), scanTail, 1, req)
					require.ErrorIs(t, err, errPressure, "unACKed burst identities cannot be transferred as suppression proof")
					require.Equal(t, liveIDLimit, sink.count())
					require.Zero(t, g.confirmed.Load())
					sink.ack(^uint64(0))
					require.NoError(t, a.confirm(g))
				}
				_, err = a.scanPages(ctx, g, now.Add(-10*time.Minute).UnixNano(), now.UnixNano(), scanTail, 1, req)
				require.NoError(t, err)
				require.Equal(t, liveIDLimit+1, sink.count())
				require.Greater(t, g.forgottenBefore, b.Add(6*time.Minute).UnixNano())
			}
			if trigger == "missing-count" {
				_, err = a.scanPages(ctx, g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, req)
				require.Error(t, err, "unconsumed retained identities cannot satisfy a source count")
				require.True(t, errors.Is(err, errIncomplete) || errors.Is(err, errYield))
				require.NoError(t, a.confirm(g))
				require.Zero(t, g.confirmed.Load())
				return
			}
			var last error
			// Repeated retries plus a further 20 hours of source-time aging cannot
			// repair either guard; no further vendor failure or pending ACK exists.
			for turn := 0; turn < 1000; turn++ {
				next, scanErr := a.scanPages(ctx, g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, req)
				last = scanErr
				if scanErr == nil {
					require.NoError(t, a.complete(g, next, false))
				} else {
					require.True(t, errors.Is(scanErr, errPressure) || errors.Is(scanErr, errIncomplete) || errors.Is(scanErr, errYield), "%v", scanErr)
				}
				require.NoError(t, a.confirm(g))
				require.LessOrEqual(t, len(g.live), liveIDLimit)
				if g.liveProof != nil {
					require.Same(t, g.liveScan, g.liveProof, "suppression proof aliases the existing settled lane")
					require.LessOrEqual(t, len(g.liveProof.ids)+len(g.liveProof.retained), liveIDLimit)
				}
				if g.position.Boundary >= b.Add(10*time.Minute).UnixNano() {
					break
				}
				now = now.Add(12 * time.Minute)
			}
			if trigger == "pending-then-ACK" {
				require.Zero(t, g.confirmed.Load(), "the fresh tail's unACKed admission still gates all complete source candidates")
				sink.ack(^uint64(0))
				require.NoError(t, a.confirm(g))
			}
			require.Equal(t, g.serial, g.confirmed.Load(), "complete source marker awaits cumulative ACK")

			saved := g.position
			t.Logf("trigger=%s admitted=%d ACKed=%d cache=%d boundaryOffset=%s confirmedOffset=%s forgottenOffset=%s last=%v sourceAge=%s", trigger, sink.count(), g.acked.Load(), len(g.live), time.Duration(g.position.Boundary-b.UnixNano()), time.Duration(saved.Boundary-b.UnixNano()), time.Duration(g.forgottenBefore-b.UnixNano()), last, now.Sub(b))
			require.GreaterOrEqual(t, g.position.Boundary, b.Add(10*time.Minute).UnixNano(), "finite ACKed burst must not permanently block healthy settled catch-up")
			require.GreaterOrEqual(t, saved.Boundary, b.Add(10*time.Minute).UnixNano(), "healthy complete windows also become confirmed after ACK")
			require.Equal(t, liveIDLimit+1, sink.count(), "no repeat burst admissions")
		})
	}
}
