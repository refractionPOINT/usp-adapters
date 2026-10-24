package usp_entraid

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
)

func fillHistory(g *entraGeneration, stream entraStream, stamp int64) {
	for i := 0; len(g.live) < liveIDLimit; i++ {
		var key [16]byte
		binary.BigEndian.PutUint64(key[:8], uint64(i))
		g.live[key] = liveID{expiry: stamp + int64(historyHorizon), stream: streamCode(stream.name)}
	}
}

func TestEntraUnexpiredHistoryPressurePreservesProtectedAdmissions(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	sink := &captureSink{withheld: true}
	a, g, store := harness(t, server, sink)
	stream := a.streams[0]
	base := fixtureBaseTime()
	s := g.position[stream.name]
	s.Boundary = base.Add(time.Hour).UnixNano()
	s.WindowEnd = s.Boundary + int64(time.Minute)
	s.Replay = s.Coverage
	s.ReplayEnd = s.Boundary
	g.position = entraPosition{stream.name: s}
	a.runtime.now = func() time.Time { return base.Add(2 * time.Hour) }
	for _, v := range []struct {
		id    string
		stamp time.Time
	}{{"fresh", base.Add(59 * time.Minute)}, {"unfinished", base.Add(time.Hour + time.Second)}} {
		require.NoError(t, a.shipItem(context.Background(), g, stream, item(v.id, v.stamp), s.Coverage, s.WindowEnd))
	}
	require.NoError(t, a.candidate(g))
	sink.ack(^uint64(0))
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.NoError(t, a.shipItem(context.Background(), g, stream, item("pending-old", base.Add(time.Second)), s.Coverage, s.WindowEnd))
	require.NoError(t, a.candidate(g))
	fillHistory(g, stream, base.Add(10*time.Minute).UnixNano())
	// This unfinished record is old in wall time, but is still above the source's
	// complete frontier. An expiry-only policy must not forget it either.
	a.runtime.now = func() time.Time { return base.Add(100 * time.Hour) }
	a.reclaimExpiredHistory(g)
	for _, id := range []string{"fresh", "unfinished", "pending-old"} {
		require.Contains(t, g.live, eventDigest(a.sourceFingerprint(), stream.name, id), id)
	}
	require.Less(t, len(g.live), liveIDLimit, "optional ACKed history must yield capacity")
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.LessOrEqual(t, decodeBoundary(t, store, stream.name), s.Boundary)
	// Now exercise precisely the unexpired case, separately from expiry.
	fillHistory(g, stream, base.Add(20*time.Minute).UnixNano())
	a.runtime.now = func() time.Time { return base.Add(2 * time.Hour) }
	a.reclaimExpiredHistory(g)
	require.Less(t, len(g.live), liveIDLimit, "unexpired optional ACKed history must yield before 72h")
	require.Greater(t, g.position[stream.name].Coverage, base.Add(20*time.Minute).UnixNano())
	require.Equal(t, s.Coverage, g.position[stream.name].Unverified)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.LessOrEqual(t, decodeBoundary(t, store, stream.name), s.Boundary, "pending ACK cannot fabricate a complete frontier")
}

func decodeBoundary(t *testing.T, s *memoryStore, name string) int64 {
	t.Helper()
	p, err := decodePosition(s.snapshot().Position)
	require.NoError(t, err)
	return p[name].Boundary
}

func TestEntraPressureInvalidatesDelayedHistoricalScans(t *testing.T) {
	var calls atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		start, _ := bounds(t, r)
		sendPage(w, []map[string]interface{}{item("forgotten", start)}, "")
	})
	sink := &captureSink{}
	a, g, _ := harness(t, server, sink)
	stream := a.streams[0]
	base := fixtureBaseTime()
	s := g.position[stream.name]
	s.Boundary = base.Add(time.Hour).UnixNano()
	s.WindowEnd = s.Boundary + int64(time.Minute)
	s.Replay = s.Coverage
	s.ReplayEnd = s.Boundary
	g.position = entraPosition{stream.name: s}
	for _, lane := range []string{"replay", "settle"} {
		work := a.newScan(stream, s.Coverage, s.Boundary, s.Boundary)
		work.next = a.windowURL(stream, s.Coverage, s.Boundary) + "&$skiptoken=delayed"
		g.scans[stream.name+"/"+lane] = work
	}
	fillHistory(g, stream, base.Add(10*time.Minute).UnixNano())
	a.runtime.now = func() time.Time { return base.Add(100 * time.Hour) }
	a.reclaimExpiredHistory(g)
	for _, lane := range []string{"replay", "settle"} {
		require.NotContains(t, g.scans, stream.name+"/"+lane, "a delayed old cursor cannot survive discarded protection")
		_, err := a.scanLane(context.Background(), g, stream, s.Coverage, s.Boundary, lane, &graphRequests{})
		require.Error(t, err, "stale scheduling bounds cannot recreate a forgotten scan")
	}
	require.Zero(t, calls.Load())
	require.Zero(t, sink.count())
}

func TestEntraObsoleteHistoryPrunedAndCompactedWhileIdle(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	a, g, _ := harness(t, server, &captureSink{})
	stream := a.streams[0]
	base := fixtureBaseTime()
	s := g.position[stream.name]
	s.Boundary = base.Add(time.Hour).UnixNano()
	s.WindowEnd = s.Boundary + int64(time.Hour)
	s.Coverage = base.Add(50 * time.Minute).UnixNano()
	g.position = entraPosition{stream.name: s}
	a.streams = a.streams[:1]
	a.runtime.now = func() time.Time { return base.Add(time.Hour) }
	runtime.GC()
	var before, peak, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fillHistory(g, stream, base.UnixNano())
	retained := liveIDLimit / 8
	for i := 0; i < retained; i++ {
		var key [16]byte
		binary.BigEndian.PutUint64(key[:8], uint64(i))
		v := g.live[key]
		v.expiry = base.Add(59*time.Minute + historyHorizon).UnixNano()
		g.live[key] = v
	}
	runtime.GC()
	runtime.ReadMemStats(&peak)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	a.collect(ctx, g)
	cancel()
	require.Len(t, g.live, retained, "obsolete confirmed history below Coverage must be pruned without reaching pressure")
	// Inspect private counters without requiring those fields in the frozen source
	// overlay used to prove this test's behavioral baseline failure.
	m := reflect.ValueOf(g).Elem().FieldByName("maintenance")
	require.True(t, m.IsValid())
	require.EqualValues(t, 1, m.FieldByName("sweeps").Int(), "a 1ms idle loop may not rescan the full map")
	require.LessOrEqual(t, m.FieldByName("maxVisited").Int(), int64(2*liveIDLimit+retained))
	require.EqualValues(t, 1, m.FieldByName("compactions").Int())
	t.Logf("sweeps=%d max_entries_visited=%d compactions=%d", m.FieldByName("sweeps").Int(), m.FieldByName("maxVisited").Int(), m.FieldByName("compactions").Int())
	runtime.GC()
	runtime.ReadMemStats(&after)
	t.Logf("heap baseline=%d peak=%d compacted=%d retained_delta=%d", before.HeapAlloc, peak.HeapAlloc, after.HeapAlloc, int64(after.HeapAlloc)-int64(before.HeapAlloc))
	require.Less(t, int64(after.HeapAlloc)-int64(before.HeapAlloc), int64(8<<20), "deleting entries alone leaves the large map allocation resident")
	runtime.KeepAlive(g)
}

func TestEntraMalformedVendorPagesEscalateAfterValidation(t *testing.T) {
	for _, kind := range []string{"item", "timestamp", "link", "body", "redirect", "cycle", "oversized", "page-size"} {
		t.Run(kind, func(t *testing.T) {
			base := fixtureBaseTime()
			var clock atomic.Int64
			clock.Store(base.UnixNano())
			var errorsSeen atomic.Int32
			var valid atomic.Bool
			server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
				start, _ := bounds(t, r)
				if valid.Load() {
					sendPage(w, nil, "")
					return
				}
				switch kind {
				case "item":
					sendPage(w, []map[string]interface{}{item("valid-prefix", start), {"activityDateTime": start.Format(time.RFC3339Nano)}}, "")
				case "timestamp":
					sendPage(w, []map[string]interface{}{{"id": "bad", "activityDateTime": "invalid-sensitive-value"}}, "")
				case "link":
					sendPage(w, nil, "https://invalid.example/path?token=sensitive-value")
				case "body":
					fmt.Fprint(w, `{"value": invalid-sensitive-value}`)
				case "redirect":
					w.Header().Set("Location", "https://invalid.example/?token=sensitive-value")
					w.WriteHeader(302)
				case "oversized":
					fmt.Fprint(w, strings.Repeat(" ", (16<<20)+1))
				case "page-size":
					items := make([]map[string]interface{}, 1001)
					for i := range items {
						items[i] = item(strconv.Itoa(i), start)
					}
					sendPage(w, items, "")
				case "cycle":
					sendPage(w, nil, "http://"+r.Host+r.URL.String())
				}
			})
			sink := &captureSink{}
			a, g, _ := harness(t, server, sink)
			stream := a.streams[0]
			a.runtime.now = func() time.Time { return time.Unix(0, clock.Load()) }
			a.conf.ClientOptions.OnError = func(err error) {
				require.NotContains(t, err.Error(), "sensitive")
				require.NotContains(t, err.Error(), "http")
				errorsSeen.Add(1)
			}
			requests := &graphRequests{}
			retry := func() {
				_, err := a.scanWindow(context.Background(), g, stream, base.UnixNano(), base.Add(time.Minute).UnixNano(), true, requests)
				require.Error(t, err)
			}
			retry()
			require.Zero(t, sink.count(), "validate the whole page before admitting its prefix")
			clock.Store(base.Add(59 * time.Minute).UnixNano())
			retry()
			require.Zero(t, errorsSeen.Load())
			// A valid page in another lane proves availability, but does not repair
			// this traversal's deterministic invalid response or reset its episode.
			valid.Store(true)
			_, err := a.scanLane(context.Background(), g, stream, base.Add(time.Hour).UnixNano(), base.Add(2*time.Hour).UnixNano(), "settle", requests)
			require.NoError(t, err)
			valid.Store(false)
			clock.Store(base.Add(61 * time.Minute).UnixNano())
			retry()
			require.Equal(t, int32(1), errorsSeen.Load())
			clock.Store(base.Add(122 * time.Minute).UnixNano())
			retry()
			require.Equal(t, int32(1), errorsSeen.Load(), "persistent invalid vendor data must become visible after a continuous hour")
			require.Equal(t, base.UnixNano(), g.position[stream.name].Boundary)
		})
	}
}

func TestEntraPermanentVendorFailuresRespectOrdinaryPoll(t *testing.T) {
	base := fixtureBaseTime()
	var clock atomic.Int64
	clock.Store(base.UnixNano())
	var calls, callbacks atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(403) })
	a, _, _ := harness(t, server, &captureSink{})
	a.pollInterval = 30 * time.Second
	a.runtime.now = func() time.Time { return time.Unix(0, clock.Load()) }
	a.conf.ClientOptions.OnError = func(error) { callbacks.Add(1) }
	requests := &graphRequests{}
	stream := a.streams[0]
	request := func(replay bool) {
		_, _ = a.requestPage(context.Background(), stream, a.windowURL(stream, base.UnixNano(), base.Add(time.Minute).UnixNano()), replay, requests, &requestFailure{})
	}
	request(false)
	for i := 0; i < 20; i++ {
		request(i%2 == 0)
	}
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, int32(1), callbacks.Load(), "permanent failures must not consume a minute's request budget and callbacks immediately")
	clock.Store(base.Add(30*time.Second - time.Nanosecond).UnixNano())
	request(true)
	require.Equal(t, int32(1), calls.Load())
	clock.Store(base.Add(30 * time.Second).UnixNano())
	request(true)
	require.Equal(t, int32(2), calls.Load())
}

// The producer sink retains sequence/count and a compact test bitmap, not event
// payloads. Reconciliation and settle must never emit an already admitted ID.
type pressureSink struct {
	mu         sync.Mutex
	n          uint64
	seen       []uint64
	duplicates uint64
}

func (s *pressureSink) Ship(m *protocol.DataMessage, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, _ := strconv.Atoi(m.JsonPayload["id"].(string))
	word, bit := id/64, uint(id%64)
	for len(s.seen) <= word {
		s.seen = append(s.seen, 0)
	}
	if s.seen[word]&(1<<bit) != 0 {
		s.duplicates++
	}
	s.seen[word] |= 1 << bit
	s.n++
	m.SeqNum = s.n
	return nil
}
func (s *pressureSink) GetUnsent() ([]*protocol.DataMessage, error) { return nil, nil }
func (s *pressureSink) Drain(time.Duration) error                   { return nil }
func (s *pressureSink) Close() ([]*protocol.DataMessage, error)     { return nil, nil }
func (s *pressureSink) counts() (uint64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n, s.duplicates
}

func TestEntraUnexpiredCapResumesPagedACKedProducer(t *testing.T) {
	base := fixtureBaseTime()
	step := 11 * time.Millisecond
	var clock atomic.Int64
	clock.Store(base.Add(3 * time.Hour).UnixNano())
	var pages atomic.Int64
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		pages.Add(1)
		start, end := bounds(t, r)
		offset, _ := strconv.Atoi(r.URL.Query().Get("$skiptoken"))
		first := base.Add(time.Duration((start.UnixNano()-base.UnixNano()+int64(step)-1)/int64(step)) * step)
		at := first.Add(time.Duration(offset) * step)
		items := make([]map[string]interface{}, 0, 1000)
		for len(items) < 1000 && at.Before(end) {
			id := (at.UnixNano() - base.UnixNano()) / int64(step)
			items = append(items, map[string]interface{}{"id": strconv.FormatInt(id, 10), "activityDateTime": at.Format(time.RFC3339Nano)})
			at = at.Add(step)
		}
		next := ""
		if at.Before(end) {
			q := url.Values{"$filter": {r.URL.Query().Get("$filter")}, "$skiptoken": {strconv.Itoa(offset + len(items))}}
			next = "http://" + r.Host + r.URL.Path + "?" + q.Encode()
		}
		sendPage(w, items, next)
	})
	sink := &pressureSink{}
	a, g, store := harness(t, server, sink)
	a.streams = a.streams[:1]
	stream := a.streams[0]
	g.position = entraPosition{stream.name: g.position[stream.name]}
	a.runtime.window = 30 * time.Second
	a.runtime.reconcileInterval = 15 * time.Minute
	a.runtime.now = func() time.Time { return time.Unix(0, clock.Load()) }
	s := g.position[stream.name]
	s.WindowEnd = s.Boundary + int64(a.runtime.window)
	g.position[stream.name] = s
	var onErrors atomic.Int32
	a.conf.ClientOptions.OnError = func(error) { onErrors.Add(1) }
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a.collect(ctx, g) }()
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = a.saveConfirmed(ctx, g)
			}
		}
	}()
	defer func() { cancel(); wg.Wait() }()
	require.Eventually(t, func() bool { n, _ := sink.counts(); return n >= liveIDLimit }, 120*time.Second, 10*time.Millisecond, "fill the real cap with unexpired actual HTTP/ACK admissions")
	boundary := decodeBoundary(t, store, stream.name)
	clock.Store(base.Add(4 * time.Hour).UnixNano())
	require.Eventually(t, func() bool { return decodeBoundary(t, store, stream.name) > boundary+int64(30*time.Minute) }, 30*time.Second, 10*time.Millisecond, "live producer must advance within the next synthetic hour rather than wait days for expiry")
	cancel()
	wg.Wait()
	n, duplicates := sink.counts()
	require.Greater(t, n, uint64(liveIDLimit))
	require.Zero(t, duplicates)
	require.Zero(t, onErrors.Load())
	require.LessOrEqual(t, len(g.live), liveIDLimit)
	require.Greater(t, g.position[stream.name].Coverage, s.Coverage)
	require.Equal(t, s.Coverage, g.position[stream.name].Unverified)
	t.Logf("admitted=%d pages=%d boundary_progress=%s retained=%d", n, pages.Load(), time.Duration(decodeBoundary(t, store, stream.name)-boundary), len(g.live))
}

var errFixturePageAdmission = errors.New("fixture admission failure")

type failedPageSink struct {
	*captureSink
	failNext atomic.Bool
}

func (s *failedPageSink) Ship(m *protocol.DataMessage, d time.Duration) error {
	if s.failNext.Swap(false) {
		return errFixturePageAdmission
	}
	return s.captureSink.Ship(m, d)
}

func TestEntraPartialValidatedPageResumesWithoutRefetch(t *testing.T) {
	base := fixtureBaseTime()
	var calls atomic.Int32
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		sendPage(w, []map[string]interface{}{item("page-first", base), item("page-second", base.Add(time.Second))}, "")
	})
	sink := &failedPageSink{captureSink: &captureSink{withheld: true}}
	a, g, store := harness(t, server, sink)
	stream := a.streams[0]
	// All slots initially protect unfinished work in a different collection. The
	// first item uses the sole free slot; the second must wait. No source frontier
	// can advance over that partially admitted page.
	other := a.streams[1]
	s := g.position[other.name]
	s.Boundary = base.Add(-time.Hour).UnixNano()
	s.WindowEnd = base.Add(time.Minute).UnixNano()
	s.Coverage = s.Boundary
	g.position[other.name] = s
	fillHistory(g, other, base.Add(-30*time.Minute).UnixNano())
	for key := range g.live {
		delete(g.live, key)
		break
	}
	requests := &graphRequests{}
	pos := g.position[stream.name]
	_, err := a.scanWindow(context.Background(), g, stream, pos.Boundary, pos.WindowEnd, false, requests)
	require.ErrorIs(t, err, errPressure)
	require.Equal(t, 1, sink.count())
	require.Equal(t, pos.Boundary, g.position[stream.name].Boundary)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, pos.Boundary, decodeBoundary(t, store, stream.name))
	// A different complete collection moves past its old, confirmed history. The
	// blocked page now has room without pretending its own first record ACKed.
	s = g.position[other.name]
	s.Boundary = base.Add(time.Hour).UnixNano()
	s.WindowEnd = s.Boundary + int64(time.Minute)
	g.position[other.name] = s
	a.runtime.now = func() time.Time { return base.Add(2 * time.Hour) }
	a.reclaimExpiredHistory(g)
	sink.failNext.Store(true)
	_, err = a.scanWindow(context.Background(), g, stream, pos.Boundary, pos.WindowEnd, false, requests)
	require.ErrorIs(t, err, errFixturePageAdmission)
	require.Equal(t, 1, sink.count(), "failed Ship must not consume an offset or sequence")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = a.scanWindow(canceled, g, stream, pos.Boundary, pos.WindowEnd, false, requests)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, sink.count())
	end, err := a.scanWindow(context.Background(), g, stream, pos.Boundary, pos.WindowEnd, false, requests)
	require.NoError(t, err)
	require.Equal(t, int32(1), calls.Load(), "resume the validated page offset; never fetch the admitted prefix again")
	require.Equal(t, 2, sink.count())
	require.NoError(t, a.complete(g, stream, end, false))
	sink.ack(1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, pos.Boundary, decodeBoundary(t, store, stream.name), "own second admission remains part of the complete-window ACK dependency")
	sink.ack(2)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, end, decodeBoundary(t, store, stream.name))
}

func TestEntraPartialAdmissionACKProofKeepsCompleteSource(t *testing.T) {
	server := vendorServer(t, func(w http.ResponseWriter, r *http.Request) { sendPage(w, nil, "") })
	sink := &captureSink{withheld: true}
	a, g, store := harness(t, server, sink)
	stream := a.streams[0]
	base := fixtureBaseTime()
	pos := g.position[stream.name]
	for i := 0; i < 600; i++ {
		require.NoError(t, a.shipItem(context.Background(), g, stream, item(strconv.Itoa(i), base.Add(time.Duration(i)*time.Millisecond)), pos.Boundary, pos.WindowEnd))
	}
	// More incomplete IDs than fit the persisted summary must not make the actual
	// admission ACK proof depend on advancing an unfinished source position.
	fillHistory(g, stream, base.Add(time.Minute).UnixNano())
	a.reclaimExpiredHistory(g)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Zero(t, g.confirmed.Load())
	sink.ack(599)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Zero(t, g.confirmed.Load())
	sink.ack(600)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Positive(t, g.confirmed.Load(), "a partial page's successful admissions need a real cumulative ACK proof")
	require.Equal(t, pos.Boundary, decodeBoundary(t, store, stream.name))
	require.Equal(t, pos.Boundary, g.position[stream.name].Boundary)
	require.Len(t, g.live, liveIDLimit, "confirmed unfinished work is still mandatory protection")
	saved, err := decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	require.Equal(t, pos.Boundary, saved[stream.name].Coverage)
	require.Len(t, saved[stream.name].IDs, savedIDLimit/len(g.position))
	require.Equal(t, base.Add(599*time.Millisecond+historyHorizon).UnixNano(), saved[stream.name].IDs[0].expiry)
}
