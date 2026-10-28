package usp_o365

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- fakes -------------------------------------------------------------------------

// fakeBatchDeduper is a utils.BatchDeduper that counts its calls and logs, in
// order, everything done to it and to the sink wired to it.
type fakeBatchDeduper struct {
	mu   sync.Mutex
	seen map[string]time.Time

	checkMany [][]utils.DedupeKey
	addMany   [][]utils.DedupeKey
	ops       []string // "checkmany", "addmany:<keys>", "ship"

	checkCalls       int
	checkAndAddCalls int

	// failOpen makes CheckMany answer false for every key, like an
	// unreachable backend. shortAnswer makes it answer with no results.
	failOpen    bool
	shortAnswer bool
}

func newFakeBatchDeduper() *fakeBatchDeduper {
	return &fakeBatchDeduper{seen: map[string]time.Time{}}
}

func (d *fakeBatchDeduper) CheckAndAdd(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.checkAndAddCalls++
	_, ok := d.seen[key]
	d.seen[key] = time.Time{}
	return ok
}

func (d *fakeBatchDeduper) Check(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.checkCalls++
	_, ok := d.seen[key]
	return ok
}

func (d *fakeBatchDeduper) Close() {}

func (d *fakeBatchDeduper) CheckMany(keys []utils.DedupeKey) []bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.checkMany = append(d.checkMany, append([]utils.DedupeKey(nil), keys...))
	d.ops = append(d.ops, "checkmany")
	if d.shortAnswer {
		return nil
	}
	out := make([]bool, len(keys))
	if d.failOpen {
		return out
	}
	for i, k := range keys {
		_, out[i] = d.seen[k.Key]
	}
	return out
}

func (d *fakeBatchDeduper) AddMany(keys []utils.DedupeKey) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.addMany = append(d.addMany, append([]utils.DedupeKey(nil), keys...))
	names := make([]string, len(keys))
	for i, k := range keys {
		d.seen[k.Key] = k.Time
		names[i] = k.Key
	}
	d.ops = append(d.ops, "addmany:"+strings.Join(names, ","))
}

func (d *fakeBatchDeduper) logOp(op string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ops = append(d.ops, op)
}

func (d *fakeBatchDeduper) has(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[key]
	return ok
}

func (d *fakeBatchDeduper) recordedTime(key string) time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.seen[key]
}

// reset forgets the call counts and the op log, not the recorded keys.
func (d *fakeBatchDeduper) reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.checkMany, d.addMany, d.ops = nil, nil, nil
	d.checkCalls, d.checkAndAddCalls = 0, 0
}

// calls returns the CheckMany and AddMany calls whose keys all satisfy isBlob
// (or none do, for events).
func (d *fakeBatchDeduper) split(isBlob func(string) bool) (blobChecks, eventChecks, blobAdds, eventAdds [][]utils.DedupeKey) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.checkMany {
		if isBlob(c[0].Key) {
			blobChecks = append(blobChecks, c)
		} else {
			eventChecks = append(eventChecks, c)
		}
	}
	for _, c := range d.addMany {
		if isBlob(c[0].Key) {
			blobAdds = append(blobAdds, c)
		} else {
			eventAdds = append(eventAdds, c)
		}
	}
	return
}

func isBlobKey(k string) bool { return strings.HasPrefix(k, "blob-") }

func sizes(calls [][]utils.DedupeKey) []int {
	out := make([]int, len(calls))
	for i, c := range calls {
		out[i] = len(c)
	}
	return out
}

func keyNames(keys []utils.DedupeKey) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = k.Key
	}
	return out
}

// logSink is a sink that logs its Ships into a fakeBatchDeduper's op log, and
// fails the failOn-th Ship call (counting from 1, 0 = never).
type logSink struct {
	captureSink
	d      *fakeBatchDeduper
	fmu    sync.Mutex
	calls  int
	failOn int
}

func (s *logSink) Ship(m *protocol.DataMessage, to time.Duration) error {
	s.fmu.Lock()
	s.calls++
	fail := s.calls == s.failOn
	s.fmu.Unlock()
	if fail {
		return fmt.Errorf("connection to LimaCharlie lost")
	}
	if s.d != nil {
		s.d.logOp("ship")
	}
	return s.captureSink.Ship(m, to)
}

// statsRecorder collects the OnDedupeStats calls.
type statsRecorder struct {
	mu    sync.Mutex
	calls []DedupeStats
}

func (r *statsRecorder) hook(s DedupeStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, s)
}

func (r *statsRecorder) all() []DedupeStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]DedupeStats(nil), r.calls...)
}

// --- fixtures ----------------------------------------------------------------------

// blobServer is a minimal Management API serving one fixed listing page and
// the blobs it points to.
type blobServer struct {
	mu    sync.Mutex
	items []listItem
	blobs map[string]string
	hits  map[string]int
	base  string
}

func newBlobServer(t *testing.T) *blobServer {
	t.Helper()
	b := &blobServer{blobs: map[string]string{}, hits: map[string]int{}}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if r.URL.Path == "/list" {
			writeJSON(w, http.StatusOK, b.items)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/blob/")
		b.hits[id]++
		body, ok := b.blobs[id]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	b.base = s.URL
	return b
}

// add lists a blob with the given contentCreated and events (raw JSON).
func (b *blobServer) add(id, created string, events ...string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.items = append(b.items, listItem{ContentID: id, ContentURI: b.base + "/blob/" + id, ContentCreated: created})
	b.blobs[id] = "[" + strings.Join(events, ",") + "]"
}

func (b *blobServer) listURL() string { return b.base + "/list" }

func (b *blobServer) hitCount(id string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hits[id]
}

// newBatchAdapter returns an adapter wired like the constructor wires one for
// a BatchDeduper, polling srv directly (no background goroutines).
func newBatchAdapter(t *testing.T, srv *blobServer, dd *fakeBatchDeduper, sink uspSink) (*Office365Adapter, *statsRecorder) {
	t.Helper()
	conf := validConfig(t)
	fastRetries(&conf)
	conf.contentRetryLimit = 5 * time.Second
	conf.Deduper = dd
	stats := &statsRecorder{}
	conf.OnDedupeStats = stats.hook
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Office365Adapter{conf: conf, batcher: dd, checker: dd, httpClient: http.DefaultClient,
		uspClient: sink, doStop: utils.NewEvent(), stopCtx: ctx, cancelStop: cancel}, stats
}

func event(id, creationTime string) string {
	if id == "" {
		return fmt.Sprintf(`{"CreationTime":%q,"Operation":"NoId"}`, creationTime)
	}
	return fmt.Sprintf(`{"Id":%q,"CreationTime":%q,"Operation":"Send"}`, id, creationTime)
}

func eventN(prefix string, n int) string {
	return event(fmt.Sprintf("%s-%04d", prefix, n), "2026-10-28T12:34:56")
}

// --- time parsing ------------------------------------------------------------------

func TestParseSourceTime(t *testing.T) {
	want := time.Date(2026, 10, 28, 12, 34, 56, 0, time.UTC)
	for name, tc := range map[string]struct {
		in   string
		want time.Time
	}{
		"audit CreationTime, no zone":         {"2026-10-28T12:34:56", want},
		"fractional seconds, no zone":         {"2026-10-28T12:34:56.789", want.Add(789 * time.Millisecond)},
		"contentCreated with Z":               {"2026-10-28T12:34:56.000Z", want},
		"Z without fraction":                  {"2026-10-28T12:34:56Z", want},
		"fraction with Z":                     {"2026-10-28T12:34:56.789Z", want.Add(789 * time.Millisecond)},
		"offset converted to UTC":             {"2026-10-28T14:34:56+02:00", want},
		"surrounding whitespace":              {" 2026-10-28T12:34:56 ", want},
		"empty":                               {"", time.Time{}},
		"garbage":                             {"yesterday-ish", time.Time{}},
		"date only":                           {"2026-10-28", time.Time{}},
		"out of range month":                  {"2026-13-28T12:34:56", time.Time{}},
		"unix timestamp is not a layout here": {"1793190896", time.Time{}},
	} {
		t.Run(name, func(t *testing.T) {
			got := parseSourceTime(tc.in)
			assert.True(t, tc.want.Equal(got), "want %v got %v", tc.want, got)
			if !tc.want.IsZero() {
				assert.Equal(t, time.UTC, got.Location())
			}
		})
	}
}

// --- constructor wiring ------------------------------------------------------------

func TestDeduperWiring(t *testing.T) {
	const ct = "Audit.General"
	for name, tc := range map[string]struct {
		deduper     utils.Deduper
		wantBatch   bool
		wantChecker bool
	}{
		"default local deduper": {nil, true, true},
		"batch deduper":         {newFakeBatchDeduper(), true, true},
		"checking deduper":      {newSharedDeduper(), false, true},
		"plain deduper":         {newLegacyDeduper(), false, false},
	} {
		t.Run(name, func(t *testing.T) {
			m := newMockO365(t)
			conf := newTestConfig(t, m, ct)
			conf.PollInterval = time.Hour
			conf.Deduper = tc.deduper
			a, _, _ := startAdapter(t, conf, &captureSink{})
			assert.Equal(t, tc.wantBatch, a.batcher != nil)
			assert.Equal(t, tc.wantChecker, a.checker != nil)
		})
	}
}

// --- batching ----------------------------------------------------------------------

// Blobs are checked once per listing page and events once per blob, not once
// per key, and the times handed to the deduper come from the API's own fields.
func TestBatchCallsAndTimes(t *testing.T) {
	const ct = "Audit.Exchange"
	m := newMockO365(t)
	m.listPageSize = 2 // 5 blobs: pages of 2, 2, 1
	created := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Millisecond)
	evTime := created.Add(-time.Minute).Truncate(time.Second)
	for b := 1; b <= 5; b++ {
		var recs []string
		for e := 1; e <= 3; e++ {
			recs = append(recs, auditRecord(t, fmt.Sprintf("eeeeeeee-0000-0000-%04d-%012d", b, e), "Send", "Exchange", "user@example.com", evTime))
		}
		m.addBlob(ct, contentBlob{id: fmt.Sprintf("blob-%d", b), created: created, records: recs})
	}

	dd := newFakeBatchDeduper()
	sink := &logSink{d: dd}
	stats := &statsRecorder{}
	conf := newTestConfig(t, m, ct)
	conf.PollInterval = time.Hour
	conf.Deduper = dd
	conf.OnDedupeStats = stats.hook
	startAdapter(t, conf, sink)

	require.Eventually(t, func() bool { return len(stats.all()) == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, []DedupeStats{{BlobsFetched: 5, EventsShipped: 15}}, stats.all())
	assert.Equal(t, 15, sink.count())

	blobChecks, eventChecks, blobAdds, eventAdds := dd.split(isBlobKey)
	assert.Equal(t, []int{2, 2, 1}, sizes(blobChecks), "one CheckMany per listing page")
	assert.Equal(t, []int{3, 3, 3, 3, 3}, sizes(eventChecks), "one CheckMany per blob")
	assert.Equal(t, []int{1, 1, 1, 1, 1}, sizes(blobAdds), "each blob recorded once, when done")
	assert.Equal(t, []int{3, 3, 3, 3, 3}, sizes(eventAdds), "one AddMany per blob's events")
	assert.Zero(t, dd.checkCalls, "no per-key Check")
	assert.Zero(t, dd.checkAndAddCalls, "no per-key CheckAndAdd")

	for _, c := range blobChecks {
		for _, k := range c {
			assert.True(t, created.Equal(k.Time), "blob key time is contentCreated: %v", k.Time)
		}
	}
	for _, c := range eventChecks {
		for _, k := range c {
			assert.True(t, evTime.Equal(k.Time), "event key time is CreationTime: %v vs %v", k.Time, evTime)
		}
	}
	// What is recorded carries the same time as what was checked.
	assert.True(t, created.Equal(dd.recordedTime("blob-3")))
	assert.True(t, evTime.Equal(dd.recordedTime("eeeeeeee-0000-0000-0003-000000000002")))
}

// A second instance sharing the deduper skips every blob with one call per
// page and downloads nothing.
func TestBatchSecondPollSkipsRecordedBlobs(t *testing.T) {
	srv := newBlobServer(t)
	srv.add("blob-1", "2026-10-28T12:00:00.000Z", eventN("a", 1), eventN("a", 2))
	srv.add("blob-2", "2026-10-28T12:01:00.000Z", eventN("b", 1))
	dd := newFakeBatchDeduper()

	a1, stats1 := newBatchAdapter(t, srv, dd, &logSink{d: dd})
	require.True(t, a1.pollOnce(srv.listURL(), &failureState{}))
	assert.Equal(t, []DedupeStats{{BlobsFetched: 2, EventsShipped: 3}}, stats1.all())

	dd.reset()
	sink2 := &logSink{d: dd}
	a2, stats2 := newBatchAdapter(t, srv, dd, sink2)
	require.True(t, a2.pollOnce(srv.listURL(), &failureState{}))
	assert.Equal(t, []DedupeStats{{BlobsSkipped: 2}}, stats2.all())
	assert.Equal(t, []string{"checkmany"}, dd.ops, "one blob-level check and nothing else")
	assert.Zero(t, sink2.count())
	assert.Equal(t, 1, srv.hitCount("blob-1"), "recorded blobs aren't downloaded again")
}

func TestBatchDuplicatesAndRepeats(t *testing.T) {
	srv := newBlobServer(t)
	dup := event("dup-event", "2026-10-28T12:00:00")
	other := event("other-event", "2026-10-28T12:00:01")
	srv.add("blob-a", "2026-10-28T12:00:00.000Z", dup, other, other, other)
	srv.add("blob-b", "2026-10-28T12:01:00.000Z", dup, event("fresh", "2026-10-28T12:01:00"))
	dd := newFakeBatchDeduper()
	sink := &logSink{d: dd}
	a, stats := newBatchAdapter(t, srv, dd, sink)

	require.True(t, a.pollOnce(srv.listURL(), &failureState{}))

	counts := sink.payloadCounts()
	assert.Equal(t, 1, counts[dup], "cross-blob duplicate ships once")
	assert.Equal(t, 1, counts[other], "in-blob repeats ship once")
	assert.Len(t, counts, 3)
	assert.Equal(t, 3, sink.count())
	assert.Equal(t, []DedupeStats{{BlobsFetched: 2, EventsShipped: 3, EventsDuplicate: 1, EventsRepeated: 2}}, stats.all())

	_, eventChecks, _, eventAdds := dd.split(isBlobKey)
	require.Len(t, eventChecks, 2)
	assert.Equal(t, []string{"dup-event", "other-event"}, keyNames(eventChecks[0]), "in-blob repeats never reach the deduper")
	assert.Equal(t, []string{"dup-event", "fresh"}, keyNames(eventChecks[1]))
	require.Len(t, eventAdds, 2)
	assert.Equal(t, []string{"dup-event", "other-event"}, keyNames(eventAdds[0]))
	assert.Equal(t, []string{"fresh"}, keyNames(eventAdds[1]), "a skipped duplicate isn't recorded again")
}

// Ids shipped by an earlier instance are recorded in the deduper and skipped.
func TestBatchDuplicateRecordedByAnotherInstance(t *testing.T) {
	srv := newBlobServer(t)
	srv.add("blob-1", "2026-10-28T12:00:00.000Z", eventN("x", 1), eventN("x", 2), eventN("x", 3))
	dd := newFakeBatchDeduper()
	dd.AddMany([]utils.DedupeKey{{Key: "x-0002"}})
	dd.reset()
	sink := &logSink{d: dd}
	a, stats := newBatchAdapter(t, srv, dd, sink)

	require.True(t, a.pollOnce(srv.listURL(), &failureState{}))
	assert.Equal(t, 2, sink.count())
	assert.Equal(t, 0, sink.payloadCounts()[eventN("x", 2)])
	assert.Equal(t, []DedupeStats{{BlobsFetched: 1, EventsShipped: 2, EventsDuplicate: 1}}, stats.all())
}

func TestBatchBlobRecordedOnlyAfterAllEventsShipped(t *testing.T) {
	srv := newBlobServer(t)
	srv.add("blob-1", "2026-10-28T12:00:00.000Z", eventN("a", 1), eventN("a", 2), eventN("a", 3))
	dd := newFakeBatchDeduper()
	a, _ := newBatchAdapter(t, srv, dd, &logSink{d: dd})

	require.True(t, a.pollOnce(srv.listURL(), &failureState{}))

	// blob check, event check, 3 ships, events recorded, then the blob.
	assert.Equal(t, []string{"checkmany", "checkmany", "ship", "ship", "ship",
		"addmany:a-0001,a-0002,a-0003", "addmany:blob-1"}, dd.ops)
}

func TestBatchPartialShipFailureRecordsExactlyShippedEvents(t *testing.T) {
	srv := newBlobServer(t)
	srv.add("blob-1", "2026-10-28T12:00:00.000Z", eventN("a", 1), eventN("a", 2), eventN("a", 3), eventN("a", 4), eventN("a", 5))
	dd := newFakeBatchDeduper()
	sink1 := &logSink{d: dd, failOn: 3}
	a1, stats1 := newBatchAdapter(t, srv, dd, sink1)
	cb := &callbacks{}
	cb.install(t, &a1.conf)

	assert.False(t, a1.pollOnce(srv.listURL(), &failureState{}))
	assert.Equal(t, 2, sink1.count())
	assert.True(t, dd.has("a-0001"))
	assert.True(t, dd.has("a-0002"))
	for _, id := range []string{"a-0003", "a-0004", "a-0005", "blob-1"} {
		assert.False(t, dd.has(id), "%s wasn't shipped and must stay eligible", id)
	}
	assert.Len(t, cb.errors(), 1, "the Ship failure is reported")
	assert.Equal(t, []DedupeStats{{EventsShipped: 2}}, stats1.all(), "stats are reported even when the poll aborts")

	// The retry ships the rest, once.
	sink2 := &logSink{d: dd}
	a2, stats2 := newBatchAdapter(t, srv, dd, sink2)
	require.True(t, a2.pollOnce(srv.listURL(), &failureState{}))
	assert.Equal(t, 3, sink2.count())
	for i := 3; i <= 5; i++ {
		assert.Equal(t, 1, sink2.payloadCounts()[eventN("a", i)])
	}
	assert.Equal(t, []DedupeStats{{BlobsFetched: 1, EventsShipped: 3, EventsDuplicate: 2}}, stats2.all())
	assert.True(t, dd.has("blob-1"))
}

// The first Ship failing records nothing, not even an empty AddMany.
func TestBatchShipFailureOnFirstEventRecordsNothing(t *testing.T) {
	srv := newBlobServer(t)
	srv.add("blob-1", "2026-10-28T12:00:00.000Z", eventN("a", 1), eventN("a", 2))
	dd := newFakeBatchDeduper()
	a, _ := newBatchAdapter(t, srv, dd, &logSink{d: dd, failOn: 1})
	cb := &callbacks{}
	cb.install(t, &a.conf)

	assert.False(t, a.pollOnce(srv.listURL(), &failureState{}))
	_, _, blobAdds, eventAdds := dd.split(isBlobKey)
	assert.Empty(t, blobAdds)
	assert.Empty(t, eventAdds)
}

// Events are checked and recorded in chunks of at most 1000, and the
// repeat detection spans the chunks of a blob. A failure in a later chunk
// still records every event of the earlier ones and the shipped part of its own.
func TestBatchChunking(t *testing.T) {
	const n = 2500
	events := make([]string, 0, n+1)
	for i := 1; i <= n; i++ {
		events = append(events, eventN("c", i))
	}
	events = append(events, eventN("c", 1)) // repeat of an event of the first chunk

	t.Run("all shipped", func(t *testing.T) {
		srv := newBlobServer(t)
		srv.add("blob-1", "2026-10-28T12:00:00.000Z", events...)
		dd := newFakeBatchDeduper()
		sink := &logSink{d: dd}
		a, stats := newBatchAdapter(t, srv, dd, sink)

		require.True(t, a.pollOnce(srv.listURL(), &failureState{}))
		assert.Equal(t, n, sink.count())
		assert.Equal(t, []DedupeStats{{BlobsFetched: 1, EventsShipped: n, EventsRepeated: 1}}, stats.all())

		_, eventChecks, _, eventAdds := dd.split(isBlobKey)
		assert.Equal(t, []int{1000, 1000, 500}, sizes(eventChecks))
		assert.Equal(t, []int{1000, 1000, 500}, sizes(eventAdds))
	})

	t.Run("failure in the second chunk", func(t *testing.T) {
		srv := newBlobServer(t)
		srv.add("blob-1", "2026-10-28T12:00:00.000Z", events...)
		dd := newFakeBatchDeduper()
		sink := &logSink{d: dd, failOn: 1500}
		a, _ := newBatchAdapter(t, srv, dd, sink)
		cb := &callbacks{}
		cb.install(t, &a.conf)

		assert.False(t, a.pollOnce(srv.listURL(), &failureState{}))
		assert.Equal(t, 1499, sink.count())
		_, _, blobAdds, eventAdds := dd.split(isBlobKey)
		assert.Empty(t, blobAdds)
		assert.Equal(t, []int{1000, 499}, sizes(eventAdds))
		for i := 1; i <= 1499; i++ {
			require.True(t, dd.has(fmt.Sprintf("c-%04d", i)), "event %d was shipped", i)
		}
		assert.False(t, dd.has("c-1500"))
		assert.False(t, dd.has("c-2500"))
	})
}

// A backend that fails open ships everything, and nothing breaks.
func TestBatchFailOpenShipsEverything(t *testing.T) {
	for name, setup := range map[string]func(*fakeBatchDeduper){
		"all false":    func(d *fakeBatchDeduper) { d.failOpen = true },
		"short answer": func(d *fakeBatchDeduper) { d.shortAnswer = true },
	} {
		t.Run(name, func(t *testing.T) {
			srv := newBlobServer(t)
			srv.add("blob-1", "2026-10-28T12:00:00.000Z", eventN("a", 1), eventN("a", 2))
			dd := newFakeBatchDeduper()
			// Everything was done before, but the backend can't tell.
			dd.AddMany([]utils.DedupeKey{{Key: "blob-1"}, {Key: "a-0001"}, {Key: "a-0002"}})
			dd.reset()
			setup(dd)
			sink := &logSink{d: dd}
			a, stats := newBatchAdapter(t, srv, dd, sink)

			require.True(t, a.pollOnce(srv.listURL(), &failureState{}))
			assert.Equal(t, 2, sink.count(), "unknown means not done: ship")
			assert.Equal(t, 1, srv.hitCount("blob-1"))
			assert.Equal(t, []DedupeStats{{BlobsFetched: 1, EventsShipped: 2}}, stats.all())
		})
	}
}

func TestBatchEventsWithoutIdAlwaysShipAndAreNeverRecorded(t *testing.T) {
	srv := newBlobServer(t)
	noID := event("", "2026-10-28T12:00:00")
	srv.add("blob-1", "2026-10-28T12:00:00.000Z", noID, event("with-id", "2026-10-28T12:00:01"), noID)
	srv.add("blob-2", "2026-10-28T12:01:00.000Z", noID)
	dd := newFakeBatchDeduper()
	sink := &logSink{d: dd}
	a, stats := newBatchAdapter(t, srv, dd, sink)

	require.True(t, a.pollOnce(srv.listURL(), &failureState{}))
	assert.Equal(t, 4, sink.count())
	assert.Equal(t, 3, sink.payloadCounts()[noID], "identical events without an Id are all shipped")
	assert.Equal(t, []DedupeStats{{BlobsFetched: 2, EventsShipped: 4}}, stats.all())

	_, eventChecks, _, eventAdds := dd.split(isBlobKey)
	require.Len(t, eventChecks, 1, "blob-2 has nothing to check")
	assert.Equal(t, []string{"with-id"}, keyNames(eventChecks[0]))
	require.Len(t, eventAdds, 1, "blob-2 has nothing to record")
	assert.Equal(t, []string{"with-id"}, keyNames(eventAdds[0]))
	assert.False(t, dd.has(""))
}

// A blob of only Id-less events doesn't call the deduper at event level.
func TestBatchBlobWithoutIdsSkipsEventCalls(t *testing.T) {
	srv := newBlobServer(t)
	srv.add("blob-1", "2026-10-28T12:00:00.000Z", event("", "2026-10-28T12:00:00"))
	dd := newFakeBatchDeduper()
	a, _ := newBatchAdapter(t, srv, dd, &logSink{d: dd})

	require.True(t, a.pollOnce(srv.listURL(), &failureState{}))
	assert.Equal(t, []string{"checkmany", "ship", "addmany:blob-1"}, dd.ops)
}

// Missing or unparseable times are passed as the zero time, never guessed.
func TestBatchBadAndMissingTimes(t *testing.T) {
	srv := newBlobServer(t)
	srv.add("blob-missing", "", `{"Id":"e-no-time","Operation":"x"}`, event("e-garbage", "not a time"), event("e-empty", ""))
	srv.add("blob-garbage", "last tuesday", event("e-zoned", "2026-10-28T14:34:56+02:00"), event("e-frac", "2026-10-28T12:34:56.5"))
	srv.add("blob-good", "2026-10-28T12:34:56.123Z", event("e-good", "2026-10-28T12:34:56"))
	dd := newFakeBatchDeduper()
	a, _ := newBatchAdapter(t, srv, dd, &logSink{d: dd})

	require.True(t, a.pollOnce(srv.listURL(), &failureState{}))

	good := time.Date(2026, 10, 28, 12, 34, 56, 0, time.UTC)
	for key, want := range map[string]time.Time{
		"blob-missing": {},
		"blob-garbage": {},
		"blob-good":    good.Add(123 * time.Millisecond),
		"e-no-time":    {},
		"e-garbage":    {},
		"e-empty":      {},
		"e-zoned":      good,
		"e-frac":       good.Add(500 * time.Millisecond),
		"e-good":       good,
	} {
		require.True(t, dd.has(key), key)
		assert.True(t, want.Equal(dd.recordedTime(key)), "%s: want %v got %v", key, want, dd.recordedTime(key))
	}

	// The time given when checking is the one given when recording.
	dd.mu.Lock()
	defer dd.mu.Unlock()
	for _, c := range dd.checkMany {
		for _, k := range c {
			assert.True(t, k.Time.Equal(dd.seen[k.Key]), "%s checked with %v, recorded with %v", k.Key, k.Time, dd.seen[k.Key])
		}
	}
}

// A blob with a non-array body still records and doesn't call the event level.
func TestBatchEmptyBlob(t *testing.T) {
	srv := newBlobServer(t)
	srv.add("blob-1", "2026-10-28T12:00:00.000Z")
	dd := newFakeBatchDeduper()
	a, stats := newBatchAdapter(t, srv, dd, &logSink{d: dd})

	require.True(t, a.pollOnce(srv.listURL(), &failureState{}))
	assert.Equal(t, []string{"checkmany", "addmany:blob-1"}, dd.ops)
	assert.Equal(t, []DedupeStats{{BlobsFetched: 1}}, stats.all())
}

func TestBatchEmptyListingDoesNotCallDeduper(t *testing.T) {
	srv := newBlobServer(t)
	dd := newFakeBatchDeduper()
	a, stats := newBatchAdapter(t, srv, dd, &logSink{d: dd})

	require.True(t, a.pollOnce(srv.listURL(), &failureState{}))
	assert.Empty(t, dd.ops)
	assert.Equal(t, []DedupeStats{{}}, stats.all(), "stats are reported once per cycle even when idle")
}

// --- stats hook --------------------------------------------------------------------

// One call per polling cycle, summing the pages, and none without a hook.
func TestStatsHookOncePerCycleAcrossPages(t *testing.T) {
	const ct = "Audit.General"
	m := newMockO365(t)
	m.listPageSize = 1
	created := time.Now().UTC().Add(-5 * time.Minute)
	dup := auditRecord(t, "55550000-1111-1111-1111-111111111111", "AlertEntityGenerated", "SecurityComplianceCenter", "admin@example.com", created)
	other := auditRecord(t, "55550000-2222-2222-2222-222222222222", "AlertTriggered", "SecurityComplianceCenter", "admin@example.com", created)
	m.addBlob(ct, contentBlob{id: "blob-a", created: created, records: []string{dup, other, other}})
	m.addBlob(ct, contentBlob{id: "blob-b", created: created, records: []string{dup}})
	m.addBlob(ct, contentBlob{id: "blob-c", created: created, records: []string{dup, other}})

	stats := &statsRecorder{}
	conf := newTestConfig(t, m, ct)
	conf.OnDedupeStats = stats.hook
	startAdapter(t, conf, &captureSink{})

	require.Eventually(t, func() bool { return len(stats.all()) >= 2 }, 5*time.Second, 10*time.Millisecond)
	calls := stats.all()
	assert.Equal(t, DedupeStats{BlobsFetched: 3, EventsShipped: 2, EventsDuplicate: 3, EventsRepeated: 1}, calls[0],
		"three pages, one call")
	assert.Equal(t, DedupeStats{BlobsSkipped: 3}, calls[1], "later cycles report what they skipped")
}

func TestNoStatsHookIsFine(t *testing.T) {
	srv := newBlobServer(t)
	srv.add("blob-1", "2026-10-28T12:00:00.000Z", eventN("a", 1))
	dd := newFakeBatchDeduper()
	sink := &logSink{d: dd}
	a, _ := newBatchAdapter(t, srv, dd, sink)
	a.conf.OnDedupeStats = nil
	require.True(t, a.pollOnce(srv.listURL(), &failureState{}))
	assert.Equal(t, 1, sink.count())
}

// --- the other dedupers ------------------------------------------------------------

// A deduper that only checks per key keeps working per key, and reports stats.
func TestCheckingDeduperPathUnchanged(t *testing.T) {
	srv := newBlobServer(t)
	dup := event("dup-event", "2026-10-28T12:00:00")
	srv.add("blob-a", "2026-10-28T12:00:00.000Z", dup, event("other", "2026-10-28T12:00:01"))
	srv.add("blob-b", "2026-10-28T12:01:00.000Z", dup)
	dd := newSharedDeduper()
	sink := &captureSink{}
	stats := &statsRecorder{}
	conf := validConfig(t)
	fastRetries(&conf)
	conf.Deduper = dd
	conf.OnDedupeStats = stats.hook
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a := &Office365Adapter{conf: conf, checker: dd, httpClient: http.DefaultClient,
		uspClient: sink, doStop: utils.NewEvent(), stopCtx: ctx, cancelStop: cancel}

	require.True(t, a.pollOnce(srv.listURL(), &failureState{}))
	assert.Equal(t, 2, sink.count())
	assert.Equal(t, 1, sink.payloadCounts()[dup])
	assert.True(t, dd.Check("blob-a"))
	assert.True(t, dd.Check("blob-b"))
	assert.True(t, dd.Check("dup-event"))
	assert.Equal(t, []DedupeStats{{BlobsFetched: 2, EventsShipped: 2, EventsDuplicate: 1}}, stats.all())

	require.True(t, a.pollOnce(srv.listURL(), &failureState{}))
	assert.Equal(t, 2, sink.count())
	assert.Equal(t, DedupeStats{BlobsSkipped: 2}, stats.all()[1])
}

// A deduper that can only check-and-record records before the work, as ever.
func TestPlainDeduperPathUnchanged(t *testing.T) {
	srv := newBlobServer(t)
	dup := event("dup-event", "2026-10-28T12:00:00")
	srv.add("blob-a", "2026-10-28T12:00:00.000Z", dup, event("other", "2026-10-28T12:00:01"))
	srv.add("blob-b", "2026-10-28T12:01:00.000Z", dup)
	dd := newLegacyDeduper()
	sink := &captureSink{}
	stats := &statsRecorder{}
	conf := validConfig(t)
	fastRetries(&conf)
	conf.Deduper = dd
	conf.OnDedupeStats = stats.hook
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a := &Office365Adapter{conf: conf, httpClient: http.DefaultClient,
		uspClient: sink, doStop: utils.NewEvent(), stopCtx: ctx, cancelStop: cancel}

	require.True(t, a.pollOnce(srv.listURL(), &failureState{}))
	assert.Equal(t, 2, sink.count())
	assert.Equal(t, 1, sink.payloadCounts()[dup])
	assert.Equal(t, []DedupeStats{{BlobsFetched: 2, EventsShipped: 2, EventsDuplicate: 1}}, stats.all())

	require.True(t, a.pollOnce(srv.listURL(), &failureState{}))
	assert.Equal(t, 2, sink.count())
	assert.Equal(t, DedupeStats{BlobsSkipped: 2}, stats.all()[1])
}

// With the default local deduper (a BatchDeduper) the adapter ships a
// cross-blob duplicate once, end to end.
func TestDefaultLocalDeduperUsesBatchPath(t *testing.T) {
	const ct = "Audit.General"
	m := newMockO365(t)
	created := time.Now().UTC().Add(-5 * time.Minute)
	dup := auditRecord(t, "66660000-1111-1111-1111-111111111111", "AlertTriggered", "SecurityComplianceCenter", "admin@example.com", created)
	m.addBlob(ct, contentBlob{id: "blob-a", created: created, records: []string{dup}})
	m.addBlob(ct, contentBlob{id: "blob-b", created: created, records: []string{dup}})

	stats := &statsRecorder{}
	sink := &captureSink{}
	conf := newTestConfig(t, m, ct)
	conf.OnDedupeStats = stats.hook
	a, _, _ := startAdapter(t, conf, sink)
	require.NotNil(t, a.batcher)

	require.Eventually(t, func() bool { return len(stats.all()) >= 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, DedupeStats{BlobsFetched: 2, EventsShipped: 1, EventsDuplicate: 1}, stats.all()[0])
	assert.Equal(t, 1, sink.payloadCounts()[dup])
}
