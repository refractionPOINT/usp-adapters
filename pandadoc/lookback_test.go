package usp_pandadoc

// Regression tests for the poll window and the dedupe bookkeeping it depends
// on. A lookback shorter than the API's ingestion lag asks only for a slice of
// time the backend has not caught up to yet, so every poll returns an empty
// result set and nothing is ever ingested.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// documentedTimeLayout renders request_time the way PandaDoc documents it:
// millisecond precision with no zone designator, e.g. "2024-07-15T18:59:38.000".
// time.RFC3339Nano rejects this form.
const documentedTimeLayout = "2006-01-02T15:04:05.000"

// windowRecorder is a minimal mock of the log list endpoint that records the
// since/to bounds of every request it serves.
type windowRecorder struct {
	mu      sync.Mutex
	entries []utils.Dict
	windows [][2]string
	seen    []time.Time
}

func (m *windowRecorder) observed() ([][2]string, []time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w := make([][2]string, len(m.windows))
	copy(w, m.windows)
	s := make([]time.Time, len(m.seen))
	copy(s, m.seen)
	return w, s
}

func (m *windowRecorder) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		since, to := q.Get("since"), q.Get("to")

		m.mu.Lock()
		m.windows = append(m.windows, [2]string{since, to})
		m.seen = append(m.seen, time.Now())
		start, errS := time.ParseInLocation(documentedTimeLayout, since, time.UTC)
		end, errE := time.ParseInLocation(documentedTimeLayout, to, time.UTC)
		matched := []utils.Dict{}
		if errS == nil && errE == nil {
			for _, e := range m.entries {
				ts, perr := parseLogTime(e["request_time"].(string))
				if perr != nil {
					continue
				}
				if !ts.Before(start) && !ts.After(end) {
					matched = append(matched, e)
				}
			}
		}
		m.mu.Unlock()

		assert.NoError(t, errS, "since must use the documented layout")
		assert.NoError(t, errE, "to must use the documented layout")

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": matched})
	}
}

// documentedEntry builds a log entry whose request_time uses the zone-less
// form PandaDoc documents, rather than the RFC3339 form the fixtures use.
func documentedEntry(id string, requestTime time.Time) utils.Dict {
	e := auditLogEntry(id, requestTime)
	e["request_time"] = requestTime.UTC().Truncate(time.Millisecond).Format(documentedTimeLayout)
	return e
}

// newIdleAdapter builds an adapter whose background poll loop is parked, so a
// test can drive makeOneRequest by hand and get a deterministic result.
func newIdleAdapter(t *testing.T, serverURL string, sink uspSink) *PandaDocAdapter {
	t.Helper()
	conf := PandaDocConfig{
		ClientOptions: testClientOptions(t),
		ApiKey:        "test-api-key",
		URL:           serverURL,
		PollInterval:  1 * time.Hour,
	}
	a, _, err := newPandaDocAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// TestLookbackReachesBackOverlapPeriod pins the steady-state lookback. The
// window start was previously max(since, now-overlapPeriod) fed by a moving
// cursor, so the cursor could only ever shrink the window.
func TestLookbackReachesBackOverlapPeriod(t *testing.T) {
	mock := &windowRecorder{}
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{})
	_, _, err := a.makeOneRequest(time.Now().Add(-24 * time.Hour))
	require.NoError(t, err)

	windows, seen := mock.observed()
	require.Len(t, windows, 1)
	start, err := time.ParseInLocation(documentedTimeLayout, windows[0][0], time.UTC)
	require.NoError(t, err)

	lookback := seen[0].UTC().Sub(start)
	t.Logf("steady-state lookback: %v (overlapPeriod = %v)", lookback.Truncate(time.Second), overlapPeriod)
	assert.InDelta(t, overlapPeriod.Seconds(), lookback.Seconds(), 2)
	assert.Greater(t, lookback, 60*time.Second,
		"the lookback must exceed a realistic ingestion lag; 30s did not")
}

// TestLaggedEntryIsIngested covers a record whose request_time is already five
// minutes old by the time the API returns it. Under the previous 30s lookback
// it was permanently unreachable.
func TestLaggedEntryIsIngested(t *testing.T) {
	mock := &windowRecorder{}
	mock.entries = append(mock.entries, documentedEntry("fake-log-lagged", time.Now().Add(-5*time.Minute)))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{})
	items, _, err := a.makeOneRequest(time.Now().Add(-24 * time.Hour))
	require.NoError(t, err)
	require.Len(t, items, 1, "a 5-minute-old record must be reachable")
	assert.Equal(t, "fake-log-lagged", items[0]["id"])
}

// TestDedupeRetainsDocumentedRequestTime is the coupled defect. PandaDoc
// documents request_time without a zone; time.RFC3339Nano rejects that, the
// error was discarded, every dedupe entry was stamped with the zero time and
// culled at the end of the same poll. Widening the window without fixing this
// would have re-shipped every record on every poll.
func TestDedupeRetainsDocumentedRequestTime(t *testing.T) {
	mock := &windowRecorder{}
	mock.entries = append(mock.entries, documentedEntry("fake-log-realfmt", time.Now().Add(-2*time.Minute)))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{})
	steady := time.Now().Add(-24 * time.Hour)

	first, _, err := a.makeOneRequest(steady)
	require.NoError(t, err)
	require.Len(t, first, 1, "the record ships on the first poll")
	assert.Len(t, a.dedupe, 1, "the dedupe entry must survive the post-poll cull")

	second, _, err := a.makeOneRequest(steady)
	require.NoError(t, err)
	assert.Empty(t, second, "the same record must not re-ship on an overlapping poll")
}

// TestParseLogTimeAcceptsDocumentedForm pins the format mismatch itself.
func TestParseLogTimeAcceptsDocumentedForm(t *testing.T) {
	const documented = "2024-07-15T18:59:38.000" // PandaDoc's own documented example

	_, err := time.Parse(time.RFC3339Nano, documented)
	assert.Error(t, err, "time.RFC3339Nano cannot parse the documented request_time")

	parsed, err := parseLogTime(documented)
	assert.NoError(t, err)
	assert.False(t, parsed.IsZero())

	// The zoned form the fixtures use must still parse.
	parsed, err = parseLogTime("2024-07-15T18:59:38.000Z")
	assert.NoError(t, err)
	assert.False(t, parsed.IsZero())
}

// TestNonOKReturnsAnError pins that an API failure reaches the caller. The
// previous code returned the nil error from the preceding successful Do.
func TestNonOKReturnsAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"detail":"boom"}`))
	}))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{})
	items, _, err := a.makeOneRequest(time.Now().Add(-24 * time.Hour))
	assert.Nil(t, items)
	assert.Error(t, err, "a non-200 must surface as an error, not a silent empty poll")
}
