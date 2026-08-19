package usp_mimecast

// Regression tests for the poll window.
//
// The Mimecast audit index does not make a record retrievable at its
// eventTime; there is an ingestion lag. An adapter whose lookback is shorter
// than that lag asks only for a slice of time the index has not caught up to
// yet, so every poll returns an empty data array and nothing is ever ingested.
// These tests pin the lookback, the dedupe bookkeeping that a wide lookback
// depends on, and the timestamp format the API actually returns.

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

// parseAnyMimecastTime accepts both the RFC3339 "Z" form the existing fixtures
// use and the colonless "+0000" form Mimecast actually returns.
func parseAnyMimecastTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	return time.Parse(mimecastRequestTimeLayout, s)
}

// fakeAuditEventRealFormat is fakeAuditEvent with eventTime rendered the way
// the live Mimecast API renders it: ISO 8601 with a colonless numeric offset.
func fakeAuditEventRealFormat(id string, eventTime time.Time) utils.Dict {
	e := fakeLogonEvent(id, eventTime)
	e["eventTime"] = eventTime.UTC().Truncate(time.Second).Format(mimecastRequestTimeLayout)
	return e
}

// window is one observed (startDateTime, endDateTime) pair as rendered by the
// adapter, plus the wall-clock time the request arrived.
type window struct {
	start, end, seen time.Time
}

// laggingMimecast is a mock of the Mimecast audit API that models the one
// behaviour that breaks a short lookback: an audit record with eventTime T is
// not retrievable until T+indexDelay, even for a request whose window
// contains T.
type laggingMimecast struct {
	mu         sync.Mutex
	events     []utils.Dict
	windows    []window
	indexDelay time.Duration
}

func (m *laggingMimecast) seenWindows() []window {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]window, len(m.windows))
	copy(out, m.windows)
	return out
}

func (m *laggingMimecast) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"access_token": mockAccessToken})
			return
		}
		require.Equal(t, "/api/audit/get-audit-events", r.URL.Path)

		raw, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		var req auditRequestBody
		assert.NoError(t, json.Unmarshal(raw, &req))
		assert.Len(t, req.Data, 1)

		start, errS := time.Parse(mimecastRequestTimeLayout, req.Data[0].StartDateTime)
		end, errE := time.Parse(mimecastRequestTimeLayout, req.Data[0].EndDateTime)
		assert.NoError(t, errS)
		assert.NoError(t, errE)

		now := time.Now()
		m.mu.Lock()
		m.windows = append(m.windows, window{start: start, end: end, seen: now})
		matched := []utils.Dict{}
		for _, e := range m.events {
			ts, perr := parseAnyMimecastTime(e["eventTime"].(string))
			if perr != nil {
				continue
			}
			// Indexing lag: not retrievable yet, regardless of the window.
			if now.Before(ts.Add(m.indexDelay)) {
				continue
			}
			if !ts.Before(start) && !ts.After(end) {
				matched = append(matched, e)
			}
		}
		m.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"meta": map[string]interface{}{"status": 200, "pagination": map[string]interface{}{"pageSize": 50}},
			"data": matched,
			"fail": []interface{}{},
		})
	}
}

// newIdleAdapter builds an adapter pointed at serverURL whose background poll
// loop is effectively parked, so a test can drive makeOneRequest by hand and
// get a deterministic result.
func newIdleAdapter(t *testing.T, serverURL string, sink uspSink) *MimecastAdapter {
	t.Helper()
	conf := MimecastConfig{
		ClientOptions: testClientOptions(t),
		ClientId:      "cid",
		ClientSecret:  "secret",
		BaseURL:       serverURL,
		PollInterval:  1 * time.Hour,
	}
	require.NoError(t, conf.Validate())
	a, _, err := newMimecastAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// --- the defect --------------------------------------------------------------

// TestLookbackReachesBackOverlapPeriod is the core assertion. For an adapter in
// steady state (started long enough ago that the start-up floor is inert), the
// requested window must reach back a full overlapPeriod.
//
// Against the original code (overlapPeriod = 30s) the oldest event the adapter
// could ever ask for was 30 seconds old, which is shorter than the Mimecast
// audit index's ingestion lag -- so every poll returned an empty data array.
func TestLookbackReachesBackOverlapPeriod(t *testing.T) {
	mock := &laggingMimecast{}
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{})

	steadyState := time.Now().Add(-24 * time.Hour) // adapter has been up a day
	_, _, err := a.makeOneRequest(steadyState)
	require.NoError(t, err)

	seen := mock.seenWindows()
	require.Len(t, seen, 1)
	lookback := seen[0].seen.Sub(seen[0].start)
	t.Logf("steady-state lookback: %v (overlapPeriod = %v)", lookback.Truncate(time.Second), overlapPeriod)

	assert.InDelta(t, overlapPeriod.Seconds(), lookback.Seconds(), 2,
		"the window must reach back a full overlapPeriod")
	assert.Greater(t, lookback, 60*time.Second,
		"the lookback must exceed a realistic Mimecast indexing lag; 30s did not")
}

// TestLaggedEventIsIngested covers the failing scenario end to end: a record
// whose eventTime is already 5 minutes old by the time Mimecast makes it
// retrievable. Under the previous 30s lookback it was permanently unreachable.
func TestLaggedEventIsIngested(t *testing.T) {
	mock := &laggingMimecast{}
	lagged := fakeAuditEventRealFormat("fake-audit-id-lagged", time.Now().Add(-5*time.Minute))
	mock.events = append(mock.events, lagged)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	a := newIdleAdapter(t, server.URL, sink)

	items, _, err := a.makeOneRequest(time.Now().Add(-24 * time.Hour))
	require.NoError(t, err)
	require.Len(t, items, 1, "a 5-minute-old record must be reachable")
	assert.Equal(t, "fake-audit-id-lagged", items[0]["id"])
}

// TestDedupeRetainsRealEventTimes covers the coupled defect. Mimecast returns
// eventTime with a colonless offset, which time.RFC3339 rejects. The original
// code discarded that parse error, stamped every dedupe entry with the zero
// time, and culled it at the end of the same poll -- so widening the window
// alone would have re-shipped every record on every poll.
func TestDedupeRetainsRealEventTimes(t *testing.T) {
	mock := &laggingMimecast{}
	mock.events = append(mock.events, fakeAuditEventRealFormat("fake-audit-id-realfmt", time.Now().Add(-2*time.Minute)))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	a := newIdleAdapter(t, server.URL, sink)
	steadyState := time.Now().Add(-24 * time.Hour)

	first, _, err := a.makeOneRequest(steadyState)
	require.NoError(t, err)
	require.Len(t, first, 1, "the record ships on the first poll")

	assert.Len(t, a.dedupe, 1, "the dedupe entry must survive the post-poll cull")

	second, _, err := a.makeOneRequest(steadyState)
	require.NoError(t, err)
	assert.Empty(t, second, "the same record must not re-ship on an overlapping poll")
}

// TestEventTimeFormatIsNotRFC3339 pins the format mismatch itself.
func TestEventTimeFormatIsNotRFC3339(t *testing.T) {
	const documented = "2026-08-14T11:19:20+0000" // Mimecast API 2.0 eventTime format

	_, err := time.Parse(time.RFC3339, documented)
	assert.Error(t, err, "time.RFC3339 cannot parse Mimecast's documented eventTime")

	parsed, err := parseEventTime(documented)
	assert.NoError(t, err, "parseEventTime must accept the documented form")
	assert.False(t, parsed.IsZero())

	// And it must still accept the RFC3339 form the existing fixtures use.
	parsed, err = parseEventTime("2026-08-14T11:19:20Z")
	assert.NoError(t, err)
	assert.False(t, parsed.IsZero())
}

// TestNonOKReturnsAnError pins that an API failure is reported to the caller.
// The original code returned the (nil) err from a successful http.Client.Do,
// so a 500 was indistinguishable from a successful empty poll.
func TestNonOKReturnsAnError(t *testing.T) {
	mock := newMockMimecast("cid", "secret")
	mock.auditStatus = http.StatusInternalServerError
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{})
	items, _, err := a.makeOneRequest(time.Now().Add(-24 * time.Hour))
	assert.Nil(t, items)
	assert.Error(t, err, "a non-200 must surface as an error, not a silent empty poll")
}
