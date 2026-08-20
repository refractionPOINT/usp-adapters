package usp_zendesk

// Regression tests for the poll window, the dedupe key, and cursor pagination.
//
// These three are coupled. Widening the lookback puts more records in a window,
// which makes pagination load-bearing; and a dedupe key that collapses to the
// same value for every record silently drops all but the first record of each
// batch, which a wider window makes worse rather than better.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// numericIDFixture builds an audit log record the way Zendesk actually returns
// one: id typed as an integer, so it arrives as a JSON number. The shared
// fixtures use string ids because the adapter previously could not consume
// anything else.
func numericIDFixture(id int, createdAt time.Time, action string) utils.Dict {
	r := auditLogFixture(fmt.Sprintf("%d", id), createdAt, action)
	r["id"] = id
	return r
}

// newIdleAdapter builds an adapter whose background poll loop is parked, so a
// test can drive makeOneRequest by hand and get a deterministic result.
func newIdleAdapter(t *testing.T, serverURL string, sink uspSink) *ZendeskAdapter {
	t.Helper()
	conf := ZendeskConfig{
		ClientOptions: testClientOptions(t),
		ApiToken:      mockToken,
		ZendeskDomain: mockDomain,
		ZendeskEmail:  mockEmail,
		BaseURL:       serverURL,
		PollInterval:  1 * time.Hour,
	}
	a, _, err := newZendeskAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// TestRecordIDHandlesNumericIDs is the unit-level statement of the dedupe key
// defect: Zendesk types id as an integer, and encoding/json decodes a JSON
// number into a float64, so a plain string assertion yields "" for every
// record.
func TestRecordIDHandlesNumericIDs(t *testing.T) {
	var decoded utils.Dict
	require.NoError(t, json.Unmarshal([]byte(`{"id": 35436}`), &decoded))

	_, wasString := decoded["id"].(string)
	assert.False(t, wasString, "a JSON integer id does not assert to a string")

	assert.Equal(t, "35436", recordID(decoded["id"]))
	assert.Equal(t, "abc", recordID("abc"), "string ids must still work")
	assert.Equal(t, "", recordID(nil))
}

// TestNumericIDsAllShip is the defect in practice. Every record in a batch
// shared the "" dedupe key, so the first record was recorded as seen and every
// record after it was skipped as a duplicate -- roughly one event per poll.
func TestNumericIDsAllShip(t *testing.T) {
	mock := newMockZendesk(mockEmail, mockToken)
	base := time.Now().Add(-2 * time.Minute)
	const total = 5
	for i := 0; i < total; i++ {
		mock.addLog(numericIDFixture(35436+i, base.Add(time.Duration(i)*time.Second), "create"))
	}
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{})
	items, _, err := a.makeOneRequest(time.Now().Add(-24 * time.Hour))
	require.NoError(t, err)

	require.Len(t, items, total, "every record in the batch must ship, not just the first")

	ids := map[string]bool{}
	for _, it := range items {
		ids[recordID(it["id"])] = true
	}
	assert.Len(t, ids, total, "each record must carry a distinct dedupe key")
}

// TestLookbackReachesBackOverlapPeriod pins the steady-state lookback. The
// window start was previously max(since, now-overlapPeriod) fed by a moving
// cursor, so the cursor could only ever shrink the window.
func TestLookbackReachesBackOverlapPeriod(t *testing.T) {
	mock := newMockZendesk(mockEmail, mockToken)
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{})
	sent := time.Now()
	_, _, err := a.makeOneRequest(time.Now().Add(-24 * time.Hour))
	require.NoError(t, err)

	_, _, _, q := mock.lastRequest()
	bounds := q["filter[created_at][]"]
	require.Len(t, bounds, 2, "the window query must carry both bounds")
	start, err := time.Parse(time.RFC3339, bounds[0])
	require.NoError(t, err)

	lookback := sent.UTC().Sub(start)
	t.Logf("steady-state lookback: %v (overlapPeriod = %v)", lookback.Truncate(time.Second), overlapPeriod)
	assert.InDelta(t, overlapPeriod.Seconds(), lookback.Seconds(), 2)
	assert.Greater(t, lookback, 60*time.Second,
		"the lookback must exceed a realistic ingestion lag; 30s did not")
}

// TestLaggedRecordIsIngested covers a record whose created_at is already five
// minutes old by the time the API returns it. Under the previous 30s lookback
// it was permanently unreachable.
func TestLaggedRecordIsIngested(t *testing.T) {
	mock := newMockZendesk(mockEmail, mockToken)
	mock.addLog(numericIDFixture(90001, time.Now().Add(-5*time.Minute), "update"))
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{})
	items, _, err := a.makeOneRequest(time.Now().Add(-24 * time.Hour))
	require.NoError(t, err)
	require.Len(t, items, 1, "a 5-minute-old record must be reachable")
	assert.Equal(t, "90001", recordID(items[0]["id"]))
}

// TestPaginationFollowsNextURL pins cursor pagination. links.next is a full
// URL; the adapter previously fed it back verbatim as the first
// filter[created_at][] value, producing a malformed follow-up request. A wider
// window makes this path load-bearing rather than rarely reached.
func TestPaginationFollowsNextURL(t *testing.T) {
	mock := newMockZendesk(mockEmail, mockToken)
	base := time.Now().Add(-10 * time.Minute)
	const total = 250 // page[size] is 100, so three pages
	for i := 0; i < total; i++ {
		mock.addLog(numericIDFixture(70000+i, base.Add(time.Duration(i)*time.Millisecond*100), "create"))
	}
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{})
	items, _, err := a.makeOneRequest(time.Now().Add(-24 * time.Hour))
	require.NoError(t, err)

	assert.Len(t, items, total, "every page must be consumed")
	assert.GreaterOrEqual(t, mock.cursorRequestCount(), 2,
		"250 records at page[size] 100 require following links.next twice")
}

// TestNonOKReturnsAnError pins that an API failure reaches the caller. The
// previous code returned the nil error from the preceding successful Do.
func TestNonOKReturnsAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{})
	items, _, err := a.makeOneRequest(time.Now().Add(-24 * time.Hour))
	assert.Nil(t, items)
	assert.Error(t, err, "a non-200 must surface as an error, not a silent empty poll")
}
