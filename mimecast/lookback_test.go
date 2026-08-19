package usp_mimecast

// Regression tests for the poll window, its pagination and its dedupe
// bookkeeping.
//
// The Mimecast audit index does not make a record retrievable at its
// eventTime; there is an ingestion lag. An adapter whose lookback is shorter
// than that lag asks only for a slice of time the index has not caught up to
// yet, so every poll returns an empty data array and nothing is ever ingested.
// These tests pin the lookback, the dedupe bookkeeping that a wide lookback
// depends on, the timestamp format the API actually returns, and the
// pagination behaviour a wide window makes routine rather than exotic.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newIdleAdapter builds an adapter pointed at serverURL whose background poll
// loop is effectively parked, so a test can drive makeOneRequest by hand and
// get a deterministic result. Any field set on tweak overrides the defaults.
func newIdleAdapter(t *testing.T, serverURL string, sink uspSink, tweak ...func(*MimecastConfig)) *MimecastAdapter {
	t.Helper()
	conf := MimecastConfig{
		ClientOptions: testClientOptions(t),
		ClientId:      "cid",
		ClientSecret:  "secret",
		BaseURL:       serverURL,
		PollInterval:  1 * time.Hour,
	}
	for _, f := range tweak {
		f(&conf)
	}
	require.NoError(t, conf.Validate())
	a, _, err := newMimecastAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// newIdleMock returns a mock wired to a running server plus an adapter parked
// on it, the setup nearly every test below wants.
func newIdleMock(t *testing.T, tweak ...func(*MimecastConfig)) (*mockMimecast, *MimecastAdapter) {
	t.Helper()
	mock := newMockMimecast("cid", "secret")
	server := httptest.NewServer(mock.handler(t))
	t.Cleanup(server.Close)
	return mock, newIdleAdapter(t, server.URL, &captureSink{}, tweak...)
}

func itemIDs(items []utils.Dict) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it["id"].(string))
	}
	return out
}

// --- the defect --------------------------------------------------------------

// TestLookbackReachesBackOverlapPeriod is the core assertion: the requested
// window must reach back a full overlapPeriod.
//
// Against the original code (overlapPeriod = 30s, and a moving cursor fed into
// a max() clamp that could only shrink the window further) the oldest event the
// adapter could ever ask for was 30 seconds old, which is shorter than the
// Mimecast audit index's ingestion lag -- so every poll returned an empty data
// array.
func TestLookbackReachesBackOverlapPeriod(t *testing.T) {
	mock, a := newIdleMock(t)

	_, err := a.makeOneRequest()
	require.NoError(t, err)

	seen := mock.seenWindows()
	require.Len(t, seen, 1)
	lookback := seen[0].seen.Sub(seen[0].start)
	t.Logf("steady-state lookback: %v (overlapPeriod = %v)", lookback.Truncate(time.Second), defaultOverlapPeriod)

	assert.InDelta(t, defaultOverlapPeriod.Seconds(), lookback.Seconds(), 5,
		"the window must reach back a full overlapPeriod")
	assert.Greater(t, lookback, 60*time.Second,
		"the lookback must exceed a realistic Mimecast indexing lag; 30s did not")
	assert.False(t, seen[0].end.Before(seen[0].start), "the window must not be inverted")
}

// TestLookbackDoesNotShrinkAcrossPolls pins the inversion that caused the
// outage. The old code narrowed the window every time it saw an event; the
// window must instead stay a fixed width no matter how many polls run or what
// they return.
func TestLookbackDoesNotShrinkAcrossPolls(t *testing.T) {
	mock, a := newIdleMock(t)
	mock.addEvent(fakeLogonEvent("fake-audit-id-0001", time.Now().Add(-1*time.Minute)))

	for i := 0; i < 3; i++ {
		_, err := a.makeOneRequest()
		require.NoError(t, err)
	}

	for i, w := range mock.seenWindows() {
		lookback := w.seen.Sub(w.start)
		assert.InDelta(t, defaultOverlapPeriod.Seconds(), lookback.Seconds(), 5,
			"poll %d reached back %v; the window must not shrink once events start arriving", i, lookback)
	}
}

// TestLaggedEventIsIngested covers the failing scenario end to end: a record
// whose eventTime is already 5 minutes old by the time Mimecast makes it
// retrievable. Under the previous 30s lookback it was permanently unreachable.
func TestLaggedEventIsIngested(t *testing.T) {
	mock, a := newIdleMock(t)
	mock.setIndexDelay(5 * time.Minute)
	mock.addEvent(fakeLogonEvent("fake-audit-id-lagged", time.Now().Add(-5*time.Minute)))

	items, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Len(t, items, 1, "a 5-minute-old record must be reachable")
	assert.Equal(t, "fake-audit-id-lagged", items[0]["id"])
}

// TestEventStillIndexingIsPickedUpOnALaterPoll is the other half of the lag:
// a record too fresh to be retrievable must not be lost, just deferred.
func TestEventStillIndexingIsPickedUpOnALaterPoll(t *testing.T) {
	mock, a := newIdleMock(t)
	// Not retrievable until 2s after its eventTime. Fixtures truncate
	// eventTime to the second, so the delay has to clear that first.
	mock.setIndexDelay(2 * time.Second)
	mock.addEvent(fakeLogonEvent("fake-audit-id-indexing", time.Now()))

	items, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Empty(t, items, "a record the index has not caught up to yet is not returned")

	require.Eventually(t, func() bool {
		got, err := a.makeOneRequest()
		require.NoError(t, err)
		return len(got) == 1
	}, 5*time.Second, 100*time.Millisecond, "the record must arrive once the index catches up")
}

// TestDedupeRetainsRealEventTimes covers the coupled defect. Mimecast returns
// eventTime with a colonless offset, which time.RFC3339 rejects. The original
// code discarded that parse error, stamped every dedupe entry with the zero
// time, and culled it at the end of the same poll -- so widening the window
// alone would have re-shipped every record on every poll.
func TestDedupeRetainsRealEventTimes(t *testing.T) {
	mock, a := newIdleMock(t)
	mock.addEvent(fakeLogonEvent("fake-audit-id-realfmt", time.Now().Add(-2*time.Minute)))

	first, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Len(t, first, 1, "the record ships on the first poll")

	assert.Len(t, a.dedupe, 1, "the dedupe entry must survive the post-poll cull")

	second, err := a.makeOneRequest()
	require.NoError(t, err)
	assert.Empty(t, second, "the same record must not re-ship on an overlapping poll")
}

// TestEventTimeFormats pins every rendering parseEventTime must accept. The
// documented Mimecast form is the colonless one; the others are covered so a
// future API change cannot silently reintroduce the zero-timestamp bug.
func TestEventTimeFormats(t *testing.T) {
	want := time.Date(2026, 8, 14, 11, 19, 20, 0, time.UTC)

	for _, tc := range []struct {
		name, in string
		want     time.Time
	}{
		{"documented colonless offset", "2026-08-14T11:19:20+0000", want},
		{"rfc3339 Z", "2026-08-14T11:19:20Z", want},
		{"rfc3339 colon offset", "2026-08-14T11:19:20+00:00", want},
		{"fractional seconds", "2026-08-14T11:19:20.123+0000", want.Add(123 * time.Millisecond)},
		{"non-UTC offset", "2026-08-14T07:19:20-0400", want},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseEventTime(tc.in)
			require.NoError(t, err)
			assert.True(t, got.UTC().Equal(tc.want), "parsed %s, want %s", got.UTC(), tc.want)
		})
	}

	_, err := parseEventTime("not a timestamp")
	assert.Error(t, err, "garbage must be reported, not silently zero-valued")
}

// TestUnparseableEventTimeStillDedupes pins the fallback. A record the adapter
// cannot timestamp must still ship exactly once: stamping it with the zero time
// (what the original code did) meant it was culled instantly and re-shipped on
// every subsequent poll.
func TestUnparseableEventTimeStillDedupes(t *testing.T) {
	mock, a := newIdleMock(t)
	broken := fakeLogonEvent("fake-audit-id-broken", time.Now().Add(-1*time.Minute))
	broken["eventTime"] = "" // the API occasionally omits it
	mock.addEventUnfiltered(broken)

	first, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Equal(t, []string{"fake-audit-id-broken"}, itemIDs(first))

	second, err := a.makeOneRequest()
	require.NoError(t, err)
	assert.Empty(t, second, "a record with an unusable eventTime must not re-ship every poll")
}

// TestNonOKReturnsAnError pins that an API failure is reported to the caller.
// The original code returned the (nil) err from a successful http.Client.Do,
// so a 500 was indistinguishable from a successful empty poll.
func TestNonOKReturnsAnError(t *testing.T) {
	mock, a := newIdleMock(t)
	mock.setAuditStatus(http.StatusInternalServerError)

	items, err := a.makeOneRequest()
	assert.Empty(t, items)
	assert.Error(t, err, "a non-200 must surface as an error, not a silent empty poll")
}

// --- pagination --------------------------------------------------------------

// TestPartialPaginationFailureDoesNotLoseEvents is the defect a wide window
// turns from theoretical into routine. Dedupe entries and shipped records must
// move together: recording an id for a page that is then thrown away suppresses
// that record on every later poll, losing it permanently.
//
// A 30-second window essentially never paginated, so this could not bite. A
// 30-minute window on a busy tenant paginates on every poll.
func TestPartialPaginationFailureDoesNotLoseEvents(t *testing.T) {
	mock, a := newIdleMock(t)
	// 120 events at the adapter's pageSize of 50 is three pages.
	base := time.Now().Add(-10 * time.Minute)
	for i := 0; i < 120; i++ {
		mock.addEvent(fakeLogonEvent(fmt.Sprintf("fake-audit-id-%04d", i), base.Add(time.Duration(i)*time.Millisecond)))
	}

	// Page 1 succeeds, page 2 fails.
	mock.failNextAuditRequests(0, http.StatusInternalServerError)

	partial, err := a.makeOneRequest()
	require.Error(t, err, "the failed page must be reported")
	require.Len(t, partial, 50, "the page that was read successfully must still be returned, not discarded")

	// The API recovers. Everything not yet delivered must arrive, and nothing
	// already delivered may repeat.
	rest, err := a.makeOneRequest()
	require.NoError(t, err)

	all := append(itemIDs(partial), itemIDs(rest)...)
	assert.Len(t, all, 120, "every record must be delivered exactly once across the two polls")
	assert.Len(t, dedupeStrings(all), 120, "no record may be delivered twice")
}

func dedupeStrings(in []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, s := range in {
		out[s] = struct{}{}
	}
	return out
}

// TestPaginationStopsOnANonAdvancingToken pins the guard against an infinite
// poll. A server that keeps echoing the same meta.pagination.next would
// otherwise spin until the page cap, hammering the API for minutes.
func TestPaginationStopsOnANonAdvancingToken(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	server := httptest.NewServer(pathologicalPagerHandler(t, stuckToken))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.makeOneRequest()
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("makeOneRequest did not terminate on a non-advancing pagination token")
	}

	assert.Contains(t, logs.joinedWarnings(), "did not advance",
		"stopping early must be reported, not silent")
}

// TestPaginationPageCapIsReportedNotSilent pins that hitting the cap warns.
// Truncating a window silently reads as "we collected everything" when we did
// not.
func TestPaginationPageCapIsReportedNotSilent(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	server := httptest.NewServer(pathologicalPagerHandler(t, endlessPages))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })

	items, err := a.makeOneRequest()
	require.NoError(t, err)
	assert.Len(t, items, maxPagesPerPoll, "the cap must stop pagination at exactly maxPagesPerPoll pages")
	assert.Contains(t, logs.joinedWarnings(), "page cap",
		"a truncated window must be reported")
}

// --- authentication ----------------------------------------------------------

// TestTokenIsFetchedOncePerPollNotPerPage pins the token cache. The OAuth
// exchange used to run inside the pagination loop, so a window spanning N pages
// cost 2N requests against a rate-limited API instead of N+1.
func TestTokenIsFetchedOncePerPollNotPerPage(t *testing.T) {
	mock, a := newIdleMock(t)
	base := time.Now().Add(-10 * time.Minute)
	for i := 0; i < 120; i++ {
		mock.addEvent(fakeLogonEvent(fmt.Sprintf("fake-audit-id-%04d", i), base.Add(time.Duration(i)*time.Millisecond)))
	}

	items, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Len(t, items, 120)

	assert.GreaterOrEqual(t, mock.auditCount(), 3, "120 records at pageSize 50 must span at least 3 pages")
	assert.Equal(t, 1, mock.tokenCount(), "a multi-page window must not re-authenticate per page")

	// A second poll reuses the cached token: the mock advertises expires_in
	// 1800, comfortably longer than this test takes.
	_, err = a.makeOneRequest()
	require.NoError(t, err)
	assert.Equal(t, 1, mock.tokenCount(), "a token still within its lifetime must be reused across polls")
}

// TestExpiredTokenIsRefreshedMidPoll pins recovery from a token that expires
// server-side while cached. Without it, caching would turn a transient 401 into
// a permanently dead adapter.
func TestExpiredTokenIsRefreshedMidPoll(t *testing.T) {
	mock, a := newIdleMock(t)
	mock.addEvent(fakeLogonEvent("fake-audit-id-0001", time.Now().Add(-1*time.Minute)))

	first, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.Equal(t, 1, mock.tokenCount())

	// The token expires server-side, and a new event shows up.
	mock.rotateToken()
	mock.addEvent(fakeLogonEvent("fake-audit-id-0002", time.Now().Add(-1*time.Minute)))

	second, err := a.makeOneRequest()
	require.NoError(t, err, "an expired token must be recovered from, not surfaced as a poll failure")
	assert.Equal(t, []string{"fake-audit-id-0002"}, itemIDs(second))
	assert.Equal(t, 2, mock.tokenCount(), "the adapter must have re-authenticated exactly once")
}

// TestTokenIsNotCachedWithoutExpiresIn pins the conservative fallback: a token
// endpoint that omits expires_in must not cause an indefinitely-cached token,
// but must still be cached long enough to help within one poll.
func TestTokenIsNotCachedWithoutExpiresIn(t *testing.T) {
	mock, a := newIdleMock(t)
	mock.setTokenExpiresIn(0) // omit the field entirely
	base := time.Now().Add(-10 * time.Minute)
	for i := 0; i < 120; i++ {
		mock.addEvent(fakeLogonEvent(fmt.Sprintf("fake-audit-id-%04d", i), base.Add(time.Duration(i)*time.Millisecond)))
	}

	_, err := a.makeOneRequest()
	require.NoError(t, err)
	assert.Equal(t, 1, mock.tokenCount(),
		"even without expires_in the token must be shared across a multi-page window")
	assert.False(t, a.tokenExpiry.IsZero())
	assert.LessOrEqual(t, time.Until(a.tokenExpiry), defaultTokenTTL,
		"a token with no advertised lifetime must not be cached beyond the conservative default")
}

// TestClientSecretWithSpecialCharactersAuthenticates pins the form encoding of
// the credentials exchange. Concatenating the secret into the body unescaped
// turns a "+" into a space and truncates at an "&", producing an authentication
// failure with no clue as to why.
func TestClientSecretWithSpecialCharactersAuthenticates(t *testing.T) {
	const clientID = "client id+with&specials"
	const clientSecret = "s3cr3t+with&specials=and spaces"

	mock := newMockMimecast(clientID, clientSecret)
	mock.addEvent(fakeLogonEvent("fake-audit-id-0001", time.Now().Add(-1*time.Minute)))
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) {
		c.ClientId = clientID
		c.ClientSecret = clientSecret
	})

	items, err := a.makeOneRequest()
	require.NoError(t, err)
	assert.Equal(t, []string{"fake-audit-id-0001"}, itemIDs(items))
}

// --- configuration and bookkeeping -------------------------------------------

// TestOverlapPeriodIsConfigurable pins the escape hatch. 30 minutes is a chosen
// bound, not a measured one; a tenant whose index lag is worse must be able to
// raise it without a rebuild.
func TestOverlapPeriodIsConfigurable(t *testing.T) {
	mock, a := newIdleMock(t, func(c *MimecastConfig) { c.OverlapPeriod = "2h" })
	require.Equal(t, 2*time.Hour, a.overlapPeriod)

	_, err := a.makeOneRequest()
	require.NoError(t, err)

	seen := mock.seenWindows()
	require.Len(t, seen, 1)
	assert.InDelta(t, (2 * time.Hour).Seconds(), seen[0].seen.Sub(seen[0].start).Seconds(), 5)
}

func TestOverlapPeriodValidation(t *testing.T) {
	base := func() MimecastConfig {
		return MimecastConfig{ClientOptions: testClientOptions(t), ClientId: "i", ClientSecret: "s"}
	}

	t.Run("empty means the default", func(t *testing.T) {
		c := base()
		require.NoError(t, c.Validate())
		d, err := c.resolveOverlapPeriod()
		require.NoError(t, err)
		assert.Equal(t, defaultOverlapPeriod, d)
	})

	for _, bad := range []string{"nonsense", "0s", "-5m", "48h"} {
		t.Run("rejects "+bad, func(t *testing.T) {
			c := base()
			c.OverlapPeriod = bad
			assert.Error(t, c.Validate())

			// The container does not call Validate, so the constructor must
			// reject it too rather than starting with a broken window.
			_, _, err := newMimecastAdapter(context.Background(), c, &captureSink{})
			assert.Error(t, err, "the constructor must not accept an overlap_period Validate rejects")
		})
	}
}

// TestDedupeIsCulledAtTheWindowEdge pins that the map stays bounded and that
// the cull cutoff lines up with the window start: culling earlier re-ships,
// culling later grows without limit.
func TestDedupeIsCulledAtTheWindowEdge(t *testing.T) {
	mock, a := newIdleMock(t, func(c *MimecastConfig) { c.OverlapPeriod = "2m" })

	inside := fakeLogonEvent("fake-audit-id-inside", time.Now().Add(-30*time.Second))
	mock.addEvent(inside)

	items, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Len(t, a.dedupe, 1)

	// Plant an entry that is already older than the window and poll again.
	a.dedupe["fake-audit-id-ancient"] = time.Now().Add(-10 * time.Minute).Unix()
	_, err = a.makeOneRequest()
	require.NoError(t, err)

	assert.NotContains(t, a.dedupe, "fake-audit-id-ancient",
		"an entry whose record can no longer be returned must be culled")
	assert.Contains(t, a.dedupe, "fake-audit-id-inside",
		"an entry still inside the window must be kept, or its record re-ships")
}

// TestWarnsWhenRecordsArriveNearTheWindowEdge pins the only outward signal that
// the ingestion lag is approaching overlap_period. Past that point the adapter
// goes back to ingesting nothing with no other symptom -- the exact failure this
// change fixes -- so the approach must be visible while it is still fixable.
func TestWarnsWhenRecordsArriveNearTheWindowEdge(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	mock, a := newIdleMock(t, func(c *MimecastConfig) {
		c.ClientOptions = opts
		c.OverlapPeriod = "10m"
	})

	// 9 minutes of lag against a 10-minute window is past the 80% threshold.
	mock.addEvent(fakeLogonEvent("fake-audit-id-late", time.Now().Add(-9*time.Minute)))

	items, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Len(t, items, 1)

	assert.Contains(t, logs.joinedWarnings(), "overlap_period",
		"records arriving near the window edge must be reported")
}

// TestNoWarningWhenRecordsArrivePromptly is the negative case: the warning must
// not fire in normal operation, or it is noise nobody reads.
func TestNoWarningWhenRecordsArrivePromptly(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	mock, a := newIdleMock(t, func(c *MimecastConfig) {
		c.ClientOptions = opts
		c.OverlapPeriod = "10m"
	})
	mock.addEvent(fakeLogonEvent("fake-audit-id-prompt", time.Now().Add(-30*time.Second)))

	_, err := a.makeOneRequest()
	require.NoError(t, err)
	assert.NotContains(t, logs.joinedWarnings(), "overlap_period")
}

// TestRestartReshipsOnlyWithinTheOverlapWindow documents the accepted cost of a
// fixed window and an in-memory dedupe map: a restart re-ships the records
// still inside the window, and nothing older. Duplicates beat gaps for security
// telemetry, but the bound matters -- an unbounded replay would be a different
// problem.
func TestRestartReshipsOnlyWithinTheOverlapWindow(t *testing.T) {
	mock := newMockMimecast("cid", "secret")
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	mock.addEvent(fakeLogonEvent("fake-audit-id-recent", time.Now().Add(-2*time.Minute)))
	mock.addEvent(fakeLogonEvent("fake-audit-id-old", time.Now().Add(-90*time.Minute)))

	first := newIdleAdapter(t, server.URL, &captureSink{})
	items, err := first.makeOneRequest()
	require.NoError(t, err)
	require.Equal(t, []string{"fake-audit-id-recent"}, itemIDs(items),
		"only the record inside the window is in scope")

	// A restart: a brand-new adapter with an empty dedupe map.
	second := newIdleAdapter(t, server.URL, &captureSink{})
	afterRestart, err := second.makeOneRequest()
	require.NoError(t, err)
	assert.Equal(t, []string{"fake-audit-id-recent"}, itemIDs(afterRestart),
		"a restart re-ships the current window, and only the current window")
}

// --- a deliberately broken pagination server ---------------------------------

type pagerMode int

const (
	// stuckToken keeps returning the same meta.pagination.next forever.
	stuckToken pagerMode = iota
	// endlessPages returns a fresh, advancing token forever.
	endlessPages
)

// pathologicalPagerHandler models a server that never stops paginating. It is
// not a model of Mimecast -- it exists to prove the adapter's own guards
// terminate rather than spinning against a misbehaving API.
func pathologicalPagerHandler(t *testing.T, mode pagerMode) http.HandlerFunc {
	page := 0
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/oauth/token" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token": mockAccessToken,
				"expires_in":   1800,
			})
			return
		}
		assert.Equal(t, "/api/audit/get-audit-events", r.URL.Path)

		next := "stuck-token"
		if mode == endlessPages {
			next = fmt.Sprintf("page-%d", page)
		}
		// One record per page, so the caller can count how far it got.
		record := fakeLogonEvent(fmt.Sprintf("fake-audit-id-page-%d", page), time.Now().Add(-1*time.Minute))
		page++

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"meta": map[string]interface{}{
				"status":     200,
				"pagination": map[string]interface{}{"pageSize": auditPageSize, "next": next},
			},
			"data": []utils.Dict{record},
			"fail": []interface{}{},
		})
	}
}

// --- HTTP client wiring ------------------------------------------------------

// countingTransport counts the requests that flow through it.
type countingTransport struct {
	inner http.RoundTripper
	mu    sync.Mutex
	n     int
}

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return c.inner.RoundTrip(r)
}

func (c *countingTransport) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// TestAllRequestsUseTheConfiguredHTTPClient pins that the adapter actually
// sends through the http.Client it builds at construction. It used to build a
// throwaway client per request, so a.httpClient -- and the dial and request
// timeouts configured on it -- was dead code, and the CloseIdleConnections call
// in Close() drained an always-empty pool. A caller tightening those timeouts
// would have had no effect whatsoever.
func TestAllRequestsUseTheConfiguredHTTPClient(t *testing.T) {
	mock := newMockMimecast("cid", "secret")
	base := time.Now().Add(-10 * time.Minute)
	for i := 0; i < 120; i++ { // 3 pages at pageSize 50
		mock.addEvent(fakeLogonEvent(fmt.Sprintf("fake-audit-id-%04d", i), base.Add(time.Duration(i)*time.Millisecond)))
	}
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{})
	counter := &countingTransport{inner: a.httpClient.Transport}
	a.httpClient.Transport = counter

	items, err := a.makeOneRequest()
	require.NoError(t, err)
	require.Len(t, items, 120)
	require.GreaterOrEqual(t, mock.auditCount(), 3, "120 records at pageSize 50 must span at least 3 pages")

	assert.Equal(t, mock.auditCount()+mock.tokenCount(), counter.count(),
		"every audit page and token exchange must go through the adapter's configured http.Client")
	assert.Greater(t, counter.count(), 0)
}

// TestPartialFailureOnA200IsReported pins that the fail array Mimecast can
// return alongside an HTTP 200 is surfaced. Silence on a degraded response is
// how the original outage stayed invisible.
func TestPartialFailureOnA200IsReported(t *testing.T) {
	opts, logs := recordingClientOptions(t)
	server := httptest.NewServer(partialFailureHandler(t))
	defer server.Close()

	a := newIdleAdapter(t, server.URL, &captureSink{}, func(c *MimecastConfig) { c.ClientOptions = opts })

	items, err := a.makeOneRequest()
	require.NoError(t, err, "a 200 carrying data is not a poll failure")
	require.Len(t, items, 1, "the data alongside the failure must still be collected")

	assert.Contains(t, logs.joinedWarnings(), "partial_failure_1",
		"a fail entry returned with a 200 must be reported")
}

// partialFailureHandler returns a 200 whose envelope carries both a record and
// a populated fail array, the way the API reports a partially-served request.
func partialFailureHandler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/oauth/token" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token": mockAccessToken,
				"expires_in":   1800,
			})
			return
		}
		assert.Equal(t, "/api/audit/get-audit-events", r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"meta": map[string]interface{}{
				"status":     200,
				"pagination": map[string]interface{}{"pageSize": auditPageSize},
			},
			"data": []utils.Dict{fakeLogonEvent("fake-audit-id-partial", time.Now().Add(-1*time.Minute))},
			"fail": []map[string]interface{}{
				{"errors": []map[string]interface{}{
					{"code": "partial_failure_1", "message": "Some data could not be retrieved", "retryable": true},
				}},
			},
		})
	}
}
