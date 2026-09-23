package usp_sublime

// Regression tests for the ship cutoff.
//
// This endpoint takes no server-side time filter, so the adapter downloads the
// audit log and decides client-side what to ship. That decision previously used
// a monotonic high-water mark: the newest created_at seen so far. Any event the
// API surfaced late -- with a created_at behind that mark -- was already past
// the cutoff when it appeared and was dropped for good. The cutoff is now a
// window, and dedupe (not the cutoff) is what prevents re-shipping.

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLateArrivingEventIsShipped is the defect. An event that appears in the
// log only after a newer one has already been seen must still ship.
func TestLateArrivingEventIsShipped(t *testing.T) {
	mock := newMockSublime("test-api-key")
	newer := time.Now().Add(-1 * time.Minute)
	mock.appendEvent(realisticAuditEvent(
		"aaaaaaaa-1111-1111-1111-111111111111", "message.view_contents",
		newer.UTC().Format(time.RFC3339Nano)))

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	a := newDirectAdapter(t, server.URL)
	notBefore := time.Now().Add(-overlapPeriod)

	first, _, err := a.makeOneRequest(notBefore)
	require.NoError(t, err)
	require.Len(t, first, 1, "the newer event ships on the first poll")

	// A second event now surfaces whose created_at predates the one already
	// shipped -- the shape an ingestion lag produces.
	mock.appendEvent(realisticAuditEvent(
		"bbbbbbbb-2222-2222-2222-222222222222", "message_group.trash",
		newer.Add(-30*time.Second).UTC().Format(time.RFC3339Nano)))

	second, _, err := a.makeOneRequest(notBefore)
	require.NoError(t, err)
	require.Len(t, second, 1, "a late-arriving older event must still ship")
	assert.Equal(t, "bbbbbbbb-2222-2222-2222-222222222222", second[0]["id"])
}

// TestCutoffReachesBackOverlapPeriod pins that the cutoff is a window rather
// than a high-water mark, and that it is wide enough to clear a realistic lag.
func TestCutoffReachesBackOverlapPeriod(t *testing.T) {
	mock := newMockSublime("test-api-key")
	// Inside the window but far older than the previous 30s behaviour allowed.
	mock.appendEvent(realisticAuditEvent(
		"cccccccc-3333-3333-3333-333333333333", "message.view_contents",
		time.Now().Add(-5*time.Minute).UTC().Format(time.RFC3339Nano)))
	// Outside the window: must not ship.
	mock.appendEvent(realisticAuditEvent(
		"dddddddd-4444-4444-4444-444444444444", "message_group.trash",
		time.Now().Add(-2*time.Hour).UTC().Format(time.RFC3339Nano)))

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	assert.Greater(t, overlapPeriod, 60*time.Second,
		"the cutoff must reach past a realistic ingestion lag; 30s did not")

	a := newDirectAdapter(t, server.URL)
	items, _, err := a.makeOneRequest(time.Now().Add(-overlapPeriod))
	require.NoError(t, err)

	require.Len(t, items, 1, "only the event inside the overlap window ships")
	assert.Equal(t, "cccccccc-3333-3333-3333-333333333333", items[0]["id"])
}

// TestDedupePreventsReshipping confirms the safety property the wider cutoff
// depends on: the same event inside the window must not ship twice.
func TestDedupePreventsReshipping(t *testing.T) {
	mock := newMockSublime("test-api-key")
	mock.appendEvent(realisticAuditEvent(
		"eeeeeeee-5555-5555-5555-555555555555", "message.view_contents",
		time.Now().Add(-2*time.Minute).UTC().Format(time.RFC3339Nano)))

	server := httptest.NewServer(mock.handler())
	defer server.Close()

	a := newDirectAdapter(t, server.URL)
	notBefore := time.Now().Add(-overlapPeriod)

	first, _, err := a.makeOneRequest(notBefore)
	require.NoError(t, err)
	require.Len(t, first, 1)
	assert.Len(t, a.dedupe, 1, "the dedupe entry must survive the post-poll cull")

	second, _, err := a.makeOneRequest(notBefore)
	require.NoError(t, err)
	assert.Empty(t, second, "the same event must not re-ship while inside the window")
}

// TestNonOKReturnsAnError pins that an API failure reaches the caller rather
// than looking like a successful empty poll.
func TestNonOKReturnsAnErrorToCaller(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"message":"upstream"}`))
	}))
	defer server.Close()

	a := newDirectAdapter(t, server.URL)
	items, _, err := a.makeOneRequest(time.Now().Add(-overlapPeriod))
	assert.Nil(t, items)
	assert.Error(t, err)
}
