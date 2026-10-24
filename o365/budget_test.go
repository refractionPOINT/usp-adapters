package usp_o365

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The second failure arrives late in the budget: remembering only the delay
// left at the deadline would let the next poll violate the vendor's cooldown.
func TestContentBudgetBoundsRetryAfterAndPreservesCooldown(t *testing.T) {
	var failedHits, healthyHits atomic.Int32
	var throttledAt atomic.Int64
	a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/list":
			writeJSON(w, http.StatusOK, []listItem{
				{ContentID: "deferred", ContentURI: "http://" + r.Host + "/deferred"},
				{ContentID: "healthy", ContentURI: "http://" + r.Host + "/healthy"},
			})
		case "/deferred":
			switch failedHits.Add(1) {
			case 1:
				time.Sleep(80 * time.Millisecond)
				w.WriteHeader(http.StatusServiceUnavailable)
			case 2:
				throttledAt.Store(time.Now().UnixNano())
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
			default:
				_, _ = w.Write([]byte(`[{"Id":"recovered-event"}]`))
			}
		case "/healthy":
			healthyHits.Add(1)
			_, _ = w.Write([]byte(`[{"Id":"healthy-event"}]`))
		}
	}))
	a.conf.contentRetryLimit = 160 * time.Millisecond
	a.conf.maxRetryAfter = 2 * time.Second
	cb := &callbacks{}
	cb.install(t, &a.conf)
	fs := &failureState{}
	start := time.Now()
	require.False(t, a.pollOnce(base+"/list", fs))
	require.Less(t, time.Since(start), 500*time.Millisecond, "a long Retry-After must not hold up healthy content")
	require.Equal(t, int32(2), failedHits.Load(), "exhausting the budget must not issue an early retry")
	require.Equal(t, 1, a.uspClient.(*captureSink).count())
	require.False(t, a.checker.Check("deferred"), "the deferred blob must remain eligible")
	require.True(t, a.checker.Check("healthy"))
	left := fs.leftBlobs["deferred"]
	assert.False(t, left.notBefore.Before(time.Unix(0, throttledAt.Load()).Add(time.Second)), "retain the full retry time from the response")
	firstOutage := fs.since

	// Model the next poll arriving before the remaining Retry-After elapses.
	time.Sleep(80 * time.Millisecond)
	assert.False(t, a.pollOnce(base+"/list", fs))
	assert.Equal(t, int32(2), failedHits.Load(), "polling must not retry the blob before Retry-After")
	assert.Equal(t, firstOutage, fs.since, "deferring a blob must not clear a continuous outage")
	assert.Equal(t, left.since, fs.leftBlobs["deferred"].since, "polls must preserve the independent failure clock")

	// An overdue deferred blob must still be reported, including when it
	// expires before its cooldown. Keep the retry time until it can be forgotten.
	left.since = time.Now().Add(-2 * a.conf.failureEscalation)
	left.expires = time.Now().Add(-time.Second)
	fs.leftBlobs["deferred"] = left
	assert.False(t, a.pollOnce(base+"/list", fs))
	assert.Len(t, cb.errors(), 1)
	assert.Equal(t, left.notBefore, fs.leftBlobs["deferred"].notBefore)
	assert.Equal(t, int32(2), failedHits.Load(), "expiry must not discard an outstanding vendor cooldown")

	time.Sleep(time.Until(left.notBefore) + 10*time.Millisecond)
	assert.True(t, a.pollOnce(base+"/list", fs))
	assert.True(t, a.checker.Check("deferred"))
	assert.Equal(t, int32(3), failedHits.Load())
	assert.Equal(t, int32(1), healthyHits.Load(), "the shared checking deduper must skip completed content")
	assert.Equal(t, 2, a.uspClient.(*captureSink).count())
	assert.Empty(t, fs.leftBlobs, "recovery must clear the bounded failure state")
}

func TestContentBudgetCancelsInFlightRequestsAndRecovers(t *testing.T) {
	for _, phase := range []string{"headers", "body", "token refresh"} {
		t.Run(phase, func(t *testing.T) {
			var recoverBlob, cancelled atomic.Bool
			var badHits, healthyHits, tokenHits atomic.Int32
			a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/list":
					writeJSON(w, http.StatusOK, []listItem{
						{ContentID: "hung", ContentURI: "http://" + r.Host + "/hung"},
						{ContentID: "healthy", ContentURI: "http://" + r.Host + "/healthy"},
					})
					return
				case "/token":
					_, _ = io.ReadAll(r.Body)
					n := tokenHits.Add(1)
					if n != 2 {
						expires := 3600
						if n == 1 {
							expires = 1 // force a refresh after the listing
						}
						writeJSON(w, http.StatusOK, map[string]interface{}{
							"token_type": "Bearer", "expires_in": expires, "access_token": testAccessToken,
						})
						return
					}
				case "/healthy":
					healthyHits.Add(1)
					_, _ = w.Write([]byte(`[{"Id":"healthy-event"}]`))
					return
				case "/hung":
					badHits.Add(1)
					if recoverBlob.Load() {
						_, _ = w.Write([]byte(`[{"Id":"recovered-event"}]`))
						return
					}
					if phase == "body" {
						w.Header().Set("Retry-After", "1")
						w.WriteHeader(http.StatusTooManyRequests)
						_, _ = w.Write([]byte(`[{"Id":`))
						w.(http.Flusher).Flush()
					}
				}
				select {
				case <-r.Context().Done():
					cancelled.Store(true)
				case <-time.After(time.Second): // bounded cleanup if cancellation regresses
				}
			}))
			a.conf.contentRetryLimit = 80 * time.Millisecond
			a.conf.maxRetryAfter = 2 * time.Second
			if phase == "token refresh" {
				a.endpointType = "custom"
				a.conf.TokenURL = base + "/token"
				require.NoError(t, a.updateBearerToken())
			}
			cb := &callbacks{}
			cb.install(t, &a.conf)
			fs := &failureState{}
			start := time.Now()
			require.False(t, a.pollOnce(base+"/list", fs))
			require.Less(t, time.Since(start), 400*time.Millisecond, "the content deadline must interrupt the request and let healthy content ship")
			require.Eventually(t, cancelled.Load, time.Second, time.Millisecond, "the vendor request itself must be cancelled")
			assert.Equal(t, 1, a.uspClient.(*captureSink).count())
			assert.Empty(t, cb.errors(), "budget exhaustion is retryable, not fatal")
			assert.False(t, a.checker.Check("hung"))
			assert.False(t, a.checker.Check("recovered-event"))
			assert.Contains(t, fs.leftBlobs, "hung")
			if phase == "body" {
				assert.True(t, fs.leftBlobs["hung"].notBefore.After(start.Add(time.Second)),
					"a body deadline must not discard Retry-After headers already received")
			}
			time.Sleep(time.Until(fs.leftBlobs["hung"].notBefore) + time.Millisecond)

			// A fresh collector sharing the same CheckingDeduper must recover
			// the unshipped blob and skip the healthy blob already committed.
			recoverBlob.Store(true)
			restarted := &Office365Adapter{conf: a.conf, checker: a.checker, httpClient: a.httpClient,
				uspClient: a.uspClient, doStop: utils.NewEvent(), stopCtx: a.stopCtx}
			assert.True(t, restarted.pollOnce(base+"/list", &failureState{}))
			assert.True(t, a.checker.Check("hung"))
			assert.Equal(t, 2, a.uspClient.(*captureSink).count())
			assert.Equal(t, int32(1), healthyHits.Load())
			wantBadHits := int32(2)
			if phase == "token refresh" {
				wantBadHits = 1 // the cancelled refresh never reached the blob
			}
			assert.Equal(t, wantBadHits, badHits.Load())
			assert.Empty(t, cb.errors())
		})
	}
}

func TestContentBudgetDoesNotLimitListingRetries(t *testing.T) {
	var listHits atomic.Int32
	a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/list" {
			if listHits.Add(1) == 1 {
				// Both a request and its retry wait exceed the content budget.
				time.Sleep(80 * time.Millisecond)
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			writeJSON(w, http.StatusOK, []listItem{{ContentID: "healthy", ContentURI: "http://" + r.Host + "/healthy"}})
			return
		}
		_, _ = w.Write([]byte(`[{"Id":"healthy-event"}]`))
	}))
	a.conf.contentRetryLimit = 40 * time.Millisecond
	a.conf.maxRetryAfter = 2 * time.Second
	cb := &callbacks{}
	cb.install(t, &a.conf)
	start := time.Now()
	assert.True(t, a.pollOnce(base+"/list", &failureState{}))
	assert.GreaterOrEqual(t, time.Since(start), time.Second)
	assert.Equal(t, int32(2), listHits.Load())
	assert.Equal(t, 1, a.uspClient.(*captureSink).count())
	assert.Empty(t, cb.errors())
}

func TestContentCooldownAtTrackingCapacity(t *testing.T) {
	var hits atomic.Int32
	a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	a.conf.contentRetryLimit = 40 * time.Millisecond
	a.conf.maxRetryAfter = 2 * time.Second
	cb := &callbacks{}
	cb.install(t, &a.conf)
	fs := &failureState{leftBlobs: make(map[string]leftBlob)}
	now := time.Now()
	for i := 0; i < maxLeftBlobs; i++ {
		fs.leftBlobs[fmt.Sprint(i)] = leftBlob{since: now, expires: now.Add(time.Hour)}
	}
	items := []listItem{{ContentID: "overflow", ContentURI: base}, {ContentID: "untracked", ContentURI: base}}
	complete, abort := a.processItems(items, fs)
	assert.False(t, complete)
	assert.False(t, abort)
	assert.Equal(t, int32(1), hits.Load(), "capacity fallback must defer other untracked blobs conservatively")
	assert.Len(t, fs.leftBlobs, maxLeftBlobs)
	assert.True(t, fs.untrackedNotBefore.After(now.Add(time.Second)))
	a.processItems(items, fs)
	assert.Equal(t, int32(1), hits.Load(), "the bounded fallback must retain the retry time across polls")
	assert.Len(t, cb.errors(), 1, "capacity exhaustion remains visible without flooding")
	assert.False(t, a.checker.Check("overflow"))
	assert.False(t, a.checker.Check("untracked"))
}

// A content request must not inherit another collector's longer token-refresh
// deadline while waiting for the shared token cache lock.
func TestContentBudgetWhileAnotherTokenRefreshRuns(t *testing.T) {
	tokenStarted := make(chan struct{})
	releaseToken := make(chan struct{})
	defer close(releaseToken)
	var tokenHits, contentHits atomic.Int32
	a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_, _ = io.ReadAll(r.Body)
			if tokenHits.Add(1) == 1 {
				close(tokenStarted)
			}
			select {
			case <-releaseToken:
			case <-r.Context().Done():
			case <-time.After(time.Second): // bound a broken lock-cancellation path
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"token_type": "Bearer", "expires_in": 3600, "access_token": testAccessToken,
			})
			return
		}
		contentHits.Add(1)
		_, _ = w.Write([]byte(`[]`))
	}))
	a.endpointType = "custom"
	a.conf.TokenURL = base + "/token"
	require.NoError(t, a.updateBearerToken())
	// Start a listing request whose refresh outlives the content budget.
	listingDone := make(chan struct{})
	go func() {
		defer close(listingDone)
		_, _, _ = a.getWithRetry(base+"/list", "list api", &failureState{}, 0)
	}()
	defer func() {
		a.doStop.Set()
		a.cancelStop()
		<-listingDone
	}()
	select {
	case <-tokenStarted:
	case <-time.After(time.Second):
		t.Fatal("listing did not start its token refresh")
	}
	start := time.Now()
	_, _, err := a.getWithRetry(base+"/content", "content api", &failureState{}, 50*time.Millisecond)
	var ae *apiError
	require.ErrorAs(t, err, &ae)
	assert.True(t, ae.transient)
	assert.Less(t, time.Since(start), 300*time.Millisecond, "waiting for a shared refresh must respect the content deadline")
	assert.Equal(t, int32(1), tokenHits.Load(), "the blocked request must not start another refresh")
	assert.Zero(t, contentHits.Load())
}
