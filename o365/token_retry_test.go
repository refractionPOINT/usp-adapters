package usp_o365

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tokenSuccess(w http.ResponseWriter, expires int) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"token_type": "Bearer", "expires_in": expires, "access_token": testAccessToken,
	})
}

func enableTokenFixture(t *testing.T, a *Office365Adapter, base string) {
	t.Helper()
	a.endpointType = "custom"
	a.conf.TokenURL = base + "/token"
	require.NoError(t, a.updateBearerToken())
}

// A successful, short-lived token primes OAuth2's auth-style cache before the
// throttle. Both another collector and a later poll must share the cooldown,
// even after the first content request exhausts its own budget.
func TestTokenRefreshRetryAfterShared(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var tokenHits, contentHits atomic.Int32
			var failedAt, recoveredAt atomic.Int64
			a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/token" {
					_, _ = io.ReadAll(r.Body)
					switch tokenHits.Add(1) {
					case 1:
						tokenSuccess(w, 1)
					case 2:
						failedAt.Store(time.Now().UnixNano())
						w.Header().Set("Retry-After", "2")
						writeJSON(w, status, map[string]string{"error": "temporarily_unavailable"})
					default:
						recoveredAt.Store(time.Now().UnixNano())
						tokenSuccess(w, 3600)
					}
					return
				}
				contentHits.Add(1)
				_, _ = w.Write([]byte(`[{"Id":"healthy-event"}]`))
			}))
			a.conf.retryBaseDelay = defaultRetryBaseDelay
			a.conf.retryMaxDelay = defaultRetryMaxDelay
			a.conf.maxRetryAfter = defaultMaxRetryAfter
			a.conf.contentRetryLimit = time.Second
			enableTokenFixture(t, a, base)
			cb := &callbacks{}
			cb.install(t, &a.conf)
			_, _, err := a.get(a.stopCtx, base+"/prime")
			require.NoError(t, err)

			_, _, err = a.getWithRetry(base+"/first", "content api", &failureState{}, 60*time.Millisecond)
			var ae *apiError
			require.ErrorAs(t, err, &ae)
			require.True(t, ae.transient)
			require.Greater(t, ae.retryAfter, time.Second, "token failures must preserve the remaining Retry-After")
			require.Equal(t, int32(2), tokenHits.Load())

			// A POST waiting at the shared endpoint boundary must close its
			// original request body when its own deadline expires.
			body := &tokenRetryBody{Reader: strings.NewReader("synthetic request")}
			ctx, cancel := context.WithTimeout(a.stopCtx, 40*time.Millisecond)
			post, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/post", body)
			require.NoError(t, err)
			_, err = a.httpClient.Do(post)
			cancel()
			require.ErrorAs(t, err, &ae)
			assert.True(t, ae.transient)
			assert.True(t, body.closed.Load(), "a cancelled cooldown must close the API request body")
			assert.Equal(t, int32(2), tokenHits.Load())

			// A collector with no content deadline waits for recovery. Short
			// content requests still stop while waiting for its refresh lock.
			listingDone := make(chan error, 1)
			go func() {
				_, _, err := a.getWithRetry(base+"/list", "list api", &failureState{}, 0)
				listingDone <- err
			}()
			t.Cleanup(func() { a.cancelStop() })
			for _, path := range []string{"other-collector", "later-poll"} {
				start := time.Now()
				_, _, err = a.getWithRetry(base+"/"+path, "content api", &failureState{}, 50*time.Millisecond)
				require.ErrorAs(t, err, &ae)
				assert.True(t, ae.transient)
				assert.Less(t, time.Since(start), 400*time.Millisecond)
				assert.Equal(t, int32(2), tokenHits.Load(), "another collector must not refresh before Retry-After")
				assert.Equal(t, int32(1), contentHits.Load(), "no content request may use the expired token")
			}
			select {
			case err := <-listingDone:
				require.NoError(t, err)
			case <-time.After(4 * time.Second):
				a.cancelStop()
				t.Fatal("listing failed to recover after the shared cooldown")
			}
			assert.GreaterOrEqual(t, time.Duration(recoveredAt.Load()-failedAt.Load()), 2*time.Second)
			require.Equal(t, int32(3), tokenHits.Load())

			// Concurrent healthy calls reuse the recovered token. A complete
			// blob still ships and commits through the normal collector path.
			var wg sync.WaitGroup
			for range 20 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, _, err := a.get(a.stopCtx, base+"/cached")
					assert.NoError(t, err)
				}()
			}
			wg.Wait()
			// The recovered cached-token path must also close POST bodies.
			body = &tokenRetryBody{Reader: strings.NewReader("synthetic request")}
			post, err = http.NewRequestWithContext(a.stopCtx, http.MethodPost, base+"/post", body)
			require.NoError(t, err)
			resp, err := a.httpClient.Do(post)
			require.NoError(t, err)
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			require.Eventually(t, body.closed.Load, time.Second, time.Millisecond)

			complete, abort := a.processItems([]listItem{{ContentID: "healthy-blob", ContentURI: base + "/healthy"}}, &failureState{})
			assert.True(t, complete)
			assert.False(t, abort)
			assert.True(t, a.checker.Check("healthy-blob"))
			assert.Equal(t, 1, a.uspClient.(*captureSink).count())
			assert.Equal(t, int32(3), tokenHits.Load(), "cached-token concurrency must not trigger refreshes")
			assert.Empty(t, cb.errors())
		})
	}
}

// AuthStyleAutoDetect may issue a second request inside a single Token call.
// Enforce Retry-After at that HTTP boundary without choosing an auth style.
func TestTokenInitialProbeRetryAfterCap(t *testing.T) {
	for _, tc := range []struct{ header, fallback string }{
		{"3600", ""},
		{"9223372036854775807", "invalid"},
		{time.Now().Add(time.Hour).UTC().Format(http.TimeFormat), "-1"},
	} {
		t.Run(tc.header, func(t *testing.T) {
			var tokenHits atomic.Int32
			var firstAt, secondAt, fallbackAt, fallbackRecoveryAt atomic.Int64
			var firstBasic, secondBasic atomic.Bool
			var secondClientID atomic.Bool
			a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/token" {
					_, _ = w.Write([]byte(`[]`))
					return
				}
				_, _, basic := r.BasicAuth()
				_ = r.ParseForm()
				switch tokenHits.Add(1) {
				case 1:
					firstAt.Store(time.Now().UnixNano())
					firstBasic.Store(basic)
					w.Header().Set("Retry-After", tc.header)
					writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "temporarily_unavailable"})
				case 2:
					secondAt.Store(time.Now().UnixNano())
					secondBasic.Store(basic)
					secondClientID.Store(r.Form.Get("client_id") == tokenRetryClientID)
					tokenSuccess(w, 1)
				case 3:
					fallbackAt.Store(time.Now().UnixNano())
					w.Header().Set("Retry-After", tc.fallback)
					writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "temporarily_unavailable"})
				default:
					fallbackRecoveryAt.Store(time.Now().UnixNano())
					tokenSuccess(w, 3600)
				}
			}))
			a.conf.maxRetryAfter = 80 * time.Millisecond
			a.conf.retryBaseDelay = 80 * time.Millisecond
			a.conf.retryMaxDelay = 80 * time.Millisecond
			// Use synthetic client credentials for both auth styles.
			a.conf.ClientID = tokenRetryClientID
			enableTokenFixture(t, a, base)
			ctx, cancel := context.WithTimeout(a.stopCtx, time.Second)
			defer cancel()
			_, _, err := a.get(ctx, base+"/content")
			require.NoError(t, err)
			assert.Equal(t, int32(2), tokenHits.Load())
			delay := time.Duration(secondAt.Load() - firstAt.Load())
			assert.GreaterOrEqual(t, delay, 80*time.Millisecond, "internal auth-style probing must honor the capped cooldown")
			assert.Less(t, delay, 500*time.Millisecond, "a large header must remain capped")
			assert.True(t, firstBasic.Load())
			assert.False(t, secondBasic.Load())
			assert.True(t, secondClientID.Load(), "retain OAuth2's normal auth-style negotiation")

			// Absent, invalid and negative headers must still use the normal
			// jittered retry backoff once the auth style is cached.
			_, _, err = a.getWithRetry(base+"/fallback", "content api", &failureState{}, time.Second)
			require.NoError(t, err)
			assert.Equal(t, int32(4), tokenHits.Load())
			delay = time.Duration(fallbackRecoveryAt.Load() - fallbackAt.Load())
			assert.GreaterOrEqual(t, delay, 40*time.Millisecond)
			assert.Less(t, delay, 500*time.Millisecond)
		})
	}
}

const tokenRetryClientID = "token-retry-test-client"

// OAuth2 drops the HTTP response when reading the error body fails. Retain
// its status and cooldown even if the request deadline also cancels a probe.
func TestTokenRetryAfterStalledErrorBody(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusUnauthorized} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var tokenHits atomic.Int32
			var bodyCancelled atomic.Bool
			a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/token" {
					t.Error("content must not be requested without a valid token")
					return
				}
				tokenHits.Add(1)
				_, _ = io.ReadAll(r.Body)
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":`))
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
					bodyCancelled.Store(true)
				case <-time.After(time.Second):
				}
			}))
			a.conf.maxRetryAfter = defaultMaxRetryAfter
			enableTokenFixture(t, a, base)
			ctx, cancel := context.WithTimeout(a.stopCtx, 60*time.Millisecond)
			defer cancel()
			_, _, err := a.get(ctx, base+"/content")
			var ae *apiError
			require.ErrorAs(t, err, &ae)
			assert.Equal(t, status != http.StatusUnauthorized, ae.transient, "body cancellation must preserve the HTTP failure classification")
			assert.Contains(t, ae.Error(), http.StatusText(status))
			if status != http.StatusUnauthorized {
				assert.Greater(t, ae.retryAfter, 500*time.Millisecond, "capture cooldown headers before reading the body")
			} else {
				assert.Zero(t, ae.retryAfter, "credential rejection must not enter a cooldown")
			}
			assert.Equal(t, int32(1), tokenHits.Load(), "a cancelled probe must not issue another HTTP request")
			require.Eventually(t, bodyCancelled.Load, time.Second, time.Millisecond, "cancel and close the stalled response body")
		})
	}
}

// The first token request can throttle during subscription registration, before
// any token or auth style is cached. Close must interrupt the internal probe's
// cooldown before a subscription request reaches the server.
func TestTokenRetryAfterCloseDuringRegistration(t *testing.T) {
	var tokenHits, registrationHits atomic.Int32
	a, base := pollFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			tokenHits.Add(1)
			_, _ = io.ReadAll(r.Body)
			w.Header().Set("Retry-After", "2")
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "temporarily_unavailable"})
			return
		}
		registrationHits.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	a.transport = a.httpClient.Transport.(*http.Transport)
	a.conf.maxRetryAfter = defaultMaxRetryAfter
	enableTokenFixture(t, a, base)
	cb := &callbacks{}
	cb.install(t, &a.conf)
	done := make(chan error, 1)
	a.wgSenders.Add(1)
	go func() {
		defer a.wgSenders.Done()
		_, err := a.makeOneRegistrationRequest(base + "/register")
		done <- err
	}()
	require.Eventually(t, func() bool { return tokenHits.Load() > 0 }, time.Second, time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	start := time.Now()
	require.NoError(t, a.Close())
	assert.Less(t, time.Since(start), 400*time.Millisecond)
	var ae *apiError
	require.ErrorAs(t, <-done, &ae)
	assert.True(t, ae.transient)
	assert.Greater(t, ae.retryAfter, time.Second)
	assert.Contains(t, ae.Error(), "Service Unavailable")
	assert.Equal(t, int32(1), tokenHits.Load(), "Close must not allow an early auth-style retry")
	assert.Zero(t, registrationHits.Load())
}

// Track the body owned by the authenticated API request, which must be closed
// even if obtaining a token fails before that request reaches the network.
type tokenRetryBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *tokenRetryBody) Close() error {
	b.closed.Store(true)
	return nil
}
