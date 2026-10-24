package usp_okta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

func directAdapter(t *testing.T, base string) *OktaAdapter {
	t.Helper()
	endpoint, err := oktaEndpoint(base)
	require.NoError(t, err)
	return &OktaAdapter{conf: OktaConfig{URL: base, ApiKey: "dummy-token", ClientOptions: testClientOptions(t)}, endpoint: endpoint, httpClient: &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func TestOpaquePollingDrainsTimestampTiesAndSleepsOnEmpty(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	var emptyTimes []time.Time
	var firstRequest time.Time
	const total = 350
	published := time.Now().UTC().Format(time.RFC3339Nano)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.URL.Query().Get("until"))
		require.Equal(t, "SSWS dummy-token", r.Header.Get("Authorization"))
		after := r.URL.Query().Get("after")
		start := 0
		if after != "" {
			var err error
			start, err = strconv.Atoi(strings.TrimPrefix(after, "opaque /?=,"))
			require.NoError(t, err)
		} else {
			require.Equal(t, "ASCENDING", r.URL.Query().Get("sortOrder"))
		}
		end := start + 100
		if end > total {
			end = total
		}
		// Lowercase escapes, reserved characters, plus sign and parameter ordering
		// pin the byte-exact whole next URL contract rather than reconstructed since.
		opaque := fmt.Sprintf("http://%s%s?after=opaque+%%2f%%3f%%3d%%2c%d&x=a%%2fb", r.Host, logsURL, end)
		mu.Lock()
		requests = append(requests, r.URL.RequestURI())
		if firstRequest.IsZero() {
			firstRequest = time.Now()
		}
		if start == total {
			emptyTimes = append(emptyTimes, time.Now())
		}
		mu.Unlock()
		w.Header().Set("Link", "<"+opaque+">; rel=\"next\"")
		items := []utils.Dict{}
		for i := start; i < end; i++ {
			items = append(items, utils.Dict{"uuid": fmt.Sprint(i), "published": published})
		}
		_ = json.NewEncoder(w).Encode(items)
	}))
	defer server.Close()
	sink := &captureSink{}
	conf := OktaConfig{URL: server.URL, ApiKey: "dummy-token", ClientOptions: testClientOptions(t), PollInterval: 100 * time.Millisecond}
	a, _, err := newOktaAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	defer a.Close()
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(emptyTimes) >= 3 }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, total, sink.count())
	mu.Lock()
	defer mu.Unlock()
	require.Less(t, emptyTimes[0].Sub(firstRequest), 200*time.Millisecond, "nonempty pages must drain without poll sleeps")
	require.Contains(t, requests[1], "after=opaque+%2f%3f%3d%2c100&x=a%2fb")
	for i := 1; i < len(emptyTimes); i++ {
		require.GreaterOrEqual(t, emptyTimes[i].Sub(emptyTimes[i-1]), 100*time.Millisecond)
	}
}

func TestContinuationAndRedirectNeverForwardCredentials(t *testing.T) {
	var leaked atomic.Int32
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1) }))
	defer foreign.Close()
	var next string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("redirect") == "1" {
			http.Redirect(w, r, foreign.URL+logsURL, http.StatusFound)
			return
		}
		w.Header().Set("Link", "<"+next+">; rel=\"next\"")
		_, _ = w.Write([]byte(`[{"uuid":"one"}]`))
	}))
	defer server.Close()
	a := directAdapter(t, server.URL)
	for _, bad := range []string{foreign.URL + logsURL, server.URL + "/other", server.URL + "/api/v1/%6cogs", server.URL + logsURL + "?access_token=secret", server.URL + logsURL + "#fragment", "not a URL"} {
		next = bad
		page, err := a.requestPage(context.Background(), server.URL+logsURL)
		require.Error(t, err)
		require.Empty(t, page.next)
	}
	_, err := a.requestPage(context.Background(), server.URL+logsURL+"?redirect=1")
	require.Error(t, err)
	require.Zero(t, leaked.Load())
}

func TestNextLinkParsingPreservesOpaqueURL(t *testing.T) {
	raw := "https://example.okta.com/api/v1/logs?after=a,b%2f+z&x=1"
	got, err := nextLink([]string{`<https://example.okta.com/api/v1/logs>; rel="self", <` + raw + `>; title="a,b"; rel="next"`})
	require.NoError(t, err)
	require.Equal(t, raw, got)
	for _, h := range []string{`<a>; rel="self"`, `<a>; rel="next", <b>; rel="next"`, `<a; rel="next"`} {
		_, err = nextLink([]string{h})
		require.Error(t, err)
	}
}

func TestRateBudgetAndRetryAfter(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		limit, remaining string
		wait             bool
	}{{"100", "10", true}, {"100", "11", false}, {"5", "1", true}, {"5", "2", false}} {
		h := http.Header{}
		h.Set("X-Rate-Limit-Limit", tc.limit)
		h.Set("X-Rate-Limit-Remaining", tc.remaining)
		h.Set("X-Rate-Limit-Reset", strconv.FormatInt(now.Add(time.Minute).Unix(), 10))
		d := rateDelay(h, now)
		require.Equal(t, tc.wait, d > 0)
		if tc.wait {
			require.Greater(t, d, 59*time.Second)
		}
	}
	require.Equal(t, 12*time.Second, retryAfter("12", now))
	require.Greater(t, retryAfter(now.Add(time.Minute).UTC().Format(http.TimeFormat), now), 59*time.Second)
	var status int = 429
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "15")
		w.WriteHeader(status)
	}))
	defer server.Close()
	a := directAdapter(t, server.URL)
	page, err := a.requestPage(context.Background(), server.URL+logsURL)
	require.ErrorIs(t, err, errRequestRetry)
	require.GreaterOrEqual(t, page.delay, 15*time.Second)
	status = 503
	_, err = a.requestPage(context.Background(), server.URL+logsURL)
	require.ErrorIs(t, err, errRequestRetry)
}

func TestCredentialFailureDoesNotBecomeExpiredCursor(t *testing.T) {
	status := 400
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"errorCode":"E0000001","errorSummary":"secret response"}`))
	}))
	defer server.Close()
	a := directAdapter(t, server.URL)
	_, err := a.requestPage(context.Background(), server.URL+logsURL+"?after=opaque")
	require.ErrorIs(t, err, errInvalidContinuation)
	for _, code := range []int{401, 403} {
		status = code
		_, err = a.requestPage(context.Background(), server.URL+logsURL+"?after=opaque")
		require.ErrorIs(t, err, errVendorCredential)
		require.NotContains(t, err.Error(), "secret")
	}
}

func TestTransportAndBackoffCancellation(t *testing.T) {
	for _, status := range []int{429, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			called := make(chan struct{}, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case called <- struct{}{}:
				default:
				}
				w.Header().Set("Retry-After", "120")
				w.WriteHeader(status)
			}))
			defer server.Close()
			a, stopped, err := newOktaAdapter(context.Background(), OktaConfig{URL: server.URL, ApiKey: "dummy", ClientOptions: testClientOptions(t), PollInterval: time.Millisecond}, &captureSink{})
			require.NoError(t, err)
			<-called
			start := time.Now()
			require.NoError(t, a.Close())
			require.Less(t, time.Since(start), time.Second)
			<-stopped
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	a := directAdapter(t, server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := a.requestPage(ctx, server.URL+logsURL)
	require.Error(t, err)
	require.True(t, errors.Is(err, errRequestRetry))
}

func TestRetryAfterSurvivesTruncatedOrOversizeErrorBody(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		for _, body := range []string{"truncated", "oversize"} {
			t.Run(fmt.Sprintf("%d/%s", status, body), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Retry-After", "120")
					if body == "truncated" {
						w.Header().Set("Content-Length", "100")
						w.WriteHeader(status)
						fmt.Fprint(w, "short")
						return
					}
					w.WriteHeader(status)
					var block [4096]byte
					for remaining := (16 << 20) + 1; remaining > 0; {
						n := min(remaining, len(block))
						if _, err := w.Write(block[:n]); err != nil {
							return
						}
						remaining -= n
					}
				}))
				defer server.Close()
				a := directAdapter(t, server.URL)
				page, err := a.requestPage(context.Background(), server.URL+logsURL)
				require.ErrorIs(t, err, errRequestRetry)
				require.GreaterOrEqual(t, page.delay, 120*time.Second, "body failure must not discard a vendor header delay")
				require.Empty(t, page.items)
				require.Empty(t, page.next)
			})
		}
	}
}
