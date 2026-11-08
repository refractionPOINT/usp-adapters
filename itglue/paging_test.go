package usp_itglue

import (
	"encoding/json"
	"github.com/refractionPOINT/go-uspclient"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

func TestPagingFailuresRetryExactPage(t *testing.T) {
	var mu sync.Mutex
	hits := map[string]int{}
	var firstQuery string
	var changed bool
	var firstTailHits int
	stamp := time.Now()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		page := r.URL.Query().Get("page[number]")
		hits[page]++
		if page == "" && hits["2"] == 0 {
			if firstQuery == "" {
				firstQuery = r.URL.RawQuery
			}
			if r.URL.RawQuery != firstQuery {
				changed = true
			}
		}
		if hits[page] == 1 {
			w.WriteHeader(503)
			return
		}
		var next interface{}
		id := "first"
		if page == "" {
			next = "http://" + r.Host + "/logs?page[number]=2"
		} else {
			id = "tail"
			if firstTailHits == 0 {
				firstTailHits = hits[page]
			}
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data": []utils.Dict{realisticLogRecord(id, stamp)}, "links": map[string]interface{}{"next": next},
		})
	}))
	defer server.Close()
	adapter, _, sink := startAdapter(t, server.URL, "test-only")
	defer adapter.Close()
	require.Eventually(t, func() bool { return sink.idsShipped()["tail"] != 0 }, 5*time.Second, time.Millisecond)
	adapter.doStop.Set()
	adapter.wgSenders.Wait()
	mu.Lock()
	defer mu.Unlock()
	require.False(t, changed, "first-page time filter must survive errors")
	require.Equal(t, 2, firstTailHits, "retry the failed continuation before polling again")
}

type pagingTransport func(*http.Request) (*http.Response, error)

func (f pagingTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPagingLinksStayOnConfiguredOrigin(t *testing.T) {
	for _, tc := range []struct {
		name, link        string
		allowed, redirect bool
	}{
		{name: "relative", link: "/logs?page[number]=2", allowed: true},
		{name: "equivalent_origin", link: "https://API.EXAMPLE.TEST:443/logs?page=2", allowed: true},
		{name: "different_port", link: "https://api.example.test:8443/logs"},
		{name: "hostile_host", link: "https://hostile.example.test/logs"},
		{name: "changed_scheme", link: "http://api.example.test/logs"},
		{name: "hostile_redirect", link: "/redirect", redirect: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			reported := 0
			a := &ITGlueAdapter{baseURL: "https://api.example.test", doStop: utils.NewEvent(), conf: ITGlueConfig{Token: "test-only", ClientOptions: uspclient.ClientOptions{OnError: func(error) { reported++ }, OnWarning: func(string) {}}}}
			a.httpClient = &http.Client{Transport: pagingTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				require.Equal(t, "https", r.URL.Scheme, "refused request reached transport")
				require.True(t, strings.EqualFold("api.example.test", r.URL.Hostname()), "credentials reached a foreign host")
				require.True(t, r.URL.Port() == "" || r.URL.Port() == "443", "credentials reached another port")
				require.Equal(t, "test-only", r.Header.Get("x-api-key"))
				body := `{"data":[],"links":{}}`
				status := 200
				headers := make(http.Header)
				if calls == 1 {
					encoded, _ := json.Marshal(map[string]interface{}{"data": []interface{}{}, "links": map[string]string{"next": tc.link}})
					body = string(encoded)
				}
				if tc.redirect && calls == 2 {
					status = 302
					headers.Set("Location", "https://hostile.example.test/logs")
				}
				return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: headers, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})}
			_, next := a.makeOneRequest("/logs", "")
			require.Equal(t, tc.link, next)
			a.makeOneRequest("/logs", next)
			want := 1
			if tc.allowed || tc.redirect {
				want = 2
			}
			require.Equal(t, want, calls)
			if tc.allowed {
				require.Zero(t, reported)
			} else {
				require.Equal(t, 1, reported, "refusal must be visible")
			}
		})
	}
}
