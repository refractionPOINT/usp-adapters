package usp_wiz

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

func TestReviewPartialGraphQL(t *testing.T) {
	for _, tc := range []struct {
		name, path, data string
		fail             bool
	}{
		{"optional-item", `["issues","nodes",0,"description"]`, `{"issues":{"nodes":[{"id":"a","createdAt":"2026-01-02T00:00:00Z","description":null}],"pageInfo":{"hasNextPage":false}}}`, false},
		{"optional-sibling", `["issues","totalCount"]`, `{"issues":{"nodes":[{"id":"a","createdAt":"2026-01-02T00:00:00Z"}],"pageInfo":{"hasNextPage":false}}}`, false},
		{"other-root", `["other"]`, `{"issues":{"nodes":[{"id":"a","createdAt":"2026-01-02T00:00:00Z"}],"pageInfo":{"hasNextPage":false}}}`, false},
		{"connection", `["issues"]`, `{"issues":null}`, true},
		{"id", `["issues","nodes",0,"id"]`, `{"issues":{"nodes":[{"id":null,"createdAt":"2026-01-02T00:00:00Z"}]}}`, true},
		{"time", `["issues","nodes",0,"createdAt"]`, `{"issues":{"nodes":[{"id":"a","createdAt":null}]}}`, true},
		{"pageInfo", `["issues","pageInfo"]`, `{"issues":{"nodes":[],"pageInfo":null}}`, true},
		{"error-only", `["issues"]`, `null`, true},
		{"unscoped", `null`, `{"issues":{"nodes":[]}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			warnings := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"data":%s,"errors":[{"message":"unavailable","path":%s}]}`, tc.data, tc.path)
			}))
			defer srv.Close()
			a := &WizAdapter{conf: WizConfig{URL: srv.URL, Query: `{ issues { nodes { id createdAt } } }`, DataPath: []string{"data", "issues", "nodes"}, IDField: "id", TimeField: "createdAt", ClientOptions: uspclient.ClientOptions{OnWarning: func(string) { warnings++ }}}, httpClient: srv.Client(), accessToken: "test", expiresAt: time.Now().Add(time.Hour)}
			items, since, id, err := a.makeOneGraphQLRequest("before", "previous")
			if tc.fail {
				require.Error(t, err)
				require.Empty(t, items)
				require.Equal(t, "before", since)
				require.Equal(t, "previous", id)
			} else {
				require.NoError(t, err)
				require.Len(t, items, 1)
				require.Equal(t, 1, warnings)
				require.Equal(t, "a", id)
			}
		})
	}
}

func TestReviewQueryMustConsumeAfter(t *testing.T) {
	for _, query := range []string{
		`{ issues { nodes { id createdAt } pageInfo { hasNextPage endCursor } } }`,
		"query($after: String) { issues { nodes { id createdAt } pageInfo { hasNextPage endCursor } } } # after: $after",
	} {
		t.Run(query, func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				fmt.Fprint(w, `{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"same"}}}}`)
			}))
			defer srv.Close()
			a := &WizAdapter{conf: WizConfig{URL: srv.URL, Query: query, DataPath: []string{"data", "issues", "nodes"}, IDField: "id", TimeField: "createdAt"}, httpClient: srv.Client(), accessToken: "test", expiresAt: time.Now().Add(time.Hour)}
			_, since, id, err := a.makeOneGraphQLRequest("before", "previous")
			require.ErrorContains(t, err, "configuration")
			require.Equal(t, 1, calls)
			require.Equal(t, "before", since)
			require.Equal(t, "previous", id)
		})
	}
}

func TestReviewShutdownIsQuiet(t *testing.T) {
	stop := utils.NewEvent()
	errors := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stop.Set()
		fmt.Fprint(w, `{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"next"}}}}`)
	}))
	defer srv.Close()
	a := &WizAdapter{conf: WizConfig{URL: srv.URL, Query: `query($after: String) { issues(after:$after) { nodes { id createdAt } pageInfo { hasNextPage endCursor } } }`, DataPath: []string{"data", "issues", "nodes"}, IDField: "id", TimeField: "createdAt", PollInterval: time.Millisecond, ClientOptions: uspclient.ClientOptions{DebugLog: func(string) {}, OnError: func(error) { errors++ }}}, doStop: stop, httpClient: srv.Client(), accessToken: "test", expiresAt: time.Now().Add(time.Hour)}
	a.wgSenders.Add(1)
	a.fetchEvents()
	require.Zero(t, errors)
}

func TestReviewMalformedRecordDoesNotBlockLaterPolls(t *testing.T) {
	for _, paged := range []bool{false, true} {
		t.Run(fmt.Sprint(paged), func(t *testing.T) {
			stop := utils.NewEvent()
			roots, warnings := 0, 0
			nextFloor := ""
			const stamp = "2026-11-09T00:00:00Z"
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Variables map[string]interface{} `json:"variables"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				if body.Variables["after"] == "tail" {
					fmt.Fprint(w, `{"data":{"issues":{"nodes":[{"id":"older","createdAt":"2026-11-08T23:00:00Z"}],"pageInfo":{"hasNextPage":false}}}}`)
					return
				}
				roots++
				if roots > 1 {
					filters := body.Variables["filterBy"].(map[string]interface{})
					nextFloor = filters["createdAt"].(map[string]interface{})["after"].(string)
					stop.Set()
					fmt.Fprint(w, `{"data":{"issues":{"nodes":[]}}}`)
					return
				}
				page := ""
				if paged {
					page = `,"pageInfo":{"hasNextPage":true,"endCursor":"tail"}`
				}
				fmt.Fprintf(w, `{"data":{"issues":{"nodes":[null,{"id":null,"createdAt":%q},{"id":"missing-time"},{"id":"valid","createdAt":%q}]%s}}}`, stamp, stamp, page)
			}))
			defer srv.Close()
			a := &WizAdapter{conf: WizConfig{URL: srv.URL, Query: `query($after:String) { issues(after:$after) { nodes { id createdAt } pageInfo { hasNextPage endCursor } } }`, DataPath: []string{"data", "issues", "nodes"}, IDField: "id", TimeField: "createdAt", PollInterval: time.Millisecond, ClientOptions: uspclient.ClientOptions{DebugLog: func(string) {}, OnError: func(error) { stop.Set() }, OnWarning: func(string) { warnings++ }}}, uspClient: &captureSink{}, doStop: stop, httpClient: srv.Client(), accessToken: "test", expiresAt: time.Now().Add(time.Hour)}
			a.wgSenders.Add(1)
			a.fetchEvents()
			require.Equal(t, 2, roots, "malformed records must not block later polls")
			require.Equal(t, stamp, nextFloor, "first valid record must provide the watermark")
			require.Equal(t, 3, warnings)
			want := 1
			if paged {
				want = 2
			}
			require.Equal(t, want, a.uspClient.(*captureSink).count())
		})
	}
}
