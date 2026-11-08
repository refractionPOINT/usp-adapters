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
		{"optional-item", `["issues","nodes",0,"description"]`, `{"issues":{"nodes":[{"id":"bad","createdAt":"2026-01-02T00:00:00Z","description":null},{"id":"a","createdAt":"2026-01-02T00:00:00Z"}],"pageInfo":{"hasNextPage":false}}}`, false},
		{"optional-sibling", `["issues","totalCount"]`, `{"issues":{"nodes":[{"id":"a","createdAt":"2026-01-02T00:00:00Z"}],"pageInfo":{"hasNextPage":false}}}`, false},
		{"other-root", `["other"]`, `{"issues":{"nodes":[{"id":"a","createdAt":"2026-01-02T00:00:00Z"}],"pageInfo":{"hasNextPage":false}}}`, false},
		{"connection", `["issues"]`, `{"issues":null}`, true},
		{"id", `["issues","nodes",0,"id"]`, `{"issues":{"nodes":[{"id":null,"createdAt":"2026-01-02T00:00:00Z"},{"id":"a","createdAt":"2026-01-02T00:00:00Z"}]}}`, false},
		{"time", `["issues","nodes",0,"createdAt"]`, `{"issues":{"nodes":[{"id":"bad","createdAt":null},{"id":"a","createdAt":"2026-01-02T00:00:00Z"}]}}`, false},
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
			a := &WizAdapter{conf: WizConfig{URL: srv.URL, Query: `query($after:String) { issues(after:$after) { nodes { id createdAt } pageInfo { hasNextPage endCursor } } }`, DataPath: []string{"data", "issues", "nodes"}, IDField: "id", TimeField: "createdAt", ClientOptions: uspclient.ClientOptions{OnWarning: func(string) { warnings++ }}}, httpClient: srv.Client(), accessToken: "test", expiresAt: time.Now().Add(time.Hour)}
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

func TestReviewUnsupportedCursorKeepsCollecting(t *testing.T) {
	for _, tc := range []struct{ query, pageInfo string }{
		{`{ issues { nodes { id createdAt } pageInfo { hasNextPage endCursor } } }`, `{"hasNextPage":true,"endCursor":"ignored"}`},
		{"query($after: String) { issues { nodes { id createdAt } pageInfo { hasNextPage endCursor } } } # after: $after", `{"hasNextPage":true,"endCursor":"ignored"}`},
		{`{ issues { nodes { id createdAt } pageInfo { hasNextPage } } }`, `{"hasNextPage":true}`},
		{`{ issues { nodes { id createdAt } pageInfo { endCursor } } }`, `{"endCursor":"ignored"}`},
		{`{ issues { nodes { id createdAt } pageInfo { endCursor } } }`, `null`},
	} {
		t.Run(tc.query+tc.pageInfo, func(t *testing.T) {
			calls, errorsSeen := 0, 0
			warnings := []string{}
			stop := utils.NewEvent()
			nextFloor := ""
			const stamp = "2026-11-09T00:00:00Z"
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var body struct {
					Variables map[string]interface{} `json:"variables"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				if calls == 2 {
					nextFloor = body.Variables["filterBy"].(map[string]interface{})["createdAt"].(map[string]interface{})["after"].(string)
					stop.Set()
				}
				fmt.Fprintf(w, `{"data":{"issues":{"nodes":[{"id":"event-%d","createdAt":%q}],"pageInfo":%s}}}`, calls, stamp, tc.pageInfo)
			}))
			defer srv.Close()
			sink := &captureSink{}
			a := &WizAdapter{conf: WizConfig{URL: srv.URL, Query: tc.query, DataPath: []string{"data", "issues", "nodes"}, IDField: "id", TimeField: "createdAt", PollInterval: time.Millisecond, ClientOptions: uspclient.ClientOptions{DebugLog: func(string) {}, OnWarning: func(s string) { warnings = append(warnings, s) }, OnError: func(error) { errorsSeen++ }}}, doStop: stop, uspClient: sink, httpClient: srv.Client(), accessToken: "test", expiresAt: time.Now().Add(time.Hour)}
			a.wgSenders.Add(1)
			a.fetchEvents()
			require.Equal(t, 2, calls, "one first-page request per poll")
			require.Equal(t, 2, sink.count(), "keep collecting usable records on successive polls")
			require.Zero(t, errorsSeen, "unsupported cursor query must not trigger the fatal or Org Error callback")
			require.Len(t, warnings, 2, "exactly one pagination warning per poll")
			for _, warning := range warnings {
				require.Contains(t, warning, "wiz/README.md")
				require.Contains(t, warning, "$after: String")
				require.Contains(t, warning, "after: $after")
				require.Contains(t, warning, "pageInfo { hasNextPage endCursor }")
			}
			require.Equal(t, stamp, nextFloor, "retain first-page checkpoint behavior")
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

func TestFinalScopedGraphQLErrorsKeepUsableRecords(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy-paging=%v", legacy), func(t *testing.T) {
			stop := utils.NewEvent()
			polls, errorsSeen := 0, 0
			warnings := []string{}
			const stamp = "2026-11-09T00:00:00Z"
			query := `query($after:String) { issues(after:$after) { nodes { id createdAt } pageInfo { hasNextPage endCursor } } }`
			if legacy {
				query = `{ issues { nodes { id createdAt } pageInfo { endCursor } } }`
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				polls++
				if polls == 2 {
					stop.Set()
				}
				if legacy {
					fmt.Fprintf(w, `{"data":{"issues":{"nodes":[{"id":"good-%d","createdAt":%q}],"pageInfo":null}},"errors":[{"path":["issues","pageInfo","endCursor"],"message":"unavailable"}]}`, polls, stamp)
				} else {
					// A field error marks only its indexed record, even when values remain
					// present. Two field errors on one record must produce one skip warning.
					fmt.Fprintf(w, `{"data":{"issues":{"nodes":[{"id":"bad","createdAt":%q},{"id":"good-%d","createdAt":%q}],"pageInfo":{"hasNextPage":false}}},"errors":[{"path":["issues","nodes",0,"id"]},{"path":["issues","nodes",0,"createdAt"]}]}`, stamp, polls, stamp)
				}
			}))
			defer srv.Close()
			sink := &captureSink{}
			a := &WizAdapter{conf: WizConfig{URL: srv.URL, Query: query, DataPath: []string{"data", "issues", "nodes"}, IDField: "id", TimeField: "createdAt", PollInterval: time.Millisecond, ClientOptions: uspclient.ClientOptions{DebugLog: func(string) {}, OnWarning: func(s string) { warnings = append(warnings, s) }, OnError: func(error) { errorsSeen++; stop.Set() }}}, doStop: stop, uspClient: sink, httpClient: srv.Client(), accessToken: "test", expiresAt: time.Now().Add(time.Hour)}
			a.wgSenders.Add(1)
			a.fetchEvents()
			require.Equal(t, 2, polls)
			require.Zero(t, errorsSeen, "scoped errors must not enter the fatal/Org Error path")
			require.Len(t, warnings, 2, "one relevant warning per poll")
			messages := sink.snapshot()
			require.Len(t, messages, 2)
			for _, message := range messages {
				require.NotEqual(t, "bad", message.JsonPayload["id"])
			}
			for _, warning := range warnings {
				if legacy {
					require.Contains(t, warning, "wiz/README.md")
				} else {
					require.Equal(t, "skipping GraphQL record after field error", warning)
				}
			}
		})
	}
}
