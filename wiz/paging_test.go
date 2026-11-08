package usp_wiz

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPagingCompleteBeforeCheckpoint(t *testing.T) {
	for _, mode := range []string{"success", "http", "graphql", "repeated", "empty-page", "three-pages", "missing-cursor"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var body struct {
					Variables map[string]interface{} `json:"variables"`
				}
				if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&body)) {
					w.WriteHeader(400)
					return
				}
				if calls == 1 {
					assert.Nil(t, body.Variables["after"])
					nodes := `[{"id":"new","createdAt":"2026-01-02T00:00:00Z"}]`
					if mode == "empty-page" {
						nodes = `[]`
					}
					w.Write([]byte(`{"data":{"issues":{"nodes":` + nodes + `,"pageInfo":{"hasNextPage":true,"endCursor":"page-2"}}}}`))
					return
				}
				if calls == 3 {
					assert.Equal(t, "page-3", body.Variables["after"])
					w.Write([]byte(`{"data":{"issues":{"nodes":[{"id":"oldest","createdAt":"2026-01-01T00:30:00Z"}],"pageInfo":{"hasNextPage":false}}}}`))
					return
				}
				assert.Equal(t, "page-2", body.Variables["after"])
				switch mode {
				case "three-pages":
					w.Write([]byte(`{"data":{"issues":{"nodes":[{"id":"middle","createdAt":"2026-01-01T01:00:00Z"}],"pageInfo":{"hasNextPage":true,"endCursor":"page-3"}}}}`))
				case "missing-cursor":
					w.Write([]byte(`{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":true}}}}`))
				case "http":
					w.WriteHeader(503)
				case "graphql":
					w.Write([]byte(`{"errors":[{"message":"retry"}],"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":false}}}}`))
				case "repeated":
					w.Write([]byte(`{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"page-2"}}}}`))
				default:
					w.Write([]byte(`{"data":{"issues":{"nodes":[{"id":"old","createdAt":"2026-01-01T01:00:00Z"}],"pageInfo":{"hasNextPage":false}}}}`))
				}
			}))
			defer srv.Close()
			a := &WizAdapter{conf: WizConfig{URL: srv.URL, Query: `query($after: String) { issues(after:$after) { nodes { id createdAt } pageInfo { hasNextPage endCursor } } }`, DataPath: []string{"data", "issues", "nodes"}, TimeField: "createdAt", IDField: "id"}, httpClient: srv.Client(), accessToken: "test", expiresAt: time.Now().Add(time.Hour)}
			items, since, id, err := a.makeOneGraphQLRequest("2026-01-01T00:00:00Z", "previous")
			if mode != "success" && mode != "empty-page" && mode != "three-pages" {
				require.Error(t, err)
				require.Empty(t, items)
				require.Equal(t, "2026-01-01T00:00:00Z", since)
				require.Equal(t, "previous", id)
			} else {
				require.NoError(t, err)
				want := 2
				if mode == "empty-page" {
					want = 1
				}
				if mode == "three-pages" {
					want = 3
				}
				require.Len(t, items, want)
				if mode != "empty-page" {
					require.Equal(t, "2026-01-02T00:00:00Z", since)
				}
			}
			wantCalls := 2
			if mode == "three-pages" {
				wantCalls = 3
			}
			require.Equal(t, wantCalls, calls)
		})
	}
}
