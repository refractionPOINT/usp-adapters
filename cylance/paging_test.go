package usp_cylance

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPagingFailureRetainsUncommittedListings(t *testing.T) {
	for _, filter := range []string{"start", "start_time"} {
		t.Run(filter, func(t *testing.T) {
			fail := true
			since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				page := r.URL.Query().Get("page")
				assert.Equal(t, since.Format(time.RFC3339), r.URL.Query().Get(filter), "time filter must stay fixed on every page")
				if page == "2" && fail {
					w.WriteHeader(503)
					return
				}
				n := 1
				if page == "2" {
					n = 2
				}
				json.NewEncoder(w).Encode(map[string]interface{}{
					"page_number": n, "total_pages": 2, "page_items": []utils.Dict{{"id": page, "created": since.Add(time.Duration(n) * time.Second).Format(time.RFC3339)}},
				})
			}))
			defer server.Close()
			a := &CylanceAdapter{ctx: context.Background(), httpClient: server.Client(), accessToken: "test-only", tokenExpiresAt: time.Now().Add(time.Hour),
				conf: CylanceConfig{ClientOptions: uspclient.ClientOptions{OnError: func(error) {}, OnWarning: func(string) {}}}}
			api := Api{key: "test", idField: "id", timeField: "created", startFilterField: filter, returnedTimeFormat: time.RFC3339, dedupe: map[string]int64{}}
			items, cursor, err := a.getEventsRequest(a.ctx, server.URL, api, since)
			require.Error(t, err)
			require.Empty(t, items)
			require.Equal(t, since, cursor, "failed walk must not advance")
			require.Empty(t, api.dedupe, "discarded pages must remain replayable")
			fail = false
			items, cursor, err = a.getEventsRequest(a.ctx, server.URL, api, since)
			require.NoError(t, err)
			require.Len(t, items, 2, "retry must include records from the successful first page")
			require.Equal(t, since.Add(2*time.Second), cursor)
		})
	}
}
