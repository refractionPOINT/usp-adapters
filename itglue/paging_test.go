package usp_itglue

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
