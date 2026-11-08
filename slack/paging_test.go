package usp_slack

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPagingRetainsCursorPastFiveFailures(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	floor := ""
	newest := time.Now().Unix() + 10
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		q := r.URL.Query()
		if attempts > 6 {
			json.NewEncoder(w).Encode(utils.Dict{})
			return
		}
		if floor == "" {
			floor = q.Get("oldest")
		}
		assert.Equal(t, floor, q.Get("oldest"), "time filter must remain fixed during continuation")
		if q.Get("cursor") == "" {
			json.NewEncoder(w).Encode(utils.Dict{"entries": []utils.Dict{{"id": "first", "date_create": newest}}, "response_metadata": utils.Dict{"next_cursor": "tail"}})
			return
		}
		assert.Equal(t, "tail", q.Get("cursor"))
		attempts++
		if attempts <= 6 {
			http.Error(w, "temporary failure", 500)
			return
		}
		json.NewEncoder(w).Encode(utils.Dict{"entries": []utils.Dict{{"id": "tail", "date_create": newest - 1}}})
	}))
	defer server.Close()
	sink := &captureSink{}
	conf := testConf(t, server.URL, "fake")
	conf.PollInterval = time.Millisecond
	a, _, err := newSlackAdapter(t.Context(), conf, sink)
	require.NoError(t, err)
	defer a.Close()
	require.Eventually(t, func() bool { return shipCounts(sink)["tail"] == 1 }, 2*time.Second, time.Millisecond, "failed continuation must survive the retry budget")
	a.doStop.Set()
	assert.Equal(t, 1, shipCounts(sink)["first"])
}
