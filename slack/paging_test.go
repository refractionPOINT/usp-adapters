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
	require.Eventually(t, func() bool { return shipCounts(sink)["tail"] == 1 }, 10*time.Second, time.Millisecond, "failed continuation must survive the retry budget")
	a.doStop.Set()
	assert.Equal(t, 1, shipCounts(sink)["first"])
}

func TestPagingRejectedCursorRestartsOriginalFloor(t *testing.T) {
	var mu sync.Mutex
	roots, rejected := 0, 0
	floor := ""
	newest := time.Now().Unix() + 10
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		q := r.URL.Query()
		if floor == "" {
			floor = q.Get("oldest")
		}
		// After recovery ordinary polls are free to advance the floor.
		if q.Get("oldest") != floor {
			json.NewEncoder(w).Encode(utils.Dict{})
			return
		}
		switch q.Get("cursor") {
		case "":
			roots++
			token := "rejected"
			entries := []utils.Dict{{"id": "first", "date_create": newest}}
			if roots > 1 {
				token = "replacement"
				entries = append(entries, utils.Dict{"id": "new", "date_create": newest + 1})
			}
			json.NewEncoder(w).Encode(utils.Dict{"entries": entries, "response_metadata": utils.Dict{"next_cursor": token}})
		case "rejected":
			rejected++
			http.Error(w, "invalid cursor", 400)
		case "replacement":
			assert.Equal(t, floor, q.Get("oldest"))
			json.NewEncoder(w).Encode(utils.Dict{"entries": []utils.Dict{{"id": "tail", "date_create": newest - 1}}})
		default:
			t.Errorf("unexpected cursor %q", q.Get("cursor"))
			http.Error(w, "bad cursor", 400)
		}
	}))
	defer server.Close()
	sink := &captureSink{}
	conf := testConf(t, server.URL, "fake")
	conf.PollInterval = time.Millisecond
	a, _, err := newSlackAdapter(t.Context(), conf, sink)
	require.NoError(t, err)
	defer a.Close()
	require.Eventually(t, func() bool { counts := shipCounts(sink); return counts["tail"] == 1 && counts["new"] == 1 }, 10*time.Second, time.Millisecond, "a permanently rejected cursor must recover through the original root floor")
	a.doStop.Set()
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 2, roots)
	assert.LessOrEqual(t, rejected, 12, "recovery must be bounded")
	assert.Equal(t, 2, shipCounts(sink)["first"], "restarting the root may replay admitted pages")
}
