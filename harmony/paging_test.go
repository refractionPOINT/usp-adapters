package usp_harmony

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
)

func TestPagingCanceledRetainsWindow(t *testing.T) {
	fake := &fakeGateway{cancelEventsTimes: 1, eventPages: [][]utils.Dict{{{"id": "recovered"}}}}
	base := fake.handler()
	var mu sync.Mutex
	var starts []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == eventsQueryPath {
			var body utils.Dict
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			frame, _ := body.GetDict("timeframe")
			start, _ := frame.GetString("startTime")
			mu.Lock()
			starts = append(starts, start)
			mu.Unlock()
			writeJSON(w, 200, utils.Dict{"success": true, "data": utils.Dict{"taskId": "task-1"}})
			return
		}
		base.ServeHTTP(w, r)
	}))
	defer srv.Close()
	a, _, err := NewHarmonyAdapter(context.Background(), HarmonyConfig{ClientOptions: validClientOptions(), ClientID: "c", AccessKey: "s", URL: srv.URL, Events: EventsConfig{Enabled: true, CloudServices: []string{"Harmony Endpoint"}, PollInterval: "10ms"}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	waitUntil(t, 3*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(starts) >= 2 }, "expected retry")
	mu.Lock()
	defer mu.Unlock()
	if starts[0] != starts[1] {
		t.Fatalf("canceled task skipped window: first start %s, retry start %s", starts[0], starts[1])
	}
}
