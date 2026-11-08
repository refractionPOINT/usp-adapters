package usp_harmony

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
)

func TestReviewCanceledWindowBoundedThenCatchUp(t *testing.T) {
	stop := utils.NewEvent()
	var starts, ends []time.Time
	submits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			submits++
			var body utils.Dict
			json.NewDecoder(r.Body).Decode(&body)
			frame, _ := body.GetDict("timeframe")
			ss, _ := frame.GetString("startTime")
			ee, _ := frame.GetString("endTime")
			s, _ := time.Parse(time.RFC3339, ss)
			e, _ := time.Parse(time.RFC3339, ee)
			starts = append(starts, s)
			ends = append(ends, e)
			if submits == 4 {
				stop.Set()
			}
			writeJSON(w, 200, utils.Dict{"data": utils.Dict{"taskId": "task"}})
		} else {
			state := "Canceled"
			if submits >= 3 {
				state = "Done"
			}
			writeJSON(w, 200, utils.Dict{"data": utils.Dict{"state": state}})
		}
	}))
	defer srv.Close()
	// Requests serialize times to whole seconds. A wait longer than one second
	// makes this test fail on the old moving-end implementation deterministically.
	a := &HarmonyAdapter{conf: HarmonyConfig{URL: srv.URL, ClientOptions: validClientOptions(), Events: EventsConfig{pollInterval: 1100 * time.Millisecond}}, httpClient: srv.Client(), doStop: stop}
	// Seed auth so this test exercises only event queries.
	a.cachedToken = "test"
	a.tokenExpires = time.Now().Add(time.Hour)
	a.ctx = context.Background()
	a.wgSenders.Add(1)
	a.fetchEventsForService("Harmony Endpoint")
	if len(starts) != 4 {
		t.Fatalf("expected 2 cancellations, recovery, then next window: %d", len(starts))
	}
	for i := 1; i < 3; i++ {
		if !starts[i].Equal(starts[0]) || !ends[i].Equal(ends[0]) {
			t.Errorf("retry %d changed canceled window: %v..%v vs %v..%v", i, starts[i], ends[i], starts[0], ends[0])
		}
	}
	if !starts[3].Equal(ends[0]) {
		t.Errorf("recovery did not continue from completed end: %v != %v", starts[3], ends[0])
	}
	for i := range starts {
		if ends[i].Sub(starts[i]) > defaultEventsInitialLookback {
			t.Errorf("window exceeds bound: %v", ends[i].Sub(starts[i]))
		}
	}
}
