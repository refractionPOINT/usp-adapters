//go:build halopsa_live
// +build halopsa_live

package usp_halopsa

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
)

// These tests run only with -tags halopsa_live and require live creds via
// HALOPSA_INSTANCE_URL / HALOPSA_CLIENT_ID / HALOPSA_CLIENT_SECRET.

func liveAdapter(t *testing.T) *HaloPSAAdapter {
	t.Helper()
	inst := os.Getenv("HALOPSA_INSTANCE_URL")
	id := os.Getenv("HALOPSA_CLIENT_ID")
	sec := os.Getenv("HALOPSA_CLIENT_SECRET")
	if inst == "" || id == "" || sec == "" {
		t.Skip("HALOPSA_INSTANCE_URL/HALOPSA_CLIENT_ID/HALOPSA_CLIENT_SECRET not set")
	}

	noop := func(string) {}
	noopErr := func(err error) { t.Logf("adapter error: %v", err) }
	a := &HaloPSAAdapter{
		conf: HaloPSAConfig{
			ClientOptions: uspclient.ClientOptions{
				DebugLog:  noop,
				OnError:   noopErr,
				OnWarning: noop,
			},
			InstanceURL:  inst,
			ClientID:     id,
			ClientSecret: sec,
			Scope:        defaultScope,
			IDField:      defaultIDField,
			PageSize:     5,
			PollInterval: defaultPollInterval,
		},
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{Dial: (&net.Dialer{Timeout: 10 * time.Second}).Dial},
		},
	}
	return a
}

func TestLive_GetToken(t *testing.T) {
	a := liveAdapter(t)
	tok, err := a.getToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok == "" {
		t.Fatal("empty token")
	}
	if time.Until(a.tokenExpiry) <= time.Minute {
		t.Fatalf("token expires too soon: %v", a.tokenExpiry)
	}
	// Cached token returned without a refresh.
	tok2, err := a.getToken()
	if err != nil || tok2 != tok {
		t.Fatalf("expected cached token, got %v / %v", tok2, err)
	}
}

func TestLive_RequestPage_Tickets(t *testing.T) {
	a := liveAdapter(t)
	a.conf.Endpoint = "Tickets"
	tok, err := a.getToken()
	if err != nil {
		t.Fatal(err)
	}
	items, count, err := a.requestPage(tok, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatalf("expected non-zero record_count on Tickets")
	}
	if len(items) == 0 {
		t.Fatalf("expected at least one ticket")
	}
	if _, ok := items[0].GetInt("id"); !ok {
		t.Fatal("ticket missing id field")
	}
	t.Logf("Tickets page1: %d records (total=%d)", len(items), count)
}

func TestLive_RequestPage_Actions(t *testing.T) {
	a := liveAdapter(t)
	a.conf.Endpoint = "Actions"
	tok, _ := a.getToken()
	items, count, err := a.requestPage(tok, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 || len(items) == 0 {
		t.Fatalf("expected Actions records, got count=%d items=%d", count, len(items))
	}
	t.Logf("Actions page1: %d records (total=%d)", len(items), count)
}

func TestLive_RequestPage_Agent_ResultsEnvelope(t *testing.T) {
	a := liveAdapter(t)
	a.conf.Endpoint = "Agent"
	tok, _ := a.getToken()
	items, count, err := a.requestPage(tok, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Agent returns {"results":[...]} envelope — the auto-detect should still work.
	if len(items) == 0 {
		t.Fatalf("Agent envelope auto-detect failed; got 0 items (count=%d)", count)
	}
	t.Logf("Agent page1: %d records (auto-detected results envelope)", len(items))
}

func TestLive_CollectNewItems_Tickets(t *testing.T) {
	a := liveAdapter(t)
	a.conf.Endpoint = "Tickets"

	tok, _ := a.getToken()
	first, _, err := a.requestPage(tok, 1)
	if err != nil || len(first) == 0 {
		t.Fatalf("seed page: items=%d err=%v", len(first), err)
	}
	// Pick a cursor that should leave exactly one new record on top.
	cursor, _ := first[1].GetInt("id")
	expectedTopID, _ := first[0].GetInt("id")

	fetch := func(pageNo int) ([]utils.Dict, int, error) {
		return a.requestPage(tok, pageNo)
	}
	items, newMax, err := collectNewItems(fetch, "id", cursor, nil)
	if err != nil {
		t.Fatal(err)
	}
	if newMax != expectedTopID {
		t.Fatalf("newMax=%d, want %d", newMax, expectedTopID)
	}
	if len(items) == 0 {
		t.Fatalf("expected at least one new item beyond cursor=%d", cursor)
	}
	// All returned ids must be > cursor and oldest-first.
	var prev uint64
	for _, it := range items {
		id, _ := it.GetInt("id")
		if id <= cursor {
			t.Fatalf("returned id=%d <= cursor=%d", id, cursor)
		}
		if id < prev {
			t.Fatalf("not oldest-first: %d after %d", id, prev)
		}
		prev = id
	}
	t.Logf("collectNewItems with cursor=%d returned %d new ids up to %d", cursor, len(items), newMax)
}

func TestLive_PollLifecycle_Tickets(t *testing.T) {
	a := liveAdapter(t)
	a.conf.Endpoint = "Tickets"

	// First poll establishes baseline and returns nothing.
	items, err := a.poll()
	if err != nil {
		t.Fatal(err)
	}
	if items != nil {
		t.Fatalf("expected baseline to ship nothing, got %d items", len(items))
	}
	if !a.initialized || a.lastMaxID == 0 {
		t.Fatalf("expected baseline established, initialized=%v lastMaxID=%d", a.initialized, a.lastMaxID)
	}
	baseline := a.lastMaxID

	// Second poll should ship nothing because cursor is already at the top.
	items, err = a.poll()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no new items on second poll, got %d", len(items))
	}

	// Simulate downtime by rewinding the cursor by one ticket.
	pageToken, _ := a.getToken()
	page1, _, _ := a.requestPage(pageToken, 1)
	if len(page1) < 2 {
		t.Skip("not enough tickets to simulate rewind")
	}
	rewindTo, _ := page1[1].GetInt("id")
	a.lastMaxID = rewindTo

	items, err = a.poll()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) == 0 {
		t.Fatalf("expected catch-up items after rewind")
	}
	if a.lastMaxID != baseline {
		t.Fatalf("cursor after catch-up = %d, want %d", a.lastMaxID, baseline)
	}
}

// TestLive_FullJSONSampleOnce dumps a single record per endpoint so we can
// pattern-match the mock fixtures against real-world shapes.
func TestLive_FullJSONSampleOnce(t *testing.T) {
	a := liveAdapter(t)
	tok, _ := a.getToken()
	for _, ep := range []string{"Tickets", "Actions", "Agent"} {
		a.conf.Endpoint = ep
		a.conf.DataField = ""
		items, _, err := a.requestPage(tok, 1)
		if err != nil {
			t.Errorf("%s: %v", ep, err)
			continue
		}
		if len(items) == 0 {
			t.Logf("%s: no records", ep)
			continue
		}
		b, _ := json.MarshalIndent(items[0], "", "  ")
		t.Logf("%s sample (first %d bytes):\n%s", ep, min(len(b), 400), b[:min(len(b), 400)])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

