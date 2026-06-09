package usp_cynet

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
)

// validClientOptions returns a ClientOptions populated with the minimum fields
// required to satisfy uspclient validation while keeping the client in
// TestSinkMode (no network). The callback hooks are non-nil because the
// adapter calls DebugLog / OnError / OnWarning unconditionally.
func validClientOptions() uspclient.ClientOptions {
	return uspclient.ClientOptions{
		Identity: uspclient.Identity{
			Oid:             "00000000-0000-0000-0000-000000000001",
			InstallationKey: "00000000-0000-0000-0000-000000000002",
		},
		Platform:      "json",
		SensorSeedKey: "cynet-test",
		TestSinkMode:  true,
		DebugLog:      func(string) {},
		OnError:       func(error) {},
		OnWarning:     func(string) {},
	}
}

// newTestAdapter builds an adapter usable by the pure-helper tests
// (parseIncidentJSON / processAlerts) without standing up a USP client.
func newTestAdapter(opts uspclient.ClientOptions) *CynetAdapter {
	return &CynetAdapter{
		conf:         CynetConfig{ClientOptions: opts},
		alertsDedupe: make(map[string]int64),
		fnvHasher:    fnv.New64a(),
	}
}

// A synthetic IncidentJsonDescription value shaped like what Cynet delivers: a
// JSON-encoded string with nested objects. All identifiers are fabricated.
const sampleIncidentJSON = `{"Hostname":"TEST-HOST-01","Host Ip":"10.0.0.1","Alert Name":"File Dumped on the Disk","EPS Prevention":false,"Parent Process Details":{"Process SHA256":"1111111111111111111111111111111111111111111111111111111111111111","Process PID":1234,"Process Path":"c:\\windows\\example.exe"},"Extra Info":{"Detection Engine":"Cynet AV","Malware Type":"riskware"}}`

func TestParseIncidentJSON(t *testing.T) {
	t.Run("string is parsed into a nested object in place", func(t *testing.T) {
		a := newTestAdapter(validClientOptions())
		entity := utils.Dict{
			"IncidentName":            "File Dumped on the Disk",
			"IncidentJsonDescription": sampleIncidentJSON,
		}

		a.parseIncidentJSON(entity)

		nested, ok := entity["IncidentJsonDescription"].(utils.Dict)
		if !ok {
			t.Fatalf("expected IncidentJsonDescription to be parsed into utils.Dict, got %T", entity["IncidentJsonDescription"])
		}
		if nested["Hostname"] != "TEST-HOST-01" {
			t.Fatalf("expected parsed Hostname=TEST-HOST-01, got %v", nested["Hostname"])
		}
		parent, ok := nested["Parent Process Details"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected nested Parent Process Details object, got %T", nested["Parent Process Details"])
		}
		if parent["Process SHA256"] != "1111111111111111111111111111111111111111111111111111111111111111" {
			t.Fatalf("nested subfield not addressable after parse: %v", parent["Process SHA256"])
		}
		// Sibling fields must be untouched.
		if entity["IncidentName"] != "File Dumped on the Disk" {
			t.Fatalf("sibling field was modified: %v", entity["IncidentName"])
		}
	})

	t.Run("absent field is a no-op", func(t *testing.T) {
		a := newTestAdapter(validClientOptions())
		entity := utils.Dict{"IncidentName": "x"}
		a.parseIncidentJSON(entity)
		if _, exists := entity["IncidentJsonDescription"]; exists {
			t.Fatalf("parse should not invent the field when absent")
		}
	})

	t.Run("empty string is left as-is", func(t *testing.T) {
		a := newTestAdapter(validClientOptions())
		entity := utils.Dict{"IncidentJsonDescription": ""}
		a.parseIncidentJSON(entity)
		if entity["IncidentJsonDescription"] != "" {
			t.Fatalf("empty string should be left untouched, got %v", entity["IncidentJsonDescription"])
		}
	})

	t.Run("already-object field is left as-is", func(t *testing.T) {
		a := newTestAdapter(validClientOptions())
		obj := utils.Dict{"Hostname": "already-parsed"}
		entity := utils.Dict{"IncidentJsonDescription": obj}
		a.parseIncidentJSON(entity)
		got, ok := entity["IncidentJsonDescription"].(utils.Dict)
		if !ok || got["Hostname"] != "already-parsed" {
			t.Fatalf("pre-parsed object should be left untouched, got %T %v", entity["IncidentJsonDescription"], entity["IncidentJsonDescription"])
		}
	})

	t.Run("malformed JSON is left as the raw string and warns", func(t *testing.T) {
		var warned bool
		opts := validClientOptions()
		opts.OnWarning = func(string) { warned = true }
		a := newTestAdapter(opts)

		entity := utils.Dict{"IncidentJsonDescription": "{not valid json"}
		a.parseIncidentJSON(entity)

		if entity["IncidentJsonDescription"] != "{not valid json" {
			t.Fatalf("malformed JSON should be preserved as the raw string, got %v", entity["IncidentJsonDescription"])
		}
		if !warned {
			t.Fatalf("expected a warning to be emitted on malformed JSON")
		}
	})
}

func TestProcessAlerts(t *testing.T) {
	t.Run("parses IncidentJsonDescription on returned entities", func(t *testing.T) {
		a := newTestAdapter(validClientOptions())
		resp := &AlertsResponse{
			SyncTimeUtc: "2026-05-09T14:21:43.318Z",
			Entities: []utils.Dict{
				{"Sha256Hex": "abc", "IncidentJsonDescription": sampleIncidentJSON},
			},
		}

		alerts, _ := a.processAlerts(resp)
		if len(alerts) != 1 {
			t.Fatalf("expected 1 alert, got %d", len(alerts))
		}
		if _, ok := alerts[0]["IncidentJsonDescription"].(utils.Dict); !ok {
			t.Fatalf("processAlerts should parse IncidentJsonDescription, got %T", alerts[0]["IncidentJsonDescription"])
		}
	})

	t.Run("deduplicates identical entities across responses", func(t *testing.T) {
		a := newTestAdapter(validClientOptions())
		entity := utils.Dict{"Sha256Hex": "dup", "IncidentJsonDescription": sampleIncidentJSON}

		first, _ := a.processAlerts(&AlertsResponse{SyncTimeUtc: "2026-05-09T14:21:43.318Z", Entities: []utils.Dict{entity}})
		if len(first) != 1 {
			t.Fatalf("expected 1 alert on first pass, got %d", len(first))
		}

		// Re-submit the same raw entity; it must be deduped despite the prior
		// parse mutating the copy returned to the caller.
		second, _ := a.processAlerts(&AlertsResponse{SyncTimeUtc: "2026-05-09T14:21:43.318Z", Entities: []utils.Dict{
			{"Sha256Hex": "dup", "IncidentJsonDescription": sampleIncidentJSON},
		}})
		if len(second) != 0 {
			t.Fatalf("expected duplicate to be dropped, got %d alerts", len(second))
		}
	})
}

// fakeCynet is a minimal stand-in for the Cynet API: it serves the token and
// bulk-alerts endpoints and counts the calls it receives.
type fakeCynet struct {
	authCalls   int32
	alertsCalls int32
}

func (f *fakeCynet) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(authEndpoint, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.authCalls, 1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(AuthResponse{AccessToken: "test-token"})
	})
	mux.HandleFunc(alertsEndpoint, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.alertsCalls, 1)
		w.Header().Set("Content-Type", "application/json")
		// One short page (< pageSize) so the fetcher stops after one request.
		json.NewEncoder(w).Encode(AlertsResponse{
			SyncTimeUtc: "2026-05-09T14:21:43.318Z",
			Entities: []utils.Dict{
				{"Sha256Hex": "e2e", "IncidentJsonDescription": sampleIncidentJSON},
			},
		})
	})
	return mux
}

func TestFetchEndToEnd(t *testing.T) {
	fake := &fakeCynet{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	conf := CynetConfig{
		ClientOptions: validClientOptions(),
		AccessKey:     "ak",
		SecretKey:     "sk",
		SiteID:        "site-1",
		URL:           srv.URL,
	}

	adapter, _, err := NewCynetAdapter(context.Background(), conf)
	if err != nil {
		t.Fatalf("NewCynetAdapter: %v", err)
	}
	defer adapter.Close()

	waitUntil(t, 3*time.Second, func() bool {
		return atomic.LoadInt32(&fake.alertsCalls) >= 1
	}, "expected the alerts endpoint to be polled")

	if got := atomic.LoadInt32(&fake.authCalls); got == 0 {
		t.Fatalf("expected authentication to be called, got 0")
	}
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v: %s", timeout, msg)
}

// compile-time guard that the sample fixture is valid JSON, so a typo in the
// fixture fails fast rather than masquerading as a parse bug.
var _ = func() bool {
	var v utils.Dict
	if err := json.Unmarshal([]byte(sampleIncidentJSON), &v); err != nil {
		panic(fmt.Sprintf("sampleIncidentJSON fixture is not valid JSON: %v", err))
	}
	return true
}()
