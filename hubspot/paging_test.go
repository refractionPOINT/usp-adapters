package usp_hubspot

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPagingOpaqueAfter(t *testing.T)              { testPagingRetry(t, false) }
func TestPagingRetainsPagesAfterFailure(t *testing.T) { testPagingRetry(t, true) }
func testPagingRetry(t *testing.T, fail bool) {
	calls := 0
	start := ""
	until := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		q := r.URL.Query()
		if calls == 1 {
			start = q.Get("occurredAfter")
			until = q.Get("occurredBefore")
		}
		if (!fail && calls > 1) || (fail && calls == 3) {
			assert.Equal(t, "opaque+/=", q.Get("after"))
			assert.Equal(t, start, q.Get("occurredAfter"))
			assert.Equal(t, until, q.Get("occurredBefore"))
		}
		if fail && calls == 2 {
			http.Error(w, "temporary failure", 500)
			return
		}
		if calls == 1 || (fail && q.Get("after") == "" && q.Get("occurredAfter") != "opaque+/=") {
			json.NewEncoder(w).Encode(utils.Dict{"results": []utils.Dict{{"id": "first", "occurredAt": time.Now().UTC().Format(time.RFC3339)}}, "paging": utils.Dict{"next": utils.Dict{"after": "opaque+/=", "link": "unused"}}})
			return
		}
		json.NewEncoder(w).Encode(utils.Dict{"results": []utils.Dict{{"id": "tail", "occurredAt": time.Now().UTC().Format(time.RFC3339)}}})
	}))
	defer server.Close()
	a := &HubSpotAdapter{apiURL: server.URL, httpClient: server.Client(), doStop: utils.NewEvent(), dedupe: map[string]int64{}, conf: HubSpotConfig{ClientOptions: testClientOptions(t)}}
	boundary := time.Now().Add(-time.Minute)
	items := a.makeOneRequest(boundary)
	if fail {
		require.Nil(t, items)
		require.Empty(t, a.dedupe, "unreturned records must not be suppressed")
		items = a.makeOneRequest(boundary.Add(time.Hour))
	}
	require.Len(t, items, 2, "completed traversal must retain both pages")
	assert.Equal(t, "first", items[0]["id"])
	assert.Equal(t, "tail", items[1]["id"])
}

func TestPagingRejectedCursorRestartsOriginalWindow(t *testing.T) {
	roots, rejected := 0, 0
	start, until := "", ""
	stamp := time.Now().UTC().Format(time.RFC3339)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if start == "" {
			start = q.Get("occurredAfter")
			until = q.Get("occurredBefore")
		}
		assert.Equal(t, start, q.Get("occurredAfter"))
		assert.Equal(t, until, q.Get("occurredBefore"))
		switch q.Get("after") {
		case "":
			roots++
			token := "rejected"
			entries := []utils.Dict{{"id": "first", "occurredAt": stamp}}
			if roots > 1 {
				token = "replacement"
				entries = append(entries, utils.Dict{"id": "new", "occurredAt": stamp})
			}
			json.NewEncoder(w).Encode(utils.Dict{"results": entries, "paging": utils.Dict{"next": utils.Dict{"after": token}}})
		case "rejected":
			rejected++
			http.Error(w, "invalid cursor", 400)
		case "replacement":
			json.NewEncoder(w).Encode(utils.Dict{"results": []utils.Dict{{"id": "tail", "occurredAt": stamp}}})
		default:
			t.Errorf("unexpected cursor %q", q.Get("after"))
			http.Error(w, "bad cursor", 400)
		}
	}))
	defer server.Close()
	a := &HubSpotAdapter{apiURL: server.URL, httpClient: server.Client(), doStop: utils.NewEvent(), dedupe: map[string]int64{}, conf: HubSpotConfig{ClientOptions: testClientOptions(t)}}
	var items []utils.Dict
	for i := 0; i < 12; i++ {
		// A later caller floor must not replace the unfinished window.
		items = a.makeOneRequest(time.Now().Add(-time.Minute).Add(time.Duration(i) * time.Hour))
		if items != nil {
			break
		}
		require.Empty(t, a.dedupe, "a failed continuation must not commit the window")
	}
	require.Len(t, items, 3, "retain old pages and include new root and tail records")
	ids := []string{}
	for _, item := range items {
		ids = append(ids, item["id"].(string))
	}
	assert.Equal(t, []string{"first", "new", "tail"}, ids)
	assert.Equal(t, 2, roots)
	assert.LessOrEqual(t, rejected, 12)
}
