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
