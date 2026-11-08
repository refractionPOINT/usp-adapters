package usp_duo

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	duoapi "github.com/duosecurity/duo_api_golang"
	duoadmin "github.com/duosecurity/duo_api_golang/admin"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pagingSink struct {
	mu  sync.Mutex
	ids []string
}

func (s *pagingSink) Ship(m *protocol.DataMessage, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = append(s.ids, fmt.Sprint(m.JsonPayload["id"]))
	return nil
}
func (s *pagingSink) Drain(time.Duration) error               { return nil }
func (s *pagingSink) Close() ([]*protocol.DataMessage, error) { return nil, nil }
func (s *pagingSink) count() int                              { s.mu.Lock(); defer s.mu.Unlock(); return len(s.ids) }

func TestPagingFirstPageAndFailedContinuation(t *testing.T) {
	for _, kind := range []string{"auth", "admin"} {
		t.Run(kind, func(t *testing.T) {
			sink := &pagingSink{}
			var mu sync.Mutex
			calls := 0
			firstMin := ""
			firstMax := ""
			continuation := ""
			base := time.Now().Unix() + 10
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				calls++
				q := r.URL.Query()
				if calls == 1 {
					firstMin = q.Get("mintime")
					firstMax = q.Get("maxtime")
				}
				if calls > 1 && calls <= 3 {
					if kind == "auth" {
						assert.Equal(t, firstMin, q.Get("mintime"))
						assert.Equal(t, firstMax, q.Get("maxtime"))
						assert.Equal(t, "offset,token", q.Get("next_offset"))
					}
					if calls == 2 {
						continuation = r.URL.RawQuery
						http.Error(w, "temporary failure", 500)
						return
					}
					if calls == 3 {
						assert.Equal(t, continuation, r.URL.RawQuery)
					}
				}
				logs := []map[string]interface{}{}
				if calls == 1 {
					n := 1
					if kind == "admin" {
						n = 1000
					}
					for i := 0; i < n; i++ {
						logs = append(logs, map[string]interface{}{"id": fmt.Sprint(i), "timestamp": base + int64(i)})
					}
				}
				if calls == 3 {
					logs = append(logs, map[string]interface{}{"id": "tail", "timestamp": base + 1001})
				}
				if kind == "auth" {
					meta := map[string]interface{}{}
					if calls == 1 {
						meta["next_offset"] = []string{"offset", "token"}
					}
					json.NewEncoder(w).Encode(map[string]interface{}{"stat": "OK", "response": map[string]interface{}{"authlogs": logs, "metadata": meta}})
				} else {
					json.NewEncoder(w).Encode(map[string]interface{}{"stat": "OK", "response": logs})
				}
			}))
			defer server.Close()
			api := duoapi.NewDuoApi("fake", "fake", strings.TrimPrefix(server.URL, "https://"), "test")
			api.SetCustomHTTPClient(server.Client())
			a := &DuoAdapter{uspClient: sink, adminClient: duoadmin.New(*api), doStop: utils.NewEvent(), pollInterval: time.Millisecond, conf: DuoConfig{ClientOptions: uspclient.ClientOptions{OnError: func(error) {}, OnWarning: func(string) {}, DebugLog: func(string) {}}}}
			done := make(chan struct{})
			go func() {
				defer close(done)
				if kind == "auth" {
					a.fetchAuthLogs()
				} else {
					a.fetchAdminLogs()
				}
			}()
			defer func() { a.doStop.Set(); <-done }()
			want := 2
			if kind == "admin" {
				want = 1001
			}
			require.Eventually(t, func() bool { return sink.count() >= want }, time.Second, time.Millisecond, "first page and continuation must both ship")
			a.doStop.Set()
			<-done
			sink.mu.Lock()
			defer sink.mu.Unlock()
			require.Contains(t, sink.ids, "0", "the initial response must be shipped")
			require.Contains(t, sink.ids, "tail")
		})
	}
}
