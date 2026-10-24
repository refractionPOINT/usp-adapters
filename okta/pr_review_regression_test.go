package usp_okta

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
)

type prProducerSink struct {
	ackSink
	recordsMu sync.Mutex
	seen      map[string]int
}

func (s *prProducerSink) Ship(m *protocol.DataMessage, d time.Duration) error {
	if err := s.ackSink.Ship(m, d); err != nil {
		return err
	}
	s.recordsMu.Lock()
	defer s.recordsMu.Unlock()
	s.seen[m.JsonPayload["uuid"].(string)]++
	return nil
}
func TestOktaUnexpiredACKedProducerCrossesLiveBound(t *testing.T) {
	const total = 2*liveIDLimit + 4096
	sink := &prProducerSink{seen: map[string]int{}}
	doneVendor := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offset, _ := strconv.Atoi(r.URL.Query().Get("after"))
		end := min(offset+1000, total)
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=%d>; rel="next"`, r.Host, logsURL, end))
		records := []map[string]interface{}{}
		for i := offset; i < end; i++ {
			records = append(records, map[string]interface{}{"uuid": fmt.Sprint(i), "published": "2026-01-02T03:00:00Z"})
		}
		if offset == total {
			select {
			case <-doneVendor:
			default:
				close(doneVendor)
			}
		}
		_ = json.NewEncoder(w).Encode(records)
	}))
	defer server.Close()
	a, g := localHarness(t, server.URL, sink)
	var confirms sync.WaitGroup
	confirmCtx, cancelConfirm := context.WithCancel(context.Background())
	confirms.Add(1)
	go func() {
		defer confirms.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-confirmCtx.Done():
				return
			case <-ticker.C:
				sink.ack(uint64(sink.count()))
				_ = a.confirm(g)
			}
		}
	}()
	defer func() { cancelConfirm(); confirms.Wait() }()
	stop := runCollector(t, a, g)
	select {
	case <-doneVendor:
	case <-time.After(30 * time.Second):
		t.Errorf("fully ACKed producer stalled before %d unexpired admissions; got %d", total, sink.count())
	}
	stop()
	cancelConfirm()
	confirms.Wait()
	require.Equal(t, total, sink.count(), "ACKed completed pages must yield before the 30-minute expiry")
	require.Len(t, sink.seen, total, "no missing IDs")
	for id, n := range sink.seen {
		require.Equal(t, 1, n, "duplicate %s", id)
	}
	require.LessOrEqual(t, len(g.live), liveIDLimit)
	for _, entry := range g.live {
		require.Greater(t, entry.expiry, time.Now().Unix(), "recovery must not rely on expiry")
	}
}

func TestOktaPressureProtectsPriorIDReferencedByCurrentPage(t *testing.T) {
	sink := &prProducerSink{seen: map[string]int{}}
	var a *OktaAdapter
	var g *oktaGeneration
	doneVendor := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("after")
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=done>; rel="next"`, r.Host, logsURL))
		if page != "second" && page != "done" {
			offset, _ := strconv.Atoi(page)
			end := min(offset+1000, liveIDLimit-1)
			next := fmt.Sprint(end)
			if end == liveIDLimit-1 {
				next = "second"
			}
			w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?after=%s>; rel="next"`, r.Host, logsURL, next))
			records := make([]map[string]interface{}, end-offset)
			for i := range records {
				records[i] = map[string]interface{}{"uuid": fmt.Sprint("prior-", offset+i)}
			}
			_ = json.NewEncoder(w).Encode(records)
			return
		}
		if page == "second" {
			sink.ack(uint64(liveIDLimit - 1))
			_ = a.confirm(g)
			fmt.Fprint(w, `[{"uuid":"prior-0"},{"uuid":"new-one"},{"uuid":"new-two"},{"uuid":"prior-0"}]`)
			return
		}
		select {
		case <-doneVendor:
		default:
			close(doneVendor)
		}
		fmt.Fprint(w, `[]`)
	}))
	defer server.Close()
	a, g = localHarness(t, server.URL, sink)
	stop := runCollector(t, a, g)
	select {
	case <-doneVendor:
	case <-time.After(10 * time.Second):
		t.Errorf("confirmed prior-page IDs failed to yield to current page; got %d admissions", sink.count())
	}
	stop()
	require.Equal(t, liveIDLimit+1, sink.count(), "current-page repeated prior ID must not be retired and re-admitted")
	require.Equal(t, 1, sink.seen["prior-0"])
	require.Equal(t, uint64((liveIDLimit-1+999)/1000), g.confirmed.Load(), "partial/withheld ACK does not complete current page")
	require.Contains(t, g.live, eventDigest("prior-0"))
}
