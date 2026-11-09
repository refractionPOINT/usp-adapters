package usp_okta

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/lookbacktest"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

func TestLookbackRestartGapSameTimestampAndConfirmedReplay(t *testing.T) {
	now := time.Now().UTC()
	stamp := now.Add(-30 * time.Minute)
	var calls atomic.Int32
	since := make(chan time.Time, 100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Query().Get("after") == "expired" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"errorCode":"E0000001"}`)
			return
		}
		parsed, _ := time.Parse(time.RFC3339, r.URL.Query().Get("since"))
		select {
		case since <- parsed:
		default:
		}
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s>; rel="next"`, r.Host, r.URL.RequestURI()))
		if !parsed.After(stamp) {
			fmt.Fprintf(w, `[{"uuid":"one","published":%q},{"uuid":"two","published":%q}]`, stamp.Format(time.RFC3339Nano), stamp.Format(time.RFC3339Nano))
		} else {
			fmt.Fprint(w, `[]`)
		}
	}))
	defer server.Close()
	deduper, err := utils.NewLocalDeduper(time.Hour, 2*time.Hour)
	require.NoError(t, err)
	defer deduper.Close()
	client, received := lookbacktest.Client(t, nil)
	a, _ := localHarness(t, server.URL, client)
	a.conf.Lookback = &utils.LookbackConfig{Window: time.Hour, Namespace: "record", Deduper: deduper.(utils.CheckingDeduper)}
	g := newOktaGeneration(1, a.initialPosition())
	g.position.Next = server.URL + logsURL + "?after=expired"
	stop := runCollector(t, a, g)
	for i := 0; i < 2; i++ {
		lookbacktest.Receive(t, received)
	}
	require.Eventually(t, func() bool { return calls.Load() > 2 }, 5*time.Second, time.Millisecond)
	stop()
	require.WithinDuration(t, now.Add(-time.Hour), <-since, 2*time.Second)
	// New generation/client keeps only shared confirmed markers, not local IDs.
	client2, received2 := lookbacktest.Client(t, nil)
	b, _ := localHarness(t, server.URL, client2)
	b.conf.Lookback = a.conf.Lookback
	before := calls.Load()
	stop2 := runCollector(t, b, newOktaGeneration(2, b.initialPosition()))
	require.Eventually(t, func() bool { return calls.Load() > before+2 }, 5*time.Second, time.Millisecond)
	stop2()
	select {
	case <-received2:
		t.Fatal("confirmed restart event replayed")
	default:
	}
}
