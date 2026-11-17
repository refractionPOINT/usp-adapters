package usp_okta

import (
	"context"
	"errors"
	"fmt"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"net/http"
	"net/http/httptest"
	"strings"
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

type lookbackErrorSink struct {
	ackSink
	err error
}

func (s *lookbackErrorSink) ShipWithReceipt(*protocol.DataMessage, time.Duration) (uspclient.AdmissionReceipt, error) {
	return uspclient.AdmissionReceipt{}, s.err
}
func (*lookbackErrorSink) ObserveReceipt(uspclient.AdmissionReceipt) uspclient.ReceiptObservation {
	return uspclient.ReceiptObservation{}
}
func TestLookbackAdmissionErrorClassification(t *testing.T) {
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s>; rel="next"`, r.Host, r.URL.RequestURI()))
		fmt.Fprint(w, `[{"uuid":"one"}]`)
	}))
	defer vendor.Close()
	for _, err := range []error{uspclient.ErrorClosed, acktrack.ErrSequence, acktrack.ErrDependency, acktrack.ErrLimit, uspclient.ErrorReceiptUnavailable, errors.New("admission failure")} {
		var sink uspSink = &lookbackErrorSink{err: err}
		if errors.Is(err, uspclient.ErrorReceiptUnavailable) {
			sink = &ackSink{}
		}
		a, g := localHarness(t, vendor.URL, sink)
		d, e := utils.NewLocalDeduper(time.Hour, 2*time.Hour)
		require.NoError(t, e)
		a.conf.Lookback = &utils.LookbackConfig{Window: time.Hour, Namespace: "record", Deduper: d.(utils.CheckingDeduper)}
		reports, issues := 0, 0
		var failures uint64
		a.conf.Lookback.Observer = func(o utils.LookbackObservation) {
			failures += o.Failures
			if o.Failures > 0 {
				require.Equal(t, utils.LookbackFailureDelivery, o.Failure)
			}
		}
		a.conf.ClientOptions.OnError = func(error) { reports++ }
		a.conf.ClientOptions.DebugLog = func(code string) {
			if strings.Contains(code, "admission_tracking_failed") {
				issues++
			}
		}
		a.collect(context.Background(), g)
		wantFailures := 1
		if errors.Is(err, uspclient.ErrorClosed) {
			wantFailures = 0 // shutdown, not a delivery failure
		}
		require.EqualValues(t, wantFailures, failures)
		a.flushNotifications()
		a.notifier.Close()
		d.Close()
		tracking := errors.Is(err, acktrack.ErrSequence) || errors.Is(err, acktrack.ErrDependency) || errors.Is(err, acktrack.ErrLimit)
		require.Equal(t, tracking, issues == 1)
		require.Equal(t, !tracking && !errors.Is(err, uspclient.ErrorClosed), reports == 1)
	}
}

func TestLookbackObserverCollection(t *testing.T) {
	for _, continuous := range []bool{false, true} {
		t.Run(fmt.Sprint(continuous), func(t *testing.T) {
			var calls atomic.Int32
			vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Link", fmt.Sprintf(`<http://%s%s>; rel="next"`, r.Host, r.URL.RequestURI()))
				if calls.Load() == 1 {
					fmt.Fprint(w, `[{"uuid":"one"},{"uuid":"one"},{"uuid":null}]`)
				} else {
					if continuous {
						fmt.Fprintf(w, `[{"uuid":"one","published":%q}]`, time.Now().UTC().Format(time.RFC3339Nano))
					} else {
						fmt.Fprint(w, `[]`)
					}
				}
			}))
			defer vendor.Close()
			client, _ := lookbacktest.Client(t, nil)
			a, _ := localHarness(t, vendor.URL, client)
			d, err := utils.NewLocalDeduper(time.Hour, 2*time.Hour)
			require.NoError(t, err)
			defer d.Close()
			var starts, completed, fetched, confirmed, invalid, duplicates uint64
			a.conf.Lookback = &utils.LookbackConfig{Window: time.Hour, Namespace: "record", Deduper: d.(utils.CheckingDeduper), Observer: func(o utils.LookbackObservation) {
				starts += o.Starts
				completed += o.Completed
				fetched += o.Fetched
				confirmed += o.Confirmed
				invalid += o.InvalidIDs
				duplicates += o.Duplicates
			}}
			g := newOktaGeneration(1, a.initialPosition())
			ctx, cancel := context.WithCancel(context.Background())
			original := a.conf.Lookback.Observer
			a.conf.Lookback.Observer = func(o utils.LookbackObservation) {
				original(o)
				if completed == 1 && calls.Load() >= 3 {
					cancel()
				}
			}
			timeout, stop := context.WithTimeout(ctx, 5*time.Second)
			defer stop()
			defer cancel()
			a.collect(timeout, g)
			require.EqualValues(t, 1, starts)
			require.EqualValues(t, 1, completed)
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			if continuous {
				require.EqualValues(t, 5, fetched)
			} else {
				require.EqualValues(t, 3, fetched)
			}
			require.EqualValues(t, 1, confirmed)
			require.EqualValues(t, 1, invalid)
			if continuous {
				require.EqualValues(t, 3, duplicates)
			} else {
				require.EqualValues(t, 1, duplicates)
			}
		})
	}
}
