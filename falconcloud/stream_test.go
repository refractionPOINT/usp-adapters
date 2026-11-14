package usp_falconcloud

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/crowdstrike/gofalcon/falcon"
	"github.com/crowdstrike/gofalcon/falcon/client"
	"github.com/crowdstrike/gofalcon/falcon/client/event_streams"
	"github.com/crowdstrike/gofalcon/falcon/models"
	"github.com/crowdstrike/gofalcon/falcon/models/streaming_models"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

type refreshRecorder struct {
	event_streams.ClientService
	calls chan int64
}

func (r *refreshRecorder) RefreshActiveStreamSession(p *event_streams.RefreshActiveStreamSessionParams, _ ...event_streams.ClientOption) (*event_streams.RefreshActiveStreamSessionOK, error) {
	r.calls <- p.Partition
	return &event_streams.RefreshActiveStreamSessionOK{}, nil
}
func descriptor(feed string, partition int64) *models.MainAvailableStreamV2 {
	interval := int64(2)
	refresh := fmt.Sprintf("https://api.crowdstrike.com/sensors/entities/datafeed-actions/v1/%d", partition)
	return &models.MainAvailableStreamV2{
		DataFeedURL: &feed, RefreshActiveSessionInterval: &interval,
		RefreshActiveSessionURL: &refresh, SessionToken: &models.MainSessionToken{Token: new(string)},
	}
}
func testAdapter(t *testing.T) (*FalconCloudAdapter, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a := &FalconCloudAdapter{ctx: ctx, cancel: cancel}
	errs := make(chan error, 4)
	a.conf.ClientOptions.OnError = func(err error) { errs <- err }
	return a, errs
}
func TestFeedFailureIsVisibleAndStops(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusUnauthorized, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			a, errs := testAdapter(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				// Empty successful response is EOF. Error bodies must never be events.
				if status != http.StatusOK {
					fmt.Fprint(w, `feed refused`)
				}
			}))
			defer server.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				a.handleStream(&client.CrowdStrikeAPISpecification{}, "app", descriptor(server.URL+"?appId=app", 0))
			}()
			select {
			case err := <-errs:
				if status != http.StatusOK && !strings.Contains(err.Error(), fmt.Sprint(status)) {
					t.Errorf("error lacks feed status: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Error("feed failure did not reach OnError")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("stream did not stop for restart")
			}
		})
	}
}
func TestClosedEventsPreservesFinalError(t *testing.T) {
	a, errs := testAdapter(t)
	stream := &falcon.StreamingHandle{Events: make(chan *streaming_models.EventItem), Errors: make(chan falcon.StreamingError, 1)}
	close(stream.Events)
	// Wait until the consumer has seen the closed Events channel before the
	// error arrives. The adapter must continue waiting for that final error.
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.consumeStream(stream, time.Time{}, func(*protocol.DataMessage, time.Duration) error { t.Error("unexpected event"); return nil })
	}()
	select {
	case <-done:
		t.Fatal("closed Events hid the final error")
	case <-time.After(20 * time.Millisecond):
	}
	stream.Errors <- falcon.StreamingError{Fatal: true, Err: fmt.Errorf("feed ended")}
	close(stream.Errors)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop")
	}
	select {
	case <-errs:
	default:
		t.Fatal("final error did not reach OnError")
	}
}
func TestAppIDMatchesDocumentedContract(t *testing.T) {
	// https://developer.crowdstrike.com/api-reference/collections/event-streams/
	valid := regexp.MustCompile(`^[a-zA-Z0-9]{1,32}$`)
	if appID := newAppID(); !valid.MatchString(appID) {
		t.Fatalf("app ID violates vendor contract: %q", appID)
	}
}
func TestEachStreamRefreshesItsDiscoveredPartition(t *testing.T) {
	a, _ := testAdapter(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	recorder := &refreshRecorder{calls: make(chan int64, 16)}
	api := &client.CrowdStrikeAPISpecification{EventStreams: recorder}
	for _, partition := range []int64{0, 3} {
		sh, err := a.newStream(api, "app", descriptor(server.URL+"?appId=app", partition), 0)
		if err != nil {
			t.Fatal(err)
		}
		defer sh.Close()
	}
	defer a.cancel()
	seen := map[int64]bool{}
	deadline := time.After(3 * time.Second)
	for !seen[0] || !seen[3] {
		select {
		case p := <-recorder.calls:
			seen[p] = true
		case <-deadline:
			t.Fatalf("refresh partitions = %v, want 0 and 3", seen)
		}
	}
	if api.EventStreams != recorder {
		t.Fatal("shared API client changed")
	}
}
