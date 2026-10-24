package usp_azure_event_hub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	amqp "github.com/Azure/go-amqp"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

type discoveryHub struct {
	testHub
	partitions func(context.Context) ([]string, error)
	receive    func(context.Context, string, eventhub.Handler, receiverStart) (hubListener, error)
}

func (h *discoveryHub) Partitions(ctx context.Context) ([]string, error) {
	if h.partitions != nil {
		return h.partitions(ctx)
	}
	return h.testHub.Partitions(ctx)
}
func (h *discoveryHub) Receive(ctx context.Context, id string, handler eventhub.Handler, start receiverStart) (hubListener, error) {
	if h.receive != nil {
		return h.receive(ctx, id, handler, start)
	}
	return h.testHub.Receive(ctx, id, handler, start)
}
func seededStore(t *testing.T) *testStore {
	t.Helper()
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	raw, err := encodePosition(map[string]partitionPosition{"0": fixturePosition(100, 0)})
	require.NoError(t, err)
	return &testStore{found: true, value: utils.Checkpoint{SchemaVersion: checkpointSchema, SourceFingerprint: source, Position: raw, CheckpointFence: utils.CheckpointFence{Nonce: "seed", Revision: "seed"}}}
}
func newReviewAdapter(t *testing.T, h adapterHub, c adapterClient, s utils.CheckpointStore, issue func(string), onError func(error)) *EventHubAdapter {
	t.Helper()
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	a, err := newAdapter(context.Background(), EventHubConfig{CheckpointStore: s, CheckpointIssue: issue, ClientOptions: uspclient.ClientOptions{OnError: onError}}, h, c, source)
	require.NoError(t, err)
	a.discoverEvery = 5 * time.Millisecond
	a.saveEvery = 5 * time.Millisecond
	return a
}

func TestForwardGapProgressAcrossTwoRestarts(t *testing.T) {
	store := seededStore(t)
	for restart := 0; restart < 2; restart++ {
		c := newTestClient()
		h := &testHub{ids: []string{"0"}}
		var gaps, errorsCount atomic.Int32
		a := newReviewAdapter(t, h, c, store, func(code string) {
			if code == "sequence_gap" {
				gaps.Add(1)
			}
		}, func(error) { errorsCount.Add(1) })
		startWorker(t, a)
		call := eventuallyCalls(t, h, 1)[0]
		wantStart := "100"
		if restart == 1 {
			wantStart = "900000"
		}
		require.Equal(t, wantStart, call.start.Offset)
		offset, seq := int64(900000+restart), int64(5000+restart)
		require.NoError(t, call.handler(context.Background(), fixtureEvent(offset, seq)), "forward history gap must continue actual available data")
		require.Equal(t, 1, c.calls())
		require.Equal(t, wantStart, savedPositions(t, store)["0"].Offset, "receipt alone cannot adopt the gap event")
		c.ack(1)
		require.Eventually(t, func() bool {
			p, e := decodePosition(store.snapshot().Position)
			return e == nil && p["0"].Offset == fmt.Sprint(offset)
		}, time.Second, time.Millisecond)
		if restart == 0 {
			require.EqualValues(t, 1, gaps.Load())
		} else {
			require.Zero(t, gaps.Load())
		}
		require.Zero(t, errorsCount.Load())
		require.NoError(t, a.Close())
	}
}
func TestForwardGapPreservesEarlierPendingACKDependency(t *testing.T) {
	c := newTestClient()
	a, g, s := harness(t, c)
	var gaps atomic.Int32
	a.conf.CheckpointIssue = func(code string) {
		if code == "sequence_gap" {
			gaps.Add(1)
		}
	}
	require.NoError(t, process(t, a, g, "0", 101, 1))
	require.NoError(t, process(t, a, g, "0", 900000, 5000))
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Empty(t, savedPositions(t, s))
	require.EqualValues(t, 1, gaps.Load())
	c.ack(1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, "101", savedPositions(t, s)["0"].Offset)
	c.ack(2)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, "900000", savedPositions(t, s)["0"].Offset)
}
func TestPeriodicManagementFailurePreservesHealthyReceivers(t *testing.T) {
	h := &discoveryHub{testHub: testHub{ids: []string{"0"}}}
	var calls atomic.Int32
	failed := make(chan struct{})
	h.partitions = func(ctx context.Context) ([]string, error) {
		if calls.Add(1) == 2 {
			close(failed)
			return nil, errors.New("management connection reset")
		}
		return h.testHub.Partitions(ctx)
	}
	var vendorErrors, retries atomic.Int32
	c := newTestClient()
	a := newReviewAdapter(t, h, c, nil, func(code string) {
		if code == "discovery_retry" {
			retries.Add(1)
		}
	}, func(error) { vendorErrors.Add(1) })
	startWorker(t, a)
	call := eventuallyCalls(t, &h.testHub, 1)[0]
	<-failed
	require.Eventually(t, func() bool { return calls.Load() >= 3 || a.ctx.Err() != nil }, time.Second, time.Millisecond)
	require.NoError(t, call.handler(context.Background(), fixtureEvent(100, 0)), "one periodic management error must not retire healthy receivers")
	require.Equal(t, 1, c.calls())
	require.Len(t, h.snapshot(), 1, "healthy receiver must not be reopened at latest")
	require.Positive(t, retries.Load())
	require.Zero(t, vendorErrors.Load())
	c.ack(1)
	require.NoError(t, a.Close())
}
func TestSequentialReceiverSetupGetsIndependentBudgets(t *testing.T) {
	h := &discoveryHub{testHub: testHub{ids: []string{"0", "1"}}}
	var budgetsMu sync.Mutex
	var budgets []time.Duration
	h.partitions = func(ctx context.Context) ([]string, error) {
		time.Sleep(40 * time.Millisecond)
		return h.testHub.Partitions(ctx)
	}
	h.receive = func(ctx context.Context, id string, handler eventhub.Handler, start receiverStart) (hubListener, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			return nil, errors.New("missing setup deadline")
		}
		budgetsMu.Lock()
		budgets = append(budgets, time.Until(deadline))
		budgetsMu.Unlock()
		time.Sleep(40 * time.Millisecond)
		return h.testHub.Receive(ctx, id, handler, start)
	}
	a := newReviewAdapter(t, h, newTestClient(), nil, nil, nil)
	a.discoverEvery = time.Hour
	startWorker(t, a)
	eventuallyCalls(t, &h.testHub, 2)
	budgetsMu.Lock()
	defer budgetsMu.Unlock()
	require.Len(t, budgets, 2)
	for _, budget := range budgets {
		require.Greater(t, budget, 30*time.Second-20*time.Millisecond, "management/another receiver consumed this receiver's budget")
		require.LessOrEqual(t, budget, 30*time.Second)
	}
	require.NoError(t, a.Close())
}
func TestPeriodicPartialReceiverSetupRetriesWithoutOrphans(t *testing.T) {
	h := &discoveryHub{testHub: testHub{ids: []string{"0"}}}
	var attempts atomic.Int32
	partial := &testListener{done: make(chan struct{})}
	failed := make(chan struct{})
	h.receive = func(ctx context.Context, id string, handler eventhub.Handler, start receiverStart) (hubListener, error) {
		if id == "1" && attempts.Add(1) == 1 {
			close(failed)
			return partial, errors.New("temporary link timeout")
		}
		return h.testHub.Receive(ctx, id, handler, start)
	}
	c := newTestClient()
	var vendorErrors atomic.Int32
	a := newReviewAdapter(t, h, c, nil, nil, func(error) { vendorErrors.Add(1) })
	startWorker(t, a)
	first := eventuallyCalls(t, &h.testHub, 1)[0]
	h.setIDs("0", "1")
	<-failed
	calls := eventuallyCalls(t, &h.testHub, 2)
	require.Equal(t, "1", calls[1].id)
	require.EqualValues(t, 2, attempts.Load())
	select {
	case <-partial.Done():
	default:
		t.Fatal("partial failed receiver was orphaned")
	}
	require.NoError(t, first.handler(context.Background(), fixtureEvent(100, 0)))
	require.NoError(t, calls[1].handler(context.Background(), fixtureEvent(200, 0)))
	c.ack(2)
	require.Zero(t, vendorErrors.Load())
	require.NoError(t, a.Close())
}

type gatedErrorListener struct {
	testListener
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
}

func (l *gatedErrorListener) Err() error {
	l.enterOnce.Do(func() { close(l.entered) })
	<-l.release
	return l.err
}
func TestVerifiedDecodeFailureSurvivesHandlerCancellationRace(t *testing.T) {
	for _, handlerFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(handlerFirst), func(t *testing.T) {
			listener := &gatedErrorListener{testListener: testListener{done: make(chan struct{}), err: errors.New("decode failure")}, entered: make(chan struct{}), release: make(chan struct{})}
			h := &discoveryHub{testHub: testHub{ids: []string{"0"}}}
			received := make(chan eventhub.Handler, 1)
			h.receive = func(context.Context, string, eventhub.Handler, receiverStart) (hubListener, error) {
				return nil, errors.New("unconfigured")
			}
			h.receive = func(_ context.Context, _ string, handler eventhub.Handler, _ receiverStart) (hubListener, error) {
				received <- handler
				return listener, nil
			}
			callbacks := make(chan error, 2)
			c := newTestClient()
			a := newReviewAdapter(t, h, c, nil, nil, func(err error) { callbacks <- err })
			startWorker(t, a)
			handler := <-received
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			// When handler wins, listener Done was already closed by the SDK, but its
			// monitor cannot finish Err until after cancellation. The other order lets
			// the monitor capture the cause first. Both must report exactly one cause.
			listener.once.Do(func() { close(listener.done) })
			if !handlerFirst {
				<-listener.entered
			}
			_ = handler(ctx, fixtureEvent(100, 0))
			close(listener.release)
			select {
			case err := <-callbacks:
				require.Contains(t, strings.ToLower(err.Error()), "decode")
			case <-time.After(time.Second):
				t.Fatal("verified vendor decode failure disappeared behind cancellation")
			}
			require.Zero(t, c.calls())
			require.NoError(t, a.Close())
			select {
			case <-callbacks:
				t.Fatal("duplicate vendor failure")
			default:
			}
		})
	}
}
func TestVendorCauseDiagnosticsExcludeSensitiveDescriptions(t *testing.T) {
	h := &testHub{ids: []string{"0"}}
	callbacks := make(chan error, 1)
	a := newReviewAdapter(t, h, newTestClient(), nil, nil, func(err error) { callbacks <- err })
	startWorker(t, a)
	call := eventuallyCalls(t, h, 1)[0]
	call.listener.err = &amqp.Error{Condition: amqp.ErrCondUnauthorizedAccess, Description: "SharedAccessKey=secret-fixture https://example.invalid/path?sig=sas-fixture payload-fixture"}
	require.NoError(t, call.listener.Close(context.Background()))
	select {
	case err := <-callbacks:
		require.Contains(t, err.Error(), "unauthorized-access")
		for _, secret := range []string{"secret-fixture", "sas-fixture", "payload-fixture", "example.invalid", "https://"} {
			require.NotContains(t, err.Error(), secret)
		}
	case <-time.After(time.Second):
		t.Fatal("missing vendor error")
	}
	require.NoError(t, a.Close())
}
func TestConstructorKeepsSafeSDKParserCause(t *testing.T) {
	conf := EventHubConfig{ConnectionString: "Endpoint=sb://example.servicebus.windows.net/;EntityPath=events"}
	_, _, err := NewEventHubAdapter(context.Background(), conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "SharedAccessKeyName")
	require.Contains(t, err.Error(), "must not be empty")
}
