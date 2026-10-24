package usp_azure_event_hub

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	amqp "github.com/Azure/go-amqp"
	"github.com/mitchellh/mapstructure"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

func TestManagementFailureEscalatesAfterContinuousHourAndResets(t *testing.T) {
	h := &discoveryHub{testHub: testHub{ids: []string{"0"}}}
	var fail atomic.Bool
	var failures, successes, retries atomic.Int32
	var clock atomic.Int64
	clock.Store(time.Unix(1700000000, 0).UnixNano())
	h.partitions = func(ctx context.Context) ([]string, error) {
		if fail.Load() {
			failures.Add(1)
			return nil, &amqp.Error{Condition: amqp.ErrCondInternalError}
		}
		successes.Add(1)
		return h.testHub.Partitions(ctx)
	}
	callbacks := make(chan error, 2)
	c := newTestClient()
	a := newReviewAdapter(t, h, c, nil, func(code string) {
		if code == "discovery_retry" {
			retries.Add(1)
		}
	}, func(err error) { callbacks <- err })
	a.now = func() time.Time { return time.Unix(0, clock.Load()) }
	startWorker(t, a)
	call := eventuallyCalls(t, &h.testHub, 1)[0]
	waitRetry := func() {
		t.Helper()
		before := retries.Load()
		require.Eventually(t, func() bool { return retries.Load() > before || len(callbacks) > 0 }, time.Second, time.Millisecond)
	}
	quiet := func() {
		t.Helper()
		select {
		case err := <-callbacks:
			t.Fatalf("failure budget escalated too early: %v", err)
		default:
		}
	}
	fail.Store(true)
	waitRetry()
	clock.Add(int64(59 * time.Minute))
	waitRetry()
	quiet()
	require.NoError(t, call.handler(context.Background(), fixtureEvent(100, 0)))
	c.ack(1)
	fail.Store(false)
	before := successes.Load()
	require.Eventually(t, func() bool { return successes.Load() > before }, time.Second, time.Millisecond)
	// A successful read breaks the episode. Jumping beyond the old hour must
	// start a fresh budget, not inherit the resolved outage's original clock.
	clock.Add(int64(2 * time.Minute))
	fail.Store(true)
	waitRetry()
	quiet()
	clock.Add(int64(59 * time.Minute))
	waitRetry()
	quiet()
	require.NoError(t, call.handler(context.Background(), fixtureEvent(101, 1)))
	c.ack(2)
	clock.Add(int64(time.Minute))
	select {
	case err := <-callbacks:
		require.Contains(t, err.Error(), "GetRuntimeInformation()")
		require.Contains(t, err.Error(), "amqp:internal-error")
	case <-time.After(time.Second):
		t.Fatal("continuous one-hour outage never escalated")
	}
	require.Greater(t, failures.Load(), int32(1))
	require.NoError(t, a.Close())
	select {
	case <-callbacks:
		t.Fatal("duplicate vendor callback")
	default:
	}
}
func TestReceiverFailureBudgetSurvivesManagementSuccess(t *testing.T) {
	h := &discoveryHub{testHub: testHub{ids: []string{"0"}}}
	var clock atomic.Int64
	clock.Store(time.Unix(1700000000, 0).UnixNano())
	var retries atomic.Int32
	h.receive = func(ctx context.Context, id string, handler eventhub.Handler, start receiverStart) (hubListener, error) {
		if id == "1" {
			return nil, &amqp.Error{Condition: amqp.ErrCondUnauthorizedAccess}
		}
		return h.testHub.Receive(ctx, id, handler, start)
	}
	callbacks := make(chan error, 1)
	c := newTestClient()
	a := newReviewAdapter(t, h, c, nil, func(code string) {
		if code == "receiver_start_retry" {
			retries.Add(1)
		}
	}, func(err error) { callbacks <- err })
	a.now = func() time.Time { return time.Unix(0, clock.Load()) }
	startWorker(t, a)
	first := eventuallyCalls(t, &h.testHub, 1)[0]
	h.setIDs("0", "1")
	require.Eventually(t, func() bool { return retries.Load() > 0 }, time.Second, time.Millisecond)
	clock.Add(int64(59 * time.Minute))
	before := retries.Load()
	require.Eventually(t, func() bool { return retries.Load() > before }, time.Second, time.Millisecond)
	select {
	case err := <-callbacks:
		t.Fatalf("receiver outage escalated early: %v", err)
	default:
	}
	require.NoError(t, first.handler(context.Background(), fixtureEvent(100, 0)))
	c.ack(1)
	clock.Add(int64(time.Minute))
	select {
	case err := <-callbacks:
		require.Contains(t, err.Error(), "Receive()")
		require.Contains(t, err.Error(), "unauthorized-access")
	case <-time.After(time.Second):
		t.Fatal("management success incorrectly reset the failed receiver's budget")
	}
	require.NoError(t, a.Close())
}
func TestPartialSetupRetainsAdmittedOffsetAndPendingHistory(t *testing.T) {
	h := &discoveryHub{testHub: testHub{ids: []string{"0"}}}
	var attempts atomic.Int32
	partial := &testListener{done: make(chan struct{})}
	var retiredHandler eventhub.Handler
	h.receive = func(ctx context.Context, id string, handler eventhub.Handler, start receiverStart) (hubListener, error) {
		if id == "1" && attempts.Add(1) == 1 {
			retiredHandler = handler
			if err := handler(context.Background(), fixtureEvent(200, 0)); err != nil {
				return nil, err
			}
			return partial, errors.New("temporary link timeout")
		}
		return h.testHub.Receive(ctx, id, handler, start)
	}
	c := newTestClient()
	store := &testStore{}
	a := newReviewAdapter(t, h, c, store, nil, nil)
	startWorker(t, a)
	eventuallyCalls(t, &h.testHub, 1)
	h.setIDs("0", "1")
	calls := eventuallyCalls(t, &h.testHub, 2)
	require.Equal(t, "200", calls[1].start.Offset, "a setup retry must retain an already admitted in-process boundary")
	pending, err := c.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.EqualValues(t, 1, pending[0].SeqNum)
	require.Empty(t, savedPositions(t, store), "unACKed partial-setup receipt is not a restart checkpoint")
	require.Error(t, retiredHandler(context.Background(), fixtureEvent(201, 1)))
	require.NoError(t, calls[1].handler(context.Background(), fixtureEvent(201, 1)))
	c.ack(1)
	require.Eventually(t, func() bool {
		p, e := decodePosition(store.snapshot().Position)
		return e == nil && p["1"].Offset == "200"
	}, time.Second, time.Millisecond)
	c.ack(2)
	require.NoError(t, a.Close())
	require.Equal(t, "201", savedPositions(t, store)["1"].Offset)
}
func TestInternalStopsRemainMetricOnly(t *testing.T) {
	for _, mode := range []string{"tracker_limit", "backward_position", "unverified_cancel", "invalid_partition", "nil_receiver"} {
		t.Run(mode, func(t *testing.T) {
			c := newTestClient()
			h := &testHub{ids: []string{"0", "1"}}
			callbacks := make(chan error, 2)
			var stalls atomic.Int32
			store := seededStore(t)
			if mode == "invalid_partition" {
				h.ids = []string{strings.Repeat("x", maxIdentityBytes+1)}
			}
			a := newReviewAdapter(t, h, c, store, func(code string) {
				if code == "internal_stall" {
					stalls.Add(1)
				}
			}, func(err error) { callbacks <- err })
			if mode == "tracker_limit" {
				var err error
				a.tracker, err = utils.NewACKTracker(c, 1, 1100)
				require.NoError(t, err)
			}
			if mode == "nil_receiver" {
				a.hub = &discoveryHub{testHub: testHub{ids: []string{"0", "1"}}, receive: func(context.Context, string, eventhub.Handler, receiverStart) (hubListener, error) { return nil, nil }}
			}
			workerDone := startWorker(t, a)
			if mode != "invalid_partition" && mode != "nil_receiver" {
				calls := eventuallyCalls(t, h, 2)
				switch mode {
				case "tracker_limit":
					require.NoError(t, calls[0].handler(context.Background(), fixtureEvent(101, 1)))
					require.ErrorIs(t, calls[1].handler(context.Background(), fixtureEvent(200, 0)), utils.ErrCheckpointLimit)
				case "backward_position":
					require.Error(t, calls[0].handler(context.Background(), fixtureEvent(99, 1)))
					require.Zero(t, c.calls())
					require.NoError(t, a.ctx.Err(), "backward suspicion must pause only its partition pending proof")
					require.NoError(t, calls[1].handler(context.Background(), fixtureEvent(200, 0)))
					require.Equal(t, "100", savedPositions(t, store)["0"].Offset)
					c.ack(1)
					require.NoError(t, a.Close())
					<-workerDone
					require.Empty(t, callbacks, "unproven reset must remain operational-only")
					return
				case "unverified_cancel":
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					require.Error(t, calls[0].handler(ctx, fixtureEvent(101, 1)))
				}
			}
			select {
			case <-a.chStopped:
			case <-time.After(time.Second):
				t.Fatal("internal source worker did not stop")
			}
			require.EqualValues(t, 1, stalls.Load())
			select {
			case err := <-callbacks:
				t.Fatalf("internal stop became a vendor error: %v", err)
			default:
			}
			if mode != "tracker_limit" {
				require.Equal(t, "100", savedPositions(t, store)["0"].Offset, "internal/reset suspicion must not clear or skip stored state")
			}
			c.ack(100)
			_ = a.Close()
			select {
			case err := <-callbacks:
				t.Fatalf("delayed internal stop became a vendor error: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
		})
	}
}
func TestMalformedVendorEventReportsOnceAfterFreeze(t *testing.T) {
	c := newTestClient()
	h := &testHub{ids: []string{"0"}}
	callbacks := make(chan error, 2)
	a := newReviewAdapter(t, h, c, nil, nil, func(err error) { callbacks <- err })
	startWorker(t, a)
	call := eventuallyCalls(t, h, 1)[0]
	event := fixtureEvent(100, 0)
	event.SystemProperties.Offset = nil
	require.Error(t, call.handler(context.Background(), event))
	require.Error(t, call.handler(context.Background(), fixtureEvent(101, 1)))
	require.Zero(t, c.calls())
	select {
	case err := <-callbacks:
		require.Contains(t, err.Error(), "invalid event metadata")
	case <-time.After(time.Second):
		t.Fatal("verified malformed vendor event was silently hidden")
	}
	require.NoError(t, a.Close())
	select {
	case <-callbacks:
		t.Fatal("duplicate malformed-event callback")
	default:
	}
}
func TestSafeVendorCauseCategories(t *testing.T) {
	secret := "sensitive-fixture"
	cases := []struct {
		name       string
		err        error
		diagnostic string
	}{
		{"wrapped-amqp", fmt.Errorf("wrapped: %w", &amqp.LinkError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondUnauthorizedAccess, Description: secret}}), "unauthorized-access"},
		{"decode", &mapstructure.Error{Errors: []string{"bad annotation " + secret}}, "decode failure"},
		{"url", &url.Error{Op: "POST", URL: "https://example.invalid?sig=" + secret, Err: &net.OpError{Op: "dial", Err: errors.New(secret)}}, "network connection failed"},
		{"dns", &net.DNSError{Err: secret, Name: secret}, "name resolution failed"},
		{"unknown-condition", &amqp.Error{Condition: amqp.ErrCond(secret), Description: secret}, "AMQP protocol error"},
		{"unknown-raw", errors.New(secret + " ?sig=" + secret), "unclassified cause"},
		{"auth", errors.New("CBS token authentication failed " + secret), "authentication failed"},
		{"untrusted-parser-suffix", errors.New(`key "SharedAccessKey" must not be empty ` + secret), "unclassified cause"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			safe := vendorFailure("ListenerHandle.Err()", c.err)
			require.Contains(t, safe.Error(), c.diagnostic)
			require.NotContains(t, safe.Error(), secret)
			require.NotContains(t, safe.Error(), "https://")
			require.Nil(t, errors.Unwrap(safe), "raw error must not survive as an unwrap cause")
		})
	}
}

func TestCleanupOnlySDKErrorAndInternalCauseStayQuiet(t *testing.T) {
	for _, mode := range []string{"generation_retiring", "adapter_closing", "closed_client", "internal_checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			a, g, _ := harness(t, newTestClient())
			l := &testListener{done: make(chan struct{}), err: &amqp.ConnError{}}
			switch mode {
			case "generation_retiring":
				g.retiring.Store(true)
			case "adapter_closing":
				a.closing.Store(true)
			case "closed_client":
				l.err = uspclient.ErrorClosed
			case "internal_checkpoint":
				l.err = utils.ErrCheckpointLimit
			}
			close(l.done)
			a.captureListenerFailure(g, l)
			a.failureMu.Lock()
			failure := a.failure
			a.failureMu.Unlock()
			require.Nil(t, failure, "intentional cleanup/internal state must not create a vendor failure")
		})
	}
}

type overwrittenCauseListener struct {
	done  chan struct{}
	cause error
}

func (l *overwrittenCauseListener) Done() <-chan struct{} { return l.done }
func (l *overwrittenCauseListener) Err() error            { return l.cause }
func (l *overwrittenCauseListener) Close(context.Context) error {
	l.cause = context.Canceled
	return nil
}
func TestCanceledGenerationSnapshotsVerifiedCauseBeforeListenerCleanup(t *testing.T) {
	a, g, _ := harness(t, newTestClient())
	l := &overwrittenCauseListener{done: make(chan struct{}), cause: &amqp.Error{Condition: amqp.ErrCondDecodeError}}
	close(l.done)
	g.parts["0"] = &partitionState{listener: l}
	g.cancel()
	a.stopGeneration(g)
	a.failureMu.Lock()
	failure := a.failure
	a.failureMu.Unlock()
	require.Error(t, failure)
	require.Contains(t, failure.Error(), "amqp:decode-error")
	require.ErrorIs(t, l.Err(), context.Canceled, "fixture must erase its cause during cleanup")
}

func TestFailedShipReturnsSanitizedVendorCause(t *testing.T) {
	c := newTestClient()
	c.fail = &amqp.Error{Condition: amqp.ErrCondUnauthorizedAccess, Description: "https://example.invalid?sig=sensitive-fixture payload-fixture"}
	a, g, _ := harness(t, c)
	err := process(t, a, g, "0", 100, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Ship(): amqp:unauthorized-access")
	require.NotContains(t, err.Error(), "sensitive-fixture")
	require.NotContains(t, err.Error(), "payload-fixture")
	require.NotContains(t, err.Error(), "https://")
	require.Nil(t, errors.Unwrap(err))
	a.failureMu.Lock()
	failure := a.failure
	a.failureMu.Unlock()
	require.EqualError(t, failure, err.Error())
	require.Error(t, process(t, a, g, "0", 101, 1))
	require.Equal(t, 1, c.calls())
}

func TestSDKReceiveKeepsNilHandleAsNilInterface(t *testing.T) {
	cause := &amqp.Error{Condition: amqp.ErrCondUnauthorizedAccess}
	listener, err := sdkListenerResult(nil, cause)
	require.True(t, listener == nil, "a typed nil interface would send failed setup cleanup through a nil SDK handle")
	require.Same(t, cause, err)
	listener, err = sdkListenerResult(nil, nil)
	require.True(t, listener == nil)
	require.NoError(t, err)
	sdk := &eventhub.ListenerHandle{}
	listener, err = sdkListenerResult(sdk, cause)
	require.Same(t, sdk, listener)
	require.Same(t, cause, err)
}
