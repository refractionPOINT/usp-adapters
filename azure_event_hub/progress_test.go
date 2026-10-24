package usp_azure_event_hub

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
)

const testConnection = "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=not-a-real-key;EntityPath=events"

type testClient struct {
	activeShips    int
	mu             sync.Mutex
	seq            uint64
	pending        []*protocol.DataMessage
	fail           error
	blocked        bool
	blockedStarted chan struct{}
	sealed         chan struct{}
	sealOnce       sync.Once
	shipCalls      int
	maxTimeout     time.Duration
	drainCalls     int
}

func newTestClient() *testClient {
	return &testClient{sealed: make(chan struct{}), blockedStarted: make(chan struct{}, 1)}
}
func (c *testClient) Ship(msg *protocol.DataMessage, timeout time.Duration) error {
	c.mu.Lock()
	c.shipCalls++
	c.activeShips++
	defer func() { c.mu.Lock(); c.activeShips--; c.mu.Unlock() }()
	if timeout > c.maxTimeout {
		c.maxTimeout = timeout
	}
	fail, blocked := c.fail, c.blocked
	c.mu.Unlock()
	select {
	case <-c.sealed:
		return uspclient.ErrorClosed
	default:
	}
	if fail != nil {
		return fail
	}
	if blocked {
		select {
		case c.blockedStarted <- struct{}{}:
		default:
		}
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-c.sealed:
			return uspclient.ErrorClosed
		case <-timer.C:
			return uspclient.ErrorBufferFull
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	msg.SeqNum = c.seq
	c.pending = append(c.pending, msg)
	return nil
}
func (c *testClient) GetUnsent() ([]*protocol.DataMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*protocol.DataMessage(nil), c.pending...), nil
}
func (c *testClient) ack(seq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.pending) > 0 && c.pending[0].SeqNum <= seq {
		c.pending = c.pending[1:]
	}
}
func (c *testClient) BeginDrain() { c.sealOnce.Do(func() { close(c.sealed) }) }
func (c *testClient) Drain(time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drainCalls++
	return nil
}
func (c *testClient) Close() ([]*protocol.DataMessage, error) { c.BeginDrain(); return c.GetUnsent() }
func (c *testClient) calls() int                              { c.mu.Lock(); defer c.mu.Unlock(); return c.shipCalls }

type testListener struct {
	done    chan struct{}
	once    sync.Once
	err     error
	onClose func()
}

func (l *testListener) Close(context.Context) error {
	l.once.Do(func() {
		if l.onClose != nil {
			l.onClose()
		}
		close(l.done)
	})
	return nil
}
func (l *testListener) Done() <-chan struct{} { return l.done }
func (l *testListener) Err() error            { return l.err }

type receiverCall struct {
	id       string
	start    receiverStart
	handler  eventhub.Handler
	listener *testListener
}
type testHub struct {
	mu         sync.Mutex
	ids        []string
	calls      []receiverCall
	receiveErr error
	closeFn    func()
}

func (h *testHub) Partitions(context.Context) ([]string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.ids...), nil
}
func (h *testHub) Receive(_ context.Context, id string, handler eventhub.Handler, start receiverStart) (hubListener, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.receiveErr != nil {
		return nil, h.receiveErr
	}
	l := &testListener{done: make(chan struct{})}
	h.calls = append(h.calls, receiverCall{id, start, handler, l})
	return l, nil
}
func (h *testHub) Close(context.Context) error {
	if h.closeFn != nil {
		h.closeFn()
	}
	return nil
}
func (h *testHub) snapshot() []receiverCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]receiverCall(nil), h.calls...)
}
func (h *testHub) setIDs(ids ...string) { h.mu.Lock(); defer h.mu.Unlock(); h.ids = ids }

func fixtureEvent(offset, seq int64) *eventhub.Event {
	enqueued := time.Unix(1700000000, 123000000)
	return &eventhub.Event{Data: []byte("fixture"), SystemProperties: &eventhub.SystemProperties{
		Offset: &offset, SequenceNumber: &seq, EnqueuedTime: &enqueued,
		Annotations: map[string]interface{}{"x-opt-offset": offset, "x-opt-sequence-number": seq, "x-opt-enqueued-time": enqueued},
	}}
}
func fixturePosition(offset, seq int64) partitionPosition {
	p, _ := eventPosition(fixtureEvent(offset, seq))
	return p
}
func harness(t *testing.T, client adapterClient) (*EventHubAdapter, *generation) {
	t.Helper()
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	a, err := newAdapter(context.Background(), EventHubConfig{ConnectionString: testConnection}, &testHub{}, client, source)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(a.ctx)
	g := &generation{id: 1, ctx: ctx, cancel: cancel, parts: make(map[string]*partitionState), confirmed: make(map[string]int64), stopped: make(chan struct{}, 1)}
	t.Cleanup(a.cancel)
	t.Cleanup(cancel)
	return a, g
}
func process(t *testing.T, a *EventHubAdapter, g *generation, id string, offset, seq int64) error {
	t.Helper()
	p := g.parts[id]
	if p == nil {
		p = &partitionState{}
		g.parts[id] = p
	}
	return a.processPartitionEvent(context.Background(), g, id, p, fixtureEvent(offset, seq))
}

func startWorker(t *testing.T, a *EventHubAdapter) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); a.run() }()
	t.Cleanup(func() {
		a.cancel()
		select {
		case <-a.chStopped:
		case <-time.After(3 * time.Second):
			t.Error("worker did not stop")
		}
	})
	return done
}
func eventuallyCalls(t *testing.T, h *testHub, count int) []receiverCall {
	t.Helper()
	require.Eventually(t, func() bool { return len(h.snapshot()) >= count }, 2*time.Second, time.Millisecond)
	return h.snapshot()
}

func TestSourceCredentialRotationAndSourceSwitch(t *testing.T) {
	a, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	b, err := sourceFingerprint("Endpoint=sb://EXAMPLE.servicebus.windows.net;SharedAccessKeyName=rotated;SharedAccessKey=other;EntityPath=events")
	require.NoError(t, err)
	require.Equal(t, a, b)
	for _, connection := range []string{
		"Endpoint=sb://other.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=key;EntityPath=events",
		"Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=key;EntityPath=other",
	} {
		b, err = sourceFingerprint(connection)
		require.NoError(t, err)
		require.NotEqual(t, a, b)
	}
}

func TestReceiverOptionsUseSDKExactOffsetAndLatest(t *testing.T) {
	for _, offset := range []string{"", "123456", "000123"} {
		options := receiverStart{Offset: offset}.options()
		require.Len(t, options, 1)
		option := reflect.ValueOf(options[0])
		receiver := reflect.New(option.Type().In(0).Elem())
		result := option.Call([]reflect.Value{receiver})
		require.True(t, result[0].IsNil())
		want := offset
		if want == "" {
			want = "@latest"
		}
		require.Equal(t, want, receiver.Elem().FieldByName("checkpoint").FieldByName("Offset").String())
	}
}
func TestACKOnlyAndPartialFrontier(t *testing.T) {
	c := newTestClient()
	a, g := harness(t, c)
	require.NoError(t, process(t, a, g, "0", 100, 0))
	require.NoError(t, process(t, a, g, "0", 101, 1))
	require.NoError(t, process(t, a, g, "1", 200, 0))
	require.NoError(t, a.confirm(g))
	require.Empty(t, g.confirmed, "successful Ship is not an ACK")
	c.ack(1)
	require.NoError(t, a.confirm(g))
	require.Equal(t, map[string]int64{"0": 0}, g.confirmed)
	c.ack(3)
	require.NoError(t, a.confirm(g))
	require.Equal(t, map[string]int64{"0": 1, "1": 0}, g.confirmed)
}
func TestFailedSendFreezesBeforeLaterHandlerAndClose(t *testing.T) {
	c := newTestClient()
	a, g := harness(t, c)
	require.NoError(t, process(t, a, g, "0", 100, 0))
	c.ack(1)
	c.mu.Lock()
	c.fail = errors.New("send failed")
	c.mu.Unlock()
	require.Error(t, process(t, a, g, "0", 101, 1))
	c.mu.Lock()
	c.fail = nil
	c.mu.Unlock()
	require.Error(t, process(t, a, g, "0", 101, 1))
	require.Error(t, process(t, a, g, "0", 102, 2))
	require.Equal(t, 2, c.calls(), "later handler crossed a failed-send hole")
	a.final = g
	close(a.chStopped)
	require.NoError(t, a.confirm(g))
	require.NoError(t, a.Close())
	require.Equal(t, map[string]int64{"0": 0}, g.confirmed, "failed send never confirms beyond its hole")
}
func TestInvalidAndParseFailedEventsFreeze(t *testing.T) {
	for _, mode := range []string{"nil", "metadata", "partial-parse", "canceled-parse"} {
		t.Run(mode, func(t *testing.T) {
			c := newTestClient()
			a, g := harness(t, c)
			p := &partitionState{}
			g.parts["0"] = p
			event := fixtureEvent(100, 0)
			ctx := context.Background()
			switch mode {
			case "nil":
				event = nil
			case "metadata":
				event.SystemProperties.Offset = nil
			case "partial-parse":
				event.SystemProperties.Annotations = nil
			case "canceled-parse":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			require.Error(t, a.processPartitionEvent(ctx, g, "0", p, event))
			require.Error(t, process(t, a, g, "0", 101, 1))
			require.Zero(t, c.calls())
			require.NoError(t, a.confirm(g))
			require.Empty(t, g.confirmed)
		})
	}
}
func TestPositionTiesAndGaps(t *testing.T) {
	for _, test := range []struct {
		name        string
		offset, seq int64
		valid       bool
	}{{"duplicate", 100, 10, true}, {"timestamp-tie", 101, 11, true}, {"offset-tie", 100, 11, false}, {"sequence-tie", 101, 10, false}, {"sequence-gap", 102, 12, true}, {"offset-reversed", 99, 11, false}} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestClient()
			a, g := harness(t, c)
			require.NoError(t, process(t, a, g, "0", 100, 10))
			err := process(t, a, g, "0", test.offset, test.seq)
			if test.valid {
				require.NoError(t, err)
				require.Equal(t, 2, c.calls())
			} else {
				require.Error(t, err)
				require.Equal(t, 1, c.calls())
			}
		})
	}
}

func TestOldGenerationCandidatesAreIgnored(t *testing.T) {
	c := newTestClient()
	a, g := harness(t, c)
	require.NoError(t, process(t, a, g, "0", 100, 0))
	c.ack(1)
	g.id = 2
	require.NoError(t, a.confirm(g))
	require.Empty(t, g.confirmed, "an old local generation marker must be ignored")
}

func TestPartitionDiscoveryStartsLatest(t *testing.T) {
	c := newTestClient()
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	h := &testHub{ids: []string{"0"}}
	a, err := newAdapter(context.Background(), EventHubConfig{}, h, c, source)
	require.NoError(t, err)
	a.discoverEvery = 5 * time.Millisecond
	startWorker(t, a)
	calls := eventuallyCalls(t, h, 1)
	require.Empty(t, calls[0].start.Offset)
	h.setIDs("0", "1")
	calls = eventuallyCalls(t, h, 2)
	require.Equal(t, "1", calls[1].id)
	require.Empty(t, calls[1].start.Offset)
}
func TestShutdownImmediatelyUnblocksProducerAndIsIdempotent(t *testing.T) {
	c := newTestClient()
	c.blocked = true
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	h := &testHub{ids: []string{"0"}}
	var vendorErrors atomic.Int32
	a, err := newAdapter(context.Background(), EventHubConfig{ClientOptions: uspclient.ClientOptions{OnError: func(error) { vendorErrors.Add(1) }}}, h, c, source)
	require.NoError(t, err)
	startWorker(t, a)
	call := eventuallyCalls(t, h, 1)[0]
	done := make(chan error, 1)
	go func() { done <- call.handler(context.Background(), fixtureEvent(100, 0)) }()
	select {
	case <-c.blockedStarted:
	case <-time.After(time.Second):
		t.Fatal("producer not blocked")
	}
	start := time.Now()
	require.NoError(t, a.Close())
	require.Less(t, time.Since(start), 500*time.Millisecond)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("producer leaked")
	}
	require.Zero(t, vendorErrors.Load())
	require.NoError(t, a.Close())
	c.mu.Lock()
	require.Equal(t, 1, c.drainCalls)
	require.LessOrEqual(t, c.maxTimeout, time.Second)
	c.mu.Unlock()
}
func TestListenerFailureStopsOnceWithoutBlockedNotifications(t *testing.T) {
	c := newTestClient()
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	h := &testHub{ids: []string{"0", "1"}}
	a, err := newAdapter(context.Background(), EventHubConfig{}, h, c, source)
	require.NoError(t, err)
	startWorker(t, a)
	calls := eventuallyCalls(t, h, 2)
	for _, call := range calls {
		require.NoError(t, call.listener.Close(context.Background()))
	}
	select {
	case <-a.chStopped:
	case <-time.After(time.Second):
		t.Fatal("listener notifications blocked")
	}
	require.NoError(t, a.Close())
}

func TestConcurrentPartitionsSerializeAndFreeze(t *testing.T) {
	c := newTestClient()
	a, g := harness(t, c)
	for i := 0; i < 16; i++ {
		g.parts[fmt.Sprint(i)] = &partitionState{}
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprint(i)
			for n := 0; n < 100; n++ {
				if a.processPartitionEvent(context.Background(), g, id, g.parts[id], fixtureEvent(int64(100+n), int64(n))) != nil {
					return
				}
			}
		}(i)
	}
	wg.Wait()
	require.Equal(t, 1600, c.calls())
	c.ack(1600)
	require.NoError(t, a.confirm(g))
	require.Len(t, g.confirmed, 16)
}

func TestSameSDKReceiverReconnectKeepsAdmittedTail(t *testing.T) {
	c := newTestClient()
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	h := &testHub{ids: []string{"0"}}
	a, err := newAdapter(context.Background(), EventHubConfig{}, h, c, source)
	require.NoError(t, err)
	a.ackEvery = 5 * time.Millisecond
	startWorker(t, a)
	call := eventuallyCalls(t, h, 1)[0]
	require.NoError(t, call.handler(context.Background(), fixtureEvent(100, 0)))
	// SDK receiver recovery continues the same handler from its own in-memory
	// admitted offset, independently of the older ACK frontier. No new Receive call.
	require.NoError(t, call.handler(context.Background(), fixtureEvent(101, 1)))
	pending, err := c.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	require.Len(t, h.snapshot(), 1)
	c.ack(1)
	require.ErrorContains(t, a.Close(), "1 unacknowledged messages")
	require.Len(t, h.snapshot(), 1)
	require.Equal(t, 2, c.calls())
	pending, err = c.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.EqualValues(t, 2, pending[0].SeqNum)
}

func TestCanceledHandlerCannotAdmitAcrossAnotherPartitionHole(t *testing.T) {
	c := newTestClient()
	c.blocked = true
	a, g := harness(t, c)
	g.parts["0"] = &partitionState{}
	g.parts["1"] = &partitionState{}
	blocked := make(chan error, 1)
	go func() {
		blocked <- a.processPartitionEvent(context.Background(), g, "0", g.parts["0"], fixtureEvent(100, 0))
	}()
	select {
	case <-c.blockedStarted:
	case <-time.After(time.Second):
		t.Fatal("not blocked")
	}
	bad := make(chan error, 1)
	go func() { bad <- a.processPartitionEvent(context.Background(), g, "1", g.parts["1"], nil) }()
	require.Error(t, <-bad)
	select {
	case <-blocked:
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("cancellation did not bound the in-flight Ship")
	}
	require.NoError(t, a.confirm(g))
	require.Empty(t, g.confirmed)
	c.mu.Lock()
	require.LessOrEqual(t, c.maxTimeout, time.Second)
	c.mu.Unlock()
}

func TestImmediateBufferFullDoesNotHotLoop(t *testing.T) {
	c := newTestClient()
	c.fail = uspclient.ErrorBufferFull
	a, g := harness(t, c)
	ctx, cancel := context.WithTimeout(g.ctx, 35*time.Millisecond)
	defer cancel()
	g.ctx = ctx
	require.Error(t, process(t, a, g, "0", 100, 0))
	require.LessOrEqual(t, c.calls(), 5, "immediate BufferFull must wait between attempts")
	require.NoError(t, a.confirm(g))
	require.Empty(t, g.confirmed)
}

func TestClosedAdmissionFreezesWithoutConfirming(t *testing.T) {
	c := newTestClient()
	c.fail = uspclient.ErrorClosed
	a, g := harness(t, c)
	var vendorErrors atomic.Int32
	a.conf.ClientOptions.OnError = func(error) { vendorErrors.Add(1) }
	require.NoError(t, process(t, a, g, "0", 100, 0))
	c.mu.Lock()
	c.fail = nil
	c.mu.Unlock()
	require.Error(t, process(t, a, g, "0", 101, 1))
	require.Equal(t, 1, c.calls())
	require.NoError(t, a.confirm(g))
	require.Empty(t, g.confirmed)
	require.Zero(t, vendorErrors.Load())
}

func TestListenerErrorCallbackCanCloseAdapter(t *testing.T) {
	c := newTestClient()
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	h := &testHub{ids: []string{"0"}}
	var a *EventHubAdapter
	callbackDone := make(chan struct{})
	conf := EventHubConfig{ClientOptions: uspclient.ClientOptions{OnError: func(error) { _ = a.Close(); close(callbackDone) }}}
	a, err = newAdapter(context.Background(), conf, h, c, source)
	require.NoError(t, err)
	startWorker(t, a)
	call := eventuallyCalls(t, h, 1)[0]
	call.listener.err = errors.New("receiver failed")
	require.NoError(t, call.listener.Close(context.Background()))
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("OnError callback deadlocked closing its own adapter")
	}
	require.NoError(t, a.Close())
}
