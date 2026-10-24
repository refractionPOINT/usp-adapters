package usp_azure_event_hub

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

const testConnection = "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=not-a-real-key;EntityPath=events"

type testStore struct {
	beforeLoad        func()
	mu                sync.Mutex
	value             utils.Checkpoint
	found             bool
	refuse            bool
	loadErr, errorCAS error
	ambiguous         bool
	blockLoad         bool
	loads, swaps      int
}

func (s *testStore) Load(ctx context.Context) (utils.Checkpoint, bool, error) {
	if s.beforeLoad != nil {
		s.beforeLoad()
	}
	s.mu.Lock()
	s.loads++
	block := s.blockLoad
	if !block {
		defer s.mu.Unlock()
		return s.value.Clone(), s.found, s.loadErr
	}
	s.mu.Unlock()
	<-ctx.Done()
	return utils.Checkpoint{}, false, ctx.Err()
}
func (s *testStore) CompareAndSwap(ctx context.Context, f utils.CheckpointFence, next utils.Checkpoint) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.swaps++
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if s.refuse && !s.found {
		return false, utils.ErrCheckpointAllocationRefused
	}
	if s.errorCAS != nil {
		return false, s.errorCAS
	}
	if s.found && s.value.CheckpointFence == next.CheckpointFence && s.value.Attempt == next.Attempt {
		return true, nil
	}
	if (!s.found && f != (utils.CheckpointFence{})) || (s.found && f != s.value.CheckpointFence) {
		return false, nil
	}
	s.value = next.Clone()
	s.found = true
	if s.ambiguous {
		s.ambiguous = false
		s.loadErr = errors.New("unavailable")
		return false, errors.New("unknown outcome")
	}
	return true, nil
}
func (s *testStore) snapshot() utils.Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value.Clone()
}

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
func harness(t *testing.T, client adapterClient) (*EventHubAdapter, *generation, *testStore) {
	t.Helper()
	store := &testStore{}
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	a, err := newAdapter(context.Background(), EventHubConfig{CheckpointStore: store}, &testHub{}, client, source)
	require.NoError(t, err)
	session, raw, err := a.openCheckpoint()
	require.NoError(t, err)
	positions, err := decodePosition(raw)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(a.ctx)
	g := &generation{id: 1, ctx: ctx, cancel: cancel, session: session, parts: make(map[string]*partitionState), confirmed: positions, latest: raw, stopped: make(chan struct{}, 1)}
	t.Cleanup(a.cancel)
	t.Cleanup(cancel)
	return a, g, store
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
func savedPositions(t *testing.T, store *testStore) map[string]partitionPosition {
	t.Helper()
	p, err := decodePosition(store.snapshot().Position)
	require.NoError(t, err)
	return p
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

func TestCheckpointConfigGoOnly(t *testing.T) {
	c := EventHubConfig{CheckpointStore: &testStore{}, CheckpointIssue: func(string) {}}
	b, err := json.Marshal(c)
	require.NoError(t, err)
	require.NotContains(t, string(b), "CheckpointStore")
	require.NotContains(t, string(b), "checkpoint_store")
	require.NotContains(t, string(b), "CheckpointIssue")
	require.NotContains(t, string(b), "checkpoint_issue")
	b, err = yaml.Marshal(c)
	require.NoError(t, err)
	require.NotContains(t, string(b), "checkpointstore")
	require.NotContains(t, string(b), "checkpoint_store")
	require.NotContains(t, string(b), "checkpointissue")
	require.NotContains(t, string(b), "checkpoint_issue")
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
func TestPositionCompactExactAndCompleteSizeCap(t *testing.T) {
	positions := map[string]partitionPosition{}
	for i := 0; i < 1024; i++ {
		positions[strconv.Itoa(i)] = fixturePosition(int64(9000000000+i), int64(i))
	}
	raw, err := encodePosition(positions)
	require.NoError(t, err)
	require.Len(t, raw, 26627)
	require.LessOrEqual(t, len(raw)+64, 32768)
	decoded, err := decodePosition(raw)
	require.NoError(t, err)
	require.Equal(t, positions, decoded)
	positions["partition-name"] = partitionPosition{"000123", 100, time.Now().UnixNano()}
	raw, err = encodePosition(positions)
	require.NoError(t, err)
	decoded, err = decodePosition(raw)
	require.NoError(t, err)
	require.Equal(t, positions, decoded)
	for i := 1024; i < 1500; i++ {
		positions[strconv.Itoa(i)] = fixturePosition(int64(i), int64(i))
	}
	raw, err = encodePosition(positions)
	require.ErrorIs(t, err, utils.ErrCheckpointLimit)
	require.Nil(t, raw, "must reject the complete snapshot, never truncate")
}
func TestPositionRejectsCorruption(t *testing.T) {
	raw, err := encodePosition(map[string]partitionPosition{"0": fixturePosition(12, 2)})
	require.NoError(t, err)
	for n := 0; n < len(raw); n++ {
		_, err = decodePosition(raw[:n])
		require.Error(t, err, "truncated position at %d", n)
	}
	for _, offset := range []string{"", "-1", "1' OR 1=1", "9223372036854775808"} {
		_, err := encodePosition(map[string]partitionPosition{"0": {offset, 1, 1700000000000000000}})
		require.Error(t, err)
	}
	bad := append([]byte(nil), raw...)
	binary.BigEndian.PutUint16(bad[1:3], 2)
	bad = append(bad, raw[3:]...)
	_, err = decodePosition(bad)
	require.Error(t, err, "duplicate partition")
}
func TestR1NilMissingRefusedLatestAndValidResume(t *testing.T) {
	for _, mode := range []string{"nil", "missing", "refused", "valid", "changed-source"} {
		t.Run(mode, func(t *testing.T) {
			source, err := sourceFingerprint(testConnection)
			require.NoError(t, err)
			store := &testStore{refuse: mode == "refused"}
			if mode == "valid" || mode == "changed-source" {
				raw, e := encodePosition(map[string]partitionPosition{"0": fixturePosition(123, 10)})
				require.NoError(t, e)
				fp := source
				if mode == "changed-source" {
					fp[0] ^= 1
				}
				store.value = utils.Checkpoint{SchemaVersion: checkpointSchema, SourceFingerprint: fp, Position: raw, CheckpointFence: utils.CheckpointFence{Nonce: "old", Revision: "old"}}
				store.found = true
			}
			conf := EventHubConfig{CheckpointStore: store}
			if mode == "nil" {
				conf.CheckpointStore = nil
			}
			h := &testHub{ids: []string{"0", "new-partition"}}
			a, e := newAdapter(context.Background(), conf, h, newTestClient(), source)
			require.NoError(t, e)
			startWorker(t, a)
			calls := eventuallyCalls(t, h, 2)
			for _, call := range calls {
				want := ""
				if mode == "valid" && call.id == "0" {
					want = "123"
				}
				require.Equal(t, want, call.start.Offset)
			}
			if mode == "valid" {
				require.Equal(t, source, store.snapshot().SourceFingerprint)
			}
		})
	}
}
func TestCheckpointUnavailableAndCorruptWaitCancellable(t *testing.T) {
	for _, mode := range []string{"unavailable", "corrupt", "schema", "blocking"} {
		t.Run(mode, func(t *testing.T) {
			source, err := sourceFingerprint(testConnection)
			require.NoError(t, err)
			store := &testStore{}
			switch mode {
			case "unavailable":
				store.loadErr = errors.New("unavailable")
			case "blocking":
				store.blockLoad = true
			default:
				store.found = true
				store.value = utils.Checkpoint{SourceFingerprint: source, SchemaVersion: checkpointSchema, Position: []byte("bad"), CheckpointFence: utils.CheckpointFence{Nonce: "n", Revision: "r"}}
				if mode == "schema" {
					store.value.Position, _ = encodePosition(nil)
					store.value.SchemaVersion = 2
				}
			}
			var errorsCount, issues atomic.Int32
			h := &testHub{ids: []string{"0"}}
			start := time.Now()
			a, e := newAdapter(context.Background(), EventHubConfig{CheckpointStore: store, CheckpointIssue: func(string) { issues.Add(1) }, ClientOptions: uspclient.ClientOptions{OnError: func(error) { errorsCount.Add(1) }}}, h, newTestClient(), source)
			require.NoError(t, e)
			require.Less(t, time.Since(start), 100*time.Millisecond)
			startWorker(t, a)
			require.Eventually(t, func() bool { store.mu.Lock(); defer store.mu.Unlock(); return store.loads > 0 }, time.Second, time.Millisecond)
			require.Empty(t, h.snapshot(), "unavailable/corrupt state cannot select latest")
			start = time.Now()
			require.NoError(t, a.Close())
			require.Less(t, time.Since(start), 500*time.Millisecond)
			require.Zero(t, errorsCount.Load())
			if mode != "blocking" {
				require.Eventually(t, func() bool { return issues.Load() > 0 }, time.Second, time.Millisecond)
			}
		})
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
	a, g, s := harness(t, c)
	require.NoError(t, process(t, a, g, "0", 100, 0))
	require.NoError(t, process(t, a, g, "0", 101, 1))
	require.NoError(t, process(t, a, g, "1", 200, 0))
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Empty(t, savedPositions(t, s), "successful Ship is not an ACK")
	c.ack(1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, map[string]partitionPosition{"0": fixturePosition(100, 0)}, savedPositions(t, s))
	c.ack(3)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, map[string]partitionPosition{"0": fixturePosition(101, 1), "1": fixturePosition(200, 0)}, savedPositions(t, s))
}
func TestFailedSendFreezesBeforeLaterHandlerAndClose(t *testing.T) {
	c := newTestClient()
	a, g, s := harness(t, c)
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
	require.NoError(t, a.Close())
	require.Equal(t, map[string]partitionPosition{"0": fixturePosition(100, 0)}, savedPositions(t, s), "Close may save only the older confirmed offset")
}
func TestInvalidAndParseFailedEventsFreeze(t *testing.T) {
	for _, mode := range []string{"nil", "metadata", "partial-parse", "canceled-parse"} {
		t.Run(mode, func(t *testing.T) {
			c := newTestClient()
			a, g, s := harness(t, c)
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
			require.NoError(t, a.saveConfirmed(context.Background(), g))
			require.Empty(t, savedPositions(t, s))
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
			a, g, _ := harness(t, c)
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
func TestOversizeRetainsLastCompleteCheckpointAndAllPositions(t *testing.T) {
	c := newTestClient()
	a, g, s := harness(t, c)
	var stalls atomic.Int32
	a.conf.CheckpointIssue = func(code string) {
		if code == "position_size_stall" {
			stalls.Add(1)
		}
	}
	require.NoError(t, process(t, a, g, "0", 100, 0))
	c.ack(1)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	before := s.snapshot().Position
	for i := 1; i < 1500; i++ {
		require.NoError(t, process(t, a, g, strconv.Itoa(i), 100, 0))
	}
	c.ack(1500)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, before, s.snapshot().Position)
	require.Len(t, g.confirmed, 1500)
	require.EqualValues(t, 1, stalls.Load())
	require.NoError(t, process(t, a, g, "0", 101, 1))
	require.Equal(t, 1501, c.calls(), "oversize state must keep ingestion running")
}
func TestUnknownSaveRetainsNewestCoalescedPosition(t *testing.T) {
	c := newTestClient()
	a, g, s := harness(t, c)
	require.NoError(t, process(t, a, g, "0", 100, 0))
	c.ack(1)
	s.mu.Lock()
	s.ambiguous = true
	s.mu.Unlock()
	require.Error(t, a.saveConfirmed(context.Background(), g))
	require.NoError(t, process(t, a, g, "0", 101, 1))
	require.NoError(t, process(t, a, g, "0", 102, 2))
	c.ack(3)
	s.mu.Lock()
	s.errorCAS = errors.New("still unknown")
	s.mu.Unlock()
	require.Error(t, a.saveConfirmed(context.Background(), g))
	latest, err := decodePosition(g.latest)
	require.NoError(t, err)
	require.Equal(t, "102", latest["0"].Offset)
	s.mu.Lock()
	s.loadErr = nil
	s.errorCAS = nil
	s.mu.Unlock()
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, "102", savedPositions(t, s)["0"].Offset)
}
func TestOldGenerationCandidatesAreIgnored(t *testing.T) {
	c := newTestClient()
	a, g, s := harness(t, c)
	require.NoError(t, process(t, a, g, "0", 100, 0))
	c.ack(1)
	g.id = 2
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Empty(t, savedPositions(t, s), "joined old generation must not advance a new claim")
}
func TestCheckpointStoreOperationsHaveDeadline(t *testing.T) {
	s := deadlineStore{t: t}
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	a, err := newAdapter(context.Background(), EventHubConfig{CheckpointStore: s}, &testHub{}, newTestClient(), source)
	require.NoError(t, err)
	defer a.cancel()
	_, _, err = a.openCheckpoint()
	require.NoError(t, err)
}

type deadlineStore struct{ t *testing.T }

func (s deadlineStore) Load(ctx context.Context) (utils.Checkpoint, bool, error) {
	d, ok := ctx.Deadline()
	require.True(s.t, ok)
	require.LessOrEqual(s.t, time.Until(d), 2*time.Second)
	return utils.Checkpoint{}, false, nil
}
func (s deadlineStore) CompareAndSwap(ctx context.Context, _ utils.CheckpointFence, _ utils.Checkpoint) (bool, error) {
	d, ok := ctx.Deadline()
	require.True(s.t, ok)
	require.LessOrEqual(s.t, time.Until(d), 2*time.Second)
	return true, nil
}

func TestPartitionDiscoveryKeepsR1Latest(t *testing.T) {
	c := newTestClient()
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	h := &testHub{ids: []string{"0"}}
	a, err := newAdapter(context.Background(), EventHubConfig{CheckpointStore: &testStore{}}, h, c, source)
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

// A lost save fence disables persistence for this acquisition; it never
// supplies authority to reload/reclaim or change the active source traversal.
func TestSaveConflictDisablesPersistenceAndRetainsDirectFrontier(t *testing.T) {
	c := newTestClient()
	a, g, s := harness(t, c)
	var conflicts, issues, vendorErrors atomic.Int32
	a.conf.CheckpointIssue = func(code string) {
		issues.Add(1)
		if code == "ownership_conflict" {
			conflicts.Add(1)
		}
	}
	a.conf.ClientOptions.OnError = func(error) { vendorErrors.Add(1) }
	require.NoError(t, process(t, a, g, "0", 100, 0))
	require.NoError(t, process(t, a, g, "0", 101, 1))
	c.ack(1)
	s.mu.Lock()
	s.value.Nonce, s.value.Revision = "successor", "successor"
	s.mu.Unlock()
	oldClient, oldTracker, oldPart := a.uspClient, a.tracker, g.parts["0"]
	require.NoError(t, a.saveConfirmed(context.Background(), g), "fence loss must not escape into collector control flow")
	require.Nil(t, g.session, "lost persistence must stay disabled for this acquisition")
	s.mu.Lock()
	loads, swaps := s.loads, s.swaps
	s.mu.Unlock()
	require.Equal(t, "100", g.confirmed["0"].Offset)
	require.NoError(t, process(t, a, g, "0", 102, 2))
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, "100", g.confirmed["0"].Offset, "pending 101/102 must remain dependencies")
	c.ack(3)
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Equal(t, "102", g.confirmed["0"].Offset)
	require.Same(t, oldClient, a.uspClient)
	require.Same(t, oldTracker, a.tracker)
	require.Same(t, oldPart, g.parts["0"])
	require.Equal(t, "102", oldPart.last.Offset)
	a.final = g
	close(a.chStopped)
	require.NoError(t, a.Close())
	s.mu.Lock()
	require.Equal(t, loads, s.loads, "lost writer must never reload")
	require.Equal(t, swaps, s.swaps, "neither ticks nor Close may retry a lost write")
	require.Equal(t, "successor", s.value.Nonce)
	s.mu.Unlock()
	require.EqualValues(t, 1, conflicts.Load())
	require.EqualValues(t, 1, issues.Load(), "lost save must emit one operational issue")
	require.Zero(t, vendorErrors.Load())
	a.failureMu.Lock()
	failure := a.failure
	a.failureMu.Unlock()
	require.Nil(t, failure, "checkpoint fence loss must not be queued as a vendor cause")
}

// Simulate a competing fence arriving after an uncertain older write resolves,
// immediately before Close offers the retained newer complete snapshot.
type conflictAfterCommitStore struct {
	*testStore
	replaceAfterCommit bool
}

func (s *conflictAfterCommitStore) CompareAndSwap(ctx context.Context, fence utils.CheckpointFence, next utils.Checkpoint) (bool, error) {
	ok, err := s.testStore.CompareAndSwap(ctx, fence, next)
	if ok && s.replaceAfterCommit {
		s.replaceAfterCommit = false
		s.mu.Lock()
		s.value.Nonce, s.value.Revision = "successor", "successor"
		s.mu.Unlock()
	}
	return ok, err
}
func TestCloseFinalFlushConflictDisablesPersistence(t *testing.T) {
	c := newTestClient()
	a, g, s := harness(t, c)
	wrapped := &conflictAfterCommitStore{testStore: s}
	var err error
	g.session, _, err = utils.OpenCheckpoint(a.ctx, wrapped, checkpointSchema, a.source,
		func() ([]byte, error) { return encodePosition(nil) },
		func(raw []byte) error { _, err := decodePosition(raw); return err }, nil)
	require.NoError(t, err)
	var conflicts atomic.Int32
	a.conf.CheckpointIssue = func(code string) {
		if code == "ownership_conflict" {
			conflicts.Add(1)
		}
	}
	require.NoError(t, process(t, a, g, "0", 100, 0))
	c.ack(1)
	s.ambiguous = true
	require.Error(t, a.saveConfirmed(context.Background(), g))
	require.NoError(t, process(t, a, g, "0", 101, 1))
	c.ack(2)
	wrapped.replaceAfterCommit = true
	a.final = g
	close(a.chStopped)
	require.NoError(t, a.Close(), "final flush fence loss is operational only")
	require.Nil(t, g.session)
	require.EqualValues(t, 1, conflicts.Load())
	require.Equal(t, "101", g.confirmed["0"].Offset)
	require.Equal(t, "100", savedPositions(t, s)["0"].Offset)
	require.Equal(t, "successor", s.snapshot().Nonce)
}

func TestReceiverSaveConflictKeepsTraversalAndPendingSDKHistory(t *testing.T) {
	c := newTestClient()
	a, _, s := harness(t, c)
	h := &testHub{ids: []string{"0"}}
	a.hub = h
	a.saveEvery = 5 * time.Millisecond
	var conflicts, vendorErrors atomic.Int32
	a.conf.CheckpointIssue = func(code string) {
		if code == "ownership_conflict" {
			conflicts.Add(1)
		}
	}
	a.conf.ClientOptions.OnError = func(error) { vendorErrors.Add(1) }
	workerDone := startWorker(t, a)
	first := eventuallyCalls(t, h, 1)[0]
	require.NoError(t, first.handler(context.Background(), fixtureEvent(100, 0)))
	c.ack(1)
	require.Eventually(t, func() bool { p, e := decodePosition(s.snapshot().Position); return e == nil && p["0"].Offset == "100" }, time.Second, time.Millisecond)
	require.NoError(t, first.handler(context.Background(), fixtureEvent(101, 1)))
	s.mu.Lock()
	s.value.Nonce, s.value.Revision = "successor", "successor"
	s.mu.Unlock()
	require.NoError(t, first.handler(context.Background(), fixtureEvent(102, 2)))
	c.ack(2) // confirm 101; accepted 102 must stay pending across the lost save
	require.Eventually(t, func() bool { return conflicts.Load() != 0 }, time.Second, time.Millisecond)
	require.Len(t, h.snapshot(), 1, "save conflict must not reopen a receiver")
	select {
	case <-first.listener.Done():
		t.Fatal("save conflict retired the live listener")
	default:
	}
	s.mu.Lock()
	loads, swaps := s.loads, s.swaps
	s.mu.Unlock()
	require.NoError(t, first.handler(context.Background(), fixtureEvent(103, 3)))
	require.Equal(t, 4, c.calls(), "each actual event must be admitted once without replay")
	pending, err := c.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	require.EqualValues(t, 3, pending[0].SeqNum)
	require.EqualValues(t, 4, pending[1].SeqNum)
	c.ack(4)
	// Allow multiple save ticks after the additional ACK, then join for inspection.
	time.Sleep(25 * time.Millisecond)
	a.cancel()
	<-a.chStopped
	require.NoError(t, a.Close())
	<-workerDone // includes any post-join vendor callback
	require.Nil(t, a.final.session)
	require.Equal(t, "103", a.final.confirmed["0"].Offset)
	require.Equal(t, "103", a.final.parts["0"].last.Offset)
	require.Len(t, h.snapshot(), 1)
	s.mu.Lock()
	require.Equal(t, loads, s.loads)
	require.Equal(t, swaps, s.swaps)
	require.Equal(t, "100", savedPositionOffset(s.value.Position, "0"))
	s.mu.Unlock()
	require.EqualValues(t, 1, conflicts.Load())
	require.Zero(t, vendorErrors.Load())
}

func savedPositionOffset(raw []byte, id string) string {
	positions, _ := decodePosition(raw)
	return positions[id].Offset
}

func TestConcurrentPartitionsSerializeAndFreeze(t *testing.T) {
	c := newTestClient()
	a, g, _ := harness(t, c)
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
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Len(t, g.confirmed, 16)
}

func TestCloseFlushesNewestAfterFrozenUnknownSave(t *testing.T) {
	c := newTestClient()
	a, g, s := harness(t, c)
	require.NoError(t, process(t, a, g, "0", 100, 0))
	c.ack(1)
	s.mu.Lock()
	s.ambiguous = true
	s.mu.Unlock()
	require.Error(t, a.saveConfirmed(context.Background(), g))
	require.NoError(t, process(t, a, g, "0", 101, 1))
	c.ack(2)
	a.final = g
	close(a.chStopped)
	require.NoError(t, a.Close())
	require.Equal(t, "101", savedPositions(t, s)["0"].Offset, "final save lost a newer confirmed frontier behind an older uncertain attempt")
}

func TestSameSDKReceiverReconnectKeepsAdmittedTail(t *testing.T) {
	c := newTestClient()
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	h := &testHub{ids: []string{"0"}}
	a, err := newAdapter(context.Background(), EventHubConfig{CheckpointStore: &testStore{}}, h, c, source)
	require.NoError(t, err)
	a.saveEvery = 5 * time.Millisecond
	startWorker(t, a)
	call := eventuallyCalls(t, h, 1)[0]
	require.NoError(t, call.handler(context.Background(), fixtureEvent(100, 0)))
	// SDK receiver recovery continues the same handler from its own in-memory
	// admitted offset, not the older persisted ACK frontier. No new Receive call.
	require.NoError(t, call.handler(context.Background(), fixtureEvent(101, 1)))
	pending, err := c.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	require.Len(t, h.snapshot(), 1)
	c.ack(1)
	require.Eventually(t, func() bool {
		p, e := decodePosition(a.conf.CheckpointStore.(*testStore).snapshot().Position)
		return e == nil && p["0"].Offset == "100"
	}, time.Second, time.Millisecond)
	require.Len(t, h.snapshot(), 1)
	require.Equal(t, 2, c.calls())
}

func TestReceiverStartErrorPreservesSavedPosition(t *testing.T) {
	source, err := sourceFingerprint(testConnection)
	require.NoError(t, err)
	raw, err := encodePosition(map[string]partitionPosition{"0": fixturePosition(100, 0)})
	require.NoError(t, err)
	store := &testStore{found: true, value: utils.Checkpoint{SchemaVersion: checkpointSchema, SourceFingerprint: source, Position: raw, CheckpointFence: utils.CheckpointFence{Nonce: "n", Revision: "r"}}}
	h := &testHub{ids: []string{"0"}, receiveErr: errors.New("receiver setup rejected offset")}
	var codesMu sync.Mutex
	var codes []string
	a, err := newAdapter(context.Background(), EventHubConfig{CheckpointStore: store, CheckpointIssue: func(code string) { codesMu.Lock(); defer codesMu.Unlock(); codes = append(codes, code) }}, h, newTestClient(), source)
	require.NoError(t, err)
	startWorker(t, a)
	select {
	case <-a.chStopped:
	case <-time.After(time.Second):
		t.Fatal("initial receiver setup error did not stop")
	}
	require.Equal(t, raw, store.snapshot().Position)
	require.Empty(t, h.snapshot())
	codesMu.Lock()
	require.Contains(t, codes, "receiver_start_failed")
	codesMu.Unlock()
	require.NoError(t, a.Close())
}

func TestCanceledHandlerCannotAdmitAcrossAnotherPartitionHole(t *testing.T) {
	c := newTestClient()
	c.blocked = true
	a, g, s := harness(t, c)
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
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Empty(t, savedPositions(t, s))
	c.mu.Lock()
	require.LessOrEqual(t, c.maxTimeout, time.Second)
	c.mu.Unlock()
}

func TestImmediateBufferFullDoesNotHotLoop(t *testing.T) {
	c := newTestClient()
	c.fail = uspclient.ErrorBufferFull
	a, g, s := harness(t, c)
	ctx, cancel := context.WithTimeout(g.ctx, 35*time.Millisecond)
	defer cancel()
	g.ctx = ctx
	require.Error(t, process(t, a, g, "0", 100, 0))
	require.LessOrEqual(t, c.calls(), 5, "immediate BufferFull must wait between attempts")
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Empty(t, savedPositions(t, s))
}

func TestClosedAdmissionFreezesWithoutCheckpoint(t *testing.T) {
	c := newTestClient()
	c.fail = uspclient.ErrorClosed
	a, g, s := harness(t, c)
	var vendorErrors atomic.Int32
	a.conf.ClientOptions.OnError = func(error) { vendorErrors.Add(1) }
	require.NoError(t, process(t, a, g, "0", 100, 0))
	c.mu.Lock()
	c.fail = nil
	c.mu.Unlock()
	require.Error(t, process(t, a, g, "0", 101, 1))
	require.Equal(t, 1, c.calls())
	require.NoError(t, a.saveConfirmed(context.Background(), g))
	require.Empty(t, savedPositions(t, s))
	require.Zero(t, vendorErrors.Load())
}

func TestSaveConflictDoesNotCancelBlockedProducer(t *testing.T) {
	c := newTestClient()
	a, _, s := harness(t, c)
	h := &testHub{ids: []string{"0"}}
	a.hub = h
	a.saveEvery = 5 * time.Millisecond
	var conflicts, vendorErrors atomic.Int32
	a.conf.CheckpointIssue = func(code string) {
		if code == "ownership_conflict" {
			conflicts.Add(1)
		}
	}
	a.conf.ClientOptions.OnError = func(error) { vendorErrors.Add(1) }
	workerDone := startWorker(t, a)
	first := eventuallyCalls(t, h, 1)[0]
	require.NoError(t, first.handler(context.Background(), fixtureEvent(100, 0)))
	c.mu.Lock()
	c.blocked = true
	c.mu.Unlock()
	blocked := make(chan error, 1)
	go func() { blocked <- first.handler(context.Background(), fixtureEvent(101, 1)) }()
	select {
	case <-c.blockedStarted:
	case <-time.After(time.Second):
		t.Fatal("producer did not block")
	}
	s.mu.Lock()
	s.value.Nonce, s.value.Revision = "successor", "successor"
	s.mu.Unlock()
	c.ack(1)
	require.Eventually(t, func() bool { return conflicts.Load() != 0 }, 2*time.Second, time.Millisecond)
	require.Len(t, h.snapshot(), 1, "fence loss must not replace the live traversal")
	select {
	case err := <-blocked:
		t.Fatalf("fence loss canceled in-flight admission: %v", err)
	default:
	}
	c.mu.Lock()
	require.Equal(t, 1, c.activeShips, "same pending Ship must survive persistence loss")
	c.blocked = false
	c.mu.Unlock()
	select {
	case err := <-blocked:
		require.NoError(t, err)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("producer did not continue")
	}
	require.NoError(t, first.handler(context.Background(), fixtureEvent(102, 2)))
	pending, err := c.GetUnsent()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	require.NoError(t, a.ctx.Err())
	select {
	case <-c.sealed:
		t.Fatal("conflict sealed the SDK")
	default:
	}
	c.ack(3)
	require.NoError(t, a.Close())
	<-workerDone
	require.EqualValues(t, 1, conflicts.Load())
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
