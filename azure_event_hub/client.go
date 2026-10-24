package usp_azure_event_hub

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

type adapterClient interface {
	utils.CheckpointClient
	BeginDrain()
	Drain(time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type EventHubAdapter struct {
	conf      EventHubConfig
	uspClient adapterClient
	hub       adapterHub
	tracker   *utils.ACKTracker
	ctx       context.Context
	cancel    context.CancelFunc
	chStopped chan struct{}
	closeOnce sync.Once
	closeErr  error
	failureMu sync.Mutex
	failure   error
	closing   atomic.Bool
	stalled   atomic.Bool
	admission chan struct{}
	final     *generation // worker-owned until chStopped closes
	source    [32]byte
	// Per-instance scheduling seams; no global mutable hooks or customer settings.
	saveEvery, discoverEvery time.Duration
	now                      func() time.Time
}
type EventHubConfig struct {
	ClientOptions    uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ConnectionString string                  `json:"connection_string" yaml:"connection_string"`
	// CheckpointStore is an optional caller-owned restart store, never configuration.
	CheckpointStore utils.CheckpointStore `json:"-" yaml:"-"`
	// CheckpointIssue receives fixed operational codes; it must be concurrency-safe and nonblocking.
	CheckpointIssue func(string) `json:"-" yaml:"-"`
}

func (c *EventHubConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ConnectionString == "" {
		return errors.New("missing connection_string")
	}
	return nil
}

func NewEventHubAdapter(ctx context.Context, conf EventHubConfig) (*EventHubAdapter, chan struct{}, error) {
	source, err := sourceFingerprint(conf.ConnectionString)
	if err != nil {
		return nil, nil, vendorFailure("event hub connection", err)
	}
	// Leave HubWithOffsetPersistence unspecified: the default MemoryPersister
	// must continue recording successful admission for ordinary SDK reconnects.
	hub, err := eventhub.NewHubFromConnectionString(conf.ConnectionString)
	if err != nil {
		return nil, nil, vendorFailure("event hub connection", err)
	}
	client, err := uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = hub.Close(closeCtx)
		return nil, nil, err
	}
	a, err := newAdapter(ctx, conf, newAzureHub(conf.ConnectionString, hub), client, source)
	if err != nil {
		_, _ = client.Close()
		return nil, nil, err
	}
	go a.run()
	return a, a.chStopped, nil
}
func newAdapter(ctx context.Context, conf EventHubConfig, hub adapterHub, client adapterClient, source [32]byte) (*EventHubAdapter, error) {
	tracker, err := utils.NewACKTracker(client, maxPartitions, 1100)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	return &EventHubAdapter{conf: conf, hub: hub, uspClient: client, tracker: tracker, ctx: ctx, cancel: cancel,
		now: time.Now, source: source, chStopped: make(chan struct{}), admission: make(chan struct{}, 1), saveEvery: checkpointSaveInterval(), discoverEvery: time.Minute}, nil
}

type partitionState struct {
	mu       sync.Mutex // serializes same-partition handlers, including synchronous freeze
	retired  bool
	suspect  bool
	tag      uint64
	last     *partitionPosition
	listener hubListener
	ctx      context.Context
	cancel   context.CancelFunc
}
type generation struct {
	tags              map[string]uint64
	resets            map[string]*partitionReset
	nextTag           uint64
	runtimeCursor     int
	persistenceLost   bool
	resetRequested    chan string
	runtimePreferred  string
	id                uint64
	ctx               context.Context
	cancel            context.CancelFunc
	session           *utils.CheckpointSession
	parts             map[string]*partitionState
	confirmed         map[string]partitionPosition
	latest            []byte
	stopped           chan struct{}
	monitors          sync.WaitGroup
	retiring          atomic.Bool
	managementFailure failureEpisode
	receiverFailures  map[string]failureEpisode
	retryPositions    map[string]partitionPosition
}

func (a *EventHubAdapter) run() {
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = a.hub.Close(ctx)
		cancel()
		// Release the worker before invoking caller callbacks. An OnError
		// callback may itself call Close; it must never join its own worker.
		a.failureMu.Lock()
		failure := a.failure
		a.failureMu.Unlock()
		if failure == nil && a.stalled.Load() && !a.closing.Load() {
			a.issue("internal_stall")
		}
		close(a.chStopped)
		if failure != nil && a.conf.ClientOptions.OnError != nil {
			a.conf.ClientOptions.OnError(failure)
		}
	}()
	// One checkpoint acquisition per adapter lifetime. A lost save fence only
	// disables persistence; it cannot authorize a new claim or source traversal.
	session, raw, err := a.openCheckpoint()
	if err != nil {
		return
	}
	positions, err := decodePosition(raw)
	if err != nil {
		a.issue("load_wait")
		return
	} // OpenCheckpoint already validates
	ctx, cancel := context.WithCancel(a.ctx)
	g := &generation{id: 1, ctx: ctx, cancel: cancel, session: session, parts: make(map[string]*partitionState), confirmed: positions, latest: raw, stopped: make(chan struct{}, 1)}
	a.final = g
	_ = a.collect(g)
	a.stopGeneration(g)
}
func (a *EventHubAdapter) openCheckpoint() (*utils.CheckpointSession, []byte, error) {
	initial := func() ([]byte, error) { return encodePosition(nil) }
	if a.conf.CheckpointStore == nil {
		b, err := initial()
		return nil, b, err
	}
	return utils.OpenCheckpoint(a.ctx, boundedStore{a.conf.CheckpointStore}, checkpointSchema, a.source, initial,
		func(b []byte) error { _, err := decodePosition(b); return err }, a.conf.CheckpointIssue)
}
func (a *EventHubAdapter) collect(g *generation) error {
	g.resetRequested = make(chan string, 1)
	if err := a.discover(g, true); err != nil {
		return err
	}
	save := time.NewTicker(a.saveEvery)
	defer save.Stop()
	discover := time.NewTicker(a.discoverEvery)
	defer discover.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return g.ctx.Err()
		case <-g.stopped:
			return errors.New("event hub listener stopped")
		case <-save.C:
			_ = a.saveConfirmed(g.ctx, g)
		case id := <-g.resetRequested:
			g.runtimePreferred = id
			if err := a.discover(g, false); err != nil {
				return err
			}
		case <-discover.C:
			if err := a.discover(g, false); err != nil {
				return err
			}
		}
	}
}

// Separate management and receiver episodes preserve healthy source work while
// periodic discovery retries. Only a continuous hour of the same failing
// operation escalates. Any successful management read resets its own episode.
type failureEpisode struct {
	since  time.Time
	active bool
}

func (e *failureEpisode) failed(now time.Time) bool {
	if !e.active {
		e.since, e.active = now, true
	}
	return now.Sub(e.since) >= time.Hour
}
func (a *EventHubAdapter) discover(g *generation, initial bool) error {
	ctx, cancel := context.WithTimeout(g.ctx, 30*time.Second)
	ids, err := a.hub.Partitions(ctx)
	cancel()
	if err != nil {
		if g.ctx.Err() != nil {
			return g.ctx.Err()
		}
		if !initial && !g.managementFailure.failed(a.now()) {
			a.issue("discovery_retry")
			return nil
		}
		a.vendorError(g.ctx, "GetRuntimeInformation()", err)
		return err
	}
	g.managementFailure = failureEpisode{}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if !validPartition(id) {
			a.stalled.Store(true)
			a.issue("partition_invalid")
			return utils.ErrCheckpointInvalid
		}
		seen[id] = struct{}{}
	}
	known := make(map[string]struct{}, len(g.parts)+len(g.tags)+len(seen))
	for id := range g.parts {
		known[id] = struct{}{}
	}
	for id := range g.tags {
		known[id] = struct{}{}
	}
	for id := range seen {
		known[id] = struct{}{}
	}
	if len(known) > maxPartitions {
		a.stalled.Store(true)
		a.issue("partition_limit")
		return utils.ErrCheckpointLimit
	}

	if g.receiverFailures == nil {
		g.receiverFailures = make(map[string]failureEpisode)
		g.retryPositions = make(map[string]partitionPosition)
	}
	for id := range g.receiverFailures {
		if _, ok := seen[id]; !ok {
			delete(g.receiverFailures, id)
			delete(g.retryPositions, id)
		}
	}
	if !initial {
		a.inspectOnePartition(g, ids)
	}
	for _, id := range ids {
		if g.ctx.Err() != nil {
			return g.ctx.Err()
		}
		if initial {
			a.inspectPartitionReset(g, id)
		}
		if g.ctx.Err() != nil {
			return g.ctx.Err()
		}
		if g.resets[id] != nil {
			continue
		}
		if _, exists := g.parts[id]; exists {
			continue
		}
		sourceCtx, sourceCancel := context.WithCancel(g.ctx)
		p := &partitionState{ctx: sourceCtx, cancel: sourceCancel, tag: g.tags[id]}
		start := receiverStart{resetTag: p.tag}
		if saved, ok := g.confirmed[id]; ok {
			start.Offset = saved.Offset
			copy := saved
			p.last = &copy
		}
		if admitted, ok := g.retryPositions[id]; ok {
			// A partially opened receiver may already have admitted this exact
			// position. Its messages still belong to this same SDK/tracker, so
			// preserve that in-process boundary across the setup retry.
			copy := admitted
			p.last = &copy
			start.Offset = admitted.Offset
		}
		// Each sequential setup gets its own complete budget. This setup context
		// does not own SDK listener lifetime (the pinned SDK uses Background there).
		receiveCtx, receiveCancel := context.WithTimeout(g.ctx, 30*time.Second)
		listener, e := a.hub.Receive(receiveCtx, id, func(handlerCtx context.Context, event *eventhub.Event) error {
			return a.processPartitionEvent(handlerCtx, g, id, p, event)
		}, start)
		receiveCancel()
		if e == nil && listener == nil {
			sourceCancel()
			a.stalled.Store(true)
			a.issue("receiver_handle_invalid")
			return errors.New("event hub receiver returned no listener")
		}
		if e != nil {
			sourceCancel()
			if listener != nil {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = listener.Close(closeCtx)
				closeCancel()
			}
			p.mu.Lock()
			p.retired = true
			if p.last != nil {
				g.retryPositions[id] = *p.last
			}
			p.mu.Unlock()
			if g.ctx.Err() != nil {
				return g.ctx.Err()
			}
			a.issue("receiver_start_failed")
			episode := g.receiverFailures[id]
			fatal := initial || episode.failed(a.now())
			g.receiverFailures[id] = episode
			if fatal {
				a.vendorError(g.ctx, "Receive()", e)
				return e
			}
			a.issue("receiver_start_retry")
			continue // failed partial setup is not published as a started partition
		}
		p.listener = listener
		g.parts[id] = p
		delete(g.receiverFailures, id)
		delete(g.retryPositions, id)
		g.monitors.Add(1)
		go func() {
			defer g.monitors.Done()
			select {
			case <-g.ctx.Done():
				return
			case <-sourceCtx.Done():
				return
			case <-listener.Done():
				if sourceCtx.Err() != nil {
					return
				}
				a.captureListenerFailure(g, listener)
				if g.ctx.Err() == nil {
					a.stalled.Store(true)
					a.issue("listener_stopped")
					select {
					case g.stopped <- struct{}{}:
					default:
					}
				}
			}
		}()
	}
	return nil
}

// Snapshot a verified SDK cause independently of the handler's cancellation.
// Cleanup calls this before intentionally closing any listener, so a parse
// failure cannot disappear merely because its canceled handler ran first.
func (a *EventHubAdapter) captureListenerFailure(g *generation, listener hubListener) {
	if a.closing.Load() || g.retiring.Load() {
		return
	}
	select {
	case <-listener.Done():
	default:
		return
	}
	if err := listener.Err(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, uspclient.ErrorClosed) {
		if checkpointInternalError(err) {
			a.stalled.Store(true)
			return
		}
		a.recordFailure(vendorFailure("ListenerHandle.Err()", err).Error())
	}
}
func (a *EventHubAdapter) stopGeneration(g *generation) {
	for _, p := range g.parts {
		if p.listener != nil {
			a.captureListenerFailure(g, p.listener)
		}
	}
	g.retiring.Store(true) // no cleanup-induced SDK error becomes a vendor failure
	g.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, p := range g.parts {
		if p.cancel != nil {
			p.cancel()
		}
		if p.listener != nil {
			_ = p.listener.Close(ctx)
		}
	}
	for _, p := range g.parts {
		// Receivers may deliver a late callback after Done: acquiring this mutex
		// joins current work, and retirement forbids all future admission/staging.
		p.mu.Lock()
		p.retired = true
		p.mu.Unlock()
	}
	g.monitors.Wait()
}
func (a *EventHubAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.closing.Store(true)
		a.cancel()
		a.uspClient.BeginDrain() // immediate wake; preserves the SDK's accepted buffer
		stop := utils.StopBlockedProducers(a.uspClient)
		defer stop()
		<-a.chStopped
		drainErr := a.uspClient.Drain(time.Minute)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		var saveErr error
		if a.final != nil {
			saveErr = a.saveConfirmed(ctx, a.final)
			// An uncertain older attempt may just have resolved. Flush the
			// retained latest COMPLETE ACK snapshot within the SAME 2s budget.
			g := a.final
			if saveErr == nil && g.session != nil && !bytes.Equal(g.session.Current().Position, g.latest) {
				saveErr = a.savePosition(ctx, g)
			}
		}
		cancel()
		_, closeErr := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
		a.closeErr = errors.Join(drainErr, saveErr, closeErr)
	})
	return a.closeErr
}
func (a *EventHubAdapter) vendorError(ctx context.Context, text string, cause error) {
	if ctx.Err() == nil && !a.closing.Load() {
		if checkpointInternalError(cause) {
			a.stalled.Store(true)
			return
		}
		a.recordFailure(vendorFailure(text, cause).Error())
	}
}
func (a *EventHubAdapter) recordFailure(text string) {
	a.failureMu.Lock()
	if a.failure == nil {
		a.failure = errors.New(text)
	}
	a.failureMu.Unlock()
}
func (a *EventHubAdapter) freeze(p *partitionState, code string) {
	p.retired = true
	switch code {
	case "receiver_canceled", "tracker_limit", "position_invalid", "position_order_invalid":
		a.stalled.Store(true)
	}
	a.cancel()
	a.issue(code)
}
func eventPosition(event *eventhub.Event) (partitionPosition, error) {
	if event == nil || event.SystemProperties == nil {
		return partitionPosition{}, utils.ErrCheckpointInvalid
	}
	props := event.SystemProperties
	// eventFromMsg can return a partially decoded event. Only successful SDK
	// decoding installs Annotations; required source metadata must also exist.
	if props.Annotations == nil || props.Offset == nil || props.SequenceNumber == nil || props.EnqueuedTime == nil {
		return partitionPosition{}, utils.ErrCheckpointInvalid
	}
	offset := strconv.FormatInt(*props.Offset, 10)
	if raw, ok := props.Annotations["x-opt-offset"]; ok {
		offset = fmt.Sprint(raw)
	}
	p := partitionPosition{offset, *props.SequenceNumber, props.EnqueuedTime.UnixNano()}
	if !validPosition(p) || !time.Unix(0, p.Enqueued).Equal(*props.EnqueuedTime) {
		return partitionPosition{}, utils.ErrCheckpointInvalid
	}
	return p, nil
}
func (a *EventHubAdapter) processPartitionEvent(ctx context.Context, g *generation, id string, p *partitionState, event *eventhub.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	sourceCtx := g.ctx
	if p.ctx != nil {
		sourceCtx = p.ctx
	}
	if p.retired || p.suspect || sourceCtx.Err() != nil {
		return context.Canceled
	}
	// The SDK cancels ctx on eventFromMsg failure but still invokes the handler.
	// Freeze before any later callback can Ship, even if metadata partially decoded.
	if ctx.Err() != nil {
		a.freeze(p, "receiver_canceled")
		return ctx.Err()
	}
	position, err := eventPosition(event)
	if err != nil {
		a.recordFailure(vendorFailure("event hub event", errors.New("invalid event metadata")).Error())
		a.freeze(p, "event_invalid")
		return err
	}
	if p.last != nil {
		before, _ := strconv.ParseInt(p.last.Offset, 10, 64)
		after, _ := strconv.ParseInt(position.Offset, 10, 64)
		if position.Sequence != p.last.Sequence || position.Offset != p.last.Offset {
			if position.Sequence <= p.last.Sequence || after <= before {
				// A backward delivery alone proves no reset. Pause only this source
				// under its handler lock and ask the worker for fresh runtime evidence.
				p.suspect = true
				a.issue("position_order_invalid")
				select {
				case g.resetRequested <- id:
				default:
				}
				return utils.ErrCheckpointInvalid
			}
			if position.Sequence-p.last.Sequence != 1 {
				a.issue("sequence_gap") // a discontinuity is not evidence of recovered history
			}
		}
	}
	msg := &protocol.DataMessage{TextPayload: string(event.Data), TimestampMs: uint64(time.Now().UnixMilli())}
	dep, err := a.ship(sourceCtx, msg)
	if err != nil {
		// Source cancellation retires this callback. Checkpoint fence loss never
		// cancels source work or seals its shared SDK.
		if sourceCtx.Err() != nil {
			p.retired = true
			if errors.Is(err, uspclient.ErrorClosed) {
				return nil
			}
			return err
		}
		report := g.ctx.Err() == nil && !errors.Is(err, uspclient.ErrorClosed) && !errors.Is(err, context.Canceled) && !checkpointInternalError(err)
		var safeFailure error
		if report {
			safeFailure = vendorFailure("Ship()", err)
			a.recordFailure(safeFailure.Error())
		}
		if checkpointInternalError(err) {
			a.stalled.Store(true)
		}
		a.freeze(p, "send_stopped")
		if errors.Is(err, uspclient.ErrorClosed) {
			return nil
		} // SDK must not report intentional Close as vendor failure
		if safeFailure != nil {
			return safeFailure
		}
		return err
	}
	raw, err := encodePosition(map[string]partitionPosition{id: position})
	if err != nil {
		a.freeze(p, "position_invalid")
		return err
	}
	tag := p.tag
	if tag == 0 {
		tag = g.id
	}
	candidate := binary.BigEndian.AppendUint64(nil, tag)
	candidate = append(candidate, raw...)
	if err = a.tracker.Candidate(id, candidate, dep); err != nil {
		a.freeze(p, "tracker_limit")
		return err
	}
	copy := position
	p.last = &copy
	return nil
}
func (a *EventHubAdapter) ship(ctx context.Context, msg *protocol.DataMessage) (utils.ACKDependency, error) {
	// Cancellation-aware admission gate also bounds waiters on ACKTracker's own
	// serial Ship mutex. Every SDK attempt is <=1s, including the first/retries.
	select {
	case <-ctx.Done():
		return utils.ACKDependency{}, ctx.Err()
	case a.admission <- struct{}{}:
	}
	defer func() { <-a.admission }()
	start := time.Now()
	warned := false
	for ctx.Err() == nil {
		dep, err := a.tracker.Ship(msg, time.Second)
		if !errors.Is(err, uspclient.ErrorBufferFull) {
			return dep, err
		}
		if !warned && time.Since(start) >= 10*time.Second {
			warned = true
			if a.conf.ClientOptions.DebugLog != nil {
				a.conf.ClientOptions.DebugLog("stream falling behind")
			}
		}
		if time.Since(start) >= time.Hour {
			return utils.ACKDependency{}, uspclient.ErrorBufferFull
		}
		// Mocks/closed byte budgets can fail immediately; never spin on BufferFull.
		if !utils.CheckpointWait(ctx, 10*time.Millisecond) {
			break
		}
	}
	return utils.ACKDependency{}, ctx.Err()
}
