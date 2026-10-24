package usp_azure_event_hub

import (
	"context"
	"strconv"
	"time"

	"github.com/Azure/azure-amqp-common-go/v4/conn"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
)

// The timestamp describes this uncached request's observation, never the last
// event's activity time. Cached evidence must retain its original observation
// time. The production implementation always performs a new SDK management RPC.
type partitionRuntimeReader interface {
	PartitionRuntime(context.Context, string) (*eventhub.HubPartitionRuntimeInformation, time.Time, error)
}

func (h *azureHub) PartitionRuntime(ctx context.Context, id string) (*eventhub.HubPartitionRuntimeInformation, time.Time, error) {
	info, err := h.hub.GetPartitionInformation(ctx, id)
	return info, time.Now(), err
}

type partitionReset struct {
	generation uint64
	source     [32]byte
	previous   *partitionState
}

func (a *EventHubAdapter) resetCurrent(g *generation, id string, r *partitionReset) bool {
	return g.ctx.Err() == nil && a.ctx.Err() == nil && !a.closing.Load() && !g.retiring.Load() &&
		g.id == r.generation && a.source == r.source && g.parts[id] == r.previous
}

// Only a positive, nonempty last position constitutes reset evidence. The SDK
// loses numeric field presence during mapstructure decoding; a default-zero
// last sequence or offset cannot certify even the first event or an empty hub.
func runtimeProvesReset(info *eventhub.HubPartitionRuntimeInformation, entity, id string, saved int64) bool {
	if info == nil || info.HubPath != entity || info.PartitionID != id || info.LastSequenceNumber <= 0 ||
		info.BeginningSequenceNumber < 0 || info.BeginningSequenceNumber > info.LastSequenceNumber ||
		info.LastEnqueuedTimeUtc.IsZero() || info.LastEnqueuedTimeUtc.UnixNano() <= 0 {
		return false
	}
	p := partitionPosition{Offset: info.LastEnqueuedOffset, Sequence: info.LastSequenceNumber, Enqueued: info.LastEnqueuedTimeUtc.UnixNano()}
	offset, err := strconv.ParseInt(info.LastEnqueuedOffset, 10, 64)
	return err == nil && offset > 0 && validPosition(p) && saved > info.LastSequenceNumber
}

func (a *EventHubAdapter) inspectPartitionReset(g *generation, id string) {
	if pending := g.resets[id]; pending != nil {
		a.finishPartitionReset(g, id, pending)
		return
	}
	reader, ok := a.hub.(partitionRuntimeReader)
	saved, hasSaved := g.confirmed[id]
	if !ok || !hasSaved || g.ctx.Err() != nil || a.closing.Load() {
		return
	}
	source, err := sourceFingerprint(a.conf.ConnectionString)
	parsed, parseErr := conn.ParsedConnectionFromStr(a.conf.ConnectionString)
	if err != nil || parseErr != nil || source != a.source {
		return
	}
	proof := &partitionReset{generation: g.id, source: a.source, previous: g.parts[id]}
	started := time.Now()
	ctx, cancel := context.WithTimeout(g.ctx, 2*time.Second)
	info, observed, err := reader.PartitionRuntime(ctx, id)
	completed := time.Now()
	validContext := ctx.Err() == nil
	cancel()
	if !a.resetCurrent(g, id, proof) {
		return
	}
	if err != nil || !validContext {
		a.issue("partition_runtime_wait")
		return
	}
	if observed.Before(started) || observed.After(completed) {
		a.issue("partition_runtime_stale")
		return
	}
	if g.confirmed[id] != saved || !runtimeProvesReset(info, parsed.HubName, id, saved) {
		return
	}
	if g.nextTag < g.id {
		g.nextTag = g.id
	}
	if g.nextTag == ^uint64(0) {
		a.issue("partition_reset_limit")
		return
	}
	if g.tags == nil {
		g.tags = make(map[string]uint64)
		g.resets = make(map[string]*partitionReset)
	}
	g.nextTag++
	g.tags[id] = g.nextTag // old accepted candidates remain pending, but cannot restore this offset
	g.resets[id] = proof
	delete(g.confirmed, id) // verified live reset, not an ACK
	a.issue("partition_history_gap")
	a.finishPartitionReset(g, id, proof)
}

// Retire only the verified partition. Accepted SDK history and every other
// partition remain on the same client/tracker. The new receiver uses latest.
func (a *EventHubAdapter) finishPartitionReset(g *generation, id string, r *partitionReset) {
	if !a.resetCurrent(g, id, r) {
		return
	}
	if old := r.previous; old != nil {
		// Cancel before taking the handler lock, so even an in-flight buffer-full
		// producer joins within its bounded Ship attempt. Never drain/seal the SDK.
		if old.cancel != nil {
			old.cancel()
		}
		old.mu.Lock()
		old.retired = true
		old.mu.Unlock()
		if old.listener != nil {
			closeCtx, closeCancel := context.WithTimeout(g.ctx, 2*time.Second)
			_ = old.listener.Close(closeCtx)
			closeCancel()
		}
	}
	if !a.resetCurrent(g, id, r) {
		return
	}
	delete(g.parts, id)
	delete(g.retryPositions, id)
	delete(g.receiverFailures, id)
	delete(g.resets, id)
	a.issue("partition_reset")
}

// Periodic discovery checks at
// most one, prioritizing an unresolved reset or paused backward observation;
// otherwise it rotates through known partitions. No unbounded query fan-out.
func (a *EventHubAdapter) inspectOnePartition(g *generation, ids []string) {
	for _, id := range ids {
		if g.resets[id] != nil {
			a.inspectPartitionReset(g, id)
			return
		}
	}
	if id := g.runtimePreferred; id != "" {
		g.runtimePreferred = ""
		for _, current := range ids {
			if id == current {
				a.inspectPartitionReset(g, id)
				return
			}
		}
	}

	for checked := 0; checked < len(ids); checked++ {
		id := ids[g.runtimeCursor%len(ids)]
		g.runtimeCursor = (g.runtimeCursor + 1) % len(ids)
		if _, ok := g.confirmed[id]; ok {
			a.inspectPartitionReset(g, id)
			return
		}
	}
}
