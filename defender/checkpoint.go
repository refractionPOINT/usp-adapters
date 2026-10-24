package usp_defender

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	checkpointPositionLimit = 8*1024 - 64 // complete encoded value reserves envelope metadata
	savedIDLimit            = 256
	liveIDLimit             = 65536
	historyHorizon          = 24 * time.Hour
)

type savedID struct {
	digest [16]byte
	expiry int64
}
type liveID struct {
	dependency utils.ACKDependency
	serial     uint64
	expiry     int64
}

// Bounds are exact UTC nanoseconds. No access token or aged page URL is stored.
// Boundary is the end of complete live collection; WindowEnd retains its fixed
// pending upper bound. Replay..ReplayEnd retains a reconciliation interval.
// Coverage is the complete restored ID suffix; Unverified retains older gaps
// excluded by the saved-summary cap. Zero Replay means no active history scan.
type defenderPosition struct {
	Boundary, WindowEnd, Coverage, Replay, ReplayEnd, Unverified int64
	IDs                                                          []savedID
}
type defenderGeneration struct {
	id, serial           uint64
	session              *utils.CheckpointSession
	position             defenderPosition
	live                 map[[16]byte]liveID
	recent               []savedID
	dependency           utils.ACKDependency
	hasDependency        bool
	confirmed            atomic.Uint64
	liveScan, replayScan *graphScan
	latest               []byte // manager-owned; Confirm consumes even if persistence fails
}

func newGeneration(id uint64, session *utils.CheckpointSession, p defenderPosition) *defenderGeneration {
	g := &defenderGeneration{id: id, session: session, position: p, live: make(map[[16]byte]liveID)}
	for _, entry := range p.IDs {
		g.live[entry.digest] = liveID{expiry: entry.expiry}
		g.recent = append(g.recent, entry)
	}
	return g
}

func (a *DefenderAdapter) sourceFingerprint() [32]byte {
	// Structured encoding prevents ambiguous tenant/endpoint delimiters. Client
	// credentials and the token endpoint do not identify the alert stream.
	fields := []string{"defender", a.conf.endpoint(), strings.ToLower(a.collectionURL.Scheme + "://" + a.collectionURL.Host), a.collectionURL.EscapedPath(), a.conf.TenantID, "alerts_v2/createdDateTime"}
	raw, _ := json.Marshal(fields)
	return sha256.Sum256(raw)
}
func eventDigest(source [32]byte, id string) [16]byte {
	h := sha256.New()
	h.Write(source[:])
	h.Write([]byte(id))
	var d [16]byte
	copy(d[:], h.Sum(nil))
	return d
}
func (g *defenderGeneration) remember(entry savedID) {
	g.recent = append(g.recent, entry)
	sort.Slice(g.recent, func(i, j int) bool { return g.recent[i].expiry > g.recent[j].expiry })
	// Retain one omitted entry so encoding can set an honest complete suffix.
	if len(g.recent) > savedIDLimit+1 {
		g.recent = g.recent[:savedIDLimit+1]
	}
}

func encodePosition(p defenderPosition) ([]byte, error) {
	const base = 51
	if p.Boundary <= 0 || p.WindowEnd <= p.Boundary || p.Coverage <= 0 || p.Coverage > p.Boundary || p.Unverified < 0 || p.Unverified > p.Coverage || (p.Replay == 0) != (p.ReplayEnd == 0) || (p.Replay != 0 && (p.Replay > p.ReplayEnd || p.ReplayEnd > p.Boundary)) {
		return nil, utils.ErrCheckpointInvalid
	}
	ids := append([]savedID(nil), p.IDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i].expiry > ids[j].expiry })
	count := len(ids)
	if count > savedIDLimit {
		count = savedIDLimit
	}
	if max := (checkpointPositionLimit - base) / 24; count > max {
		count = max
	}
	coverage := p.Coverage
	if count < len(ids) {
		// Exclude the entire timestamp tie containing any omitted entry.
		suffix := ids[count].expiry - int64(historyHorizon) + 1
		if suffix > coverage {
			coverage = suffix
		}
		if coverage > p.Boundary {
			coverage = p.Boundary
		}
	}
	if coverage > p.Coverage && (p.Unverified == 0 || p.Coverage < p.Unverified) {
		p.Unverified = p.Coverage
	}
	b := make([]byte, 0, base+24*count)
	b = append(b, 1)
	for _, v := range []int64{p.Boundary, p.WindowEnd, coverage, p.Replay, p.ReplayEnd, p.Unverified} {
		b = binary.BigEndian.AppendUint64(b, uint64(v))
	}
	b = binary.BigEndian.AppendUint16(b, uint16(count))
	for _, entry := range ids[:count] {
		b = append(b, entry.digest[:]...)
		b = binary.BigEndian.AppendUint64(b, uint64(entry.expiry))
	}
	return b, nil
}
func decodePosition(b []byte) (defenderPosition, error) {
	var p defenderPosition
	if len(b) < 51 || len(b) > checkpointPositionLimit || b[0] != 1 {
		return p, utils.ErrCheckpointInvalid
	}
	p.Boundary = int64(binary.BigEndian.Uint64(b[1:]))
	p.WindowEnd = int64(binary.BigEndian.Uint64(b[9:]))
	p.Coverage = int64(binary.BigEndian.Uint64(b[17:]))
	p.Replay = int64(binary.BigEndian.Uint64(b[25:]))
	p.ReplayEnd = int64(binary.BigEndian.Uint64(b[33:]))
	p.Unverified = int64(binary.BigEndian.Uint64(b[41:]))
	count := int(binary.BigEndian.Uint16(b[49:]))
	if count > savedIDLimit || len(b) != 51+24*count {
		return p, utils.ErrCheckpointInvalid
	}
	if _, err := encodePosition(p); err != nil {
		return p, err
	}
	b = b[51:]
	for range count {
		var entry savedID
		copy(entry.digest[:], b[:16])
		entry.expiry = int64(binary.BigEndian.Uint64(b[16:]))
		if entry.expiry <= 0 {
			return p, utils.ErrCheckpointInvalid
		}
		p.IDs = append(p.IDs, entry)
		b = b[24:]
	}
	return p, nil
}
func candidateBytes(generation, serial uint64, raw []byte) []byte {
	b := binary.BigEndian.AppendUint64(nil, generation)
	b = binary.BigEndian.AppendUint64(b, serial)
	return append(b, raw...)
}
func (a *DefenderAdapter) initialPosition() ([]byte, error) {
	now := a.startedAt
	if now == 0 {
		now = a.runtime.now().UTC().UnixNano()
	}
	now -= now % int64(graphTimeQuantum)
	return encodePosition(defenderPosition{Boundary: now, WindowEnd: now + int64(a.runtime.window), Coverage: now})
}
func (a *DefenderAdapter) saveConfirmed(ctx context.Context, g *defenderGeneration) error {
	confirmed, err := a.tracker.Confirm()
	if err != nil {
		a.issue("ack_observation_failed")
		return err
	}
	if raw := confirmed["alerts"]; len(raw) >= 16 && binary.BigEndian.Uint64(raw) == g.id {
		serial := binary.BigEndian.Uint64(raw[8:])
		if serial >= g.confirmed.Load() {
			g.confirmed.Store(serial)
			g.latest = append(g.latest[:0], raw[16:]...)
		}
	}
	if g.session == nil || g.latest == nil {
		return nil
	}
	if err := g.session.Save(ctx, g.latest); err != nil {
		a.issue("save_retry")
		return err
	}
	// A successful Save may only resolve an older frozen attempt. Keep latest
	// until Current matches it, offering it again at the next bounded save tick.
	return nil
}
func (a *DefenderAdapter) run() {
	var generation uint64
	for a.ctx.Err() == nil {
		generation++
		var session *utils.CheckpointSession
		var raw []byte
		var err error
		if a.conf.CheckpointStore != nil {
			session, raw, err = utils.OpenCheckpoint(a.ctx, a.conf.CheckpointStore, 1, a.sourceFingerprint(), a.initialPosition, func(b []byte) error { _, e := decodePosition(b); return e }, a.issue)
		} else {
			raw, err = a.initialPosition()
		}
		if err != nil {
			return
		}
		p, err := decodePosition(raw)
		if err != nil {
			a.issue("invalid_position")
			return
		}
		g := newGeneration(generation, session, p)
		a.generation = g
		ctx, cancel := context.WithCancel(a.ctx)
		done := make(chan struct{})
		go func() { defer close(done); a.collect(ctx, g) }()
		timer := time.NewTimer(a.runtime.saveInterval())
		conflict := false
	manager:
		for {
			select {
			case <-a.ctx.Done():
				break manager
			case <-done:
				break manager
			case <-timer.C:
				saveCtx, stop := context.WithTimeout(ctx, 2*time.Second)
				err := a.saveConfirmed(saveCtx, g)
				stop()
				if errors.Is(err, utils.ErrCheckpointConflict) {
					conflict = true
					break manager
				}
				timer.Reset(a.runtime.saveInterval())
			}
		}
		timer.Stop()
		cancel()
		<-done
		if !conflict {
			return
		}
		a.issue("ownership_changed")
		a.generation = nil
		// Preserve the SDK AND tracker sequence history. Old generation candidates
		// may be consumed, but can never be interpreted as this generation's state.
	}
}

// Shutdown may resolve a frozen older write and then offer the newest confirmed
// state, all within the caller's single two-second deadline.
func (a *DefenderAdapter) finalSave(ctx context.Context, g *defenderGeneration) {
	if a.saveConfirmed(ctx, g) != nil {
		return
	}
	if g.session != nil && g.latest != nil && !bytes.Equal(g.latest, g.session.Current().Position) && ctx.Err() == nil {
		if err := g.session.Save(ctx, g.latest); err != nil {
			a.issue("save_retry")
		}
	}
}
