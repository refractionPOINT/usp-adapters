package usp_entraid

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
	checkpointPositionLimit = 32*1024 - 64
	savedIDLimit            = 1024
	liveIDLimit             = 262144
	historyHorizon          = 72 * time.Hour
)

type savedID struct {
	digest [16]byte
	expiry int64
}
type liveID struct {
	dependency utils.ACKDependency
	serial     uint64
	expiry     int64
	stream     byte
}

// All times are exact UTC nanoseconds. Only complete half-open windows advance
// Boundary. Replay bounds survive restart; Graph continuation tokens do not.
// Coverage is a complete ID suffix, not a promise of vendor retained history.
type streamPosition struct {
	Boundary, WindowEnd, Coverage, Replay, ReplayEnd, Unverified int64
	Deep                                                         bool
	IDs                                                          []savedID
}
type entraPosition map[string]streamPosition

type entraGeneration struct {
	id, serial    uint64
	session       *utils.CheckpointSession
	position      entraPosition
	live          map[[16]byte]liveID
	recent        map[string][]savedID
	dependency    utils.ACKDependency
	hasDependency bool
	confirmed     atomic.Uint64
	scans         map[string]*graphScan
	latest        []byte // manager-owned: consumed Confirm results survive failed saves
}

func newGeneration(id uint64, session *utils.CheckpointSession, p entraPosition) *entraGeneration {
	g := &entraGeneration{id: id, session: session, position: p, live: make(map[[16]byte]liveID), recent: make(map[string][]savedID), scans: make(map[string]*graphScan)}
	for name, s := range p {
		for _, entry := range s.IDs {
			g.live[entry.digest] = liveID{expiry: entry.expiry, stream: streamCode(name)}
			g.recent[name] = append(g.recent[name], entry)
		}
	}
	return g
}

func (a *EntraIDAdapter) sourceFingerprint() [32]byte {
	names := make([]string, 0, len(a.streams))
	for _, s := range a.streams {
		names = append(names, s.name)
	}
	sort.Strings(names)
	// Credentials, including application ID, are deliberately excluded. The host
	// binds this source identity to the authoritative record incarnation.
	fields := []string{"entraid", a.conf.endpoint(), strings.TrimRight(a.conf.graphEndpoint(), "/"), a.conf.TenantID, a.conf.scope()}
	fields = append(fields, names...)
	raw, _ := json.Marshal(fields)
	return sha256.Sum256(raw)
}
func eventDigest(source [32]byte, stream, id string) [16]byte {
	h := sha256.New()
	h.Write(source[:])
	h.Write([]byte{streamCode(stream)})
	h.Write([]byte(id))
	var d [16]byte
	copy(d[:], h.Sum(nil))
	return d
}
func streamCode(name string) byte {
	switch name {
	case "risk_detections":
		return 1
	case "sign_ins":
		return 2
	case "audit_logs":
		return 3
	}
	return 0
}
func streamName(code byte) string {
	switch code {
	case 1:
		return "risk_detections"
	case 2:
		return "sign_ins"
	case 3:
		return "audit_logs"
	}
	return ""
}

func encodePosition(p entraPosition) ([]byte, error) {
	if len(p) < 1 || len(p) > 3 {
		return nil, utils.ErrCheckpointInvalid
	}
	names := make([]string, 0, len(p))
	for n := range p {
		if streamCode(n) == 0 {
			return nil, utils.ErrCheckpointInvalid
		}
		names = append(names, n)
	}
	sort.Strings(names)
	b := []byte{1, byte(len(names))}
	// Reserve metadata for every stream before dividing the optional ID budget.
	quota := savedIDLimit / len(names)
	byBytes := (checkpointPositionLimit - 2 - 52*len(names)) / (24 * len(names))
	if quota > byBytes {
		quota = byBytes
	}
	for _, name := range names {
		s := p[name]
		if s.Boundary <= 0 || s.WindowEnd <= s.Boundary || s.Coverage <= 0 || s.Coverage > s.Boundary || s.Unverified < 0 || s.Unverified > s.Coverage || (s.Replay == 0) != (s.ReplayEnd == 0) || (s.Replay != 0 && (s.Replay > s.ReplayEnd || s.ReplayEnd > s.Boundary)) {
			return nil, utils.ErrCheckpointInvalid
		}
		for _, bound := range []int64{s.Boundary, s.WindowEnd, s.Coverage, s.Replay, s.ReplayEnd, s.Unverified} {
			if bound%int64(graphTimeQuantum) != 0 {
				return nil, utils.ErrCheckpointInvalid
			}
		}
		ids := append([]savedID(nil), s.IDs...)
		sort.Slice(ids, func(i, j int) bool { return ids[i].expiry > ids[j].expiry })
		count := len(ids)
		if count > quota {
			count = quota
		}
		if count < len(ids) {
			suffix := ids[count].expiry - int64(historyHorizon)
			// Exclude the complete omitted timestamp tie and round UP to Graph's
			// 100ns precision. An unrepresentable upper bound could otherwise
			// exclude a boundary record in both adjacent half-open requests.
			suffix += int64(graphTimeQuantum) - suffix%int64(graphTimeQuantum)
			if suffix > s.Coverage {
				if s.Unverified == 0 || s.Coverage < s.Unverified {
					s.Unverified = s.Coverage
				}
				s.Coverage = suffix
			}
			if s.Coverage > s.Boundary {
				s.Coverage = s.Boundary
			}
		}
		b = append(b, streamCode(name))
		for _, v := range []int64{s.Boundary, s.WindowEnd, s.Coverage, s.Replay, s.ReplayEnd, s.Unverified} {
			b = binary.BigEndian.AppendUint64(b, uint64(v))
		}
		if s.Deep {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
		b = binary.BigEndian.AppendUint16(b, uint16(count))
		for _, id := range ids[:count] {
			if id.expiry <= 0 {
				return nil, utils.ErrCheckpointInvalid
			}
			b = append(b, id.digest[:]...)
			b = binary.BigEndian.AppendUint64(b, uint64(id.expiry))
		}
	}
	if len(b) > checkpointPositionLimit {
		return nil, utils.ErrCheckpointLimit
	}
	return b, nil
}
func decodePosition(b []byte) (entraPosition, error) {
	invalid := utils.ErrCheckpointInvalid
	if len(b) < 2 || len(b) > checkpointPositionLimit || b[0] != 1 || b[1] < 1 || b[1] > 3 {
		return nil, invalid
	}
	count := int(b[1])
	b = b[2:]
	p := make(entraPosition)
	total := 0
	for range count {
		if len(b) < 52 {
			return nil, invalid
		}
		name := streamName(b[0])
		if name == "" {
			return nil, invalid
		}
		if _, ok := p[name]; ok {
			return nil, invalid
		}
		s := streamPosition{}
		fields := []*int64{&s.Boundary, &s.WindowEnd, &s.Coverage, &s.Replay, &s.ReplayEnd, &s.Unverified}
		for i, v := range fields {
			*v = int64(binary.BigEndian.Uint64(b[1+8*i:]))
		}
		if b[49] > 1 {
			return nil, invalid
		}
		s.Deep = b[49] == 1
		n := int(binary.BigEndian.Uint16(b[50:]))
		total += n
		b = b[52:]
		if total > savedIDLimit || len(b) < 24*n {
			return nil, invalid
		}
		for range n {
			var id savedID
			copy(id.digest[:], b[:16])
			id.expiry = int64(binary.BigEndian.Uint64(b[16:]))
			if id.expiry <= 0 {
				return nil, invalid
			}
			s.IDs = append(s.IDs, id)
			b = b[24:]
		}
		p[name] = s
	}
	if len(b) != 0 {
		return nil, invalid
	}
	if _, err := encodePosition(p); err != nil {
		return nil, err
	}
	return p, nil
}
func (a *EntraIDAdapter) validatePosition(b []byte) error {
	p, err := decodePosition(b)
	if err != nil {
		return err
	}
	if len(p) != len(a.streams) {
		return utils.ErrCheckpointInvalid
	}
	for _, s := range a.streams {
		if _, ok := p[s.name]; !ok {
			return utils.ErrCheckpointInvalid
		}
	}
	return nil
}
func candidateBytes(generation, serial uint64, raw []byte) []byte {
	b := binary.BigEndian.AppendUint64(nil, generation)
	b = binary.BigEndian.AppendUint64(b, serial)
	return append(b, raw...)
}
func (a *EntraIDAdapter) initialPosition() ([]byte, error) {
	start := a.startedAt
	if start == 0 {
		start = a.runtime.now().UTC().UnixNano()
	}
	start -= start % int64(graphTimeQuantum)
	p := make(entraPosition)
	for _, s := range a.streams {
		p[s.name] = streamPosition{Boundary: start, WindowEnd: start + int64(a.runtime.window), Coverage: start}
	}
	return encodePosition(p)
}
func (g *entraGeneration) remember(name string, entry savedID) {
	ids := g.recent[name]
	for _, old := range ids {
		if old.digest == entry.digest {
			return
		}
	}
	ids = append(ids, entry)
	sort.Slice(ids, func(i, j int) bool { return ids[i].expiry > ids[j].expiry })
	// One omitted ID is necessary to compute the first incomplete timestamp tie.
	quota := savedIDLimit / len(g.position)
	if len(ids) > quota+1 {
		ids = ids[:quota+1]
	}
	g.recent[name] = ids
}
func (a *EntraIDAdapter) candidate(g *entraGeneration) error {
	g.serial++
	for name, s := range g.position {
		s.IDs = g.recent[name]
		g.position[name] = s
	}
	raw, err := encodePosition(g.position)
	if err != nil {
		a.issue("position_size_or_state")
		return err
	}
	saved, _ := decodePosition(raw)
	for name, state := range saved {
		if state.Coverage > g.position[name].Coverage {
			a.issue("coverage_limited")
			break
		}
	}
	if g.hasDependency {
		return a.tracker.Candidate("entra", candidateBytes(g.id, g.serial, raw), g.dependency)
	}
	return a.tracker.Candidate("entra", candidateBytes(g.id, g.serial, raw))
}
func (a *EntraIDAdapter) saveConfirmed(ctx context.Context, g *entraGeneration) error {
	positions, err := a.tracker.Confirm()
	if err != nil {
		a.issue("ack_observation_failed")
		return err
	}
	if raw := positions["entra"]; len(raw) >= 16 && binary.BigEndian.Uint64(raw) == g.id {
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
	return nil
}
func (a *EntraIDAdapter) finalSave(ctx context.Context, g *entraGeneration) {
	if a.saveConfirmed(ctx, g) != nil {
		return
	}
	if g.session != nil && g.latest != nil && !bytes.Equal(g.latest, g.session.Current().Position) && ctx.Err() == nil {
		if g.session.Save(ctx, g.latest) != nil {
			a.issue("save_retry")
		}
	}
}
func (a *EntraIDAdapter) run() {
	var generation uint64
	for a.ctx.Err() == nil {
		generation++
		var session *utils.CheckpointSession
		var raw []byte
		var err error
		if a.conf.CheckpointStore != nil {
			session, raw, err = utils.OpenCheckpoint(a.ctx, a.conf.CheckpointStore, 1, a.sourceFingerprint(), a.initialPosition, a.validatePosition, a.issue)
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
		// The same SDK and tracker retain their sequence history. Old generation
		// probes are never reinterpreted or rebased onto a successor's fence.
	}
}
