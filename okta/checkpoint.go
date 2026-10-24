package usp_okta

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	// Reserve envelope space within the COMPLETE 8KiB encoded store value.
	checkpointPositionLimit = 8*1024 - 64
	savedIDLimit            = 256
	liveIDLimit             = 65536
)

type savedID struct {
	digest [16]byte
	expiry int64
}
type liveID struct {
	dependency utils.ACKDependency
	page       uint64
	expiry     int64
}
type oktaPosition struct {
	// Boundary records the original complete scan request. It is not a safe
	// replay anchor when restored ID coverage is incomplete. Published time is
	// never a continuation boundary.
	Boundary, Next string
	IDs            []savedID // newest first, only persisted after the whole page is ACKed
	// Coverage is an admission-time suffix of complete saved history, not a
	// guarantee of a fixed number of hours. Zero means no complete ID coverage.
	Coverage int64
}

type oktaGeneration struct {
	id, page      uint64
	session       *utils.CheckpointSession
	position      oktaPosition
	live          map[[16]byte]liveID
	recent        []savedID
	dependency    utils.ACKDependency
	hasDependency bool
	confirmed     atomic.Uint64
	latest        []byte // manager-owned: retains consumed confirmations until CAS
}

func newOktaGeneration(id uint64, session *utils.CheckpointSession, p oktaPosition) *oktaGeneration {
	g := &oktaGeneration{id: id, session: session, position: p, live: make(map[[16]byte]liveID)}
	for _, entry := range p.IDs {
		if entry.expiry > time.Now().Unix() {
			g.live[entry.digest] = liveID{expiry: entry.expiry}
			g.recent = append(g.recent, entry)
		}
	}
	return g
}

func eventDigest(id string) [16]byte {
	sum := sha256.Sum256([]byte("okta/system-log\x00" + id))
	var digest [16]byte
	copy(digest[:], sum[:16])
	return digest
}

func (g *oktaGeneration) remember(id savedID) {
	g.recent = append(g.recent, savedID{})
	copy(g.recent[1:], g.recent[:len(g.recent)-1])
	g.recent[0] = id
	if len(g.recent) > savedIDLimit {
		g.recent = g.recent[:savedIDLimit]
	}
}

func (g *oktaGeneration) makeRoom(now time.Time) bool {
	confirmed := g.confirmed.Load()
	for id, entry := range g.live {
		if entry.expiry <= now.Unix() && entry.page <= confirmed {
			delete(g.live, id)
		}
	}
	return len(g.live) < liveIDLimit
}

// Binary encoding uses 24B per ID. Never truncate an opaque URL. Optional oldest
// IDs alone are reduced to fit, and coverage reports the actual retained suffix.
func encodeOktaPosition(p oktaPosition) ([]byte, error) {
	base := 1 + 2 + len(p.Boundary) + 2 + len(p.Next) + 8 + 2
	if len(p.Boundary) > 65535 || len(p.Next) > 65535 || base > checkpointPositionLimit {
		return nil, utils.ErrCheckpointLimit
	}
	count := (checkpointPositionLimit - base) / 24
	if count > savedIDLimit {
		count = savedIDLimit
	}
	if count > len(p.IDs) {
		count = len(p.IDs)
	}
	ids := p.IDs[:count]
	coverage := int64(0)
	if count > 0 {
		coverage = ids[count-1].expiry - int64(overlapPeriod/time.Second) + 1
	}
	b := make([]byte, 0, base+24*count)
	b = append(b, 1)
	b = binary.BigEndian.AppendUint16(b, uint16(len(p.Boundary)))
	b = append(b, p.Boundary...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(p.Next)))
	b = append(b, p.Next...)
	b = binary.BigEndian.AppendUint64(b, uint64(coverage))
	b = binary.BigEndian.AppendUint16(b, uint16(count))
	for _, entry := range ids {
		b = append(b, entry.digest[:]...)
		b = binary.BigEndian.AppendUint64(b, uint64(entry.expiry))
	}
	return b, nil
}

func (a *OktaAdapter) decodePosition(b []byte) (oktaPosition, error) {
	var p oktaPosition
	if len(b) < 15 || len(b) > checkpointPositionLimit || b[0] != 1 {
		return p, utils.ErrCheckpointInvalid
	}
	b = b[1:]
	readString := func() (string, bool) {
		if len(b) < 2 {
			return "", false
		}
		n := int(binary.BigEndian.Uint16(b))
		b = b[2:]
		if n > len(b) {
			return "", false
		}
		value := string(b[:n])
		b = b[n:]
		return value, true
	}
	var ok bool
	if p.Boundary, ok = readString(); !ok {
		return p, utils.ErrCheckpointInvalid
	}
	if p.Next, ok = readString(); !ok {
		return p, utils.ErrCheckpointInvalid
	}
	if len(b) < 10 {
		return p, utils.ErrCheckpointInvalid
	}
	p.Coverage = int64(binary.BigEndian.Uint64(b))
	count := int(binary.BigEndian.Uint16(b[8:]))
	b = b[10:]
	if count > savedIDLimit || len(b) != count*24 || a.validateContinuation(p.Boundary) != nil || a.validateContinuation(p.Next) != nil {
		return p, utils.ErrCheckpointInvalid
	}
	// A persisted restart boundary is always an ascending polling request.
	boundary, _ := a.endpoint.Parse(p.Boundary)
	if boundary.Query().Get("until") != "" || boundary.Query().Get("sortOrder") != "ASCENDING" {
		return p, utils.ErrCheckpointInvalid
	}
	if _, err := time.Parse(time.RFC3339, boundary.Query().Get("since")); err != nil {
		return p, utils.ErrCheckpointInvalid
	}
	for range count {
		var entry savedID
		copy(entry.digest[:], b[:16])
		entry.expiry = int64(binary.BigEndian.Uint64(b[16:]))
		p.IDs = append(p.IDs, entry)
		b = b[24:]
	}
	return p, nil
}

func candidateBytes(generation, page uint64, raw []byte) []byte {
	b := make([]byte, 0, 16+len(raw))
	b = binary.BigEndian.AppendUint64(b, generation)
	b = binary.BigEndian.AppendUint64(b, page)
	return append(b, raw...)
}

func (a *OktaAdapter) saveConfirmed(ctx context.Context, g *oktaGeneration) error {
	confirmed, err := a.tracker.Confirm()
	if err != nil {
		a.issue("ack_observation_failed")
		return err
	}
	if raw, ok := confirmed["logs"]; ok && len(raw) >= 16 && binary.BigEndian.Uint64(raw) == g.id {
		page := binary.BigEndian.Uint64(raw[8:])
		if page >= g.confirmed.Load() {
			g.confirmed.Store(page)
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

func saveInterval() time.Duration {
	var b [1]byte
	_, _ = rand.Read(b[:])
	return 30*time.Second + time.Duration(b[0])*3*time.Second/256
}
