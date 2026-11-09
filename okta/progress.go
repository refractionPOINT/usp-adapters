package usp_okta

import (
	"crypto/sha256"
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
)

const liveIDLimit = 65536

type liveID struct {
	dependency      acktrack.ACKDependency
	page            uint64
	expiry          int64
	sharedAttempted bool
	sharedDigest    [32]byte
}
type oktaPosition struct {
	Next     string
	Since    time.Time
	Observed time.Time
}
type oktaGeneration struct {
	id, page      uint64
	position      oktaPosition
	live          map[[16]byte]liveID
	dependency    acktrack.ACKDependency
	hasDependency bool
	confirmed     atomic.Uint64
	dedupFrontier uint64
}

func newOktaGeneration(id uint64, p oktaPosition) *oktaGeneration {
	return &oktaGeneration{id: id, position: p, live: make(map[[16]byte]liveID)}
}

// sharedEventDigest includes the feed and retains all SHA-256 bits. Only the
// in-process bounded cache uses a truncated digest.
func sharedEventDigest(id string) [32]byte {
	return sha256.Sum256([]byte("okta/system-log\x00" + id))
}
func eventDigest(id string) [16]byte {
	sum := sharedEventDigest(id)
	var d [16]byte
	copy(d[:], sum[:16])
	return d
}
func (g *oktaGeneration) makeRoom(now time.Time) bool {
	confirmed := g.confirmed.Load()
	pressure := len(g.live) >= liveIDLimit
	for id, entry := range g.live {
		if entry.page <= confirmed && (pressure || entry.expiry <= now.Unix()) {
			delete(g.live, id)
		}
	}
	return len(g.live) < liveIDLimit
}

// Local page marker, never a cursor or restart position.
func candidateBytes(generation, page uint64) []byte {
	b := binary.BigEndian.AppendUint64(nil, generation)
	return binary.BigEndian.AppendUint64(b, page)
}
func (a *OktaAdapter) confirm(g *oktaGeneration) error {
	a.confirmMu.Lock()
	defer a.confirmMu.Unlock()
	positions, err := a.tracker.Confirm()
	if err != nil {
		a.issue("ack_observation_failed")
		return err
	}
	if raw := positions["logs"]; len(raw) >= 16 && binary.BigEndian.Uint64(raw) == g.id {
		g.confirmed.Store(binary.BigEndian.Uint64(raw[8:16]))
		if a.conf.ResumeState != nil && len(raw) > 16 {
			a.conf.ResumeState.Confirm(append([]byte(nil), raw[16:]...))
		}
	}
	return nil
}
