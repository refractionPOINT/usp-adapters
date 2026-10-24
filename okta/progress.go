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
	dependency acktrack.ACKDependency
	page       uint64
	expiry     int64
}
type oktaPosition struct{ Next string }
type oktaGeneration struct {
	id, page      uint64
	position      oktaPosition
	live          map[[16]byte]liveID
	dependency    acktrack.ACKDependency
	hasDependency bool
	confirmed     atomic.Uint64
}

func newOktaGeneration(id uint64, p oktaPosition) *oktaGeneration {
	return &oktaGeneration{id: id, position: p, live: make(map[[16]byte]liveID)}
}
func eventDigest(id string) [16]byte {
	sum := sha256.Sum256([]byte("okta/system-log\x00" + id))
	var d [16]byte
	copy(d[:], sum[:16])
	return d
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

// Local page marker, never a cursor or restart position.
func candidateBytes(generation, page uint64) []byte {
	b := binary.BigEndian.AppendUint64(nil, generation)
	return binary.BigEndian.AppendUint64(b, page)
}
func (a *OktaAdapter) confirm(g *oktaGeneration) error {
	positions, err := a.tracker.Confirm()
	if err != nil {
		a.issue("ack_observation_failed")
		return err
	}
	if raw := positions["logs"]; len(raw) == 16 && binary.BigEndian.Uint64(raw) == g.id {
		g.confirmed.Store(binary.BigEndian.Uint64(raw[8:]))
	}
	return nil
}
