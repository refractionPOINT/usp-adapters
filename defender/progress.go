package usp_defender

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"strings"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
)

const (
	liveIDLimit    = 65536
	historyHorizon = 24 * time.Hour
)

type liveID struct {
	dependency acktrack.ACKDependency
	admission  uint64
	expiry     int64
}

// Bounds are exact UTC nanoseconds. No access token is retained between requests.
// Boundary is the end of complete live collection; WindowEnd retains its fixed
// pending upper bound. Replay..ReplayEnd retains a reconciliation interval.
// Coverage is the locally retained history suffix; Unverified retains gaps
// from optional history retirement. Zero Replay means no active history scan.
type defenderPosition struct {
	Boundary, WindowEnd, Coverage, Replay, ReplayEnd, Unverified int64
}
type defenderGeneration struct {
	id, serial           uint64
	admitted             uint64 // collector-owned successful admissions, independent of source serial
	acked                atomic.Uint64
	forgottenBefore      int64 // collector-owned: scans cannot restart across reclaimed exact history
	forgottenGapAt       int64 // actual discarded ID at or after the advertised suffix
	pruneAfter           time.Time
	pruneSource          int64
	pruneACK             uint64
	pruneACKRetry        bool
	position             defenderPosition
	live                 map[[16]byte]liveID
	dependency           acktrack.ACKDependency
	hasDependency        bool
	confirmed            atomic.Uint64
	liveScan, replayScan *graphScan
	liveProof            *graphScan // aliases the settled lane's bounded proof, including between intervals
	lostBefore           int64      // retired identities without a carried settled proof
	tailScan             *graphScan
}

func newGeneration(id uint64, p defenderPosition) *defenderGeneration {
	return &defenderGeneration{id: id, position: p, live: make(map[[16]byte]liveID)}
}

func (a *DefenderAdapter) sourceFingerprint() [32]byte {
	a.identityOnce.Do(func() {
		// Structured encoding prevents ambiguous tenant/endpoint delimiters. Client
		// credentials and the token endpoint do not identify the alert stream.
		fields := []string{"defender", a.conf.endpoint(), strings.ToLower(a.collectionURL.Scheme + "://" + a.collectionURL.Host), a.collectionURL.EscapedPath(), a.conf.TenantID, "alerts_v2/createdDateTime"}
		raw, _ := json.Marshal(fields)
		a.identity = sha256.Sum256(raw)
	})
	return a.identity
}
func eventDigest(source [32]byte, id string) [16]byte {
	h := sha256.New()
	h.Write(source[:])
	h.Write([]byte(id))
	var d [16]byte
	copy(d[:], h.Sum(nil))
	return d
}

func (a *DefenderAdapter) initialPosition() defenderPosition {
	now := a.startedAt
	if now == 0 {
		now = a.runtime.now().UTC().UnixNano()
	}
	now -= now % int64(graphTimeQuantum)
	return defenderPosition{Boundary: now, WindowEnd: now + int64(a.runtime.window), Coverage: now}
}

// Local generation/ordinal marker; it contains no serialized source state.
func candidateBytes(generation, serial uint64) []byte {
	b := binary.BigEndian.AppendUint64(nil, generation)
	return binary.BigEndian.AppendUint64(b, serial)
}

func (a *DefenderAdapter) confirm(g *defenderGeneration) error {
	confirmed, err := a.tracker.Confirm()
	if err != nil {
		a.issue("ack_observation_failed")
		return err
	}
	// This independent stream certifies only a completed admission prefix. It
	// carries no source bounds or suppression summary. Failed/in-flight
	// pages can never become source candidates through this observation.
	if raw := confirmed["admissions"]; len(raw) == 16 && binary.BigEndian.Uint64(raw) == g.id {
		g.acked.Store(binary.BigEndian.Uint64(raw[8:]))
	}
	if raw := confirmed["alerts"]; len(raw) == 16 && binary.BigEndian.Uint64(raw) == g.id {
		serial := binary.BigEndian.Uint64(raw[8:])
		if serial >= g.confirmed.Load() {
			g.confirmed.Store(serial)
		}
	}
	return nil
}
func (a *DefenderAdapter) run() {
	g := newGeneration(1, a.initialPosition())
	a.generation = g
	done := make(chan struct{})
	go func() { defer close(done); a.collect(a.ctx, g) }()
	timer := time.NewTimer(a.runtime.ackInterval())
	defer timer.Stop()
	for {
		select {
		case <-a.ctx.Done():
			<-done
			return
		case <-done:
			return
		case <-timer.C:
			_ = a.confirm(g)
			timer.Reset(a.runtime.ackInterval())
		}
	}
}
