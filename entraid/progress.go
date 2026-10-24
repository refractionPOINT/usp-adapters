package usp_entraid

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
)

const (
	liveIDLimit    = 262144
	historyHorizon = 72 * time.Hour
)

type liveID struct {
	dependency acktrack.ACKDependency
	serial     uint64
	expiry     int64
	stream     byte
}

// All times are exact UTC nanoseconds. Only complete half-open windows advance
// Boundary. Replay bounds and Graph continuation tokens live only in this process.
// Coverage describes local retained IDs, not vendor retention.
type streamPosition struct {
	Boundary, WindowEnd, Coverage, Replay, ReplayEnd, Unverified int64
	Deep                                                         bool
}
type entraPosition map[string]streamPosition

type entraGeneration struct {
	id, serial    uint64
	position      entraPosition
	live          map[[16]byte]liveID
	dependency    acktrack.ACKDependency
	hasDependency bool
	confirmed     atomic.Uint64
	scans         map[string]*graphScan
	maintenance   historyMaintenance
}

func newGeneration(id uint64, p entraPosition) *entraGeneration {
	return &entraGeneration{id: id, position: p, live: make(map[[16]byte]liveID), scans: make(map[string]*graphScan)}
}

func (a *EntraIDAdapter) sourceFingerprint() [32]byte {
	names := make([]string, 0, len(a.streams))
	for _, s := range a.streams {
		names = append(names, s.name)
	}
	sort.Strings(names)
	// Credentials, including application ID, are deliberately excluded so that
	// credential rotation preserves this collection identity.
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

func (a *EntraIDAdapter) initialPosition() entraPosition {
	start := a.startedAt
	if start == 0 {
		start = a.runtime.now().UTC().UnixNano()
	}
	start -= start % int64(graphTimeQuantum)
	p := make(entraPosition)
	for _, s := range a.streams {
		p[s.name] = streamPosition{Boundary: start, WindowEnd: start + int64(a.runtime.window), Coverage: start}
	}
	return p
}

// Local generation/ordinal marker; it contains no serialized source state.
func candidateBytes(generation, serial uint64) []byte {
	b := binary.BigEndian.AppendUint64(nil, generation)
	return binary.BigEndian.AppendUint64(b, serial)
}

func (a *EntraIDAdapter) confirm(g *entraGeneration) error {
	positions, err := a.tracker.Confirm()
	if err != nil {
		a.issue("ack_observation_failed")
		return err
	}
	if raw := positions["entra"]; len(raw) == 16 && binary.BigEndian.Uint64(raw) == g.id {
		serial := binary.BigEndian.Uint64(raw[8:])
		if serial >= g.confirmed.Load() {
			g.confirmed.Store(serial)
		}
	}
	return nil
}
func (a *EntraIDAdapter) candidate(g *entraGeneration) error {
	g.serial++
	if g.hasDependency {
		return a.tracker.Candidate("entra", candidateBytes(g.id, g.serial), g.dependency)
	}
	return a.tracker.Candidate("entra", candidateBytes(g.id, g.serial))
}
func (a *EntraIDAdapter) run() {
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
