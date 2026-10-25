package usp_entraid

import (
	"context"
	"errors"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
)

const settleLookback = 5 * time.Minute

// Maintenance is collector-owned. Each sweep visits at most twice the bounded
// cache plus its retained entries during compaction. Idle polling does not scan
// the map: obsolete history is checked every 30s, pressure at most once a second.
type historyMaintenance struct {
	pressureWall, pruneWall                        time.Time
	pressureSource, pruneSource                    int64
	dependency                                     acktrack.ACKDependency
	peak, sweeps, visited, maxVisited, compactions int
}

// reclaimExpiredHistory prioritizes live collection over optional historical
// coverage, including unexpired history. Older history yields before the complete
// settle tail; only unfinished live work and unACKed admissions are mandatory. Reducing optional
// coverage explicitly leaves Unverified; it never completes a source window.
func (a *EntraIDAdapter) reclaimExpiredHistory(g *entraGeneration) {
	a.maintainHistory(g, true)
}

func (a *EntraIDAdapter) maintainHistory(g *entraGeneration, pressure bool) {
	m := &g.maintenance
	now, wall := a.runtime.now().UnixNano(), time.Now()
	lastWall, lastSource, interval := m.pruneWall, m.pruneSource, 30*time.Second
	if pressure {
		lastWall, lastSource, interval = m.pressureWall, m.pressureSource, time.Second
	}
	if !lastWall.IsZero() && wall.Sub(lastWall) < interval && now >= lastSource && now-lastSource < int64(interval) {
		a.exposeAdmissions(g, pressure)
		return
	}
	if pressure {
		m.pressureWall, m.pressureSource = wall, now
	} else {
		m.pruneWall, m.pruneSource = wall, now
	}
	if len(g.live) > m.peak {
		m.peak = len(g.live)
	}
	confirmed := g.confirmed.Load()
	// Classify against one pre-reclamation snapshot. Updating Coverage below must
	// not change which tier is removed during the second bounded map pass.
	before := make(entraPosition, len(g.position))
	for name, position := range g.position {
		before[name] = position
	}
	tier := func(v liveID) int {
		if v.serial > confirmed {
			return 0
		}
		stamp := v.expiry - int64(historyHorizon)
		rank := 1
		for name, position := range before {
			if v.stream != 0 && v.stream != streamCode(name) {
				continue
			}
			// The unfinished live window is never optional, even after ACK.
			if stamp >= position.Boundary {
				return 0
			}
			if stamp >= position.Coverage {
				if !pressure {
					return 0
				}
				if stamp >= position.Boundary-int64(settleLookback) {
					rank = 2
				}
			}
		}
		return rank
	}
	cutoffsByTier := [3]map[string]int64{nil, {}, {}}
	counts := [3]int{}
	visits := 0
	for _, v := range g.live {
		visits++
		rank := tier(v)
		if rank == 0 {
			continue
		}
		counts[rank]++
		stamp := v.expiry - int64(historyHorizon)
		cutoff := stamp + int64(graphTimeQuantum) - stamp%int64(graphTimeQuantum)
		for name := range before {
			if v.stream != 0 && v.stream != streamCode(name) {
				continue
			}
			if cutoff > cutoffsByTier[rank][name] {
				cutoffsByTier[rank][name] = cutoff
			}
		}
	}
	selected := 1
	if pressure && counts[1] == 0 {
		selected = 2
	}
	cutoffs := cutoffsByTier[selected]

	reduced := false
	for name, cutoff := range cutoffs {
		s := g.position[name]
		if cutoff <= s.Coverage {
			continue
		}
		reduced = true
		lost := s.Coverage
		if s.Replay != 0 && s.Replay < lost {
			lost = s.Replay
		}
		if s.Unverified == 0 || lost < s.Unverified {
			s.Unverified = lost
		}
		s.Coverage = cutoff
		if s.Replay != 0 && s.Replay < cutoff {
			s.Replay = cutoff
			if s.Replay >= s.ReplayEnd {
				s.Replay, s.ReplayEnd = 0, 0
			}
		}
		g.position[name] = s
		// Discard delayed old traversals before removing any dedupe protection.
		delete(g.scans, name+"/replay")
		delete(g.scans, name+"/settle")
	}
	removed := 0
	for key, v := range g.live {
		visits++
		if tier(v) == selected {
			delete(g.live, key)
			removed++
		}
	}
	// Go's delete retains buckets. Rebuilding a substantially smaller map releases
	// the old allocation after GC, temporarily retaining old and new maps together.
	if m.peak >= 4096 && len(g.live) <= m.peak/4 {
		compact := make(map[[16]byte]liveID, len(g.live))
		for key, v := range g.live {
			compact[key] = v
			visits++
		}
		g.live = compact
		m.peak = len(g.live)
		m.compactions++
	}
	m.sweeps++
	m.visited += visits
	if visits > m.maxVisited {
		m.maxVisited = visits
	}
	if reduced {
		a.issue("history_coverage_reduced")
	}
	if removed > 0 {
		if a.candidate(g) != nil {
			a.issue("candidate_rejected")
		}
	} else {
		a.exposeAdmissions(g, pressure)
	}
}

// A partial page's admitted prefix can ACK before its source window completes.
// Publish those actual tracker dependencies with the unchanged complete boundary;
// this proves admission ACKs without fabricating source progress. Coalesce repeats.
func (a *EntraIDAdapter) exposeAdmissions(g *entraGeneration, pressure bool) {
	if pressure && g.hasDependency && g.maintenance.dependency != g.dependency {
		if a.candidate(g) != nil {
			a.issue("candidate_rejected")
		} else {
			g.maintenance.dependency = g.dependency
		}
	}
}

// liveStep shares the live budget between forward collection and the regular
// five-minute settle recheck. Each traversal keeps fixed bounds while paging;
// settle never advances the complete live boundary or starts before coverage.
func (a *EntraIDAdapter) liveStep(ctx context.Context, g *entraGeneration, stream entraStream, requests *graphRequests, nextSettle map[string]time.Time, preferSettle map[string]bool) error {
	now := a.runtime.now()
	s := g.position[stream.name]
	key := stream.name + "/settle"
	work := g.scans[key]
	settleDue := work != nil || !now.Before(nextSettle[stream.name])
	start := s.Boundary - int64(settleLookback)
	if start < s.Coverage {
		start = s.Coverage
	}
	settleDue = settleDue && (work != nil || start < s.Boundary)
	forwardDue := s.WindowEnd <= now.UnixNano()
	settleFirst := settleDue && (preferSettle[stream.name] || !forwardDue)
	// A lane in backoff yields its permit to the other ready traversal. At
	// most one actual request is made; global/vendor limits still gate both.
	for attempt := 0; attempt < 2; attempt++ {
		before := requests.serial
		var err error
		if settleFirst {
			if !settleDue {
				return nil
			}
			end := s.Boundary
			if work != nil {
				start, end = work.start, work.targetEnd
			}
			_, err = a.scanLane(ctx, g, stream, start, end, "settle", requests)
			if requests.serial != before {
				preferSettle[stream.name] = false
			}
			if err == nil {
				nextSettle[stream.name] = now.Add(a.pollInterval)
				return a.candidate(g)
			}
		} else {
			if !forwardDue {
				return nil
			}
			var end int64
			end, err = a.scanWindow(ctx, g, stream, s.Boundary, s.WindowEnd, false, requests)
			if requests.serial != before {
				preferSettle[stream.name] = true
			}
			if err == nil {
				return a.complete(g, stream, end, false)
			}
		}
		if !errors.Is(err, errYield) || requests.serial != before {
			return err
		}
		settleFirst = !settleFirst
	}

	return errYield
}

// Normalize local replay bounds before any lane issues HTTP work, including
// settle. Cancellation must not leave a stale interval pretending wider coverage.
func (a *EntraIDAdapter) clampReplay(g *entraGeneration, stream entraStream) {
	s := g.position[stream.name]
	if s.Replay != 0 && s.Replay < s.Coverage {
		a.issue("coverage_limited")
		if s.Unverified == 0 || s.Replay < s.Unverified {
			s.Unverified = s.Replay
		}
		s.Replay = s.Coverage
		delete(g.scans, stream.name+"/replay")
		if s.Replay >= s.ReplayEnd {
			s.Replay, s.ReplayEnd = 0, 0
		}
		g.position[stream.name] = s
		if a.candidate(g) != nil {
			a.issue("candidate_rejected")
		}
	}
}

func (a *EntraIDAdapter) collect(ctx context.Context, g *entraGeneration) {
	if !acktrack.Wait(ctx, a.pollInterval) {
		return
	}
	requests := &graphRequests{}
	nextReplay := make(map[string]time.Time)
	nextSettle := make(map[string]time.Time)
	preferSettle := make(map[string]bool)
	for _, stream := range a.streams {
		nextReplay[stream.name] = a.runtime.now().Add(a.runtime.reconcileInterval)
		if g.position[stream.name].Replay != 0 {
			nextReplay[stream.name] = a.runtime.now()
		}
	}
	liveTurn, replayTurn := 0, 0
	for ctx.Err() == nil {
		now := a.runtime.now()
		a.maintainHistory(g, false)
		liveStart := liveTurn
		for i := range a.streams {
			index := (liveStart + i) % len(a.streams)
			stream := a.streams[index]
			s := g.position[stream.name]
			floor := now.Add(-historyHorizon).UnixNano()
			floor -= floor % int64(graphTimeQuantum)
			if floor > s.Coverage && s.Replay == 0 {
				if floor > s.Boundary {
					floor = s.Boundary
				}
				s.Coverage = floor
				g.position[stream.name] = s
				delete(g.scans, stream.name+"/settle")
				delete(g.scans, stream.name+"/replay")
			}
			a.clampReplay(g, stream)
			before := requests.serial
			err := a.liveStep(ctx, g, stream, requests, nextSettle, preferSettle)
			if requests.serial != before {
				liveTurn = (index + 1) % len(a.streams)
			}
			if err != nil && !a.collectionFailure(ctx, err) {
				return
			}
		}
		replayStart := replayTurn
		for i := range a.streams {
			index := (replayStart + i) % len(a.streams)
			stream := a.streams[index]
			s := g.position[stream.name]
			if now.Before(nextReplay[stream.name]) {
				continue
			}

			if s.Replay == 0 {
				start := s.Coverage
				horizon := historyHorizon
				if stream.name != "risk_detections" && !s.Deep {
					horizon = 3 * time.Hour
				}
				floor := now.Add(-horizon).UnixNano()
				floor -= floor % int64(graphTimeQuantum)
				if floor > start {
					start = floor
				}
				if start >= s.Boundary {
					nextReplay[stream.name] = now.Add(a.runtime.reconcileInterval)
					continue
				}
				s.Replay, s.ReplayEnd = start, s.Boundary
				g.position[stream.name] = s
				if a.candidate(g) != nil {
					a.issue("candidate_rejected")
				}
			}
			end := s.Replay + int64(a.runtime.replayWindow)
			if end > s.ReplayEnd {
				end = s.ReplayEnd
			}
			before := requests.serial
			end, err := a.scanWindow(ctx, g, stream, s.Replay, end, true, requests)
			if requests.serial != before {
				replayTurn = (index + 1) % len(a.streams)
			}
			if err == nil {
				if a.complete(g, stream, end, true) != nil {
					a.issue("candidate_rejected")
				}
				if g.position[stream.name].Replay == 0 {
					nextReplay[stream.name] = now.Add(a.runtime.reconcileInterval)
				}
			} else if !a.collectionFailure(ctx, err) {
				return
			}
		}
		tick := a.pollInterval
		if tick > 250*time.Millisecond {
			tick = 250 * time.Millisecond
		}
		if !acktrack.Wait(ctx, tick) {
			return
		}
	}
}
