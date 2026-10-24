package usp_entraid

import (
	"context"
	"errors"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
)

const settleLookback = 5 * time.Minute

// reclaimExpiredHistory releases only ACK-confirmed, expired historical IDs
// below a complete live boundary. Before removing their dedupe protection, it
// records the lost historical suffix in Unverified and truncates affected replay
// work. Complete live boundaries never move, and pending dependencies survive.
// This is an explicit coverage reduction under pressure, not successful replay.
func (a *EntraIDAdapter) reclaimExpiredHistory(g *entraGeneration) {
	now := a.runtime.now().UnixNano()
	cutoffs := make(map[string]int64)
	coverageReduced := false
	remove := make([][16]byte, 0)
	for key, v := range g.live {
		if v.expiry >= now || v.serial > g.confirmed.Load() {
			continue
		}
		stamp := v.expiry - int64(historyHorizon)
		eligible := true
		for name, s := range g.position {
			if v.stream != 0 && v.stream != streamCode(name) {
				continue
			}
			if stamp >= s.Boundary {
				eligible = false
				break
			}
		}
		if !eligible {
			continue
		}
		remove = append(remove, key)
		cutoff := stamp + int64(graphTimeQuantum) - stamp%int64(graphTimeQuantum)
		for name := range g.position {
			if v.stream != 0 && v.stream != streamCode(name) {
				continue
			}
			if cutoff > cutoffs[name] {
				cutoffs[name] = cutoff
			}
		}
	}
	if len(remove) == 0 {
		// An incomplete page may be holding admissions which have since ACKed.
		// Publish their dependency without advancing any incomplete source boundary.
		if a.candidate(g) != nil {
			a.issue("candidate_rejected")
		}
		return
	}
	for name, cutoff := range cutoffs {
		s := g.position[name]
		if cutoff <= s.Coverage {
			continue
		}
		coverageReduced = true
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
		delete(g.scans, name+"/replay")
		delete(g.scans, name+"/settle")
	}
	for _, key := range remove {
		delete(g.live, key)
	}
	for name, ids := range g.recent {
		kept := ids[:0]
		for _, id := range ids {
			if _, ok := g.live[id.digest]; ok {
				kept = append(kept, id)
			}
		}
		g.recent[name] = kept
	}
	if coverageReduced {
		a.issue("history_coverage_reduced")
	}
	if a.candidate(g) != nil {
		a.issue("candidate_rejected")
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

// Normalize restored replay bounds before any lane issues HTTP work, including
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
	if !utils.CheckpointWait(ctx, a.pollInterval) {
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
		if !utils.CheckpointWait(ctx, tick) {
			return
		}
	}
}
