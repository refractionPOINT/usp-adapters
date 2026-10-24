package usp_defender

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const graphTimeQuantum = 100 * time.Nanosecond
const graphTimeLayout = "2006-01-02T15:04:05.0000000Z"

var (
	errRetry      = errors.New("Defender request temporarily unavailable")
	errThrottle   = fmt.Errorf("Defender request throttled: %w", errRetry)
	errCredential = errors.New("Defender credentials or permissions rejected")
	errPage       = errors.New("Defender page invalid")
	errIncomplete = errors.New("Defender interval completeness unproven")
	errYield      = errors.New("Defender page budget yielded")
	errPressure   = errors.New("Defender live history at capacity")
)

type defenderRuntime struct {
	now                                                                         func() time.Time
	window, settle, liveSpacing, replaySpacing, failureLimit, reconcileInterval time.Duration
	saveInterval                                                                func() time.Duration
}

func defaultRuntime() defenderRuntime {
	return defenderRuntime{now: time.Now, window: 5 * time.Minute, settle: 5 * time.Minute, liveSpacing: time.Minute / 24, replaySpacing: time.Minute / 6, failureLimit: time.Hour, reconcileInterval: 15 * time.Minute,
		saveInterval: func() time.Duration {
			return 30*time.Second + time.Duration(time.Now().UnixNano()%int64(3*time.Second))
		}}
}
func collectionEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.Host == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, errors.New("invalid Defender endpoint")
	}
	return u, nil
}
func (a *DefenderAdapter) validateContinuation(raw string) error {
	u, err := url.Parse(raw)
	if len(raw) > 65536 || err != nil || u == nil || u.User != nil || u.Fragment != "" || u.Opaque != "" || !strings.EqualFold(u.Scheme, a.collectionURL.Scheme) || !strings.EqualFold(u.Host, a.collectionURL.Host) || u.EscapedPath() != a.collectionURL.EscapedPath() {
		return errPage
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return errPage
	}
	for k := range q {
		switch strings.ToLower(k) {
		case "access_token", "token", "authorization", "client_secret", "api_key":
			return errPage
		}
	}
	return nil
}
func (a *DefenderAdapter) windowURL(start, end int64) string {
	u := *a.collectionURL
	q := u.Query()
	q.Set("$filter", fmt.Sprintf("createdDateTime ge %s and createdDateTime lt %s", time.Unix(0, start).UTC().Format(graphTimeLayout), time.Unix(0, end).UTC().Format(graphTimeLayout)))
	u.RawQuery = q.Encode()
	return u.String()
}
func retryAfter(value string, now time.Time) time.Duration {
	if n, err := strconv.ParseInt(value, 10, 32); err == nil && n >= 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(value); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

// doJSON sanitizes every transport/status/parse error, including token responses.
// The copied client rejects redirects even when a test injects its transport.
func (a *DefenderAdapter) doJSON(ctx context.Context, method, raw, body, bearer string, out interface{}) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, method, raw, strings.NewReader(body))
	if err != nil {
		return 0, errPage
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		req.Header.Set("Accept", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	client := *a.httpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return 0, errRetry
	}
	defer resp.Body.Close()
	delay := retryAfter(resp.Header.Get("Retry-After"), time.Now())
	if resp.StatusCode == 429 {
		return delay, errThrottle
	}
	if resp.StatusCode >= 500 {
		return delay, errRetry
	}
	if resp.StatusCode == 401 || resp.StatusCode == 403 || (method == http.MethodPost && resp.StatusCode == 400) {
		return delay, errCredential
	}
	if resp.StatusCode != http.StatusOK {
		return delay, errPage
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (16<<20)+1))
	if err != nil {
		return delay, errRetry
	}
	if len(data) > 16<<20 || json.Unmarshal(data, out) != nil {
		return delay, errPage
	}
	return delay, nil
}

type graphPage struct {
	Items []map[string]interface{} `json:"value"`
	Next  string                   `json:"@odata.nextLink"`
	Count *int64                   `json:"@odata.count"`
}
type graphRequests struct {
	failureStart                      time.Time
	reported                          bool
	liveNext, replayNext, globalNext  time.Time
	liveRetry, replayRetry            time.Time
	liveBackoff, replayBackoff        time.Duration
	foregroundAttempts, totalAttempts uint64
}

func jitterDelay(minimum time.Duration) time.Duration {
	var b [1]byte
	_, _ = rand.Read(b[:])
	return minimum + time.Duration(b[0])*(minimum/2560)
}
func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// Each call makes at most one request attempt. Rate limits and transient retry
// deadlines are retained by the scheduler, so a failing history page cannot
// occupy the worker while independent fresh/live intervals are eligible.
func (a *DefenderAdapter) requestPage(ctx context.Context, raw string, replay bool, state *graphRequests) (graphPage, error) {
	var page graphPage
	if ctx.Err() != nil {
		return page, ctx.Err()
	}
	if a.validateContinuation(raw) != nil {
		return page, errPage
	}
	next, retry, backoff := &state.liveNext, &state.liveRetry, &state.liveBackoff
	spacing := a.runtime.liveSpacing
	if replay {
		next, retry, backoff = &state.replayNext, &state.replayRetry, &state.replayBackoff
		spacing = a.runtime.replaySpacing
	}
	now := time.Now()
	if now.Before(later(state.globalNext, later(*next, *retry))) {
		return page, errYield
	}
	*next = now.Add(spacing)
	state.totalAttempts++
	if !replay {
		state.foregroundAttempts++
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	form := url.Values{"client_id": {a.conf.ClientID}, "client_secret": {a.conf.ClientSecret}, "scope": {a.conf.scope()}, "grant_type": {"client_credentials"}}
	delay, err := a.doJSON(ctx, http.MethodPost, a.conf.tokenURL(), form.Encode(), "", &token)
	if delay > 0 {
		state.globalNext = later(state.globalNext, time.Now().Add(delay))
	}
	if err == nil && token.AccessToken == "" {
		err = errCredential
	}
	if err == nil && delay > 0 {
		return page, errYield
	} // honor a token endpoint's cooldown before another request
	if err == nil {
		delay, err = a.doJSON(ctx, http.MethodGet, raw, "", token.AccessToken, &page)
		if delay > 0 {
			state.globalNext = later(state.globalNext, time.Now().Add(delay))
		}
	}
	if ctx.Err() != nil {
		return graphPage{}, ctx.Err()
	}
	if err == nil {
		if page.Items == nil || len(page.Items) > 1000 || (page.Count != nil && *page.Count < 0) || (page.Next != "" && a.validateContinuation(page.Next) != nil) {
			return graphPage{}, errPage
		}
		// Valid page collection, rather than an entire interval, breaks a continuous
		// vendor failure episode. Its opaque continuation may still be outstanding.
		state.failureStart = time.Time{}
		state.reported = false
		*retry = time.Time{}
		*backoff = 0
		return page, nil
	}
	if errors.Is(err, errCredential) {
		if a.conf.ClientOptions.OnError != nil {
			a.conf.ClientOptions.OnError(errCredential)
		}
		*retry = time.Now().Add(jitterDelay(a.pollInterval))
		return graphPage{}, errCredential
	}
	if !errors.Is(err, errRetry) {
		return graphPage{}, err
	}
	if state.failureStart.IsZero() {
		state.failureStart = time.Now()
	}
	if !state.reported && time.Since(state.failureStart) >= a.runtime.failureLimit {
		if a.conf.ClientOptions.OnError != nil {
			a.conf.ClientOptions.OnError(errors.New("Defender API unavailable for a continuous retry interval"))
		}
		state.reported = true
	}
	a.issue("vendor_retry")
	if *backoff == 0 {
		*backoff = time.Second
	}
	if delay < *backoff {
		delay = *backoff
	}
	*retry = time.Now().Add(jitterDelay(delay))
	// Retry-After applies across lanes; an unqualified 429 also pauses all lanes.
	// A replay-specific 5xx/transport backoff alone does not pause fresh traffic.
	if errors.Is(err, errThrottle) {
		state.globalNext = later(state.globalNext, *retry)
	}
	*backoff *= 2
	if *backoff > 30*time.Second {
		*backoff = 30 * time.Second
	}
	return graphPage{}, errYield
}

func (a *DefenderAdapter) shipItem(ctx context.Context, g *defenderGeneration, item map[string]interface{}, start, end int64) error {
	return a.shipScanItem(ctx, g, item, start, end, nil)
}

func (a *DefenderAdapter) shipScanItem(ctx context.Context, g *defenderGeneration, item map[string]interface{}, start, end int64, work *graphScan) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	id, ok := item["id"].(string)
	stamp, okTime := item["createdDateTime"].(string)
	created, err := time.Parse(time.RFC3339Nano, stamp)
	if !ok || id == "" || !okTime || err != nil {
		return errPage
	}
	t := created.UnixNano()
	// Ignore older alerts returned because their update time changed. Never use
	// lastUpdateDateTime either as selection, identity version, or a cursor.
	if t < start || t >= end {
		return nil
	}
	digest := eventDigest(a.sourceFingerprint(), id)
	if g.liveProof != nil {
		if _, known := g.liveProof.retained[digest]; known {
			return nil
		}
	}
	entry, exists := g.live[digest]
	if !exists {
		// After reclamation this bounded set still proves earlier IDs in this exact
		// opaque traversal were consumed. Pending entries remain in g.live.
		if work != nil {
			if _, consumed := work.ids[digest]; consumed {
				return nil
			}
		}
		if len(g.live) >= liveIDLimit {
			a.reclaim(g, work)
			if len(g.live) >= liveIDLimit {
				return errPressure
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		msg := &protocol.DataMessage{JsonPayload: item, TimestampMs: uint64(time.Now().UnixMilli())}
		dep, err := a.tracker.Ship(msg, time.Second)
		for errors.Is(err, uspclient.ErrorBufferFull) && ctx.Err() == nil {
			if a.conf.ClientOptions.OnWarning != nil {
				a.conf.ClientOptions.OnWarning("stream falling behind")
			}
			dep, err = a.tracker.Ship(msg, time.Second)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			if errors.Is(err, utils.ErrCheckpointSequence) || errors.Is(err, utils.ErrCheckpointDependency) || errors.Is(err, utils.ErrCheckpointLimit) {
				a.issue("admission_tracking_failed")
				return err
			}
			if !errors.Is(err, uspclient.ErrorClosed) && a.conf.ClientOptions.OnError != nil {
				a.conf.ClientOptions.OnError(errors.New("Defender event admission failed"))
			}
			return err
		}
		// Single traversal serializes dedupe with admission. No other traversal may
		// suppress this ID until its Ship has returned successfully.
		g.admitted++
		// A one-record admission prefix is complete after successful Ship, even
		// when its source page is incomplete. This stream is never persisted and
		// cannot certify a source frontier or suppression summary. Confirm snapshots
		// it before GetUnsent, so concurrent/newer admissions cannot bless it.
		if err := a.tracker.Candidate("admissions", candidateBytes(g.id, g.admitted, nil), dep); err != nil {
			return err
		}
		entry = liveID{dependency: dep, admission: g.admitted, expiry: t + int64(historyHorizon)}
		g.live[digest] = entry
		g.remember(savedID{digest: digest, expiry: entry.expiry})
	}
	if entry.admission > g.acked.Load() {
		dep, err := g.dependency.Include(entry.dependency)
		if err != nil {
			return err
		}
		g.dependency = dep
		g.hasDependency = true
	}
	return nil
}

// Prefer optional ACKed history behind the settled frontier and outside the
// recent prompt overlap. An old source timestamp alone does not make an ID
// safe to forget: each current traversal still needs its unconsumed IDs.
func (a *DefenderAdapter) reclaim(g *defenderGeneration, work *graphScan) {
	now, acked := a.runtime.now().UnixNano(), g.acked.Load()
	wall := time.Now()
	sourceDelta := now - g.pruneSource
	if wall.Before(g.pruneAfter) && sourceDelta < int64(a.runtime.window) && sourceDelta > -int64(a.runtime.window) {
		// At most one additional ACK-triggered walk per cooldown. A full protected
		// set cannot cause 65k-entry map walks on every idle scheduler turn.
		if acked <= g.pruneACK || g.pruneACKRetry {
			return
		}
		g.pruneACKRetry = true
	} else {
		interval := a.pollInterval
		if interval < time.Second {
			interval = time.Second
		}
		g.pruneAfter = wall.Add(interval)
		g.pruneSource = now
		g.pruneACKRetry = false
	}
	g.pruneACK = acked
	protected := now - int64(a.runtime.window+a.runtime.settle)
	if protected > g.position.Boundary {
		protected = g.position.Boundary
	}
	const batch = liveIDLimit / 16
	removed := 0
	scans := [3]*graphScan{g.liveScan, g.replayScan, g.tailScan}
	// First discard optional history. Only when that yields nothing retire
	// expired, consumed settled IDs, then expired history that compromises an
	// old traversal. This last tier keeps fresh traffic available at a stalled
	// source boundary without pretending that the old interval completed.
	for tier := 0; tier < 3 && removed == 0; tier++ {
		if tier == 1 && work != nil {
			removed = a.retainSettledProof(g, work, now, acked, batch)
			if removed != 0 {
				break
			}
		}
		for key, v := range g.live {
			if v.admission > acked {
				continue
			}
			created := v.expiry - int64(historyHorizon)
			if work != nil && created >= work.start && created < work.end {
				if _, consumed := work.ids[key]; !consumed {
					continue
				}
			}
			if tier == 0 {
				if created >= protected {
					continue
				}
			} else {
				if v.expiry >= now {
					continue
				}
				if tier == 1 {
					live := g.liveScan
					if live == nil || created < live.start || created >= live.end {
						continue
					}
					if _, consumed := live.ids[key]; !consumed {
						continue
					}
				}
			}
			harmless := true
			for _, scan := range scans {
				if scan != nil && !scan.blocked && created >= scan.start && created < scan.end {
					if _, consumed := scan.ids[key]; !consumed {
						harmless = false
						break
					}
				}
			}
			if tier < 2 && !harmless {
				continue
			}
			// A production traversal cannot destroy an ahead-of-frontier identity
			// when the bounded settled proof could not retain it.
			if work != nil && created >= g.position.Boundary {
				continue
			}
			delete(g.live, key)
			if before := created + int64(graphTimeQuantum); before > g.lostBefore {
				g.lostBefore = before
			}
			if created >= g.position.Coverage && (g.forgottenGapAt == 0 || created < g.forgottenGapAt) {
				g.forgottenGapAt = created
			}
			removed++
			if before := created + int64(graphTimeQuantum); before > g.forgottenBefore {
				g.forgottenBefore = before
			}
			for _, scan := range scans {
				if scan != nil && scan != work && created >= scan.start && created < scan.end {
					if _, consumed := scan.ids[key]; !consumed {
						scan.blocked = true
						scan.page = nil // this exact traversal can no longer resume safely
					}
				}
			}
			if removed >= batch {
				break
			}
		}
	}
	if removed == 0 {
		return
	}
	// A full batch paid for this walk with thousands of successful admissions.
	// Permit another productive batch without an artificial intake rate ceiling;
	// failed or tiny walks retain their cooldown against idle pressure loops.
	if removed == batch {
		g.pruneAfter = time.Time{}
	}
	// Every remaining active traversal either retained each necessary ID or
	// consumed it already. The global historical cutoff cannot block that exact
	// continuation merely because another lane performed the reclamation.
	for _, scan := range scans {
		if scan != nil && !scan.blocked {
			scan.reclaimed = true
		}
	}
	if work != nil && !work.blocked {
		work.reclaimed = true
	}
	g.narrowHistory()
	a.issue("coverage_limited")
}

// Narrowing a suffix excludes an explicitly unverified gap. It never advances
// Boundary or claims that an abandoned reconciliation prefix was delivered.
func (g *defenderGeneration) narrowHistory() {
	if g.forgottenBefore == 0 || g.forgottenGapAt == 0 || g.forgottenGapAt >= g.position.Boundary {
		return
	}
	coverage := g.forgottenBefore
	if coverage > g.position.Boundary {
		coverage = g.position.Boundary
	}
	if coverage > g.position.Coverage {
		if g.position.Unverified == 0 || g.position.Coverage < g.position.Unverified {
			g.position.Unverified = g.position.Coverage
		}
		g.position.Coverage = coverage
	}
}

// scanWindow streams pages and yields a COMPLETE prefix. A terminal full page,
// count shortfall, repeated link, or page budget is never a complete interval.
// Split the SAME fixed interval; timestamp ties cannot be skipped by a max cursor.
type retainedID struct {
	created  int64
	consumed bool
}

type graphScan struct {
	start, targetEnd, end int64
	next                  string // only in memory; restart re-runs the exact saved interval bounds
	links                 map[[32]byte]struct{}
	ids                   map[[16]byte]struct{}
	expected              int64
	page                  *graphPage // at most one validated page per lane, retained on pressure
	offset                int
	blocked               bool                    // exact history was reclaimed by a different traversal
	reclaimed             bool                    // this traversal owns a safe retained continuation past reclaimed IDs
	retained              map[[16]byte]retainedID // shares the existing 65,536-key lane budget with ids
	seenRetained          int
	finished              bool
	terminalTie           bool
}

func (a *DefenderAdapter) newScan(start, targetEnd, end int64) *graphScan {
	return &graphScan{start: start, targetEnd: targetEnd, end: end, next: a.windowURL(start, end), links: make(map[[32]byte]struct{}), ids: make(map[[16]byte]struct{})}
}
func (a *DefenderAdapter) scanWindow(ctx context.Context, g *defenderGeneration, start, end int64, replay bool, requests *graphRequests) (int64, error) {
	mode, budget := scanLive, 24
	if replay {
		mode, budget = scanReplay, 6
	}
	return a.scanPages(ctx, g, start, end, mode, budget, requests)
}

type scanMode uint8

const (
	scanLive scanMode = iota
	scanReplay
	scanTail
)

func (a *DefenderAdapter) scanPages(ctx context.Context, g *defenderGeneration, start, end int64, mode scanMode, pageBudget int, requests *graphRequests) (int64, error) {
	if mode == scanLive {
		defer func() { g.liveProof = g.liveScan }()
	}
	slot := &g.liveScan
	if mode == scanReplay {
		slot = &g.replayScan
	}
	if mode == scanTail {
		slot = &g.tailScan
	}
	if mode == scanLive && g.liveProof != nil {
		*slot = g.liveProof
	}
	cutoff := g.forgottenBefore
	if mode == scanLive {
		cutoff = g.lostBefore
	}
	if *slot == nil || (*slot).start != start || (*slot).targetEnd != end {
		if *slot != nil {
			(*slot).page = nil
		}
		if start < cutoff {
			return start, errIncomplete
		}
		if mode == scanLive && g.liveProof != nil {
			*slot = a.restartProofScan(g.liveProof, start, end, end)
		} else {
			*slot = a.newScan(start, end, end)
		}
	}
	work := *slot
	if mode == scanLive {
		g.liveProof = work
	}
	if work.blocked || (start < cutoff && !work.reclaimed) {
		work.page = nil
		return start, errIncomplete
	}
	if work.finished {
		return work.end, nil
	}

	for used := 0; ; {
		if used >= pageBudget {
			return start, errYield
		}
		key := sha256.Sum256([]byte(work.next))
		_, repeated := work.links[key]
		incomplete := repeated || len(work.links) >= 128
		if !incomplete {
			if work.page == nil {
				page, err := a.requestPage(ctx, work.next, mode == scanReplay, requests)
				if err != nil {
					return start, err
				}
				work.page = &page
				work.offset = 0
				if page.Count != nil && *page.Count > work.expected {
					work.expected = *page.Count
				}
			}
			used++ // a retained page also consumes this turn's work budget
			page := work.page
			for work.offset < len(page.Items) {
				if err := a.shipScanItem(ctx, g, page.Items[work.offset], start, work.end, work); err != nil {
					if errors.Is(err, errPage) {
						work.page = nil
					} // refetch; successful prefix remains suppressed
					return start, err
				}
				id, _ := page.Items[work.offset]["id"].(string)
				created, _ := time.Parse(time.RFC3339Nano, page.Items[work.offset]["createdDateTime"].(string))
				if created.UnixNano() >= start && created.UnixNano() < work.end {
					key := eventDigest(a.sourceFingerprint(), id)
					if proof, ok := work.retained[key]; ok {
						if !proof.consumed {
							proof.consumed = true
							work.retained[key] = proof
							work.seenRetained++
						}
					} else if len(work.ids)+len(work.retained) < liveIDLimit {
						work.ids[key] = struct{}{}
					}
				}

				work.offset++
			}
			work.links[key] = struct{}{}
			work.page = nil
			if len(work.retained) > 0 && len(page.Items) == 1000 && page.Next == "" {
				stamp, _ := page.Items[0]["createdDateTime"].(string)
				work.terminalTie = true
				for _, item := range page.Items {
					if item["createdDateTime"] != stamp {
						work.terminalTie = false
						break
					}
				}
			}
			if page.Next != "" {
				work.next = page.Next
				continue
			}
			if len(page.Items) < 1000 && int64(len(work.ids)+work.seenRetained) >= work.expected {
				if mode == scanLive && len(work.retained) > 0 {
					work.finished = true
				} else {
					*slot = nil
					if mode == scanLive {
						g.liveProof = nil
					}
				}
				return work.end, nil
			}
		}
		// A full terminal page/count shortfall/cycle is not complete. Narrow the
		// same fixed interval; retain this smaller attempt across scheduling yields.
		// Restarting this interval after forgetting its digests would repeat old
		// deliveries. Keep the exact unresolved scan, even for ambiguous pages.
		if work.expected > liveIDLimit || work.terminalTie || start < cutoff {
			return start, errIncomplete
		}
		if work.end-start <= int64(graphTimeQuantum) {
			if mode == scanLive {
				*slot = a.restartProofScan(work, start, end, work.end)
			} else {
				*slot = a.newScan(start, end, work.end)
			}
			return start, errIncomplete
		}
		midpoint := start + (work.end-start)/2
		midpoint -= midpoint % int64(graphTimeQuantum)
		if midpoint <= start {
			if mode == scanLive {
				*slot = a.restartProofScan(work, start, end, work.end)
			} else {
				*slot = a.newScan(start, end, work.end)
			}
			return start, errIncomplete
		}
		a.issue("interval_split")
		if mode == scanLive {
			work = a.restartProofScan(work, start, end, midpoint)
		} else {
			work = a.newScan(start, end, midpoint)
		}
		*slot = work
	}
}

func (a *DefenderAdapter) complete(g *defenderGeneration, end int64, replay bool) error {
	if replay {
		g.position.Replay = end
		if end >= g.position.ReplayEnd {
			g.position.Replay, g.position.ReplayEnd = 0, 0
		}
	} else {
		g.position.Boundary = end
		if end >= g.position.WindowEnd {
			g.position.WindowEnd = end + int64(a.runtime.window)
		}
	}
	if !replay && g.liveProof != nil {
		for key, proof := range g.liveProof.retained {
			if proof.created < g.position.Boundary {
				delete(g.liveProof.retained, key)
				if proof.consumed {
					g.liveProof.seenRetained--
				}
				if before := proof.created + int64(graphTimeQuantum); before > g.lostBefore {
					g.lostBefore = before
				}
			}
		}
		if g.liveProof.finished && len(g.liveProof.retained) == 0 {
			g.liveProof = nil
			g.liveScan = nil
		}
	}
	g.narrowHistory()
	g.serial++
	g.position.IDs = append([]savedID(nil), g.recent...)
	g.position.omittedPast = g.summaryOmission()
	raw, err := encodePosition(g.position)
	if err != nil {
		return err
	}
	// All summary IDs and all complete boundaries depend on the max admission,
	// including records suppressed while their earlier send remained unACKed.
	if g.hasDependency {
		return a.tracker.Candidate("alerts", candidateBytes(g.id, g.serial, raw), g.dependency)
	}
	return a.tracker.Candidate("alerts", candidateBytes(g.id, g.serial, raw))
}

// collect interleaves one page per lane. The open tail ships promptly but never
// stages a durable frontier; only completely drained, settled live windows do.
func (a *DefenderAdapter) collect(ctx context.Context, g *defenderGeneration) {
	if !utils.CheckpointWait(ctx, a.pollInterval) {
		return
	}
	requests := &graphRequests{}
	nextReplay := time.Now().Add(a.runtime.reconcileInterval)
	if g.position.Replay != 0 {
		nextReplay = time.Now()
	}
	var nextTail time.Time
	tailFirst := true
	for ctx.Err() == nil {
		now := a.runtime.now().UTC()
		floor := now.Add(-historyHorizon).UnixNano()
		if floor > g.position.Coverage && floor < g.position.Boundary && g.position.Replay == 0 {
			g.position.Coverage = floor
		}
		before, foregroundBefore := requests.totalAttempts, requests.foregroundAttempts
		live := func() bool {
			if g.position.WindowEnd > now.Add(-a.runtime.settle).UnixNano() {
				return true
			}
			end, err := a.scanPages(ctx, g, g.position.Boundary, g.position.WindowEnd, scanLive, 1, requests)
			if errors.Is(err, errYield) {
				return true
			}
			if err != nil {
				return a.collectionFailure(ctx, err)
			}
			if a.complete(g, end, false) != nil {
				a.issue("candidate_rejected")
				return false
			}
			return true
		}
		tail := func() bool {
			work := g.tailScan
			// A prompt scan is opportunistic: after one poll of source-time lag,
			// refresh even an unfinishable tie/continuation. The settled and
			// history lanes retain their exact unresolved bounds and alone own
			// completeness. Keep all admitted IDs, ACK dependencies and quota
			// deadlines; replacing this single scan never certifies its end.
			if work != nil && now.Sub(time.Unix(0, work.targetEnd)) >= a.pollInterval {
				g.tailScan = nil
				work = nil
			}
			if work == nil {
				if time.Now().Before(nextTail) {
					return true
				}
				end := now.UnixNano()
				end -= end % int64(graphTimeQuantum)
				start := end - int64(a.runtime.window+a.runtime.settle)
				if start < g.position.Boundary {
					start = g.position.Boundary
				}
				if start >= end {
					return true
				}
				work = a.newScan(start, end, end)
				g.tailScan = work
			}
			end, err := a.scanPages(ctx, g, work.start, work.targetEnd, scanTail, 1, requests)
			if errors.Is(err, errYield) {
				return true
			}
			if err != nil {
				return a.collectionFailure(ctx, err)
			}
			if end < work.targetEnd {
				g.tailScan = a.newScan(end, work.targetEnd, work.targetEnd)
			} else {
				nextTail = time.Now().Add(a.pollInterval)
			}
			// No Candidate/complete call here: even ACKed tail records do not prove a
			// settled source boundary or that another tail page/window is complete.
			return true
		}
		if tailFirst {
			if !tail() || !live() {
				return
			}
		} else {
			if !live() || !tail() {
				return
			}
		}
		if requests.foregroundAttempts != foregroundBefore {
			tailFirst = !tailFirst
		}
		if !time.Now().Before(nextReplay) {
			if g.position.Replay == 0 {
				start := g.position.Coverage
				if start < g.position.Boundary {
					g.position.Replay, g.position.ReplayEnd = start, g.position.Boundary
					if a.complete(g, g.position.Boundary, false) != nil {
						a.issue("candidate_rejected")
						return
					}
				} else {
					a.issue("coverage_limited")
					nextReplay = time.Now().Add(a.runtime.reconcileInterval)
				}
			}
			if g.position.Replay != 0 && g.position.Replay < g.position.Coverage {
				a.issue("coverage_limited")
				if g.position.Unverified == 0 || g.position.Replay < g.position.Unverified {
					g.position.Unverified = g.position.Replay
				}
				g.position.Replay = g.position.Coverage
				if g.position.Replay >= g.position.ReplayEnd {
					g.position.Replay, g.position.ReplayEnd = 0, 0
					nextReplay = time.Now().Add(a.runtime.reconcileInterval)
				}
			}
			if g.position.Replay != 0 {
				end := g.position.Replay + int64(a.runtime.window)
				if end > g.position.ReplayEnd {
					end = g.position.ReplayEnd
				}
				end, err := a.scanPages(ctx, g, g.position.Replay, end, scanReplay, 1, requests)
				if err != nil {
					if !errors.Is(err, errYield) && !a.collectionFailure(ctx, err) {
						return
					}
				} else {
					if a.complete(g, end, true) != nil {
						a.issue("candidate_rejected")
						return
					}
					if g.position.Replay == 0 {
						nextReplay = time.Now().Add(a.runtime.reconcileInterval)
					}
				}
			}
		}
		// One immediate turn can follow progress. With all lanes deferred, sleep to
		// the earliest rate/backoff/poll deadline instead of spinning on errYield.
		if requests.totalAttempts != before {
			continue
		}
		wait := a.pollInterval
		wall := time.Now()
		for _, deadline := range []time.Time{requests.liveNext, requests.replayNext, requests.globalNext, requests.liveRetry, requests.replayRetry, nextTail, nextReplay} {
			if d := deadline.Sub(wall); d > 0 && d < wait {
				wait = d
			}
		}
		if !utils.CheckpointWait(ctx, wait) {
			return
		}
	}
}

func (a *DefenderAdapter) collectionFailure(ctx context.Context, err error) bool {
	if ctx.Err() != nil || errors.Is(err, uspclient.ErrorClosed) {
		return false
	}
	switch {
	case errors.Is(err, errPressure):
		a.issue("live_history_pressure")
	case errors.Is(err, errIncomplete):
		a.issue("interval_stalled")
	case errors.Is(err, errCredential): // already reported as a vendor failure
	case errors.Is(err, errPage):
		a.issue("invalid_page")
	default:
		return false
	}
	return true
}

// Retain an ACKed identity in the settled lane before releasing its live slot.
// This is suppression proof only: unconsumed keys never contribute to count or
// certify a source boundary. The map moves with that lane into its next window.
func (a *DefenderAdapter) retainSettledProof(g *defenderGeneration, work *graphScan, now int64, acked uint64, batch int) int {
	owner := g.liveProof
	if owner == nil {
		owner = g.liveScan
		if owner == nil {
			owner = a.newScan(g.position.Boundary, g.position.WindowEnd, g.position.WindowEnd)
			g.liveScan = owner
		}
		g.liveProof = owner
	}
	// A partially observed pending set stays pinned. Once cumulatively ACKed,
	// a finite burst can be moved without waiting for a completed source window.
	for _, entry := range g.live {
		if entry.admission > acked {
			return 0
		}
	}
	if owner.retained == nil {
		owner.retained = make(map[[16]byte]retainedID)
	}
	removed := 0
	compacted := false
	floor := now - int64(a.runtime.window+a.runtime.settle)
	for key, entry := range g.live {
		created := entry.expiry - int64(historyHorizon)
		if created < g.position.Boundary || created >= floor {
			continue
		}
		_, consumed := owner.ids[key]
		if !consumed && len(owner.ids)+len(owner.retained) >= liveIDLimit {
			// At most one bounded compaction walk per reclamation attempt. Retired
			// proofs are never victims; ordinary markers still have live identities.
			if !compacted {
				compacted = true
				freed := 0
				for prior := range owner.ids {
					if _, present := g.live[prior]; present {
						delete(owner.ids, prior)
						freed++
						if freed >= batch {
							break
						}
					}
				}
			}
			if len(owner.ids)+len(owner.retained) >= liveIDLimit {
				continue
			}
		}

		if consumed {
			delete(owner.ids, key)
			owner.seenRetained++
		}
		owner.retained[key] = retainedID{created: created, consumed: consumed}
		delete(g.live, key)
		if before := created + int64(graphTimeQuantum); before > g.forgottenBefore {
			g.forgottenBefore = before
		}
		if created >= g.position.Coverage && (g.forgottenGapAt == 0 || created < g.forgottenGapAt) {
			g.forgottenGapAt = created
		}
		for _, scan := range []*graphScan{g.replayScan, g.tailScan} {
			if scan != nil && scan != work && created >= scan.start && created < scan.end {
				if _, seen := scan.ids[key]; !seen {
					scan.blocked = true
					scan.page = nil
				}
			}
		}
		removed++
		if removed >= batch {
			break
		}
	}
	return removed
}

// recent keeps only 257 newest IDs. Older real omissions can still be present
// in live/settled proof, so inspect those bounded sets before encoding history.
func (g *defenderGeneration) summaryOmission() int64 {
	selected := make(map[[16]byte]struct{}, savedIDLimit)
	for i, entry := range g.recent {
		if i < savedIDLimit {
			selected[entry.digest] = struct{}{}
		}
	}
	var omitted int64
	note := func(key [16]byte, created int64) {
		if _, saved := selected[key]; !saved && created < g.position.Boundary && created > omitted {
			omitted = created
		}
	}
	for key, entry := range g.live {
		note(key, entry.expiry-int64(historyHorizon))
	}
	if g.liveProof != nil {
		for key, proof := range g.liveProof.retained {
			note(key, proof.created)
		}
	}
	for _, entry := range g.recent {
		note(entry.digest, entry.expiry-int64(historyHorizon))
	}
	return omitted
}

// Reset source observations while moving the same bounded suppression proof.
// Retained identities outside a narrowed prefix remain available to its suffix.
func (a *DefenderAdapter) restartProofScan(old *graphScan, start, target, end int64) *graphScan {
	fresh := a.newScan(start, target, end)
	if old != nil && len(old.retained) > 0 {
		fresh.retained = old.retained
		for key, proof := range fresh.retained {
			proof.consumed = false
			fresh.retained[key] = proof
		}
		*old = *fresh
		return old
	}
	return fresh
}
