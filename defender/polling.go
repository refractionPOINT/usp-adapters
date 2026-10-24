package usp_defender

import (
	"context"
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
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
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
	failureStart         time.Time
	reported             bool
	liveNext, replayNext time.Time
}

func (a *DefenderAdapter) requestPage(ctx context.Context, raw string, replay bool, state *graphRequests) (graphPage, error) {
	var page graphPage
	if a.validateContinuation(raw) != nil {
		return page, errPage
	}
	backoff := time.Second
	for ctx.Err() == nil {
		next := &state.liveNext
		spacing := a.runtime.liveSpacing
		if replay {
			next = &state.replayNext
			spacing = a.runtime.replaySpacing
		}
		if wait := time.Until(*next); wait > 0 && !utils.CheckpointWait(ctx, wait) {
			return page, ctx.Err()
		}
		*next = time.Now().Add(spacing)
		var token struct {
			AccessToken string `json:"access_token"`
		}
		form := url.Values{"client_id": {a.conf.ClientID}, "client_secret": {a.conf.ClientSecret}, "scope": {a.conf.scope()}, "grant_type": {"client_credentials"}}
		delay, err := a.doJSON(ctx, http.MethodPost, a.conf.tokenURL(), form.Encode(), "", &token)
		if err == nil && token.AccessToken == "" {
			err = errCredential
		}
		if err == nil {
			page = graphPage{}
			delay, err = a.doJSON(ctx, http.MethodGet, raw, "", token.AccessToken, &page)
		}
		if err == nil {
			if page.Items == nil || len(page.Items) > 1000 || (page.Count != nil && *page.Count < 0) || (page.Next != "" && a.validateContinuation(page.Next) != nil) {
				return graphPage{}, errPage
			}
			if delay > 0 && !utils.CheckpointWait(ctx, delay) {
				return graphPage{}, ctx.Err()
			}
			return page, nil
		}
		if ctx.Err() != nil {
			return graphPage{}, ctx.Err()
		}
		if errors.Is(err, errCredential) {
			if a.conf.ClientOptions.OnError != nil {
				a.conf.ClientOptions.OnError(errCredential)
			}
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
		if delay < backoff {
			delay = backoff
		}
		if !utils.CheckpointWait(ctx, delay) {
			return graphPage{}, ctx.Err()
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
	return graphPage{}, ctx.Err()
}

func (a *DefenderAdapter) shipItem(ctx context.Context, g *defenderGeneration, item map[string]interface{}, start, end int64) error {
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
	entry, exists := g.live[digest]
	if !exists {
		if len(g.live) >= liveIDLimit {
			for key, v := range g.live {
				if v.expiry < a.runtime.now().UnixNano() && v.serial <= g.confirmed.Load() {
					delete(g.live, key)
				}
			}
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
		entry = liveID{dependency: dep, serial: g.serial + 1, expiry: t + int64(historyHorizon)}
		g.live[digest] = entry
		g.remember(savedID{digest: digest, expiry: entry.expiry})
	}
	if entry.serial > g.confirmed.Load() {
		dep, err := g.dependency.Include(entry.dependency)
		if err != nil {
			return err
		}
		g.dependency = dep
		g.hasDependency = true
	}
	return nil
}

// scanWindow streams pages and yields a COMPLETE prefix. A terminal full page,
// count shortfall, repeated link, or page budget is never a complete interval.
// Split the SAME fixed interval; timestamp ties cannot be skipped by a max cursor.
type graphScan struct {
	start, targetEnd, end int64
	next                  string // only in memory; restart re-runs the exact saved interval bounds
	links                 map[[32]byte]struct{}
	ids                   map[[16]byte]struct{}
	expected              int64
}

func (a *DefenderAdapter) newScan(start, targetEnd, end int64) *graphScan {
	return &graphScan{start: start, targetEnd: targetEnd, end: end, next: a.windowURL(start, end), links: make(map[[32]byte]struct{}), ids: make(map[[16]byte]struct{})}
}
func (a *DefenderAdapter) scanWindow(ctx context.Context, g *defenderGeneration, start, end int64, replay bool, requests *graphRequests) (int64, error) {
	slot := &g.liveScan
	pageBudget := 24
	if replay {
		slot = &g.replayScan
		pageBudget = 6
	}
	if *slot == nil || (*slot).start != start || (*slot).targetEnd != end {
		*slot = a.newScan(start, end, end)
	}
	work := *slot
	for used := 0; ; {
		if used >= pageBudget {
			return start, errYield
		}
		key := sha256.Sum256([]byte(work.next))
		_, repeated := work.links[key]
		incomplete := repeated || len(work.links) >= 128
		if !incomplete {
			page, err := a.requestPage(ctx, work.next, replay, requests)
			if err != nil {
				return start, err
			}
			used++
			if page.Count != nil && *page.Count > work.expected {
				work.expected = *page.Count
			}
			for _, item := range page.Items {
				if err := a.shipItem(ctx, g, item, start, work.end); err != nil {
					return start, err
				}
				id, _ := item["id"].(string)
				work.ids[eventDigest(a.sourceFingerprint(), id)] = struct{}{}
				if len(work.ids) > liveIDLimit {
					return start, errPressure
				}
			}
			work.links[key] = struct{}{}
			if page.Next != "" {
				work.next = page.Next
				continue
			}
			if len(page.Items) < 1000 && int64(len(work.ids)) >= work.expected {
				*slot = nil
				return work.end, nil
			}
		}
		// A full terminal page/count shortfall/cycle is not complete. Narrow the
		// same fixed interval; retain this smaller attempt across scheduling yields.
		if work.end-start <= int64(graphTimeQuantum) {
			*slot = a.newScan(start, end, work.end)
			return start, errIncomplete
		}
		midpoint := start + (work.end-start)/2
		midpoint -= midpoint % int64(graphTimeQuantum)
		if midpoint <= start {
			*slot = a.newScan(start, end, work.end)
			return start, errIncomplete
		}
		a.issue("interval_split")
		work = a.newScan(start, end, midpoint)
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
	g.serial++
	g.position.IDs = append([]savedID(nil), g.recent...)
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
func (a *DefenderAdapter) collect(ctx context.Context, g *defenderGeneration) {
	if !utils.CheckpointWait(ctx, a.pollInterval) {
		return
	}
	requests := &graphRequests{}
	nextReplay := time.Now().Add(a.runtime.reconcileInterval)
	if g.position.Replay != 0 {
		nextReplay = time.Now()
	}
	for ctx.Err() == nil {
		now := a.runtime.now().UTC()
		// Expiry describes a creation-time horizon, independent of the saved cap.
		floor := now.Add(-historyHorizon).UnixNano()
		if floor > g.position.Coverage && floor < g.position.Boundary && g.position.Replay == 0 {
			g.position.Coverage = floor
		}
		liveEnd := g.position.WindowEnd
		didWork := false
		if liveEnd <= now.Add(-a.runtime.settle).UnixNano() {
			end, err := a.scanWindow(ctx, g, g.position.Boundary, liveEnd, false, requests)
			if errors.Is(err, errYield) {
				didWork = true
			} else if err != nil {
				if !a.collectionFailure(ctx, err) {
					return
				}
			} else {
				if err = a.complete(g, end, false); err != nil {
					a.issue("candidate_rejected")
					return
				}
				requests.failureStart = time.Time{}
				requests.reported = false
				didWork = true
			}
		}
		// One small reconciliation chunk per live chunk, at most 6 requests/minute.
		// Rechecks use complete retained ID coverage, never a broad missing-state replay.
		if !time.Now().Before(nextReplay) {
			if g.position.Replay == 0 {
				start := g.position.Coverage
				if start < g.position.Boundary {
					g.position.Replay, g.position.ReplayEnd = start, g.position.Boundary
					if err := a.complete(g, g.position.Boundary, false); err != nil {
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
				end, err := a.scanWindow(ctx, g, g.position.Replay, end, true, requests)
				if errors.Is(err, errYield) {
					didWork = true
				} else if err != nil {
					if !a.collectionFailure(ctx, err) {
						return
					}
				} else {
					if err = a.complete(g, end, true); err != nil {
						a.issue("candidate_rejected")
						return
					}
					requests.failureStart = time.Time{}
					requests.reported = false
					didWork = true
					if g.position.Replay == 0 {
						nextReplay = time.Now().Add(a.runtime.reconcileInterval)
					}
				}
			}
		}
		if !didWork && !utils.CheckpointWait(ctx, a.pollInterval) {
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
	return utils.CheckpointWait(ctx, a.pollInterval)
}
