package usp_entraid

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
const graphWindowLayout = "2006-01-02T15:04:05.0000000Z"

var (
	errRetry      = errors.New("Entra API temporarily unavailable")
	errVendor     = errors.New("Entra API rejected credentials, permissions or request")
	errPage       = errors.New("Entra page invalid")
	errYield      = errors.New("Entra request budget yielded")
	errPressure   = errors.New("Entra live history at capacity")
	errIncomplete = errors.New("Entra interval completeness unproven")
)

type entraRuntime struct {
	now                                                                   func() time.Time
	start                                                                 int64 // test-only initial clock; zero uses constructor time
	window, replayWindow, reconcileInterval, failureLimit, requestTimeout time.Duration
	liveSpacing, replaySpacing, globalSpacing                             time.Duration
	liveBudget, replayBudget                                              int
	saveInterval                                                          func() time.Duration
}

func defaultRuntime() entraRuntime {
	return entraRuntime{now: time.Now, window: 30 * time.Second, replayWindow: 15 * time.Minute, reconcileInterval: 15 * time.Minute, failureLimit: time.Hour, requestTimeout: 30 * time.Second,
		liveSpacing: time.Minute / 24, replaySpacing: time.Minute / 6, globalSpacing: time.Minute / 30, liveBudget: 24, replayBudget: 6,
		saveInterval: func() time.Duration {
			return 30*time.Second + time.Duration(time.Now().UnixNano()%int64(3*time.Second))
		}}
}
func collectionEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.Host == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" || (u.Scheme != "https" && u.Scheme != "http") || u.RawQuery != "" {
		return nil, errors.New("invalid Entra endpoint")
	}
	return u, nil
}
func (a *EntraIDAdapter) validateContinuation(raw string, stream entraStream) error {
	u, err := url.Parse(raw)
	base, _ := url.Parse(a.conf.graphEndpoint() + stream.path)
	if len(raw) > 65536 || err != nil || u == nil || base == nil || u.User != nil || u.Fragment != "" || u.Opaque != "" || !strings.EqualFold(u.Scheme, base.Scheme) || !strings.EqualFold(u.Host, base.Host) || u.EscapedPath() != base.EscapedPath() {
		return errPage
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return errPage
	}
	for key := range q {
		switch strings.ToLower(key) {
		case "access_token", "token", "authorization", "client_secret", "api_key":
			return errPage
		}
	}
	return nil
}
func (a *EntraIDAdapter) windowURL(stream entraStream, start, end int64) string {
	u, _ := url.Parse(a.conf.graphEndpoint() + stream.path)
	q := u.Query()
	q.Set("$filter", fmt.Sprintf("%s ge %s and %s lt %s", stream.tsField, time.Unix(0, start).UTC().Format(graphWindowLayout), stream.tsField, time.Unix(0, end).UTC().Format(graphWindowLayout)))
	if stream.orderBy != "" {
		q.Set("$orderby", stream.orderBy)
	}
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
func (a *EntraIDAdapter) doJSON(ctx context.Context, method, raw, body, bearer string, out interface{}) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, a.runtime.requestTimeout)
	defer cancel()
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
	// Status and minimum vendor delay take precedence over any body read, even
	// for truncated, oversized or indefinitely streaming throttling responses.
	delay := retryAfter(resp.Header.Get("Retry-After"), a.runtime.now())
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		return delay, errRetry
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return delay, errVendor
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
	serial                                         uint64
	liveNext, replayNext, globalNext, blockedUntil time.Time
	liveCalls, replayCalls                         []time.Time
	episodes                                       map[string]*failureEpisode
	permanentNext                                  map[string]time.Time
}
type requestFailure struct {
	retryAt time.Time
	delay   time.Duration
	// Invalid data pins this traversal, even when other lanes remain available.
	invalid failureEpisode
}
type failureEpisode struct {
	since    time.Time
	reported bool
}

func (s *graphRequests) episode(stream string) *failureEpisode {
	if s.episodes == nil {
		s.episodes = make(map[string]*failureEpisode)
	}
	if s.episodes[stream] == nil {
		s.episodes[stream] = &failureEpisode{}
	}
	return s.episodes[stream]
}

func retainMinute(calls []time.Time, now time.Time) []time.Time {
	n := 0
	for _, t := range calls {
		if t.After(now.Add(-time.Minute)) {
			calls[n] = t
			n++
		}
	}
	return calls[:n]
}
func (a *EntraIDAdapter) reserveRequest(replay bool, state *graphRequests) bool {
	now := a.runtime.now()
	if now.Before(state.blockedUntil) || now.Before(state.globalNext) {
		return false
	}
	state.liveCalls = retainMinute(state.liveCalls, now)
	state.replayCalls = retainMinute(state.replayCalls, now)
	calls, next, spacing, limit := &state.liveCalls, &state.liveNext, a.runtime.liveSpacing, a.runtime.liveBudget
	if replay {
		calls, next, spacing, limit = &state.replayCalls, &state.replayNext, a.runtime.replaySpacing, a.runtime.replayBudget
	}
	if now.Before(*next) || len(*calls) >= limit {
		return false
	}
	state.serial++
	*calls = append(*calls, now)
	*next = now.Add(spacing)
	state.globalNext = now.Add(a.runtime.globalSpacing)
	return true
}
func (a *EntraIDAdapter) requestPage(ctx context.Context, stream entraStream, raw string, replay bool, state *graphRequests, failure *requestFailure, validate ...func(graphPage) error) (graphPage, error) {
	var page graphPage
	if a.validateContinuation(raw, stream) != nil {
		return page, errPage
	}
	now := a.runtime.now()
	if now.Before(failure.retryAt) || now.Before(state.permanentNext[stream.name]) || !a.reserveRequest(replay, state) {
		return page, errYield
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	form := url.Values{"client_id": {a.conf.ClientID}, "client_secret": {a.conf.ClientSecret}, "scope": {a.conf.scope()}, "grant_type": {"client_credentials"}}
	delay, err := a.doJSON(ctx, http.MethodPost, a.conf.tokenURL(), form.Encode(), "", &token)
	if delay > 0 {
		state.blockedUntil = a.runtime.now().Add(delay)
	}
	// A delay on a successful token exchange must also postpone the collection.
	// Keep no bearer token in persistent state; retry the exchange after the delay.
	if err == nil && token.AccessToken == "" {
		err = errPage
	}
	if err == nil && delay > 0 {
		return page, errYield
	}
	if err == nil {
		delay, err = a.doJSON(ctx, http.MethodGet, raw, "", token.AccessToken, &page)
		if delay > 0 {
			state.blockedUntil = a.runtime.now().Add(delay)
		}
	}
	if ctx.Err() != nil {
		return graphPage{}, ctx.Err()
	}
	if err == nil {
		if page.Items == nil || len(page.Items) > 1000 || (page.Count != nil && *page.Count < 0) || (page.Next != "" && a.validateContinuation(page.Next, stream) != nil) {
			err = errPage
		}
		if err == nil {
			for _, item := range page.Items {
				id, ok := item["id"].(string)
				stamp, okTime := item[stream.tsField].(string)
				_, parseErr := time.Parse(time.RFC3339Nano, stamp)
				if !ok || id == "" || !okTime || parseErr != nil {
					err = errPage
					break
				}
			}
		}
		if err == nil {
			for _, check := range validate {
				if check(page) != nil {
					err = errPage
					break
				}
			}
		}
	}
	if err == nil {
		*failure = requestFailure{}
		// A real page from either lane proves this collection is available.
		// The other lane's retry deadline/backoff remains independent.
		*state.episode(stream.name) = failureEpisode{}
		return page, nil
	}
	if errors.Is(err, errVendor) {
		if delay < a.pollInterval {
			delay = a.pollInterval
		}
		failure.retryAt = a.runtime.now().Add(delay)
		if state.permanentNext == nil {
			state.permanentNext = make(map[string]time.Time)
		}
		state.permanentNext[stream.name] = failure.retryAt
		if a.conf.ClientOptions.OnError != nil {
			a.conf.ClientOptions.OnError(errVendor)
		}
		return graphPage{}, errVendor
	}
	if !errors.Is(err, errRetry) && !errors.Is(err, errPage) {
		return graphPage{}, err
	}
	now = a.runtime.now()
	episode := state.episode(stream.name)
	if errors.Is(err, errPage) {
		episode = &failure.invalid
	}
	if episode.since.IsZero() {
		episode.since = now
	}
	if !episode.reported && now.Sub(episode.since) >= a.runtime.failureLimit {
		if a.conf.ClientOptions.OnError != nil {
			a.conf.ClientOptions.OnError(errors.New("Entra collection unavailable or invalid for a continuous retry interval"))
		}
		episode.reported = true
	}
	if failure.delay == 0 {
		failure.delay = time.Second
	}
	if delay < failure.delay {
		delay = failure.delay
	}
	// Positive jitter never shortens Retry-After; retries remain schedulable so a
	// reconciliation outage cannot monopolize the live collector's goroutine.
	jitter := time.Duration(time.Now().UnixNano() % int64(delay/10+1))
	failure.retryAt = now.Add(delay + jitter)
	if failure.delay < 30*time.Second {
		failure.delay *= 2
		if failure.delay > 30*time.Second {
			failure.delay = 30 * time.Second
		}
	}
	a.issue("vendor_retry")
	return graphPage{}, err
}

func (a *EntraIDAdapter) shipItem(ctx context.Context, g *entraGeneration, stream entraStream, item map[string]interface{}, start, end int64) error {
	id, ok := item["id"].(string)
	stamp, okTime := item[stream.tsField].(string)
	created, err := time.Parse(time.RFC3339Nano, stamp)
	if !ok || id == "" || !okTime || err != nil {
		return errPage
	}
	t := created.UnixNano()
	if start < g.position[stream.name].Coverage {
		return errPressure
	}
	if t < start || t >= end {
		return nil
	}
	digest := eventDigest(a.sourceFingerprint(), stream.name, id)
	entry, exists := g.live[digest]
	if !exists {
		if len(g.live) >= liveIDLimit {
			a.reclaimExpiredHistory(g)
			// Reclamation may shorten an unfinished historical interval. Abort
			// its old page before admitting/suppressing anything outside coverage.
			if start < g.position[stream.name].Coverage || len(g.live) >= liveIDLimit {
				return errPressure
			}
		}
		msg := &protocol.DataMessage{JsonPayload: item, TimestampMs: uint64(time.Now().UnixMilli())}
		var dep utils.ACKDependency
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			dep, err = a.tracker.Ship(msg, 250*time.Millisecond)
			if !errors.Is(err, uspclient.ErrorBufferFull) {
				break
			}
			if !utils.CheckpointWait(ctx, 10*time.Millisecond) {
				return ctx.Err()
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			a.issue("admission_failed")
			return err
		}
		// Collection is serialized. A failed/in-flight Ship is never present in
		// its collection-qualified dedupe map.
		entry = liveID{dependency: dep, serial: g.serial + 1, expiry: t + int64(historyHorizon), stream: streamCode(stream.name)}
		g.live[digest] = entry
		g.remember(stream.name, savedID{digest: digest, expiry: entry.expiry})
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

type graphScan struct {
	start, end, targetEnd int64
	next                  string
	links                 map[[32]byte]struct{}
	ids                   map[[16]byte]struct{}
	expected              int64
	failure               requestFailure
	page                  *graphPage
	offset                int
}

func (a *EntraIDAdapter) newScan(stream entraStream, start, target, end int64) *graphScan {
	return &graphScan{start: start, end: end, targetEnd: target, next: a.windowURL(stream, start, end), links: make(map[[32]byte]struct{}), ids: make(map[[16]byte]struct{})}
}

// Each scheduling turn handles at most one bounded page. Continuations live only
// in memory; splitting and yielding never advance the complete source boundary.
func (a *EntraIDAdapter) scanWindow(ctx context.Context, g *entraGeneration, stream entraStream, start, end int64, replay bool, requests *graphRequests) (int64, error) {
	lane := "live"
	if replay {
		lane = "replay"
	}
	return a.scanLane(ctx, g, stream, start, end, lane, requests)
}
func (a *EntraIDAdapter) scanLane(ctx context.Context, g *entraGeneration, stream entraStream, start, end int64, lane string, requests *graphRequests) (int64, error) {
	key := stream.name + "/" + lane
	if start < g.position[stream.name].Coverage {
		return start, errPressure
	}
	work := g.scans[key]
	if work == nil || work.start != start || work.targetEnd != end {
		work = a.newScan(stream, start, end, end)
		g.scans[key] = work
	}
	hash := sha256.Sum256([]byte(work.next))
	_, repeated := work.links[hash]
	incomplete := repeated || len(work.links) >= 128
	if !incomplete {
		if work.page == nil {
			page, err := a.requestPage(ctx, stream, work.next, lane == "replay", requests, &work.failure, func(page graphPage) error {
				if page.Next != "" {
					nextHash := sha256.Sum256([]byte(page.Next))
					if _, seen := work.links[nextHash]; seen || nextHash == hash {
						return errPage
					}
				}
				return nil
			})
			if err != nil {
				return start, err
			}
			work.page = &page
			if page.Count != nil && *page.Count > work.expected {
				work.expected = *page.Count
			}
		}
		page := work.page
		for work.offset < len(page.Items) {
			if ctx.Err() != nil {
				return start, ctx.Err()
			}
			item := page.Items[work.offset]
			if err := a.shipItem(ctx, g, stream, item, start, work.end); err != nil {
				return start, err
			}
			id, _ := item["id"].(string)
			work.ids[eventDigest(a.sourceFingerprint(), stream.name, id)] = struct{}{}
			work.offset++ // only a successful admission/suppression consumes this item
			if len(work.ids) > 128000 {
				return start, errIncomplete
			}
		}
		work.page = nil
		work.offset = 0
		work.links[hash] = struct{}{}
		if page.Next != "" {
			work.next = page.Next
			return start, errYield
		}
		if len(page.Items) < 1000 && int64(len(work.ids)) >= work.expected {
			delete(g.scans, key)
			return work.end, nil
		}
	}
	if work.end-start <= int64(graphTimeQuantum) {
		g.scans[key] = a.newScan(stream, start, end, work.end)
		return start, errIncomplete
	}
	midpoint := start + (work.end-start)/2
	midpoint -= midpoint % int64(graphTimeQuantum)
	if midpoint <= start {
		return start, errIncomplete
	}
	g.scans[key] = a.newScan(stream, start, end, midpoint)
	a.issue("interval_split")
	return start, errYield
}
func (a *EntraIDAdapter) complete(g *entraGeneration, stream entraStream, end int64, replay bool) error {
	s := g.position[stream.name]
	if replay {
		s.Replay = end
		if end >= s.ReplayEnd {
			s.Replay, s.ReplayEnd = 0, 0
			if stream.name != "risk_detections" {
				s.Deep = !s.Deep
			}
		}
	} else {
		s.Boundary = end
		if end >= s.WindowEnd {
			s.WindowEnd = end + int64(a.runtime.window)
		}
	}
	g.position[stream.name] = s
	return a.candidate(g)
}
func (a *EntraIDAdapter) collectionFailure(ctx context.Context, err error) bool {
	if ctx.Err() != nil || errors.Is(err, uspclient.ErrorClosed) {
		return false
	}
	switch {
	case errors.Is(err, errYield), errors.Is(err, errRetry), errors.Is(err, errVendor):
		return true
	case errors.Is(err, errPressure):
		a.issue("live_history_pressure")
	case errors.Is(err, errIncomplete):
		a.issue("interval_stalled")
	case errors.Is(err, errPage):
		a.issue("invalid_page")
	default:
		a.issue("collection_stalled")
	}
	return true
}
