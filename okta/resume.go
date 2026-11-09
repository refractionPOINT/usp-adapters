package usp_okta

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
)

// Position is base64-encoded inside a JSON envelope: leave room for that
// expansion plus the embedder's identity, fencing and revision metadata.
const maxResumeBytes = 4 * 1024
const recoveryHorizon = 3 * time.Hour
const dedupTimeout = 250 * time.Millisecond
const dedupBackoff = 30 * time.Second

type savedPosition struct {
	Version  int       `json:"v"`
	Query    string    `json:"q"`
	Since    time.Time `json:"since"`
	Observed time.Time `json:"observed"`
}

type preparedResume struct {
	initial  []byte
	position oktaPosition
	ready    bool
	replay   bool
}

// Prepare restores progress before construction. Call outside any short client
// construction timeout. Repeated attempts retain the same initial baseline.
// Configure all injected hooks before calling; do not mutate them afterwards.
func (c *OktaConfig) Prepare(ctx context.Context) error {
	if c.ResumeState == nil {
		return nil
	}
	endpoint, err := oktaEndpoint(c.URL)
	if err != nil {
		return err
	}
	if c.ResumeDeduper != nil && c.ResumeNamespace == "" {
		return errors.New("resume dedup namespace required")
	}
	if c.prepared == nil {
		now := time.Now().UTC()
		q := url.Values{"since": {now.Format(time.RFC3339Nano)}, "sortOrder": {"ASCENDING"}}
		raw, _ := json.Marshal(savedPosition{1, q.Encode(), now, now})
		c.prepared = &preparedResume{initial: raw}
	}
	if c.prepared.ready {
		return nil
	}
	raw, err := c.ResumeState.Restore(ctx, append([]byte(nil), c.prepared.initial...))
	if err != nil {
		return err
	}
	// Accept a valid older bounded envelope for fallback, never emit its
	// oversized cursor. The ordinary decoder/output budget stays at 4 KiB.
	p, err := decodePositionLimit(raw, 8*1024)
	if err != nil {
		return err
	}
	cutoff := time.Now().UTC().Add(-recoveryHorizon)
	if p.Observed.Before(cutoff) {
		c.stat("skipped", uint64(cutoff.Sub(p.Observed)/time.Minute))
		p.Query = baselineQuery(cutoff)
		p.Since = cutoff
		// Do not Confirm this cap: a successful page must establish its frontier.
	}
	if len(raw) > maxResumeBytes {
		p.Since = c.fallbackSince(p.Since, p.Observed, time.Now().UTC())
		p.Query = baselineQuery(p.Since)
	}
	u := *endpoint
	u.RawQuery = p.Query
	a := OktaAdapter{endpoint: endpoint}
	if a.validateContinuation(u.String()) != nil {
		return errors.New("invalid saved continuation")
	}
	c.prepared.position = oktaPosition{Next: u.String(), Since: p.Since, Observed: p.Observed}
	c.prepared.replay = string(raw) != string(c.prepared.initial)
	c.prepared.ready = true
	return nil
}

func decodePosition(raw []byte) (savedPosition, error) {
	return decodePositionLimit(raw, maxResumeBytes)
}
func decodePositionLimit(raw []byte, limit int) (savedPosition, error) {
	var p savedPosition
	if len(raw) > limit || json.Unmarshal(raw, &p) != nil || p.Version != 1 || p.Observed.IsZero() || len(p.Query) == 0 {
		return p, errors.New("invalid resume position")
	}
	if _, err := url.ParseQuery(p.Query); err != nil {
		return p, errors.New("invalid resume query")
	}
	return p, nil
}

func baselineQuery(since time.Time) string {
	return url.Values{"since": {since.UTC().Format(time.RFC3339Nano)}, "sortOrder": {"ASCENDING"}}.Encode()
}

func (c *OktaConfig) stat(class string, n uint64) {
	if n != 0 && c.ResumeStats != nil {
		c.ResumeStats(class, n)
	}
}

// fallbackSince chooses a bounded inclusive recovery request, never a proven
// completeness watermark. Unknown/skipped counters describe the limitation.
// Publishing this request as a checkpoint still requires all page dependencies.
func (c *OktaConfig) fallbackSince(since, observed, now time.Time) time.Time {
	if since.IsZero() || (!observed.IsZero() && since.Before(observed.Add(-overlapPeriod))) {
		reference := observed
		if reference.IsZero() || reference.After(now) {
			reference = now
		}
		since = reference.Add(-overlapPeriod)
	}
	cutoff := now.Add(-recoveryHorizon)
	if since.Before(cutoff) {
		c.stat("skipped", uint64(cutoff.Sub(since)/time.Minute))
		since = cutoff
	}
	c.stat("unknown", 1)
	return since
}

func (a *OktaAdapter) resumeCandidate(g *oktaGeneration) ([]byte, error) {
	marker := candidateBytes(g.id, g.page)
	if a.conf.ResumeState == nil {
		return marker, nil
	}
	u, err := url.Parse(g.position.Next)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(savedPosition{1, u.RawQuery, g.position.Since, time.Now().UTC()})
	if err != nil {
		return nil, err
	}
	if len(raw) > maxResumeBytes {
		// Keep the whole page's dependencies while replacing only its unpersistable
		// continuation. No cursor truncation and no confirmation before its ACKs.
		g.position.Since = a.conf.fallbackSince(g.position.Since, g.position.Observed, time.Now().UTC())
		u := *a.endpoint
		u.RawQuery = baselineQuery(g.position.Since)
		g.position.Next = u.String()
		raw, err = json.Marshal(savedPosition{1, u.RawQuery, g.position.Since, time.Now().UTC()})
		if err != nil || len(raw) > maxResumeBytes {
			return nil, errors.New("resume fallback exceeds bound")
		}
	}
	return append(marker, raw...), nil
}

func dedupKey(id [32]byte) string { return hex.EncodeToString(id[:]) }

// Called only by the producer, or after it has joined. Pending reservations
// remain local dependencies and are never shared as evidence of delivery.
func (a *OktaAdapter) flushDedup(ctx context.Context, g *oktaGeneration) {
	if a.conf.ResumeState == nil || a.conf.ResumeDeduper == nil {
		return
	}
	confirmed := g.confirmed.Load()
	if confirmed <= g.dedupFrontier {
		return
	}
	g.dedupFrontier = confirmed
	ctx, cancel := context.WithTimeout(ctx, dedupTimeout)
	defer cancel()
	keys := make([]string, 0, 1000)
	flush := func() {
		// A failed attempt permits replay. Never retry this reservation or renew a
		// shared marker's TTL merely because confirmation is observed again.
		if !time.Now().Before(a.dedupRetryAfter) {
			if err := a.conf.ResumeDeduper.Add(ctx, a.conf.ResumeNamespace, keys); err != nil {
				a.dedupRetryAfter = time.Now().Add(dedupBackoff)
			}
		}
		keys = keys[:0]
	}

	for id, entry := range g.live {
		if entry.page <= confirmed && !entry.sharedAttempted {
			entry.sharedAttempted = true
			g.live[id] = entry
			keys = append(keys, dedupKey(entry.sharedDigest))
			if len(keys) == cap(keys) {
				flush()
				if ctx.Err() != nil {
					return
				}
			}
		}
	}
	if len(keys) > 0 {
		flush()
	}
}

func (a *OktaAdapter) sharedDuplicates(ctx context.Context, items []utils.Dict) map[[16]byte]bool {
	out := make(map[[16]byte]bool)
	if a.conf.ResumeState == nil || a.conf.ResumeDeduper == nil || time.Now().Before(a.dedupRetryAfter) {
		return out
	}
	keys := make([]string, 0, len(items))
	ids := make([][16]byte, 0, len(items))
	for _, item := range items {
		if id, _ := item["uuid"].(string); id != "" {
			digest := eventDigest(id)
			keys = append(keys, dedupKey(sharedEventDigest(id)))
			ids = append(ids, digest)
		}
	}
	if len(keys) == 0 {
		return out
	}
	opCtx, cancel := context.WithTimeout(ctx, dedupTimeout)
	defer cancel()
	seen, err := a.conf.ResumeDeduper.Check(opCtx, a.conf.ResumeNamespace, keys)
	if err != nil || len(seen) != len(keys) {
		a.dedupRetryAfter = time.Now().Add(dedupBackoff)
		return out
	}
	for i, ok := range seen {
		if ok {
			out[ids[i]] = true
		}
	}
	return out
}
