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

// Leave room for the embedder's identity, fencing and version envelope.
const maxResumeBytes = 6 * 1024
const recoveryHorizon = 3 * time.Hour

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
	p, err := decodePosition(raw)
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
	u := *endpoint
	u.RawQuery = p.Query
	a := OktaAdapter{endpoint: endpoint}
	if a.validateContinuation(u.String()) != nil {
		return errors.New("invalid saved continuation")
	}
	c.prepared.position = oktaPosition{Next: u.String(), Since: p.Since}
	c.prepared.replay = string(raw) != string(c.prepared.initial)
	c.prepared.ready = true
	return nil
}

func decodePosition(raw []byte) (savedPosition, error) {
	var p savedPosition
	if len(raw) > maxResumeBytes || json.Unmarshal(raw, &p) != nil || p.Version != 1 || p.Observed.IsZero() || len(p.Query) == 0 {
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
	if err != nil || len(raw) > maxResumeBytes {
		return nil, errors.New("resume position exceeds bound")
	}
	return append(marker, raw...), nil
}

func dedupKey(id [16]byte) string { return "okta/system-log/v1/" + hex.EncodeToString(id[:]) }

// Called only by the producer, or after it has joined. Pending reservations
// remain local dependencies and are never shared as evidence of delivery.
func (a *OktaAdapter) flushDedup(ctx context.Context, g *oktaGeneration) {
	if a.conf.ResumeState == nil || a.conf.ResumeDeduper == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	keys := make([]string, 0, 1000)
	ids := make([][16]byte, 0, 1000)
	flush := func() {
		err := a.conf.ResumeDeduper.Add(ctx, a.conf.ResumeNamespace, keys)
		if err == nil {
			for _, id := range ids {
				entry := g.live[id]
				entry.shared = true
				g.live[id] = entry
			}
		}
		keys, ids = keys[:0], ids[:0]
	}
	confirmed := g.confirmed.Load()
	for id, entry := range g.live {
		if entry.page <= confirmed && !entry.shared {
			keys = append(keys, dedupKey(id))
			ids = append(ids, id)
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
	if a.conf.ResumeState == nil || a.conf.ResumeDeduper == nil {
		return out
	}
	keys := make([]string, 0, len(items))
	ids := make([][16]byte, 0, len(items))
	for _, item := range items {
		if id, _ := item["uuid"].(string); id != "" {
			digest := eventDigest(id)
			keys = append(keys, dedupKey(digest))
			ids = append(ids, digest)
		}
	}
	if len(keys) == 0 {
		return out
	}
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	seen, err := a.conf.ResumeDeduper.Check(opCtx, a.conf.ResumeNamespace, keys)
	if err != nil || len(seen) != len(keys) {
		return out
	}
	for i, ok := range seen {
		if ok {
			out[ids[i]] = true
		}
	}
	return out
}
