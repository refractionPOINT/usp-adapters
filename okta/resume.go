package usp_okta

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/refractionPOINT/usp-adapters/resume"
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
	initial       []byte
	recovery      []byte
	positionBytes []byte
	position      oktaPosition
	ready         bool
	replay        bool
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
	if c.ResumeDeduper != nil || c.ResumeNamespace != "" {
		decoded, err := hex.DecodeString(c.ResumeNamespace)
		if err != nil || len(decoded) != 32 || c.ResumeNamespace != strings.ToLower(c.ResumeNamespace) {
			return errors.New("resume namespace must be 64 lowercase hex characters")
		}
	}
	if c.prepared == nil {
		now := time.Now().UTC()
		initial, _ := json.Marshal(savedPosition{1, baselineQuery(now), now, now})
		c.prepared = &preparedResume{initial: initial}
	}
	if c.prepared.ready {
		return nil
	}
	var raw []byte
	if c.prepared.recovery == nil {
		raw, err = c.ResumeState.Restore(ctx, append([]byte(nil), c.prepared.initial...))
		if err != nil && !errors.Is(err, resume.ErrInvalidPosition) {
			return err
		}
		if err != nil || !validSavedPosition(raw, endpoint) {
			now := time.Now().UTC()
			since := now.Add(-overlapPeriod)
			c.prepared.recovery, _ = json.Marshal(savedPosition{1, baselineQuery(since), since, now})
		}
	}
	recovering := c.prepared.recovery != nil
	if recovering {
		recoverer, ok := c.ResumeState.(resume.Recoverer)
		if !ok {
			return fmt.Errorf("%w: state provider must implement Recoverer", resume.ErrInvalidPosition)
		}
		raw, err = recoverer.Recover(ctx, append([]byte(nil), c.prepared.recovery...))
		if err != nil {
			return err
		}
		if !validSavedPosition(raw, endpoint) {
			return resume.ErrInvalidPosition
		}
	}
	p, err := decodePosition(raw)
	if err != nil {
		return err
	}
	// Accumulate preparation counters only after all reads/recovery/validation
	// succeed. Retrying bad state or failed recovery never inflates them.
	var skipped uint64
	cutoff := time.Now().UTC().Add(-recoveryHorizon)
	if p.Observed.Before(cutoff) {
		skipped = uint64(cutoff.Sub(p.Observed) / time.Minute)
		p.Query = baselineQuery(cutoff)
		p.Since = cutoff
	}
	u := *endpoint
	u.RawQuery = p.Query
	c.prepared.position = oktaPosition{Next: u.String(), Since: p.Since, Observed: p.Observed}
	c.prepared.positionBytes, err = json.Marshal(p)
	if err != nil || len(c.prepared.positionBytes) > maxResumeBytes {
		return resume.ErrInvalidPosition
	}
	c.prepared.replay = recovering || string(raw) != string(c.prepared.initial)
	c.prepared.ready = true
	c.stat("skipped", skipped)
	if recovering {
		c.stat("unknown", 1)
	}
	return nil
}

func validSavedPosition(raw []byte, endpoint *url.URL) bool {
	p, err := decodePosition(raw)
	if err != nil {
		return false
	}
	canonical, err := json.Marshal(p)
	if err != nil || len(canonical) > maxResumeBytes {
		return false
	}
	u := *endpoint
	u.RawQuery = p.Query
	a := OktaAdapter{endpoint: endpoint}
	return a.validateContinuation(u.String()) == nil
}

func decodePosition(raw []byte) (savedPosition, error) {
	var p savedPosition
	if len(raw) > maxResumeBytes || json.Unmarshal(raw, &p) != nil || p.Version != 1 || p.Observed.IsZero() || len(p.Query) == 0 {
		return p, resume.ErrInvalidPosition
	}
	if _, err := url.ParseQuery(p.Query); err != nil {
		return p, resume.ErrInvalidPosition
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
		// The vendor cursor remains valid for live traversal. Only the durable
		// candidate falls back, without advancing its prior safe source boundary.
		if !g.oversizedEpisode {
			a.conf.stat("unknown", 1)
			g.oversizedEpisode = true
		}
		raw = g.persistable
		if len(raw) == 0 {
			raw = a.conf.prepared.positionBytes
		}
		if len(raw) == 0 || len(raw) > maxResumeBytes {
			return nil, resume.ErrInvalidPosition
		}
	} else {
		g.persistable = append(g.persistable[:0], raw...)
		g.oversizedEpisode = false
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
