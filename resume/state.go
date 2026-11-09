// Package resume defines optional persistence and confirmed-delivery deduplication
// hooks for embedders. Adapters without injected state retain their defaults.
package resume

import "context"

// State owns compatible, fenced progress for one source incarnation. Restore
// must durably initialize initial if no compatible position exists, before it
// returns. Storage read retries, source compatibility and ownership are the
// implementation's responsibility. Neither argument nor result may be retained
// without copying. Confirm copies a source-complete, remotely confirmed position
// into an immutable in-memory snapshot; it performs no IO. The embedder saves
// snapshots periodically and after adapter Close, before releasing ownership.
// Methods must be safe for concurrent use. ACKs represent the peer's acceptance
// contract, including any explicit rejection, not durable downstream processing.
type State interface {
	Restore(context.Context, []byte) ([]byte, error)
	Confirm([]byte)
}

// Deduper stores only confirmed delivery markers. Check returns one result per
// key, in order. Add is invoked after validated confirmation, never admission.
// Adapters attempt Add at most once per newly confirmed local reservation;
// a failed attempt is not retried and repeated confirmation does not renew TTL.
// Failures permit duplicates. Implementations must bound retention and obey ctx.
// Methods finish using key slices before returning; retain only copied keys.
// namespace is a stable hashed creation/source identity; keys include feed and
// vendor identity. No payload or credential belongs in these methods.
type Deduper interface {
	Check(ctx context.Context, namespace string, keys []string) ([]bool, error)
	Add(ctx context.Context, namespace string, keys []string) error
}

// Stats receives fixed classes only: replayed, deduped, skipped, unknown.
// replayed counts records examined during restored traversal, including new
// records; it is NOT a duplicate count. skipped counts omitted recovery minutes.
// Callbacks must be concurrency safe, bounded, and must not block collection.
type Stats func(class string, count uint64)
