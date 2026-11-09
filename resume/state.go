// Package resume defines optional persistence and confirmed-delivery deduplication
// hooks for embedders. Adapters without injected state retain their defaults.
package resume

import (
	"context"
	"errors"
)

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

// ErrInvalidPosition identifies invalid/incompatible position data after a
// successful authoritative storage read. Never wrap a transport, storage-read,
// cancellation or ownership error with this sentinel.
var ErrInvalidPosition = errors.New("invalid resume position")

// Recoverer optionally supports atomic durable rebaselining of invalid position
// data. Recover runs only after a known successful storage read (or Restore's
// ErrInvalidPosition), under that read's revision and ownership fence. It must
// durably establish baseline before returning valid position bytes. Retry IO
// internally; a caller retry supplies the same baseline. Do not retain slices
// without copying. Recovery must not bypass source identity or ownership checks.
type Recoverer interface {
	Recover(ctx context.Context, baseline []byte) ([]byte, error)
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
