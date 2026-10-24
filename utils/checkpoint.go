package utils

import "context"

// CheckpointFence identifies one exact stored generation. The zero fence means
// the key must be missing; existing revisions and nonces must be nonempty.
type CheckpointFence struct {
	Revision string
	Nonce    string
}

// Checkpoint is source-neutral restart state. Position is an opaque, complete
// source snapshot, never a local USP sequence number. Its encoding and schema
// validation belong to the adapter. SourceFingerprint is a digest of nonsecret
// source identity only: credential rotation must not change it. Never include
// credentials in Position, tokens, or fingerprint input.
type Checkpoint struct {
	SchemaVersion     uint32
	SourceFingerprint [32]byte
	Position          []byte
	CheckpointFence
	Attempt string
}

// Clone gives the caller independent ownership of the opaque position.
func (c Checkpoint) Clone() Checkpoint {
	c.Position = append([]byte(nil), c.Position...)
	return c
}

// CheckpointStore is optionally supplied by the caller, not customer config.
// Implementations must honor cancellation, be safe for concurrent calls, copy
// values at their boundary, and impose their own encoded-size/resource limits.
// No store is selected by this package; nil preserves the adapter's startup
// behavior. A caller must not interpret an unavailable read as missing state.
//
// Load returns found=false, err=nil ONLY for a missing key. Other failures,
// including unavailable or unreadable state, return an error.
//
// CompareAndSwap atomically replaces the complete value only if its fence
// matches expected (a zero fence requires absence). false,nil means conflict;
// an error can mean an unknown write outcome. Each new attempt uses a unique
// nonempty Attempt and Revision; Nonce identifies the writer instance. If that
// EXACT attempt/value is still stored, retrying must return true without another
// write, even though expected no longer matches. Once superseded, the normal
// fence conflict applies; no token history is required. Reusing a token for
// different state must fail. Readback of the exact attempted fence AND attempt
// resolves a lost reply; unavailable readback leaves the outcome unknown.
//
// A new writer first claims a fresh nonce via CAS without advancing positions.
// After conflict discard stale candidates, reload and claim again; never rebase
// an old candidate onto a new fence. Source changes replace this same slot's
// generation rather than resurrecting old source keys. Stop/join store work on
// lease loss. Store failures are operational errors, not vendor failures.
type CheckpointStore interface {
	Load(context.Context) (value Checkpoint, found bool, err error)
	CompareAndSwap(ctx context.Context, expected CheckpointFence, next Checkpoint) (applied bool, err error)
}
