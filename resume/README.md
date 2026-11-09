# Optional adapter progress

Okta supports embedding with `OktaConfig.ResumeState`. Call `conf.Prepare(ctx)`
before `NewOktaAdapter(ctx, conf)`, outside short constructor deadlines. Preparation
restores compatible state or durably initializes one fixed startup baseline.
Failed preparation can be retried on the same config without moving that baseline.
A prepared config belongs to one acquisition: create and prepare a new config when
replacing an adapter, even for credential-only edits.

The state provider owns source/creation identity, compatibility, ownership fencing,
continuity across previous collectors, internal read retries, periodic saves and a
bounded final save after adapter Close and before releasing ownership. Credential
rotation retains compatible progress; actual source changes must not inherit an
unrelated cursor. Explicit disable uses a fresh baseline; automatic failures may
recover. Unknown predecessor capability requires a conservative fresh baseline
and a counted discontinuity. These decisions must occur inside Restore, before
polling starts. Adapters without state injection retain their startup behavior.

`Confirm` copies a complete remotely confirmed source position into memory without
IO. Save this snapshot rather than inspecting a running producer. A drain failure
still permits publishing an already confirmed prefix. Successful client admission,
failed requests, incomplete pages and unacknowledged records never advance it.
Okta's polling continuation is preserved, including after empty pages. Event
publication timestamps are not used as a source completeness watermark.

Positions contain bounded opaque continuation query state and timestamp fallback,
not payloads, credentials or event-ID lists. The vendor blob is limited to 6 KiB,
leaving overhead for an 8 KiB enclosing identity/version envelope. A stale saved
observation beyond three hours starts at that recovery boundary, counting omitted
minutes. Rejected cursors fall back inclusively to the stored boundary, capped at
three hours; absent safe timestamps use a thirty-minute overlap and count unknown
history. A startup timestamp more than thirty minutes behind the last successful
page observation is no longer a recent cursor fallback. If that cursor expires,
use the same short unknown-history overlap; do not count previously collected
startup history as skipped. Neither event publication times nor page observation
times establish a new source completeness boundary. Historical completeness
remains bounded by vendor availability, retention
and late publication. An opaque cursor's source age cannot be inferred from event
publication timestamps.

Optional `ResumeDeduper` checks batches before admission and adds hashed feed/UUID
markers only after validated ACK. Shared keys are exactly 64 lowercase hex
characters from the full SHA-256 digest of feed and vendor UUID. Each newly
confirmed local ID gets at most one bounded Add attempt; neither repeat
confirmation nor a failed Add renews its marker TTL. Provide a stable **hashed**
creation/source
identity in `ResumeNamespace`; never use credentials or raw customer identifiers.
Use a versioned namespace isolated from any admission-time markers. Bound marker
retention to the thirty-minute overlap and budget backend capacity before use.
Dedup failure permits duplicates. Backend operations have a 250 ms budget and
failures enter a shared thirty-second cooldown. The adapter scans pending markers
only when the confirmed page frontier advances, not on every timer tick. Pending
duplicates retain the original local
admission dependency; they are never shared as delivery evidence. Local history
and page dependency tracking are bounded; checkpoints do not retain event lists.

`ResumeStats` receives only fixed classes: `replayed` counts valid records examined
on restored traversal up to its first successful empty page, including new records
(it is not proof of duplicate delivery); `deduped` counts confirmed shared-marker
suppression; `skipped` counts whole recovery minutes omitted by a cap; `unknown`
counts rejected-cursor fallback episodes. A host counts its own continuity breaks.
Callbacks must be bounded and concurrency safe. Backend failures must not become
customer configuration errors.

ACK means the deployed peer's terminal acceptance contract, potentially including
explicit rejection; it does not prove durable downstream storage or Output
completion. A crash between acceptance and saving can replay records. Exact-once
delivery is not promised. The embedder must ensure its peer confirms the required
acceptance stage before enabling saved progress.
