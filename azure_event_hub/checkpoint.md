The optional Go-only `CheckpointStore` provides restart checkpoints. Both it
and the concurrent, nonblocking `CheckpointIssue` callback are excluded from
JSON and YAML.
Existing constructor and customer configuration signatures are unchanged.

Release-one startup keeps the current latest behavior when the store is nil,
state is positively missing, allocation of a missing slot is refused, or a
partition is newly discovered. There is no timestamp lookback. A fresh claim
contains an empty partition map: until the first ACK confirms an exact source
offset, a crash can still lose the unadopted first-start boundary. Choosing a
clock boundary instead would change release-one behavior.

Valid state resumes each partition exclusively after its recorded offset. The
source fingerprint uses the Azure SDK's parsed namespace, entity and existing
default consumer group, never its credentials. The adapter does not establish
ownership authority for a store supplied by its caller. The caller must bind
that store to a valid acquisition and stop the adapter before relinquishing it.

Loading/claiming happens after constructor return, before opening receivers.
Unavailable, malformed and version-incompatible same-source state waits with
cancellation. Internal checkpoint errors use fixed operational codes rather
than vendor errors. Each store operation has a two-second deadline; store
implementations must honor cancellation. Writes coalesce at a jittered 30–33
second interval and save only complete ACK-confirmed positions. An uncertain
write remains frozen while the newest confirmed complete snapshot is retained.

Each partition serializes handlers. A failed admission or invalid/partially
decoded event retires the partition and stops collection synchronously, so no
later offset crosses a failed-send hole. Source offsets are not event-ID
dedupe. Duplicate exact offset/sequence deliveries may be emitted again.

A strictly forward offset/sequence jump emits `sequence_gap` and continues
collecting the actual available event. Its source position becomes eligible
only after that event's actual cumulative ACK; earlier pending admissions stay
dependencies. This is progress beyond a source discontinuity, not proof of loss, recovery
of skipped records, or proof that retention caused the gap. There is no
newest-now or timestamp fallback for a forward gap.

A backward/inconsistent delivery pauses only that partition under its handler
lock and requests fresh partition-runtime evidence. It does not by itself
permit a reset. Unproven suspicion remains an explicit operational partition
stall; other healthy receivers continue. Initial startup checks stored
partitions using the pinned SDK's `GetPartitionInformation`. Periodic discovery
checks at most one partition per pass, rotating through known partitions;
backward notifications and unresolved reset writes take priority. Whole-hub
discovery failure supplies no partition evidence and performs no reset.
Each partition query has its own cancellable two-second context.

Only a fresh, uncached, positive partition-runtime observation with
`savedSequence > currentLastSequence` permits abandoning that partition's
previous incarnation position. The response must match the current entity and
partition, contain a positive last sequence and numeric last offset, nonzero
enqueue time, and a consistent nonnegative beginning sequence. A beginning
sequence of zero with a positive last position is not an empty partition.
The typed SDK API does not retain numeric field presence, so a default-zero
last sequence/offset, confirmed empty sentinel, missing/ambiguous fields,
equal/higher sequence, stale/cached response, error or expired query cannot
reset. Observation time refers to this management request, never event age.
The result remains bound to its source, generation, partition and checkpoint
session, and is rejected after cancellation, Close or known fence loss.

An approved reset omits ONLY that partition from the complete existing
checkpoint map, emits `partition_history_gap`, and confirms that omission
before opening latest. This is explicit abandonment of an unrecoverable old
position, not a fabricated ACK or an omission to make state fit. It uses the
existing schema/caps. If an uncertain older write resolves, its successful
resolution alone does not certify the omission: the actual committed position
must omit that partition. An unknown reset write stays pending; other live
source work can continue. An existing store's lost save fence prevents the
pending reset and all subsequent persistence, preserving the active receivers.

Once omission is confirmed, the old source context is canceled and its handler
is joined/retired under its lock. Only that receiver is replaced at latest,
with `partition_reset`; a local candidate tag rejects late old confirmations.
The same USP client, tracker, accepted pending messages and cumulative ACK
dependencies remain. New-incarnation offsets are saved only after their actual
ACKs. Before the first new ACK, the persisted omission means a fresh process
also uses R1 latest rather than resurrecting the old position. Nil/missing/new
partitions retain today's latest behavior. Reset/history-gap and inconclusive
metadata remain operational only, never vendor error callbacks.

Detecting a recreation and starting latest can lose events produced between
recreation and detection, just as today's latest start does. Detection is
also delayed by partition rotation, failed queries and uncertain checkpoint
writes. A possible LATER improvement is reading the NEW incarnation from its
earliest RETAINED event to avoid that gap, at the risk of replay after
misdetection. That behavior is not implemented. There is no earliest or
time-based reset read, broad fallback, or new customer setting.

Periodic management failures keep healthy receivers running and emit
`discovery_retry`. Each operation has a cancellable 30-second setup budget,
separate for management and for each sequential receiver. A continuous hour
of management failures escalates once; any successful management read resets
that episode. Failed new receivers have separate one-hour episodes, so healthy
management reads cannot hide an indefinitely failing receiver. Successful
receiver setup resets its episode. Initial startup failures remain fatal.
Failed partial setup is canceled, closed and retired before retry; it is never
published as a started receiver. Any exact position already admitted by a
partial receiver is retained in-process while its USP messages remain pending.

Verified vendor/SDK causes are sanitized and reported once after source work
has joined; error callbacks may call `Close`. The sanitizer retains fixed SDK
parser messages, allowlisted AMQP conditions and diagnostic categories. It
never includes raw descriptions, arbitrary causes, URLs, SAS values or event
annotations. A canceled parse handler cannot erase a verified listener cause:
cleanup snapshots the listener error before intentionally closing it.
Malformed vendor event metadata is a stop-and-report error, never a poison
skip. Cancellation without a verified vendor cause, checkpoint/tracker and
capacity failures remain operational only. An unresolved internal stop emits
`internal_stall` alongside its specific fixed issue code; it is not converted
to a vendor error or a claim of recovered history.

The Azure SDK's default memory persister remains intact for ordinary receiver
reconnects: admitted but unACKed records remain in the same USP client buffer.
A save conflict disables persistence for this adapter acquisition and emits
`ownership_conflict` once. It leaves the same receivers, partition positions,
in-flight sends, USP client, ACK tracker and pending dependencies running.
Later ACKs still advance the bounded in-memory frontier. There is no reload,
claim retry, source restart, receiver replay or further CAS, including on Close.
A fresh adapter acquisition may attempt persistence again; that acquisition's
authority remains the caller's responsibility.

The currently pinned shared `OpenCheckpoint` helper still retries initial-claim
conflicts. This adapter's tests use that base helper and do not establish
one-attempt initial-claim semantics. A separate shared-helper change is required
for that behavior; the adapter already accepts a nil session with a validated
start position. No shared helper is changed here.

The complete encoded store value is capped at 32,768 bytes; adapter positions
reserve 64 bytes for the store envelope and may use at most 32,704 bytes.
Numeric IDs with exact offsets, sequence numbers and enqueue nanoseconds use
26 bytes per partition (26,627 bytes for 1,024 partitions). Other validated
names and noncanonical decimal offsets round-trip using escaped encodings.
Snapshots never truncate or drop a partition to fit. An oversized snapshot
keeps the last complete checkpoint, retains the bounded live positions, emits
`position_size_stall`, and continues ingestion with limited restart adoption.
There are at most 4,096 tracked partitions and two coalesced candidates per
partition, independent of event count. The USP buffer retains its own limits.

Shutdown cancels collection and immediately begins USP drain before joining
source handlers. Listener/hub close operations receive a two-second deadline;
the Azure SDK does not expose a join of its internal AMQP goroutines, so late
callbacks are retired under the partition lock and cannot admit or stage data.
The SDK can still write its MemoryPersister after an old handler returns:
that post-callback write is outside the adapter lock and is not claimed joined.
For a verified reset only, the replacement partition gets a fresh Azure SDK
Hub with its own default MemoryPersister. Late old SDK writes remain confined
to the old Hub, so they cannot replace the new incarnation's reconnect state.
Other partitions keep their existing Hub and receivers. Partial setup retries
reuse the same new Hub/persister; ordinary SDK reconnect behavior is unchanged.
One replacement Hub per reset partition is retained, superseded Hubs are closed,
and the runtime partition bound includes retained reset tags. Outside this
reset isolation, a late write on an ordinary partial-setup retry can still
select an earlier admitted position; accepted messages belong to the same USP
client. The SDK's context-free connection close depends on its transport.
The adapter closes every returned partial listener. The pinned SDK can also
return a nil handle after an internal connection/setup failure; its
`newSessionAndLink` error paths do not visibly close every previously opened
resource, and `Hub.Receive` does not publish that failed receiver. Cleanup of
those unexposed resources is not established by these mocks or claimed here.
Every admission attempt is at most one second, including buffer-full retries;
retry delays prevent a hot loop. After bounded receiver cleanup, USP drains
for at most one minute, confirms and saves within one shared two-second budget,
then closes. Any unACKed buffer is reported through the existing close result.

Validation uses local hub/listener mocks and a loopback USP websocket with the
pinned USP SDK. It does not exercise an Azure AMQP service, prove retained
history availability, or constitute full pipeline testing. A scratch copy of
the actual pinned Azure decoder confirms an unchecked `Data[0]` panic on nil
or empty data-section lists before the adapter handler can run. Whether Azure
can deliver that shape was not established. An adapter handler cannot guard
that earlier dependency operation; no dependency patch is included here.
