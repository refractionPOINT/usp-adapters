# Adapter patterns — copy from an existing adapter

Don't work from snippets — **find the existing adapter closest to your source
and copy its patterns.** The code in the repo is the source of truth and stays
current. Adapters pull from very different places (raw HTTP, vendor SDKs,
sockets, files, mailboxes), so there is no single template — but they all expose
the **same contract** to the rest of the system.

## The universal contract (every adapter)

Whatever the source, an adapter:

- Lives in its own package `usp_myadapter` under `myadapter/` (usually just
  `client.go`; the well-tested ones add `*_test.go` and sometimes split out an
  `api.go`).
- Defines a config struct that **embeds `uspclient.ClientOptions`** as
  `client_options` (json+yaml tags on every field) and usually a `Validate()`
  that fills defaults.
- Exposes `func NewMyAdapter(ctx context.Context, conf MyAdapterConfig)
  (*MyAdapter, chan struct{}, error)` and a `Close() error`. The returned
  `chan struct{}` is closed when the adapter stops on its own.
- Reads from the source however that source works, and **ships each event
  verbatim** as a `protocol.DataMessage{JsonPayload, EventType, TimestampMs}`
  through the usp client (`Ship` → `Drain`/`Close` on shutdown). LimaCharlie
  does parsing/field-mapping in the cloud — don't reshape payloads.

Open any two adapters and you'll see this contract; everything else is
source-specific.

## Pick the closest example, then copy it

| Your source is… | Read & copy | Notes |
|---|---|---|
| **HTTP API you poll** (REST, paginated) | `threatlocker/`, `1password/`, `gmail/` | The most thorough examples. `threatlocker/` has a clean `api.go`/`client.go` split, dedup, retries, and a full mock-backed test suite. `1password/` shows cursor/`has_more` paging. |
| **A vendor SDK / cloud client** | `s3/` (AWS SDK), `sqs/` (AWS SDK), `pubsub/` + `gcs/` + `bigquery/` (GCP SDK), `azure_event_hub/` (Azure SDK) | No hand-rolled HTTP — you drive the SDK. Copy how it constructs the SDK client from config credentials and pumps results into `Ship`. |
| **A streaming socket / push** | `syslog/`, `azure_event_hub/` | Long-lived connection, events arrive continuously rather than by polling. |
| **A mailbox** | `imap/`, `gmail/` | IMAP library vs. Gmail REST. |
| **Local files / OS sources** | `file/`, `evtx/`, `wel/`, `mac_unified_logging/`, `stdin/` | Tail a file, read Windows event logs, read stdin. |
| **Another HTTP vendor (auth variety)** | `okta/`, `sophos/`, `defender/`, `mimecast/`, `wiz/` | More auth/pagination variations to mine. |

Grep the chosen adapter end to end and replicate its structure for yours. If two
examples are close, read both.

## What the well-tested adapters do (worth imitating regardless of source)

These come from `threatlocker/` and `gmail/`, the strongest test exemplars:

- **Config + `Validate()`** — embed `ClientOptions`, validate it first, then fill
  every default. (`ThreatLockerConfig` / `ThreatLockerConfig.Validate`.)
- **A test seam for the external dependency.** `threatlocker` defines a small
  `uspSink` interface (the subset of `*uspclient.Client` it uses) and an
  unexported `newThreatLockerAdapter(ctx, conf, sink)` so tests inject an
  in-memory sink instead of a real LimaCharlie connection. The *same idea*
  generalizes to any source: put the external client behind a small interface so
  a fake can stand in. For HTTP that fake is an `httptest` server; for an SDK it's
  a fake implementing the interface you call.
- **Lifecycle** — `utils.NewEvent()` stop signal, a `sync.WaitGroup` for
  goroutines, a `chStopped` closed when they exit, a `sync.Once`-guarded
  idempotent `Close()` that drains and closes the usp client and the source
  client. (`ThreatLockerAdapter` / its `Close`/`runFeed`.)
- **Exactly-once shipping** — when a source re-delivers (re-polled pages,
  at-least-once queues), dedup with `utils.NewLocalDeduper` keyed by a stable
  record id (configured field → common fields → content hash). See
  `recordID`/`dedupeKey`, and the long comment on `pollFeed` explaining why it
  re-walks pages rather than stopping early.
- **Robust transport (HTTP case)** — `HTTPError{StatusCode,...}`,
  `isTransientError` (retry 5xx/429/network; not other 4xx or ctx-cancel),
  exponential backoff, and **stop the adapter on 401/403**. (`threatlocker/api.go`.)
- **JSON fidelity** — decode records with `utils.UnmarshalCleanJSON` (preserves
  integer precision) into `utils.Dict`; extract fields with `FindOneString` /
  `FindInt`.
- **Backpressure** — on `uspclient.ErrorBufferFull`, re-`Ship` with a long
  timeout. (`ThreatLockerAdapter.ship`.)

## Testing the mock / fake (see `threatlocker/mock_test.go`, `gmail/mock_test.go`)

The point of the test seam is end-to-end tests **without live credentials**:

- `captureSink` — an in-memory `uspSink` recording shipped messages to assert on.
- `mockThreatLocker` + `handler` — an `httptest` server that **reproduces the
  real API**: checks auth, paginates over an in-memory dataset the way the real
  API does, returns the real envelope shape. For an SDK-based adapter, the
  equivalent is a fake implementing the interface your adapter calls.
- Realistic fixtures (`realisticApprovalRequest`) shaped like real payloads —
  nested objects, arrays, mixed scalar types, real id/timestamp fields.
- Tests to mirror: all records ship (verbatim payload + parsed timestamp + event
  type), re-poll doesn't re-ship, a mid-run record ships once, multi-page is
  fully walked, multiple types tagged correctly, bad auth stops + ships nothing.

`testClientOptions(t)` in `threatlocker/client_test.go` shows the test
`ClientOptions` to use (`TestSinkMode: true` + log callbacks).

## Non-negotiables

- Ship payloads **verbatim**.
- **Auth failure stops the adapter**; transient errors retry.
- If the source can re-deliver, **dedup so each event ships once**.
- The **mock/fake mirrors the real source shape** so tests stay meaningful
  without live credentials.
