# Adapter patterns — a reading guide

Don't copy snippets from this file — **read the real code** and imitate it. The
code in the repo is the source of truth and stays current; this file just tells
you which symbols to study and why. `threatlocker/` is the canonical
polling-HTTP-API adapter; open these files and find the named symbols.

Replace `ThreatLocker`/`threatlocker` with your names; the Go package is
`usp_myadapter`.

## `threatlocker/api.go` — transport

| Symbol | What to learn / copy |
|---|---|
| `HTTPError` | Carry `StatusCode`/`URL`/`Body` so callers classify retry-vs-give-up without parsing strings. |
| `isTransientError` | The retry policy: retry 5xx, 429, and pre-response network errors; do **not** retry other 4xx or context cancellation. |
| `ThreatLockerClient` + `NewThreatLockerClient` | A thin client holding `baseURL`, the credential, and an `http.Client` with sane timeouts/dialer. |
| `Post` (the request helper) | One generic helper: set the **exact** auth header(s) the real API needs, read the body, turn non-2xx into `*HTTPError`. Adapt to GET/query params if your API uses them. |
| `Close` | `CloseIdleConnections()`. |

## `threatlocker/client.go` — config + adapter

| Symbol | What to learn / copy |
|---|---|
| `ThreatLockerConfig` | Embed `uspclient.ClientOptions` as `client_options`; `json`+`yaml` tags on every field; a `Deduper utils.Deduper` field tagged `json:"-"` as a test/embedder seam. |
| `ThreatLockerConfig.Validate` | Call `c.ClientOptions.Validate()` first, check required fields, then fill every default. This is where defaults live. |
| `uspSink` interface | The seam that makes credential-free E2E tests possible: the subset of `*uspclient.Client` the adapter uses (`Ship`/`Drain`/`Close`). `*uspclient.Client` satisfies it; tests inject an in-memory sink. |
| `NewThreatLockerAdapter` vs `newThreatLockerAdapter` | Exported constructor delegates to an unexported one taking a `sink`; `nil` sink ⇒ build a real `uspclient.NewClient`. Note how an owned deduper is created with `utils.NewLocalDeduper` and cleaned up on error. |
| `Close` | Idempotent via `sync.Once`: `doStop.Set()`, `wgSenders.Wait()`, `Drain`, `Close`, release the deduper. |
| `runFeed` / `pollFeed` | The poll loop. First poll fires immediately, then `doStop.WaitFor(interval)`. **Read the long comment on `pollFeed`** explaining why every page is re-walked each poll and why dedup (not an early exit) is what ships each record once. |
| `fetchPage` | Exponential backoff bounded by `MaxRetryAttempts`; **401/403 stops the whole adapter**, other permanent errors abandon just this poll. |
| `ship` | Build `protocol.DataMessage{JsonPayload, EventType, TimestampMs}`, ship **verbatim**, handle `uspclient.ErrorBufferFull` (backpressure) by re-shipping with a long timeout. |
| `eventTime` | Extract the record's timestamp into UnixMilli; fall back to `time.Now()` when absent/unparseable (and `DebugLog` it). |
| `recordID` / `dedupeKey` | Resolve a stable id (configured field → common id fields → content hash); namespace the dedupe key per feed/type. |
| `extractItems` / `rawMessagesToDicts` | Handle bare-array vs object-envelope responses; decode each record with `utils.UnmarshalCleanJSON` (preserves integer precision — don't `json.Unmarshal` into `map[string]interface{}`, it coerces ints to float64). |

`utils.Dict` helpers worth knowing: `FindOneString("a/b")` (first string at a
`/`-path), `FindInt("path")` (all int matches — use to detect a present `0`).

## `threatlocker/mock_test.go` — the credential-free E2E harness

| Symbol | What to learn / copy |
|---|---|
| `captureSink` | The in-memory `uspSink` that records shipped messages; `count`/`snapshot` for assertions. |
| `mockThreatLocker` + `handler` | An `httptest` handler that **reproduces the real API**: checks the auth header, paginates over an in-memory dataset the same way the real API does, and can return a bare array or an object envelope. |
| `realisticApprovalRequest` / `realisticActionLog` | Fixtures shaped like **real** payloads — nested objects, arrays, mixed scalar types, real id/timestamp field names. Build yours from a real sanitized sample. |
| `TestMock*` tests | The coverage to mirror: all-records-ship + verbatim payload/timestamp, no re-ship on re-poll, mid-run record ships once, multi-page walk, multiple feeds tagged, bad token stops + ships nothing. |

`threatlocker/client_test.go` → `testClientOptions(t)` shows the `ClientOptions`
to use in tests (note `TestSinkMode: true` and the log callbacks).

## When your API differs from ThreatLocker

ThreatLocker paginates by page-number and ends on a short page. If yours uses a
cursor/`next` token or a `has_more` flag, study **`1password/`** instead (see
commit `1fda4a3`, "drain pages using has_more"), and make your mock use the same
mechanism. For OAuth-refresh auth see **`gmail/`**; for non-polling sources
(streams/queues/webhooks) see `sqs/`, `pubsub/`, `azure_event_hub/`, `syslog/`.

## Non-negotiables

- **Ship payloads verbatim** — LimaCharlie parses/maps fields in the cloud.
- **Auth failure (401/403) stops the adapter**; transient errors retry.
- **Dedup ships each record exactly once** even though pages re-fetch each poll.
- **The mock mirrors the real wire shape** (auth, pagination, envelope, field
  names/formats) so tests stay meaningful without live credentials.
