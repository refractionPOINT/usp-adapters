---
name: create-adapter
description: Guide for building a new LimaCharlie USP adapter in this repo — researching the source, implementing it by copying the closest existing adapter, writing thorough tests with a mock/fake of the source, optionally live-testing against real credentials, validating the mock's fidelity, and shipping a documented PR. Use when the user wants to add a new adapter / data source / integration (e.g. "add an adapter for <vendor>", "ingest <product> logs into LimaCharlie").
---

# Building a new USP adapter

A USP adapter reads events from some external source and ships them to
LimaCharlie as USP `DataMessage`s. Sources vary widely — REST APIs you poll,
vendor SDKs (AWS/GCP/Azure), streaming sockets, mailboxes, local files, stdin —
so **there is no single template**. What every adapter shares is one contract
(see `references/patterns.md`); everything else you get by **copying the existing
adapter closest to your source**.

The strongest, best-tested examples to imitate are `threatlocker/` and `gmail/`
(both have full mock-backed test suites). But pick by source type:

- HTTP API you poll → `threatlocker/`, `1password/`, `gmail/`
- a vendor SDK / cloud client → `s3/`, `sqs/` (AWS), `pubsub/`, `gcs/`,
  `bigquery/` (GCP), `azure_event_hub/` (Azure)
- streaming socket / push → `syslog/`, `azure_event_hub/`
- mailbox → `imap/`, `gmail/`
- local files / OS sources → `file/`, `evtx/`, `wel/`, `stdin/`

`references/patterns.md` maps source types to examples and lists the shared
contract and the patterns worth copying regardless of source. Read it plus your
chosen example(s) before writing code.

Work through the phases in order. Don't skip research — getting the source's real
shape right up front is what makes the implementation and the mock correct.

## Phase 0 — Scope it with the user

Before writing anything, confirm:

- **What source / vendor** and **which events** they want ingested.
- **How you reach the source**: a documented HTTP API, an official SDK, a
  socket/stream, a file? This decides which existing adapter you copy.
- **Auth method**: API key/token, OAuth refresh token, basic auth, cloud
  credentials/service account, etc. This drives the config struct and the mock's
  auth check.
- **Whether they have working credentials** you can use to live-test (ask now;
  you'll need it in Phase 5). If they do, agree how they'll provide them — env
  vars are simplest. **Never commit credentials.** This repo already uses files
  like `*-do-not-commit.txt` for local secrets; follow that and check
  `.gitignore`.

## Phase 1 — Research the source

Read the real vendor documentation / SDK reference — do not rely on memory. Use
WebFetch / WebSearch. Capture, before coding:

- **Authentication** — exact mechanism (header + token format, OAuth refresh
  flow, SDK credential object), token lifetime/refresh.
- **How you list/receive events** — for an HTTP API: the endpoint(s), method,
  request params, and **how pagination works** (page number, cursor/`next`,
  `has_more`, offset/limit, time window) — pagination is the #1 source of adapter
  bugs. For an SDK: which client method streams/lists the events and how it
  signals "no more". For a stream/queue: how messages are received and
  acknowledged.
- **The event shape** — a real sample payload, the **stable per-record id field**
  (for dedup) and the **timestamp field** (for event time), with exact
  paths/formats. For HTTP, the response envelope (bare array vs. `{"data":[...]}`).
- **Rate limits / error model** — status codes, retry-after, throttling.

Record the doc URLs and a couple of **real sample payloads** — sanitized
versions go into the mock fixtures and the README. Grab an OpenAPI/Swagger spec,
Postman collection, or SDK example if one exists; it's the most reliable source
of shapes.

## Phase 2 — Implement by copying the closest adapter

Create package directory `myadapter/` (lowercase; the Go package is
`usp_myadapter`). Most adapters are a single `client.go`; the well-tested ones
add `*_test.go` and sometimes split transport into `api.go` (as `threatlocker`
does for HTTP). **Mirror the file layout of whichever adapter you're copying.**

Every adapter, whatever the source, must honor the universal contract (detailed
in `references/patterns.md`):

- Config struct embedding `uspclient.ClientOptions` as `client_options`
  (json+yaml tags on every field) with a `Validate()` that fills defaults.
- `func NewMyAdapter(ctx, conf) (*MyAdapter, chan struct{}, error)` and an
  idempotent `Close() error`; the channel closes when the adapter stops itself.
- Read from the source however it works, and **ship each event verbatim** as a
  `protocol.DataMessage{JsonPayload, EventType, TimestampMs}` through the usp
  client. Don't reshape payloads — LimaCharlie parses/maps in the cloud.

Patterns worth copying from the well-tested adapters (all in
`references/patterns.md`): a **test seam** that puts the external dependency
behind a small interface so a fake/mock can stand in (`threatlocker`'s `uspSink`
+ unexported `newThreatLockerAdapter(ctx,conf,sink)`); lifecycle via
`utils.NewEvent()` + `WaitGroup` + `sync.Once` `Close`; **exactly-once shipping**
via `utils.NewLocalDeduper` keyed on a stable id when the source can re-deliver;
robust transport (`HTTPError` + `isTransientError`, backoff, **stop on 401/403**)
for HTTP sources; JSON fidelity via `utils.UnmarshalCleanJSON`/`utils.Dict`; and
backpressure handling on `uspclient.ErrorBufferFull`.

Then **register** the adapter (see `references/registration.md` — easiest is to
grep an existing adapter like `threatlocker` across these files and replicate):
- `containers/conf/all.go`: import the package, add a field to `GeneralConfigs`.
- `containers/general/tool.go`: import it, add the `else if method == "myadapter"`
  dispatch branch.
- root `README.md`: add a short section with a run example.

Build with `go build ./containers/general` and `go fmt ./myadapter/...`.

## Phase 3 — Write a mock/fake of the source

The whole point is end-to-end tests that run **without live credentials**, by
substituting a stand-in for the external source through the test seam. Model on
`threatlocker/mock_test.go` and `gmail/mock_test.go`:

- For an **HTTP** source: an `httptest.NewServer` whose handler **reproduces the
  real API** — checks auth the way the real API does, paginates over an in-memory
  dataset with the *same* mechanism, returns the *same* envelope shape.
- For an **SDK / non-HTTP** source: a fake implementing the small interface your
  adapter calls, returning realistic results and exercising the same "more
  data / no more" and error behaviors.
- **Realistic fixtures** matching the Phase-1 sample payloads — nested objects,
  arrays, mixed scalar types, the real id and timestamp field names/formats.

End-to-end tests to write:
- All events ship; event type, parsed timestamp, and **verbatim payload** match.
- Re-reading/re-polling does not re-ship (dedup works, when applicable).
- An event appearing mid-run ships exactly once.
- A multi-page / multi-batch dataset is fully consumed, each event once.
- Multiple event types (if applicable) are tagged correctly.
- Bad auth → adapter stops, ships nothing.

## Phase 4 — Run the tests

Standard `go test`:

```
go test ./myadapter/...
```

All tests must pass and be **fully enabled** — never skip a test or weaken an
assertion to get green. If a `run_local_test.sh` ever exists at the repo root,
prefer it (it handles local system dependencies). There isn't one today, so
`go test` is correct for a self-contained adapter.

## Phase 5 — Live-test with real credentials (if available)

If the user provided credentials in Phase 0, run the real adapter against the
live source to prove it works end-to-end. Easiest is via the built binary
(matches production), pointed at a test OID:

```
go build ./containers/general
./general myadapter \
  client_options.identity.oid=$OID \
  client_options.identity.installation_key=$INSTALLATION_KEY \
  client_options.platform=json \
  client_options.sensor_seed_key=myadapter \
  <your-auth-config>=$CREDENTIAL ...
```

Confirm a new sensor appears in the user's LimaCharlie org and events flow.
**Capture the real responses/messages** while it runs (log the raw payloads, or
use a proxy) — these are gold for Phase 5b. If you write a throwaway integration
test to do the capture, **remove it before the PR**; do not leave a skipped test
behind.

### Phase 5b — Validate the mock against reality

With real data in hand, **diff it against your mock**:

- Does the real **pagination / batching** match what the mock assumes (sizes,
  the end-of-data signal, cursor/`has_more`)?
- Is the **envelope / result shape** exactly what the mock returns?
- Do the **id** and **timestamp** field paths/formats match? Are timestamps in a
  format your parser handles?
- Do the **fixtures** carry the field types the real data has (big integers,
  nullable fields, nested arrays)?
- Does **auth** rejection behave as the mock simulates?

Update the mock and fixtures to mirror captured reality, so the tests would have
caught any shape mistake and future maintainers can evolve the adapter
confidently **without** live credentials. Re-run Phase 4.

If the user has **no** credentials, say so plainly, make the mock as faithful as
possible from the documented shapes (Phase 1), and note in the PR that it hasn't
been live-verified.

## Phase 6 — Document and open a draft PR

Write `myadapter/README.md` (model on `threatlocker/README.md`):
- One-line description + links to the vendor and **the API/SDK docs** (Phase 1).
- **Authentication**: how to obtain the credential and how it's supplied.
- **Configuration table**: every config key, required?, description, defaults.
- **What the data looks like**: which events are collected, the event types, a
  sample payload.
- **How it works** (polling/streaming, pagination/batching, dedup) and
  operational caveats (rate limits, required filters, region/instance, etc.).
- CLI and YAML run examples.

Add the short root-`README.md` entry too and make sure the example matches.

Then open the PR **as a draft**:

```
git checkout -b add-myadapter-adapter
git add myadapter/ containers/conf/all.go containers/general/tool.go README.md
git commit -m "Add MyAdapter adapter (<one-line of what it ingests>)"   # honor the repo commit-trailer rule
git push -u origin add-myadapter-adapter
gh pr create --draft --title "Add MyAdapter adapter" --body "<summary>"
```

PR body: what it ingests, API/SDK docs link, auth method, event types/sample
data, how it was tested (mock coverage + whether live-verified), follow-ups. Be
careful to stage **only** your adapter's files — don't sweep in unrelated
working-tree changes.

## Checklist

- [ ] Source researched from real docs; auth, how-events-arrive, pagination/
      batching, id+timestamp fields, sample payload recorded.
- [ ] Closest existing adapter identified and copied; universal contract honored
      (embeds `ClientOptions`, `New…/Close`, ships verbatim `DataMessage`s).
- [ ] Patterns copied where applicable: test seam, lifecycle, dedup, transient/
      auth handling, JSON fidelity, backpressure.
- [ ] Registered in `containers/conf/all.go` and `containers/general/tool.go`;
      `go build ./containers/general` passes.
- [ ] Mock/fake reproduces the real source (auth, pagination/batching, shape);
      realistic fixtures.
- [ ] Tests pass, fully enabled (`go test ./myadapter/...`).
- [ ] Live-tested if creds available; mock validated against captured data.
- [ ] `myadapter/README.md` + root README entry written with doc links and
      sample data.
- [ ] Draft PR opened with only the adapter's files staged.
