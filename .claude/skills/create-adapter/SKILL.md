---
name: create-adapter
description: Guide for building a new LimaCharlie USP adapter in this repo — researching the source API, implementing it following existing patterns, writing thorough mock-backed tests, optionally live-testing against a real API key, validating the mock's fidelity, and shipping a documented PR. Use when the user wants to add a new adapter / data source / integration (e.g. "add an adapter for <vendor>", "ingest <product> logs into LimaCharlie").
---

# Building a new USP adapter

A USP adapter pulls events from some external source (a SaaS API, a queue, a
file) and ships them to LimaCharlie as USP `DataMessage`s. This skill walks you
through building a new one the way the existing adapters in this repo are built,
with thorough tests that don't require live credentials to run in CI.

**The canonical example to read and imitate is `threatlocker/`** — a polling
HTTP-API adapter with a clean separation between transport (`api.go`), adapter
logic (`client.go`), unit tests (`client_test.go`), and an end-to-end mock
(`mock_test.go`). Read all four before writing code. `1password/`, `gmail/`, and
`sublime/` are good secondary references for other API shapes.

Work through the phases below in order. Don't skip the research phase — getting
the API shape right up front is what makes the rest (and the mock) correct.

## Phase 0 — Scope it with the user

Before writing anything, confirm with the user:

- **What source / vendor** and **which events** they want ingested.
- **Auth method**: API key/token, OAuth refresh token, basic auth, service
  account, etc. This drives the config struct and the mock's auth check.
- **Whether they have working credentials** you can use to live-test (ask now;
  you'll need the answer in Phase 5). If they do, find out how they want to
  provide them — env vars are simplest. **Never commit credentials.** Note that
  this repo already uses files like `*-do-not-commit.txt` for local secrets;
  follow that convention and double-check `.gitignore`.

If the source is a pull-based HTTP API that paginates and you poll it on an
interval, the `threatlocker` pattern fits almost exactly. If it's push-based
(webhook/socket) or a stream/queue, study the closest existing adapter instead
(`syslog`, `sqs`, `pubsub`, `azure_event_hub`).

## Phase 1 — Research the API

Read the actual vendor documentation — do not rely on memory. Use WebFetch /
WebSearch to pull:

- The **authentication** scheme (header name, token format, token lifetime,
  refresh flow). Capture the exact header(s) the request must carry.
- The **listing/query endpoint(s)** for the events: HTTP method, URL shape,
  request body/query parameters, and especially **how pagination works**
  (page number, cursor/`next` token, `has_more` flag, offset/limit, time
  window). Pagination correctness is the #1 source of adapter bugs.
- The **response envelope**: is the records array bare (`[...]`) or wrapped
  (`{"data": [...]}`)? What key holds it?
- A **stable per-record ID field** (for deduplication) and a **timestamp field**
  (for event time). Note their exact paths and formats.
- **Rate limits** and the **error model** (status codes, retry-after).

Record the doc URLs and a couple of **real example response payloads** — you'll
paste sanitized versions of these into the mock and the README. If a vendor
OpenAPI/Swagger spec or Postman collection exists, grab it; it's the most
reliable source for request/response shapes.

Write down, before coding:
- the auth header(s),
- the endpoint URL(s) + method,
- the pagination mechanism,
- the items-array location,
- the id + timestamp field paths,
- a sample record (verbatim from docs).

## Phase 2 — Implement, following existing patterns

Create a new package directory `myadapter/` (lowercase, no underscores in the
dir name to match siblings like `threatlocker`, `pandadoc`; the Go package is
`usp_myadapter`).

Mirror the `threatlocker` file layout:

- **`api.go`** — a thin HTTP client: a constructor, a single generic request
  helper (`Post`/`Get`) that sets auth headers and returns the raw body, an
  `HTTPError{StatusCode, URL, Body}` type, an `isTransientError()` classifier
  (retry 5xx/429/network; don't retry other 4xx or context-cancel), and a
  `Close()` that releases idle connections.
- **`client.go`** — the config struct + `Validate()` (apply defaults here),
  the adapter struct, `NewMyAdapter(ctx, conf) (*MyAdapter, chan struct{},
  error)`, the poll loop, pagination, dedup, timestamp extraction, and `Ship`.
- **`client_test.go`** — unit tests for the pure logic (validation, pagination
  math, id/timestamp extraction, transient-error classification) plus the
  shared `testClientOptions(t)` helper.
- **`mock_test.go`** — the end-to-end test against an in-memory mock API.
- **`README.md`** — adapter documentation (see Phase 6).

Key conventions to copy exactly (see `references/patterns.md` for annotated
snippets):

1. **Config**: embed `uspclient.ClientOptions` as `client_options`. Use
   `json`+`yaml` tags on every field. Implement `Validate()` that calls
   `c.ClientOptions.Validate()`, checks required fields, and fills defaults.
2. **The sink seam**: define a small `uspSink` interface (`Ship`/`Drain`/
   `Close`) and an unexported `newMyAdapter(ctx, conf, sink)` constructor so
   tests can inject an in-memory sink. The exported constructor passes `nil`
   and a real `uspclient.NewClient` is built. This is what makes end-to-end
   tests possible without a LimaCharlie connection (also set
   `TestSinkMode: true` in test `ClientOptions`).
3. **Stop/lifecycle**: use `utils.NewEvent()` for the stop signal, a
   `sync.WaitGroup` for feed goroutines, a `chStopped` channel closed when all
   senders exit, and a `sync.Once`-guarded idempotent `Close()`.
4. **Dedup**: every poll re-fetches pages, so dedup — not an early exit — is
   what ships each record once. Use `utils.NewLocalDeduper(window, ttl)` and a
   per-feed-namespaced key. Resolve the record id from the configured field,
   fall back to common id fields, then a content hash.
5. **Shipping**: build `protocol.DataMessage{JsonPayload, EventType,
   TimestampMs}`. Ship payloads **verbatim** — don't reshape the vendor JSON
   (LimaCharlie parses/maps in the cloud). Handle `uspclient.ErrorBufferFull`
   by retrying the Ship with a long timeout (backpressure).
6. **Retries**: wrap the page fetch in exponential backoff bounded by
   `MaxRetryAttempts`; stop the whole adapter on 401/403 (auth failure affects
   every poll), abandon just the current poll on other permanent errors.
7. **JSON**: decode records with `utils.UnmarshalCleanJSON` (preserves integer
   precision); use `utils.Dict` for record maps and its `FindOneString`/
   `FindInt` helpers for field extraction.

Then **register** the adapter (3 edits — see `references/registration.md`):
- `containers/conf/all.go`: import the package, add a field to `GeneralConfigs`.
- `containers/general/tool.go`: import the package, add the `else if method ==
  "myadapter"` dispatch branch.
- root `README.md`: add a short "### MyAdapter" section with a run example.

Build with `go build ./containers/general` and `go fmt ./myadapter/...` before
moving on.

## Phase 3 — Write the mock (model the real API shape)

The mock is the heart of testable-without-credentials. In `mock_test.go` build
an `httptest.NewServer` whose handler faithfully reproduces the **real** API:

- **Auth**: reject requests missing/with the wrong token exactly as the real API
  does (right status code, right-ish error body). Have a test that asserts a bad
  token stops the adapter and ships nothing.
- **Pagination**: implement the *same* pagination scheme the real API uses
  (page number, cursor, or `has_more`) over an in-memory dataset, so the
  adapter's paging logic is genuinely exercised. Test a multi-page dataset.
- **Envelope**: return the records in the *same* shape (bare array vs.
  `{"data":[...]}`). If the API can do both, cover both.
- **Realistic fixtures**: build record fixtures that match real payloads from
  Phase 1 — nested objects, arrays, mixed scalar types, the real id and
  timestamp field names/formats. Copy structure from a real (sanitized) sample.

End-to-end tests to write (model them on `threatlocker/mock_test.go`):
- All records ship; event type, parsed timestamp, and **verbatim payload** match.
- Re-polling does not re-ship (dedup works).
- A record appearing mid-run ships exactly once.
- A multi-page dataset is fully walked, each record once.
- Multiple feeds/types (if applicable) are tagged correctly.
- Bad auth → adapter stops, ships nothing.

## Phase 4 — Run the tests

This repo's tests are standard `go test`. Run:

```
go test ./myadapter/...
```

All tests must pass and be **fully enabled** — never skip a test or weaken an
assertion to get green. If a `run_local_test.sh` exists at the repo root, prefer
running that instead (it handles local system dependencies). There isn't one
today, so `go test` is correct for a self-contained adapter.

## Phase 5 — Live-test with real credentials (if available)

If the user provided credentials in Phase 0, run the real adapter against the
live API to prove it actually works end-to-end. Two options:

- **Via the built binary** (matches production), using a sink/test OID, e.g.:
  ```
  go build ./containers/general
  ./general myadapter \
    client_options.identity.oid=$OID \
    client_options.identity.installation_key=$INSTALLATION_KEY \
    client_options.platform=json \
    client_options.sensor_seed_key=myadapter \
    api_key=$MYADAPTER_API_KEY
  ```
  Confirm a new sensor appears in the user's LimaCharlie org and events flow.
- **Via a temporary integration test** gated on an env var being present (so CI
  without the secret still passes by the var being absent — but do **not** make
  this a permanent skipped test; remove it before the PR, its purpose is the
  live capture, not CI). Capture the real request/response wire bytes while it
  runs.

**Capture the real responses** during this run (log the raw bodies, or use a
proxy). These captures are gold for Phase 5b.

### Phase 5b — Validate the mock against reality

With real responses in hand, **diff them against your mock**:

- Does the real **pagination** behave as the mock assumes (page sizes, the
  end-of-data signal, cursor/`has_more`)? Fix the mock if not.
- Is the **envelope** key and shape exactly what the mock returns?
- Do the **id** and **timestamp** field paths/formats match? Are timestamps in
  the format your parser handles?
- Do the **fixtures** contain the field types the real data has (numbers that
  are big integers, nullable fields, nested arrays)?
- Does **auth** rejection use the status code your mock returns?

Update the mock and fixtures so they mirror the captured reality. The goal: the
mock-backed tests would have caught any wire-shape mistake, so future
maintainers can evolve the adapter confidently **without** live credentials.
Then re-run Phase 4 and confirm everything still passes.

If the user has **no** credentials, say so plainly, make the mock as faithful as
possible from the documented shapes (Phase 1), and note in the PR that it hasn't
been live-verified.

## Phase 6 — Document and open a draft PR

Write `myadapter/README.md` (model it on `threatlocker/README.md`):
- One-line description + link to the vendor and **the API docs** (the URLs from
  Phase 1).
- **Authentication**: how to obtain the credential, what header it maps to.
- **Configuration table**: every config key, required?, description, defaults.
- **What the data looks like**: which events are collected, the event types, and
  a sample payload.
- **How polling/pagination/dedup works** and any operational caveats (rate
  limits, required filters, instance selection, etc.).
- CLI and YAML run examples.

Also add the short root-`README.md` entry (Phase 2) and ensure the example there
matches.

Then open the PR **as a draft**:

```
git checkout -b add-myadapter-adapter
git add myadapter/ containers/conf/all.go containers/general/tool.go README.md
git commit -m "Add MyAdapter adapter (<one-line of what it ingests>)"   # see commit trailer rule
git push -u origin add-myadapter-adapter
gh pr create --draft --title "Add MyAdapter adapter" --body "<summary>"
```

PR body should cover: what the adapter ingests, the API docs link, auth method,
the event types/sample data, how it was tested (mock coverage + whether it was
live-verified), and any follow-ups. Be careful to stage **only** your adapter's
files — don't sweep in unrelated working-tree changes.

## Checklist

- [ ] API researched from real docs; auth, pagination, envelope, id+timestamp
      fields, sample payload recorded.
- [ ] `api.go` / `client.go` follow the `threatlocker` patterns (sink seam,
      `utils` deduper/event, verbatim payloads, retry + auth-stop).
- [ ] Registered in `containers/conf/all.go` and `containers/general/tool.go`;
      `go build ./containers/general` passes.
- [ ] Mock reproduces real auth + pagination + envelope; realistic fixtures.
- [ ] End-to-end + unit tests pass, fully enabled (`go test ./myadapter/...`).
- [ ] Live-tested if creds available; mock validated against captured responses.
- [ ] `myadapter/README.md` + root README entry written with API doc links and
      sample data.
- [ ] Draft PR opened with only the adapter's files staged.
