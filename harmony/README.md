# Harmony Adapter

USP adapter for [Check Point Harmony](https://www.checkpoint.com/harmony/) — pulls events and email-entity data through the Infinity Portal gateway and ships it to LimaCharlie.

The adapter has two sources. You can enable either or both.

| Source | API | Use it for |
| --- | --- | --- |
| `events` | Infinity Events / Logs-as-a-Service (`/app/laas-logs-api`) | The unified event stream across Harmony products (Endpoint, Email & Collaboration, Mobile, Connect, Browse). |
| `entities` | HEC `/app/hec-api/v1.0/search/query` | Every email-entity scenario: targeted server-side-filtered feeds (restore requests, content/recipient watches, DLP-flagged mail, …), or an unfiltered firehose as a preset. Adding a new scenario is a config entry, not a code change. |

A previous `emails` firehose source was folded into `entities` as a preset. See [Migrating from `emails`](#migrating-from-emails) below.

## Credentials and gateway URL

Generate an Infinity Portal API key (Global Settings → API Keys) with the services you need attached. One key with both "Logs as a Service" and "Harmony Email & Collaboration" is fine.

```yaml
harmony:
  client_id: "<infinity portal client id>"
  access_key: "<infinity portal access key>"

  # Optional. Defaults to the global gateway.
  # Use the regional variant if your tenant lives in a regional data center,
  # e.g. https://cloudinfra-gw-us.portal.checkpoint.com. The /app/laas-logs-api
  # and /app/hec-api paths share the same hostname per region.
  url: "https://cloudinfra-gw.portal.checkpoint.com"

  events:   { enabled: false, ... }
  entities: { enabled: false, queries: [ ... ] }
```

The adapter rejects a config where no source is enabled.

## Source: `events` (Infinity Events)

Polls Logs-as-a-Service for each configured cloud service.

```yaml
harmony:
  events:
    enabled: true
    # Defaults to the full suite. Names must match the gateway exactly —
    # the email service is "Harmony Email & Collaboration" (ampersand, not
    # the word "and"; the gateway rejects "and").
    cloud_services:
      - "Harmony Endpoint"
      - "Harmony Email & Collaboration"
      - "Harmony Mobile"
      - "Harmony Connect"
      - "Harmony Browse"
    # Optional Infinity Events query filter applied to every cloud_service.
    filter: ""
    poll_interval: 60s
    page_limit: 100   # gateway minimum is 10
    limit: 5000       # per-cloud-service window cap; gateway minimum is 10
```

If a configured `cloud_service` is not provisioned for the tenant the gateway returns the query in state `Canceled`; the adapter logs one warning per poll and keeps going (it does not surface as an error). Remove the service from `cloud_services` to silence the warning.

Each shipped record is annotated:

```
_lc_harmony_source:  "infinity_events"
_lc_harmony_service: "<the cloud_service it came from>"
```

## Source: `entities`

A single source that takes a list of named queries. Each query is one HEC `search/query` request shape: a `saas` scope, an optional set of server-side predicates (`entityExtendedFilter`), and a choice of cursor mode. Restore requests, the firehose, and ad-hoc content/recipient watches are all just different `EntityQuery` entries.

### Two cursor modes

| Mode | When to use | `entityFilter` sent | Cursor |
| --- | --- | --- | --- |
| **Window mode** (`cursor_field` empty) | The matching email is itself recent — content/recipient/detection filters, or the unfiltered firehose. | `saas` + `startDate` + `endDate` + `saasEntity` (received-time window). | Rolling window + dedup. |
| **Cursor mode** (`cursor_field` set) | The event of interest is decoupled in time from the email's receipt — e.g. a restore request on an old quarantined email. | `saas` + wide `startDate` only — no `endDate`, no `saasEntity`. | Adapter auto-injects `{cursor_field} greaterThan {cursor}` and advances `cursor` to the newest value seen. **`cursor_field` must reference a timestamp-typed field** under `entityPayload.*` or `entityInfo.*`. |

Either mode is bounded by server-side predicates, so the gateway's per-query record ceiling is not approached — and the design scales independently of total mail volume.

> **Restore requests need cursor mode.** A window-mode query (or the firehose preset) cannot surface a restore request. The `entityFilter` window filters on the email's *received* time, but the underlying quarantined email may have been received hours, days, or months before the restore was requested — so it isn't in any recent received-time window. Use the `restore_requests` preset below.

### Filter predicates

Each predicate is one server-side `entityExtendedFilter` clause, ANDed by the gateway:

```yaml
filter:
  - {attr: <saasAttrName>, op: <saasAttrOp>, value: "<saasAttrValue>"}
```

`attr` is a Check Point [saasAttrName](https://sc1.checkpoint.com/documents/Harmony_Email_and_Collaboration_API_Reference/Topics-HEC-Avanan-API-Reference-Guide/Managing-Secured-Entities/Search-query.htm) (e.g. `entityPayload.subject`, `entityPayload.recipients`, `entityPayload.isRestoreRequested`). `op` is one of: `is`, `isNot`, `contains`, `notContains`, `startsWith`, `isEmpty`, `isNotEmpty`, `greaterThan`, `lessThan`. `value` is a string; booleans are spelled as the string `"true"` / `"false"`.

Unknown ops are rejected at startup so a typo fails loudly instead of silently matching nothing.

### Annotations

Each shipped record carries:

```
_lc_harmony_source: "entities"
_lc_harmony_query:  "<the query's name>"
_lc_harmony_saas:   "office365_emails" | "google_mail"
```

Use `_lc_harmony_query` to route per-scenario downstream — e.g. send `restore_requests` to one D&R rule and `vip_recipient_watch` to another.

### Examples

#### 1. Quarantined-email restore requests (cursor mode, canonical preset)

Mirrors Check Point's own XSOAR `restore_requests`. Wide received-time floor so old quarantined mail is in range; server-side filter selects only the restore requests; cursor advances on `restoreRequestTime`.

```yaml
harmony:
  entities:
    enabled: true
    queries:
      - name: restore_requests
        saas: [office365_emails, google_mail]
        filter:
          - {attr: entityPayload.isRestoreRequested, op: is, value: "true"}
        cursor_field: entityPayload.restoreRequestTime
        lookback: 360h          # 15 days — how old the underlying email may be
        initial_lookback: 1h    # how far back the cursor starts on first poll
        poll_interval: 5m
```

The same email re-emits when its lifecycle advances (gateway bumps `entityUpdated` on requested → declined / restored).

#### 2. Email firehose (window mode, no filter, splits included)

Every email entity HEC processes, with verdict / quarantine flags inline. Triage downstream. Equivalent to the old `emails` source.

```yaml
harmony:
  entities:
    enabled: true
    queries:
      - name: emails
        saas: [office365_emails, google_mail]
        include_splits: true   # ship "split" master records (firehose semantics)
        lookback: 1h
        poll_interval: 5m
```

The HEC `search/query` endpoint enforces a per-query record ceiling (~10,000 records, oldest-first within the window). The default 1h lookback keeps the firehose under that on a typical tenant, but a very high-volume tenant could exceed it within an hour and silently lose the oldest events. For targeted feeds prefer a filtered query — server-side filtering makes the result set independent of total mail volume.

#### 3. Subject / sender watch (window mode)

A targeted content filter on recent mail.

```yaml
harmony:
  entities:
    enabled: true
    queries:
      - name: invoice_subject_watch
        saas: [office365_emails]
        filter:
          - {attr: entityPayload.subject,    op: contains, value: "INVOICE"}
          - {attr: entityPayload.fromDomain, op: is,       value: "example.com"}
        lookback: 1h
        poll_interval: 5m
```

#### 4. VIP-recipient watch (window mode, multiple predicates AND'd)

```yaml
harmony:
  entities:
    enabled: true
    queries:
      - name: vip_recipient_watch
        saas: [office365_emails, google_mail]
        filter:
          - {attr: entityPayload.recipients, op: is,       value: "ceo@example.com"}
          - {attr: entityPayload.subject,    op: contains, value: "wire"}
        lookback: 1h
        poll_interval: 5m
```

#### 5. Multiple queries in one source

Just list them. Each query runs independently, has its own dedup state, and ships with its own `_lc_harmony_query` annotation.

```yaml
harmony:
  entities:
    enabled: true
    queries:
      - name: restore_requests
        filter:
          - {attr: entityPayload.isRestoreRequested, op: is, value: "true"}
        cursor_field: entityPayload.restoreRequestTime
        lookback: 360h
        initial_lookback: 1h
        poll_interval: 5m

      - name: invoice_subject_watch
        filter:
          - {attr: entityPayload.subject, op: contains, value: "INVOICE"}
        lookback: 1h
        poll_interval: 5m
```

### Configuration reference

| Field | Default | Notes |
| --- | --- | --- |
| `name` | — (required) | Identifier for the query. Must be unique within `entities.queries`. Appears in errors and as `_lc_harmony_query`. |
| `saas` | `[office365_emails, google_mail]` | Each saas runs its own worker. Must be one of the supported values above. |
| `filter` | `[]` | List of `{attr, op, value}` predicates passed through as `entityExtendedFilter`. Empty is allowed (then the query is bounded only by the entity window and, in cursor mode, the injected cursor predicate). |
| `cursor_field` | `""` | Empty → window mode. Set to `entityPayload.<k>` or `entityInfo.<k>` (timestamp-typed) → cursor mode. |
| `include_splits` | `false` | If true, ship `entityPayload.emailSplit == "split"` master records alongside their child copies (firehose semantics). Default skips them so a single email isn't double-emitted per query. |
| `lookback` | `1h` (window) / `360h` (cursor) | Floor on `entityFilter.startDate` (received time). |
| `initial_lookback` | `1h` | Cursor mode only: how far back the cursor starts on the first poll. |
| `poll_interval` | `5m` | Time between polls. |

### Operational notes

- **Per-query record ceiling.** HEC enforces a `recordsNumber: 10000` per-query cap, oldest-first within the window. Targeted server-side filters keep the matching result set small and well below the cap; only the unfiltered firehose preset (`include_splits: true`, no filter) can approach it on a high-volume tenant. The maximum scroll-page count is bounded at 1000 pages as an anti-spin safety net; hitting it surfaces a loud error rather than silently truncating.
- **Scroll mechanics.** HEC's scroll handle is *stable* — the same `scrollId` comes back on every page. Termination is signalled by an empty page, not by a changed/empty scroll id. The adapter re-sends the handle until it receives an empty page.
- **Dedup.** Per-query, keyed on `(query name, entityId, entityUpdated)` and, in cursor mode, also the `cursor_field` value. Unchanged repeats are suppressed; lifecycle advances (the gateway bumps `entityUpdated`) re-emit.
- **Split emails.** See `include_splits` above. Default-off avoids double-emission on filtered queries; set true on a firehose-style query to match the gateway's raw output.
- **Retries.** Gateway calls absorb transient HTTP 502/503/504 and timeout/reset errors via bounded retry with jitter. A persistent 401 triggers a single token refresh + re-issue; a persistent failure surfaces as an error.

## Migrating from `emails`

The previous `emails` source has been removed. Adapter configs carrying `harmony.emails: {enabled: true}` will fail Validate at startup with a clear message pointing here.

**Before:**

```yaml
harmony:
  emails:
    enabled: true
    saas: [office365_emails, google_mail]
    lookback: 1h
    poll_interval: 5m
```

**After:**

```yaml
harmony:
  entities:
    enabled: true
    queries:
      - name: emails              # picked any name; appears as _lc_harmony_query
        saas: [office365_emails, google_mail]
        include_splits: true      # matches the old firehose semantics
        lookback: 1h
        poll_interval: 5m
```

Downstream rules / dashboards that filter on `_lc_harmony_source: "emails"` need to be updated to filter on `_lc_harmony_source: "entities"` (plus optionally `_lc_harmony_query: "emails"` if you want to scope to this specific feed).

## References

- Check Point HEC API reference: <https://sc1.checkpoint.com/documents/Harmony_Email_and_Collaboration_API_Reference/CP_Harmony_Email_Collaboration_API_Reference_Guide.pdf>
- HEC `search/query` endpoint: <https://sc1.checkpoint.com/documents/Harmony_Email_and_Collaboration_API_Reference/Topics-HEC-Avanan-API-Reference-Guide/Managing-Secured-Entities/Search-query.htm>
- The cursor-mode pattern (wide `startDate`, no `endDate`, server-side filter + `restoreRequestTime` cursor) mirrors Check Point's official Cortex XSOAR integration `CheckPointHEC.py → restore_requests()`.
