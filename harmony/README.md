# Harmony Adapter

USP adapter for [Check Point Harmony](https://www.checkpoint.com/harmony/) — pulls events and email-entity data through the Infinity Portal gateway and ships it to LimaCharlie.

The adapter has three independent sources. You can enable any combination.

| Source | API | Use it for | Result-set shape |
| --- | --- | --- | --- |
| `events` | Infinity Events / Logs-as-a-Service (`/app/laas-logs-api`) | The unified event stream across Harmony products (Endpoint, Email & Collaboration, Mobile, Connect, Browse). | Per-service polled stream. |
| `emails` | HEC `/app/hec-api/v1.0/search/query` | **Firehose** — every email entity HEC processes, with its security verdicts and quarantine/restore flags inline. | Every email in a rolling received-time window. |
| `entities` | HEC `/app/hec-api/v1.0/search/query` | **Targeted, server-side-filtered** entity feeds: a list of named queries. The general-purpose source for any HEC scenario that is *not* "ship every email" (restore requests, content/recipient watches, DLP-flagged mail, …). | Only the entities the gateway-side filter matches. |

Adding a new HEC scenario should almost always be a new `entities.queries` entry, not a new source.

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

  events: { enabled: false, ... }
  emails: { enabled: false, ... }
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
_lc_harmony_source: "infinity_events"
_lc_harmony_service: "<the cloud_service it came from>"
```

## Source: `emails` (firehose)

Ships **every** email entity HEC processes in a rolling received-time window, deduped on `(entityId, entityUpdated)`. There is no server-side filter — triage and alerting happen downstream.

```yaml
harmony:
  emails:
    enabled: true
    saas: [office365_emails, google_mail]  # default
    poll_interval: 5m
    lookback: 1h
```

Annotations on each shipped record:

```
_lc_harmony_source: "emails"
_lc_harmony_saas:   "office365_emails" | "google_mail"
```

**Scaling note.** The HEC `search/query` endpoint enforces a per-query record ceiling (~10,000 records, oldest-first within the window). The default 1h lookback keeps the unfiltered feed under that on a typical tenant, but a very high-volume tenant could exceed it within an hour and silently lose the oldest events in that window. For targeted feeds prefer the `entities` source, which is server-side filtered and therefore independent of total mail volume.

**Important: the firehose cannot surface restore requests.** The `entityFilter.startDate/endDate` window filters on the email's *received* time, not on `entityUpdated` and not on `restoreRequestTime`. A restore request is raised against an email received and quarantined earlier (often hours, days, or months ago), so its `isRestoreRequested` flag never appears in a recent received-time window. Use the `entities` source with a `restore_requests` query for that — see the preset below.

## Source: `entities` (generic, server-side filtered)

A single source that takes a list of named queries. Each query is one HEC `search/query` request shape: a `saas` scope, an optional set of server-side predicates (`entityExtendedFilter`), and a choice of cursor mode.

### Two cursor modes

| Mode | When to use | `entityFilter` sent | Cursor |
| --- | --- | --- | --- |
| **Window mode** (`cursor_field` empty) | The matching email is itself recent — content/recipient/detection filters. | `saas` + `startDate` + `endDate` + `saasEntity` (received-time window). | Rolling window + dedup. |
| **Cursor mode** (`cursor_field` set) | The event of interest is decoupled in time from the email's receipt — e.g. a restore request on an old quarantined email. | `saas` + wide `startDate` only — no `endDate`, no `saasEntity`. | Adapter auto-injects `{cursor_field} greaterThan {cursor}` and advances `cursor` to the newest value seen. **`cursor_field` must reference a timestamp-typed field** under `entityPayload.*` or `entityInfo.*`. |

Either mode is bounded by server-side predicates, so the gateway's per-query record ceiling is not approached — and the design scales independently of total mail volume.

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

#### 1. Quarantined-email restore requests (cursor-mode canonical preset)

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

#### 2. Subject / sender watch (window mode)

A targeted content filter on recent mail. No `cursor_field` — the matching email is itself recent.

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

#### 3. VIP-recipient watch (window mode, multiple predicates)

All predicates AND on the gateway side. Combine content + recipient + sender as needed.

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

#### 4. Multiple queries in one source

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
| `lookback` | `1h` (window) / `360h` (cursor) | Floor on `entityFilter.startDate` (received time). |
| `initial_lookback` | `1h` | Cursor mode only: how far back the cursor starts on the first poll. |
| `poll_interval` | `5m` | Time between polls. |

### Operational notes

- **Per-query record ceiling.** HEC enforces a `recordsNumber: 10000` per-query cap, oldest-first within the window. Targeted server-side filters (the point of this source) keep the matching result set small and well below the cap. The maximum scroll-page count is bounded at 1000 pages as an anti-spin safety net; hitting it surfaces a loud error rather than silently truncating.
- **Scroll mechanics.** HEC's scroll handle is *stable* — the same `scrollId` comes back on every page. Termination is signalled by an empty page, not by a changed/empty scroll id. The adapter re-sends the handle until it receives an empty page.
- **Dedup.** Per-query, keyed on `(query name, entityId, entityUpdated)` and, in cursor mode, also the `cursor_field` value. Unchanged repeats are suppressed; lifecycle advances (the gateway bumps `entityUpdated`) re-emit.
- **Split emails.** A record with `entityPayload.emailSplit == "split"` is the master of a split email; the child carries the actionable copy. The `entities` source skips masters to avoid double-emission. The `emails` firehose does not — it ships everything.
- **Retries.** Gateway calls absorb transient HTTP 502/503/504 and timeout/reset errors via bounded retry with jitter. A persistent 401 triggers a single token refresh + re-issue; a persistent failure surfaces as an error.

## References

- Check Point HEC API reference: <https://sc1.checkpoint.com/documents/Harmony_Email_and_Collaboration_API_Reference/CP_Harmony_Email_Collaboration_API_Reference_Guide.pdf>
- HEC `search/query` endpoint: <https://sc1.checkpoint.com/documents/Harmony_Email_and_Collaboration_API_Reference/Topics-HEC-Avanan-API-Reference-Guide/Managing-Secured-Entities/Search-query.htm>
- The `entities` source's cursor-mode pattern (wide `startDate`, no `endDate`, server-side filter + `restoreRequestTime` cursor) mirrors Check Point's official Cortex XSOAR integration `CheckPointHEC.py → restore_requests()`.
