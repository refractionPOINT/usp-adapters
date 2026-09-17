# Netskope Adapter

Pulls security telemetry from the [Netskope](https://www.netskope.com/) REST API
v2 **dataexport iterator** into LimaCharlie. Events are forwarded in their
original Netskope JSON form — the adapter does not reshape payloads.

Netskope exposes its telemetry as a set of iterator streams, one per **event
type** and one per **alert type**. Each stream is consumed by polling

```
GET /api/v2/events/dataexport/{events|alerts}/{type}?index=<cursor>&operation=next
```

The adapter models each stream as a **feed**, so adding a new stream is purely a
configuration change — no code change required.

Docs: [REST API v2 dataexport iterator](https://docs.netskope.com/en/using-the-rest-api-v2-dataexport-iterator-endpoints/),
[REST API v2 overview](https://docs.netskope.com/en/rest-api-v2-overview-312207/),
[API tokens](https://docs.netskope.com/en/api-tokens-2/).

## How the iterator works

The `index` query parameter is a **consumer-chosen cursor name**. Netskope stores
the read position for that name **server-side**, so the adapter keeps no state of
its own: a restart simply resumes the same `index` with `operation=next` — no
gaps, no re-reads. Each call returns up to **10,000 records** plus a `wait_time`
(seconds) telling the consumer how long to wait before the next call; the adapter
honours it (and the documented **4 requests/second/endpoint** rate limit).

> Each stream needs its own unique `index`. The adapter derives one per feed as
> `<index_prefix>_<event|alert>_<type>` (e.g. `netskope_alert_dlp`).
> **Never point two collectors at the same `index`** — concurrent consumers of one
> index corrupt each other's position.

## Out of the box

With no `feeds` configured, the adapter collects every **alert** stream plus the
**audit** event stream:

| Default feeds | Streams |
|---|---|
| Alerts (`alert_<type>`) | `dlp`, `malware`, `malsite`, `ctep`, `compromisedcredential`, `uba`, `policy`, `quarantine`, `remediation`, `securityassessment`, `watchlist` |
| Events (`event_<type>`) | `audit` |

Alerts are the high-signal, lower-volume detections a SOC cares about most. The
`page` / `application` / `network` event streams are **very high volume** and are
left opt-in — add them with `event_types` or a custom `feeds` entry.

Select a subset with `alert_types` / `event_types`, or replace the set entirely
with `feeds`. An explicit empty list (`alert_types: []`) disables that whole kind.

## Authentication

Create a REST API v2 token under **Settings → Tools → REST API v2 → New Token**
in your Netskope tenant, and grant it the **Read** scope for every
`/api/v2/events/dataexport/...` endpoint the adapter will poll. The token is sent
in the `Netskope-Api-Token` header.

The API root is `https://<tenant>/api/v2`, where `<tenant>` is your tenant host
(commonly `<name>.goskope.com`, but EU/other regions and custom hosts differ —
use the exact host your tenant uses). Provide it via `tenant`, or set `base_url`
to override the whole root.

## Configuration

| Key | Required | Description |
|-----|----------|-------------|
| `client_options` | yes | Standard USP adapter options (see the repo README). |
| `token` | yes | Netskope REST API v2 token, scoped to the dataexport endpoints. |
| `tenant` | yes* | Tenant host, e.g. `acme.goskope.com`. *Required unless `base_url` is set. |
| `base_url` | no | Full API root override, e.g. `https://acme.eu.goskope.com/api/v2`. |
| `index_prefix` | no | Namespaces the per-feed iterator cursor names. Must be unique to this adapter on the tenant. Defaults to `sensor_seed_key` (or `limacharlie`). |
| `alert_types` | no | Alert streams to collect. Absent = all defaults; `[]` = none. Ignored when `feeds` is set. |
| `event_types` | no | Event streams to collect. Absent = `["audit"]`; `[]` = none. Ignored when `feeds` is set. |
| `start_time` | no | One-time seed for a **new** stream's starting position: epoch seconds, RFC3339, or a relative Go duration ago (`24h`). Absent = `operation=next` (resume, or start from Netskope's earliest retained position). See the note below. |
| `feeds` | no | Explicit list of streams to poll. When set, it replaces the defaults entirely. |
| `poll_interval` | no | Idle cadence when the server returns no `wait_time` and there is no backlog. Default `30s`. |
| `max_wait_time` | no | Cap on a server-provided `wait_time`, so a quiet stream is still polled at least this often. Default `5m`. |
| `dedupe_ttl` | no | How long a record id is remembered to suppress re-shipping (covers iterator resend / re-seed overlap). Default `1h`. |
| `retry_base_delay` / `max_retry_delay` / `max_retry_attempts` | no | Transient-failure retry tuning. |

### Feed fields

| Key | Required | Description |
|-----|----------|-------------|
| `kind` | yes | `events` or `alerts`. |
| `type` | yes | The Netskope stream type within the kind (e.g. `page`, `audit`, `dlp`, `malware`). |
| `name` | no | Labels the feed and becomes the `EventType` of every shipped record. Defaults to `<event\|alert>_<type>`. |
| `index` | no | Iterator cursor name. Defaults to `<index_prefix>_<event\|alert>_<type>`. |
| `timestamp_field` | no | Record field holding the event time (epoch seconds or ISO-8601). Default `timestamp`. |
| `id_field` | no | Record field holding a stable id, used for dedup. Falls back to `_id`/`id`, then a content hash. |

### About `start_time`

By default the adapter uses `operation=next`, which resumes an existing cursor or,
for a brand-new `index`, starts from Netskope's earliest retained position. This
is restart-safe (no gaps, no duplicates).

`start_time` seeds a brand-new stream at a specific point — useful to bound an
initial backfill (e.g. `start_time: 24h`). **It re-seeds on every process
restart**, so leaving it set means a restart re-reads from that point; the
deduper absorbs short overlaps, but for the cleanest behavior set it for the
initial backfill, then remove it.

## How polling works

One goroutine per feed walks `operation=next` forever, shipping each record
verbatim with `EventType` set to the feed name and the event timestamp taken from
the record's `timestamp` (epoch seconds). Between calls it waits the server's
`wait_time` (capped by `max_wait_time`); with a backlog and no hint it drains
quickly, and when idle it polls at `poll_interval`.

An in-memory deduper (keyed per feed on the record id) guards the only cases where
the iterator can re-deliver a record — an `operation=resend` retry or a
`start_time` re-seed after a restart.

Errors coming back from the Netskope API — persistent 5xx, a malformed response,
or an authentication failure (401/403, e.g. a token missing the endpoint's scope)
— are **logged as warnings and never stop the adapter**. The failing feed skips
that poll and retries on the next interval while the other feeds keep running.
This is deliberate: a problem on Netskope's side cannot be fixed by restarting the
collector, so the adapter does not treat it as fatal (which, in the hosted
cloud-adapter environment, would tear the whole adapter down and eventually
disable it). Only a failure *delivering* events to LimaCharlie is fatal, because
there a restart re-establishes the connection.

Transient API failures (HTTP 5xx, 429, network errors) are retried with
exponential backoff; on a 429 the adapter honours the server's `Retry-After` /
`RateLimit-Reset` hint.

## Examples

Default (all alert streams + audit events), via the CLI:

```
./general netskope \
  client_options.identity.oid=$OID \
  client_options.identity.installation_key=$INSTALLATION_KEY \
  client_options.platform=json \
  client_options.sensor_seed_key=netskope \
  token=$NETSKOPE_API_TOKEN \
  tenant=acme.goskope.com
```

Collecting a focused set plus high-volume page events, with a bounded initial
backfill, via a YAML config file:

```yaml
netskope:
  client_options:
    identity:
      oid: $OID
      installation_key: $INSTALLATION_KEY
    platform: json
    sensor_seed_key: netskope
  token: $NETSKOPE_API_TOKEN
  tenant: acme.goskope.com
  start_time: 24h          # one-time backfill; remove after the first run
  alert_types:
    - dlp
    - malware
    - ctep
    - compromisedcredential
    - uba
  event_types:
    - audit
    - page                 # high volume
```

Fully custom feeds (overrides the defaults entirely):

```yaml
netskope:
  client_options:
    identity:
      oid: $OID
      installation_key: $INSTALLATION_KEY
    platform: json
    sensor_seed_key: netskope
  token: $NETSKOPE_API_TOKEN
  tenant: acme.goskope.com
  feeds:
    - kind: alerts
      type: dlp
    - kind: alerts
      type: malware
    - kind: events
      type: application
      name: saas_activity     # custom EventType label
```
