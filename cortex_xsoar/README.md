# Cortex XSOAR adapter

Pulls **incidents** from Palo Alto Networks [Cortex XSOAR](https://www.paloaltonetworks.com/cortex/cortex-xsoar)
(formerly Demisto) into LimaCharlie. Incidents are shipped verbatim, in their
original XSOAR JSON form; LimaCharlie does field parsing and mapping in the cloud.

The same adapter works against both **Cortex XSOAR 6** (on-prem) and **Cortex
XSOAR 8** (cloud) — the request and response shapes are identical, only the API
path prefix and the authentication headers differ, both selected from
configuration.

API references this is built against:

- Search incidents by filter (`POST /incidents/search`) — the [demisto-py SDK
  swagger](https://github.com/demisto/demisto-py/blob/master/server_api_swagger.json)
  (`searchIncidents`, `SearchIncidentsData`, `IncidentFilter`, `Order`) and the
  [`SearchIncidentsV2` content script](https://github.com/demisto/content/blob/master/Packs/CommonScripts/Scripts/SearchIncidentsV2/README.md)
  (sample incident payload).
- API keys & authentication — [Cortex XSOAR 8 "Get started with APIs"](https://docs-cortex.paloaltonetworks.com/r/Cortex-XSOAR/8/Cortex-XSOAR-Administrator-Guide/Get-Started-with-APIs)
  and the [demisto-py auth client](https://github.com/demisto/demisto-py/blob/master/demisto_client/__init__.py)
  (standard vs. advanced key headers; the advanced-key signing scheme).

## Authentication

Create an API key under **Settings → Integrations → API Keys**. XSOAR offers two
key security levels, and the cloud (v8) product additionally requires the key's
numeric **ID**:

| Key type / version | `Authorization` | `x-xdr-auth-id` | Signed (`x-xdr-nonce`, `x-xdr-timestamp`) |
|---|---|---|---|
| Standard, XSOAR 6 | the raw API key | — | — |
| Standard, XSOAR 8 | the raw API key | the key's ID | — |
| Advanced, either version | SHA-256 hex of `apiKey + nonce + timestamp` | the key's ID | yes |

- **Standard** keys send the key verbatim in the `Authorization` header.
- **Advanced** keys are signed per-request: the adapter sends a random 64-char
  nonce and a millisecond timestamp, and `Authorization` carries the lowercase
  hex SHA-256 of `apiKey + nonce + timestamp` (plain SHA-256, not HMAC). The
  server re-validates this against its clock, so **the adapter host's clock must
  be in sync** or requests are rejected with HTTP 401.

Set `advanced: true` for an advanced key, and supply `api_key_id` for XSOAR 8 and
for any advanced key. The `api_key_id` is the **ID** column shown next to the key
in the API Keys table.

## Configuration

| Key | Required | Default | Description |
|---|---|---|---|
| `url` | yes | — | XSOAR server base URL. v6: the server root (`https://xsoar.example.com`). v8: the host from the API key's **Copy URL** action (`https://api-<tenant>...paloaltonetworks.com`); the `/xsoar/public/v1` path is added automatically. |
| `api_key` | yes | — | The API key value. |
| `api_key_id` | for v8 / advanced | — | The numeric key ID (`x-xdr-auth-id`). Required for XSOAR 8 and for advanced keys; ignored for an XSOAR 6 standard key. |
| `api_version` | no | `6` | `6` (endpoints at the server root) or `8` (endpoints under `/xsoar/public/v1`). |
| `advanced` | no | `false` | Use the advanced (signed) API-key scheme. Requires `api_key_id`. |
| `query` | no | — | Lucene filter ANDed with the modified-time cursor every poll, e.g. `type:Phishing and severity:>=2`. Empty collects every incident. |
| `event_type` | no | `incident` | The `EventType` stamped on every shipped message. |
| `timestamp_field` | no | `modified` | Incident field used as the event time. `modified` timestamps each shipped version at its update; set to `created` or `occurred` for the incident's birth/occurrence time. |
| `initial_lookback` | no | `24h` | How far back the first poll reaches. Later polls continue from the cursor. |
| `page_size` | no | `100` | Incidents per page. Capped at `1000` by XSOAR. |
| `poll_interval` | no | `1m` | Wait between polls. |
| `max_pages` | no | `200` | Per-poll page cap, bounding a first poll against a large backlog. |
| `dedupe_ttl` | no | `168h` (7d) | How long an incident version is remembered to suppress re-shipping across overlapping polls. |
| `insecure_skip_verify` | no | `false` | Disable TLS verification. Only for on-prem v6 behind a self-signed certificate. |
| `base_path` | no | — | Override the API path prefix (escape hatch for non-standard deployments). |
| `retry_base_delay` / `max_retry_delay` / `max_retry_attempts` | no | `5s` / `30s` / `3` | Transient-failure retry tuning. |

## How it works

- **Endpoint.** Each poll issues `POST {url}[/xsoar/public/v1]/incidents/search`
  with a `filter` carrying the query, an ascending sort on `modified`, and
  0-indexed `page` / `size`. The response envelope is `{"data": [...], "total": N}`.
- **Incremental by `modified`.** The adapter keeps a high-water cursor on the
  incident `modified` time. Each poll queries `modified:>="<cursor>"` (sorted
  ascending) and advances the cursor to the newest `modified` seen. Because an
  incident's `modified` time changes whenever it is updated (re-opened,
  re-assigned, a note added, …), **updated incidents are re-collected** — so the
  platform sees the incident's evolution, not just its creation.
- **Exactly-once per version.** Deduplication is keyed on `id` + `modified`, so a
  re-fetched boundary incident or an unchanged re-poll is suppressed, while a
  genuine update (new `modified`) ships again as a new version. The first poll's
  reach is bounded by `initial_lookback`.
- **Pagination.** Pages are walked until a short page or the reported `total` is
  reached, bounded by `max_pages` (a warning is emitted if the cap is hit; the
  remainder is collected on later polls as the cursor advances).
- **Errors.** Source-side failures (4xx/5xx, auth rejection, network blips,
  malformed responses) are logged as warnings and the poll is skipped and retried
  next interval — they never stop the adapter (a restart cannot fix a bad key or
  an unreachable instance). 5xx and 429 are retried with exponential backoff.
  Only a failure delivering to LimaCharlie is fatal.

XSOAR publishes no numeric rate limit for this endpoint; the adapter still honors
a `429 Too Many Requests` with backoff if the instance returns one.

## What the data looks like

Each shipped event is a full XSOAR incident object. Notable fields: `id` (stable
identifier), `modified` / `created` / `occurred` (RFC 3339 times; an unset time
such as `closed` on an open incident is the Go zero-time `0001-01-01T00:00:00Z`),
`severity` and `status` as **numbers** (severity 0–4, status 0 Pending / 1 Active
/ 2 Closed / 3 Archive), `name`, `type`, `owner`, `labels`, and `CustomFields`.

```json
{
  "id": "978",
  "version": 7,
  "name": "Suspicious login - case 978",
  "type": "Phishing",
  "severity": 3,
  "status": 1,
  "owner": "analyst1",
  "created": "2026-05-21T13:45:30.162034Z",
  "modified": "2026-05-21T13:50:05.001Z",
  "occurred": "2026-05-21T13:45:30.162034Z",
  "closed": "0001-01-01T00:00:00Z",
  "labels": [{ "type": "Email/from", "value": "attacker@evil.example" }],
  "CustomFields": { "sourceip": "203.0.113.10", "verdict": "malicious" },
  "sourceBrand": "Mail Listener v2",
  "investigationId": "978"
}
```

## Run examples

XSOAR 6 (on-prem), standard key:

```
./general cortex_xsoar \
  client_options.identity.oid=$OID \
  client_options.identity.installation_key=$INSTALLATION_KEY \
  client_options.platform=json \
  client_options.sensor_seed_key=cortex_xsoar \
  url=https://xsoar.example.com \
  api_key=$XSOAR_API_KEY
```

XSOAR 8 (cloud), standard key with an auth ID:

```
./general cortex_xsoar \
  client_options.identity.oid=$OID \
  client_options.identity.installation_key=$INSTALLATION_KEY \
  client_options.platform=json \
  client_options.sensor_seed_key=cortex_xsoar \
  url=https://api-your-tenant.xdr.us.paloaltonetworks.com \
  api_version=8 \
  api_key=$XSOAR_API_KEY \
  api_key_id=$XSOAR_API_KEY_ID
```

YAML (XSOAR 6 with an advanced key, collecting only medium+ incidents):

```yaml
cortex_xsoar:
  url: https://xsoar.example.com
  api_key: "<api-key>"
  api_key_id: "42"
  advanced: true
  query: "severity:>=2"
  poll_interval: 1m
  client_options:
    identity:
      oid: "<oid>"
      installation_key: "<installation-key>"
    platform: json
    sensor_seed_key: cortex_xsoar
```

## Testing

```
go test ./cortex_xsoar/...
```

The suite runs end-to-end against an in-memory mock of the XSOAR API that
reproduces standard and advanced authentication, the `modified` filter and
ascending sort, 0-indexed pagination, and the `{"data":[...],"total":N}`
envelope. It has **not** yet been live-verified against a real Cortex XSOAR
instance; the mock and fixtures mirror the documented API shapes (see the
references above).
