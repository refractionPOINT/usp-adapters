# ThreatLocker Adapter

Pulls events from the [ThreatLocker](https://www.threatlocker.com/) Portal API
into LimaCharlie. Events are forwarded in their original ThreatLocker JSON form
— the adapter does not reshape payloads.

The adapter is intentionally generic. The ThreatLocker Portal API is uniform:
every queryable resource exposes a `<Resource>GetByParameters` endpoint that
takes a `POST` with a JSON body describing the filter, sort order and
pagination. The adapter models each such endpoint as a **feed**, so adding a new
event type is purely a configuration change — no code change required.

## Out of the box

With no `feeds` configured, the adapter polls a single feed: **pending
Application Control approval requests** (`statusId = 1`). Each pending request
is shipped exactly once, which is well suited to triggering automated
investigations in LimaCharlie.

## Authentication

Create an API token under **Portal → Administration → API Users** in the
ThreatLocker portal. The token is sent verbatim in the `Authorization` header.

The API root is `https://portalapi.<instance>.threatlocker.com/portalapi`, where
`<instance>` is your ThreatLocker instance. Provide it via `instance`, or set
`base_url` to override the whole root.

## Configuration

| Key | Required | Description |
|-----|----------|-------------|
| `client_options` | yes | Standard USP adapter options (see the repo README). |
| `api_key` | yes | ThreatLocker API token. |
| `instance` | yes* | ThreatLocker instance identifier (e.g. `g`). *Required unless `base_url` is set. |
| `base_url` | no | Full API root override, e.g. `https://portalapi.g.threatlocker.com/portalapi`. |
| `managed_organization_id` | no | Scopes every request to that organization via the `managedOrganizationId` header. |
| `feeds` | no | List of feeds to poll. Defaults to pending approval requests. |
| `page_size` | no | Records per page. Default `100` (max `1000`). |
| `poll_interval` | no | Wait between polls, as a Go duration in nanoseconds. Default `60000000000` (1 minute). |
| `dedupe_ttl` | no | How long a record id is remembered to suppress re-shipping. Default 7 days. |
| `retry_base_delay` / `max_retry_delay` / `max_retry_attempts` | no | Transient-failure retry tuning. |

### Feed fields

| Key | Required | Description |
|-----|----------|-------------|
| `name` | yes | Labels the feed and becomes the `EventType` of every shipped event. |
| `url` | yes | API path of the `*GetByParameters` endpoint, relative to the API root. |
| `parameters` | no | JSON object merged into the request body (resource-specific filters). |
| `order_by` | no | Sort field. Default `dateTime`. |
| `items_path` | no | Key holding the records array when the response is an object envelope. Auto-detected (`data`, `pageItems`, ...) when empty. |
| `timestamp_field` | no | Path to the record's event time (supports `/`-separated nested paths). Default `dateTime`. |
| `id_field` | no | Path to the record's stable identifier, used for deduplication. Falls back to common id fields, then a content hash. |
| `max_pages` | no | Caps pages fetched per poll. Default `100`. |

## How polling works

On every poll the adapter walks a feed's pages (`pageNumber`/`pageSize`) until
the result set is exhausted (a short or empty page) or the feed's `max_pages`
cap is reached. An in-memory deduper, keyed per feed, guarantees each record is
shipped to LimaCharlie exactly once even though pages are re-fetched on every
poll. Transient API failures (HTTP 5xx, 429, network errors) are retried with
exponential backoff; an authentication failure (401/403) stops the adapter.

The adapter deliberately re-walks every page rather than stopping early at the
first page of already-seen records: the API paginates by offset over a live,
mutable list, so a record can shift across a page boundary between two page
fetches and an early stop could skip it permanently. Re-walking costs more
requests but is correct.

For a large or high-churn feed, bound each poll's work with the feed's
`parameters` (e.g. a date-range filter) and a `max_pages` that comfortably
exceeds the feed's expected size. Querying newest-first (`isAscending = false`,
the default) keeps the most recent records when `max_pages` truncates.

## Examples

Default (pending approval requests), via the CLI:

```
./general threatlocker \
  client_options.identity.oid=$OID \
  client_options.identity.installation_key=$INSTALLATION_KEY \
  client_options.platform=json \
  client_options.sensor_seed_key=threatlocker \
  api_key=$THREATLOCKER_API_TOKEN \
  instance=g
```

Multiple feeds, via a YAML config file:

```yaml
threatlocker:
  client_options:
    identity:
      oid: $OID
      installation_key: $INSTALLATION_KEY
    platform: json
    sensor_seed_key: threatlocker
  api_key: $THREATLOCKER_API_TOKEN
  instance: g
  feeds:
    - name: approval_request
      url: ApprovalRequest/ApprovalRequestGetByParameters
      parameters:
        statusId: 1            # pending
        showChildOrganizations: false
      timestamp_field: dateTime
      id_field: approvalRequestId
    - name: unified_audit
      url: ActionLog/ActionLogGetByParametersV2
      parameters:
        # endpoint-specific filters (e.g. a date range) go here
      timestamp_field: dateTime
      # id_field: set to the record's stable identifier for reliable dedup
```
