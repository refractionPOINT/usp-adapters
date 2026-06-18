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

With no `feeds` configured, the adapter polls three feeds that together cover
ThreatLocker's primary telemetry surfaces:

| Default feed | ThreatLocker endpoint | What it carries |
|---|---|---|
| `approval_request` | `ApprovalRequest/ApprovalRequestGetByParameters` (`statusId = 1`) | Pending Application Control whitelist requests — one event per new request, shipped exactly once. Well suited to triggering automated investigations in LimaCharlie. |
| `unified_audit` | `ActionLog/ActionLogGetByParametersV2` | The **Unified Audit** — ThreatLocker's combined event stream of `execute` / `install` / `network` / `registry` / `read` / `write` / `move` / `delete` / `baseline` / `powershell` / `elevate` / web activity across every module. Polled on a 5-minute rolling window. |
| `system_audit` | `SystemAudit/SystemAuditGetByParameters` | Portal / administrator activity — logins, policy edits, approval decisions, organization changes. Polled on a 5-minute rolling window. |

`unified_audit` and `system_audit` both *require* a `startDate`/`endDate` filter
on every request; the adapter rewrites those fields automatically (see the
`window` feed setting below). At-least-once delivery is preserved by the
per-feed deduper.

Override `feeds` in your config to add custom feeds (e.g. denied approval
requests, child-organization-scoped queries) or to replace the defaults
entirely.

## Authentication

Create an API token under **Portal → Administration → API Users** in the
ThreatLocker portal. The token is sent verbatim in the `Authorization` header.

The API root is `https://portalapi.<instance>.threatlocker.com/portalapi`, where
`<instance>` is your ThreatLocker instance. Provide it via `instance`, or set
`base_url` to override the whole root.

### Finding your instance

ThreatLocker hosts each tenant on one of several lettered instances (`b`, `c`,
`d`, …) and API tokens are scoped to the instance that minted them. To find
yours, open the ThreatLocker Portal, click the **Help** button in the top-right
corner of any page, and read the letter in parentheses next to **ThreatLocker
Access** (e.g. `ThreatLocker Access (C)` → `instance: c`).

> ⚠️ **A token from one instance returns `403 TOKEN_REVOKED` on every other
> instance** — the API does not distinguish "wrong instance" from a genuinely
> revoked token. If you are confident the token is active and still see
> `TOKEN_REVOKED`, double-check the instance before assuming the token was
> revoked.

## Parent-organization tokens

ThreatLocker scopes every query to the **currently managed organization** — the
organization that minted the token (or the one named by the
`managed_organization_id` header). By default the feeds ask only for *that*
organization's own records (`showChildOrganizations` / `viewChildOrganizations`
are `false`).

This is correct for a token scoped to a single (leaf) organization. It is **not**
what you want for a token scoped to a parent/master organization: a parent has no
endpoints of its own, so with the child-org flags off you will see

- no `approval_request` events (the parent has no pending approvals of its own),
- `system_audit` events that are mostly the adapter's *own* API polling, and
- an HTTP 500 from the `unified_audit` (`ActionLog`) endpoint.

For a parent/master token, set **`include_child_organizations: true`**. The
default feeds then flip their child-org flags on and return the parent's children
(and grandchildren) too. Alternatively, set `managed_organization_id` to a
specific child's GUID to scope every request down to that one child.

## Configuration

| Key | Required | Description |
|-----|----------|-------------|
| `client_options` | yes | Standard USP adapter options (see the repo README). |
| `api_key` | yes | ThreatLocker API token. |
| `instance` | yes* | ThreatLocker instance identifier (e.g. `g`). *Required unless `base_url` is set. |
| `base_url` | no | Full API root override, e.g. `https://portalapi.g.threatlocker.com/portalapi`. |
| `managed_organization_id` | no | Scopes every request to that organization via the `managedOrganizationId` header. |
| `include_child_organizations` | no | When `true`, the default feeds include child (and grandchild) organizations in their results. **Set this when the token is scoped to a parent/master organization** — see [Parent-organization tokens](#parent-organization-tokens). Default `false`. Only affects the default feeds; with custom `feeds`, set the flags in each feed's `parameters` yourself. |
| `feeds` | no | List of feeds to poll. Defaults to the three feeds listed under [Out of the box](#out-of-the-box). |
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
| `window` | no | When set (e.g. `5m`), rewrites `startDate` / `endDate` on every poll to a rolling `[now-window-poll_interval, now]` range. Required for endpoints that mandate a date range (`ActionLog`, `SystemAudit`). The overlap with the previous poll is absorbed by the deduper. |
| `start_date_field` / `end_date_field` | no | Override the request-body field names used by `window`. Defaults: `startDate` / `endDate`. |

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

Adding a custom feed (e.g. ship *denied* approval requests too) on top of the
defaults, via a YAML config file:

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
    # Re-declare the three defaults explicitly so this list fully replaces them,
    # then append the custom feed.
    - name: approval_request
      url: ApprovalRequest/ApprovalRequestGetByParameters
      parameters:
        statusId: 1            # pending
        showChildOrganizations: false
      id_field: approvalRequestId
    - name: unified_audit
      url: ActionLog/ActionLogGetByParametersV2
      window: 5m
      parameters:
        paramsFieldsDto: []
        groupBys: []
        exportMode: false
        showTotalCount: false
        showChildOrganizations: false
        onlyTrueDenies: false
        simulateDeny: false
      id_field: actionLogId
    - name: system_audit
      url: SystemAudit/SystemAuditGetByParameters
      window: 5m
      parameters:
        viewChildOrganizations: false
      id_field: systemAuditId
    - name: approval_request_denied
      url: ApprovalRequest/ApprovalRequestGetByParameters
      parameters:
        statusId: 3            # denied
        showChildOrganizations: false
      id_field: approvalRequestId
```
