# QuickBooks Online Adapter

Collects change / activity data from a [QuickBooks Online](https://quickbooks.intuit.com/)
company into LimaCharlie, via the Accounting API's **ChangeDataCapture (CDC)**
operation. Each changed entity is forwarded in its original QuickBooks JSON
form — the adapter does not reshape payloads — tagged with its entity type as
the LimaCharlie `EventType`.

## What this collects (and what it does not)

> ⚠️ **QuickBooks Online's user-attributed "Audit Log"** — the UI feature under
> *Settings → Audit Log* that shows *who* did *what* — **is not exposed through
> any public Intuit API.** Intuit confirms this repeatedly; the Audit Log is an
> internal report only available in the web UI.

The authoritative programmatic way to track activity in a QuickBooks company is
**ChangeDataCapture**, which this adapter polls. CDC returns the **full current
state of every entity that changed** since a given timestamp — creations,
updates, and deletions (reported with `"status": "Deleted"`). It tells you
*what changed and when*, but not the acting user and not a field-level diff.

If you need a true user-attributed audit trail, that data is not available from
QuickBooks programmatically and this adapter cannot provide it.

References:
- [ChangeDataCapture operation](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/changedatacapture)
- [CDC overview](https://developer.intuit.com/app/developer/qbo/docs/learn/explore-the-quickbooks-online-api/change-data-capture)
- [OAuth 2.0](https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization/oauth-2.0)

## Out of the box

With no `entities` configured, the adapter polls CDC for a default set of core
transactional and name-list entities that together form a company's activity
trail:

```
Account, Bill, BillPayment, CreditMemo, Customer, Employee, Estimate,
Invoice, Item, JournalEntry, Payment, Purchase, SalesReceipt, Vendor,
VendorCredit
```

Override `entities` to widen or narrow this list to the objects you care about.

## Authentication

QuickBooks Online uses **OAuth 2.0** (authorization-code grant). There is no
service-account flow: a human must authorize the app once against the company,
which yields a long-lived **refresh token** and the company's **realm id**
(a.k.a. company id). The adapter exchanges the refresh token for short-lived
(1-hour) access tokens and refreshes them automatically.

You need, from the [Intuit developer portal](https://developer.intuit.com/):

| Value | Where it comes from |
|-------|---------------------|
| `client_id` / `client_secret` | Your app → **Keys & credentials**. |
| `refresh_token` | The result of running the OAuth authorization-code flow against the target company (e.g. with Intuit's [OAuth playground](https://developer.intuit.com/app/developer/playground) or your own connect flow). Must be granted the `com.intuit.quickbooks.accounting` scope. |
| `realm_id` | Returned on the OAuth callback (`?realmId=...`); it is the QuickBooks company id. |

### Refresh-token rotation

Intuit may return a **new refresh token** on any refresh call. The adapter
always adopts the latest value for the lifetime of the process and logs a
warning when a rotation happens (the value itself is a secret and is never
logged). The refresh token you configure remains valid until its own expiry, so
a process restart is safe. For very long-lived deployments, persist rotated
tokens from your own OAuth store. If the refresh token is revoked or expires,
the adapter stops with a clear credential error.

## Configuration

| Key | Required | Description |
|-----|----------|-------------|
| `client_options` | yes | Standard USP adapter options (see the repo README). |
| `client_id` | yes | OAuth 2.0 app client id. |
| `client_secret` | yes | OAuth 2.0 app client secret. |
| `refresh_token` | yes | Long-lived OAuth refresh token authorized for the company. |
| `realm_id` | yes | QuickBooks company id (realmId). |
| `entities` | no | List of CDC entity names to poll. Defaults to the set under [Out of the box](#out-of-the-box). |
| `sandbox` | no | Use the sandbox API root instead of production. Default `false`. |
| `base_url` | no | Full API root override (e.g. for testing). When set, `sandbox` is ignored. |
| `token_url` | no | OAuth token endpoint override (e.g. for testing). |
| `minor_version` | no | Pins the Accounting API schema minor version. Default `75`. |
| `poll_interval` | no | Wait between CDC polls, as a Go duration in nanoseconds. Default `300000000000` (5 minutes). |
| `overlap` | no | Extends each poll's window backwards past the previous poll so a slipped cycle leaves no gap. The deduper absorbs the overlap. Default 5 minutes. |
| `initial_lookback` | no | When > 0, the first poll reaches back this far (capped at 30 days) to backfill recent changes on startup. Default `0` (collect from launch time). |
| `dedupe_ttl` | no | How long a change's identity is remembered to suppress re-shipping it. Default 7 days. |
| `retry_base_delay` / `max_retry_delay` / `max_retry_attempts` | no | Transient-failure retry tuning. |

## How polling works

On each poll the adapter issues one CDC request covering the window
`[since - overlap, now]`, where `since` is the high-water mark advanced after
every successful poll. It parses the `CDCResponse` envelope, flattens every
per-entity block, and ships each change exactly once — an in-memory deduper
keyed on `entityType | Id | LastUpdatedTime` absorbs the overlap between
consecutive windows while still emitting a fresh event when the *same* object
is edited again (a new `LastUpdatedTime`).

A few QuickBooks-specific constraints the adapter handles:

- **30-day horizon.** CDC rejects a `changedSince` older than 30 days. The
  adapter clamps the window to just under 30 days and warns if it has to.
- **1000-object cap.** A single CDC response carries at most 1000 objects. If a
  response hits that cap, changes may have been dropped from the window; the
  adapter warns and recommends a shorter `poll_interval` or a narrower
  `entities` list.
- **Rate limits.** QuickBooks throttles at ~500 requests/minute per realm and
  returns HTTP 429 when exceeded. The adapter retries 429 and 5xx responses
  with exponential backoff.
- **Token expiry.** Access tokens last one hour and are refreshed transparently;
  a 401 triggers a one-shot refresh-and-retry. A permanently rejected refresh
  token (revoked/expired, or bad client credentials) stops the adapter with a
  credential error.

The event time of each shipped record is taken from
`MetaData.LastUpdatedTime`, falling back to the collection time when absent.

## Examples

Production, via the CLI (default entity set):

```
./general quickbooks \
  client_options.identity.oid=$OID \
  client_options.identity.installation_key=$INSTALLATION_KEY \
  client_options.platform=json \
  client_options.sensor_seed_key=quickbooks \
  client_id=$QB_CLIENT_ID \
  client_secret=$QB_CLIENT_SECRET \
  refresh_token=$QB_REFRESH_TOKEN \
  realm_id=$QB_REALM_ID
```

Narrowing the entity set and backfilling the last 7 days on startup, via a YAML
config file:

```yaml
quickbooks:
  client_options:
    identity:
      oid: $OID
      installation_key: $INSTALLATION_KEY
    platform: json
    sensor_seed_key: quickbooks
  client_id: $QB_CLIENT_ID
  client_secret: $QB_CLIENT_SECRET
  refresh_token: $QB_REFRESH_TOKEN
  realm_id: $QB_REALM_ID
  entities:
    - Invoice
    - Bill
    - Payment
    - JournalEntry
  initial_lookback: 168h   # 7 days
  poll_interval: 5m
```

Against the sandbox company:

```yaml
quickbooks:
  # ... credentials ...
  sandbox: true
```
