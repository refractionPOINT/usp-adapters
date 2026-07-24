# Cloudflare Access Adapter

Pulls [Cloudflare Access](https://developers.cloudflare.com/cloudflare-one/policies/access/)
per-request audit logs into LimaCharlie via the
[Access Requests API](https://developers.cloudflare.com/cloudflare-one/insights/logs/dashboard-logs/access-authentication-logs/#per-request-audit-logs).
Events are forwarded in their original Cloudflare JSON form — the adapter does
not reshape payloads.

## What it collects

Every Access authentication event that results in a new HTTP request: policy
evaluations, application logins, and MFA challenges served to Access
applications. Per Cloudflare's docs, purely client-side interactions that never
generate a request are **not** captured by this API — for full session-level
data, see Cloudflare's Logpush (Enterprise-only), which this adapter does not
use.

Each shipped event has `EventType` `access_requests` and a JSON payload shaped
like:

```json
{
  "created_at": "2026-07-02T15:03:00Z",
  "user_email": "user@example.com",
  "user_id": "b4bf51f2-c72b-50b1-a179-20b76a49b18b",
  "ip_address": "203.0.113.10",
  "country": "US",
  "app_domain": "kibana.example.com",
  "app_name": "CFZT_App_kibana_example_com",
  "app_uid": "968a35b8-def0-45de-9d2f-f65d514fa76c",
  "app_type": "self_hosted",
  "action": "login",
  "allowed": true,
  "connection": "azureAD",
  "ray_id": "a1cbcd95da9aa9e9"
}
```

## Authentication

Create a Cloudflare API token (**My Profile → API Tokens**) scoped to
**Account → Access: Audit Logs → Read** for the account that owns the Access
applications. The token is sent as a bearer credential
(`Authorization: Bearer <token>`).

You also need the **Account ID**, found on the right sidebar of any zone/account
overview page in the Cloudflare dashboard, or via `GET /accounts` /
`wrangler whoami`.

The legacy global API key (`X-Auth-Email` / `X-Auth-Key`) is not supported —
use a scoped API token.

## Configuration

| Key | Required | Description |
|-----|----------|-------------|
| `client_options` | yes | Standard USP adapter options (see the repo README). |
| `api_token` | yes | Cloudflare API token scoped to `Access: Audit Logs Read`. |
| `account_id` | yes | Cloudflare account ID that owns the Access applications. |
| `base_url` | no | Full API root override. Default `https://api.cloudflare.com/client/v4`. |
| `poll_interval` | no | Wait between polls. Default `1m` (1 minute). |
| `initial_lookback` | no | How far back the first poll reaches for historical events. Default `1h` (1 hour). |
| `limit` | no | Records requested per API call. Cloudflare hard-caps this at `1000` server-side regardless of a higher value; the adapter clamps to that cap. Default `1000`. |
| `max_pages` | no | Caps paginated calls made within a single poll, bounding the work done when a poll's window holds more records than fit in one response. Default `100`. |
| `dedupe_ttl` | no | How long a request's `ray_id` is remembered to suppress re-shipping it across overlapping polls. Default `24h`. |
| `retry_base_delay` / `max_retry_delay` / `max_retry_attempts` | no | Transient-failure retry tuning. Durations default to `5s` / `30s`; `max_retry_attempts` defaults to `3`. |

> **Duration fields in a YAML config file** (`poll_interval`, `initial_lookback`, `dedupe_ttl`,
> `retry_base_delay`, `max_retry_delay`) must be given as a **Go duration string**
> (e.g. `6h`, `90s`, `1m30s`) — not a raw nanosecond integer. This repo's YAML loader
> (`gopkg.in/yaml.v3`) intentionally refuses to decode a bare integer into a
> `time.Duration` field (unlike `poll_interval=60000000000` passed as a CLI
> `key=value` argument, which uses a different, integer-friendly decode path).
> Using a raw integer in a YAML file fails with an error like
> `cannot unmarshal !!int ... into time.Duration`.

## How polling works

The `access_requests` endpoint has no cursor — it is filtered by a
`since`/`until` time window and capped at `limit` results per call
(`direction` is always requested as `asc`, oldest-first, so pagination can walk
forward in time). Each poll:

1. Queries `[watermark, now)`, where `watermark` starts at
   `now - initial_lookback` on adapter start.
2. If the response is a full page (`== limit` records), advances `since` to
   the last record's `created_at` and re-queries the same window — repeated
   until a short page shows the window is exhausted, or `max_pages` is hit.
3. Sets the watermark to `now` (the poll's `until`) for the next poll.

Because windows can abut or overlap (e.g. after a `max_pages` cutoff, or a
record repeated across the boundary), an in-memory deduper keyed on `ray_id`
guarantees each request is shipped to LimaCharlie exactly once. Records lacking
a `ray_id` fall back to a content hash so deduplication still works.

Errors coming back from the Cloudflare API — including persistent 5xx, a
malformed response, or an authentication failure (401/403) — are **logged as
warnings and never stop the adapter**. The poll is skipped and retried on the
next interval, since restarting the adapter cannot fix a problem on
Cloudflare's side (and in the hosted cloud-adapter environment, a fatal error
tears the adapter down and eventually disables it). Only a failure *delivering*
events to LimaCharlie is fatal, because there a restart re-establishes the
connection.

Transient failures (HTTP 5xx, 429, network errors) are retried within a single
poll with exponential backoff before the poll is skipped.

## Example

```
./general cloudflare_access \
  client_options.identity.oid=$OID \
  client_options.identity.installation_key=$INSTALLATION_KEY \
  client_options.platform=json \
  client_options.sensor_seed_key=cloudflare_access \
  api_token=$CF_API_TOKEN \
  account_id=$CF_ACCOUNT_ID
```

YAML:

```yaml
cloudflare_access:
  client_options:
    identity:
      oid: $OID
      installation_key: $INSTALLATION_KEY
    platform: json
    sensor_seed_key: cloudflare_access
  api_token: $CF_API_TOKEN
  account_id: $CF_ACCOUNT_ID
  # initial_lookback: 6h   # duration string, not nanoseconds -- see note above
```
