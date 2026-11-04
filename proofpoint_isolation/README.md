# Proofpoint Isolation Adapter

Pulls usage data from the
[Proofpoint Isolation](https://www.proofpoint.com/us/products/cloud-security/isolation)
Reporting API into LimaCharlie. Records are forwarded in their original JSON
form; the adapter does not reshape payloads.

Proofpoint Isolation comes as two products. Each has its own Reporting API host,
and the adapter can collect from either or both:

| Product | Reporting API endpoint | Event type |
|---|---|---|
| Browser and Email Isolation | `https://proofpointisolation.com/api/v2/reporting/usage-data` | `browser_email_isolation` |
| URL Isolation | `https://urlisolation.com/api/v2/reporting/usage-data` | `url_isolation` |

The adapter uses API version 2. V2 has the same request interface as V1 and
differs only in the JSON it returns: Proofpoint adds new data elements to V2
results as they become available.

References:

- Proofpoint's own Reporting API client, [`pfptcommunity/iso2web`](https://github.com/pfptcommunity/iso2web).
  It is the clearest public description of the request/job/page flow.
- [Proofpoint API code snippets](https://github.com/pfptcommunity/pfptcommunity)
- The full API documentation is in the Isolation console under
  **Product Settings → Reporting API**.

## Authentication

In each Isolation console, go to **Product Settings → Reporting API** and copy
the Reporting API key. Supply the Browser/Email Isolation key as `web_api_key`
and the URL Isolation key as `url_api_key`. Set at least one. A product whose
key is not set is not polled.

The key goes in an `Authorization: Bearer <key>` header, never in the URL. The
API answers HTTP 403 for an invalid key and 400 when the key is missing. Depending on
the tenant, one key may work for both products; if so, set it as both
`web_api_key` and `url_api_key`.

## Configuration

| Key | Required | Description |
|---|---|---|
| `client_options` | yes | Standard USP client options (`identity.oid`, `identity.installation_key`, `platform: json`, `sensor_seed_key`, ...). |
| `web_api_key` | one of | Reporting API key for Browser and Email Isolation. |
| `url_api_key` | one of | Reporting API key for URL Isolation. |
| `web_base_url` | no | Override the Browser/Email Isolation API root. Default `https://proofpointisolation.com`. |
| `url_base_url` | no | Override the URL Isolation API root. Default `https://urlisolation.com`. |
| `page_size` | no | Records per page, 1–10000. Default `10000`. |
| `poll_interval_seconds` | no | Wait between queries. Default `300`. |
| `initial_lookback_minutes` | no | How far back the first query reaches when the adapter starts. Default `60`, maximum `43200` (30 days, the API's retention). |
| `overlap_minutes` | no | How far each query reaches back before the end of the previous one, to catch records the API indexes late. Default `15`. |
| `job_poll_interval_seconds` | no | Wait between checks of a report job that has not completed yet. Default `5`. |
| `job_timeout_minutes` | no | How long a report job may stay incomplete before the query is dropped and retried on the next poll. Default `10`. |
| `max_pages` | no | Most pages walked per query. Guards against a page token that never ends. Default `1000`. |
| `dedupe_ttl_hours` | no | How long a shipped record is remembered so it is not shipped again. Must be longer than the overlap plus the poll interval. Default `24`. |
| `max_retry_attempts` | no | Tries per request on transient failures (5xx, 429, network), with exponential backoff. Default `3`. |

## What the data looks like

There is one record per request the user made through Isolation. Each record
has the user, the URL, and the URL's classification, disposition and
categories. A V2 record looks like this (sanitized):

```json
{
  "userId": "uid-0001",
  "userName": "user@example.com",
  "url": "https://example.com/",
  "date": "2025-01-01T19:44:35.000+0000",
  "region": "US",
  "zone": "CENTRAL",
  "classification": "SUSPICIOUS",
  "disposition": "ISOLATED",
  "categories": ["Newly Registered Domains"]
}
```

The event time comes from `date`. Records from Browser/Email Isolation are
tagged `browser_email_isolation` and records from URL Isolation are tagged
`url_isolation`.

## How it works

The usage-data endpoint runs report jobs. For each product, on every poll the
adapter does the following:

1. It sends `GET /api/v2/reporting/usage-data?from=…&to=…&pageSize=…` to start
   a report job for the time range `[from, now]`. Times are UTC in the format
   `YYYY-MM-DDTHH:MM:SS.mmm`.
2. While the job's `status` is not `COMPLETED`, it polls again with the
   returned `jobId`. A status that looks like a failure (for example `FAILED`),
   or a job still incomplete after `job_timeout_minutes`, drops the query for
   this poll.
3. Once the job is `COMPLETED`, it ships the page's `data` and requests the
   next page by sending back the `jobId` and the returned `pageToken`. It stops
   when a response has no `pageToken`.

The first query starts `initial_lookback_minutes` ago. Each later query starts
`overlap_minutes` before the end of the previous query, so records the API
indexes a little late are still picked up. If a query fails part way, the
window does not move forward and the next poll retries the same range.

Usage records have no ID field, so the adapter deduplicates on a hash of each
record's content, separately for each product. This ships each record once even
though consecutive queries overlap. If two records are identical in every
field, they collapse into one.

Errors from Proofpoint's side never stop the adapter. This covers a rejected
key, 4xx and 5xx responses, failed jobs and malformed responses. The adapter
logs a warning, skips that poll, and retries on the next interval, so fixing a
key takes effect without a restart. A bad key for one product does not affect
the other. Only failing to deliver to LimaCharlie is fatal.

The adapter keeps its position in memory. After a restart it collects from
`initial_lookback_minutes` ago again. Raise that value to cover longer outages,
up to the API's 30-day retention.

## Running

Both products, from the CLI:

```
./general proofpoint_isolation \
  client_options.identity.oid=$OID \
  client_options.identity.installation_key=$INSTALLATION_KEY \
  client_options.platform=json \
  client_options.sensor_seed_key=proofpoint_isolation \
  web_api_key=$PP_ISOLATION_WEB_API_KEY \
  url_api_key=$PP_ISOLATION_URL_API_KEY
```

URL Isolation only, from a YAML config file:

```yaml
proofpoint_isolation:
  client_options:
    identity:
      oid: $OID
      installation_key: $INSTALLATION_KEY
    platform: json
    sensor_seed_key: proofpoint_isolation
  url_api_key: $PP_ISOLATION_URL_API_KEY
  poll_interval_seconds: 300
  initial_lookback_minutes: 1440
```
