# Palo Alto SaaS Security (Prisma SaaS) Adapter

Reads the event stream of [Palo Alto Networks SaaS Security](https://docs.paloaltonetworks.com/saas-security)
(formerly Prisma SaaS) through its **Log Events API** and ships it to
LimaCharlie. Events are forwarded in their original JSON form; the adapter does
not reshape payloads.

Sources used to build and verify the adapter (no live tenant was available, see
[Verification](#verification)):

- [Log Events API](https://docs.paloaltonetworks.com/saas-security/data-security/syslog-and-api-integration/api-client-integration/log-events-api) (event shapes, long-poll behaviour)
- [Retrieve a Token](https://docs.paloaltonetworks.com/saas-security/data-security/syslog-and-api-integration/api-client-integration/retrieve-a-token) and [Authentication Errors](https://docs.paloaltonetworks.com/saas-security/data-security/syslog-and-api-integration/api-client-integration/authentication-errors)
- [Add Your API Client App](https://docs.paloaltonetworks.com/saas-security/data-security/syslog-and-api-integration/api-client-integration/add-your-api-client-app)
- The [Cortex XSOAR SaaS Security Event Collector](https://github.com/demisto/content/tree/master/Packs/PrismaSaasSecurity/Integrations/SaasSecurityEventCollector)
  source, which is the reference for the `log_events_bulk` endpoint that the
  official pages do not describe.

## Authentication

The adapter uses an **API client** and the OAuth2 *client credentials* grant.

1. Log in to Strata Cloud Manager and go to **Configuration → SaaS Security →
   Settings → Directory & External Services → Add Syslog/API Client**.
2. Choose **API Client** and give it a unique name.
3. Grant the **Log File Access** scope (the `api_access` scope in the token
   request). No other scope is needed.
4. Save, then **copy the Client ID and Client Secret immediately**: the secret
   is shown once and cannot be displayed again.
5. Pick the `region` that matches your tenant (see below).

The adapter exchanges the credentials for a bearer token with
`POST {host}/oauth/token` (HTTP Basic `client_id:client_secret`, form body
`grant_type=client_credentials&scope=api_access`), caches it, and requests a new
one shortly before `expires_in` elapses (about two hours). If an events request
is answered with 401 the token is refreshed once and the request retried.

Licensing: the Log Events API requires the **Data Security** license (or CASB-X
/ CASB-PA, which include it).

## Configuration

| Key | Required | Description |
|-----|----------|-------------|
| `client_options` | yes | Standard USP adapter options (see the repo README). `client_options.platform` defaults to `prisma_saas` when empty. |
| `client_id` | yes | API client ID. |
| `client_secret` | yes | API client secret. In the LimaCharlie cloud this can be a hive secret reference, `hive://secret/<name>`. It is never logged. |
| `region` | no | Tenant region, default `us`. One of `us`, `eu`, `apac`, `uk`, `uk2`, `in`, `au`, `jp`. An unknown value is rejected. |
| `base_url` | no | Full API root override (must be `https`). Takes precedence over `region`; use it for a region this adapter does not know yet. |
| `max_concurrent_polls` | no | Number of concurrent readers, default `4`, at most `30`. See [How it works](#how-it-works). |

Region to API host:

| `region` | Host |
|---|---|
| `us` | `https://api.aperture.paloaltonetworks.com` |
| `eu` | `https://api.aperture-eu.paloaltonetworks.com` |
| `apac` | `https://api.aperture-apac.paloaltonetworks.com` |
| `uk` | `https://api.aperture-uk.paloaltonetworks.com` |
| `uk2` | `https://api.uk2.prisma-saas.paloaltonetworks.com` |
| `in` | `https://api.in1.prisma-saas.paloaltonetworks.com` |
| `au` | `https://api.au1.prisma-saas.paloaltonetworks.com` |
| `jp` | `https://api.jp1.prisma-saas.paloaltonetworks.com` |

## Event types

Every event carries a `log_type`, which becomes the LimaCharlie event type; the
event time is taken from the ISO8601 `timestamp` field (falling back to the time
of receipt if it is missing or unparseable).

| `log_type` | What it is |
|---|---|
| `activity_monitoring` | A user's action on a file, folder or user object in a monitored cloud app. |
| `incident` | A risk (policy match) detected on an asset, with severity 0-5. |
| `remediation` | An action taken to remediate an incident (quarantine, remove public links, ...). |
| `policy_violation` | A policy violation, with the action taken and by whom. |
| `admin_audit` | Administrator activity on the SaaS Security console (logins, settings, policy changes). |

Sample `activity_monitoring` event:

```json
{
  "log_type": "activity_monitoring",
  "item_type": "File",
  "item_name": "My File",
  "user": "John Smith",
  "source_ip": "10.10.10.10",
  "location": "Somewhere, USA",
  "action": "delete",
  "target_name": null,
  "target_type": null,
  "serial": "mySerial",
  "cloud_app_instance": "My Cloud App",
  "timestamp": "2017-02-17T00:18:58.961Z"
}
```

Sample `incident` event:

```json
{
  "log_type": "incident",
  "severity": 1.0,
  "item_type": "File",
  "item_name": "helloworld.java",
  "asset_id": "5e9e38823cedb43cb015b460",
  "item_owner": "Admin User",
  "item_creator": "Admin User",
  "exposure": "COMPANY",
  "cloud_app_instance": "Office 365 8",
  "timestamp": "2020-05-08T23:50:55Z",
  "incident_id": "5eb5ed492021c32b37588a6c",
  "policy_rule_name": "java",
  "datetime_edited": "2020-05-08T23:50:55Z",
  "item_cloud_url": "https://www.sharepoint.com/sites/site/Shared%20Documents/foldername/helloworld.java",
  "item_sha256": "4953946b0bbcd10d872d09561bf0f0988e186b625e4af65c64691adf5af279d4",
  "item_size": 1335,
  "item_verdict": "not available"
}
```

Sample `admin_audit` event:

```json
{
  "log_type": "admin_audit",
  "admin_id": "admin@example.com",
  "admin_role": "super_admin",
  "ip": "203.0.113.7",
  "event_type": "settings",
  "item_type": "general_settings",
  "item_name": "Session timeout",
  "field": "timeout",
  "action": "edit",
  "resource_value_old": "30",
  "resource_value_new": "60",
  "timestamp": "2017-04-06T21:35:10.025Z",
  "serial": "mySerial"
}
```

## How it works

The Log Events API is a **consumable queue**: reading an event removes it, there
is no cursor, offset or replay, and events not read within about an hour expire.
Delivery from the source is therefore at-most-once, and the adapter is built
around never reading more than it can ship.

- **Endpoints.** The adapter prefers `GET /api/v1/log_events_bulk?size=100`,
  which answers `{"events":[...]}` with up to 100 events (204 when the queue is
  empty). If that endpoint answers 404 for a tenant, the adapter falls back for
  good to the documented `GET /api/v1/log_events`, a long-poll that returns one
  event per call (or 204 after a 20 s server-side timeout).
- **Concurrency.** `max_concurrent_polls` readers run side by side. Because the
  single-event endpoint returns one event per call, several long-polls are
  needed for throughput; concurrent reads pop distinct events from the queue.
- **No read-ahead.** Each reader ships every event of the batch it fetched
  before it asks for the next one. When LimaCharlie applies backpressure
  (`ErrorBufferFull`) the same event is retried until it is accepted; it is never
  dropped, and no further reads happen meanwhile. At most one batch per reader is
  ever off PAN's queue but not yet handed to LimaCharlie.
- **Empty queue.** The bulk endpoint answers immediately, so readers wait a few
  seconds between empty polls. The single-event endpoint holds the request, so a
  204 is followed by a new request straight away (unless it came back in under a
  second, which is not a real long-poll timeout).
- **Errors.** 429, 5xx and network errors are retried with exponential backoff
  (1 s up to 60 s, with jitter), honouring `Retry-After` (capped at 5 minutes).
  A 401 refreshes the token once and retries. A persistent 401, any 403, a
  rejected token request (400/401/403, e.g. `invalid_client`) or any other
  permanent 4xx **stops the adapter** with a clear error: the credentials,
  scope or region must be fixed.
- **Shutdown.** `Close` stops new reads, lets requests already in flight finish
  (up to 25 s, the long-poll timeout) and ships what they returned before
  draining the LimaCharlie connection.

## Caveats

- **One API client per tenant.** SaaS Security supports a single API client with
  log access (and a single syslog receiver). Reading with this adapter removes
  events from the queue, so it must not run alongside another consumer such as
  the Cortex XSOAR collector, and only one instance of this adapter may run per
  tenant. Running two splits the events between them.
- **At-most-once.** Events are gone from PAN once read. If the adapter or the
  host crashes between a read and the hand-off to LimaCharlie, those events are
  lost; nothing can be replayed from the source.
- **Retention.** Events not read within about an hour are discarded by PAN, so
  an outage longer than that loses data.
- **Licence.** Requires the Data Security license (or CASB-X / CASB-PA).
- **Throughput.** The single-event fallback is one event per request. Raise
  `max_concurrent_polls` if a tenant produces events faster than that.

## Verification

The adapter is tested end to end against an in-process mock of the API
(`mock_test.go`) that reproduces the documented token and `log_events`
exchanges and the `log_events_bulk` behaviour observed in the XSOAR collector.
It has **not been run against a live tenant** (none was available); the bulk
response envelope (`{"events":[...]}`, max 100 per call, 204 when empty) is taken
from the XSOAR collector source rather than from official documentation.

## Examples

Via the CLI:

```
./general prisma_saas \
  client_options.identity.oid=$OID \
  client_options.identity.installation_key=$INSTALLATION_KEY \
  client_options.platform=prisma_saas \
  client_options.sensor_seed_key=prisma_saas \
  client_id=$PRISMA_SAAS_CLIENT_ID \
  client_secret=$PRISMA_SAAS_CLIENT_SECRET \
  region=us
```

Via a YAML config file:

```yaml
prisma_saas:
  client_options:
    identity:
      oid: $OID
      installation_key: $INSTALLATION_KEY
    platform: prisma_saas
    sensor_seed_key: prisma_saas
  client_id: $PRISMA_SAAS_CLIENT_ID
  client_secret: $PRISMA_SAAS_CLIENT_SECRET
  region: eu
  max_concurrent_polls: 4
```
