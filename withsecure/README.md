# WithSecure Adapter

Pulls telemetry from the [WithSecure™ Elements](https://www.withsecure.com/)
cloud (formerly F-Secure) into LimaCharlie via the
[Elements API](https://connect.withsecure.com/api-reference/elements). Events are
forwarded in their original WithSecure JSON form — the adapter does not reshape
payloads.

## What it collects

Four streams, each shipped under its own event type:

| Event type | Elements endpoint | What it carries |
|---|---|---|
| `security_event` | `POST /security-events/v1/security-events` | The EPP protection-engine event stream — malware scanning, DeepGuard, firewall, browsing and connection control, device control, DataGuard, tamper protection, AMSI, application control, collaboration protection, and more. |
| `incident` | `GET /incidents/v1/incidents` | **Broad Context Detections** — WithSecure's correlated EDR incidents, each grouping related activity across one or more hosts. |
| `detection` | `GET /incidents/v1/detections` | The individual detections that make up a BCD, with the process evidence that triggered them (path, name, hash, command line, PID, user, privileges). |
| `audit_log` | `GET /audit-logs/v1/audit-logs` | The Elements administrative audit trail — who changed what. |

`security_event`, `incident` and `detection` are collected by default.
`audit_log` is **off by default**: it is administrative activity rather than
security telemetry, and noisy for most deployments.

## Authentication

Create an API client in the Elements Security Center under **Management →
Organization Settings → API clients**. Read-only access is sufficient — this
adapter never writes.

> ⚠️ The client secret is displayed **only once**, at creation. Save it before
> closing the dialog.

Authentication is OAuth2 client credentials against
`POST /as/token.oauth2`. The adapter mints a token (~30 minute lifetime), caches
it, and renews it transparently; a `401` mid-poll re-mints once before failing.

A bad credential **stops the adapter** rather than looping: no amount of
retrying fixes it, and silently collecting nothing is worse than failing
visibly. Any other API-side failure (5xx, 429, network blip) only skips that
poll.

## Configuration

| Key | Required | Default | Description |
|---|---|---|---|
| `client_id` | yes | | Elements API client ID. |
| `client_secret` | yes | | Elements API client secret. |
| `organization_id` | no | credential's own org | Elements organization UUID to collect. Set this when using a partner (MSSP) credential that manages several organizations. |
| `base_url` | no | `https://api.connect.withsecure.com` | API root override (e.g. the staging host). |
| `collect_security_events` | no | `true` | Collect the EPP security event stream. |
| `collect_incidents` | no | `true` | Collect Broad Context Detections. |
| `collect_detections` | no | `true` | Collect the detections under each BCD. Requires `collect_incidents`. |
| `collect_audit_logs` | no | `false` | Collect the administrative audit trail. |
| `engines` | no | all | Only these security-event engines (e.g. `deepGuard`, `firewall`, `fileScanning`). |
| `engine_groups` | no | all | Only these engine groups: `epp`, `edr`, `ecp` (collaboration protection), `xm` (exposure management). |
| `severities` | no | all | Only these severities: `critical`, `warning`, `info`. |
| `include_archived_incidents` | no | `false` | Also collect archived BCDs. Leaving this off is also faster, per the API docs. |
| `lookback` | no | `1h` | How far back the first poll of each stream reaches. Capped at 30 days for `audit_log`. |
| `poll_interval` | no | `1m` | Wait between polls of each stream. |
| `page_size` | no | endpoint max | Records per page. Clamped per endpoint (security events 200, audit logs 200, incidents 50, detections 100). |
| `max_pages` | no | `100` | Pages walked per poll. The cursor only advances over what was read, so a capped poll resumes where it left off. |
| `dedupe_ttl` | no | `24h` | How long a record id is remembered to suppress re-shipping. |
| `retry_base_delay` | no | `5s` | Backoff base for transient failures. |
| `max_retry_delay` | no | `30s` | Backoff ceiling. |
| `max_retry_attempts` | no | `3` | Attempts before skipping a poll. |
| `user_agent` | no | `LimaCharlie-usp-adapter-withsecure/1.0` | The Elements API rejects requests without a `User-Agent`. |

## How it works

**Incremental polling on a timestamp cursor.** Each stream tracks the newest
timestamp it has seen and asks only for records after it —
`persistenceTimestampStart` for security events, `updatedTimestampStart` for
incidents, `serverTimestampStart` for audit logs — always with
`exclusiveStart=true`. Without that flag the API returns records *greater than
or equal to* the bound, so the boundary record would be re-read on every poll.
Results are requested in ascending order so the cursor advances naturally as
pages are consumed.

After the first poll the cursor is always a timestamp string the API itself
emitted, so its format cannot drift from what the endpoint accepts.

**Anchor pagination.** Responses are `{"items": [...], "nextAnchor": "..."}`;
the adapter follows the opaque anchor until it is absent. `max_pages` bounds one
poll's work — hitting the cap is not data loss, since the cursor only advances
over records actually read.

**Incidents are living objects.** A Broad Context Detection accretes detections
over its lifetime, so the incident stream filters on `updatedTimestamp`, not
`createdTimestamp` — the flow WithSecure's own cookbook prescribes. Each
*version* of an incident ships once (the dedupe key includes its
`updatedTimestamp`), so you see a BCD evolve from `new` to `closed`. Detections
dedupe on their stable `detectionId`, so re-visiting an updated incident only
ships the detections that are genuinely new.

Detections are fetched **per incident** — the API has no organization-wide
detections endpoint — which is why `collect_detections` requires
`collect_incidents`.

**Deduplication.** Consecutive polls deliberately overlap and updated incidents
are re-visited, so an in-memory deduper (`dedupe_ttl`, default 24 h) guarantees
each record ships exactly once. A record missing its id field falls back to a
content hash.

## Rate limits

The Elements API allows 10,000 requests/minute per source IP in general, but
only **300/minute for the endpoints this adapter uses** (security events,
incidents, audit logs). A `429` carries `Retry-After` and is treated as
transient. If you collect many organizations from one host, raise
`poll_interval` or narrow the streams rather than tightening the loop.

## Sample event

A `security_event`:

```json
{
  "id": "07a286cc-99ba-3538-8997-557076ff95ab_0",
  "action": "blocked",
  "engine": "deepGuard",
  "severity": "warning",
  "serverTimestamp": "2026-07-30T09:31:01.092Z",
  "persistenceTimestamp": "2026-07-30T09:31:03.292Z",
  "clientTimestamp": "2026-07-30T09:31:01.000Z",
  "eventTransactionId": "0000-187cf62797634fef",
  "acknowledged": false,
  "message": "DeepGuard blocked a harmful application",
  "description": "DeepGuard event",
  "organization": { "id": "…", "name": "example-org" },
  "device": { "id": "…", "name": "DESKTOP-3D64DAK", "labels": ["finance"] },
  "target": { "id": "…", "name": "DESKTOP-3D64DAK" },
  "userName": "EXAMPLE\\jdoe",
  "details": {
    "path": "C:\\Users\\jdoe\\AppData\\Local\\Temp\\evil.exe",
    "alertType": "deepguard.harmful.blocked",
    "hostIpAddress": "10.1.2.3/24"
  }
}
```

A `detection` under a BCD:

```json
{
  "detectionId": "cc04e914-dbea-4785-83cc-60492f03b97d",
  "incidentId": "2c902c73-e2a6-40fd-9532-257ee102e1c1",
  "deviceId": "3a8f06e1-c3e8-4933-b617-e23597111644",
  "name": "suspiciousPowershellCommand",
  "detectionClass": "PROCESS",
  "severity": "medium",
  "riskLevel": "medium",
  "exePath": "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe",
  "exeHash": "c8f21ef51fa2f2033a9ca7c0cc0412c25065e2ba",
  "cmdl": "powershell -enc SQBFAFgA",
  "pid": 1234,
  "username": "EXAMPLE\\jdoe",
  "createdTimestamp": "2026-07-30T09:30:51.097Z"
}
```

The event time (`TimestampMs`) comes from the record itself:
`persistenceTimestamp` for security events, `updatedTimestamp` for incidents,
`createdTimestamp` for detections, `serverTimestamp` for audit logs.

## Running it

CLI:

```
./general withsecure \
  client_options.identity.oid=$OID \
  client_options.identity.installation_key=$INSTALLATION_KEY \
  client_options.platform=json \
  client_options.sensor_seed_key=withsecure \
  client_id=$WS_CLIENT_ID \
  client_secret=$WS_CLIENT_SECRET
```

YAML:

```yaml
withsecure:
  client_options:
    identity:
      oid: 8cbe27f4-bfa1-4afb-ba19-138cd51389cd
      installation_key: e9a3bcdf-efa2-47ae-b6df-579a02f3a54d
    platform: json
    sensor_seed_key: withsecure
  client_id: "your-elements-api-client-id"
  client_secret: "your-elements-api-client-secret"
  organization_id: "00000000-0000-0000-0000-000000000000"
  collect_audit_logs: true
  severities:
    - critical
    - warning
  poll_interval: 1m
  lookback: 24h
```

## Notes

- **One organization per adapter instance.** A partner credential can see many
  organizations, but each instance collects one `organization_id`. Run one
  instance per organization (each with its own `sensor_seed_key`) to keep them
  on separate sensors.
- Security-event queries **must** carry a time bound; the adapter always sends
  one, so `lookback` is what decides how much history the first poll pulls.
- Audit-log queries cannot span more than 30 days, so `lookback` is capped for
  that stream.
- The `security-events` query is a `POST` with a form-encoded body, not JSON.
  Array filters (`engines`, `severities`) are sent as repeated keys.
- A `403` mentioning a scope means the API client lacks the entitlement for that
  endpoint (for example, a tenant without the EDR subscription has no BCDs) —
  it is a licensing/permission problem, not an adapter bug.

## Testing

```
go test ./withsecure/...
```

The suite runs end-to-end against a mock Elements API that reproduces the real
contract: Basic-auth-only token requests whose body carries nothing beyond
`grant_type`/`scope`, the mandatory `User-Agent`, anchor pagination over the
`{items, nextAnchor}` envelope, `exclusiveStart` cursor semantics, and the flat
`{message, code, transactionId}` error object. No credentials are needed.
