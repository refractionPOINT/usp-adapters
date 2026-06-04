# Gmail Adapter

Collects telemetry from one or many Gmail mailboxes using the
[Gmail REST API](https://developers.google.com/workspace/gmail/api/reference/rest). Beyond
incoming-email telemetry, it can collect the mailbox configuration and change
signals most relevant to **Business Email Compromise (BEC)** — the mail rules,
forwarding, aliases, delegates, protocol access, and deletions an intruder uses
to persist, exfiltrate mail, and cover their tracks.

Each signal is an independent, opt-in **capability** that ships its own event
type. They are all readable with the default `gmail.readonly` scope.

With the service-account flow the adapter can watch **many mailboxes at once** —
an explicit list, or every mailbox in a Workspace domain via auto-discovery — and
ships **each mailbox to its own LimaCharlie sensor**. See
[Multiple mailboxes](#multiple-mailboxes).

## Capabilities

Enable any combination with the `collect_*` flags below. **If you set none, the
adapter defaults to message telemetry only** (`collect_messages`), preserving the
original behavior.

| Flag | Event type(s) | Gmail API | What it gives you / BEC relevance |
|------|---------------|-----------|-----------------------------------|
| `collect_messages` | `gmail_message` | [`messages.list`](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/list) + [`messages.get`](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/get) | Incoming email as telemetry (the original behavior). The raw signal for phishing/lure detection. |
| `collect_filters` | `gmail_filter` | [`settings.filters.list`](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings.filters/list) | Mail rules. Attackers create rules that auto-delete or auto-forward, or move replies about invoices/wires out of the inbox so the victim never sees them. |
| `collect_forwarding` | `gmail_forwarding_address`, `gmail_auto_forwarding` | [`settings.forwardingAddresses.list`](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings.forwardingAddresses/list), [`settings.getAutoForwarding`](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings/getAutoForwarding) | Forwarding destinations and the account-wide auto-forward toggle. A classic mail-exfiltration vector. |
| `collect_send_as` | `gmail_send_as` | [`settings.sendAs.list`](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings.sendAs/list) | Send-as / "from" identities. An added identity is an impersonation/persistence signal. |
| `collect_delegates` | `gmail_delegate` | [`settings.delegates.list`](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings.delegates/list) | Mailbox delegates. Granting a delegate is persistence. **Workspace only** — see the note below. |
| `collect_imap_pop` | `gmail_imap`, `gmail_pop` | [`settings.getImap`](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings/getImap), [`settings.getPop`](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings/getPop) | IMAP/POP access. Enabling these allows bulk mailbox download via a desktop client, bypassing browser-session controls. |
| `collect_vacation` | `gmail_vacation` | [`settings.getVacation`](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings/getVacation) | The vacation responder, occasionally abused for harvesting/social engineering. |
| `collect_history` | `gmail_history` | [`users.history.list`](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.history/list) | Mailbox changes: message **deletions** and **label changes** (marking a security alert read, trashing the fraud thread) — how an intruder covers their tracks. |

> **Delegates are Workspace-only.** Google exposes
> `settings.delegates.list` only to service-account clients with domain-wide
> delegation. On a consumer account (or without delegation) it returns an error,
> which the adapter logs and skips — it does **not** stop the adapter or affect
> the other capabilities.

## How it works

### Message telemetry (`collect_messages`)

1. Each poll lists message ids matching the configured `query` (default
   `in:inbox`), with an `after:<epoch>` time bound appended automatically so only
   recent messages are listed. Listings are paginated.
2. Each message is fetched at the configured `format` (default `full`). The full
   message resource (nested `payload` with headers, body, and parts) is forwarded
   verbatim.
3. A deduper keyed on the immutable Gmail message id guarantees each message
   ships **exactly once**, even though overlapping windows re-list recent ids.
4. The event timestamp is taken from the message's `internalDate`
   (epoch milliseconds).
5. The high-water mark only advances after a fully-successful poll, so a failed
   poll is re-covered on the next cycle with no gaps.

### Configuration-state capabilities (filters, forwarding, send-as, delegates, imap/pop, vacation)

These are **change-only**: an item is shipped when it first appears or its
content changes, and suppressed otherwise. Change detection reuses the deduper,
keyed on a hash of the item's content, so the steady state is not re-shipped on
every poll — what you see is the *appearance or modification* of a rule, address,
alias, or delegate. They poll on the slower `settings_poll_interval` (default
15m), since configuration changes rarely.

> On adapter restart the in-memory deduper is empty, so the current state is
> re-emitted once as a fresh baseline. Write detections against the state of these
> events rather than treating every event as a brand-new change.

### Mailbox history (`collect_history`)

On the first run the adapter records a baseline `historyId` from the mailbox
profile and ships nothing (there is no prior state to diff). Each later poll lists
[`users.history.list`](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.history/list)
forward from the cursor — filtered to `messageDeleted`, `labelAdded`, and
`labelRemoved` (new mail is already covered by `collect_messages`) — ships each
record, and advances the cursor only after a fully-successful pass. Gmail retains
history for roughly a week; if the cursor ages out (a 404), the adapter
re-baselines and resumes rather than stopping.

## Authentication

Choose **one** of two modes.

### 1. OAuth 2.0 refresh token (a single mailbox)

For collecting one user's mailbox. Create an OAuth client (Desktop or Web) in the
Google Cloud console, enable the Gmail API, and complete the authorization-code
flow once to obtain a refresh token for the `gmail.readonly` scope.

| Field | Description |
|-------|-------------|
| `client_id` | OAuth client id |
| `client_secret` | OAuth client secret |
| `refresh_token` | Long-lived refresh token for the mailbox owner |

### 2. Service account with domain-wide delegation (Google Workspace)

For monitoring a Workspace mailbox without per-user consent. Create a service
account, enable domain-wide delegation, and in the Workspace Admin console
authorize its client id for the `https://www.googleapis.com/auth/gmail.readonly`
scope. The adapter signs a JWT assertion impersonating the `subject`.

| Field | Description |
|-------|-------------|
| `service_account_credentials` | The service account JSON key, inline |
| `service_account_file` | Path to the service account JSON key file (alternative to the inline form) |
| `subject` | A single mailbox owner to impersonate, e.g. `user@yourdomain.com` |

Provide the mailbox(es) with `subject` (one), `subjects` (a list), and/or
`discover_mailboxes` (the whole domain) — see [Multiple mailboxes](#multiple-mailboxes).
At least one of these is required.

## Multiple mailboxes

The service-account flow can collect more than one mailbox. Each mailbox is
impersonated independently and **shipped to its own sensor**: when more than one
mailbox is collected, the sensor seed key is derived as
`<client_options.sensor_seed_key>/<mailbox-address>` and the sensor hostname is
set to the mailbox address. (A single explicit `subject` keeps the
`sensor_seed_key`/hostname you configured, verbatim.)

There are two ways to enumerate mailboxes, and they can be combined (the union is
collected):

### Static list (`subjects`)

List the mailboxes explicitly. Good for a fixed set of high-value mailboxes.

```yaml
gmail:
  service_account_file: /secrets/gmail-collector.json
  subjects:
    - ceo@yourdomain.com
    - cfo@yourdomain.com
    - ap@yourdomain.com
```

### Auto-discovery (`discover_mailboxes`)

Enumerate the Workspace domain's mailboxes via the Admin SDK
[Directory API `users.list`](https://developers.google.com/admin-sdk/directory/reference/rest/v1/users/list),
re-run on `discovery_interval` so newly-provisioned mailboxes are picked up and
deprovisioned ones are dropped automatically (their collector is stopped and torn
down). Suspended accounts are skipped unless `include_suspended` is set. If a
discovery pass comes back empty or truncated while mailboxes are already being
collected, the current set is **kept** (and a warning logged) rather than torn
down — a safety net against a misconfigured `discovery_query` or a transient API
blip.

Discovery has **two extra requirements** beyond the Gmail collection itself:

1. **An admin to impersonate** — `admin_subject` must be a Workspace admin user
   the service account impersonates for the Directory call (the Gmail collection
   of each discovered mailbox still impersonates that mailbox).
2. **An extra delegated scope** — in the Workspace Admin console, the service
   account's client id must additionally be authorized for
   `https://www.googleapis.com/auth/admin.directory.user.readonly`.

```yaml
gmail:
  service_account_file: /secrets/gmail-collector.json
  discover_mailboxes: true
  admin_subject: admin@yourdomain.com
  # customer: my_customer        # default; or restrict to a single domain:
  # domain: yourdomain.com
  # discovery_query: "orgUnitPath='/Finance'"   # optional Directory user filter (note the quoting)
  discovery_interval: 1h
```

If a discovery pass fails (e.g. the directory scope is missing), it is logged and
the current set of mailboxes keeps collecting — discovery never stops the adapter
or the explicitly-listed mailboxes.

### Multi-mailbox knobs

| Field | Default | Description |
|-------|---------|-------------|
| `subjects` | — | Static list of mailboxes to impersonate (service-account flow) |
| `discover_mailboxes` | `false` | Enumerate the domain's mailboxes via the Directory API |
| `admin_subject` | — | Admin user to impersonate for the Directory API (required with `discover_mailboxes`) |
| `customer` | `my_customer` | Directory API customer id (mutually exclusive with `domain`) |
| `domain` | — | Restrict discovery to one domain of a multi-domain Workspace |
| `discovery_query` | — | Optional Directory API user search filter |
| `discovery_interval` | `1h` | How often discovery re-enumerates |
| `include_suspended` | `false` | Also collect suspended mailboxes |
| `max_concurrent_polls` | `10` | Cap on how many mailboxes poll the Gmail API at once (protects the per-project quota) |

## Configuration

### Capability toggles

| Field | Default | Description |
|-------|---------|-------------|
| `collect_messages` | on when no capability is set | Ship incoming email as `gmail_message` |
| `collect_filters` | `false` | Ship mail filters/rules as `gmail_filter` |
| `collect_forwarding` | `false` | Ship `gmail_forwarding_address` + `gmail_auto_forwarding` |
| `collect_send_as` | `false` | Ship send-as aliases as `gmail_send_as` |
| `collect_delegates` | `false` | Ship delegates as `gmail_delegate` (Workspace only) |
| `collect_imap_pop` | `false` | Ship IMAP/POP access as `gmail_imap` / `gmail_pop` |
| `collect_vacation` | `false` | Ship the vacation responder as `gmail_vacation` |
| `collect_history` | `false` | Ship deletions/label changes as `gmail_history` |
| `settings_poll_interval` | `15m` | Cadence for the configuration-state capabilities (messages and history poll on `poll_interval`) |

### Collection knobs

| Field | Default | Description |
|-------|---------|-------------|
| `user_id` | `me` | Mailbox path segment for the refresh-token flow (`me` or an email address). Ignored by the service-account flow, where each mailbox is reached as `me` under its own impersonation |
| `query` | `in:inbox` | Gmail [search query](https://support.google.com/mail/answer/7190); a time bound is appended automatically — do not add one |
| `scopes` | `gmail.readonly` | OAuth scopes to request |
| `format` | `full` | Message detail: `minimal`, `full`, `raw`, or `metadata` |
| `metadata_headers` | — | Headers to keep when `format` is `metadata` |
| `label_ids` | — | Only list messages carrying all of these label ids |
| `include_spam_trash` | `false` | Include SPAM and TRASH messages |
| `max_results` | `100` | Page size for `messages.list` (max 500) |
| `poll_interval` | `5m` | Wait between polls |
| `overlap` | `2m` | Window backdating to avoid gaps from late-indexed mail; re-listed messages are deduped |
| `initial_lookback` | `0` | On startup, reach back this far to backfill recent mail |
| `dedupe_ttl` | `168h` (7d) | How long a message id is remembered to suppress re-shipping |
| `retry_base_delay` | `5s` | Base backoff for transient API failures |
| `max_retry_delay` | `30s` | Max backoff for transient API failures |
| `max_retry_attempts` | `3` | Attempts per request before abandoning a poll |

> **Note:** the `gmail.metadata` scope does not allow the `q` search parameter.
> If you restrict the adapter to that scope, leave `query` empty and rely on
> `label_ids` / `include_spam_trash` instead. The default `gmail.readonly` scope
> covers every capability, including all the settings/history reads; the narrower
> `gmail.metadata` scope cannot read the settings sub-resources, so a capability
> using them will be logged and skipped.

## Examples

Refresh-token flow (single mailbox), via CLI:

```
./general gmail \
  client_options.identity.oid=$OID \
  client_options.identity.installation_key=$IK \
  client_options.platform=json \
  client_options.sensor_seed_key=gmail \
  client_id=$GMAIL_CLIENT_ID \
  client_secret=$GMAIL_CLIENT_SECRET \
  refresh_token=$GMAIL_REFRESH_TOKEN \
  query="in:inbox" \
  poll_interval=5m
```

Service-account flow (Workspace), via YAML:

```yaml
gmail:
  client_options:
    identity:
      oid: 11111111-1111-1111-1111-111111111111
      installation_key: e9a3bcdf-efa2-47ae-b6df-579a02f3a54d
    platform: json
    sensor_seed_key: gmail
  service_account_file: /secrets/gmail-collector.json
  subject: soc-mailbox@yourdomain.com
  user_id: me
  query: "in:inbox"
  initial_lookback: 24h
  poll_interval: 5m
```

BEC monitoring of a Workspace mailbox — telemetry plus the persistence,
exfiltration, and tamper signals, all on (delegates requires the service-account
flow):

```yaml
gmail:
  client_options:
    identity:
      oid: 11111111-1111-1111-1111-111111111111
      installation_key: e9a3bcdf-efa2-47ae-b6df-579a02f3a54d
    platform: json
    sensor_seed_key: gmail
  service_account_file: /secrets/gmail-collector.json
  subject: soc-mailbox@yourdomain.com
  collect_messages: true
  collect_filters: true
  collect_forwarding: true
  collect_send_as: true
  collect_delegates: true
  collect_imap_pop: true
  collect_vacation: true
  collect_history: true
  poll_interval: 5m
  settings_poll_interval: 15m
```

BEC signals only (no message telemetry) for a single mailbox, via CLI — set the
BEC flags and leave `collect_messages` unset/false:

```
./general gmail \
  client_options.identity.oid=$OID \
  client_options.identity.installation_key=$IK \
  client_options.platform=json \
  client_options.sensor_seed_key=gmail \
  client_id=$GMAIL_CLIENT_ID \
  client_secret=$GMAIL_CLIENT_SECRET \
  refresh_token=$GMAIL_REFRESH_TOKEN \
  collect_filters=true \
  collect_forwarding=true \
  collect_send_as=true \
  collect_history=true
```

## Error handling

- **401 Unauthorized** — the access token is transparently refreshed once and the
  request retried.
- **429 / 5xx / 403 rate-limit** — treated as transient and retried with
  exponential backoff.
- **Rejected credentials** (a dead refresh token, a bad service account key, or a
  delegation/scope problem surfacing as a persistent 401/403) — that **mailbox's**
  collector stops, since it needs operator attention rather than endless retries.
  Other mailboxes are unaffected (a rejected impersonation for one subject does
  not imply the others are dead); if every mailbox fails, the adapter winds down.
- **A discovery pass failing** (e.g. the admin lacks the directory scope) — logged
  as a warning; the currently-collecting mailboxes (including the static ones)
  keep running, and discovery retries on the next interval.
- **404 on a single message** (deleted between the list and the fetch) — skipped;
  the poll continues.
- **A BEC capability failing** (e.g. `delegates` on a consumer account, or a
  settings sub-resource unreadable under the chosen scope) — logged as a warning
  and skipped for that cycle. It does **not** stop the adapter or affect the other
  capabilities. A genuine credential rejection (a 401 / dead token) still stops
  the adapter, as it does for message collection.
- **404 on `users.history.list`** (the cursor aged out of Gmail's ~1 week
  retained history) — the adapter re-baselines from the current mailbox state and
  resumes on the next cycle.
