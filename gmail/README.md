# Gmail Adapter

Collects incoming email from a Gmail mailbox as telemetry, using the
[Gmail REST API](https://developers.google.com/gmail/api/reference/rest). It
polls [`users.messages.list`](https://developers.google.com/gmail/api/reference/rest/v1/users.messages/list)
on a rolling time window, fetches each newly-seen message with
[`users.messages.get`](https://developers.google.com/gmail/api/reference/rest/v1/users.messages/get),
and ships it to LimaCharlie as a `gmail_message` event. The full message
resource (the nested `payload` with headers, body, and parts) is forwarded
verbatim.

## How it works

1. Each poll lists message ids matching the configured `query` (default
   `in:inbox`), with an `after:<epoch>` time bound appended automatically so only
   recent messages are listed. Listings are paginated.
2. Each message is fetched at the configured `format` (default `full`).
3. A deduper keyed on the immutable Gmail message id guarantees each message
   ships **exactly once**, even though overlapping windows re-list recent ids.
4. The event timestamp is taken from the message's `internalDate`
   (epoch milliseconds).
5. The high-water mark only advances after a fully-successful poll, so a failed
   poll is re-covered on the next cycle with no gaps.

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
| `subject` | The mailbox owner to impersonate, e.g. `user@yourdomain.com` (required) |

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `user_id` | `me` | Mailbox to read (`me` or an email address) |
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
> `label_ids` / `include_spam_trash` instead.

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

## Error handling

- **401 Unauthorized** — the access token is transparently refreshed once and the
  request retried.
- **429 / 5xx / 403 rate-limit** — treated as transient and retried with
  exponential backoff.
- **Rejected credentials** (a dead refresh token, a bad service account key, or a
  delegation/scope problem surfacing as a persistent 401/403) — the adapter stops,
  since these need operator attention rather than endless retries.
- **404 on a single message** (deleted between the list and the fetch) — skipped;
  the poll continues.
