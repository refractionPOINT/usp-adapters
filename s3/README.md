# Source deletion compatibility

Normal collection deletes a source object only after delivery has been
acknowledged. It first sends a key DELETE with `If-Match` against the downloaded
ETag. Supported endpoints retain this identity check; versioned buckets keep
key-delete semantics rather than deleting a specific version. One-time loads
retain source objects.

Some S3-compatible endpoints explicitly reject conditional DELETE. For a 501 or
`NotImplemented` response, or a recognized unsupported `If-Match`/conditional
DELETE error, the adapter retries that delivered object's key DELETE without the
condition. This restores legacy behavior automatically, without a setting. A
warning reports the fallback attempt count for the cleanup batch, its lifetime
total, and reduced replacement protection. Warning callbacks run after cleanup
workers join. Concurrent cleanup remains bounded to eight requests.

On this fallback path an object replaced after download can be deleted without
having delivered the replacement. This is the legacy unconditional-delete
limitation. Every fallback requires its own explicit unsupported response; no
endpoint-wide capability cache bypasses subsequent condition checks. Legacy
endpoints therefore incur an extra rejected request per deletion.

A 412 identity mismatch or 404 missing object is skipped for relisting, never
retried unconditionally. Permission failures, arbitrary server/transport errors,
and unrelated header errors do not enable fallback. A genuine failure of the
plain DELETE also retains the existing sticky error behavior. ETag-based
conditional deletion cannot distinguish identical-ETag replacements.
