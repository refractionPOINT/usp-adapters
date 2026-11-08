# Wiz query pagination

For cursor-paged queries, declare an `$after: String` variable and pass it to
that connection's `after` argument. Select `pageInfo { hasNextPage endCursor }`
as a sibling of the item array selected by `data_path`. Keep results ordered
newest first by the configured time field, as required by the time checkpoint.
The adapter follows each returned cursor with the same time filter, including
when an intermediate page contains no records.

A failed page, GraphQL error, missing continuation cursor or repeated cursor
leaves the time checkpoint unchanged. The next poll retries the window.
The checkpoint advances only after the complete result has been submitted to
the local USP client. This does not add a durable delivery checkpoint.

Queries without `pageInfo` retain their single-response behavior; the adapter
cannot infer pagination that the configured query does not expose. Pagination
with a different variable name, aliased `pageInfo` fields, or multiple independent
connections in one query is not supported by this traversal.
