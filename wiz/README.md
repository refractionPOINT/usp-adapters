# Wiz query pagination

For cursor-paged queries, declare an `$after: String` variable and pass it to
that connection's `after` argument. Select `pageInfo { hasNextPage endCursor }`
as a sibling of the item array selected by `data_path`. Keep results ordered
newest first by the configured time field, as required by the time checkpoint.
The adapter follows each returned cursor with the same time filter, including
when an intermediate page contains no records.

Optional-field GraphQL errors with explicit paths produce a warning while valid
records continue. Errors without a path, or errors affecting the connection,
records, paging information, IDs or times, fail the window. Malformed individual
records are skipped with warnings, preserving the existing record-level behavior
without blocking otherwise usable pages.

A failed page, required-data GraphQL error, missing continuation cursor or repeated cursor
leaves the time checkpoint unchanged. The next poll retries the window.
The checkpoint advances only after the complete result has been submitted to
the local USP client. This does not add a durable delivery checkpoint.

Queries without `pageInfo` retain their single-response behavior; the adapter
cannot infer pagination that the configured query does not expose. Pagination
with a different variable name, aliased `pageInfo` fields, or multiple independent
connections in one query is not supported by this traversal.

Before following a cursor, the adapter parses the query's GraphQL syntax to
verify that its single query operation declares `$after` and passes
`after: $after` directly on the connection identified by `data_path`. A query
that exposes pagination without consuming the cursor keeps its existing
first-page collection behavior. It emits one pagination warning per poll naming
this required query shape; this case never calls the fatal error callback. The check does not infer a vendor schema or rewrite
the query. Connection paths expressed through fragments, multiple operations,
and nonstandard cursor argument names also use the first-page compatibility
behavior; use a direct standard query for full pagination.
Queries without a continuation keep their existing behavior.

The adapter currently accumulates the complete query result in memory before
shipping it. Memory therefore grows with the full result, not just one page.
There is no page cap that silently treats a partial result as complete.
