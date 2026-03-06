# Server-Internal Keyset Pagination

[← Back to docs](../README.md)

How the streaming query server bounds database-to-server transfer using keyset pagination internally, with an optional continuation token for client resumption.

**Related:** [Metadata Tables](../reference/metadata-tables.md) · [Performance Tuning](../operations/performance-tuning.md)

---

## Problem

MariaDB Connector/J has no server-side cursors. When a `ResultSet` is closed before all rows are consumed, the driver calls `skipRemaining()`, which reads and discards every remaining row from the socket. A query matching 10 million rows transfers all 10 million over the network — even if the consumer stops after 100.

This matters because the early termination algorithm (see [Query Execution](../concepts/query-execution.md)) selects files dynamically: it streams metadata rows and stops when enough results are guaranteed. Without bounded transfer, every streaming RPC risks transferring the entire matching result set just to use a fraction of it.

## Solution

The server internally paginates using keyset cursors. Each internal SQL query is bounded by `LIMIT :batch_size`. After streaming one batch to the client, the server constructs a cursor from the last row and issues the next query. This continues until results are exhausted or the client cancels the stream.

The client sees a continuous stream of rows. Optionally, each streamed row includes a `KeysetCursor` that the client can use as a continuation token in a new RPC to resume from that point.

Maximum wasted database-to-server transfer per internal batch: one batch.

## Continuation Token

When the request sets `include_cursor = true`, each streamed response includes a structured cursor:

```protobuf
message CursorValue {
    oneof value {
        int64  int_val   = 1;
        double float_val = 2;
        string str_val   = 3;
    }
}

message KeysetCursor {
    repeated CursorValue values = 1;  // one entry per order_by field, in order
    int64                id     = 2;  // implicit tiebreaker, always ASC
}
```

`values` has exactly one `CursorValue` for each `OrderBy` entry in the request. The `id` field is the row's primary-key id and serves as the final tiebreaker regardless of the sort specification. If the cursor is omitted in a follow-up request, iteration starts from the beginning.

## Sort Specification

Sort order is expressed as a list of `OrderBy` entries:

```protobuf
enum Order {
    ORDER_UNSPECIFIED = 0;
    ASC  = 1;
    DESC = 2;
}

message OrderBy {
    string column = 1;  // column name or namespace-prefixed field
    Order  order  = 2;
}
```

`column` supports the same namespace prefixes as `filter_expression`:

| Syntax | Example | Resolves to |
|--------|---------|-------------|
| Plain built-in | `max_timestamp`, `record_count` | Physical column as-is |
| `__FILE.xxx` | `__FILE.record_count` | Physical column as-is |
| `__DIM.xxx` | `__DIM.zone` | `dim_fXX` |
| `__AGG_TYPE.key.value` | `__AGG_GTE.level.warn` | `agg_fXX` |

The `id` column is always appended implicitly as the final `ASC` tiebreaker; clients must not include it in `order_by`.

### Indexed vs. unindexed columns

Two columns have dedicated indexes that support efficient keyset pagination:

| Column | Order | Index used |
|--------|-------|------------|
| `min_timestamp` | ASC | PRIMARY KEY forward scan |
| `max_timestamp` | DESC | `idx_max_timestamp` |

All other columns — `record_count`, `clp_ir_size_bytes`, dimension columns (`dim_fXX`), aggregation columns (`agg_fXX`), etc. — are unindexed. Sorting on them causes a full table scan and requires `allow_unindexed_sort = true` in the request. The server rejects unindexed sort columns by default.

## Keyset WHERE Clause

For multi-column sorts the keyset condition is an OR-of-prefix-equalities, one disjunct per sort column plus the `id` tiebreaker. For `order_by = [(f1, DESC), (f2, ASC)]` after cursor `(v1, v2, id0)`:

```sql
(f1 < v1)
OR (f1 = v1 AND f2 > v2)
OR (f1 = v1 AND f2 = v2 AND id > id0)
```

The operator for each disjunct's leading inequality follows the column's `Order`: `<` for `DESC`, `>` for `ASC`. The `id` tiebreaker is always `>` (implicitly `ASC`).

## SQL Patterns

### Single indexed column (`max_timestamp DESC`)

**First batch:**
```sql
SELECT * FROM clp_spark
WHERE state IN (?) AND (filter_expression)
ORDER BY max_timestamp DESC, id ASC
LIMIT :batch_size
```

**Subsequent batches** (cursor = `{values: [{int_val: 1704067200}], id: 42}`):
```sql
SELECT * FROM clp_spark
WHERE state IN (?) AND (filter_expression)
  AND (
    (max_timestamp < 1704067200)
    OR (max_timestamp = 1704067200 AND id > 42)
  )
ORDER BY max_timestamp DESC, id ASC
LIMIT :batch_size
```

Uses `idx_max_timestamp`. No full table scan.

### Single indexed column (`min_timestamp ASC`)

**First batch:**
```sql
SELECT * FROM clp_spark
WHERE state IN (?) AND (filter_expression)
ORDER BY min_timestamp ASC, id ASC
LIMIT :batch_size
```

**Subsequent batches** (cursor = `{values: [{int_val: 1704000000}], id: 7}`):
```sql
SELECT * FROM clp_spark
WHERE state IN (?) AND (filter_expression)
  AND (
    (min_timestamp > 1704000000)
    OR (min_timestamp = 1704000000 AND id > 7)
  )
ORDER BY min_timestamp ASC, id ASC
LIMIT :batch_size
```

Uses the clustered PRIMARY KEY `(min_timestamp, id)` for a forward scan. No full table scan.

### Multi-column unindexed sort (`record_count DESC, max_timestamp ASC`)

Requires `allow_unindexed_sort = true`.

**First batch:**
```sql
SELECT * FROM clp_spark
WHERE state IN (?) AND (filter_expression)
ORDER BY record_count DESC, max_timestamp ASC, id ASC
LIMIT :batch_size
```

**Subsequent batches** (cursor = `{values: [{int_val: 1000}, {int_val: 1704067200}], id: 42}`):
```sql
SELECT * FROM clp_spark
WHERE state IN (?) AND (filter_expression)
  AND (
    (record_count < 1000)
    OR (record_count = 1000 AND max_timestamp > 1704067200)
    OR (record_count = 1000 AND max_timestamp = 1704067200 AND id > 42)
  )
ORDER BY record_count DESC, max_timestamp ASC, id ASC
LIMIT :batch_size
```

Full table scan each batch; the keyset condition limits how many rows MariaDB must sort before reaching the LIMIT.

## Why This Works

**Index-ordered scan (indexed columns).** `max_timestamp DESC` uses `idx_max_timestamp` for a reverse index scan; `min_timestamp ASC` uses the clustered primary key `(min_timestamp, id)` for a forward scan. Both avoid filesort.

**Partition pruning.** The keyset condition provides a bound on the ordering timestamp. Combined with RANGE partitioning by `min_timestamp`, MariaDB can skip partitions outside the bound.

**Bounded transfer.** Each internal query returns at most `batch_size` rows. The driver reads exactly that many rows from the socket, then closes the statement. `skipRemaining()` has at most zero rows to discard.

**No dedup.** The `(order_by columns..., id)` tuple uniquely identifies every position in the ordering because `id` is the primary key. The OR-of-prefix-equalities comparator guarantees no row appears in two batches.

**Monotonic order.** The keyset condition structurally prevents order from flowing backwards — each batch starts strictly after where the previous batch ended.

**Mixed-direction correctness.** Each disjunct's leading inequality uses `<` for `DESC` columns and `>` for `ASC` columns, mirroring the sort direction. The prefix-equality structure ensures earlier sort columns dominate.

## See Also

- [Metadata Tables: Schema Definition](../reference/metadata-tables.md#schema-definition) — PK is `(min_timestamp, id)`
- [Metadata Tables: Partitioning](../reference/metadata-tables.md#partitioning) — RANGE by `min_timestamp`
- [Performance Tuning](../operations/performance-tuning.md) — Index design and query optimization
