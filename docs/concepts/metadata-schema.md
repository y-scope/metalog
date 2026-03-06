# Metadata Schema

[← Back to docs](../README.md)

A file-level metadata catalog that enables queries to skip millions of files and terminate early — without opening a single log file.

**Related:** [Metadata Tables Reference](../reference/metadata-tables.md) · [Query Execution](query-execution.md) · [Naming Conventions](../reference/naming-conventions.md) · [Consolidation](consolidation.md) · [Glossary](glossary.md)

---

## TL;DR

**What:** A MariaDB/MySQL metadata catalog that tracks CLP-IR and CLP-Archive files with lifecycle states, time bounds, dimensions, and aggregations.

**Why:** Traditional metastores support split pruning — skipping files by partition key or column stats — but not TopN early termination, because they don't track per-file match counts. This metastore provides two composable mechanisms: `dim_*` columns eliminate entire files where all records fail a predicate, and `agg_*` columns store pre-computed per-file aggregates (match counts, min/max, averages, and more) for common filter values. They compose cleanly: `service=auth` and `region=us-east` filter out files that don't belong to that service and region; the remaining files have every record matching both, so the per-file error count is the exact count for the full combined predicate. A query for the latest 100 error logs for `service=auth` in `region=us-east` scans those files most-recent-first and stops as soon as it can guarantee those 100 results are the most recent — without opening a single log file.

**Key differentiators:**

| Feature | Traditional Metastores | This Design |
|---------|----------------------|-------------|
| Granularity | Partition or file | File |
| Time precision | Daily partitions | Second-precision bounds |
| Dimension filtering | 1-3 partition keys | Arbitrary `dim_*` columns |
| Aggregation-based pruning | None | Per-file `agg_*` columns (counts, min/max, averages) |
| Early termination | Scan all, sort, limit | Stop when enough results guaranteed |
| Schema model | Required upfront | Schema-less content |

**Performance:** ~20-22K rows/sec/table batch-UPSERT (single writer, batch size 5000), 10-100ms time-range queries.

---

## Schema at a Glance

| Feature | Purpose | Example |
|---------|---------|---------|
| **Lifecycle state** | Track streaming/consolidation states | 7 states: `IR_BUFFERING` → `ARCHIVE_CLOSED` |
| **Time bounds** | Second-precision pruning | `min_timestamp`, `max_timestamp` |
| **Dimensions** | Multi-dimensional file filtering | Skip files where `service ≠ 'auth'` |
| **Aggregations** | Early termination | Skip files where `level=error` agg = 0 |
| **Hash indexes** | O(1) path lookups (99.6% smaller) | `clp_ir_path_hash` VIRTUAL column |

**Two-layer architecture:**

| Layer | What It Does | Indexed By |
|-------|--------------|------------|
| **CLP metastore** | Prunes files before opening | Time bounds, 5-10 dimensions, per-file aggregations |
| **CLP files** | Filters records within files | 100+ fields via variable dictionary, MPT, ERTs |

The metastore indexes only high-frequency dimensions; CLP files contain the full superset via Merged Parse Trees (MPTs).

---

## Entry Types and Lifecycle

Three entry types, determined at file creation:

| Entry Type | Use Case | Consolidation |
|------------|----------|---------------|
| **IR-only** | Ephemeral data (test logs) | Never |
| **Archive-only** | Batch ingestion | Created directly |
| **IR+Archive** | Streaming + analytics | IR → Archive via workers |

**State transitions:**

```
IR-only:       IR_BUFFERING → IR_CLOSED → IR_PURGING → (deleted)

Archive-only:  ARCHIVE_CLOSED → ARCHIVE_PURGING → (deleted)

IR+Archive:    IR_ARCHIVE_BUFFERING → IR_ARCHIVE_CONSOLIDATION_PENDING → ARCHIVE_CLOSED → ARCHIVE_PURGING → (deleted)
               (After consolidation, the IR file in storage is deleted but clp_ir_path is preserved in the row as provenance)
```

> **States reference:** See [Naming Conventions: Lifecycle States](../reference/naming-conventions.md#lifecycle-state-naming) for complete state definitions.

---

## Storage Design

### Denormalized Fact Table

Each metadata row is a **fact** — one CLP file — with content-derived attributes embedded directly. This mirrors a denormalized star schema: in a classic star schema, a fact table holds numeric measures and foreign keys pointing to separate dimension tables (customer, product, location). Here, dimension values are stored **inline** in `dim_fNN` columns — no joins, no separate tables. Each row is a self-contained fact.

**Benefits:**
- Single-table scans with no join cost, even for multi-dimension filters
- Index-only pruning: an index on `dim_f01` (e.g., zone) can prune by zone without touching any other table — partition pruning and dimension pruning in a single scan
- Works cleanly with RANGE partitioning: no cross-partition joins

**Trade-offs and why they're acceptable:**

*Storage overhead (repeated strings):* Dimension values like `"us-east-1a"` are stored once per file row. In practice this overhead is bounded by design: only low-cardinality fields qualify as dimension columns — zones, DCs, regions, services — fields that repeat heavily across files. High-cardinality fields (user IDs, trace IDs, request paths) are never promoted to `dim_fNN`; they appear at most as sketches. A normalized star schema would not help for those fields either — you'd trade a repeated unique value for a unique FK integer, saving nothing. Page compression further reduces the overhead: low-cardinality fields produce highly compressible pages.

*Schema rigidity (fixed `dim_fNN` slots):* This is schema-**less** data — clp-s processes arbitrary semi-structured logs whose shape changes over time. The slot mechanism is designed for this: new dimensions are allocated automatically by `ColumnRegistry` when a new field appears (online DDL `ADD COLUMN` — cheap), existing slots are remapped in the registry without DDL, and unneeded slots are retired gracefully:
1. Mark the slot inactive in `_dim_registry` / `_agg_registry`
2. Stop populating the physical column for new rows
3. Wait for the retention period — all rows that had this column populated expire naturally
4. Once the slot is empty, reclaim it via lightweight, rate-limited cleanup

No table rebuild, no downtime, no bulk data migration. The entire slot lifecycle is online.

*Update anomalies:* Rows are not fully immutable — file-level fields (`max_timestamp`, `raw_size_bytes`, `record_count`) are updated frequently during the active buffering window (15 min–1 hr after first ingest, a few to a dozen updates per file). The design handles this with efficient batched UPSERTs: ~20–22K ops/sec per writer goroutine (see [Scalability](../reference/metadata-tables.md#scalability)). After the buffering window, a few more updates occur for consolidation state transitions, then the row is stable. Dimension values almost never change after first write. The `sketches` SET column and `ext` MEDIUMBLOB may still be updated as sketches are refined. The alias mechanism (dim alias, agg alias) is the intentional blur point for exposing file-object properties in the user-facing namespace.

### Storage Access Patterns

The composite PK `(min_timestamp, id)` means new rows always append to the B-tree tail of the current partition — no mid-tree page splits, no random I/O, and active leaf pages stay in the buffer pool.

**Two active zones, cold compressed middle:**

| Zone | Contents | Access pattern |
|------|----------|----------------|
| Insert tail | Last pages of the current day's partition | Sequential B-tree appends (batched) |
| Query zone | Recent partition (< 24 hr) | Hot in buffer pool; partition pruning isolates it |
| Cold middle | Older partitions with no expiring files | Rarely touched; compressed on disk |
| Delete head | First pages of the oldest expiring partition | Batched sequential deletes; pages resident during batch |

Both insertions and deletions are **batched**: active pages are loaded into the buffer pool for the duration of the batch, then flushed (compressed) or fully reclaimed. The working set — insert tail + recent query partition + current delete batch — fits in memory. Cold middle partitions never compete for buffer pool space.

Pages are stored **decompressed in the buffer pool** regardless of on-disk compression. LZ4 compression reduces disk I/O and storage, not RAM usage.

### Retention and Deletion

`expires_at` is set at ingest as approximately `min_timestamp + (retention_days × 86400)`. Because `min_timestamp` is the partition key, this creates a **strong temporal correlation**: files in old partitions expire sooner; files in new partitions expire later. Deletions therefore cluster in the oldest (coldest) partitions — never in the hot recent partition.

Deletions are **row-level**, driven by `idx_expiration` (scan `expires_at ASC`, batch delete). Each file carries its own `expires_at` reflecting its individual retention policy; whole-partition drops are not used. `expires_at` can be selectively extended for groups of files matching specific dimensions — for example, extending retention for `service=auth` files spanning an incident window. This is a first-class operational capability (see [Query Catalog](../operations/performance-tuning.md)).

Because `expires_at ≈ min_timestamp + retention`, and rows within a partition are physically ordered by `(min_timestamp, id)`, deletions proceed in **storage order**: the oldest rows in the oldest partition expire first, matching their physical B-tree position.

**Why fragmentation is benign:**
- The page being actively deleted is in the buffer pool during the batch (it's being worked on).
- Deletions are batched in large chunks in roughly storage order, so most pages drain completely within one or a few batch operations. InnoDB reclaims fully empty pages whole — the common case.
- Page compression with `PUNCH_HOLE` handles partially-drained pages: as rows are deleted, the compressed size shrinks, and InnoDB can return freed filesystem blocks to the OS.
- All fragmentation stays in cold, old partitions — the hot recent partition is unaffected.

---

## UPSERT Strategy

Metadata is written via `INSERT ... ON DUPLICATE KEY UPDATE` — every write is an upsert. The first message for a file inserts a new row; subsequent messages update the existing row in place. This enables at-least-once delivery from both Kafka and gRPC without requiring the caller to distinguish inserts from updates.

### Guarded Updates

Not every update should succeed. Once a file progresses past buffering (e.g., picked up by the Planner for consolidation), late re-deliveries from Kafka must not regress its state. Two guards prevent this:

| Guard | SQL Condition | Purpose |
|-------|---------------|---------|
| **State guard** | `state NOT IN ('IR_ARCHIVE_CONSOLIDATION_PENDING', 'ARCHIVE_CLOSED', 'ARCHIVE_PURGING')` | Reject updates to files that have progressed past ingestion |
| **Timestamp guard** | `VALUES(max_timestamp) > max_timestamp` | Reject stale re-deliveries where the incoming data is older than what's already stored |

Both guards are combined into a single `UPSERT_GUARD` condition. Each column in the `ON DUPLICATE KEY UPDATE` clause is wrapped in an `IF()`:

```sql
INSERT INTO clp_spark (...) VALUES (...)
ON DUPLICATE KEY UPDATE
    state = IF(UPSERT_GUARD, VALUES(state), state),
    record_count = IF(UPSERT_GUARD, VALUES(record_count), record_count),
    ...
    max_timestamp = IF(UPSERT_GUARD, VALUES(max_timestamp), max_timestamp)  -- MUST be last
```

When the guard evaluates to false, the column retains its current value — the row is touched but not changed.

### Column Ordering Constraint

MySQL/MariaDB evaluates `ON DUPLICATE KEY UPDATE` assignments left-to-right, and each assignment sees values set by earlier assignments in the same statement. Because the timestamp guard references `max_timestamp`, it **must be the last column assigned**. If `max_timestamp` were updated earlier, subsequent columns would evaluate `VALUES(max_timestamp) > max_timestamp` as `new > new` (always false), silently skipping their updates.

### Partition Routing

The UNIQUE index on IR path is `UNIQUE(clp_ir_path_hash, min_timestamp)` — scoped to a single partition. Every upsert for the same file must provide the same `min_timestamp` so MySQL routes the duplicate-key check to the correct partition. The Client SDK contract guarantees this: `min_timestamp` is set once at file creation (the first event's timestamp) and never changes.

---

## Partitioning

Each metadata table uses daily RANGE partitions on `min_timestamp` (event time, not ingestion time). A background goroutine on every node creates lookahead partitions and cleans up old ones, coordinated via advisory locks so only one node runs DDL at a time. Each per-table coordinator also checks partitions at startup to ensure they exist before accepting data.

### Partition Layout

```
p_20240101          ← historical catch-all (also merge target for sparse partitions)
p_20260215          ← daily partition (one per day, named p_YYYYMMDD)
p_20260216
p_20260217
  ...
p_20260225          ← lookahead (7 days ahead by default)
p_future            ← MAXVALUE safety net (should rarely contain data)
```

Each daily partition covers one UTC day: `VALUES LESS THAN (start_of_next_day_epoch)`. Daily partitions are created dynamically by the `PartitionManager`; the schema DDL seeds only the historical catch-all and `p_future`.

### Composite Primary Key

MySQL requires the partition key in every unique key — a hard engine constraint with no exceptions. Since we RANGE-partition by `min_timestamp`, it must be in the PK. We also need an auto-increment `id` for unique row identification. The composite PK `(min_timestamp, id)` satisfies both.

The PK ordering also gives excellent **write locality**: rows are inserted in roughly the same order as their `min_timestamp` values, so PK index entries append near the end of the B-tree within the current day's partition rather than scattering across it. Fewer page splits, less random I/O, and the active leaf pages stay in the buffer pool.

A side effect is that uniqueness constraints are also per-partition. The UNIQUE index on `ir_path` becomes `UNIQUE(hash(ir_path), min_timestamp)` — the same path could theoretically exist in two different daily partitions. The Client SDK contract prevents this: `min_timestamp` is set once at file creation (the first event's timestamp) and never changes — not during buffering, not across state transitions, not after consolidation. Every UPSERT for the same file uses the same `min_timestamp`, so it always hits the same partition. See the "Known Limitations" section in `schema.sql`.

### Why Daily Partitioning

#### Buffer Pool Locality

With time-series data, access patterns are heavily skewed by recency:

- **Most queries** target the last hour of data
- **Some queries** target the last 24 hours
- **The rest** are either point queries (accessing specific filtered-down rows) or large analytical jobs that scan many rows and are not latency-sensitive
- **Retention deletion** accesses the oldest 1–2 partitions (expired data) — because `expires_at ≈ min_timestamp + retention`, deletions naturally cluster in the oldest partitions (see [Retention and Deletion](#retention-and-deletion))

Middle partitions are rarely touched. Without partitioning, all this data shares the same B-tree indexes, and hot (recent) index pages compete with cold (old) pages for buffer pool space.

With daily partitions, each partition has its own independent B-tree for every index. The buffer pool naturally keeps recent partitions' index pages hot and evicts cold middle partitions. Since the majority of lookups are indexed, a table with months of data uses no more buffer pool than a table with a few days — as long as the working set fits in memory. The working set has two active zones: the **insert tail** (last pages of the current day's partition, receiving sequential appends) and the **delete head** (first pages of the oldest expiring partition, being drained in batches) — plus the recent query partition. Cold middle partitions never compete for buffer pool space (see [Storage Access Patterns](#storage-access-patterns)).

#### Query Pruning

Time-range queries skip irrelevant partitions entirely — a query for the last hour touches a single partition instead of scanning the full table's indexes.

#### Partition Housekeeping

After the retention goroutine deletes expired rows, old partitions become empty or sparse. The partition manager drops empty partitions and merges sparse ones — removing dead partition shells and reducing the number of partitions the optimizer must consider.

#### The `p_future` Trap

If no partition exists for incoming data's `min_timestamp`, MariaDB/MySQL routes it to `p_future`. Moving data out of `p_future` later requires `REORGANIZE PARTITION`, which copies rows — expensive for large volumes. Lookahead partitions prevent this by always having partitions ready before data arrives.

### Maintenance Operations

The `PartitionManager` runs two operations per table:

#### 1. Create Lookahead Partitions

Creates daily partitions for today through today + N days (default: 7).

- If `p_future` exists → `REORGANIZE PARTITION p_future INTO (p_YYYYMMDD ..., p_future MAXVALUE)` — splits the catch-all
- If no `p_future` → `ADD PARTITION (p_YYYYMMDD ...)`
- Already-existing partitions are skipped (idempotent)
- Duplicate partition errors are caught and logged (safe for concurrent execution)

Creating a lookahead partition is a **metadata-only DDL operation** (milliseconds) when `p_future` is empty, which is the normal case. Even creating all 7 default lookahead partitions completes well under a second. It only becomes expensive when `p_future` contains data that must be redistributed.

#### 2. Clean Up Old Partitions

For partitions older than the cleanup age (default: 90 days):

| Condition | Action | SQL |
|-----------|--------|-----|
| Empty (0 rows) | Drop | `ALTER TABLE DROP PARTITION p_YYYYMMDD` |
| Sparse (< 1,000 rows) | Merge into first partition | `ALTER TABLE REORGANIZE PARTITION p_20240101, p_YYYYMMDD INTO (p_20240101 ...)` |
| Above threshold | Leave alone | — |

Partitions with data are never dropped — sparse partitions are merged into the historical catch-all (`p_20240101`) via `REORGANIZE PARTITION`, which moves rows before removing the partition boundary. Recent partitions are never touched, regardless of row count — only partitions older than the cleanup age are candidates.

### When Maintenance Runs

| Trigger | Scope | Lock timeout | Purpose |
|---------|-------|-------------|---------|
| **Per-table coordinator startup** | Single table | 5s (blocking) | Ensure partitions exist before accepting data |
| **Node-level background goroutine** | All active tables | 0s (non-blocking) | Ongoing creation and cleanup |

**Startup (blocking)**: Each per-table coordinator calls `ensureLookaheadPartitions()` before starting its goroutines. This blocks for up to 5 seconds to acquire the advisory lock — if another node is already running maintenance, the startup waits briefly then proceeds (partitions may already exist). The 5-second timeout is generous given that creating 7 lookahead partitions takes well under a second (metadata-only DDL). The timeout only matters when another node holds the advisory lock.

**Background (periodic)**: Every node runs `runGlobalPartitionMaintenance()` every hour for all active tables with partition management enabled — not just tables the node owns. This provides redundancy: if a node dies, surviving nodes continue maintaining its tables' partitions. The advisory lock ensures only one node executes DDL at a time; others skip immediately.

### Advisory Lock Coordination

Multiple nodes running maintenance concurrently would cause DDL conflicts. Advisory locks prevent this:

```sql
-- Acquire (non-blocking: returns immediately if held)
SELECT GET_LOCK('pm_clp_spark', 0);

-- ... run DDL (create/drop/reorganize partitions) ...

-- Release
SELECT RELEASE_LOCK('pm_clp_spark');
```

- **One lock per table** — `pm_<table_name>`. Different tables are maintained concurrently.
- **Connection-scoped** — if the holder crashes, MariaDB/MySQL auto-releases the lock. No cleanup needed.
- **Non-blocking for background** — losers skip immediately (`GET_LOCK(..., 0)`), retry next hour.
- **Short-blocking for startup** — waits up to 5 seconds (`GET_LOCK(..., 5)`) so the coordinator doesn't start with missing partitions.

**Connection pool integration**: The advisory lock is held on a dedicated connection obtained from the pool (kept open for the duration of maintenance). DDL operations (`ALTER TABLE`) use separate pooled connections. When the maintenance function returns — normally or via error — the connection is returned to the pool and the lock is released.

### Partition Defaults

| Setting | Default | Description |
|---------|---------|-------------|
| Lookahead days | 7 | Partitions created ahead of today |
| Cleanup age | 90 days | Minimum age before a partition is eligible for drop/merge |
| Sparse row threshold | 1,000 | Row count below which old partitions are merged |
| Maintenance interval | 1 hour | How often the background goroutine runs |

### Operational Notes

**Monitoring partitions:**

```sql
SELECT PARTITION_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'clp_spark'
ORDER BY PARTITION_ORDINAL_POSITION;
```

**Data in `p_future`**: If `TABLE_ROWS > 0` for `p_future`, lookahead partitions weren't created in time. The next maintenance run will `REORGANIZE` to split the data into proper daily partitions, but this is expensive for large volumes. Investigate why maintenance didn't run (check advisory lock contention, node health).

**Manual partition operations**: Safe to run while the service is running — the advisory lock only protects automated maintenance. Manual `ALTER TABLE` statements don't acquire advisory locks, so avoid running them concurrently with the service's maintenance window.

**Partition limit**: MariaDB/MySQL supports up to 8,192 partitions per table. With daily partitions, this covers 22+ years — well beyond practical retention periods.

---

## Why This Design

This metadata catalog addresses two gaps in existing metastores.

### Gap 1: Schema-Less, Time-Series Data

Existing metastores require predefined schemas. This design supports:

| Characteristic | Description |
|----------------|-------------|
| Append-only | Files immutable once closed |
| Time-series | Every record timestamped |
| High-cardinality dimensions | Many distinct values (hosts, services) |
| Schema-less content | Semi-structured data without predefined schema |
| Investigative queries | Filtering, early termination, recent data focus |

### Gap 2: Streaming and Consolidation Pipelines

Each file goes through multiple metadata updates during its lifecycle — not a single batch commit. For an IR+Archive file, a typical sequence is:

| # | Operation | State After |
|---|-----------|-------------|
| 1 | First metadata message arrives (INSERT) | `IR_ARCHIVE_BUFFERING` |
| 2–N | Subsequent metadata updates as file grows (UPSERT: record_count, max_timestamp, aggs) | `IR_ARCHIVE_BUFFERING` |
| N+1 | File closed by producer | `IR_ARCHIVE_CONSOLIDATION_PENDING` |
| N+2 | Consolidation completes, archive metadata written | `ARCHIVE_CLOSED` |
| N+3 | Retention expires, storage deletion queued | `ARCHIVE_PURGING` |
| N+4 | Row deleted from database | (deleted) |

The number of updates during buffering (step 2–N) depends on how frequently the producer flushes metadata. The flush interval is dynamically adjusted at runtime based on content severity: as short as under a minute when failures are detected, and up to 15–30 minutes when everything is healthy. Traditional metastores are designed for batch commits where files are registered once.

---

## See Also

- [Metadata Tables Reference](../reference/metadata-tables.md) — Full DDL, column reference, index reference, scalability projections
- [Query Execution](query-execution.md) — Split pruning, early termination, query patterns
- [Schema Evolution](../guides/evolve-schema.md) — Online DDL for dynamic column addition
- [Consolidation](consolidation.md) — IR→Archive pipeline and state transitions
- [Performance Tuning](../operations/performance-tuning.md) — batch-UPSERT benchmarks and index overhead
- [Naming Conventions](../reference/naming-conventions.md) — Column, index, and state naming patterns
- [Glossary](glossary.md) — CLP terminology and data format definitions
