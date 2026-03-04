# Query Execution

[← Back to docs](../README.md)

How the metadata schema enables query optimization — pruning files and terminating queries early before any files are opened.

**Related:** [Metadata Schema](metadata-schema.md) · [Naming Conventions](../reference/naming-conventions.md) · [Glossary](glossary.md) · [Early Termination Design](../design/early-termination.md)

> **Column naming note:** SQL examples in this document use illustrative names (e.g., `dim_str128_service`, `agg_gte_level_error`) for readability. In the actual database, dimension and aggregation columns have opaque placeholder names (`dim_f01`, `agg_f01`, ...) with the logical mapping stored in `_dim_registry` and `_agg_registry`. The wire-format keys that producers send use slash-delimited notation (`dim/str128/service`, `agg_int/gte/level/error`). See [Schema Evolution](../guides/evolve-schema.md).

---

## The Problem

A user wants the 100 most recent errors from one service. The data exists across 250,000 files. Every traditional approach forces a full scan:

| Metric | Scale |
|--------|-------|
| Hosts/executors | 10K-100K |
| Files per hour | 100K-500K |
| Files retained (30 days) | 10M-100M+ |

**Why traditional approaches fail at this scale:**

| Approach | Problem |
|----------|---------|
| **Row-level indexes** (OLTP) | Write amplification: 1M events/sec multiplied by 4 indexes equals 4M updates/sec. Index size often exceeds data size. |
| **Generic query engines** (Hive/Presto) | Cannot assume row ordering within files. Must scan all files, sort globally, then return top N. |
| **Time partitioning** | Partitioning by hour still yields 250K files. The query needs 100 events, not 2.5B. |

## File-Level Metadata Approach

The insight: change **what** we index. Instead of indexing every event, index every file.

| Approach | Indexed | Scale | Size |
|----------|---------|-------|------|
| Row-level | Every event | Trillions | Petabytes |
| File-level | Every file | 100M | Gigabytes |

Same pruning power, no write-time cost, and 99.99% fewer entries to evaluate.

### Data Properties Required

| Property | What It Means | Why It Helps |
|----------|---------------|--------------|
| **Known time bounds** | `[min_timestamp, max_timestamp]` per file | Enables overlap-based pruning |
| **File-level dimensions** (`dim_*`) | All rows in file share same value | Filter files, not rows |
| **Pre-computed aggregates** (`agg_*`) | Subset counts tracked per file | Know when to stop without scanning |

---

## Split Pruning vs Early Termination

Two distinct optimizations — understanding the difference is critical.

| Optimization | What It Does | Requirement |
|--------------|--------------|-------------|
| **Split Pruning** | Skip files that *cannot* match | Know if file *might* match |
| **Early Termination** | Stop scanning once enough results guaranteed | Know *exact count* per file |

### Split Pruning

Eliminates files from consideration. All column types contribute:

| Category | How It Prunes |
|----------|---------------|
| Time bounds | Files outside query time range |
| Dimensions | Files where all records have a different value |
| Counts | Files with zero matching records |
| Sketches | Files that *definitely* don't contain a value |

### Early Termination

Stops scanning before exhausting all files. Requires exact match counts *before opening*:

- **`agg_*` columns**: Directly provide exact counts
- **`dim_*` columns**: Combined with `record_count`, provide exact counts
- **`sketches`**: Cannot support — only answer "definitely not" or "maybe"

**Why this matters:** Traditional platforms must scan all matching files, sort globally, then limit. This design stops after a fraction of files because it knows *exactly* how many results each unopened file contains — before reading a single byte of log data.

---

## Early Termination Levels

### Level 1: Basic Early Termination

**Query:**
```sql
SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100
```

**Algorithm:**

```
Variables:
- watermark: tracks min(min_timestamp) of all selected files
- cutoff_ts: locks when we first accumulate enough results

1. Order files by max_timestamp DESC

   File A: [9:30, 10:00], count=150  ← newest-ending
   File B: [9:40, 9:55], count=50
   File C: [9:00, 9:25], count=200
   ... (1M more files)

2. Check File A:
   cutoff_ts (-∞) not set, so no termination check
   SELECT File A
   accumulated=150 >= limit (100), LOCK cutoff_ts = watermark = 9:30

3. Check File B:
   max_timestamp (9:55) >= cutoff_ts (9:30), so MUST SELECT
   accumulated=200, watermark=min(9:30, 9:40)=9:30
   cutoff_ts already locked, stays at 9:30

4. Check File C:
   max_timestamp (9:25) < cutoff_ts (9:30)
   TERMINATE: File C's newest event (9:25) is older than
     the cutoff threshold (9:30)

Result: Selected 2 files instead of 1M+
```

**Termination condition:**
```
file.max_timestamp < cutoff_ts

where:
- watermark = min(min_timestamp) of selected files
- cutoff_ts = watermark at the moment cumulative first reaches limit (locked thereafter)
```

**Why ORDER BY max_timestamp DESC?** Once a file below the watermark is found, all remaining files are also below it. Processing can stop immediately.

### Level 2: Early Termination with Dimension Filters

```sql
SELECT * FROM logs WHERE service = 'auth' ORDER BY timestamp DESC LIMIT 100
```

**Key:** Dimensions are file-level constants.

**Algorithm:**
1. Filter at metadata level: `WHERE dim_str128_service = 'auth'`
2. Apply Level 1 to filtered files
3. `record_count` = exact matching count — every record in the remaining files satisfies the predicate, so no uncertainty

### Level 3: Early Termination with Non-Dimension Filters

```sql
SELECT * FROM logs WHERE level = 'ERROR' ORDER BY timestamp DESC LIMIT 100
```

**Challenge:** `level` varies within a file (INFO, WARN, ERROR mixed).

**Solution:** Pre-computed cumulative counts.

```sql
agg_gte_level_warn   INT UNSIGNED  -- WARN + ERROR + FATAL
agg_gte_level_error  INT UNSIGNED  -- ERROR + FATAL
agg_gte_level_fatal  INT UNSIGNED  -- FATAL only
```

**Why cumulative?** 3 columns support 6 query patterns:

| Query | Formula |
|-------|---------|
| All | `record_count` |
| ERROR+ | `agg_gte_level_error` |
| ERROR only | `gte_level_error - gte_level_fatal` |
| WARN only | `gte_level_warn - gte_level_error` |

**Algorithm:**
1. Filter: `WHERE agg_gte_level_error > 0`
2. Compute: `error_only = gte_level_error - gte_level_fatal`
3. Apply Level 1 using `error_only` as count
4. Terminate when accumulated ERROR count >= 100

### Algorithm Pseudocode

```
FUNCTION select_files_for_streaming(query, limit):
    cursor = stream_query("""
        SELECT file_path, min_timestamp, max_timestamp, agg_*
        FROM files
        WHERE time_overlaps(query) AND dimension_matches(query)
        ORDER BY max_timestamp DESC
    """)

    selected = []
    cumulative = 0
    watermark = INFINITY
    cutoff_ts = -INFINITY

    FOR file IN cursor:
        IF file.max_timestamp < cutoff_ts:
            BREAK

        selected.add(file)
        cumulative += calculate_matching_count(file, query)
        watermark = MIN(watermark, file.min_timestamp)

        IF cutoff_ts == -INFINITY AND cumulative >= limit:
            cutoff_ts = watermark

    RETURN selected
```

### How CLP's Early Termination Differs from Other Systems

| Aspect | MongoDB (Document-Level) | CLP (File-Level + Pre-computed Counts) |
|--------|-------------------------|----------------------------------------|
| **What's indexed** | Per-document: compound index on `(level, timestamp)` | Per-file: metadata with `agg_*` columns |
| **Early termination** | Stop after N matching documents | Skip entire files via watermark algorithm |
| **Matching count** | Must scan docs to count | **Known before scanning** via pre-computed counts |
| **Index overhead** | Per-document index maintenance | File-level metadata only (approximately 5%) |
| **Termination guarantee** | Probabilistic (depends on doc distribution) | **Guaranteed** (watermark proves remaining files older) |

---

## VIRTUAL Columns and Hash Indexes

### The Problem

Object storage paths can be 1024 characters. With utf8mb4 (4 bytes/char), that's 4096 bytes — exceeding MariaDB/MySQL's 3072-byte index limit.

### The Solution

VIRTUAL columns compute an MD5 hash at read time. The hash is stored **only in the index**, not in the row.

```sql
clp_ir_path_hash BINARY(16) AS (UNHEX(MD5(clp_ir_path))) VIRTUAL
```

| Approach | Index Key Size | At 150M Rows |
|----------|----------------|--------------|
| Direct path index | Up to 4096 bytes | Not possible |
| VIRTUAL + MD5 hash | 16 bytes | 2.4 GB |

**Benefits:**
- 99.6% smaller indexes
- O(1) lookups (<1 ms)
- No row storage overhead
- Compatible with MariaDB 10.2+, MySQL 5.7+

**NULL handling:** Path columns are nullable. `MD5(NULL) = NULL`, `UNHEX(NULL) = NULL`. This allows:
- Archive-only entries (NULL IR path) without unique conflicts
- Pre-consolidation rows (NULL archive path) save index space

### Query Pattern

```sql
-- CORRECT: Use VIRTUAL column name
WHERE clp_ir_path_hash = UNHEX(MD5('/path/to/file'))
  AND min_timestamp = ?

-- WRONG: Full table scan
WHERE clp_ir_path = '/path/to/file'
```

---

## Dynamic Index Management

Indexes prefixed with `idx_` can be added or removed based on query patterns without a service restart. `DynamicIndexManager` reconciles the current index state against the YAML config: it creates enabled indexes that are missing and drops disabled indexes that exist. Config reloads are triggered externally (e.g., by `YamlConfigLoader`).

**Three indexes are always protected and cannot be dropped:**
- `PRIMARY` — the clustered index
- `idx_consolidation` — core index for pending file queries
- `idx_expiration` — core index for deletion queries

**Online DDL:** Index creation and deletion use `ALGORITHM=INPLACE, LOCK=NONE`. If the engine does not support this (e.g., certain MariaDB configurations or index types), the operation falls back to default DDL without online guarantees. When online DDL succeeds, concurrent reads and writes are allowed during the index build.

**Workflow:**
1. Monitor slow queries and index usage statistics in the database
2. Update the YAML index configuration
3. Trigger a config reload — `DynamicIndexManager.reconcile()` applies the diff
4. Validate query performance improvement

---

## Query Catalog

### Search Queries

#### 1. Time-Range + Dimension
```sql
SELECT id, clp_ir_path, clp_archive_path, min_timestamp, max_timestamp
FROM clp_spark
WHERE min_timestamp <= [END] AND max_timestamp >= [START]
  AND dim_str128_service = 'myservice'
ORDER BY max_timestamp DESC
LIMIT 100;
```

#### 2. Archive-Only (Bulk Analytics)
```sql
SELECT DISTINCT clp_archive_path
FROM clp_spark
WHERE min_timestamp <= [END] AND max_timestamp >= [START]
  AND dim_str128_service = 'myservice'
  AND clp_archive_path IS NOT NULL;
```

#### 3. Early Termination with Count Filter
```sql
SELECT id, clp_ir_path, max_timestamp,
  (agg_gte_level_error - agg_gte_level_fatal) AS error_count
FROM clp_spark
WHERE dim_str128_service = 'auth'
  AND agg_gte_level_error > agg_gte_level_fatal
ORDER BY max_timestamp DESC;
```

### Coordinator Queries

#### 4. Pending Consolidation
```sql
SELECT clp_ir_path, min_timestamp
FROM clp_spark
WHERE state = 'IR_ARCHIVE_CONSOLIDATION_PENDING'
ORDER BY min_timestamp ASC
LIMIT 1000;
```

#### 5. Expired Files
```sql
SELECT clp_ir_path, clp_archive_path
FROM clp_spark
WHERE expires_at > 0 AND expires_at <= [NOW]
  AND state NOT IN ('IR_PURGING', 'ARCHIVE_PURGING')
LIMIT 1000;
```

### Monitoring Queries

#### 6. State Distribution
```sql
SELECT state, COUNT(*) AS file_count
FROM clp_spark
GROUP BY state;
```

#### 7. Storage by Service
```sql
SELECT dim_str128_service,
  COUNT(*) AS files,
  SUM(raw_size_bytes) AS raw_bytes,
  SUM(clp_archive_size_bytes) AS archive_bytes
FROM clp_spark
GROUP BY dim_str128_service;
```

#### 8. Consolidation Latency
```sql
SELECT clp_ir_path, dim_str128_service,
  ([NOW] - min_timestamp) / 60 AS minutes_waiting
FROM clp_spark
WHERE state = 'IR_ARCHIVE_CONSOLIDATION_PENDING'
ORDER BY min_timestamp ASC
LIMIT 100;
```

### Operational Queries

#### 9. Extend Retention (Incident)
```sql
UPDATE clp_spark
SET retention_days = 395, expires_at = min_timestamp + (395 * 86400)
WHERE dim_str128_service = 'auth-service'
  AND min_timestamp >= [INCIDENT_START]
  AND min_timestamp <= [INCIDENT_END]
  AND retention_days < 395
LIMIT 10000;
```

### Ingestion Patterns

#### 10. Insert + State Transitions
```sql
-- Insert
INSERT INTO clp_spark (
  clp_ir_storage_backend, clp_ir_bucket, clp_ir_path,
  state, min_timestamp, dim_str128_service
) VALUES ('s3', 'bucket', '/ir/file.clp', 'IR_ARCHIVE_BUFFERING', 1704067200, 'auth');

-- Close file (hash lookup)
UPDATE clp_spark
SET state = 'IR_ARCHIVE_CONSOLIDATION_PENDING', record_count = [COUNT]
WHERE clp_ir_path_hash = UNHEX(MD5('/ir/file.clp'))
  AND min_timestamp = 1704067200;

-- Consolidation complete
UPDATE clp_spark
SET state = 'ARCHIVE_CLOSED',
    clp_archive_storage_backend = 's3',
    clp_archive_bucket = 'archive-bucket',
    clp_archive_path = '/archives/archive.clp'
WHERE clp_ir_path_hash = UNHEX(MD5('/ir/file.clp'))
  AND min_timestamp = 1704067200;
```

---

## See Also

- [Metadata Schema](metadata-schema.md) — Entry types, lifecycle, denormalization rationale, partitioning
- [Metadata Tables Reference](../reference/metadata-tables.md) — DDL, column reference, scalability projections
- [Early Termination Design](../design/early-termination.md) — Runnable example, Presto integration, streaming, edge cases
- [Keyset Pagination](../design/keyset-pagination.md) — How `SplitQueryEngine` pages through metadata using keyset cursors (ORDER BY column values + row `id` as tiebreaker)
- [Platform Comparison](../reference/platform-comparison.md) — How early termination compares across systems
- [Performance Tuning](../operations/performance-tuning.md) — Benchmarks, index strategy, query optimization
- [Naming Conventions](../reference/naming-conventions.md) — Column, index, and state naming patterns
- [Glossary](glossary.md) — CLP terminology and data format definitions
