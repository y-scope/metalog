# Metadata Tables Reference

[← Back to docs](../README.md)

DDL, column reference, index reference, and scalability projections for the `clp_spark` metadata table. For conceptual design (entry types, lifecycle, denormalization rationale, partitioning), see [Metadata Schema](../concepts/metadata-schema.md).

**Related:** [Query Execution](../concepts/query-execution.md) · [Naming Conventions](naming-conventions.md) · [Glossary](../concepts/glossary.md)

---

## Column Reference

### Core Columns

| Column | Type | Purpose |
|--------|------|---------|
| `id` | BIGINT AUTO_INCREMENT | Primary key (second component of composite PK) |
| `min_timestamp`, `max_timestamp` | BIGINT | Event time bounds (epoch nanoseconds) |
| `clp_archive_created_at` | BIGINT | Archive creation time (epoch nanoseconds; 0 if not yet consolidated) |
| `clp_ir_*` | storage_backend, bucket, path | IR file location (NULL for archive-only entries) |
| `clp_archive_*` | storage_backend, bucket, path | Archive location (NULL until consolidated) |
| `state` | ENUM | Lifecycle state |
| `record_count` | INT UNSIGNED | Total log records in file |
| `raw_size_bytes` | BIGINT | Original uncompressed source size (BIGINT to support files exceeding 4 GB) |
| `clp_ir_size_bytes` | INT UNSIGNED | IR file size in bytes |
| `clp_archive_size_bytes` | INT UNSIGNED | Archive size in bytes |
| `retention_days`, `expires_at` | SMALLINT UNSIGNED, BIGINT | Retention policy and computed expiry (epoch nanoseconds) |

### Query Optimization Columns

| Category | Physical Name | Purpose | Early Termination |
|----------|---------------|---------|:------------:|
| **Dimensions** | `dim_fNN` (opaque placeholder) | Filter by categorical content attributes | Yes |
| **Aggregations** | `agg_fNN` (opaque placeholder) | Pre-computed match counts | Yes |
| **Sketches** | `sketches` SET + `ext` BLOB | Probabilistic "definitely not" pruning | No |

*See [Storage Design: Denormalized Fact Table](../concepts/metadata-schema.md#denormalized-fact-table) for design rationale.*

**Dimension columns** (`dim_fNN`): Only fields that are **constant within a file** — every record shares the same value — qualify as dimension columns. This invariant is what enables exact file-level elimination: a file with `dim_service = 'auth'` contains only `service=auth` records, so files that don't match a dimension predicate are eliminated entirely without scanning. Physical names are opaque placeholders assigned sequentially by `ColumnRegistry`; the logical field name (e.g., `service`) is stored in `_dim_registry.dim_key`. The query API resolves placeholders to logical names before returning results, so callers always work with `service = 'auth'`, never `dim_f01 = 'auth'`.

**Aggregation columns** (`agg_fNN`): Pre-computed per-file statistics that enable pruning and early termination without opening files. The `aggregation_type` in `_agg_registry` defines what is stored:

| Category | Types | Use case |
|----------|-------|----------|
| Cumulative threshold counts | `GTE`, `GT`, `LTE`, `LT` | Cumulative count of records above/below a threshold (e.g., `level ≥ ERROR` = ERROR + FATAL combined); exact equality counts are derived by subtraction (`ERROR only = error_GTE − fatal_GTE`) — enables TopN early termination |
| Equality counts | `EQ` | Count of records exactly matching a value — enables TopN early termination |
| Numeric aggregations | `MAX`, `MIN`, `AVG`, `SUM` | Pre-computed statistics over numeric fields — enables range-based file skipping (e.g., skip files where `max(latency_ms) < threshold`) |

All three categories compose with `dim_*` filters: because dimension filters eliminate entire files, the surviving files have every record satisfying those predicates, making aggregation values exact for the full combined predicate.

```sql
-- Conceptual: files where level=error count > level=fatal count → has ERROR (non-FATAL) logs
-- Physically: WHERE agg_f03 > agg_f04 (where f03=error GTE, f04=fatal GTE)
```

**Sketches** (`sketches` + `ext`): For high-cardinality fields (user_id, trace_id) where dimension columns are impractical. The `sketches` SET column declares which sketch types are present (e.g., bloom filter, cuckoo filter); the `ext` MEDIUMBLOB stores the serialized filter data.

The metastore's role with sketches is purely **"definitely not" elimination**: if the sketch says a value is absent, the file is skipped entirely; if the sketch says the value might be present, the file passes through. Sketches have false positives — a file that passes the sketch filter may not actually contain the value — so the query engine filters surviving files to produce exact results. This two-phase model (metastore prunes → query engine confirms) means sketches enable split pruning but not early termination: the metastore cannot guarantee a match count, only the absence of one.

### Hash Columns (VIRTUAL)

Path columns can be up to 4096 bytes (utf8mb4), exceeding MariaDB/MySQL's 3072-byte index limit. VIRTUAL columns with MD5 hash provide 16-byte keys stored only in the index.

```sql
clp_ir_path_hash BINARY(16) AS (UNHEX(MD5(clp_ir_path))) VIRTUAL
```

**Query pattern:**
```sql
WHERE clp_ir_path_hash = UNHEX(MD5('/path/to/file'))  -- O(1) lookup
-- NOT: WHERE clp_ir_path = '/path/to/file'           -- Full table scan!
```

See [Query Execution: VIRTUAL Columns](../concepts/query-execution.md#virtual-columns-and-hash-indexes) for details.

---

## Schema Definition

```sql
CREATE TABLE clp_spark (
    -- Primary key: (min_timestamp, id) for partitioning
    id                          BIGINT AUTO_INCREMENT,
    min_timestamp               BIGINT NOT NULL,
    max_timestamp               BIGINT NOT NULL DEFAULT 0,
    clp_archive_created_at      BIGINT NOT NULL DEFAULT 0,

    -- Storage locations (IR file; NULL = archive-only entry)
    clp_ir_storage_backend      VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    clp_ir_bucket               VARCHAR(63) CHARACTER SET ascii COLLATE ascii_bin NULL,
    clp_ir_path                 VARCHAR(1024) NULL,

    -- Storage locations (archive; NULL = not yet consolidated)
    clp_archive_storage_backend VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    clp_archive_bucket          VARCHAR(63) CHARACTER SET ascii COLLATE ascii_bin NULL,
    clp_archive_path            VARCHAR(1024) NULL,

    -- VIRTUAL columns for hash-based lookups
    clp_ir_path_hash            BINARY(16) AS (UNHEX(MD5(clp_ir_path))) VIRTUAL,
    clp_archive_path_hash       BINARY(16) AS (UNHEX(MD5(clp_archive_path))) VIRTUAL,

    -- Lifecycle state
    state ENUM('IR_BUFFERING', 'IR_CLOSED', 'IR_PURGING', 'ARCHIVE_CLOSED', 'ARCHIVE_PURGING',
               'IR_ARCHIVE_BUFFERING', 'IR_ARCHIVE_CONSOLIDATION_PENDING') NOT NULL,

    -- Metrics
    record_count                INT UNSIGNED NOT NULL DEFAULT 0,
    raw_size_bytes              BIGINT NULL,              -- original uncompressed size (may exceed 4 GB)
    clp_ir_size_bytes           INT UNSIGNED NULL,        -- IR file size (max 4 GB)
    clp_archive_size_bytes      INT UNSIGNED NULL,        -- archive size (max 4 GB)

    -- Retention
    retention_days              SMALLINT UNSIGNED NOT NULL DEFAULT 30,
    expires_at                  BIGINT NOT NULL DEFAULT 0,

    -- Dimensions, aggregations, and sketches (added dynamically; see Column Reference)
    sketches                    SET('s01','s02','s03','s04','s05','s06','s07','s08',
                                    's09','s10','s11','s12','s13','s14','s15','s16',
                                    's17','s18','s19','s20','s21','s22','s23','s24',
                                    's25','s26','s27','s28','s29','s30','s31','s32') NULL,
    ext                         MEDIUMBLOB NULL,

    -- Indexes (see Index Reference for detailed analysis)
    PRIMARY KEY (min_timestamp, id),
    KEY idx_id (id),
    UNIQUE KEY idx_clp_ir_hash (clp_ir_path_hash, min_timestamp),
    INDEX idx_clp_archive_hash (clp_archive_path_hash),
    INDEX idx_consolidation (state, min_timestamp ASC),
    INDEX idx_expiration (expires_at ASC),
    INDEX idx_max_timestamp (max_timestamp DESC)

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_bin

PARTITION BY RANGE (min_timestamp) (
    PARTITION p_20240101 VALUES LESS THAN (1704067200000000000),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);
```

**Design decisions:**

| Decision | Rationale |
|----------|-----------|
| Composite PK `(min_timestamp, id)` | MySQL requires partition key in PK; enables RANGE partitioning |
| VIRTUAL hash columns | 99.6% smaller indexes (16 bytes vs 4096) |
| Daily RANGE partitioning | Buffer pool locality, partition pruning, housekeeping |
| File timestamps as BIGINT (epoch nanoseconds) | Sub-second precision for event times; no timezone issues; 0 = unknown |
| Nullable path columns | NULL propagates to hash (no unique conflicts for archive-only entries) |

For index design rationale, see [Query Execution: Index Reference](../concepts/query-execution.md#index-reference).

---

## Index Reference

The schema defines 7 indexes. Each serves a specific access pattern.

| Index | Columns | Unique | Purpose | Used By |
|-------|---------|:------:|---------|---------|
| PRIMARY KEY | `(min_timestamp, id)` | Y | Partition key + row ID; write locality within daily partitions | All queries (InnoDB clustered index) |
| `idx_id` | `(id)` | — | AUTO_INCREMENT on partitioned tables requires `id` as the first column in some index | Internal (MySQL partitioning requirement) |
| `idx_clp_ir_hash` | `(clp_ir_path_hash, min_timestamp)` | Y | UPSERT duplicate detection, IR path lookups | Ingestion (UPSERT), Planner, state transitions |
| `idx_clp_archive_hash` | `(clp_archive_path_hash)` | — | Archive path lookups after consolidation | Query engine (archive resolution) |
| `idx_consolidation` | `(state, min_timestamp ASC)` | — | Find consolidation-pending files ordered by age | Planner (pending files query) |
| `idx_expiration` | `(expires_at ASC)` | — | Find expired files for retention cleanup | Retention goroutine (expired files query) |
| `idx_max_timestamp` | `(max_timestamp DESC)` | — | Time-range queries ordered by recency | Query engine (time-range queries) |

### Why `idx_clp_ir_hash` Is UNIQUE but `idx_clp_archive_hash` Is Not

IR paths are 1:1 with metadata rows — each IR file has exactly one row. The UNIQUE constraint enforces this and enables `ON DUPLICATE KEY UPDATE` to find the existing row for upserts.

Archive paths are 1:N — one archive contains multiple consolidated IR files, so multiple rows share the same `clp_archive_path`. A UNIQUE constraint would prevent this.

### Why `idx_id` Exists Separately

On partitioned tables, MySQL requires the AUTO_INCREMENT column to be the first (or only) column in some index. Since `id` is the second column in the primary key `(min_timestamp, id)`, a separate `KEY idx_id (id)` satisfies this requirement. It adds minimal overhead (~8 bytes/row) and is not used for application queries.

### Composite Index Column Ordering

**`idx_clp_ir_hash (clp_ir_path_hash, min_timestamp)`**: Hash first because all lookups filter by path. `min_timestamp` is included because MySQL requires the partition key in every unique index (see [Partitioning: Composite Primary Key](../concepts/metadata-schema.md#composite-primary-key)).

**`idx_consolidation (state, min_timestamp ASC)`**: State first for equality filtering (`state = 'IR_ARCHIVE_CONSOLIDATION_PENDING'`), then `min_timestamp` ascending for oldest-first ordering. The Planner reads files in age order to consolidate the oldest data first.

---

## Schema Evolution

Dynamic columns are added automatically by `ColumnRegistry` when new fields appear in ingested records. No manual DDL is required.

**Physical column names use opaque placeholders:**
- Dimension columns: `dim_f01`, `dim_f02`, ... (up to `dim_f999`)
- Aggregation columns: `agg_f01`, `agg_f02`, ... (up to `agg_f999`)

The semantic mapping from physical name to logical field name is stored in `_dim_registry` and `_agg_registry`. The public query API resolves placeholders back to logical names before returning results.

See [Schema Evolution](../guides/evolve-schema.md) for the slot allocation algorithm, the 255-byte boundary rule, and MariaDB vs. MySQL DDL locking differences.

---

## Dimension Base Type Reference

Dimension columns use 5 base types stored in `_dim_registry.base_type`. The width for string types is stored in `_dim_registry.width` and may grow as wider values arrive.

| `base_type` | SQL Column Type | Notes |
|-------------|-----------------|-------|
| `str` | `VARCHAR(width) CHARACTER SET ascii COLLATE ascii_bin NULL` | ASCII strings; width up to 65,535 |
| `str_utf8` | `VARCHAR(width) NULL` | UTF-8 strings (e.g., Unicode field names) |
| `bool` | `TINYINT(1) NULL` | Boolean (0 or 1) |
| `int` | `BIGINT NULL` | 64-bit signed integer |
| `float` | `DOUBLE NULL` | 64-bit IEEE 754 double |

**String width sizing:**

| Width Range | VARCHAR Length Prefix | Impact |
|-------------|----------------------|--------|
| <= 255 bytes | 1 byte | Can expand in-place (`MODIFY COLUMN ALGORITHM=INPLACE`) |
| > 255 bytes | 2 bytes | Crosses the length prefix boundary — triggers slot invalidation + new slot |

The 255-byte boundary is the InnoDB threshold where MySQL changes internal encoding. Crossing it requires a new column slot; the old slot is marked `INVALIDATED` in `_dim_registry`.

**Aggregation columns** use `BIGINT NOT NULL DEFAULT 0` (for `INT` value type) or `DOUBLE NOT NULL DEFAULT 0.0` (for `FLOAT` value type). The `aggregation_type` in `_agg_registry` controls how the aggregation relates to the underlying values. For example, if `aggregation_type=GTE` and `agg_value=error`:
- The count = ERROR + FATAL (everything at or above ERROR severity)
- Individual `ERROR-only = error_GTE - fatal_GTE`

---

## Scalability

### Summary

| Metric | Value |
|--------|-------|
| Row size | ~250 bytes (LZ4 compressed) |
| Index overhead | ~179 bytes/row (7 indexes, 2 dimensions) |
| Per-dimension overhead | ~39 bytes/row |

**Typical deployment (5M files/day, 30-day retention):**
- Per table: 150M rows, ~62 GB
- System-wide (5 tables): 750M rows, ~310 GB

### Throughput

Measured with a single writer goroutine, `interpolateParams=true`, 2 dimension columns, on a single-node database (see [Performance Tuning](../operations/performance-tuning.md) for full methodology and batch size guidance):

| Operation | MariaDB | MySQL |
|-----------|---------|-------|
| batch-UPSERT (500 rows/batch) | ~19K rows/sec | ~14K rows/sec |
| Time-range + dimension query (30-day table, 5M rows) | 10-100 ms | 10-100 ms |
| Point lookup (hash index) | <10 ms | <10 ms |

### Horizontal Scaling

Each coordinator writes to one table. Multiple coordinators can share one DB instance.

```
Single DB (has headroom):          Scale out (when DB saturates):
┌─────────────────────────┐        ┌─────────┐  ┌─────────┐  ┌─────────┐
│       MariaDB/MySQL     │        │ DB 1    │  │ DB 2    │  │ DB 3    │
│ ┌─────┐ ┌─────┐ ┌─────┐ │        │ table_a │  │ table_b │  │ table_c │
│ │tbl_a│ │tbl_b│ │tbl_c│ │        └────┬────┘  └────┬────┘  └────┬────┘
│ └──┬──┘ └──┬──┘ └──┬──┘ │             │            │            │
└────┼───────┼───────┼────┘        Coordinator  Coordinator  Coordinator
     │       │       │
Coord 1  Coord 2  Coord 3
```

### Storage Projections

| Files/Day | Rows (30 days) | Table Data | Indexes (2 dim) | Total |
|-----------|----------------|------------|-----------------|-------|
| 1M | 30M | ~7 GB | ~5 GB | ~12 GB |
| **5M** | **150M** | **~35 GB** | **~27 GB** | **~62 GB** |
| 10M | 300M | ~70 GB | ~54 GB | ~124 GB |
| 50M | 1.5B | ~350 GB | ~268 GB | ~618 GB |

### Dimension Scaling

Each indexed dimension adds ~39 bytes/row.

| Rows | 2 dim | 5 dim | 10 dim |
|------|-------|-------|--------|
| 30M | 5.4 GB | 8.3 GB | 13.9 GB |
| 150M | 26.8 GB | 41.4 GB | 69.6 GB |
| 300M | 53.6 GB | 82.8 GB | 139.2 GB |

---

## Database Compatibility

### Supported Databases

| Database | Version | Compression | Partitioning | Tested |
|----------|---------|-------------|--------------|--------|
| MariaDB | 10.4+ | LZ4 | Yes | Integration tests |
| MySQL | 8.0+ | LZ4 | Yes | Integration tests |
| Aurora | 8.0+ | LZ4 | Yes | Production |
| TiDB | 5.0+ | None | Yes | Manual |
| Vitess | Any | None | Auto-disabled | Manual |

### Why MariaDB/MySQL

- **Efficient UPSERTs**: InnoDB in-place updates minimize bloat
- **Partition pruning**: O(1) time-range filtering
- **Scale**: Up to 8,192 partitions (22+ years daily)

### Standard MySQL vs Vitess

Standard MySQL recommended. Vitess is supported but suboptimal:

| Factor | Standard MySQL | Vitess |
|--------|----------------|--------|
| Time-range queries | Scans 1-2 partitions | Scatter-gather all shards |
| Write latency | Single path | VTGate → VTTablet → MySQL |

---

## Implementation Reference

| Component | Location |
|-----------|----------|
| Schema | `schema/schema.sql` |
| File Records | `internal/metastore/filerecords.go` |
| Partition Manager | `internal/schema/partitionmanager.go` |
| Performance | [Performance Tuning](../operations/performance-tuning.md) |
| Benchmark | `integration-tests/benchmarks/kafka-ingestion/run.py`, `integration-tests/benchmarks/grpc-ingestion/run.py` |

**Critical DSN setting:** `interpolateParams=true` required for optimal performance — avoids server-side prepared statements and reduces round-trips.

## See Also

- [Metadata Schema](../concepts/metadata-schema.md) — Entry types, lifecycle, denormalization rationale, partitioning
- [Query Execution](../concepts/query-execution.md) — Split pruning, early termination, query patterns
- [Schema Evolution](../guides/evolve-schema.md) — Online DDL for dynamic column addition
- [Coordinator HA](../design/coordinator-ha.md) — Failover and recovery
- [Consolidation](../concepts/consolidation.md) — IR→Archive pipeline and state transitions
- [Configuration](configuration.md) — Partition maintenance settings
- [Performance Tuning](../operations/performance-tuning.md) — batch-UPSERT benchmarks and index overhead
- [Naming Conventions](naming-conventions.md) — Column, index, and state naming patterns
- [Glossary](../concepts/glossary.md) — CLP terminology and data format definitions
