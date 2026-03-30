# Schema Evolution

[← Back to docs](../README.md)

How to manage dynamic schema evolution for metadata tables — adding new dimensions and aggregates, handling string width expansion, and understanding the online DDL behavior.

## Overview

The `clp_spark` metadata table grows its columns dynamically as new dimensions and aggregates arrive from ingested records. The system uses **placeholder column names** backed by registry tables to handle this cleanly.

### Why Not Encode Field Names in Column Names?

The legacy approach (e.g., `dim_str128_application_id`) had three problems:

| Problem | Detail |
|---------|--------|
| Special characters | Field names with `.`, `@`, `-`, `/` (e.g., `@timestamp`, `k8s.pod`) can't be encoded in SQL column names |
| 64-char MySQL limit | The `dim_str128_` prefix alone eats 11+ characters, leaving little room |
| No `DROP COLUMN` | `DROP COLUMN` triggers a full table rebuild on MariaDB — so columns are recycled in-place via the INVALIDATED → AVAILABLE lifecycle |

### The ColumnRegistry Solution

Instead of encoding semantics in the column name, columns use **opaque placeholder names** (`dim_f01`, `agg_f01`). Three registry tables store the real meaning:

```
dim_f01  ──►  _dim_registry  ──►  dim_key = "k8s.pod.name", base_type = "str", width = 128
agg_f01  ──►  _agg_registry  ──►  agg_key = "level", agg_value = "error", aggregation_type = EQ, value_type = INT
```

The `ColumnRegistry` type (in the `schema` package) manages this mapping.

## Placeholder Column Names

| Placeholder | Format | Range | Example |
|-------------|--------|-------|---------|
| Dimension | `dim_fNN` | `dim_f01`–`dim_f99` | `dim_f01`, `dim_f12` |
| Aggregation | `agg_fNN` | `agg_f01`–`agg_f99` | `agg_f01`, `agg_f07` |

Slot numbers are assigned sequentially as new fields appear. Slot 1 goes to the first new dimension, slot 2 to the second, and so on.

## Registry Tables

### `_dim_registry`

Maps `dim_fNN` placeholder names to field metadata:

```sql
CREATE TABLE _dim_registry (
    table_name      VARCHAR(64) NOT NULL,
    column_name     VARCHAR(64) NOT NULL,        -- "dim_f01"
    base_type       ENUM('str','str_utf8','bool','int','float') NOT NULL,
    width           SMALLINT UNSIGNED NULL,       -- VARCHAR width (str/str_utf8 only)
    dim_key         VARCHAR(1024) NOT NULL,       -- original field name, may contain special chars
    alias_column    VARCHAR(64)   NULL,           -- optional human-readable alias
    state           ENUM('ACTIVE','INVALIDATED','AVAILABLE') NOT NULL,
    created_at      BIGINT NOT NULL,
    invalidated_at  BIGINT NULL,
    PRIMARY KEY (table_name, column_name),
    INDEX idx_dim_lookup (table_name, dim_key(255), state),
    FOREIGN KEY (table_name) REFERENCES _table(table_name) ON DELETE CASCADE
) ENGINE=InnoDB;
```

### `_agg_registry`

Maps `agg_fNN` placeholder names to aggregate metadata:

```sql
CREATE TABLE _agg_registry (
    table_name        VARCHAR(64)   NOT NULL,
    column_name       VARCHAR(64)   NOT NULL,        -- "agg_f01"
    agg_key           VARCHAR(1024) NOT NULL,        -- field being aggregated (e.g., "level")
    agg_value         VARCHAR(1024) NULL,           -- specific value (e.g., "error"), null for total
    aggregation_type  ENUM('EQ','GTE','GT','LTE','LT','SUM','AVG','MIN','MAX') NOT NULL DEFAULT 'EQ',
    value_type        ENUM('INT','FLOAT') NOT NULL DEFAULT 'INT',
    alias_column      VARCHAR(64)   NULL,            -- if set, this agg aliases an existing column
    state             ENUM('ACTIVE','INVALIDATED','AVAILABLE') NOT NULL,
    created_at        BIGINT  NOT NULL,
    invalidated_at    BIGINT  NULL,
    PRIMARY KEY (table_name, column_name),
    INDEX idx_agg_lookup (table_name, agg_key(255), state),
    FOREIGN KEY (table_name) REFERENCES _table(table_name) ON DELETE CASCADE
) ENGINE=InnoDB;
```

## Column Lifecycle

```
              ┌─────────────────────────────────────────────────────────┐
              │                                                         │
              ▼                                                         │
         [Slot allocated or reclaimed]                                  │
              │                                                         │
              ▼                                                         │
          ACTIVE ──► [admin InvalidateColumn / width crosses 255] ──► INVALIDATED
              ▲                                                         │
              │                                                         ▼
              │                                              [recycler: wait for retention,
              │                                               clear remaining rows]
              │                                                         │
              └──────────── [claimAvailableSlot] ◄── AVAILABLE ◄────────┘
```

| Status | Description |
|--------|-------------|
| `ACTIVE` | Slot is in use; column receives data and is visible to queries |
| `INVALIDATED` | Slot retired via `InvalidateColumn` RPC or width-boundary crossing; excluded from ingestion and queries |
| `AVAILABLE` | Background recycler has cleared remaining data; slot is ready for reuse by a new key |

`ColumnRegistry` only loads `ACTIVE` entries at startup. `INVALIDATED` columns remain in the
table physically (dropping them would require a full rebuild on MariaDB) but are excluded from
ingestion and queries. A background recycler scans hourly for `INVALIDATED` columns that have
aged past 30 days and have fewer than 10,000 remaining non-NULL rows, clears the data, and
transitions them to `AVAILABLE`. New allocations check for `AVAILABLE` slots before creating
fresh columns, using `SELECT ... FOR UPDATE SKIP LOCKED` for deadlock-free concurrent claiming.

## Slot Allocation

### Single-Slot Allocation

Used by `ResolveOrAllocateDim` / `ResolveOrAllocateAgg` for individual field resolution:

1. **Fast path** (RLock): check in-memory cache — if `dimKey` already mapped, return cached column name
2. **Slow path** (allocMu): re-check cache (double-checked locking), then:
   1. `ALTER TABLE ADD COLUMN dim_fNN <type>, ALGORITHM=INPLACE, LOCK=<mode>`
   2. Insert registry row (physical column exists, so no orphaned registry row on crash)
   3. Advance `nextDimSlot` and update in-memory cache

### Batch Allocation

Used by `ResolveOrAllocateDims` / `ResolveOrAllocateAggs` (called from the ingestion service) for bulk field resolution. On cold start — when a table first encounters many new fields — this avoids N individual `ALTER TABLE` statements:

1. **Fast path** (RLock): partition requests into resolved, needs-widening, and unresolved
2. Width expansion cases are handled individually (each is a `MODIFY COLUMN`)
3. **Batch path** (allocMu): all unresolved columns are added in a single multi-column `ALTER TABLE`:
   ```sql
   ALTER TABLE t ADD COLUMN dim_f01 VARCHAR(255) NULL, ADD COLUMN dim_f02 BIGINT NULL, ...
   ```
4. If the batch ALTER fails with a duplicate column error (crash recovery), falls back to per-column ALTERs with `isDuplicateColumn` tolerance
5. `nextDimSlot` is advanced immediately after the ALTER succeeds, before registry INSERTs — so if an INSERT fails mid-batch, retries generate fresh slot names

### Concurrency Safety

The `allocateNewDimSlot`, `allocateNewAggSlot`, `batchAllocateDimSlots`, and `batchAllocateAggSlots` methods are guarded by `allocMu` (`sync.Mutex`) on the `ColumnRegistry` instance, serializing all DDL within a process. The ALTER-before-INSERT ordering ensures no orphaned registry rows: if the node crashes after ALTER but before INSERT, the next allocation attempt will encounter `isDuplicateColumn` and skip the existing physical column.

Slot numbers may have gaps (e.g., from crash recovery). That's safe — the sequence is never relied upon for correctness.

### Dim Column SQL Types

| `base_type` | SQL type |
|-------------|----------|
| `str` | `VARCHAR(width) CHARACTER SET ascii COLLATE ascii_bin NULL` |
| `str_utf8` | `VARCHAR(width) NULL` |
| `bool` | `TINYINT(1) NULL` |
| `int` | `BIGINT NULL` |
| `float` | `DOUBLE NULL` |

All dimension columns are `NULL` (absent from records that don't have the field).

### Aggregation Column SQL Types

Aggregation columns use `BIGINT NULL` (for `value_type = INT`) or `DOUBLE NULL` (for `value_type = FLOAT`). `NULL` means the aggregation is absent for that record (distinguishable from zero).

## String Width Expansion

String dimensions have a `width` (max VARCHAR length). If a later record reports a longer value:

| Old width | New width | Strategy |
|-----------|-----------|----------|
| ≤ 255 | ≤ 255 | **In-place** (`MODIFY COLUMN` with `ALGORITHM=INPLACE`) — free |
| ≤ 255 | > 255 | **Invalidate + new slot** — column crosses the 1-byte/2-byte length prefix boundary, which requires a table rebuild |
| > 255 | > 255 | **In-place** if the new width still fits in the row; in practice rare |

The 255 boundary is the InnoDB row-format threshold where MySQL changes internal length encoding.

## Online DDL Locking

All ALTER TABLE statements across the `schema` package use `ALGORITHM=INPLACE` with a dialect-aware `LOCK` mode, centralized via the `lockMode(isMariaDB)` helper in `ddl.go`:

```go
// lockMode returns "SHARED" for MariaDB, "NONE" for MySQL 8.0+.
func lockMode(isMariaDB bool) string {
    if isMariaDB {
        return "SHARED"
    }
    return "NONE"
}
```

This applies to: `ColumnRegistry` (ADD COLUMN, MODIFY COLUMN), `Evolver` (ADD COLUMN), and `IndexManager` (ADD INDEX).

### MariaDB 10.4+: `LOCK=SHARED`

- Concurrent reads (SELECT)
- Blocks concurrent writes (INSERT, UPDATE) for the duration of the ALTER

**Why not `LOCK=NONE`?** MariaDB cannot use `LOCK=NONE` on tables with indexed virtual columns (which `clp_spark` has for the `clp_ir_path_hash` and `clp_archive_path_hash` hash-index columns). Attempting it yields:
```
LOCK=NONE is not supported. Reason: online rebuild with indexed virtual columns. Try LOCK=SHARED
```

**Impact:** New columns are added infrequently (only on first encounter of a new field). The connection pool queues blocked inserts automatically. In steady-state operation after the initial schema is learned, schema changes are rare.

### MySQL 8.0+: `LOCK=NONE`

- Concurrent reads (SELECT)
- Concurrent writes (INSERT, UPDATE)

MySQL 8.0 supports `LOCK=NONE` even with indexed virtual columns, giving zero-impact schema changes.

### Comparison

| | MariaDB 10.4+ | MySQL 8.0+ |
|---|---|---|
| `LOCK=NONE` with virtual columns | Not supported | Supported |
| `LOCK=SHARED` | Required | Supported |
| Write impact during ALTER | Brief block (~100ms) | None |

## Dynamic Indexes

The `IndexManager` type (in the `schema` package) reconciles index configuration at startup and on config reload:

- **Creates** indexes that are enabled in config but missing from the table
- **Drops** indexes that are disabled in config (if they exist and aren't protected)
- **Skips** columns that haven't been allocated yet (logs a warning)

Index DDL uses `ALGORITHM=INPLACE, LOCK=<mode>` via the same `lockMode` helper as column allocation.

### Protected Indexes (Never Dropped)

| Index | Purpose |
|-------|---------|
| `PRIMARY` | Primary key |
| `idx_consolidation` | Core index for pending file queries |
| `idx_expiration` | Core index for retention/deletion queries |

## Column Aliases

Both `_dim_registry` and `_agg_registry` have an `alias_column` field that provides a human-readable name for a physical column. Aliases are used by the query path to return semantic names (e.g., `hostname`) instead of physical names (e.g., `dim_f01`).

### Setting Aliases

Aliases are managed via the `SetColumnAlias` admin gRPC RPC. The RPC writes directly to the database only — it does not update in-memory caches. This keeps the admin API stateless and safe in multi-node deployments.

### Multi-Node Propagation

Each coordinator node runs a `runAliasRefresh` goroutine that calls `ColumnRegistry.RefreshAliases()` every minute. This method:

1. SELECTs `column_name, alias_column` from both registry tables
2. Compares with the in-memory cache under a write lock
3. Replaces entries whose alias changed with new immutable copies (preserving concurrent reader safety)

The 1-minute polling interval means alias changes propagate to all nodes within ~60 seconds.

## Demo

The gRPC ingestion benchmark exercises automatic schema evolution end-to-end. It starts MariaDB, runs the coordinator, and produces records that introduce new dimensions and aggregates — triggering live `ALTER TABLE ADD COLUMN` operations:

```bash
# From the metalog directory
go run ./test/benchmarks/ingestion --mode grpc --records 10000
```

Prerequisites: Docker, Go 1.25+.

## Startup Behaviour

At coordinator startup:

1. `BaseSchemaValidator` checks all system tables and the template table against expected columns, types, indexes, and partitioning (catches stale schemas before data operations begin)
2. `ColumnRegistry` loads all `ACTIVE` entries for the table from `_dim_registry` and `_agg_registry` into in-memory caches (including `alias_column`)
3. Subsequent `ResolveOrAllocateDim()` / `ResolveOrAllocateAgg()` calls check the cache first (no DB round-trip for known fields)
4. The `runAliasRefresh` goroutine starts polling for alias changes every minute

Log output on startup:
```
base schema validation passed
column registry loaded  {"table": "clp_spark", "dims": 12, "aggs": 5}
```

## See Also

- [Metadata Tables](../reference/metadata-tables.md) — Full `clp_spark` schema including base and dynamic columns
- [Naming Conventions](../reference/naming-conventions.md) — Column naming patterns
- [Architecture Overview](../concepts/overview.md) — Writer goroutine triggers schema evolution
- [Performance Tuning](../operations/performance-tuning.md) — Index overhead from dynamic columns
