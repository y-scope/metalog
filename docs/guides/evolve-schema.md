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
| No column recycling | `DROP COLUMN` triggers a full table rebuild on MariaDB — so stale columns accumulate forever |

### The ColumnRegistry Solution

Instead of encoding semantics in the column name, columns use **opaque placeholder names** (`dim_f01`, `agg_f01`). Three registry tables store the real meaning:

```
dim_f01  ──►  _dim_registry  ──►  dim_key = "k8s.pod.name", base_type = "str", width = 128
agg_f01  ──►  _agg_registry  ──►  agg_key = "level", agg_value = "error", aggregation_type = EQ, value_type = INT
```

The `ColumnRegistry` class (in `metastore/schema/ColumnRegistry.java`) manages this mapping.

## Placeholder Column Names

| Placeholder | Format | Range | Example |
|-------------|--------|-------|---------|
| Dimension | `dim_fNN` | `dim_f01`–`dim_f999` | `dim_f01`, `dim_f12` |
| Aggregation | `agg_fNN` | `agg_f01`–`agg_f999` | `agg_f01`, `agg_f07` |

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
    status          ENUM('ACTIVE','INVALIDATED','AVAILABLE') NOT NULL,
    created_at      INT UNSIGNED NOT NULL DEFAULT (UNIX_TIMESTAMP()),
    invalidated_at  INT UNSIGNED NULL,
    PRIMARY KEY (table_name, column_name),
    INDEX idx_dim_lookup (table_name, dim_key(255), status),
    FOREIGN KEY (table_name) REFERENCES _table(table_name)
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
    status            ENUM('ACTIVE','INVALIDATED','AVAILABLE') NOT NULL,
    created_at        INT UNSIGNED  NOT NULL DEFAULT (UNIX_TIMESTAMP()),
    invalidated_at    INT UNSIGNED  NULL,
    PRIMARY KEY (table_name, column_name),
    INDEX idx_agg_lookup (table_name, agg_key(255), status),
    FOREIGN KEY (table_name) REFERENCES _table(table_name)
) ENGINE=InnoDB;
```

## Column Lifecycle

```
                  ┌──────────────────────────────────────────────────────┐
                  │                                                      │
                  ▼                                                      │
             [Slot allocated]                                            │
                  │                                                      │
                  ▼                                                      │
              ACTIVE ──► [width expansion crosses 255-byte boundary] ──► INVALIDATED
                  │
                  ▼
           [future: recycled]
              AVAILABLE
```

| Status | Description |
|--------|-------------|
| `ACTIVE` | Slot is in use; column contains live data |
| `INVALIDATED` | Slot retired (e.g., type widened beyond in-place limit); column still exists but is ignored |
| `AVAILABLE` | Slot available for reuse (reserved for future recycling support) |

`ColumnRegistry` only loads `ACTIVE` entries at startup. `INVALIDATED` columns remain in the table physically (dropping them would require a full rebuild on MariaDB) but are excluded from all queries.

## Slot Allocation

### How It Works

1. Check in-memory cache: if `dimKey` already mapped, return cached column name
2. `synchronized` block: re-check cache (double-checked locking)
3. Assign next slot number (`nextDimSlot++`)
4. Insert registry row with `PRIMARY KEY (table_name, column_name)`
5. `ALTER TABLE <tableName> ADD COLUMN dim_fNN <type>, ALGORITHM=INPLACE, LOCK=...`
6. Update in-memory cache

### Thread Safety

The `allocateNewDimSlot` and `allocateNewAggSlot` methods are `synchronized` on the `ColumnRegistry` instance, so slot assignment is single-threaded within a JVM.

As an additional guard, if the registry `INSERT` fails with a duplicate key error (e.g., a column was added by another process):

1. The thread rolls back the slot number (`nextDimSlot--`)
2. Reloads from the database by `dimKey`
3. Returns the found entry's column name (or re-throws if the reload finds nothing)

Slot numbers may have gaps if a slot was tentatively reserved but then rolled back. That's safe — the sequence is never relied upon for correctness.

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

Aggregation columns use `BIGINT NOT NULL DEFAULT 0` (for `value_type = INT`) or `DOUBLE NOT NULL DEFAULT 0.0` (for `value_type = FLOAT`). Aggregates are never `NULL`.

## String Width Expansion

String dimensions have a `width` (max VARCHAR length). If a later record reports a longer value:

| Old width | New width | Strategy |
|-----------|-----------|----------|
| ≤ 255 | ≤ 255 | **In-place** (`MODIFY COLUMN` with `ALGORITHM=INPLACE`) — free |
| ≤ 255 | > 255 | **Invalidate + new slot** — column crosses the 1-byte/2-byte length prefix boundary, which requires a table rebuild |
| > 255 | > 255 | **In-place** if the new width still fits in the row; in practice rare |

The 255 boundary is the InnoDB row-format threshold where MySQL changes internal length encoding.

## Online DDL Locking

Physical column additions use `ALGORITHM=INPLACE` with a database-specific `LOCK` mode:

```java
// From ColumnRegistry.addPhysicalColumn():
String lockMode = dsl.dialect() == SQLDialect.MARIADB ? "SHARED" : "NONE";
String sql = "ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " " + columnDef
           + ", ALGORITHM=INPLACE, LOCK=" + lockMode;
```

### MariaDB 10.4+: `LOCK=SHARED`

- Concurrent reads (SELECT)
- Blocks concurrent writes (INSERT, UPDATE) for the duration of the ALTER

**Why not `LOCK=NONE`?** MariaDB cannot use `LOCK=NONE` on tables with indexed virtual columns (which `clp_spark` has for the `clp_ir_path_hash` and `clp_archive_path_hash` hash-index columns). Attempting it yields:
```
LOCK=NONE is not supported. Reason: online rebuild with indexed virtual columns. Try LOCK=SHARED
```

**Impact:** New columns are added infrequently (only on first encounter of a new field). HikariCP queues blocked inserts automatically. In steady-state operation after the initial schema is learned, schema changes are rare.

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

The `DynamicIndexManager` class (in `metastore/schema/DynamicIndexManager.java`) reconciles index configuration at startup and on config reload:

- **Creates** indexes that are enabled in config but missing from the table
- **Drops** indexes that are disabled in config (if they exist and aren't protected)
- **Skips** columns that haven't been allocated yet (logs a warning)

Index DDL also uses `ALGORITHM=INPLACE, LOCK=NONE` with a fallback to default DDL if not supported.

### Protected Indexes (Never Dropped)

| Index | Purpose |
|-------|---------|
| `PRIMARY` | Primary key |
| `idx_consolidation` | Core index for pending file queries |
| `idx_expiration` | Core index for retention/deletion queries |

## Demo

The Kafka ingestion benchmark exercises automatic schema evolution end-to-end. It starts MariaDB and Kafka, runs the coordinator, and produces records that introduce new dimensions and aggregates — triggering live `ALTER TABLE ADD COLUMN` operations:

```bash
# From the metalog directory
integration-tests/benchmarks/kafka-ingestion/run.py -r 10000

# Try proto-structured mode to exercise structured DimEntry/AggEntry evolution
integration-tests/benchmarks/kafka-ingestion/run.py -r 10000 --mode proto-structured
```

Prerequisites: Docker, JDK 17+, Maven 3.9+.

## Startup Behaviour

At coordinator startup, `ColumnRegistry` loads all `ACTIVE` entries for the table from `_dim_registry` and `_agg_registry` into in-memory caches. Subsequent `resolveOrAllocateDim()` / `resolveOrAllocateAgg()` calls check the cache first (no DB round-trip for known fields).

Log output on startup:
```
ColumnRegistry loaded for table=clp_spark: 12 dim entries, 5 agg entries
```

## See Also

- [Metadata Tables](../reference/metadata-tables.md) — Full `clp_spark` schema including base and dynamic columns
- [Naming Conventions](../reference/naming-conventions.md) — Column naming patterns
- [Architecture Overview](../concepts/overview.md) — Writer thread triggers schema evolution
- [Performance Tuning](../operations/performance-tuning.md) — Index overhead from dynamic columns
