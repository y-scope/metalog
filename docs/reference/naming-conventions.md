# Naming Conventions

[← Back to docs](../README.md)

Consistent naming conventions for schema, columns, lifecycle states, and files.

---

## Column Naming

Metadata tables use a two-layer naming scheme: **opaque physical column names** in the database, and **logical field names** in registry tables. Physical names are internal — the public API always works with logical names.

### Dimension Columns (`dim_fNN`)

Dimension columns hold per-file field values used for split pruning. Physical column names are assigned sequentially as new fields appear during ingestion:

| Format | Range | Examples |
|--------|-------|---------|
| `dim_fNN` | `dim_f01`–`dim_f999` | `dim_f01`, `dim_f12` |

The **`_dim_registry`** table maps each physical name to its logical metadata:

| Field | Description | Examples |
|-------|-------------|---------|
| `dim_key` | Field name from ingested records (any characters allowed) | `k8s.pod.name`, `@timestamp`, `service/region` |
| `base_type` | SQL type family | `str`, `str_utf8`, `bool`, `int`, `float` |
| `width` | Max VARCHAR length for `str`/`str_utf8` types | `128`, `256` |

**Base types:**

| `base_type` | SQL Type |
|-------------|----------|
| `str` | `VARCHAR(width) CHARACTER SET ascii COLLATE ascii_bin NULL` |
| `str_utf8` | `VARCHAR(width) NULL` |
| `bool` | `TINYINT(1) NULL` |
| `int` | `BIGINT NULL` |
| `float` | `DOUBLE NULL` |

**Wire format (transformer API):**

Record transformers write dimension values using **slash-delimited self-describing keys**: `dim/{typeSpec}/{fieldName}`. `BatchingWriter` parses the key to derive `dim_key`, `base_type`, and `width`, then calls `ColumnRegistry` to resolve or allocate the physical `dim_fNN` slot.

| Wire key format | `base_type` | `width` | Example |
|-----------------|-------------|---------|---------|
| `dim/str{N}/{field}` | `str` | N | `dim/str128/service_name` |
| `dim/str{N}utf8/{field}` | `str_utf8` | N | `dim/str64utf8/display_name` |
| `dim/int/{field}` | `int` | — | `dim/int/error_code` |
| `dim/float/{field}` | `float` | — | `dim/float/cpu_usage` |
| `dim/bool/{field}` | `bool` | — | `dim/bool/is_cloud` |

### Aggregation Columns (`agg_fNN`)

Aggregation columns hold per-file pre-computed aggregates (e.g., match counts) used for early termination. Physical column names follow the same sequential pattern:

| Format | Range | Examples |
|--------|-------|---------|
| `agg_fNN` | `agg_f01`–`agg_f999` | `agg_f01`, `agg_f07` |

The **`_agg_registry`** table maps each physical name to its logical metadata:

| Field | Description | Examples |
|-------|-------------|---------|
| `agg_key` | Field being aggregated | `level`, `status_code` |
| `agg_value` | Specific value (`NULL` = total) | `error`, `warn`, `500`, `NULL` |
| `aggregation_type` | EQ, GTE, GT, LTE, LT, SUM, AVG, MIN, MAX | `EQ`, `GTE` |
| `value_type` | Physical column type | `INT`, `FLOAT` |
| `alias_column` | If set, aliases an existing column | `record_count`, `NULL` |

Aggregation columns use `BIGINT NOT NULL DEFAULT 0` (INT) or `DOUBLE NOT NULL DEFAULT 0.0` (FLOAT).

**Wire format (transformer API):**

Record transformers write aggregation values using slash-delimited self-describing keys. `BatchingWriter` parses the key to derive `agg_key`, `agg_value`, and `aggregation_type`, then calls `ColumnRegistry` to resolve or allocate the physical `agg_fNN` slot.

Integer aggregations — counts and thresholds (`EQ`, `GTE`, `GT`, `LTE`, `LT`):

| Wire key format | `agg_key` | `agg_value` | `aggregation_type` | Example |
|-----------------|-----------|-------------|-------------------|---------|
| `agg_int/{type}/{field}` | `{field}` | `NULL` | `{TYPE}` | `agg_int/eq/record_count` |
| `agg_int/{type}/{field}/{qualifier}` | `{field}` | `{qualifier}` | `{TYPE}` | `agg_int/gte/level/warn` |

Float aggregations — metrics (`SUM`, `AVG`, `MIN`, `MAX`):

| Wire key format | `agg_key` | `agg_value` | `aggregation_type` | Example |
|-----------------|-----------|-------------|-------------------|---------|
| `agg_float/{type}/{field}` | `{field}` | `NULL` | `{TYPE}` | `agg_float/avg/latency` |
| `agg_float/{type}/{field}/{qualifier}` | `{field}` | `{qualifier}` | `{TYPE}` | `agg_float/sum/cpu/usage` |

The `{type}` component in the wire key is lowercase (e.g., `eq`, `gte`); `BatchingWriter` uppercases it when looking up the `AggregationType` enum.

See [Schema Evolution](../guides/evolve-schema.md) for column lifecycle (ACTIVE → INVALIDATED → AVAILABLE), online DDL details, and the slot allocation algorithm.

---

## Lifecycle State Naming

Pattern: `{FORMAT}_{STATE}` in SCREAMING_SNAKE_CASE

| Component | Values | Description |
|-----------|--------|-------------|
| `{FORMAT}` | `IR`, `ARCHIVE`, `IR_ARCHIVE` | File format(s) involved |
| `{STATE}` | `BUFFERING`, `CLOSED`, `CONSOLIDATION_PENDING`, `PURGING` | Current lifecycle state (task-queue states like `PROCESSING`, `FAILED` are separate) |

**State Reference (7 states):**

| State | Lifecycle | Description |
|-------|-----------|-------------|
| `IR_BUFFERING` | IR-only | IR file actively receiving writes |
| `IR_CLOSED` | IR-only | IR file closed, queryable, ready for purging |
| `IR_PURGING` | IR-only | IR file being deleted from storage |
| `ARCHIVE_CLOSED` | Archive-only or IR+Archive | Archive ready (from batch upload or after consolidation) |
| `ARCHIVE_PURGING` | Archive-only or IR+Archive | Archive being deleted from storage |
| `IR_ARCHIVE_BUFFERING` | IR+Archive | IR file actively receiving writes, will be consolidated |
| `IR_ARCHIVE_CONSOLIDATION_PENDING` | IR+Archive | IR file closed, awaiting consolidation into archive |

**Lifecycle Flows:**
- **IR-only:** `IR_BUFFERING` → `IR_CLOSED` → `IR_PURGING` → deleted
- **Archive-only:** Created directly as `ARCHIVE_CLOSED` → `ARCHIVE_PURGING` → deleted
- **IR+Archive (hybrid):** `IR_ARCHIVE_BUFFERING` → `IR_ARCHIVE_CONSOLIDATION_PENDING` → `ARCHIVE_CLOSED` → `ARCHIVE_PURGING` → deleted

See [Glossary: File Lifecycle](../concepts/glossary.md#file-lifecycle) for definitions.

---

## File Naming

### Documentation Files

Pattern: `{topic}.md` or `{category}-{topic}.md`

| Pattern | Use Case | Examples |
|---------|----------|----------|
| `{topic}.md` | Single-topic docs | `coordinator-internals.md`, `README.md` |
| `{category}-{topic}.md` | Category grouping | `coordinator-ha.md`, `task-queue.md` |

**Conventions:**
- Use hyphens (`-`) to separate words
- Use lowercase
- No version numbers in filenames
- No dates in filenames

### Code Files

Follow language-specific conventions:
- Java: `PascalCase.java` (e.g., `ColumnRegistry.java`, `TaskQueue.java`)
- Python: `snake_case.py`
- SQL: `snake_case.sql`

---

## Index Naming

Pattern: `idx_{purpose}`

All index names begin with `idx_` followed by a short description of the index's purpose.

**Base indexes on the metadata table:**

| Index Name | Columns | Purpose |
|------------|---------|---------|
| `idx_id` | `(id)` | AUTO_INCREMENT support on partitioned table |
| `idx_clp_ir_hash` | `(clp_ir_path_hash, min_timestamp)` | IR path uniqueness |
| `idx_clp_archive_hash` | `(clp_archive_path_hash)` | Archive path lookup |
| `idx_consolidation` | `(state, min_timestamp)` | Planner: pending files, oldest first |
| `idx_expiration` | `(expires_at)` | Retention: expired files |
| `idx_max_timestamp` | `(max_timestamp)` | Queries: time-range overlap |

Dimension indexes added by `DynamicIndexManager` follow the same `idx_` prefix convention (e.g., `idx_dim_f01` for a dimension column index).

---

## Adding New Dimensions

### When to Add a Dimension Column

Add a `dim_*` column when **all three criteria** are met:

| Criteria | Threshold | Rationale |
|----------|-----------|-----------|
| **Query frequency** | >50% of queries filter on this field | Worth the storage overhead |
| **Selectivity** | Field value **constant within each file** — every record has the same value | Required for file-level elimination; fields that vary within a file cannot be dimension columns |
| **Cardinality** | <10,000 distinct values | High cardinality → use sketches instead |

**Decision tree:**
- High-cardinality UUID (user_id, trace_id) → use `sketches` for probabilistic pruning
- Per-file constant (service, region, host) → use `dim_*` column
- Counts of specific values (errors, warnings) → use `agg_*` column

### Procedure

Dimension and aggregation columns are **auto-discovered** — no manual DDL is needed. When a new field appears in ingested records, `ColumnRegistry` allocates the next available slot and runs online DDL to add the physical column.

1. **Include the field in record metadata** — send it in the `IngestBatch` gRPC request or Kafka message. The coordinator discovers it on first ingestion and allocates a `dim_fNN` slot automatically.

2. **Verify discovery** — check the registry to confirm the field was picked up with the expected type and width:
   ```sql
   SELECT column_name, base_type, width, dim_key, status
   FROM _dim_registry WHERE table_name = 'clp_spark';
   ```

3. **Add aggregate tracking if needed:** For low-cardinality enumerated values (e.g., log levels, HTTP status codes), include aggregate fields in the ingested record. The registry maps them:
   ```
   agg_key=level, agg_value=error, aggregation_type=EQ   → agg_fNN  (exact ERROR count)
   agg_key=level, agg_value=warn, aggregation_type=GTE    → agg_fNN  (WARN + ERROR + FATAL)
   ```

4. **Add an index if frequently filtered:** Configure it in the table settings or create manually:
   ```sql
   CREATE INDEX idx_dim_f01 ON clp_spark (min_timestamp, dim_f01)
   ALGORITHM=INPLACE, LOCK=NONE;
   ```
   `DynamicIndexManager` reconciles configured indexes at startup and on config reload.

See [Schema Evolution](../guides/evolve-schema.md) for DDL procedures, the 255-byte width expansion rule, and MariaDB vs MySQL locking differences.

---

## Quick Reference

| Element | Pattern | Example |
|---------|---------|---------|
| Dimension column (physical) | `dim_fNN` | `dim_f01`, `dim_f12` |
| Aggregation column (physical) | `agg_fNN` | `agg_f01`, `agg_f07` |
| Dimension logical name | `dim_key` in `_dim_registry` | `k8s.pod.name`, `@timestamp` |
| Aggregation logical name | `agg_key` + `agg_value` (+ `aggregation_type`) in `_agg_registry` | `level` + `error` |
| Lifecycle state | `{FORMAT}_{STATE}` | `ARCHIVE_CLOSED` |
| Index | `idx_{purpose}` | `idx_consolidation`, `idx_dim_f01` |
| Doc file | `{topic}.md` | `coordinator-internals.md` |

## See Also

- [Metadata Tables Reference](metadata-tables.md) — Full schema definition and column reference
- [Glossary](../concepts/glossary.md) — CLP terminology and data format definitions
- [Schema Evolution](../guides/evolve-schema.md) — Online DDL for dynamic columns
