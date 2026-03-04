# Record Transformers

[← Back to docs](../README.md)

Record transformers convert raw Kafka message payloads into `FileRecord` objects. They enable legacy or external data sources with different schemas to be ingested without modifying the upstream producer. The gRPC ingestion path uses protobuf and maps directly to `FileRecord` without transformation.

## Why Transformers?

Different data collectors may produce different JSON schemas:

| Source | Path Field | Timestamp Field | Unit |
|--------|------------|-----------------|------|
| Standard FileRecord | `ir_path` | `min_timestamp` | seconds |
| Legacy (Spark, Athena, etc.) | `tpath` | `creationTime` | milliseconds |

Transformers normalize these differences at ingestion time.

## Architecture

```
Kafka Message (bytes) → Transformer → FileRecord → Validation → Database
```

Transformers are auto-discovered via classpath scanning using the `@TransformerName` annotation.

### Transformer Types

| Type | Implements | Use Case |
|------|------------|----------|
| **Default** | `RecordTransformer` | Auto-detects JSON or protobuf payload; parses directly as `FileRecord` |
| **Source-specific** | `RecordTransformer` | Legacy formats with optional conforming-first fast path |

### Transparent Migration

Source-specific transformers can support gradual producer migration by checking the conforming format first:

```
transform(payload)
  ├─ Try: Parse directly as FileRecord
  ├─ If valid → return directly (fast path for migrated producers)
  └─ Else → parse legacy format and map to FileRecord fields
```

This allows old and new producers to coexist on the same topic:
- **Old producers** send legacy format → legacy conversion branch runs
- **New producers** send conforming format → passes through directly

## Built-in Transformers

| Name | Class | Description |
|------|-------|-------------|
| `default` | `DefaultRecordTransformer` | Auto-detects JSON or protobuf and parses as `FileRecord` |

Additional source-specific transformers can be registered by implementing `RecordTransformer` and annotating with `@TransformerName`.

## Configuration

Set the transformer per table in `_table_kafka`:

```sql
UPDATE _table_kafka
SET record_transformer = 'spark'
WHERE table_name = 'spark_logs';
```

Or via YAML config:

```yaml
tables:
  - name: clp_spark
    kafka:
      topic: clp-metadata-spark
      recordTransformer: spark
```

## Creating a New Transformer

### Step 1: Create the class

Extend `AbstractRecordTransformer` and override `transform()`. The base class provides typed helpers for dimensions and aggregations. For legacy format support with a conforming-first fast path:

```java
package com.yscope.clp.service.coordinator.ingestion.record.transformers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yscope.clp.service.coordinator.ingestion.record.AbstractRecordTransformer;
import com.yscope.clp.service.coordinator.ingestion.record.RecordTransformer;
import com.yscope.clp.service.coordinator.ingestion.record.TransformerName;
import com.yscope.clp.service.metastore.model.FileRecord;
import com.yscope.clp.service.metastore.model.FileState;

@TransformerName("my-source")
public class MySourceRecordTransformer extends AbstractRecordTransformer {

    @Override
    public FileRecord transform(byte[] payload, ObjectMapper objectMapper)
            throws RecordTransformer.RecordTransformException {
        // Fast path: conforming producers send standard FileRecord JSON directly.
        if (payload.length > 0 && payload[0] == '{') {
            try {
                FileRecord file = objectMapper.readValue(payload, FileRecord.class);
                if (file.getIrPath() != null && !file.getIrPath().isEmpty()) {
                    return file;
                }
            } catch (JsonProcessingException ignored) {
                // Not parseable as FileRecord — fall through to legacy conversion.
            }
        }

        // Legacy conversion: map old JSON structure to FileRecord fields.
        try {
            ObjectNode root = (ObjectNode) objectMapper.readTree(payload);

            FileRecord file = new FileRecord();
            file.setIrPath(getRequiredString(root, "legacy_path"));
            file.setMinTimestamp(getRequiredLong(root, "created_ms") * 1_000_000);
            file.setMaxTimestamp(getRequiredLong(root, "updated_ms") * 1_000_000);
            file.setIrStorageBackend("tb");
            file.setIrBucket("");
            file.setState(FileState.IR_ARCHIVE_BUFFERING);

            // Dimensions: use AbstractRecordTransformer helpers — they produce
            // the slash-delimited key format (e.g. dim/str64/service) automatically.
            setStringDimension(file, root, "service", 64);
            setStringDimension(file, root, "region", 32);

            // Aggregations: integer counts with comparison semantics.
            long errorCount = getOptionalLong(root, "error_count", 0);
            long warnCount  = getOptionalLong(root, "warn_count",  0);
            setIntAgg(file, "eq",  "level", "error", errorCount);
            setIntAgg(file, "gte", "level", "warn",  errorCount + warnCount);

            return file;
        } catch (JsonProcessingException e) {
            throw new RecordTransformer.RecordTransformException("Failed to parse legacy JSON", e);
        }
    }
}
```

### Step 2: Configure

Enable for your table:

```sql
UPDATE _table_kafka SET record_transformer = 'my-source' WHERE table_name = 'my_table';
```

No additional registration needed - `RecordTransformerFactory` discovers `@TransformerName` annotated classes automatically.

## Field Mapping Reference

### Required Fields

| `FileRecord` Field | Description | Notes |
|--------------------|-------------|-------|
| `irPath` | Object key (no leading `/`) | Primary key component |
| `minTimestamp` | Start time (epoch nanoseconds) | Convert from ms: `ms * 1_000_000` |
| `maxTimestamp` | End time (epoch nanoseconds) | Convert from ms: `ms * 1_000_000` |
| `irStorageBackend` | Storage type (`tb`, `s3`) | |
| `irBucket` | Bucket name (empty for `tb`) | |
| `state` | Initial state | Usually `IR_ARCHIVE_BUFFERING` |

### Optional Fields

| `FileRecord` Field | Description |
|--------------------|-------------|
| `irSizeBytes` | File size in bytes |
| `retentionDays` | Days to retain |
| `expiresAt` | Expiration timestamp (epoch nanoseconds) |

### Dimension Key Format

Dimension keys use the **slash-delimited self-describing format**: `dim/{typeSpec}/{fieldName}`. The `ColumnRegistry` translates each key to an opaque physical placeholder column (`dim_f01`, `dim_f02`, ...) and stores the mapping in `_dim_registry`. Keys that do not match a supported format are dropped with a warning.

Use the `AbstractRecordTransformer` helpers — they construct the correct key automatically:

| Helper | Key produced | DB type | Example |
|--------|-------------|---------|---------|
| `setStringDimension(file, root, field, N)` | `dim/str{N}/{field}` | `VARCHAR(N)` ASCII | `dim/str64/service` |
| `setStringDimensionUtf8(file, root, field, N)` | `dim/str{N}utf8/{field}` | `VARCHAR(N)` UTF-8 | `dim/str128utf8/display_name` |
| `setIntDimension(file, root, field)` | `dim/int/{field}` | `BIGINT` | `dim/int/error_code` |
| `setFloatDimension(file, root, field)` | `dim/float/{field}` | `DOUBLE` | `dim/float/score` |
| `setBoolDimension(file, root, field)` | `dim/bool/{field}` | `TINYINT(1)` | `dim/bool/is_cloud` |

Field names may contain special characters (`.`, `@`, `-`). Each helper also supports **migration from plain field names**: if the slash-delimited key is absent from the JSON, it falls back to looking up the bare field name and stores the value under the slash-delimited key.

### Aggregation Key Format

Aggregation keys use the format `agg_int/{aggType}/{field}[/{qualifier}]` (integer counts) or `agg_float/{aggType}/{field}[/{qualifier}]` (float metrics). The qualifier is omitted for totals.

Use the `AbstractRecordTransformer` helpers:

| Helper | Key produced | Use case |
|--------|-------------|----------|
| `setIntAgg(file, "eq", field, null, v)` | `agg_int/eq/{field}` | Exact count (e.g. total errors) |
| `setIntAgg(file, "eq", field, qualifier, v)` | `agg_int/eq/{field}/{qualifier}` | Exact count of a specific value |
| `setIntAgg(file, "gte", field, qualifier, v)` | `agg_int/gte/{field}/{qualifier}` | Count at or above threshold |
| `setFloatAgg(file, "sum", field, null, v)` | `agg_float/sum/{field}` | Sum (e.g. total bytes) |
| `setFloatAgg(file, "avg", field, null, v)` | `agg_float/avg/{field}` | Average (e.g. mean latency) |
| `setFloatAgg(file, "min", field, null, v)` | `agg_float/min/{field}` | Minimum value |
| `setFloatAgg(file, "max", field, null, v)` | `agg_float/max/{field}` | Maximum value |

**Example — log level counts:**

```java
long errorCount = getOptionalLong(root, "error_count", 0);
long warnCount  = getOptionalLong(root, "warn_count",  0);
long infoCount  = getOptionalLong(root, "info_count",  0);

setIntAgg(file, "eq",  "level", "error", errorCount);
setIntAgg(file, "eq",  "level", "warn",  warnCount);
// gte counts include the named level and everything above it
setIntAgg(file, "gte", "level", "warn",  errorCount + warnCount);
setIntAgg(file, "gte", "level", "info",  errorCount + warnCount + infoCount);
```

The `ColumnRegistry` maps each key to an `agg_fNN` physical column with the aggregation type, field, and qualifier stored in `_agg_registry`.

## Error Handling

When transformation fails:
1. `RecordTransformer.RecordTransformException` is thrown
2. Error logged with partition/offset
3. Record dropped, counter incremented
4. Processing continues with next record

Monitor via `MetadataConsumer.getRecordsDroppedParsing()`.

## Validation

After transformation, `RecordStateValidator` checks:
- Required fields present for the declared state
- Timestamps valid (min ≤ max, not in future)
- Count hierarchy valid (debug ≥ info ≥ warn ≥ error ≥ fatal)
- Dimension values within type bounds

Invalid records are dropped with an error log.

## See Also

- [Ingestion Paths](../concepts/ingestion.md) — Kafka ingestion path uses transformers
- [Metadata Tables](../reference/metadata-tables.md) — Table design that transformers map into
- [Configuration](../reference/configuration.md) — Transformer configuration in node.yaml
- [Naming Conventions](../reference/naming-conventions.md) — Column naming patterns for dimensions and counts
