# Record Transformers

[← Back to docs](../README.md)

Record transformers convert raw Kafka message payloads into structured metadata records. They enable data sources with different schemas to be ingested without modifying the upstream producer. The gRPC ingestion path uses protobuf and maps directly to `FileRecord` without transformation.

## Why Transformers?

Different data collectors may produce different JSON schemas:

| Source | Path Field | Timestamp Field | Unit |
|--------|------------|-----------------|------|
| Standard FileRecord | `ir_path` | `min_timestamp` | seconds |
| Legacy (Spark, Athena, etc.) | `tpath` | `creationTime` | milliseconds |

Transformers normalize these differences at ingestion time.

## Architecture

There are two transformer layers in the ingestion pipeline:

```
Kafka Message (bytes)
  → MessageTransformer (kafka package)     — bytes → proto MetadataRecord
  → RecordTransformer  (ingestion package) — FileRecord + data → populated FileRecord
  → Validation → Database
```

### MessageTransformer (Kafka Layer)

Defined in `internal/kafka/consumer.go`. Converts raw Kafka message bytes into a protobuf `MetadataRecord`.

```go
type MessageTransformer interface {
    Transform(payload []byte) (*pb.MetadataRecord, error)
}
```

The built-in `AutoDetectTransformer` auto-detects JSON vs protobuf:
- JSON payloads (starting with `{`) are parsed as JSON
- Everything else is treated as protobuf

### RecordTransformer (Ingestion Layer)

Defined in `internal/coordinator/ingestion/transformer.go`. Transforms structured key-value data into `FileRecord` dimension and aggregation fields.

```go
type RecordTransformer interface {
    Transform(rec *metastore.FileRecord, data map[string]string) error
}
```

## Built-in Transformers

| Name | Type | Description |
|------|------|-------------|
| `default` | `RecordTransformer` | Maps all key-value data entries directly to dimensions |
| `json` | `RecordTransformer` | Parses a JSON payload field and flattens nested keys into dimensions |
| (auto-detect) | `MessageTransformer` | Auto-detects JSON or protobuf Kafka payloads |

## Configuration

Set the transformer per table in `node.yaml`:

```yaml
tables:
  - name: clp_spark
    kafka:
      topic: clp-metadata-spark
      recordTransformer: json
```

Or via the `_table_kafka` registry table:

```sql
UPDATE _table_kafka
SET record_transformer = 'json'
WHERE table_name = 'spark_logs';
```

## Creating a New Transformer

### Step 1: Implement the interface

Create a new `RecordTransformer` implementation. The `Transform` method receives a `FileRecord` (with base fields already set) and a `map[string]string` of key-value data to process:

```go
package ingestion

import (
	"github.com/y-scope/metalog/internal/metastore"
)

// sparkTransformer handles Spark-specific metadata fields.
type sparkTransformer struct{}

func (t *sparkTransformer) Transform(rec *metastore.FileRecord, data map[string]string) error {
	if rec.Dims == nil {
		rec.Dims = make(map[string]any)
	}

	// Map standard dimension keys using the slash-delimited format.
	// The ColumnRegistry translates these to physical columns (dim_f01, etc.)
	if v, ok := data["service"]; ok {
		rec.Dims["dim/str64/service"] = v
	}
	if v, ok := data["region"]; ok {
		rec.Dims["dim/str32/region"] = v
	}

	return nil
}
```

### Step 2: Register the transformer

Add it to the `TransformerRegistry` in an `init()` function:

```go
func init() {
	TransformerRegistry["spark"] = func() RecordTransformer {
		return &sparkTransformer{}
	}
}
```

### Step 3: Configure

Enable for your table in `node.yaml`:

```yaml
tables:
  - name: spark_logs
    kafka:
      recordTransformer: spark
```

## Field Mapping Reference

### Dimension Key Format

Dimension keys use the **slash-delimited self-describing format**: `dim/{typeSpec}/{fieldName}`. The `ColumnRegistry` translates each key to a physical column (`dim_f01`, `dim_f02`, ...) and stores the mapping in `_dim_registry`.

| Key Pattern | DB Type | Example |
|-------------|---------|---------|
| `dim/str{N}/{field}` | `VARCHAR(N)` ASCII | `dim/str64/service` |
| `dim/str{N}utf8/{field}` | `VARCHAR(N)` UTF-8 | `dim/str128utf8/display_name` |
| `dim/int/{field}` | `BIGINT` | `dim/int/error_code` |
| `dim/float/{field}` | `DOUBLE` | `dim/float/score` |
| `dim/bool/{field}` | `TINYINT(1)` | `dim/bool/is_cloud` |

### Aggregation Key Format

Aggregation keys use the format `agg_int/{aggType}/{field}[/{qualifier}]` (integer) or `agg_float/{aggType}/{field}[/{qualifier}]` (float). The qualifier is omitted for totals.

| Key Pattern | Use Case |
|-------------|----------|
| `agg_int/eq/{field}/{qualifier}` | Exact count of a specific value |
| `agg_int/gte/{field}/{qualifier}` | Count at or above threshold |
| `agg_float/sum/{field}` | Sum (e.g. total bytes) |
| `agg_float/avg/{field}` | Average (e.g. mean latency) |
| `agg_float/min/{field}` | Minimum value |
| `agg_float/max/{field}` | Maximum value |

**Example — log level counts:**

```go
func (t *sparkTransformer) Transform(rec *metastore.FileRecord, data map[string]string) error {
	if rec.Aggs == nil {
		rec.Aggs = make(map[string]any)
	}

	errorCount := parseInt64(data, "error_count")
	warnCount := parseInt64(data, "warn_count")

	rec.Aggs["agg_int/eq/level/error"] = errorCount
	rec.Aggs["agg_int/eq/level/warn"] = warnCount
	rec.Aggs["agg_int/gte/level/warn"] = errorCount + warnCount

	return nil
}
```

The `ColumnRegistry` maps each key to a physical `agg_fNN` column.

## Error Handling

When transformation fails:
1. The `Transform` method returns an error
2. The error is logged with the Kafka partition and offset
3. The record is dropped and processing continues with the next record

## See Also

- [Ingestion Paths](../concepts/ingestion.md) — Kafka ingestion path uses transformers
- [Metadata Tables](../reference/metadata-tables.md) — Table design that transformers map into
- [Configuration](../reference/configuration.md) — Transformer configuration in node.yaml
- [Naming Conventions](../reference/naming-conventions.md) — Column naming patterns for dimensions and counts
