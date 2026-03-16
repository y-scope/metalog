# Sketches

[← Back to docs](../README.md)

Probabilistic data structures for accelerating queries on high-cardinality fields — fields like user IDs, trace IDs, and session IDs that have too many distinct values for dimension columns.

**Related:** [Query Execution](query-execution.md) · [Metadata Tables](../reference/metadata-tables.md) · [Naming Conventions](../reference/naming-conventions.md) · [gRPC API](../reference/grpc-api.md) · [Glossary](glossary.md)

---

## When to Use Sketches

Dimension columns (`dim_*`) require the field to be **constant within a file** — every record shares the same value. High-cardinality fields like UUIDs violate this invariant: a file might contain thousands of distinct user IDs.

| Field Type | Cardinality | Approach |
|------------|-------------|----------|
| Per-file constant (service, region) | Low (<10K) | Dimension column |
| Enumerated values (log level, status code) | Low | Aggregation column |
| User ID, trace ID, session ID | High (millions+) | Sketch |

Sketches answer a different question than dimensions. A dimension filter says "this file definitely contains only `service=auth`." A sketch filter says "this file **definitely does not** contain `user_id=abc-123`" — or says nothing (the value might be present).

---

## How It Works

### Split Block Bloom Filter (SBBF)

Each sketch is a Parquet-compatible Split Block Bloom Filter — the same algorithm used by Apache Parquet for column chunk filtering:

- **256-bit blocks** (8 x uint32 words) with standard Parquet salt constants
- **xxHash64** (seed 0) for hashing values
- Configurable capacity with automatic rebuild when the false positive rate exceeds the threshold

During ingestion, each observed value is hashed and inserted into the filter. The filter tracks approximate unique counts and rebuilds with doubled capacity when saturated.

### Storage Layout

Sketch data is stored in two columns on each metadata row:

| Column | Type | Purpose |
|--------|------|---------|
| `sketches` | SET('s01','s02',...,'s64') | Bitmask declaring which sketch slots have data |
| `ext` | MEDIUMBLOB | LZ4-compressed msgpack blob containing filter data |

The SET column is an 8-byte bitmask — cheap to evaluate. The ext blob is only read when needed.

**`_sketch_registry` table** maps SET members to logical field names:

```
table_name  | sketch_name | sketch_key | state
------------|-------------|------------|--------
clp_spark   | s01         | uuid       | ACTIVE
clp_spark   | s02         | trace_id   | ACTIVE
clp_spark   | s03         | NULL       | AVAILABLE
...
clp_spark   | s64         | NULL       | AVAILABLE
```

All 64 SET members are pre-allocated — no `ALTER TABLE` is needed when a new sketch field appears. The registry uses the same ACTIVE/INVALIDATED/AVAILABLE lifecycle as dim and agg registries.

### ext Blob Format

The ext column stores an LZ4 frame wrapping a msgpack map:

```
LZ4(
  msgpack({
    "sketches": {
      "uuid": {
        "type": "parquet_sbbf_xxhash64",
        "data": <raw block bytes>
      },
      "trace_id": {
        "type": "parquet_sbbf_xxhash64",
        "data": <raw block bytes>
      }
    }
  })
)
```

The `type` field is a format discriminator that fully identifies the algorithm, block layout, and hash function. `data` contains the raw 256-bit blocks (32 bytes each, little-endian uint32 words). `num_blocks = len(data) / 32`.

---

## Ingestion Pipeline

```
Producer (clp-ffi-go)
  → BloomFilter.Observe(value)     // insert into SBBF
  → BloomFilter.MarshalMsgpack()   // serialize {type, data}

Proto / Kafka
  → SketchEntry { sketch_key, data }   // msgpack bytes in proto

Service.IngestWithCallback()
  → extractSketches()              // populate FileRecord.Sketches

BatchingWriter.resolveAndRemapBatch()
  → ResolveOrAllocateSketches()    // "uuid" → "s03" via registry
  → encodeSketchExt()              // build SET value + LZ4 msgpack ext blob

UpsertBatch()
  → INSERT ... sketches='s01,s03', ext=<blob>
```

### Wire Formats

**Proto (gRPC):**

```protobuf
message SketchEntry {
  string sketch_key = 1;  // e.g. "uuid"
  bytes  data       = 2;  // msgpack-encoded BloomFilterSnapshot
}
```

**Kafka JSON (self-describing key):**

```json
{
  "self_describing_kv": [
    {
      "key": "sketch/parquet_sbbf_xxhash64/uuid",
      "value": "<base64 raw SBBF block bytes>"
    }
  ]
}
```

The self-describing format puts the type in the key and the raw filter bytes (not msgpack) in the value. The Kafka consumer reconstructs the msgpack snapshot from the key components.

---

## Query Pipeline

### Usage

Sketch pruning is opt-in via the `sketch_expression` field in `StreamSplitsRequest`. It uses a separate SQL WHERE fragment from `filter_expression`:

```protobuf
StreamSplitsRequest {
  filter_expression: "uuid = 'abc-123' AND min_timestamp > 1000"
  sketch_expression: "uuid = 'abc-123'"
}
```

`filter_expression` is a hard filter (rows that don't match are excluded). `sketch_expression` is a pruning hint (files whose bloom filter says "definitely not" are dropped before the caller opens them). Without `sketch_expression`, the query works normally — no pruning, same results.

**Supported syntax** (in `sketch_expression`):
- `field = 'value'` — single equality
- `field IN ('a', 'b', 'c')` — any-match
- `field1 = 'x' AND field2 = 'y'` — multiple fields (all must pass)

**Rejected** (returns `INVALID_ARGUMENT`):
- `!=`, `NOT IN`, `<`, `>`, `<=`, `>=`, `LIKE` — bloom filters can't prove absence or ranges
- `OR` — ambiguous pruning semantics
- `NOT (...)` — negation not supported

### Execution Flow

```
Request:
  filter_expression: "uuid = 'abc-123' AND min_timestamp > 1000"
  sketch_expression: "uuid = 'abc-123'"

1. Parse sketch_expression
   predicates: [{SketchKey: "uuid", Values: ["abc-123"]}]

2. Resolve sketch key via registry
   "uuid" → SET member "s03"

3. Build conditional ext projection
   IF(FIND_IN_SET('s03',sketches)>0, ext, NULL) AS `ext`
   Avoids transferring the MEDIUMBLOB for rows without the sketch.

4. Execute SQL (filter_expression applied as WHERE clause)
   SELECT ..., IF(FIND_IN_SET('s03',sketches)>0,ext,NULL) AS `ext`
   FROM table
   WHERE uuid = 'abc-123' AND min_timestamp > 1000

5. Bloom filter evaluation (per row, in query API server)
   ext is NULL  → no sketch for this field → pass through
   ext has data → decompress LZ4 → decode msgpack → load SBBF
     → hash 'abc-123' with xxHash64
     → check filter.Contains(hash)
     → false: prune (definitely not present)
     → true: pass through (might be present)

6. QueryStats
   SplitsScanned: total rows from DB
   SplitsMatched: rows after bloom filter pruning
```

### Pruning, Not Filtering

Sketches are **pruning hints, not filters**. A row without a sketch for the queried field is not excluded — it simply doesn't benefit from pruning:

- Files ingested before sketches were configured still appear in results
- Files from producers that don't send sketch data are not penalized
- Results are identical with or without `sketch_expression` — it only affects performance
- The query engine always confirms results via `filter_expression`

The `SplitsScanned` vs `SplitsMatched` gap in `QueryStats` shows the pruning benefit.

---

## Cross-Language Compatibility

The SBBF format is designed for cross-language interoperability:

| Component | Specification |
|-----------|---------------|
| Block layout | 256-bit (8 x uint32 LE), Parquet standard |
| Salt constants | Parquet SBBF salts (8 values) |
| Hash function | xxHash64, seed 0 |
| Serialization | msgpack `{type: "parquet_sbbf_xxhash64", data: <bytes>}` |
| Compression | LZ4 frame (applied at ext column level, not per-sketch) |

Any language with xxHash64 and the ability to read little-endian uint32 arrays can evaluate the filter. The `type` string is the only field needed to identify the algorithm.

---

## See Also

- [Query Execution](query-execution.md)
- [Metadata Tables Reference](../reference/metadata-tables.md)
- [Naming Conventions](../reference/naming-conventions.md)
- [gRPC API Reference](../reference/grpc-api.md)
- [Schema Evolution](../guides/evolve-schema.md)
