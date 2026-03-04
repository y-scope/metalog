# gRPC API Reference

[← Back to docs](../README.md)

The gRPC server (default port `9090`) exposes services for querying split metadata and managing coordinator configuration. Use [grpcurl](https://github.com/fullstorydev/grpcurl) for manual testing — reflection is enabled.

**Related:** [Query Execution](../concepts/query-execution.md) · [Keyset Pagination](../design/keyset-pagination.md) · [Naming Conventions](naming-conventions.md)

---

## Architecture

```
+------------------------------------------------------------------+
|                        API Server (Vert.x)                        |
|                                                                   |
|  +--------------+    +--------------+    +--------------+         |
|  |     REST     |    |     gRPC     |    |     MCP      |         |
|  |   (future)   |    |    :9090     |    |   (future)   |         |
|  +------+-------+    +------+-------+    +------+-------+         |
|         |                   |                   |                  |
|         +-------------------+-------------------+                  |
|                             v                                     |
|                    +----------------+                              |
|                    | Query Service  |                              |
|                    +-------+--------+                              |
|                            |                                      |
|               +------------+------------+                          |
|               v                         v                          |
|        +--------------+          +--------------+                  |
|        |    Cache     |          |     Pool     |--------+         |
|        |  (Caffeine)  |          |  (HikariCP)  |        |         |
|        +--------------+          +--------------+        |         |
|                                                          |         |
+----------------------------------------------------------+---------+
                                                           v
                                                  Database Replicas
```

All protocols share the same Query Service — each is a thin adapter over Vert.x.

**Consumers:** Presto-CLP connector, Spark, ML platforms, observability tools, LLM agents

---

## Services

| Service | Proto File | Package | Default Port | Purpose |
|---------|-----------|---------|:---:|---------|
| `QuerySplitsService` | `splits.proto` | `com.yscope.clp.service.query.api.proto.grpc` | 9090 | Stream split metadata with keyset pagination |
| `MetadataService` | `metadata.proto` | `com.yscope.clp.service.query.api.proto.grpc` | 9090 | Schema introspection (tables, dimensions, aggregates, sketches) |
| `MetadataIngestionService` | `ingestion.proto` | `com.yscope.clp.service.coordinator.grpc` | 9091 | Ingest metadata records via gRPC (alternative to Kafka; disabled by default) |
| `CoordinatorService` | `coordinator.proto` | `com.yscope.clp.service.coordinator.grpc` | 9090 | Runtime table registration (no restart required) |

### Proto Definitions

**`splits.proto` — split streaming with keyset pagination:**

```protobuf
service QuerySplitsService {
  // Stream split metadata from DB. Client can cancel early for early termination.
  rpc StreamSplits(StreamSplitsRequest) returns (stream StreamSplitsResponse);
}
```

**`metadata.proto` — schema introspection:**

```protobuf
service MetadataService {
  rpc ListTables(ListTablesRequest) returns (ListTablesResponse);
  rpc ListDimensions(ListDimensionsRequest) returns (ListDimensionsResponse);
  rpc ListAggs(ListAggsRequest) returns (ListAggsResponse);
  rpc ListSketches(ListSketchesRequest) returns (ListSketchesResponse);
}
```

All `MetadataService` responses use resolved logical names (e.g., `"service"`, `"host"`) — internal placeholder column names (`dim_f01`) are never exposed to clients.

**`ingestion.proto` — metadata record ingestion:**

```protobuf
service MetadataIngestionService {
  rpc Ingest(IngestRequest) returns (IngestResponse);
}
```

Each `IngestRequest` carries a `MetadataRecord` with typed `DimEntry` and `AggEntry` fields, plus a
`SelfDescribingEntry` escape hatch for producers that use the slash-delimited key format. See
[Naming Conventions](naming-conventions.md) for the key format specification.

**`coordinator.proto` — runtime table management:**

```protobuf
service CoordinatorService {
  rpc RegisterTable(RegisterTableRequest) returns (RegisterTableResponse);
}
```

---

## QuerySplitsService

### `StreamSplits`

Streams matching splits from a metadata table. Returns rows incrementally; the client can cancel
early without waiting for all results.

```
rpc StreamSplits(StreamSplitsRequest) returns (stream StreamSplitsResponse)
```

#### Request fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `table` | string | Yes | Metadata table name (e.g., `"clp_spark"`) |
| `order_by` | repeated OrderBy | Yes | Sort specification. At least one entry required. `"id"` is not allowed — it is always the implicit final tiebreaker. |
| `limit` | int32 | No | Maximum results to return. `0` (default) = unlimited. |
| `state_filter` | repeated string | No | Filter by split state (e.g., `["ARCHIVE_CLOSED"]`). Empty = no state filter. |
| `filter_expression` | string | No | SQL WHERE fragment for additional filtering. See [Filter Expressions](#filter-expressions). |
| `cursor` | KeysetCursor | No | Resume token from a previous stream. Must have the same `order_by` as the original request. |
| `include_cursor` | bool | No | If true, each response message includes a continuation `cursor`. Default false. |
| `allow_unindexed_sort` | bool | No | If true, allow sorting on non-indexed columns (causes a full table scan per page). Default false — the server rejects unindexed sort columns. |
| `projection` | repeated string | No | Columns to include in results. Empty (default) = all columns. See [Column Projection](#column-projection). |
| `stream_idle_timeout_ms` | int64 | No | Max milliseconds to wait when the client's receive buffer is full. Only triggers for completely stuck/frozen clients, not slow ones. `0` = server default (60 000 ms). |

#### Response fields

Each streamed `StreamSplitsResponse` contains:

| Field | Type | Description |
|-------|------|-------------|
| `split` | Split | The split metadata row |
| `sequence` | int32 | 1-based sequence number |
| `stats` | QueryStats | Running totals (updated periodically) |
| `done` | bool | True only on the final summary message (no `split` set) |
| `cursor` | KeysetCursor | Resume token for this row. Only present when `include_cursor=true`. |

---

## Message Types

### `OrderBy`

```protobuf
message OrderBy {
    string column = 1;  // column name or namespace-prefixed field
    Order  order  = 2;  // ASC or DESC
}
```

`column` supports namespace prefixes for unambiguous targeting (see [Filter Expressions](#filter-expressions)):

| Syntax | Example | Resolves to |
|--------|---------|-------------|
| Plain built-in | `max_timestamp`, `record_count` | Physical column as-is |
| `__FILE.xxx` | `__FILE.record_count` | Physical column as-is |
| `__DIM.xxx` | `__DIM.zone` | `dim_fXX` |
| `__AGG_TYPE.key.value` | `__AGG_GTE.level.warn` | `agg_fXX` |

`id` is not allowed in `order_by` — it is always appended implicitly as the final ASC tiebreaker.

### `KeysetCursor`

```protobuf
message KeysetCursor {
    repeated CursorValue values = 1;  // one entry per order_by field, in order
    int64                id     = 2;  // implicit tiebreaker — always ASC
}

message CursorValue {
    oneof value {
        int64  int_val   = 1;
        double float_val = 2;
        string str_val   = 3;
    }
}
```

Cursors are query-scoped: a cursor produced by one `order_by` is only valid for the same `order_by`.
`cursor.values` must have exactly as many entries as `order_by`.

### `StreamSplitsRequest`

```protobuf
message StreamSplitsRequest {
  string          table               = 1;   // e.g., "clp_spark"
  int32           limit               = 2;   // 0 = unlimited
  repeated string projection          = 4;   // column projection (empty = all)
  repeated string state_filter        = 11;  // e.g., ["IR_ARCHIVE_BUFFERING"]
  string          filter_expression   = 12;  // WHERE fragment (validated via JSqlParser)
  KeysetCursor    cursor              = 13;  // optional continuation cursor
  repeated OrderBy order_by           = 14;  // sort spec (required, ≥1 entry; "id" not allowed)
  bool            include_cursor      = 15;  // if true, each response includes a cursor
  int64           stream_idle_timeout_ms = 16;  // 0 = server default
  bool            allow_unindexed_sort   = 17;  // allow full-table-scan sorts (default false)
}
```

### `Split`

```protobuf
message Split {
    int64               id                          = 1;
    string              clp_ir_path                 = 2;
    string              clp_archive_path            = 3;   // empty if not consolidated
    int64               min_timestamp               = 4;
    int64               max_timestamp               = 5;
    string              state                       = 6;
    map<string, string> dimensions                  = 7;   // logical key → value
    repeated AggEntry   aggs                        = 8;
    int64               record_count                = 9;
    int64               size_bytes                  = 10;
    string              clp_ir_storage_backend      = 11;
    string              clp_ir_bucket               = 12;
    string              clp_archive_storage_backend = 13;
    string              clp_archive_bucket          = 14;
}
```

### `StreamSplitsResponse`

```protobuf
message StreamSplitsResponse {
  Split        split    = 1;
  int32        sequence = 2;   // 1-based
  QueryStats   stats    = 3;   // running totals
  bool         done     = 4;   // true on the final (summary) message
  KeysetCursor cursor   = 5;   // present when include_cursor=true
}

message QueryStats {
  int64 splits_scanned = 1;
  int64 splits_matched = 2;
}
```

### `AggEntry`

```protobuf
message AggEntry {
    string          key              = 1;  // e.g., "level"
    string          value            = 2;  // e.g., "warn"; empty if no value qualifier
    AggregationType aggregation_type = 3;  // EQ, GTE, GT, LTE, LT, SUM, AVG, MIN, MAX
    oneof result {
        int64  int_value   = 4;
        double float_value = 5;
    }
}
```

---

## Filter Expressions

The `filter_expression` field accepts a SQL WHERE clause fragment validated and resolved before
execution. Invalid column names return `INVALID_ARGUMENT` immediately rather than an opaque SQL error.

### Namespace prefixes

Use prefixes to target columns unambiguously:

| Prefix | Example | Resolves to |
|--------|---------|-------------|
| _(none)_ | `record_count > 0` | File → dimension → aggregate (first match) |
| `__FILE.` | `__FILE.record_count > 0` | Physical column directly |
| `__DIM.` | `__DIM.zone = 'us-east-1a'` | `dim_fXX` for that dimension key |
| `__AGG.` or `__AGG_EQ.` | `__AGG_EQ.level.error > 0` | `agg_fXX` for EQ aggregate |
| `__AGG_GTE.` | `__AGG_GTE.level.warn > 1000` | `agg_fXX` for GTE aggregate |
| `__AGG_GT.` | `__AGG_GT.level.warn > 0` | `agg_fXX` for GT aggregate |
| `__AGG_LTE.` | `__AGG_LTE.level.info < 5000` | `agg_fXX` for LTE aggregate |
| `__AGG_LT.` | `__AGG_LT.level.debug < 100` | `agg_fXX` for LT aggregate |
| `__AGG_SUM.` | `__AGG_SUM.bytes.total > 1048576` | `agg_fXX` for SUM aggregate |
| `__AGG_MIN.` | `__AGG_MIN.latency_ms < 100` | `agg_fXX` for MIN aggregate |
| `__AGG_MAX.` | `__AGG_MAX.latency_ms > 5000` | `agg_fXX` for MAX aggregate |
| `__AGG_AVG.` | `__AGG_AVG.latency_ms > 200` | `agg_fXX` for AVG aggregate |

For `__AGG_*` prefixes: the part after the prefix is `key.value` (split on the **rightmost** dot).
If there is no dot, the whole thing is `key` with no value qualifier.

### File-level filterable columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | int64 | Row primary key |
| `state` | string | Lifecycle state (e.g., `ARCHIVE_CLOSED`) |
| `min_timestamp` | int64 | Earliest log timestamp in split |
| `max_timestamp` | int64 | Latest log timestamp in split |
| `record_count` | int64 | Number of log records |
| `clp_ir_size_bytes` | int64 | IR file size in bytes |
| `clp_ir_path` | string | IR file object key |
| `clp_ir_bucket` | string | IR file bucket |
| `clp_ir_storage_backend` | string | IR file storage type |
| `clp_archive_path` | string | Archive object key |
| `clp_archive_bucket` | string | Archive bucket |
| `clp_archive_storage_backend` | string | Archive storage type |
| `expires_at` | int64 | Retention expiry (Unix seconds) |
| `retention_days` | int32 | Retention policy in days |

### Operators and syntax

Expressions are validated against a whitelist:

- Comparisons: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
- Boolean: `AND`, `OR`, `NOT`
- Range: `BETWEEN x AND y`
- Set: `IN (v1, v2, ...)`
- Null: `IS NULL`, `IS NOT NULL`
- Grouping: `(...)`

Semicolons are rejected. Unknown column names cause `INVALID_ARGUMENT`.

### Sketch filters

Use `__SKETCH.field` to filter files using probabilistic membership sketches.
Available sketch fields: query `MetadataService/ListSketches`.

| Syntax | Meaning |
|--------|---------|
| `__SKETCH.user_id = 'abc'` | Keep only files that might contain `user_id = abc` |
| `__SKETCH.trace_id IN ('a', 'b', 'c')` | Keep only files that might contain any of the trace IDs |

**Restrictions:**
- Sketch predicates must appear as top-level AND conjuncts — not inside `OR` or `NOT`.
  Violating this returns `INVALID_ARGUMENT`.
- If a file has no sketch for the requested field, the file is kept (pruning hint only).
- Sketch filtering is best-effort: the query engine always confirms results, so no false
  negatives are possible.

---

## Column Projection

The `projection` field in `StreamSplitsRequest` controls which columns are returned in each `Split`
response. An empty list (the default) returns all columns. Multiple terms are unioned together.

### Projection term syntax

| Term | Meaning |
|------|---------|
| `*` | All columns (explicit wildcard — same as omitting projection) |
| `__FILE.*` | All file-level scalar columns |
| `__FILE.<col>` | One specific file column (e.g., `__FILE.min_timestamp`) |
| `__DIM.*` | All registered dimension columns |
| `__DIM.<key>` | One specific dimension (e.g., `__DIM.service.name`) |
| `__AGG.*` | All registered aggregate columns (all types) |
| `__AGG_<TYPE>.*` | All aggregate columns of the given type |
| `__AGG_<TYPE>.<key>` | Specific aggregate without value qualifier |
| `__AGG_<TYPE>.<key>.<value>` | Specific aggregate with value qualifier |

`<TYPE>` is one of: `EQ`, `GTE`, `GT`, `LTE`, `LT`, `SUM`, `AVG`, `MIN`, `MAX`.

When a wildcard and a specific term appear for the same group (e.g., `["__DIM.*", "__DIM.zone"]`),
the wildcard silently wins — no error, result is all dims. Unrecognised terms return
`INVALID_ARGUMENT`.

Columns required for cursor extraction (`id`, sort columns) and sketch evaluation (`sketches`,
`ext`) are always included regardless of the projection.

### Examples

```bash
# Only file-level scalar columns (no dims or aggs)
grpcurl -plaintext -d '{
  "table": "clp_spark",
  "order_by": [{"column": "max_timestamp", "order": "DESC"}],
  "projection": ["__FILE.*"]
}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits

# Only timestamps
grpcurl -plaintext -d '{
  "table": "clp_spark",
  "order_by": [{"column": "max_timestamp", "order": "DESC"}],
  "projection": ["__FILE.min_timestamp", "__FILE.max_timestamp"]
}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits

# All dims plus timestamps
grpcurl -plaintext -d '{
  "table": "clp_spark",
  "order_by": [{"column": "max_timestamp", "order": "DESC"}],
  "projection": ["__FILE.min_timestamp", "__FILE.max_timestamp", "__DIM.*"]
}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits

# Only MAX aggregates
grpcurl -plaintext -d '{
  "table": "clp_spark",
  "order_by": [{"column": "max_timestamp", "order": "DESC"}],
  "projection": ["__AGG_MAX.*"]
}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits
```

---

## Indexed Columns and Sorting

Two columns have dedicated indexes supporting efficient keyset pagination with no full-table scan:

| Column | Order | Index used |
|--------|-------|------------|
| `min_timestamp` | ASC | PRIMARY KEY forward scan |
| `max_timestamp` | DESC | `idx_max_timestamp` |

All other columns — `record_count`, dimension columns (`dim_fXX`), agg columns (`agg_fXX`), etc. —
are unindexed. Sorting on them causes a full table scan per page and requires
`allow_unindexed_sort=true`. The server rejects unindexed sort columns by default to prevent
accidental scans.

---

## Streaming & Pagination

| Protocol | Approach |
|----------|----------|
| **gRPC** | Native streaming with early termination |
| **REST** (future) | Cursor-based (server remains stateless) |

**gRPC cursor (implemented):**

Set `include_cursor=true` in `StreamSplitsRequest`. Each `StreamSplitsResponse` includes a `KeysetCursor` with one typed `CursorValue` per `order_by` field plus the row's `id`. Pass the cursor back in a new `StreamSplitsRequest` (with the same `order_by`) to resume iteration from that position.

---

## Caching

The Query Service checks cache before querying the database. Invalidation is TTL-based.

| Query Type | Cache | TTL | Rationale |
|------------|-------|-----|-----------|
| Schema | `schemaCache` | 5 min | Rarely changes |
| File by ID | `fileCache` | 30 sec | May update during lifecycle |
| Query results | `queryCache` | 10 sec | Frequently changing |
| Sketch results | `sketchCache` | 60 sec | Probabilistic structures change rarely |

**Per-query override:** `cache=false` (bypass), `cache_ttl=60s` (custom TTL).

---

## Examples

### Stream first 10 splits by most recent timestamp

```bash
grpcurl -plaintext -d '{
  "table": "clp_spark",
  "limit": 10,
  "state_filter": ["ARCHIVE_CLOSED"],
  "order_by": [{"column": "max_timestamp", "order": "DESC"}]
}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits
```

### Resume from a cursor (next page)

```bash
grpcurl -plaintext -d '{
  "table": "clp_spark",
  "limit": 10,
  "state_filter": ["ARCHIVE_CLOSED"],
  "order_by": [{"column": "max_timestamp", "order": "DESC"}],
  "cursor": {"values": [{"int_val": 1704067200}], "id": 42}
}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits
```

### Filter by time range and dimension

```bash
grpcurl -plaintext -d '{
  "table": "clp_spark",
  "order_by": [{"column": "max_timestamp", "order": "DESC"}],
  "filter_expression": "min_timestamp <= 1679976000 AND max_timestamp >= 1679711330 AND __DIM.zone = '\''us-east-1a'\''"
}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits
```

### Filter by aggregate threshold

```bash
grpcurl -plaintext -d '{
  "table": "clp_spark",
  "order_by": [{"column": "max_timestamp", "order": "DESC"}],
  "filter_expression": "__AGG_GTE.level.warn > 1000"
}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits
```

### Stream from oldest to newest with cursor enabled

```bash
grpcurl -plaintext -d '{
  "table": "clp_spark",
  "limit": 100,
  "order_by": [{"column": "min_timestamp", "order": "ASC"}],
  "include_cursor": true
}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits
```

### Multi-column unindexed sort (allow_unindexed_sort required)

```bash
grpcurl -plaintext -d '{
  "table": "clp_spark",
  "limit": 10,
  "order_by": [
    {"column": "record_count", "order": "DESC"},
    {"column": "max_timestamp", "order": "ASC"}
  ],
  "allow_unindexed_sort": true
}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits
```

---

## MetadataService

Schema introspection — lists registered tables, dimensions, aggregates, and sketch fields. All
responses use resolved logical names; physical placeholder column names (`dim_f01`, `agg_f01`,
`s01`) are never exposed.

```bash
# List tables
grpcurl -plaintext -d '{}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.MetadataService/ListTables

# List dimensions for a table
grpcurl -plaintext -d '{"table": "clp_spark"}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.MetadataService/ListDimensions

# List aggregates for a table
grpcurl -plaintext -d '{"table": "clp_spark"}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.MetadataService/ListAggs

# List sketch fields for a table
grpcurl -plaintext -d '{"table": "clp_spark"}' localhost:9090 \
  com.yscope.clp.service.query.api.proto.grpc.MetadataService/ListSketches
```

---

## Future API Categories

> **Note:** The query APIs (`QuerySplitsService`, `MetadataService`), the ingestion API (`MetadataIngestionService`), and the coordinator management API (`CoordinatorService`) are all implemented. The categories below describe higher-level planned APIs built on top of these primitives.

| Category | Primary Use Case |
|----------|------------------|
| **Analytics** (future) | Dashboards, cost attribution |
| **Search** (future) | Find files by trace ID, known patterns |
| **Semantic** (future) | Explore structure, find similar patterns |
| **MCP** (future) | LLM integration via natural language |

---

## CoordinatorService

Runtime table registration — write all registry rows and provision the physical table without
editing `node.yaml` or restarting the node. All operations are fully idempotent.

### `RegisterTable`

```
rpc RegisterTable(RegisterTableRequest) returns (RegisterTableResponse)
```

#### Request fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `table_name` | string | Yes | Metadata table name (must be a valid SQL identifier) |
| `display_name` | string | No | Human-friendly label. Defaults to `table_name`. |
| `kafka` | KafkaConfig | Yes | Kafka routing configuration |
| `kafka_poller_enabled` | optional bool | No | Enable/disable Kafka poller thread. Default: `true` when `kafka` is provided. |
| `consolidation_enabled` | optional bool | No | Enable/disable consolidation planner. Uses DB default if omitted. |
| `schema_evolution_enabled` | optional bool | No | Enable/disable auto-DDL for new fields. Uses DB default if omitted. |
| `loop_interval_ms` | optional int32 | No | Override coordinator loop interval (ms). Uses DB default if omitted. |

**`KafkaConfig` fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `topic` | string | Yes | Kafka topic to consume from |
| `bootstrap_servers` | string | Yes | Kafka broker address(es) |
| `record_transformer` | string | No | Named transformer for ingested records. Empty = default transformer. |

#### Response fields

| Field | Type | Description |
|-------|------|-------------|
| `table_name` | string | The registered table name (echoed from request) |
| `created` | bool | `true` = newly provisioned; `false` = already existed (idempotent) |

#### Behaviour

1. Validates `table_name`, `kafka`, `kafka.topic`, `kafka.bootstrap_servers` → `INVALID_ARGUMENT` on failure.
2. Writes `_table`, `_table_kafka`, `_table_config`, `_table_assignment`, and 32 `_sketch_registry` slots (all idempotent).
3. Provisions the physical metadata table with lookahead partitions (no-op if already exists).
4. The coordinator's periodic `reconcileUnits()` loop claims the new `_table_assignment` row (with `node_id = NULL`) on its next cycle (default: 60 s).

#### Examples

```bash
# Minimal registration
grpcurl -plaintext -d '{
  "table_name": "my_spark_logs",
  "kafka": {"topic": "spark-ir", "bootstrap_servers": "kafka:29092"}
}' localhost:9090 \
  com.yscope.clp.service.coordinator.grpc.CoordinatorService/RegisterTable
# → {"tableName":"my_spark_logs","created":true}

# Second call — idempotent
# → {"tableName":"my_spark_logs","created":false}

# With optional fields
grpcurl -plaintext -d '{
  "table_name": "my_spark_logs",
  "display_name": "Spark Logs",
  "kafka": {
    "topic": "spark-ir",
    "bootstrap_servers": "kafka:29092",
    "record_transformer": "spark"
  },
  "consolidation_enabled": false,
  "schema_evolution_enabled": true,
  "loop_interval_ms": 5000
}' localhost:9090 \
  com.yscope.clp.service.coordinator.grpc.CoordinatorService/RegisterTable
```

#### Error codes

| gRPC Status | Cause |
|-------------|-------|
| `INVALID_ARGUMENT` | `table_name` blank, `kafka` absent, `kafka.topic` blank, `kafka.bootstrap_servers` blank |
| `INTERNAL` | Database error |

---

## Error Codes

| gRPC Status | Cause |
|-------------|-------|
| `INVALID_ARGUMENT` | Missing `order_by`, unknown column in filter, sort, or projection, cursor size mismatch, `"id"` in `order_by`, unindexed sort without `allow_unindexed_sort=true`, sketch predicate inside `OR`/`NOT`, missing required `RegisterTable` fields |
| `CANCELLED` | Client cancelled the stream |
| `INTERNAL` | Database error or unexpected server failure |

---

## Configuration

See [Configuration Reference: API Server Configuration](configuration.md#api-server-configuration) for all API server environment variables.

---

## Project Structure

```
src/
├── main/
│   ├── java/com/yscope/clp/service/query/
│   │   ├── api/
│   │   │   ├── ApiServerConfig.java       — env-var based config
│   │   │   └── server/vertx/
│   │   │       ├── VertxApiServer.java    — Vert.x + gRPC server lifecycle
│   │   │       └── grpc/
│   │   │           ├── GrpcServer.java
│   │   │           ├── QuerySplitsGrpcService.java   — implements QuerySplitsService
│   │   │           ├── MetadataGrpcService.java      — implements MetadataService
│   │   │           └── CoordinatorGrpcService.java   — implements CoordinatorService
│   │   └── core/
│   │       ├── QueryService.java          — schema introspection
│   │       ├── CacheService.java          — Caffeine-backed TTL cache
│   │       └── splits/
│   │           ├── SplitQueryEngine.java  — keyset-paginated split queries
│   │           └── FilterExpressionValidator.java
│   └── proto/
│       ├── splits.proto                   — QuerySplitsService + Split messages
│       ├── metadata.proto                 — MetadataService messages
│       ├── ingestion.proto                — MetadataIngestionService + record types
│       └── coordinator.proto              — CoordinatorService messages
```

---

## See Also

- [Query Execution](../concepts/query-execution.md) — Pruning pipeline and early termination
- [Keyset Pagination](../design/keyset-pagination.md) — How internal pagination works
- [Metadata Tables Reference](../reference/metadata-tables.md) — Database schema and column naming
- [Naming Conventions](naming-conventions.md) — Column, index, and namespace naming patterns
- [Configuration Reference](configuration.md) — API server configuration
