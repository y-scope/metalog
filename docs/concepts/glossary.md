# Glossary

[← Back to docs](../README.md)

Quick reference for CLP platform terminology. Terms are grouped by category for easier navigation.

---

## Data Formats

### Archive
Columnar format that provides superior compression via cross-file deduplication. An archive contains internal files with metadata preserved. Created by consolidation workers (from IR files) or ingested directly without a source IR file (archive-only entry type). See [Data Model](../concepts/data-model.md) and [Consolidation](../concepts/consolidation.md).

### CLP-IR (Intermediate Representation)
Streaming, row-based compression format. Pre-parsed, compact binary that enables search without full decompression. Created at the edge during log ingestion. See [Data Model](../concepts/data-model.md) and [Ingestion Paths](../concepts/ingestion.md).

### Log Type
Static pattern (template) extracted from log messages, with variable values separated. Example: `"User <var> logged in from <var>"` is a log type that matches thousands of login messages. Log types enable semantic grouping without embeddings. See [Semantic Extraction](../concepts/semantic-extraction.md).

### Merged Parse Tree (MPT)
Merged Parse Tree that represents the discovered schema. Stores all unique schema structures, knowing all keys, hierarchy, nesting, and types without reading actual content. Field names, types, and nesting are learned from data, not declared upfront. MPTs enable schema-less ingestion by evolving as new patterns are discovered. See [Semantic Extraction](../concepts/semantic-extraction.md).

### Encoded Record Table (ERT)
Columnar storage unit within a CLP Archive. Records with identical schemas are grouped into ERTs, enabling efficient columnar compression and query execution. Each ERT contains only the columns relevant to its schema subset. See [Semantic Extraction](../concepts/semantic-extraction.md).

### Variable Dictionary
Deduplicated storage of variable values extracted from logs. Enables compression by storing each unique value once and referencing by ID. See [Semantic Extraction](../concepts/semantic-extraction.md).

---

## Schema Columns

### Dimension Column (`dim_fNN`)
Metadata field extracted from logs for split pruning and filtering. Physical column names are opaque placeholders (`dim_f01`, `dim_f12`, ...) assigned by `ColumnRegistry`. The logical field name and type are stored in `_dim_registry`. See [Naming Conventions](../reference/naming-conventions.md) and [Schema Evolution](../guides/evolve-schema.md).

### Aggregation Column (`agg_fNN`)
Pre-computed aggregate stored at the file level. Supports threshold comparisons (EQ, GTE, GT, LTE, LT) for integer counts and summary statistics (SUM, AVG, MIN, MAX) for float values. Physical column names are opaque placeholders (`agg_f01`, `agg_f07`, ...) assigned by `ColumnRegistry`. The logical mapping (field, qualifier, aggregation type, value type) is stored in `_agg_registry`. Enables early termination and timeline visualization without scanning file contents. See [Naming Conventions](../reference/naming-conventions.md) and [Schema Evolution](../guides/evolve-schema.md).

### Hash Column (`*_hash`)
Virtual column storing MD5 hash of a path. `clp_ir_path_hash` has a UNIQUE index (each IR path appears exactly once); `clp_archive_path_hash` has a non-unique index (one archive contains many IR files). Enables O(1) point lookups without indexing long path strings directly. See [Metadata Tables](../reference/metadata-tables.md).

### Sketch Column (`sketches` / `ext`)
Probabilistic data structure stored per file. The `sketches` set-column and `ext` blob store Bloom filters or Cuckoo filters that answer "definitely not" vs. "maybe" membership queries for high-cardinality values. Used in split pruning to eliminate files before opening them. See [Metadata Tables](../reference/metadata-tables.md).

---

## System Components

### clp-connector
Query connector that provides transparent access to CLP archives. Enables standard SQL queries against compressed log data. Originally built for Presto; may support additional query engines in the future.

### log-surgeon
CLP's extraction engine. Uses variable-first extraction (not regex templates) for automated semantic extraction and labeling of structured data from unstructured logs. See [Semantic Extraction](../concepts/semantic-extraction.md).

### Coordinator
Service that manages the CLP metadata catalog. Handles ingestion, lifecycle state transitions, and retention enforcement. At most one coordinator owns each table at any time; ownership is backed by a database lease. See [Architecture Overview](../concepts/overview.md).

### BatchingWriter
Coordinator component that accumulates ingested records and batch-UPSERTs them to the metadata table. Decouples ingestion throughput from database write rate by coalescing many individual records into a single INSERT ... ON DUPLICATE KEY UPDATE per batch interval. See [Ingestion Paths](../concepts/ingestion.md).

### Planner
Coordinator component that runs on a per-table schedule. Groups `IR_ARCHIVE_CONSOLIDATION_PENDING` files into consolidation task batches and inserts them into `_task_queue` for workers to claim. See [Consolidation](../concepts/consolidation.md).

### Consolidation Workers
Stateless workers that consolidate IR files into Archives. Claim tasks via `SELECT ... FOR UPDATE` + `UPDATE` (READ COMMITTED isolation), download IR files, run `clp-s c --single-file-archive`, upload the result, and mark the task complete. See [CLP Integration](../guides/integrate-clp.md).

### ColumnRegistry
Service component that manages the mapping between opaque physical column names (`dim_fNN`, `agg_fNN`) and logical field names. Auto-discovers new fields from ingested records, allocates slots sequentially, and runs online DDL to add physical columns. State is loaded from `_dim_registry` and `_agg_registry` at startup. See [Schema Evolution](../guides/evolve-schema.md).

### DynamicIndexManager
Component that reconciles index configuration at startup and on config reload. Creates indexes enabled in config, drops indexes disabled in config (except protected base indexes: `PRIMARY`, `idx_consolidation`, `idx_expiration`). Uses `ALGORITHM=INPLACE, LOCK=NONE` for online DDL with an automatic fallback to default DDL if the engine does not support it. See [Schema Evolution](../guides/evolve-schema.md) and [Naming Conventions](../reference/naming-conventions.md).

### clp-s
The CLP compression binary. Accepts IR files as input, compresses them into a columnar archive using semantic compression. Invoked by consolidation workers as: `clp-s c --single-file-archive <outputDir> <stagingDir>`. See [CLP Integration](../guides/integrate-clp.md).

---

## File Lifecycle

### States
Files progress through lifecycle states. See [Naming Conventions](../reference/naming-conventions.md) for the full state reference and lifecycle flows.

| State | Description |
|-------|-------------|
| `IR_BUFFERING` | IR file actively receiving data (IR-only lifecycle) |
| `IR_CLOSED` | IR file closed, ready for purging (IR-only lifecycle) |
| `IR_PURGING` | IR file being deleted from storage (IR-only lifecycle) |
| `ARCHIVE_CLOSED` | Archive file closed (archive-only, or consolidated IR+Archive) |
| `ARCHIVE_PURGING` | Archive file being deleted from storage |
| `IR_ARCHIVE_BUFFERING` | IR file actively receiving data, will be consolidated |
| `IR_ARCHIVE_CONSOLIDATION_PENDING` | IR file closed, awaiting consolidation into archive |

### Retention
How long data is kept before expiration. Set at ingestion time via `retention_days`. The `expires_at` timestamp is computed as `min_timestamp + (retention_days * 86400)`. See [Metadata Schema](../concepts/metadata-schema.md).

### FOR UPDATE (task claiming)
`SELECT ... FOR UPDATE` + `UPDATE` pattern used by the `Prefetcher` to batch-claim tasks from `_task_queue`. The `SELECT ... FOR UPDATE` pre-locks matching rows and returns their payload; a subsequent `UPDATE` transitions them to `processing`. The prefetcher uses READ COMMITTED isolation so that a locked row (already claimed by another node) is skipped and the prefetcher advances to the next pending task, providing correct fan-out across nodes. Worker goroutines then receive from the prefetcher's buffered channel instead of hitting the database directly. See [Task Queue](../concepts/task-queue.md).

### UPSERT
INSERT ... ON DUPLICATE KEY UPDATE: inserts a new row or updates the existing one if the primary key conflicts. Used by `BatchingWriter` to ingest metadata idempotently — re-sending the same file metadata is safe. See [Metadata Schema](../concepts/metadata-schema.md).

### Online DDL
Schema change that does not require a full table rebuild or exclusive lock. Used by `ColumnRegistry` to add new `dim_fNN` and `agg_fNN` columns (`ALGORITHM=INPLACE, LOCK=NONE`). If the engine does not support `LOCK=NONE`, the operation falls back to default DDL. See [Schema Evolution](../guides/evolve-schema.md).

---

## Query

### Split
The fundamental unit returned by the query API. One split corresponds to one metadata row — it contains the file's location (bucket + path), time range (`min_timestamp`, `max_timestamp`), record count, size, lifecycle state, and resolved dimension/aggregate values. Consumers use splits to locate the actual log data in object storage. See [gRPC API Reference](../reference/grpc-api.md).

### KeysetCursor
Pagination token used in `StreamSplits` RPC responses. Contains the values of the `ORDER BY` columns for a row (e.g., `max_timestamp`) plus a row `id` as a final tiebreaker, together uniquely identifying the row's position in the sort order. Clients pass a cursor back in a new request to resume streaming from that point. Only included in responses when the request sets `include_cursor = true`. See [Keyset Pagination](../design/keyset-pagination.md).

### SplitQueryEngine
Server-side component that executes `StreamSplits` queries against the metadata database using keyset pagination. Fetches fixed-size pages, applies filter expressions, and feeds results into the prefetch queue. See [Keyset Pagination](../design/keyset-pagination.md).

### Prefetch Queue
Bounded channel (`chan SplitWithCursor`) used in `SplitQueryEngine.StreamSplitsAsync()`. A background goroutine fetches DB pages and pushes splits (each paired with its keyset cursor) into the channel; the consumer callback drains the channel and sends results to the client. Overlaps DB fetch latency with network send time, and releases DB connections between pages. The gRPC service layer (`QuerySplitsGrpcService`) uses this via a consumer callback for streaming responses. See [gRPC API Reference](../reference/grpc-api.md).

---

## Query Optimization

### Split Pruning
Eliminating splits (files) from a query's candidate set before opening them. A file is pruned when its metadata (time bounds, dimension values, aggregation columns, sketches) guarantees it cannot contain matching records. Contrast with [Early Termination](#early-termination), which stops scanning after enough results are found rather than before files are opened. See [Query Execution](../concepts/query-execution.md).

### Early Termination
Query optimization that stops scanning when enough results are found. Uses file-level metadata (aggregation columns, timestamps) to skip files that cannot contribute to results. For `ORDER BY ... LIMIT N` queries, a **watermark** tracks the oldest timestamp among the top-N results accumulated so far — any file whose `max_timestamp` is older than the watermark is skipped entirely. See [Query Execution](../concepts/query-execution.md) and [Early Termination Design](../design/early-termination.md).

### Watermark
In early termination, the timestamp of the least-recent result in the current top-N accumulator. As newer files are scanned and better results accumulate, the watermark advances. Once a file's `max_timestamp` falls below the watermark, it can be skipped without scanning. See [Early Termination](../design/early-termination.md).

### Functional Index
Index on a computed (virtual) column. Example: index on `UNHEX(MD5(clp_ir_path))` enables O(1) lookups by path without storing the path in the index. See [Query Execution](../concepts/query-execution.md).

### Composite Index
Multi-column index for queries that filter on multiple dimensions together. Example: `(min_timestamp, dim_f01)` for time + dimension queries, where `dim_f01` is the physical column for a registered dimension field. See [Query Execution](../concepts/query-execution.md).

---

## Compression Concepts

### Compression Ratio
Ratio of original size to compressed size. CLP achieves ~32x on well-tuned logging, up to 169x on verbose/burst logging. CLP consistently achieves ~2-5x better compression than gzip on the same workload (see CLP OSDI 2021 paper, Figure 8). See [Research Papers](../reference/research.md).

### Format String Deduplication
Technique where static portions of log messages (format strings) are stored once and referenced by ID. Major contributor to CLP's compression ratio. See [Semantic Extraction](../concepts/semantic-extraction.md).

### Semantic Compression
Compression that understands data structure and meaning. Unlike byte-level compression (gzip), semantic compression exploits log patterns and variable types. See [Semantic Extraction](../concepts/semantic-extraction.md).

### Variable-First Extraction
log-surgeon's approach that extracts variables before knowing which log type a message matches. Variables are identified via patterns (not templates), enabling extraction from malformed or novel log formats. Contrast with template-first approaches that require regex templates. See [Semantic Extraction](../concepts/semantic-extraction.md).

### Semantic Variable Labeling
Process of assigning meaningful names to extracted variables based on context. Example: labeling `192.168.1.1` as `client_ip` rather than just `var_0`. Enables queries like `WHERE client_ip = '...'` instead of pattern matching. See [Semantic Extraction](../concepts/semantic-extraction.md).

### Context-Based Labeling
Labeling technique that uses surrounding text to determine variable meaning. Example: in `"User alice logged in"`, the word "User" provides context that `alice` is a username. Improves accuracy compared to type-only labeling. See [Semantic Extraction](../concepts/semantic-extraction.md).

### Format String Inference
Automatic discovery of log message templates (format strings) without predefined patterns. CLP analyzes log streams to identify static portions vs. variable portions, enabling compression even for unknown log sources. See [Semantic Extraction](../concepts/semantic-extraction.md).

---

## See Also

- [Research Papers](../reference/research.md) — Academic papers behind CLP
- [Naming Conventions](../reference/naming-conventions.md) — Column, index, and state naming patterns
- [Metadata Tables](../reference/metadata-tables.md) — Metadata table design and column reference
- [Semantic Extraction](semantic-extraction.md) — MPT, ERT, log-surgeon, and LLM-powered labeling
- [Performance Tuning](../operations/performance-tuning.md) — Split pruning, early termination, and index design
