# Plan: Metalog Rust Rewrite — Full Implementation

## Context

Metalog is being rewritten from Go (branch `snapshot-mar-28-go`, 39 commits) to Rust. The Rust implementation lives on an **orphan branch `snapshot-mar-28-rust`**. Both community and enterprise crates are implemented together now; enterprise crates will be moved to a private repository later.

### Premium Features (5 separate crates)
| Crate | Feature | What it adds |
|-------|---------|-------------|
| `metalog-aggs` | Pre-aggregated fields | agg_fNN columns, _agg_registry, __AGG.* query resolution |
| `metalog-sketches` | Sketch/bloom filters | sketches SET, ext blob, SBBF, _sketch_registry |
| `metalog-kafka` | Kafka ingestion | Kafka consumer, source assignment, _kafka_source/_kafka_assignment |
| `metalog-ha` | High availability | Multi-node coordination, table claiming, heartbeat/lease, reconciliation |
| `metalog-consolidation` | Consolidation pipeline | Planner, policies, task queue, worker pool, prefetcher, archive creation |
| `metalog-retention` | Retention lifecycle | Policy-based deletion, retention scanning, per-file retention adjustment |

### Extension pattern (same for all 4):
1. **One proto file** with reserved premium field numbers (proto3 optional = ignored when absent)
2. **Reserved `Option<T>` slots on base types** (e.g., `FileRecord.aggs: Option<AggData>`)
3. **Named processor/provider trait slots** on components (`Option<Arc<dyn XxxProvider>>`)

### Community vs Enterprise behavior
| Capability | Community (base) | Enterprise (+ premium) |
|-----------|-----------------|----------------------|
| Ingestion | gRPC push only | gRPC push + Kafka pull |
| Columns | Dimensions only | Dimensions + aggregations + sketches |
| Query | __FILE.*, __DIM.* | + __AGG.*, sketch_expression |
| Consolidation | None (IR files stay as-is) | Planner + worker pool (IR → Archive) |
| Retention | None (files accumulate, manual DELETE only) | Policy-based scanning, auto-expire, per-file adjustment |
| Task queue | Not present | Database-backed task queue with SKIP LOCKED |
| Coordination | Every node starts coordinators for all tables | Single-owner per table via HA reconciliation |
| Multi-node ingestion | Works (stateless UPSERT behind load balancer) | Same + Kafka consumers with source assignment |
| Multi-node coordinator | Runs but duplicate work (safe, wasteful) | Fair-share claiming, no duplicates, automatic failover |
| Failover | Manual restart required | Automatic (peers adopt orphaned tables) |

**Community = metadata catalog.** Ingest file metadata (gRPC), query it, retain/expire it. No IR→Archive consolidation, no Kafka, no multi-node HA.

**Community multi-node ingestion is safe.** Multiple nodes can ingest gRPC simultaneously (stateless UPSERT). Coordinator duties (partition maintenance, retention, alias refresh) run on all nodes — duplicate work is safe but wasteful (advisory locks prevent DDL conflicts, idempotent deletes prevent data issues).

## Branch Setup

```bash
git checkout --orphan snapshot-mar-28-rust
git rm -rf .
# Initialize Rust workspace
```

## Rust Toolchain & Linting (matches y-scope/clp conventions)

### Toolchain
- **Stable** Rust (default, for building)
- **Nightly** Rust (for `cargo fmt` and `cargo clippy` — unstable rustfmt options require nightly)
- Components: `clippy`, `rustfmt`
- Test runner: `cargo-nextest` (installed via `cargo install --locked cargo-nextest`)
- Toolchain file: `rust-toolchain.toml` at workspace root

### Formatting: `cargo +nightly fmt --all`
Config: `.rustfmt.toml` (from `yscope-dev-utils`):
```toml
max_width = 100
newline_style = "Unix"
tab_spaces = 4
use_field_init_shorthand = true
# Nightly-only options:
group_imports = "StdExternalCrate"
imports_granularity = "Crate"
comment_width = 100
wrap_comments = true
normalize_comments = true
normalize_doc_attributes = true
reorder_impl_items = true
error_on_line_overflow = true
error_on_unformatted = true
format_strings = true
hex_literal_case = "Lower"
condense_wildcard_suffixes = true
brace_style = "PreferSameLine"
blank_lines_upper_bound = 1
unstable_features = true
```

### Linting: `cargo +nightly clippy --all-targets --all-features -- -D warnings`
- **`-D warnings`**: All warnings are errors (zero-warning policy)
- **`--all-targets`**: Lint tests, benches, examples too
- **`--all-features`**: Lint with all features enabled

### Per-Commit Requirements
Every commit must:
1. `cargo build` — compiles
2. `cargo +nightly fmt --all --check` — format clean
3. `cargo +nightly clippy --all-targets --all-features -- -D warnings` — lint clean
4. `cargo test` — all unit tests pass (matching Go test coverage)
5. `cargo nextest run` — same tests via nextest runner

### Testing Strategy
Each Rust crate mirrors the Go package's test suite:
- Unit tests inline (`#[cfg(test)] mod tests { ... }`) or in `tests/` directory
- Integration tests requiring MariaDB use `testcontainers-rs` (same MariaDB 10.6 image)
- No `//go:build integration` equivalent needed — testcontainers tests run by default (same as the Go branch after our fix)
- Test naming: match Go test names where possible for traceability
- Mocking: `mockall` crate for trait-based mocks (replaces sqlmock)

---

## Design: Premium Extension Architecture

### Principle

Base crate **knows the shape** of premium data (type definitions) but **not how to process it**. Processing logic (resolution, UPSERT building, query evaluation) lives in premium crates, injected via trait objects at startup.

### Crate Dependency Graph

```
metalog-types (defines data structs + provider/processor traits)
    ↑              ↑              ↑              ↑
metalog-aggs   metalog-sketches  metalog-kafka  metalog-ha
    ↑              ↑              ↑              ↑
metalog (binary)  ─────────── assembles base + premium ──────────
```

Base crates (`metalog-ingestion`, `metalog-query`, `metalog-node`, etc.) depend on `metalog-types` and accept `Option<Arc<dyn XxxProvider>>` — they never depend on premium crates directly.

---

### Layer 1: Types (`metalog-types`)

**FileRecord** has reserved slots for premium data:

```rust
pub struct FileRecord {
    // --- Base fields (always present) ---
    pub id: i64,
    pub state: FileState,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub dims: HashMap<String, Value>,
    pub dim_meta: Vec<DimMeta>,
    // ... other base fields ...

    // --- Premium field slots (None in community edition) ---
    pub aggs: Option<AggData>,
    pub sketches: Option<SketchData>,
}
```

**Premium data types** defined in base (structs only, no processing logic):

```rust
// metalog-types/src/agg.rs
pub struct AggData {
    pub entries: HashMap<String, Value>,
    pub meta: Vec<AggMeta>,
}

pub struct AggMeta {
    pub key: String,
    pub value: String,
    pub agg_type: String,
    pub value_type: String,
    pub alias_col: String,
}

// metalog-types/src/sketch.rs
pub struct SketchData {
    pub sketches: HashMap<String, Vec<u8>>,
    pub set_value: Option<String>,
    pub ext_blob: Option<Vec<u8>>,
}
```

**Processor traits** defined in base (implemented by premium crates):

```rust
// metalog-types/src/processors.rs

/// Processes aggregation columns during ingestion and queries.
#[async_trait]
pub trait AggProcessor: Send + Sync {
    /// Resolve logical agg keys → physical columns for a batch.
    /// May issue ALTER TABLE to allocate new columns.
    async fn resolve_batch(&self, ctx: &Context, table: &str, records: &mut [FileRecord])
        -> Result<ResolvedAggColumns>;

    /// Physical column names to add to the INSERT clause.
    fn upsert_columns(&self, resolved: &ResolvedAggColumns) -> Vec<String>;

    /// Per-row values for the extra INSERT columns.
    fn upsert_values(&self, record: &FileRecord, resolved: &ResolvedAggColumns) -> Vec<SqlValue>;

    /// ON DUPLICATE KEY UPDATE clauses for the extra columns.
    fn upsert_update_clauses(&self, resolved: &ResolvedAggColumns, is_mariadb: bool) -> Vec<String>;

    /// Resolve __AGG.* column references during query preparation.
    fn resolve_query_column(&self, ref_name: &str, registry: &dyn ColumnLookup) -> Option<String>;

    /// DDL for system tables (_agg_registry).
    fn system_table_ddl(&self) -> &str;

    /// DDL for columns to add to template table.
    fn template_columns_ddl(&self) -> &str;
}

/// Processes sketch columns during ingestion and queries.
#[async_trait]
pub trait SketchProcessor: Send + Sync {
    /// Encode sketch data into SET value + ext blob for a batch.
    async fn resolve_batch(&self, ctx: &Context, table: &str, records: &mut [FileRecord])
        -> Result<ResolvedSketchColumns>;

    /// Extra columns for UPSERT (sketches SET, ext MEDIUMBLOB).
    fn upsert_columns(&self, resolved: &ResolvedSketchColumns) -> Vec<String>;

    /// Per-row values for sketch columns.
    fn upsert_values(&self, record: &FileRecord, resolved: &ResolvedSketchColumns) -> Vec<SqlValue>;

    /// ON DUPLICATE KEY UPDATE for sketch columns.
    fn upsert_update_clauses(&self, resolved: &ResolvedSketchColumns, is_mariadb: bool) -> Vec<String>;

    /// Parse sketch_expression from query request → predicates.
    fn parse_expression(&self, expr: &str) -> Result<Vec<SketchPredicate>>;

    /// Evaluate bloom filters against row data. Returns true if row passes.
    fn evaluate(&self, predicates: &[SketchPredicate], ext_blob: &[u8]) -> Result<bool>;

    /// Build conditional ext projection SQL.
    fn ext_projection_sql(&self, predicates: &[SketchPredicate]) -> Option<String>;

    /// DDL for system tables (_sketch_registry).
    fn system_table_ddl(&self) -> &str;

    /// DDL for template table columns (sketches SET, ext MEDIUMBLOB).
    fn template_columns_ddl(&self) -> &str;
}
```

---

### Layer 2: Ingestion (`metalog-ingestion`)

**BatchingWriter** holds optional processor slots:

```rust
pub struct BatchingWriter {
    // ... base fields ...
    agg_processor: Option<Arc<dyn AggProcessor>>,
    sketch_processor: Option<Arc<dyn SketchProcessor>>,
}

impl BatchingWriter {
    fn flush_batch(&self, table: &str, batch: &mut [FileRecord]) -> Result<()> {
        // 1. Always: resolve dims
        let dim_cols = self.resolve_dims(batch)?;

        // 2. Premium: resolve aggs (skipped if None)
        let agg_cols = if let Some(proc) = &self.agg_processor {
            Some(proc.resolve_batch(&ctx, table, batch).await?)
        } else { None };

        // 3. Premium: resolve sketches (skipped if None)
        let sketch_cols = if let Some(proc) = &self.sketch_processor {
            Some(proc.resolve_batch(&ctx, table, batch).await?)
        } else { None };

        // 4. Build UPSERT with base + premium columns
        let sql = self.build_upsert(table, &dim_cols, agg_cols.as_ref(), sketch_cols.as_ref());
        self.execute(sql, batch).await
    }
}
```

**Proto conversion** (`ConvertRecord`): Base converts `FileFields` + `DimEntry`. Premium fields (`IngestAggEntry`, `SketchEntry`) are converted by premium crates via a similar trait or by the processors themselves.

---

### Layer 3: Query (`metalog-query`)

**QueryResolver** holds optional processors:

```rust
pub struct SplitQueryEngine {
    // ... base fields ...
    agg_processor: Option<Arc<dyn AggProcessor>>,
    sketch_processor: Option<Arc<dyn SketchProcessor>>,
}

impl SplitQueryEngine {
    fn resolve_column_ref(&self, raw: &str, registry: &dyn ColumnLookup) -> Result<String> {
        if let Some(col) = resolve_file_col(raw) { return Ok(col); }
        if let Some(col) = resolve_dim_col(raw, registry) { return Ok(col); }

        // Premium: __AGG.* resolution
        if let Some(proc) = &self.agg_processor {
            if let Some(col) = proc.resolve_query_column(raw, registry) {
                return Ok(col);
            }
        }

        Err(Error::UnknownColumn(raw.into()))
    }

    fn prepare_query(&self, params: &QueryParams) -> Result<PreparedQuery> {
        // ... base preparation ...

        // Premium: sketch predicates
        let sketch_preds = if let Some(proc) = &self.sketch_processor {
            if !params.sketch_expression.is_empty() {
                Some(proc.parse_expression(&params.sketch_expression)?)
            } else { None }
        } else { None };

        // ...
    }
}
```

---

### Layer 4: Schema (`metalog-schema`)

**Table provisioning** calls premium DDL if processors are present:

```rust
pub fn ensure_table(
    db: &Pool, table_name: &str,
    agg_proc: Option<&dyn AggProcessor>,
    sketch_proc: Option<&dyn SketchProcessor>,
) -> Result<()> {
    // Base: create table from template (dims only)
    execute_ddl(db, &base_create_table(table_name)).await?;

    // Premium: add agg registry + template columns
    if let Some(proc) = agg_proc {
        execute_ddl(db, proc.system_table_ddl()).await?;
        execute_ddl(db, proc.template_columns_ddl()).await?;
    }

    // Premium: add sketch registry + SET/ext columns
    if let Some(proc) = sketch_proc {
        execute_ddl(db, proc.system_table_ddl()).await?;
        execute_ddl(db, proc.template_columns_ddl()).await?;
    }

    Ok(())
}
```

---

### Layer 5: Proto (single file, reserved fields)

```protobuf
// ingestion.proto
message MetadataRecord {
    FileFields                   file               = 1;
    repeated DimEntry            dim                = 3;
    repeated SelfDescribingEntry self_describing_kv = 4;

    // Premium fields (ignored by base, populated by premium edition)
    repeated IngestAggEntry      agg                = 2;  // premium: aggs
    repeated SketchEntry         sketch             = 5;  // premium: sketches
    reserved 6 to 15;                                     // future premium fields
}
```

Base proto conversion skips fields 2 and 5 (they're empty/default). Premium proto conversion handler fills `record.aggs` and `record.sketches`.

---

### Layer 6: Kafka Provider (`metalog-kafka`, premium)

Base Node has gRPC ingestion only. Kafka is entirely premium.

**Trait** (defined in `metalog-types`):

```rust
#[async_trait]
pub trait KafkaProvider: Send + Sync {
    /// DDL for _kafka_source and _kafka_assignment tables
    fn system_tables_ddl(&self) -> Vec<&str>;

    /// Run kafka source reconciliation loop (claim sources, start consumers)
    async fn run_source_reconciliation(&self, token: CancellationToken);

    /// Stop all running kafka units gracefully
    async fn stop_all(&self);

    /// Admin: register a new kafka source (from proto request)
    async fn register_source(&self, table: &str, source: &str, topic: &str,
        bootstrap: &str, transformer: &str, group_id: &str, required_env: &str) -> Result<bool>;

    /// Admin: delete a kafka source
    async fn delete_source(&self, table: &str, source: &str) -> Result<()>;
}
```

**In Node** (base):
```rust
pub struct Node {
    kafka: Option<Arc<dyn KafkaProvider>>,  // None in community
    // ...
}

// In start():
if let Some(kafka) = &self.kafka {
    tokio::spawn(kafka.run_source_reconciliation(token.clone()));
}
```

**In AdminHandler** (base):
```rust
async fn register_kafka_source(&self, req: ...) -> Result<...> {
    let kafka = self.kafka.as_ref()
        .ok_or(Status::unimplemented("Kafka requires premium edition"))?;
    kafka.register_source(...).await
}
```

The premium `metalog-kafka` crate contains:
- rdkafka consumer, Adapter trait, MessageTransformer
- KafkaIngestionUnit lifecycle
- KafkaSourceStore (CRUD for _kafka_source table)
- Source assignment logic (claim, release, renew leases)

---

### Layer 7: HA Provider (`metalog-ha`, premium)

Base Node is single-node: starts coordinators for all tables directly at boot, no claiming.

**Traits** (defined in `metalog-types`):

```rust
#[async_trait]
pub trait HAProvider: Send + Sync {
    /// DDL for _node_registry, _table_assignment
    fn system_tables_ddl(&self) -> Vec<&str>;

    /// Register this node in the cluster
    async fn register_node(&self) -> Result<()>;

    /// Run liveness (heartbeat/lease renewal) until cancelled
    async fn run_liveness(&self, token: CancellationToken);

    /// Run reconciliation (table claiming, watchdog, ownership verification)
    async fn run_reconciliation(
        &self,
        token: CancellationToken,
        lifecycle: Arc<dyn CoordinatorLifecycle>,
    );
}

/// Callback for HA to control coordinator lifecycle (implemented by Node)
#[async_trait]
pub trait CoordinatorLifecycle: Send + Sync {
    async fn start_coordinator(&self, table_name: &str) -> Result<()>;
    async fn stop_coordinator(&self, table_name: &str);
    fn is_coordinator_stalled(&self, table_name: &str) -> bool;
    async fn restart_coordinator(&self, table_name: &str) -> Result<()>;
}
```

**In Node** (base):
```rust
// Community: start all tables directly
async fn start(&self) {
    if let Some(ha) = &self.ha {
        // Enterprise: HA controls coordinator lifecycle
        ha.register_node().await?;
        tokio::spawn(ha.run_liveness(token.clone()));
        tokio::spawn(ha.run_reconciliation(token.clone(), self.lifecycle()));
    } else {
        // Community: single node owns all tables
        for table in self.configured_tables() {
            self.start_coordinator(&table).await?;
        }
    }
}
```

The premium `metalog-ha` crate contains:
- Node registry (heartbeat UPSERT, dead node detection)
- Table assignment (fair-share claiming, orphan adoption, CAS updates)
- Reconciliation loop (claim orphans → claim unassigned → watchdog → ownership verify)
- Two HA strategies: Heartbeat and Lease

---

### Layer 9: Consolidation Provider (`metalog-consolidation`, premium)

Base has no consolidation — IR files stay as-is forever (or until retention deletes them).

**Trait** (defined in `metalog-types`):

```rust
#[async_trait]
pub trait ConsolidationProvider: Send + Sync {
    /// DDL for _task_queue table
    fn system_tables_ddl(&self) -> Vec<&str>;

    /// Create a planner for a table (started by CoordinatorUnit)
    fn create_planner(&self, config: PlannerConfig) -> Result<Arc<dyn PlannerRunner>>;

    /// Create a worker unit (started by Node)
    fn create_worker_unit(&self, config: WorkerUnitConfig) -> Result<Arc<dyn WorkerRunner>>;
}

#[async_trait]
pub trait PlannerRunner: Send + Sync {
    async fn run(&self, token: CancellationToken);
}

#[async_trait]
pub trait WorkerRunner: Send + Sync {
    async fn start(&self, token: CancellationToken);
    async fn stop(&self);  // two-phase: stop prefetcher → drain → force cancel
}
```

**In CoordinatorUnit** (base):
```rust
// Planner is only started if consolidation provider exists AND table config has consolidation enabled
if let Some(provider) = &self.consolidation {
    if self.table_cfg.consolidation.enabled {
        let planner = provider.create_planner(planner_config)?;
        join_set.spawn(planner.run(token.clone()));
    }
}
```

**In Node** (base):
```rust
// Worker unit only started if consolidation provider exists AND worker concurrency > 0
if let Some(provider) = &self.consolidation {
    if self.config.worker.concurrency > 0 {
        let worker_unit = provider.create_worker_unit(worker_config)?;
        worker_unit.start(token.clone()).await;
    }
}
```

The premium `metalog-consolidation` crate contains:
- Task queue (Queue, Task model, payload marshal, SKIP LOCKED claiming)
- Planner (7-step pipeline, candidate discovery, task creation)
- Policy trait + TimeWindowPolicy + SparkJobPolicy + PolicyChain
- InFlightSet
- Worker Core (task execution, archive creation)
- Prefetcher (batch claim → channel → N workers)
- ArchiveCreator, Compressor trait

---

### Layer 10: Binary Assembly

```rust
// Community edition:
let node = NodeBuilder::new(config).build();
// Single node, gRPC only, dimensions only

// Enterprise edition:
let node = NodeBuilder::new(config)
    .with_agg_processor(Arc::new(metalog_aggs::AggExtension::new()))
    .with_sketch_processor(Arc::new(metalog_sketches::SketchExtension::new()))
    .with_kafka_provider(Arc::new(metalog_kafka::KafkaModule::new(ingestion_svc.clone())))
    .with_ha_provider(Arc::new(metalog_ha::HAModule::new(registry, config.coordinator)))
    .with_consolidation_provider(Arc::new(metalog_consolidation::ConsolidationModule::new()))
    .build();
```

---

## Full Workspace Structure

```
metalog-rust/
├── Cargo.toml                    # workspace root
├── proto/                        # .proto files (copied from Go, single files)
│   ├── ingestion.proto
│   ├── splits.proto
│   ├── admin.proto
│   └── metadata.proto
├── crates/
│   ├── metalog-types/            # Core types, FileState, FileRecord, configs, processor traits
│   ├── metalog-encoding/         # LZ4+msgpack codec
│   ├── metalog-timeutil/         # Epoch nanos ↔ chrono
│   ├── metalog-logutil/          # FailureLogger (throttled error logging)
│   ├── metalog-config/           # YAML config, env overrides, validation
│   ├── metalog-db/               # Connection pool, dialect detection, tx helpers, error codes
│   ├── metalog-metastore/        # FileRecords, advisory locks, UPSERT builder, metadata queries
│   ├── metalog-schema/           # ColumnRegistry (dims), PartitionManager, IndexManager, DDL
│   ├── metalog-coordinator/      # ProgressTracker, TableRegistration
│   ├── metalog-ingestion/        # BatchingWriter, IngestionService, ConvertRecord
│   # NOTE: Retention is premium (see metalog-retention below)
│   ├── metalog-query/            # SplitQueryEngine, filters, keyset cursor, cache
│   ├── metalog-storage/          # Backend trait, Registry, ArchiveCreator, S3/FS/HTTP
│   # NOTE: Kafka crates are premium (see metalog-kafka below)
│   ├── metalog-grpc/             # tonic server + handlers (ingestion, query, metadata, admin)
│   ├── metalog-health/           # HTTP health/readiness probes
│   ├── metalog-telemetry/        # OpenTelemetry metrics, Prometheus exporter
│   ├── metalog-node/             # Node, CoordinatorUnit, WorkerUnit, NodeBuilder
│   # NOTE: Node registry (table assignment, liveness) is premium (see metalog-ha)
│   │
│   ├── metalog-aggs/             # PREMIUM: AggProcessor impl, agg registry, column resolution
│   ├── metalog-sketches/         # PREMIUM: SketchProcessor impl, SBBF, ext encoding, bloom eval
│   ├── metalog-kafka/            # PREMIUM: KafkaProvider impl, rdkafka consumer, source assignment
│   ├── metalog-ha/               # PREMIUM: HAProvider impl, reconciliation, heartbeat/lease, table claiming
│   ├── metalog-consolidation/    # PREMIUM: ConsolidationProvider impl, Planner, policies, task queue, worker, prefetcher
│   └── metalog-retention/        # PREMIUM: RetentionProvider impl, strategy, scanner, per-file adjustment
│
├── src/main.rs                   # Binary entry point (enterprise edition — includes premium)
└── tests/                        # Integration tests
```

---

## Implementation Phases (Commit Sequence)

Each phase = one commit that compiles and passes tests.

### Phase 1: Foundation
**Commit 1** — `feat: initialize Rust workspace with core types and utilities`
- `Cargo.toml` (workspace)
- `metalog-types`: FileState enum, FileRecord, DimMeta, AggData/SketchData structs, processor traits, TableConfig, KafkaSource, ColumnMapping, constants
- `metalog-encoding`: LZ4+msgpack marshal/unmarshal (lz4_flex + rmp-serde)
- `metalog-timeutil`: epoch_nanos(), day_boundary_nanos(), day_partition_name()
- `metalog-logutil`: FailureLogger with Mutex

### Phase 2: Configuration & Database
**Commit 2** — `feat: add config loading with env var overrides`
- `metalog-config`: NodeConfig, DatabaseConfig, CoordinatorConfig, etc. YAML + env overrides via serde

**Commit 3** — `feat: add database connection pooling and helpers`
- `metalog-db`: Pool (sqlx::MySqlPool), detect_database_type(), with_tx(), with_deadlock_retry(), error code checks, validate_sql_identifier(), quote_identifier()

### Phase 3: Metastore
**Commit 4** — `feat: add metastore data model and table config`
- `metalog-metastore`: FileState transitions, column constants, TableConfig serde, KafkaSource model, KafkaSourceStore

**Commit 5** — `feat: add advisory locks, UPSERT builder, and metadata queries`
- `metalog-metastore`: AdvisoryLock (dedicated connection), guarded UPSERT SQL builder (MariaDB/MySQL dialect), MetadataReader

**Commit 6** — `feat: add file records repository with state machine`
- `metalog-metastore`: FileRecords (upsert_batch, find_consolidation_pending, mark_archive_closed, delete_expired, retention transitions)

### Phase 4: Schema Management
**Commit 7** — `feat: add schema DDL and embedded SQL`
- `metalog-schema`: schema.sql (include_str!), table provisioner, DDL execution

**Commit 8** — `feat: add column registry for dimensions`
- `metalog-schema`: ColumnRegistry (dim resolution, fast-path/slow-path, advisory lock allocation, batch allocation, width expansion)

**Commit 9** — `feat: add column registry lifecycle and recycling`
- `metalog-schema`: RefreshAliases, RunRecycler (background task), Snapshot

**Commit 10** — `feat: add partition manager and index manager`
- `metalog-schema`: PartitionManager (daily RANGE, lookahead, cleanup), IndexManager (reconcile), BaseSchemaValidator

### Phase 5: Proto & Ingestion
**Commit 11** — `feat: add protobuf definitions and generated code`
- `proto/` files, `build.rs` with tonic-build + prost-build

**Commit 12** — `feat: add ingestion BatchingWriter`
- `metalog-ingestion`: BatchingWriter (per-table tokio tasks, mpsc channels, batch flush, UPSERT), with processor slots

**Commit 13** — `feat: add ingestion service and proto conversion`
- `metalog-ingestion`: IngestionService, ConvertRecord (proto→FileRecord), transformers

### Phase 6: Coordinator
**Commit 14** — `feat: add table registration and progress tracking`
- `metalog-coordinator`: TableRegistration, ProgressTracker (AtomicI64)

### Phase 7: Query
**Commit 16** — `feat: add query utilities — filters, resolution, cache`
- `metalog-query`: ValidateFilterExpression (sqlparser-rs), ResolveColumnRef, RewriteFilterColumns, Cache (TTL+LRU)

**Commit 17** — `feat: add split query engine with keyset pagination`
- `metalog-query`: SplitQueryEngine (prepare_query, execute_page, keyset WHERE builder), ResolveSplit, with processor slots

### Phase 8: gRPC
**Commit 18** — `feat: add gRPC server with ingestion and admin handlers`
- `metalog-grpc`: tonic Server, IngestionHandler, AdminHandler

**Commit 19** — `feat: add gRPC query and metadata handlers`
- `metalog-grpc`: QueryHandler (StreamSplits with background prefetch), MetadataHandler

### Phase 9: Infrastructure
**Commit 20** — `feat: add telemetry and health probes`
- `metalog-telemetry`: OpenTelemetry MeterProvider, Prometheus exporter
- `metalog-health`: HTTP server (/health, /ready), ReadinessChecker trait

**Commit 20** — `feat: add shared resources`
- `metalog-node` (partial): Resources struct (DB pools, column registry cache, telemetry)

### Phase 10: Storage & Lifecycle
**Commit 21** — `feat: add storage backend interface and implementations`
- `metalog-storage`: Backend trait, Registry, S3Backend, FilesystemBackend, HTTPBackend

### Phase 10b: Node Orchestration
**Commit 22** — `feat: add node orchestrator and CLI entry point`
- `metalog-node`: Node, CoordinatorUnit (partition maintenance, alias refresh, column recycler — no planner), NodeBuilder
- `src/main.rs`: clap CLI (serve + admin subcommands), signal handling, graceful shutdown

### Phase 12: Premium Features
**Commit 24** — `feat: add aggregation column processing (premium)`
- `metalog-aggs`: AggProcessor impl, AggRegistry (resolve/allocate), UPSERT column builder, __AGG.* query resolution

**Commit 25** — `feat: add sketch/bloom filter processing (premium)`
- `metalog-sketches`: SketchProcessor impl, SketchRegistry, SBBF evaluation, ext encoding/decoding, sketch predicate parsing

**Commit 26** — `feat: add Kafka ingestion (premium)`
- `metalog-kafka`: KafkaProvider impl, rdkafka consumer, Adapter/MessageTransformer traits, KafkaIngestionUnit, source assignment (claim/release/renew), _kafka_source + _kafka_assignment DDL, admin handler delegation

**Commit 27** — `feat: add high availability (premium)`
- `metalog-ha`: HAProvider impl, node registry (heartbeat UPSERT), table assignment (fair-share claiming, orphan adoption), reconciliation loop (4-step: claim orphans → claim unassigned → watchdog → ownership verify), two HA strategies (heartbeat + lease), _node_registry + _table_assignment DDL

**Commit 28** — `feat: add consolidation pipeline (premium)`
- `metalog-consolidation`: ConsolidationProvider impl, task queue (Queue, Task, SKIP LOCKED claiming), Planner (7-step pipeline), Policy trait + TimeWindowPolicy + SparkJobPolicy + PolicyChain, InFlightSet, Worker Core + Prefetcher, ArchiveCreator + Compressor trait, two-phase worker shutdown

**Commit 29** — `feat: add retention lifecycle (premium)`
- `metalog-retention`: RetentionProvider impl, Strategy trait, DefaultStrategy (3-phase: transition expired → delete metadata → delete storage), rate-limited storage cleanup (500 ops/sec), per-file retention adjustment support

### Phase 14: Integration & Docs
**Commit 31** — `feat: add integration tests and Docker deployment`
- Integration tests (testcontainers-rs with MariaDB), Docker setup, end-to-end pipeline test

**Commit 32** — `docs: add README and configuration reference`
- README.md, configuration docs

---

## Key Rust Dependencies

| Purpose | Crate |
|---------|-------|
| Async runtime | `tokio` (full features) |
| Database | `sqlx` (mysql, runtime-tokio) |
| gRPC | `tonic`, `prost`, `tonic-build` |
| Logging | `tracing`, `tracing-subscriber` |
| Metrics | `opentelemetry`, `opentelemetry-prometheus` |
| Serialization | `serde`, `serde_yaml`, `serde_json`, `rmp-serde` |
| Compression | `lz4_flex` |
| HTTP | `axum` (health probes) |
| Kafka | `rdkafka` |
| S3 | `aws-sdk-s3` |
| SQL parsing | `sqlparser` |
| Hashing | `xxhash-rust` (for SBBF), `md5` (for path hashing) |
| UUID | `uuid` (v7 + v4) |
| CLI | `clap` |
| Testing | `testcontainers`, `tokio-test` |
| Cancellation | `tokio-util` (CancellationToken) |
| Concurrent maps | `dashmap` (for writer/coordinator maps) |

---

## Verification (per commit)

```bash
# Must all pass for every commit:
cargo build                                                          # compiles
cargo +nightly fmt --all --check                                     # format clean
cargo +nightly clippy --all-targets --all-features -- -D warnings    # lint clean
cargo test                                                           # unit tests pass
```

## Verification (final)

1. **Community build**: Remove 4 premium crates from workspace deps → still compiles, runs single-node with gRPC-only ingestion and dim-only queries
2. **Enterprise build**: Full workspace → all 4 providers registered, multi-node HA, Kafka ingestion, aggs + sketches active
3. **Graceful degradation**: Community edition returns `UNIMPLEMENTED` for Kafka admin RPCs and clear errors for `__AGG.*` / `sketch_expression` usage
4. **Integration tests**: testcontainers MariaDB 10.6, end-to-end ingestion → query (both community and enterprise paths)
5. **Proto compatibility**: Same .proto files as Go version → Java/Go clients work unchanged
6. **HA test**: Enterprise multi-node: kill a node → tables redistribute within reconciliation interval
7. **Benchmark**: Ingestion throughput comparable to Go (~30K+ rec/sec)
8. **Test parity**: Every Go unit test has a Rust equivalent (verified by comparing test function counts)
