# Metalog: Full Go Rewrite Plan

## Context

The metalog project is a Go system for CLP metadata lifecycle management (ingestion, consolidation, querying). It uses MariaDB/MySQL as the single source of truth, with gRPC APIs, Kafka ingestion, S3-compatible storage, and a distributed coordinator/worker architecture.

The system deploys as a single binary with low memory footprint, native concurrency via goroutines and channels, and fast startup.

---

## Current Progress

| Phase | Description | Code | Tests | Status |
|-------|-------------|------|-------|--------|
| 0 | Project scaffolding | ✅ | — | Done |
| 1 | Foundation (config, db, util, health) | ✅ | ✅ | Done |
| 2 | Metastore data layer | ✅ | ✅ | Done |
| 3 | Schema management | ✅ | ✅ | Done |
| 4 | Storage layer | ✅ | ✅ | Done |
| 5 | Coordinator — Ingestion | ✅ | ✅ | Done |
| 6 | Coordinator — Consolidation | ✅ | ✅ | Done |
| 7 | Worker | ✅ | ✅ | Done |
| 8 | Query layer | ✅ | ✅ | Done |
| 9 | Node architecture | ✅ | ✅ | Done |
| 10 | gRPC server | ✅ | — | Code done (thin adapters, tested via integration) |
| 11 | Kafka integration | ✅ | ✅ | Done (confluent-kafka-go) |
| 12 | Entry points | ✅ | — | Done |

### Summary
- **75 Go source files** across 14 packages
- **~8.7K lines of Go code** (excluding generated proto stubs)
- **43 test files** with 224 unit/integration tests
- All packages compile, `go vet` clean, all tests pass
- End-to-end verified with Docker compose (MariaDB + Kafka + MinIO + Go node)

### What's Done
- Go code at repo root
- Proto files with `go_package` options, generated stubs in `gen/proto/`
- Full implementation of all 12 phases (code only)
- 3 entry points: `metalog-server`, `metalog-worker`, `metalog-apiserver`
- **12 high-priority unit test files ported** (see Test Plan below)

### What's Left
1. ~~**Medium-priority unit tests**~~ ✅ Done
2. ~~**Integration tests**~~ ✅ Done (testcontainers-go MariaDB 10.6)
3. ~~**Kafka consumer**~~ ✅ Done (confluent-kafka-go)
4. ~~**Missing features**~~ ✅ Done — All planned files implemented:
   - `internal/query/cache.go` — CacheService (TTL-based, sync.RWMutex)
   - `internal/query/resolve.go` — ResolvedProjection (column resolution)
   - `internal/query/sketch.go` — SketchEvaluator (bloom/cuckoo SET filter)
   - `internal/coordinator/consolidation/archivepath.go` — ArchivePathGenerator
   - `internal/coordinator/consolidation/sparkjob.go` — SparkJobPolicy
   - `internal/coordinator/consolidation/audit.go` — AuditPolicy
   - `internal/coordinator/consolidation/factory.go` — CreatePolicy factory
   - `internal/coordinator/ingestion/defaulttransformer.go` — JsonRecordTransformer
   - `internal/storage/http.go` — HTTPBackend (read-only)
   - `internal/storage/errors.go` — StorageError types + classification helpers
   - Note: MinIO uses S3Backend (S3-compatible), GCS works via S3 compat API
5. ~~**End-to-end verification**~~ ✅ Done — Docker compose smoke test passed:
   - Go multi-stage Dockerfile (build + debian-slim runtime)
   - schema.sql embedded via `//go:embed` (self-contained binary)
   - Full stack: MariaDB 10.6 + Kafka + MinIO + Go coordinator node
   - All 4 gRPC services respond (ingestion, query, metadata, coordinator)
   - Health/readiness endpoints return OK/READY
   - Table provisioning, partition creation, table claiming all verified

---

## Test Plan

### Test Overview (43 files, 224 tests)

See individual `*_test.go` files for test details. Key coverage areas:
- Config loading, DSN building, timestamps, partition names
- FileState transitions, guarded UPSERT SQL generation, LZ4+msgpack payloads
- InFlightSet concurrent access, TimeWindow/SparkJob/Audit policy grouping
- Proto→FileRecord conversion, Kafka consumer transformer, filter validation
- Keyset cursor encoding, sketch evaluation, query cache TTL
- Storage backends (filesystem, HTTP, S3), registry, compressor, archive creator
- Worker core task execution, prefetcher backoff/distribution
- Health endpoints, DB dialect detection, SQL identifier validation
- Integration tests (testcontainers MariaDB): FileRecords, TaskQueue, ColumnRegistry, CoordinatorRegistry, Planner, QueryEngine, PartitionManager, IndexManager, AdvisoryLock

### All Tests — ✅ COMPLETED

Unit tests and integration tests (testcontainers-go MariaDB 10.6) are complete. See the test overview above for the full list.

---

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| SQL layer | Squirrel query builder + raw SQL for dynamic UPSERTs | Squirrel for standard queries; raw SQL with `strings.Builder` for guarded UPSERT (squirrel can't express `IF(guard, VALUES(x), x)`) |
| Logging | `uber-go/zap` | High-performance structured logging |
| DB driver | `go-sql-driver/mysql` | Standard, mature MySQL/MariaDB driver (no official MariaDB Corp Go driver exists; this is the de facto standard for both) |

---

## Go Module & Package Layout

```
metalog/
  go.mod                        # module github.com/yscope/metalog
  go.sum
  Makefile                      # proto generation, build, test targets

  cmd/
    metalog-server/main.go      # unified entry point (node + gRPC server)
    metalog-worker/main.go      # standalone worker binary
    metalog-apiserver/main.go   # standalone query API server

  proto/                        # .proto files (with go_package options)
    coordinator.proto
    ingestion.proto
    metadata.proto
    splits.proto
  gen/proto/                    # generated Go code (protoc-gen-go + protoc-gen-go-grpc)
    coordinatorpb/
    ingestionpb/
    metadatapb/
    splitspb/

  internal/
    config/                     # YAML config, env vars, defaults
      node.go                   # NodeConfig struct hierarchy
      database.go               # DatabaseConfig, DSN builder
      storage.go                # ObjectStorageConfig
      timeouts.go               # timeout/backoff constants

    db/                         # database pool, dialect detection, tx helpers
      pool.go                   # *sql.DB creation, pool config
      dialect.go                # DatabaseTypeDetector (MySQL/MariaDB/Aurora/TiDB)
      tx.go                     # transaction helpers, deadlock retry wrapper
      errors.go                 # MySQL error classification (isDeadlock, isDuplicateKey)
      sqlident.go               # SQL identifier validation, QuoteIdentifier

    metastore/                  # data access layer
      model.go                  # FileRecord, FileState, DeletionResult, StoragePath
      columns.go                # column name constants, BaseCols, GuardedUpdateCols
      filerecords.go            # FileRecords repository (squirrel + raw SQL for UPSERT)
      filerecords_upsert.go     # guarded UPSERT SQL builder (strings.Builder)
      advisory_lock.go          # GET_LOCK/RELEASE_LOCK helper

    taskqueue/                  # task queue subsystem
      model.go                  # Task, TaskState, TaskCounts
      queue.go                  # Queue (create, claim, complete, fail, reclaim, cleanup)
      payload.go                # TaskPayload, TaskResult (LZ4+msgpack serialization)

    schema/                     # schema management
      columnregistry.go         # ColumnRegistry (slot alloc, cache, online DDL)
      partitionmanager.go       # daily partition lifecycle
      tableprovisioner.go       # CREATE TABLE from template
      indexmanager.go           # dynamic index reconciliation
      validator.go              # RecordStateValidator
      evolution.go              # SchemaEvolution (ALTER TABLE ADD COLUMN)

    coordinator/                # coordinator business logic
      tableregistration.go      # TableRegistrationService
      progress.go               # ProgressTracker (goroutine stall detection)

    coordinator/ingestion/      # ingestion pipeline
      service.go                # IngestionService (validate + submit)
      batchingwriter.go         # BatchingWriter (per-table goroutines + channels)
      protoconverter.go         # proto MetadataRecord -> FileRecord
      transformer.go            # RecordTransformer interface + registry
      defaulttransformer.go     # JsonRecordTransformer

    coordinator/consolidation/  # consolidation workflow
      planner.go                # Planner goroutine loop
      inflightset.go            # InFlightSet (sync.RWMutex + map)
      policy.go                 # Policy interface
      timewindow.go             # TimeWindowPolicy
      sparkjob.go               # SparkJobPolicy
      audit.go                  # AuditPolicy
      factory.go                # CreatePolicy factory
      archivepath.go            # ArchivePathGenerator

    worker/                     # consolidation workers
      core.go                   # WorkerCore (task execution loop)
      prefetcher.go             # Prefetcher (batch claim goroutine + channel)

    query/                      # query layer
      engine.go                 # SplitQueryEngine
      filter.go                 # FilterExpressionValidator
      cursor.go                 # KeysetCursor encoding/decoding
      cache.go                  # CacheService (TTL-based, sync.RWMutex)
      resolve.go                # Column resolution helpers
      sketch.go                 # SketchEvaluator (bloom/cuckoo SET filter)

    storage/                    # object storage abstraction
      backend.go                # StorageBackend interface
      s3.go                     # S3StorageBackend (aws-sdk-go-v2)
      filesystem.go             # FilesystemStorageBackend
      http.go                   # HTTPBackend (read-only)
      registry.go               # StorageRegistry (name -> backend map)
      archive.go                # ArchiveCreator (download IR, run clp-s, upload)
      compressor.go             # ClpCompressor (os/exec subprocess)
      errors.go                 # StorageError types + classification helpers

    node/                       # node orchestration
      node.go                   # Node (multi-unit host, lifecycle)
      shared.go                 # SharedResources (*sql.DB, StorageRegistry, etc.)
      coordinator_unit.go       # CoordinatorUnit
      worker_unit.go            # WorkerUnit (two-phase shutdown)
      registry.go               # CoordinatorRegistry (table assignment, heartbeat/lease HA)

    grpc/                       # gRPC server layer (thin adapters)
      server.go                 # GrpcServer setup (unified port, reflection)
      ingestion.go              # IngestionGrpcService
      query.go                  # QuerySplitsGrpcService (streaming)
      metadata.go               # MetadataGrpcService (catalog)
      coordinator.go            # CoordinatorGrpcService (admin)

    kafka/                      # Kafka ingestion (confluent-kafka-go)
      consumer.go               # Consumer (single-threaded poll + drain)
      poller.go                 # MetadataPoller (per-table polling goroutine)
      factory.go                # MetadataPollerFactory

    health/                     # health checks
      server.go                 # HTTP liveness endpoint (net/http)

    timeutil/
      timestamps.go             # epoch nanosecond helpers

    testutil/
      mariadb.go                # testcontainers-go MariaDB helper
```

---

## Go Library Choices

| Go Library | Usage |
|-----------|-------|
| `github.com/Masterminds/squirrel` + raw SQL | Query building; raw SQL for dynamic UPSERT |
| `database/sql` built-in pool | Connection pooling (`SetMaxOpenConns`, `SetMaxIdleConns`, `SetConnMaxIdleTime`) |
| `github.com/go-sql-driver/mysql` | MySQL/MariaDB driver |
| `google.golang.org/grpc` + `google.golang.org/protobuf` | gRPC services |
| `github.com/confluentinc/confluent-kafka-go/v2` | librdkafka wrapper; manual offset commit |
| `github.com/aws/aws-sdk-go-v2/service/s3` | S3, MinIO, GCS (S3-compatible) |
| `github.com/pierrec/lz4/v4` | TaskPayload/TaskResult compression |
| `github.com/vmihailenco/msgpack/v5` | TaskPayload/TaskResult serialization |
| `gopkg.in/yaml.v3` | YAML config loading |
| `go.uber.org/zap` | Structured logging |
| `github.com/testcontainers/testcontainers-go` | Integration tests |

---

## SQL Layer Design

### Standard Queries (squirrel)

```go
// TaskQueue claim example
query, args, _ := sq.Select("task_id", "table_name", "input").
    From("_task_queue").
    Where(sq.Eq{"state": "pending"}).
    OrderBy("task_id ASC").
    Limit(uint64(batchSize)).
    Suffix("FOR UPDATE").
    ToSql()
rows, err := tx.QueryContext(ctx, query, args...)
```

### Guarded UPSERT (raw SQL with strings.Builder)

Squirrel cannot express `IF(guard, VALUES(col), col)`. Build this with `strings.Builder`:

```go
func buildGuardedUpsertSQL(table string, baseCols, dimCols, aggCols []string, rowCount int) (string, int) {
    // INSERT INTO <table> (col1, ..., dim_f01, agg_f01)
    // VALUES (?,?,...), (?,?,...)
    // ON DUPLICATE KEY UPDATE
    //   state = IF(guard, VALUES(state), state),
    //   dim_f01 = IF(guard, VALUES(dim_f01), dim_f01),
    //   max_timestamp = IF(guard, VALUES(max_timestamp), max_timestamp)  -- LAST
    //
    // guard = state NOT IN ('IR_ARCHIVE_CONSOLIDATION_PENDING',
    //         'ARCHIVE_CLOSED','ARCHIVE_PURGING') AND VALUES(max_timestamp) > max_timestamp
}
```

### Transaction Patterns

- **Auto-commit**: Single `db.ExecContext` / `db.QueryContext` (most operations)
- **Explicit tx**: `db.BeginTx(ctx, nil)` for multi-statement (batch upsert, reclaim)
- **READ COMMITTED tx**: `db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})` for task claim
- **Deadlock retry**: Wrap in loop checking `isDeadlock(err)`, max 10 retries with 1-50ms random jitter
- **Advisory locks**: `db.Conn(ctx)` for dedicated connection, `GET_LOCK`/`RELEASE_LOCK`

### DSN Configuration

```
user:pass@tcp(host:3306)/db?parseTime=true&interpolateParams=true&maxAllowedPacket=16777216
```

---

## Risk Areas

| Risk | Severity | Mitigation |
|------|----------|------------|
| Guarded UPSERT SQL correctness | **High** | Integration test every variant against real MariaDB |
| Deadlock retry in Go's database/sql | **High** | Verify `*mysql.MySQLError` error code 1213 detection; concurrent goroutine stress tests |
| Dynamic column handling (dim_f01..fNN) | **Medium** | Raw SQL builder for UPSERT; squirrel for standard queries with explicit column lists |
| Graceful shutdown ordering | **Medium** | Layered `context.Context` cancellation + `sync.WaitGroup` per layer |
| Proto package remapping | **Low** | Add `go_package` options to .proto files; test generated stubs compile |
| Advisory lock connection affinity | **Low** | Use `db.Conn(ctx)` for dedicated connection; `defer conn.Close()` |
| `interpolateParams=true` + SQL injection | **Low** | Validate table/column names with regex; all data values via bind params |

---

## Verification Plan

1. **Unit tests**: `go test ./internal/...` — config parsing, timestamps, state transitions, payload serialization, SQL generation
2. **Integration tests**: `go test -tags=integration ./internal/metastore/... ./internal/taskqueue/... ./internal/coordinator/...` — testcontainers-go MariaDB 10.6, load `schema/schema.sql`, run full CRUD cycles
3. **gRPC smoke test**: Start server, use `grpcurl` to call RegisterTable, Ingest, StreamSplits
4. **Docker compose**: Build Go binary, run with existing `docker/docker-compose.yml` (MariaDB, Kafka, MinIO), verify ingestion→consolidation→query flow
5. **Compatibility**: Verify the server works with the expected database schema and proto API

