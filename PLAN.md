# Metalog: Go Rewrite — Commit Structure

## Context

The metalog project is a Go system for CLP metadata lifecycle management (ingestion, consolidation, querying). It uses MariaDB/MySQL as the single source of truth, with gRPC APIs, Kafka ingestion, S3-compatible storage, and a distributed coordinator/worker architecture.

This branch (`snapshot-mar-6-go`) replays 194 files / ~32.5K lines from `snapshot-mar-4-go` onto `main`, split into 33 reviewable commits ordered by dependency graph.

---

## Commit Groups

### Group A: Scaffolding (1 commit)

| # | Message | Files | Lines |
|---|---------|-------|-------|
| C01 | `build: scaffold Go module, Makefile, and CI workflow` | `.gitignore`, `Makefile`, `go.mod`, `go.sum`, `.github/workflows/publish-snapshot.yaml` | ~693 (mostly go.sum) |

### Group B: Core Metadata Path (12 commits) — reviewers focus here

| # | Message | Key Files | Impl | Tests |
|---|---------|-----------|------|-------|
| C02 | `feat(config): add node configuration, database config, and timeouts` | `internal/config/{node,database,storage,timeouts}.go` | 284 | 89 (2 UTs) |
| C03 | `feat(config): add config watcher and configmap parser` | `internal/config/{watcher,configmap}.go` | 221 | — |
| C04 | `feat(db): add connection pool, dialect, transactions, and SQL utilities` | `internal/db/{pool,dialect,errors,sqlident,tx}.go` | 210 | 245 (8 UTs) |
| C05 | `feat(timeutil): add timestamp conversion utilities` | `internal/timeutil/timestamps.go` | 44 | 51 (5 UTs) |
| C06 | `feat(metastore): add data model, columns, UPSERT builder, and advisory lock` | `internal/metastore/{model,columns,advisory_lock,filerecords_upsert}.go` | 348 | 161 (5 UTs) |
| C07 | `feat(metastore): add file records queries and state transitions` | `internal/metastore/filerecords.go` | 461 | 362 (IT) |
| C08 | `feat(schema): add SQL schema DDL and embed` | `schema/{schema.sql,embed.go}` | 553 | — |
| C09 | `feat(schema): add table provisioner, partition/index managers, evolution` | `internal/schema/{tableprovisioner,partitionmanager,indexmanager,evolution,validator}.go` | 571 | 179 (IT) |
| C10 | `feat(schema): add column registry with thread-safe slot allocation` | `internal/schema/columnregistry.go` | 530 | 205 (IT) |
| C11 | `feat(proto): add protobuf definitions and generated code` | `proto/*.proto`, `gen/proto/**/*.go` | 331 | — (gen: 3739) |
| C12 | `feat(coordinator/ingestion): add ingestion service, batching writer, and transformers` | `internal/coordinator/ingestion/*` | 595 | 302 (15 UTs) |
| C13 | `feat(coordinator): add table registration and progress tracker` | `internal/coordinator/{tableregistration,progress}.go` | 114 | 83 (5 UTs) |

### Group C: Query Engine (3 commits)

| # | Message | Key Files | Impl | Tests |
|---|---------|-----------|------|-------|
| C14 | `feat(query): add cache, cursor, and filter expression engine` | `internal/query/{cache,cursor,filter}.go` | 251 | 334 (18 UTs) |
| C15 | `feat(query): add column resolver and projection` | `internal/query/resolve.go` | 425 | 209 (17 UTs) |
| C16 | `feat(query): add sketch builder and split query engine` | `internal/query/{sketch,engine}.go` | 361 | 240 (3 UTs + IT) |

### Group D: Supporting Infrastructure (6 commits)

| # | Message | Key Files | Impl | Tests |
|---|---------|-----------|------|-------|
| C17 | `feat(health): add health check HTTP server` | `internal/health/server.go` | 72 | 202 (6 UTs) |
| C18 | `feat(storage): add storage backends, compressor, and registry` | `internal/storage/*` (13 files) | 505 | 563 (27 UTs) |
| C19 | `feat(testutil): add MariaDB test container helper` | `internal/testutil/mariadb.go` | 171 | — (is the test helper) |
| C20 | `feat(taskqueue): add task queue with SKIP LOCKED claiming` | `internal/taskqueue/*` | 488 | 358 (2 UTs + IT) |
| C21 | `feat(worker): add worker core and task prefetcher` | `internal/worker/{core,prefetcher}.go` | 209 | 301 (10 UTs) |
| C22 | `feat(coordinator/consolidation): add consolidation planner and policies` | `internal/coordinator/consolidation/*` (15 files) | 671 | 806 (40 UTs + IT) |

### Group E: Wiring (3 commits)

| # | Message | Key Files | Impl | Tests |
|---|---------|-----------|------|-------|
| C23 | `feat(grpc): add gRPC service handlers and server` | `internal/grpc/*` | 498 | 169 (8 UTs) |
| C24 | `feat(kafka): add Kafka metadata consumer` | `internal/kafka/*` | 541 | 144 (6 UTs) |
| C25 | `feat(node): add node architecture — shared resources, units, registry, lifecycle` | `internal/node/*` (6 files) | 1254 | 287 (IT) |

### Group F: Entry Points (3 commits) — first runnable binaries at C26

| # | Message | Key Files | Impl |
|---|---------|-----------|------|
| C26 | `feat(cmd): add metalog-server and metalog-apiserver` | `cmd/metalog-server/main.go`, `cmd/metalog-apiserver/main.go` | 182 |
| C27 | `feat(cmd): add metalog-worker entry point` | `cmd/metalog-worker/main.go` | 157 |
| C28 | `feat(cmd): add benchmark tools` | `cmd/benchmark-ingestion/main.go`, `cmd/benchmark-taskqueue/main.go` | 829 |

### Group G: Deploy, Integration Tests, Docs (5 commits)

| # | Message | Key Files | Lines |
|---|---------|-----------|-------|
| C29 | `feat(deploy): add Docker, example config, and deployment files` | `config/`, `docker/*` | 361 |
| C30 | `test(integration): add benchmark and functional test infrastructure` | `integration-tests/**/*` | 1673 |
| C31 | `docs: add README, PLAN, and concept documentation` | `README.md`, `PLAN.md`, `docs/concepts/*` | 3480 |
| C32 | `docs: add design documents and getting started guides` | `docs/design/*`, `docs/getting-started/*`, `docs/guides/*` | 2811 |
| C33 | `docs: add operations and reference documentation` | `docs/operations/*`, `docs/reference/*` | 3580 |

---

## Ordering Rationale

The commit ordering follows the Go dependency graph bottom-up:

1. **Scaffolding** — module definition, build tooling
2. **Core metadata path** — packages with no internal imports first (`config`, `db`, `timeutil`), then data layer (`metastore`, `schema`), then business logic that depends on them (`ingestion`)
3. **Query engine** — depends on `metastore`, `schema`, `db`
4. **Supporting infra** — `health`, `storage`, `testutil`, `taskqueue`, `worker`, `consolidation` — each depends on earlier packages
5. **Wiring** — `grpc`, `kafka`, `node` import everything
6. **Entry points** — `cmd/*` binaries that wire it all together
7. **Non-Go files** — deploy configs, integration tests, docs

Each commit compiles and passes `go build ./... && go vet ./... && go test ./... -short` independently. Integration test files use `//go:build integration` tags and are safely included alongside their packages without affecting the `-short` test run.

---

## Results

33 commits on `snapshot-mar-6-go`, all building and passing:

| Group | Commits | Description |
|-------|---------|-------------|
| **A** | C01 | Scaffolding (go.mod, Makefile, CI) |
| **B** | C02-C13 | Core metadata path (config -> db -> metastore -> schema -> proto -> ingestion) |
| **C** | C14-C16 | Query engine (cache/cursor/filter -> resolver -> sketch/engine) |
| **D** | C17-C22 | Supporting infra (health -> storage -> testutil -> taskqueue -> worker -> consolidation) |
| **E** | C23-C25 | Wiring (gRPC -> Kafka -> node) |
| **F** | C26-C28 | Entry points (server/apiserver -> worker -> benchmarks) |
| **G** | C29-C33 | Deploy, integration tests, docs |

- **174 unit tests pass** (`go test ./... -short`)
- **Build + vet clean** at every commit
- **0 diff** against `snapshot-mar-4-go` — all 194 files replayed
