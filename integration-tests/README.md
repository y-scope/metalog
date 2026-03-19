# Integration Tests

## Which test should I run?

| Directory | What it tests | How to run | Requirements |
|-----------|--------------|------------|--------------|
| `pipeline/` | Consolidation data path (IR → archive) | `./integration-tests/pipeline/run.sh` | Docker (testcontainers), `clp-s` (auto-built) |
| `coordination/` | Multi-node HA, fight-for-master, Kafka ingestion | `./integration-tests/coordination/validate-e2e.sh` | Docker Compose |
| `benchmarks/ingestion/` | Ingestion throughput (gRPC, Kafka) | `./integration-tests/benchmarks/ingestion/run.py` | Docker Compose, CGO |
| `benchmarks/task-queue-scalability/` | Task queue scalability matrix | See `benchmarks/task-queue-scalability/README.md` | Docker Compose |
| `stacks/presto/` | Presto-CLP query integration | `./integration-tests/stacks/presto/start.sh` | Docker Compose, `clp-core` `.deb` |

## Test descriptions

### `pipeline/` — Consolidation pipeline

Go test that exercises the full consolidation data path in a single process:

1. Generates KV-IR files using `clp-ffi-go`
2. Uploads to MinIO (testcontainers)
3. Planner groups files and creates consolidation tasks
4. Worker downloads IR, compresses with real `clp-s`, uploads archive
5. Planner finalizes: marks `ARCHIVE_CLOSED`, deletes source IR

Uses testcontainers (MariaDB + MinIO) — no Docker Compose needed.

### `coordination/` — Multi-node coordination

Bash script that spins up a full Docker Compose stack with 2 coordinator replicas and validates:

1. Fight-for-master: unassigned tables are claimed without double-claims
2. Single coordinator per table
3. Kafka JSON ingestion into the metadata table
4. Unique nodeIds from Docker `HOSTNAME`
5. Periodic reconciliation picks up newly registered tables

### `benchmarks/`

- **`ingestion/`** — Measures ingestion throughput in 3 modes: gRPC push, Kafka protobuf, Kafka JSON.
- **`task-queue-scalability/`** — Measures task queue throughput across a nodes x workers matrix.

### `stacks/presto/`

Full Presto-CLP integration stack for manual testing and demos.
