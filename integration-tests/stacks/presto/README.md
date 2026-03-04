# Presto Integration Stack

A self-contained Docker Compose environment for developing and testing the
Presto CLP connector against a live CLP service with realistic metadata.

## What it does

Running `./start.sh` brings up three long-lived services:

| Service | Host port(s) | Description |
|---|---|---|
| `mariadb` | 3307 | MariaDB 10.6 — metastore database |
| `coordinator` | 9091 (gRPC ingestion), 8081 (health) | CLP coordinator — accepts records via gRPC |
| `api-server` | 50051 (gRPC query) | CLP query API — serves splits to Presto |

On startup a one-shot `data-loader` container sends 12 test records into the
coordinator via gRPC. The coordinator auto-creates the `clp_cockroachdb` table
(including dimension and aggregate columns) on the first insert. After loading,
`data-loader` exits and the three services keep running until you press Ctrl-C.

While the stack is running you can point a Presto instance at the query API on
port **50051** and issue queries.

## Prerequisites

- Docker >= 24 with the Compose V2 plugin (`docker compose`)
- **[grpcurl](https://github.com/fullstorydev/grpcurl)** (optional) — for
  smoke-testing the API from the command line

The coordinator and API server images are built from source via a multi-stage
Maven build. The first `./start.sh` run compiles the full project and takes
3–5 minutes. Subsequent runs reuse the Docker layer cache.

## Quick start

```bash
# From this directory:
./start.sh

# Run detached (background):
./start.sh -d
```

The stack is ready once you see the coordinator health-check pass and the
data-loader exits cleanly. Press **Ctrl-C** to stop all services.

## Connecting Presto

Point your Presto CLP connector at the query API server:

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `50051` |
| Protocol | gRPC (plaintext) |
| Table | `clp_cockroachdb` |

The API server exposes the `QuerySplitsService` and `MetadataService` RPCs on
port **50051**.

## Smoke-testing with grpcurl

The API server registers gRPC reflection, so you can explore it without proto
files.

```bash
# List all registered services
grpcurl -plaintext localhost:50051 list

# List available tables
grpcurl -plaintext localhost:50051 \
  com.yscope.clp.service.query.api.proto.grpc.MetadataService/ListTables

# List dimensions for the test table
grpcurl -plaintext \
  -d '{"table": "clp_cockroachdb"}' \
  localhost:50051 \
  com.yscope.clp.service.query.api.proto.grpc.MetadataService/ListDimensions

# Stream all splits, newest first
grpcurl -plaintext \
  -d '{"table": "clp_cockroachdb", "order_by": [{"column": "max_timestamp", "order": "DESC"}]}' \
  localhost:50051 \
  com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits

# Filter by zone dimension
grpcurl -plaintext \
  -d '{
    "table": "clp_cockroachdb",
    "order_by": [{"column": "max_timestamp", "order": "DESC"}],
    "filter_expression": "__DIMENSION.zone = '\''us-east-1a'\''"
  }' \
  localhost:50051 \
  com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits
```

The final message in each stream has `done: true` and a `stats` field with
`splits_scanned` and `splits_matched` counts.

## Test data

12 CockroachDB IR records across 4 nodes × 3 hours (2025-02-01 00:00–02:00 UTC).

### Dimensions

| Name | Type | Values |
|---|---|---|
| `service` | `str(128)` | `cockroachdb` |
| `host` | `str(128)` | `node01.crdb.us-east-1a`, … |
| `zone` | `str(128)` | `us-east-1a`, `us-east-1b`, `us-west-2a`, `us-west-2b` |

### Aggregates

All count log lines at or above the given severity (GTE semantics).
`level GTE info` equals `record_count` (every log line is info-or-above).

| Key | Qualifier | Value |
|---|---|---|
| `level` | `info` | `record_count` (total lines) |
| `level` | `warn` | ~5% of record_count |
| `level` | `error` | ~2% of record_count |
| `level` | `fatal` | ~0.2% of record_count |

## Directory layout

```
presto/
├── README.md
├── docker-compose.yaml
├── start.sh
├── Dockerfile          # multi-stage Maven build → JRE (coordinator + api-server)
├── Dockerfile.loader   # Python gRPC data loader
├── config/
│   └── node.yaml       # coordinator config (tables auto-created on first insert)
└── scripts/
    └── load_test_data.py
```
