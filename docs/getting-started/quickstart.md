# Quickstart

[← Back to docs](../README.md)

Setup and local development guide for the CLP Metastore Service.

## Prerequisites

- Docker and Docker Compose
- Go 1.25+

## Quick Start

### 1. Start Infrastructure

```bash
./docker/start.sh -d
```

This builds the CLP core package if needed, then starts all services (MariaDB, MinIO, coordinator nodes).

Verify services are healthy:

```bash
docker compose -f docker/docker-compose.yml ps
```

All services should report healthy status.

### 2. Build the Service

```bash
go build ./cmd/metalog
```

Produces a `metalog` binary in the current directory.

### 3. Run the Node (Coordinator + Workers)

`Node` is the main entry point. It hosts coordinators and workers in a single process.

```bash
# Run with default config path (/etc/clp/node.yaml)
./metalog serve

# Or specify a config file
./metalog serve --config docker/coordinator-node.yaml
```

Expected output (zap structured logging):

```
{"level":"info","msg":"starting server","config":"/etc/clp/node.yaml"}
{"level":"info","msg":"database pool created","poolSize":5,"minIdle":2}
{"level":"info","msg":"storage registry created","defaultBackend":"minio"}
{"level":"info","msg":"node started","coordinators":1,"workers":4}
```

### 4. Run as API Server Only (Optional)

For a read-only query API node, create a YAML config with only `database.replica` and enable `grpc`:

```bash
./metalog serve --config apiserver.yaml
```

Where `apiserver.yaml` sets `database.replica` (no primary) and enables `grpc.query: true` / `grpc.metadata: true`. No example file is included in the repo — create one based on the [Configuration Reference](../reference/configuration.md). See [Deployment](../operations/deployment.md) for details.

### 5. Register a Table via CLI

Tables are registered via the admin gRPC API. Register a table from the command line:

```bash
./metalog admin register-table \
  --addr localhost:9090 \
  --table clp_spark \
  --display-name "Spark Logs"
```

This calls the coordinator's AdminService gRPC endpoint to UPSERT the table.

Expected output:

```
table "clp_spark" created
```

---

## Running with Docker Compose

Use `docker/start.sh` — it ensures the CLP binary package is built before starting.

```bash
# Start everything (detached)
./docker/start.sh -d

# Force a fresh CLP build, then start
./docker/start.sh --rebuild-clp -d

# Scale coordinator nodes
./docker/start.sh -d --scale coordinator-node=3

# View logs
docker compose -f docker/docker-compose.yml logs -f coordinator-node
```

## Running Tests

```bash
# Run all tests
go test ./...

# Run a specific test package
go test ./coordinator/consolidation/...

# Run integration tests only (requires Docker)
go test -tags=integration ./...
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HOSTNAME` | _(OS hostname)_ | Used as `node_id` for table assignment (configurable via `coordinator.nodeIdEnvVar`) |
| `DB_PRIMARY_HOST` | — | Override `database.primary.host` |
| `DB_PRIMARY_PASSWORD` | — | Override `database.primary.password` |
| `GRPC_PORT` | — | Override `grpc.port` |

The config file path is specified via the `--config` flag (default: `/etc/clp/node.yaml`). Deployment-sensitive fields (DB credentials, ports) can be overridden via env vars — see [Configuration Reference](../reference/configuration.md#environment-variable-overrides) for the full list.

### Node Configuration (YAML)

Settings are organized by role: `database` (primary + optional replica), `storage`, `grpc`, `health`, `coordinator`, and `worker`. Per-table configuration (feature flags, consolidation policies) is managed via the admin gRPC API and stored in the database:

```yaml
database:
  primary:
    host: localhost
    port: 3306
    database: metalog_metastore
    user: root
    password: password
    poolSize: 5

storage:
  defaultBackend: minio
  backends:
    minio:
      endpoint: http://localhost:9000
      accessKey: minioadmin
      secretKey: minioadmin
      bucket: logs
      forcePathStyle: true

health:
  enabled: true
  port: 8081

coordinator:
  enabled: true
  nodeIdEnvVar: HOSTNAME       # env var for _table_assignment.node_id

# Tables are registered via admin gRPC API (AdminService/RegisterTable).
# The coordinator discovers assigned tables from the DB on startup and via
# periodic reconciliation. See docs/guides/configure-tables.md.

# Shared worker pool (claims tasks from all tables)
worker:
  concurrency: 4    # 0 = workers disabled
```

See [Configuration Reference](../reference/configuration.md) for full details.

## Verification

Confirm the system is operational:

```bash
# Check database tables
docker compose -f docker/docker-compose.yml exec mariadb mariadb -uroot -ppassword metalog_metastore \
  -e "SHOW TABLES;"

# Check MinIO buckets
docker compose -f docker/docker-compose.yml exec minio mc ls local/
```

### End-to-End Validation Script

For a thorough automated check — HA fight-for-master and single-owner enforcement — use the E2E validation script:

```bash
./test/coordination/test-multi-node.sh
```

What it tests:
1. Two coordinator nodes start and each gets a unique node ID (via `HOSTNAME`)
2. Unassigned tables are claimed within seconds; no double-claims occur
3. Only one coordinator runs per table (verified via logs)
4. Reconciliation: a table added after startup is picked up within seconds

Prerequisites: Docker, a built binary (`go build ./cmd/metalog`), and port `3307` free (or set `DB_PORT`).

## Troubleshooting

**Services not starting:**
```bash
docker compose -f docker/docker-compose.yml logs mariadb minio
```

**Connection refused errors:**
- Ensure infrastructure is healthy: `docker compose -f docker/docker-compose.yml ps`
- Check ports are not in use: `lsof -i :3306 -i :9000`

**Tests failing:**
- Ensure Docker is running (tests use testcontainers-go)
- Check for port conflicts with running infrastructure

## See Also

- [Architecture Overview](../concepts/overview.md) — Component design and data flow
- [Configuration Reference](../reference/configuration.md) — Detailed configuration reference
- [Scale Workers](../guides/scale-workers.md) — Scaling workers
