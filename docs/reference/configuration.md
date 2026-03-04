# Configuration Reference

[← Back to docs](../README.md)

Configuration is split between `node.yaml` (node-level settings) and the database (per-table config and assignments).

---

## node.yaml — Node Settings

Node-level shared resources: identity, database connection, and storage backend.

```yaml
node:
  name: coordinator-node-1       # Display name (used in logs and metrics)
  nodeIdEnvVar: HOSTNAME         # Env var whose value becomes node_id in _table_assignment
                                 # Falls back to InetAddress.getLocalHost().getHostName()

  database:
    host: localhost
    port: 3306
    database: clp_metastore
    user: root
    password: ""
    poolSize: 20
    poolMinIdle: 5
    connectionTimeout: 30000
    idleTimeout: 600000

  storage:
    defaultBackend: minio
    clpBinaryPath: /usr/bin/clp-s   # Path to the clp-s binary used for consolidation
    backends:
      minio:
        endpoint: http://localhost:9000
        accessKey: minioadmin
        secretKey: minioadmin
        forcePathStyle: true

  health:
    enabled: true
    port: 8081

  # Embedded query API server (Vert.x gRPC). Disabled by default.
  # Enable when you want a single all-in-one process instead of a separate API server.
  # api:
  #   enabled: false
  #   grpcPort: 9090

  # Ingestion batching settings (per path).
  # ingestion:
  #   kafka:
  #     recordsPerBatch: 500      # records per DB batch from Kafka poll
  #     maxQueuedRecords: 5000    # max records buffered per table
  #     flushTimeoutMs: 1000      # flush partial batch after this many ms
  #   grpc:
  #     recordsPerBatch: 1000
  #     maxQueuedRecords: 10000
  #     flushTimeoutMs: 1000

# Shared worker pool (claims tasks from all tables)
worker:
  numWorkers: 0           # 0 = no workers (coordinator-only node)
  prefetchQueueSize: 5    # tasks held in-memory; one DB batch-claim per refill
```

Override via environment variable: `NUM_WORKERS=4`

### Worker Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `worker.numWorkers` | `0` | Number of in-process worker threads. `0` = coordinator-only node. |
| `worker.prefetchQueueSize` | `5` | Capacity of the in-memory prefetch queue. The `TaskPrefetcher` batch-claims this many tasks per DB round-trip. |

**Tuning `prefetchQueueSize`:**
- **Increase** if workers are frequently starved (the queue drains before the prefetcher refills). A larger queue amortises per-batch DB latency across more workers.
- **Decrease** if you want more responsive graceful shutdown (fewer in-flight tasks to reset on stop) or lower memory footprint.
- Default of 5 is suitable for most deployments. At 4+ workers, increase to `numWorkers` so each worker gets at least one task per refill without contention.

---

## Coordinator Thread Configuration

These settings are stored in the `_table_config` database table and can be overridden per table
via `node.yaml` (under `tables[].{property}`) or at runtime via `CoordinatorService/RegisterTable`.

| Property | Default | Thread | Description |
|----------|---------|--------|-------------|
| `loopIntervalMs` | 5000 | Planner | Main coordinator loop sleep interval |
| `retentionCleanupIntervalMs` | 60000 | Retention | Retention cleanup interval |
| `storageDeletionDelayMs` | 100 | Storage Deletion | Rate limit delay between storage deletions |
| `partitionMaintenanceIntervalMs` | 3600000 | Partition Manager | Partition create/merge check interval |
| `consolidationEnabled` | `true` | Consolidation | Enable IR→Archive consolidation |
| `schemaEvolutionEnabled` | `true` | Schema Evolution | Enable auto-DDL for new fields |
| `schemaEvolutionMaxDimColumns` | 50 | Schema Evolution | Max dimension columns to allocate |
| `schemaEvolutionMaxCountColumns` | 20 | Schema Evolution | Max aggregation columns to allocate |

Ingestion batching settings (configured under `node.ingestion` in `node.yaml`):

| Property | Default | Path | Description |
|----------|---------|------|-------------|
| `ingestion.kafka.recordsPerBatch` | 500 | Kafka | Records accumulated per DB batch |
| `ingestion.kafka.maxQueuedRecords` | 5000 | Kafka | Max buffered records per table |
| `ingestion.kafka.flushTimeoutMs` | 1000 | Kafka | Flush partial batch after this delay |
| `ingestion.grpc.recordsPerBatch` | 1000 | gRPC | Records accumulated per DB batch |
| `ingestion.grpc.maxQueuedRecords` | 10000 | gRPC | Max buffered records per table |
| `ingestion.grpc.flushTimeoutMs` | 1000 | gRPC | Flush partial batch after this delay |

---

## HA Configuration

```yaml
node:
  coordinatorHaStrategy: heartbeat  # or "lease"
  reconciliationIntervalSeconds: 60 # how often to claim unassigned tables (both modes)
  orphanScanIntervalSeconds: 60     # how often to scan for tables from dead nodes

  # Heartbeat mode
  heartbeatIntervalSeconds: 30
  deadNodeThresholdSeconds: 180

  # Lease mode
  leaseTtlSeconds: 180
  leaseRenewalIntervalSeconds: 30
```

See [Deploy HA](../guides/deploy-ha.md) for operational guidance.

---

## Environment Variable Overrides

`ServiceConfig` (the legacy flat-config loader used in some deployment paths) converts its
dot-notation property keys to environment variables by uppercasing and replacing `.` with `_`.
Environment variables take precedence over values from `application.properties`.

```bash
COORDINATOR_CONSOLIDATION_ENABLED=false
COORDINATOR_DELETION_ENABLED=false
COORDINATOR_PARTITION_MANAGER_THREAD_ENABLED=true
SCHEMA_EVOLUTION_ENABLED=false
NUM_WORKERS=4
```

For the primary `node.yaml`-based deployment, per-table settings are stored in `_table_config` and
overridden at the table level via `node.yaml` (`tables[].{property}`) or at runtime via
`CoordinatorService/RegisterTable`. See [Coordinator Thread Configuration](#coordinator-thread-configuration)
for a full property reference.

---

## API Server Configuration

The API server loads configuration via `ApiServerConfig.fromEnvironment()` — environment variables only, no YAML file.

| Variable | Default | Description |
|----------|---------|-------------|
| `API_GRPC_PORT` | `9090` | gRPC server port |
| `DB_JDBC_URL` | `jdbc:mariadb://localhost:3306/metastore` | Read replica JDBC URL |
| `DB_USERNAME` | `root` | Database user |
| `DB_PASSWORD` | _(empty)_ | Database password |
| `DB_POOL_SIZE` | `20` | HikariCP max pool size (read-only) |
| `CACHE_MAX_SIZE` | `100000` | Caffeine cache max entries |
| `CACHE_SCHEMA_TTL_SECONDS` | `300` | Schema cache TTL (5 min) |
| `CACHE_QUERY_TTL_SECONDS` | `10` | Query result cache TTL |
| `CACHE_FILE_TTL_SECONDS` | `30` | File-by-ID cache TTL |
| `CACHE_SKETCH_TTL_SECONDS` | `60` | Sketch cache TTL |
| `API_REQUEST_TIMEOUT_MS` | `30000` | Default request timeout |
| `API_CURSOR_TTL_MS` | `300000` | How long a keyset cursor remains valid (5 min) |
| `API_STREAM_IDLE_TIMEOUT_MS` | `60000` | Cancel stream if client not consuming |
| `API_QUERY_TIMEOUT_MS` | `120000` | Max DB query time |
| `API_STREAMING_DB_PAGE_SIZE` | `1000` | Rows fetched per DB page in keyset pagination |

---

## Schema Auto-Creation

All schema setup is automatic. On startup, the node creates the database (via `createDatabaseIfNotExist`), initializes the `_table*` registry and `_task_queue`, and creates per-table metadata tables. See [Architecture Overview](../concepts/overview.md) for the full startup sequence.

---

## See Also

- [Architecture Overview](../concepts/overview.md) — System overview and startup sequence
- [Quickstart](../getting-started/quickstart.md) — Setup and first run
- [Configure Tables](../guides/configure-tables.md) — Declarative table registration and feature flags
- [Port Configuration](../operations/port-configuration.md) — Customizing infrastructure ports
- [Performance Tuning](../operations/performance-tuning.md) — JDBC tuning, batch size impact
- [Deploy HA](../guides/deploy-ha.md) — Node assignment, liveness, and failover
- [Scale Workers](../guides/scale-workers.md) — Scaling worker nodes and `numWorkers` tuning
- [Ingestion Paths](../concepts/ingestion.md) — gRPC vs Kafka ingestion, when to use each
- [Write Transformers](../guides/write-transformers.md) — `recordTransformer` values and Kafka message mapping
- [Schema Evolution](../guides/evolve-schema.md) — `schemaEvolutionEnabled` and online DDL
