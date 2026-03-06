# Configuration Reference

[← Back to docs](../README.md)

Configuration is loaded from `node.yaml` (node-level settings). Per-table configuration and assignments are stored in the database.

---

## node.yaml — Node Settings

Node-level shared resources: identity, database connection, storage backends, and worker pool.

```yaml
node:
  name: coordinator-node-1       # Display name (used in logs)
  nodeIdEnvVar: HOSTNAME         # Env var whose value becomes node_id in _table_assignment
                                 # Falls back to os.Hostname()

  database:
    host: localhost
    port: 3306                   # Default: 3306
    database: metalog_metastore
    user: root
    password: ""
    poolSize: 20                 # Max open connections
    poolMinIdle: 5               # Min idle connections

  storage:
    defaultBackend: minio
    irBucket: logs               # Default bucket for IR files
    archiveBucket: logs          # Default bucket for archive files
    clpBinaryPath: /usr/bin/clp-s  # Path to the clp-s binary for consolidation
    clpProcessTimeoutSeconds: 300  # Default: 300 (5 minutes)
    backends:
      minio:
        endpoint: http://localhost:9000
        accessKey: minioadmin
        secretKey: minioadmin
        region: ""               # AWS region (optional, for S3)
        forcePathStyle: true     # Required for MinIO

  health:
    enabled: true
    port: 8081                   # Default: 8081

  grpc:
    enabled: true
    port: 9090                   # Default: 9090

  reconciliationIntervalSeconds: 60  # Default: 60. How often to reconcile table assignments.

  # HA settings
  coordinatorHaStrategy: heartbeat   # "heartbeat" (default) or "lease"
  heartbeatIntervalSeconds: 30       # Default: 30. Heartbeat mode: liveness write interval.
  deadNodeThresholdSeconds: 180      # Default: 180. Heartbeat mode: seconds before node is declared dead.
  leaseTtlSeconds: 180               # Default: 180. Lease mode: lease duration.
  leaseRenewalIntervalSeconds: 30    # Default: 30. Lease mode: renewal interval (must be < leaseTtlSeconds).

# Tables to manage on this node.
tables:
  - name: clp_spark
    displayName: Spark Logs       # Human-readable name (optional)
    kafka:
      topic: clp-metadata-spark
      bootstrapServers: kafka:29092
      recordTransformer: ""       # Transformer name (empty = default)

# Shared worker pool (claims tasks from all tables).
worker:
  numWorkers: 4                  # Default: 4. Set to 0 for coordinator-only node.
  # database:                    # Optional separate DB pool for workers
  #   host: read-replica
  #   port: 3306
  #   database: metalog_metastore
  #   user: worker
  #   password: ""
```

### Configuration Reference Table

| Property | Default | Description |
|----------|---------|-------------|
| `node.name` | — | Display name for logs |
| `node.nodeIdEnvVar` | `HOSTNAME` | Env var for node identity |
| `node.database.host` | — | **Required.** Database hostname |
| `node.database.port` | `3306` | Database port (1-65535) |
| `node.database.database` | — | Database name |
| `node.database.user` | — | Database user |
| `node.database.password` | — | Database password |
| `node.database.poolSize` | `20` | Max open connections |
| `node.database.poolMinIdle` | `5` | Min idle connections |
| `node.storage.defaultBackend` | — | Default storage backend name |
| `node.storage.irBucket` | — | Default IR file bucket |
| `node.storage.archiveBucket` | — | Default archive file bucket |
| `node.storage.clpBinaryPath` | — | Path to clp-s binary |
| `node.storage.clpProcessTimeoutSeconds` | `300` | CLP process timeout |
| `node.health.enabled` | `false` | Enable HTTP health endpoint |
| `node.health.port` | `8081` | Health endpoint port |
| `node.grpc.enabled` | `false` | Enable gRPC server |
| `node.grpc.port` | `9090` | gRPC server port |
| `node.reconciliationIntervalSeconds` | `60` | Table assignment reconciliation interval |
| `node.coordinatorHaStrategy` | `heartbeat` | HA mode: `heartbeat` or `lease` |
| `node.heartbeatIntervalSeconds` | `30` | Heartbeat mode: liveness write interval |
| `node.deadNodeThresholdSeconds` | `180` | Heartbeat mode: seconds before node is dead |
| `node.leaseTtlSeconds` | `180` | Lease mode: lease duration in seconds |
| `node.leaseRenewalIntervalSeconds` | `30` | Lease mode: renewal interval (must be < TTL) |
| `worker.numWorkers` | `4` | Worker goroutines. `0` = coordinator-only node |
| `worker.database.*` | _(inherits node.database)_ | Optional separate DB pool for workers |
| `tables[].name` | — | **Required.** Table name |
| `tables[].displayName` | — | Human-readable name |
| `tables[].kafka.topic` | — | Kafka topic for this table |
| `tables[].kafka.bootstrapServers` | — | Kafka bootstrap servers |
| `tables[].kafka.recordTransformer` | `""` (default) | Transformer name |

### Internal Constants

These are not configurable via YAML but are defined in `internal/config/timeouts.go`:

| Constant | Value | Description |
|----------|-------|-------------|
| `DefaultBatchFlushInterval` | 500ms | Batching writer flush interval |
| `DefaultBatchSize` | 5000 | UPSERT batch size |
| `DefaultTaskClaimBatchSize` | 10 | Tasks claimed per worker poll |
| `DefaultTaskStaleTimeout` | 5m | Processing task reclaim timeout |
| `DefaultTaskCleanupAge` | 24h | Completed/failed task cleanup age |
| `DefaultWorkerPollInterval` | 2s | Worker task poll interval |
| `DefaultWorkerBackoffMax` | 30s | Max backoff when no tasks available |
| `DefaultPlannerInterval` | 60s | Consolidation planner run interval |
| `DefaultQueryLimit` | 1000 | Default split query page size |
| `DefaultDeadlockMaxRetries` | 10 | Max retries on SQL deadlock |

---

## Schema Auto-Creation

All schema setup is automatic. On startup, the node creates the database, initializes the `_table*` registry and `_task_queue`, and creates per-table metadata tables. See [Architecture Overview](../concepts/overview.md) for the full startup sequence.

---

## See Also

- [Architecture Overview](../concepts/overview.md) — System overview and startup sequence
- [Quickstart](../getting-started/quickstart.md) — Setup and first run
- [Configure Tables](../guides/configure-tables.md) — Declarative table registration and feature flags
- [Port Configuration](../operations/port-configuration.md) — Customizing infrastructure ports
- [Performance Tuning](../operations/performance-tuning.md) — DSN tuning, batch size impact
- [Deploy HA](../guides/deploy-ha.md) — Node assignment, liveness, and failover
- [Scale Workers](../guides/scale-workers.md) — Scaling worker nodes and `numWorkers` tuning
- [Ingestion Paths](../concepts/ingestion.md) — gRPC vs Kafka ingestion, when to use each
- [Write Transformers](../guides/write-transformers.md) — Transformer interface and registration
- [Schema Evolution](../guides/evolve-schema.md) — Online DDL for new fields
