# Configuration Reference

[← Back to docs](../README.md)

Configuration is loaded from `node.yaml` (node-level settings). Per-table configuration and assignments are stored in the database.

---

## node.yaml — Node Settings

Settings are organized by role: shared resources (`database`, `storage`), network (`grpc`, `health`), coordinator logic (`coordinator`), and worker pool (`worker`). Per-table configuration (Kafka routing, feature flags) is managed via the admin gRPC API and stored in the database.

```yaml
database:
  # Primary (RW). Required for coordinator and worker roles.
  primary:
    host: localhost
    port: 3306                     # Required
    database: metalog_metastore
    user: root
    password: ""
    poolSize: 5                    # Max open connections (default: 5)
    poolMinIdle: 2                 # Min idle connections (default: 2)

  # Replica (RO, optional). Query and metadata services use this pool when
  # configured. Falls back to primary if omitted. For replica-only deployments
  # (API server), omit primary entirely and configure only replica.
  # replica:
  #   host: replica-db
  #   port: 3306
  #   database: metalog_metastore
  #   user: reader
  #   password: secret
  #   poolSize: 10
  #   poolMinIdle: 2

storage:
  defaultBackend: minio
  backends:
    minio:
      endpoint: http://localhost:9000
      accessKey: minioadmin
      secretKey: minioadmin
      region: ""                 # AWS region (optional, for S3)
      bucket: logs               # Storage bucket for this backend
      forcePathStyle: true       # Required for MinIO

health:
  enabled: true
  port: 8081                     # Default: 8081

grpc:
  port: 9090                     # Default: 9090
  ingestion: true                # Requires database.primary
  admin: true                    # Requires database.primary
  query: true                    # Uses database.replica, falls back to primary
  metadata: true                 # Uses database.replica, falls back to primary

coordinator:
  enabled: true                      # Enable coordinator subsystem
  nodeIdEnvVar: HOSTNAME         # Env var whose value becomes node_id in _table_assignment
                                 # Falls back to os.Hostname()
  reconciliationIntervalSeconds: 60  # Default: 60. How often to reconcile table assignments.

  # HA settings
  haStrategy: heartbeat              # "heartbeat" (default) or "lease"
  heartbeatIntervalSeconds: 30       # Default: 30. Heartbeat mode: liveness write interval.
  deadNodeThresholdSeconds: 180      # Default: 180. Heartbeat mode: seconds before node is declared dead.
  leaseTtlSeconds: 180               # Default: 180. Lease mode: lease duration.
  leaseRenewalIntervalSeconds: 30    # Default: 30. Lease mode: renewal interval (must be < leaseTtlSeconds).

# Tables are registered via admin gRPC API (AdminService/RegisterTable).
# The coordinator discovers assigned tables from the DB on startup and via
# periodic reconciliation. See docs/guides/configure-tables.md.

# Shared worker pool (claims tasks from all tables).
worker:
  concurrency: 4                 # 0 = workers disabled
  # clpBinaryPath: /usr/bin/clp-s  # Auto-resolved from $PATH if omitted
  # clpProcessTimeoutSeconds: 300  # Default: 300 (5 minutes)
```

### Configuration Reference Table

| Property | Default | Description |
|----------|---------|-------------|
| `database.primary.host` | — | Primary (RW) database hostname. Required for coordinator/worker roles |
| `database.primary.port` | — | **Required.** Primary port (1-65535) |
| `database.primary.database` | — | Primary database name |
| `database.primary.user` | — | Primary database user |
| `database.primary.password` | — | Primary database password |
| `database.primary.poolSize` | `5` | Primary max open connections |
| `database.primary.poolMinIdle` | `2` | Primary min idle connections |
| `database.replica.host` | — | Replica (RO) database hostname. Optional; falls back to primary |
| `database.replica.port` | — | **Required.** Replica port (1-65535) |
| `database.replica.database` | — | Replica database name |
| `database.replica.user` | — | Replica database user |
| `database.replica.password` | — | Replica database password |
| `database.replica.poolSize` | `5` | Replica max open connections |
| `database.replica.poolMinIdle` | `2` | Replica min idle connections |
| `storage.defaultBackend` | — | Default storage backend name |
| `health.enabled` | `false` | Enable HTTP health endpoint |
| `health.port` | `8081` | Health endpoint port |
| `grpc.port` | `9090` | gRPC server port (server starts if any service is enabled) |
| `grpc.ingestion` | `false` | Enable ingestion gRPC service (requires `database.primary`) |
| `grpc.admin` | `false` | Enable admin gRPC service (requires `database.primary`) |
| `grpc.query` | `false` | Enable query gRPC service (uses `database.replica`, falls back to primary) |
| `grpc.metadata` | `false` | Enable metadata gRPC service (uses `database.replica`, falls back to primary) |
| `coordinator.enabled` | `false` | Enable coordinator subsystem |
| `coordinator.nodeIdEnvVar` | `HOSTNAME` | Env var for node identity |
| `coordinator.reconciliationIntervalSeconds` | `60` | Table assignment reconciliation interval |
| `coordinator.haStrategy` | `heartbeat` | HA mode: `heartbeat` or `lease` |
| `coordinator.heartbeatIntervalSeconds` | `30` | Heartbeat mode: liveness write interval |
| `coordinator.deadNodeThresholdSeconds` | `180` | Heartbeat mode: seconds before node is dead |
| `coordinator.leaseTtlSeconds` | `180` | Lease mode: lease duration in seconds |
| `coordinator.leaseRenewalIntervalSeconds` | `30` | Lease mode: renewal interval (must be < TTL) |
| `worker.concurrency` | `0` (disabled) | Concurrent task goroutines |
| `worker.clpBinaryPath` | `$PATH` lookup | Path to clp-s binary. Auto-resolved from `$PATH` if omitted |
| `worker.clpProcessTimeoutSeconds` | `300` | CLP process timeout |

### Internal Constants

These are not configurable via YAML. Most are defined in `config/timeouts.go` unless noted otherwise:

| Constant | Value | Location | Description |
|----------|-------|----------|-------------|
| `DefaultBatchFlushInterval` | 1s | `config/timeouts.go` | Batching writer flush interval |
| `DefaultBatchSize` | 5000 | `config/timeouts.go` | UPSERT batch size |
| `DefaultTaskClaimBatchSize` | 10 | `config/timeouts.go` | Tasks claimed per worker poll |
| `DefaultTaskStaleTimeout` | 5m | `config/timeouts.go` | Processing task reclaim timeout |
| `DefaultTaskCleanupAge` | 24h | `config/timeouts.go` | Completed/failed task cleanup age |
| `DefaultWorkerPollInterval` | 2s | `config/timeouts.go` | Worker task poll interval |
| `DefaultWorkerBackoffMax` | 30s | `config/timeouts.go` | Max backoff when no tasks available |
| `DefaultPlannerInterval` | 60s | `config/timeouts.go` | Consolidation planner run interval |
| `defaultScanInterval` | 60s | `coordinator/retention/scanner.go` | Retention strategy scan interval |
| `defaultDeleteRate` | 500/sec | `coordinator/retention/scanner.go` | Max storage deletions per second |
| `DefaultQueryLimit` | 1000 | `config/timeouts.go` | Default total result limit for split queries (0 = unlimited) |
| `DefaultQueryPageSize` | 1000 | `config/timeouts.go` | Internal SQL LIMIT per page for streaming queries |
| `DefaultDeadlockMaxRetries` | 10 | `config/timeouts.go` | Max retries on SQL deadlock |
| `DefaultDeadlockMinBackoff` | 1ms | `config/timeouts.go` | Min jitter on deadlock retry |
| `DefaultDeadlockMaxBackoff` | 50ms | `config/timeouts.go` | Max jitter on deadlock retry |
| `DefaultProgressStallTimeout` | 5m | `config/timeouts.go` | Coordinator progress stall detection |

---

## Schema Auto-Creation

All schema setup is automatic. On startup, the node creates the database, initializes the `_table*` registry and `_task_queue`, and creates per-table metadata tables. See [Architecture Overview](../concepts/overview.md) for the full startup sequence.

---

## See Also

- [Architecture Overview](../concepts/overview.md) — System overview and startup sequence
- [Quickstart](../getting-started/quickstart.md) — Setup and first run
- [Configure Tables](../guides/configure-tables.md) — Table registration and feature flags
- [Port Configuration](../operations/port-configuration.md) — Customizing infrastructure ports
- [Performance Tuning](../operations/performance-tuning.md) — DSN tuning, batch size impact
- [Deploy HA](../guides/deploy-ha.md) — Node assignment, liveness, and failover
- [Scale Workers](../guides/scale-workers.md) — Scaling worker nodes and `concurrency` tuning
- [Ingestion Paths](../concepts/ingestion.md) — gRPC vs Kafka ingestion, when to use each
- [Write Transformers](../guides/write-transformers.md) — Transformer interface and registration
- [Schema Evolution](../guides/evolve-schema.md) — Online DDL for new fields
