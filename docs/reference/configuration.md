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
    poolMinIdle: 2                 # Max idle connections kept open (default: 2; maps to SetMaxIdleConns)

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
      type: s3                     # Backend type: "s3", "fs", "http"
      endpoint: http://localhost:9000
      accessKey: minioadmin
      secretKey: minioadmin
      region: ""                 # AWS region (optional, for S3)
      bucket: logs               # Default bucket (used by all backend types)
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

logging:
  failureLogIntervalSeconds: 60    # Default: 60. Throttle repeated failure log messages.

coordinator:
  enabled: true                      # Enable coordinator subsystem
  name: ""                           # Human-readable name (default: hostname)
  nodeIdEnvVar: HOSTNAME         # Env var whose value becomes node_id in _table_assignment
                                 # Falls back to os.Hostname()
  tableCompression: ""               # "lz4", "page_compressed", "none" (default: auto-detected)
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
| `database.primary.poolMinIdle` | `2` | Primary max idle connections (maps to Go `SetMaxIdleConns`) |
| `database.replica.host` | — | Replica (RO) database hostname. Optional; falls back to primary |
| `database.replica.port` | — | **Required.** Replica port (1-65535) |
| `database.replica.database` | — | Replica database name |
| `database.replica.user` | — | Replica database user |
| `database.replica.password` | — | Replica database password |
| `database.replica.poolSize` | `5` | Replica max open connections |
| `database.replica.poolMinIdle` | `2` | Replica max idle connections (maps to Go `SetMaxIdleConns`) |
| `storage.defaultBackend` | `local` (auto) | Default storage backend name. When omitted, auto-provisions an `fs`-type backend named `"local"` with an empty `bucket` (the `fs` base directory). Configure `bucket` to a real path for production use. |
| `health.enabled` | `false` | Enable HTTP health endpoint |
| `health.port` | `8081` | Health endpoint port |
| `grpc.port` | `9090` | gRPC server port (server starts if any service is enabled) |
| `grpc.ingestion` | `false` | Enable ingestion gRPC service (requires `database.primary`) |
| `grpc.admin` | `false` | Enable admin gRPC service (requires `database.primary`) |
| `grpc.query` | `false` | Enable query gRPC service (uses `database.replica`, falls back to primary) |
| `grpc.metadata` | `false` | Enable metadata gRPC service (uses `database.replica`, falls back to primary) |
| `logging.failureLogIntervalSeconds` | `60` | Throttle interval for repeated failure log messages. First failure always logged immediately; subsequent repeat at this interval. |
| `coordinator.enabled` | `false` | Enable coordinator subsystem |
| `coordinator.name` | (hostname) | Human-readable coordinator name |
| `coordinator.nodeIdEnvVar` | `HOSTNAME` | Env var for node identity |
| `coordinator.tableCompression` | auto-detected | CREATE TABLE compression: `lz4`, `page_compressed`, or `none`. When empty, auto-detected from database type. |
| `coordinator.reconciliationIntervalSeconds` | `60` | Table assignment reconciliation interval |
| `coordinator.haStrategy` | `heartbeat` | HA mode: `heartbeat` or `lease` |
| `coordinator.heartbeatIntervalSeconds` | `30` | Heartbeat mode: liveness write interval |
| `coordinator.deadNodeThresholdSeconds` | `180` | Heartbeat mode: seconds before node is dead |
| `coordinator.leaseTtlSeconds` | `180` | Lease mode: lease duration in seconds |
| `coordinator.leaseRenewalIntervalSeconds` | `30` | Lease mode: renewal interval (must be < TTL) |
| `worker.concurrency` | `0` (disabled) | Concurrent task goroutines |
| `worker.clpBinaryPath` | `$PATH` lookup | Path to clp-s binary. Auto-resolved from `$PATH` if omitted |
| `worker.clpProcessTimeoutSeconds` | `300` | CLP process timeout |

### Storage Backend Fields

Fields are split into two groups: **factory fields** are read when the backend is created (via `ToMap()`), and **routing fields** are stored in config but passed separately at call time.

**Factory fields** — read by `CreateBackend()` during startup:

| Field | s3 | fs | http | Description |
|-------|:--:|:--:|:----:|-------------|
| `type` | ✓ | ✓ | ✓ | Backend type: `"s3"`, `"fs"`, `"http"` |
| `endpoint` | ✓ | — | — | S3-compatible endpoint URL |
| `accessKey` / `secretKey` | ✓ | — | — | S3 credentials |
| `region` | ✓ | — | — | AWS region |
| `forcePathStyle` | ✓ | — | — | Use path-style S3 URLs (required for MinIO) |
| `baseUrl` | — | — | ✓ | Base URL for HTTP backend |

> The `fs` factory reads no config fields — `NewFilesystemBackend()` takes no arguments.

**Routing fields** — read from config, passed at call time to `Get`/`Put`/`Delete`/`Exists`:

| Field | Description |
|-------|-------------|
| `bucket` | S3 bucket name, `fs` base directory path, or HTTP URL path segment (URL-escaped, between `baseUrl` and key). Used by all backend types. |

> `basePath` is declared in the config struct and included in `ToMap()` output, but no current backend factory reads it from the config map.

### Environment Variable Overrides

Deployment-sensitive config fields can be overridden by environment variables. This is useful for injecting secrets and ports in Docker/Kubernetes without modifying the YAML file. Env overrides are applied automatically after YAML loading — set the variable and it takes effect.

Only non-empty env var values trigger an override. Unset or empty variables are ignored (YAML values are preserved).

| Env Variable | Config Property | Type |
|-------------|-----------------|------|
| `DB_PRIMARY_HOST` | `database.primary.host` | string |
| `DB_PRIMARY_PORT` | `database.primary.port` | int |
| `DB_PRIMARY_DATABASE` | `database.primary.database` | string |
| `DB_PRIMARY_USER` | `database.primary.user` | string |
| `DB_PRIMARY_PASSWORD` | `database.primary.password` | string |
| `DB_REPLICA_HOST` | `database.replica.host` | string |
| `DB_REPLICA_PORT` | `database.replica.port` | int |
| `DB_REPLICA_DATABASE` | `database.replica.database` | string |
| `DB_REPLICA_USER` | `database.replica.user` | string |
| `DB_REPLICA_PASSWORD` | `database.replica.password` | string |
| `GRPC_PORT` | `grpc.port` | int |
| `HEALTH_PORT` | `health.port` | int |
| `COORDINATOR_NAME` | `coordinator.name` | string |

The mechanism uses `env` and `envprefix` struct tags on Go config types (see `config/envoverride.go`). Storage backend fields are not overridable via env vars — use YAML for storage configuration.

### YAML Variable Substitution (utility)

The `config.ExpandEnvVars()` utility (defined in `config/configmap.go`) provides shell-style `${VAR}` and `${VAR:-default}` substitution inside YAML values. Unlike the env override mechanism above, this is **not** applied automatically — it is an exported utility for callers that want to pre-process YAML bytes before unmarshalling.

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
