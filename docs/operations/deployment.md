# Deployment

[← Back to docs](../README.md)

Production deployment patterns for the CLP Metastore Service.

---

## Deployment Topologies

### Single-Node (Development / Small Scale)

All components run in one JVM. Suitable for development, testing, and small single-table deployments.

```bash
# Start infrastructure (MariaDB, Kafka, MinIO) and coordinator nodes
./docker/start.sh -d

# Or run the JVM directly against the running infrastructure
java -jar grpc-server/target/metalog-grpc-server-0.1.0-SNAPSHOT.jar config/node.yaml
```

In `node.yaml`:

```yaml
worker:
  numWorkers: 4   # Workers run as threads inside the Node JVM
```

### Production: Separate Coordinator and Worker Pools

In production, run coordinators and workers as **separate JVM processes** on dedicated machine pools for fault isolation and independent scaling. Workers access the database and object storage directly — they do not route through the coordinator.

```
┌──────────────────────────────┐   ┌──────────────────────────────┐
│  Coordinator Node(s)         │   │  Worker Pool                 │
│                              │   │                              │
│  java -jar metalog.jar       │   │  java -jar metalog.jar       │
│    node.yaml                 │   │    (standalone worker mode)   │
│                              │   │                              │
│  worker.numWorkers: 0        │   │  DB_HOST=db                  │
│  (coordinator-only)          │   │  WORKER_TABLE_NAME=clp_spark │
└──────────────────────────────┘   └──────────────────────────────┘
          │                                     │
          └──────────────── Database ───────────┘
```

Set `worker.numWorkers: 0` on coordinator nodes to disable in-process workers.

### Production: Separate API Server

The query API server runs as a separate JVM process, connecting to **read replicas** for scalability:

```bash
java -cp "grpc-server/target/metalog-grpc-server-0.1.0-SNAPSHOT.jar:grpc-server/target/lib/*" \
  com.yscope.metalog.query.api.server.vertx.VertxApiServer
```

Configure via environment variables (see [Configuration Reference](../reference/configuration.md#api-server-configuration)):

```bash
API_GRPC_PORT=9090
DB_JDBC_URL=jdbc:mariadb://replica:3306/metalog_metastore
DB_POOL_SIZE=20
```

### Multi-Coordinator HA

For high availability, run multiple coordinator nodes. Each table is owned by exactly one node at a time; nodes take over automatically on failure. See [Deploy HA](../guides/deploy-ha.md) for setup details.

```yaml
node:
  coordinatorHaStrategy: heartbeat  # or "lease"
  heartbeatIntervalSeconds: 30
  deadNodeThresholdSeconds: 180
  reconciliationIntervalSeconds: 60
```

Worst-case failover time with defaults: ~4 minutes (3 min dead threshold + up to 1 min for next reconciliation).

---

## Resource Sizing

### Coordinator Nodes

| Resource | Minimum | Recommended | Notes |
|----------|---------|-------------|-------|
| CPU | 2 cores | 4 cores | Metadata processing is I/O-bound; extra cores help Kafka polling |
| Memory | 512 MB | 2 GB | BatchingWriter queues, InFlightSet, task queue |
| Disk | Minimal | 20 GB | Log files only; no data stored locally |
| Network | 100 Mbps | 1 Gbps | Database writes, Kafka consumption |

One coordinator handles ~15,000 records/sec sustained. For throughput above 50M records/hour, add coordinators (each owns independent tables). See [Performance Tuning](performance-tuning.md) for benchmarks.

### Worker Nodes

| Resource | Minimum | Recommended | Notes |
|----------|---------|-------------|-------|
| CPU | 2 cores | 8 cores | CLP compression is CPU-bound |
| Memory | 1 GB | 4–8 GB | IR files buffered during consolidation |
| Disk | Minimal | SSD scratch | Temporary workspace for CLP binary |
| Network | 1 Gbps | 10 Gbps | Downloads IR files, uploads archives |

Workers are I/O-bound on object storage. Scale horizontally until storage bandwidth is saturated. See [Scale Workers](../guides/scale-workers.md) for guidelines.

### API Server Nodes

| Resource | Minimum | Recommended | Notes |
|----------|---------|-------------|-------|
| CPU | 2 cores | 4 cores | gRPC streaming, cache management |
| Memory | 512 MB | 2 GB | Caffeine query cache (up to 100K entries by default) |
| Network | 1 Gbps | 10 Gbps | Result streaming to clients |

---

## Database Setup

### Requirements

- MariaDB 10.4+ or MySQL 8.0+ (auto-detected at startup)
- Aurora MySQL 3.x, TiDB, and Vitess are also supported

Schema creation is **automatic** — the service creates all tables, indexes, and partitions on startup. No manual DDL required.

### JDBC Settings (Critical)

Add these parameters to the JDBC URL to avoid a 300x throughput regression:

```
jdbc:mariadb://host:3306/metalog_metastore
  ?rewriteBatchedStatements=true
  &cachePrepStmts=true
  &prepStmtCacheSize=250
  &prepStmtCacheSqlLimit=2048
```

Without `rewriteBatchedStatements=true`, batch inserts degrade from ~15,000 rec/sec to ~50 rec/sec. This is set automatically by `DatabaseConfig` — verify it is not overridden in custom JDBC URLs.

### Replication

For the coordinator/ingestion path, point to the **primary** (writes required). For the API server, point to a **read replica** to offload query traffic.

`_task_queue` can optionally be excluded from semi-sync replication for very high task churn (see [Performance Tuning: Task Queue](performance-tuning.md#replication-considerations)), but this is not recommended for most deployments.

### Partition Maintenance

Daily partitions are created automatically by the Partition Maintenance thread (runs hourly, creates `partition.lookahead.days` days ahead). No manual partition management is needed.

---

## Object Storage

### MinIO (Development / Self-Hosted)

```yaml
storage:
  defaultBackend: minio
  backends:
    minio:
      endpoint: http://minio:9000
      accessKey: minioadmin
      secretKey: minioadmin
      forcePathStyle: true  # required for MinIO
```

### AWS S3

```yaml
storage:
  defaultBackend: s3
  backends:
    s3:
      endpoint: https://s3.amazonaws.com
      accessKey: AKIAIOSFODNN7EXAMPLE
      secretKey: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
      forcePathStyle: false
```

Ensure both coordinator and worker nodes have access to object storage. Workers require read access to IR files and read/write access for archives.

---

## Container Deployment

### Docker Compose

```yaml
services:
  coordinator:
    image: metalog:latest
    command: ["/etc/clp/node.yaml"]
    environment:
      DB_HOST: mariadb
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
    volumes:
      - ./config/node.yaml:/etc/clp/node.yaml:ro
    ports:
      - "8081:8081"   # health checks

  api-server:
    image: metalog:latest
    entrypoint: ["java", "-cp", "/app/metalog.jar:/app/lib/*",
                 "com.yscope.metalog.query.api.server.vertx.VertxApiServer"]
    environment:
      API_GRPC_PORT: "9090"
      DB_JDBC_URL: "jdbc:mariadb://mariadb:3306/metalog_metastore"
    ports:
      - "9090:9090"

  worker:
    image: metalog:latest
    entrypoint: ["java", "-cp", "/app/metalog.jar:/app/lib/*",
                 "com.yscope.metalog.worker.Worker"]
    environment:
      DB_HOST: mariadb
      WORKER_TABLE_NAME: clp_spark
    deploy:
      replicas: 4
```

Scale workers independently:

```bash
docker compose -f docker/docker-compose.yml up -d --scale worker=8
```

### Kubernetes

Use the health check endpoints for liveness and readiness probes:

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 8081
  initialDelaySeconds: 10
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /health/ready
    port: 8081
  initialDelaySeconds: 5
  periodSeconds: 5
```

Set `node.nodeIdEnvVar: HOSTNAME` to use the pod name as the node ID — this ensures each pod gets a unique identity in `_table_assignment` and `_node_registry`:

```yaml
env:
  - name: HOSTNAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
```

---

## Rolling Updates

The service supports zero-downtime rolling updates because:

1. **Coordinators** use the HA liveness mechanism — a node that goes silent becomes claimable after the dead threshold (default 3 min). The replacement pod starts, claims the table, and resumes from the last committed Kafka offset.

2. **Workers** are stateless — they can be stopped and restarted at any time. Tasks that were in-progress are automatically reclaimed by the Planner after the task timeout (default 10 min, configurable via `coordinator.task.timeout.ms`).

3. **API servers** are fully stateless — rolling restarts are seamless.

### Recommended Rolling Procedure

1. **Coordinators**: Deploy one pod at a time. Wait for the replacement to become ready (`/health/ready` 200) before terminating the old one. With heartbeat HA, the old node's heartbeat goes stale and the table is claimed by the replacement within `deadNodeThresholdSeconds + reconciliationIntervalSeconds` (~4 min with defaults).

2. **Workers**: Scale up new workers before scaling down old ones. No quorum or sequencing required.

3. **API servers**: Standard rolling update — no state or connections to drain.

To reduce the failover window, lower `deadNodeThresholdSeconds` (e.g., 60s) while keeping the 3:1 threshold/interval safety ratio (`heartbeatIntervalSeconds: 20`, `deadNodeThresholdSeconds: 60`).

---

## See Also

- [Quickstart](../getting-started/quickstart.md) — Local development setup
- [Architecture Overview](../concepts/overview.md) — System overview, startup/shutdown sequences
- [Deploy HA](../guides/deploy-ha.md) — High availability configuration
- [Configuration Reference](../reference/configuration.md) — Full configuration reference
- [Performance Tuning](performance-tuning.md) — Benchmarks and optimization
- [Monitoring](monitoring.md) — Health checks and alerting
