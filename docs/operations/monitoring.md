# Monitoring

[← Back to docs](../README.md)

Health checks, metrics, and alerting for production CLP Metastore deployments.

---

## Health Checks

Any node (coordinator, worker, or API-only) exposes HTTP health endpoints on port `8081` (configurable via `health.port` in `node.yaml`) when `health.enabled: true`.

### Endpoints

| Endpoint | Purpose | Healthy response |
|----------|---------|-----------------|
| `GET /health` | Liveness (alias) | `200 OK` |
| `GET /health/live` | Liveness: is the process running? | `200 OK` |
| `GET /ready` | Readiness (alias) | `200 READY` |
| `GET /health/ready` | Readiness: is the service ready to accept work? | `200 READY` |

The liveness endpoints (`/health`, `/health/live`) always return `200 OK` — they confirm the process is running.

The readiness endpoints (`/ready`, `/health/ready`) return status `200` with body `READY` when the node is ready to serve traffic, or status `503` with body `NOT READY` otherwise. Readiness transitions to `NOT READY` during graceful shutdown (the node sets readiness to `false` before stopping subsystems), allowing load balancers to drain traffic before the process exits. All responses are plain text.

### Enable health checks

```yaml
health:
  enabled: true
  port: 8081
```

### Kubernetes probes

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 8081
  initialDelaySeconds: 10
  periodSeconds: 10
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /health/ready
    port: 8081
  initialDelaySeconds: 5
  periodSeconds: 5
  failureThreshold: 3
```

### Docker / curl

```bash
# Check liveness
curl -s http://coordinator:8081/health/live
# OK

# Check readiness
curl -s http://coordinator:8081/health/ready
# READY
```

---

## Prometheus Metrics

When telemetry is enabled (`telemetry.enabled: true` in `node.yaml`), the node exposes a Prometheus-compatible `/metrics` endpoint on the health port (default 8081):

```yaml
telemetry:
  enabled: true
  exporter: prometheus  # default
```

```bash
curl http://localhost:8081/metrics
```

Key metrics exposed:

| Metric | Type | Description |
|--------|------|-------------|
| `metalog_ingestion_records_submitted` | Counter | Records submitted to BatchingWriter (by table) |
| `metalog_ingestion_records_flushed` | Counter | Records flushed to DB (by table, status) |
| `metalog_ingestion_flush_duration_seconds` | Histogram | Batch flush latency (by table) |
| `metalog_ingestion_submit_rejected` | Counter | Records rejected (by table, reason) |
| `metalog_kafka_messages_consumed` | Counter | Kafka messages consumed (by topic) |
| `metalog_kafka_messages_failed` | Counter | Kafka message failures (by topic, reason) |

For custom exporters (Datadog, M3, etc.), see [Extending — Telemetry Exporters](../guides/extending.md#5-telemetry-exporters-metrics-backend).

## Database Visibility Queries

The database is supplementary to Prometheus metrics — useful for ad-hoc investigation and for environments where Prometheus is not yet configured.

### Ingestion throughput

```sql
-- Files ingested in the last minute
SELECT COUNT(*) AS files_last_minute,
       COUNT(*) * 60 AS projected_hourly_rate
FROM clp_spark
WHERE min_timestamp >= (UNIX_TIMESTAMP() - 60) * 1000000000;    -- last 60 seconds, in epoch nanos
```

Expected: matches your producer throughput. A sudden drop to 0 indicates an ingestion or coordinator failure.

### Coordinator progress

Stall detection is currently **in-memory only** — each coordinator unit has a `ProgressTracker` that is checked during the reconciliation loop (every 60s). Stalled coordinators are automatically restarted. There is no SQL-queryable progress signal yet (`_table_assignment.last_progress_at` exists in the schema but is not written by the coordinator).

To monitor coordinator health externally, check:

```sql
-- Table assignments and ownership
SELECT table_name, node_id,
       FROM_UNIXTIME(node_assigned_at DIV 1000000000) AS assigned_at
FROM _table_assignment
WHERE node_id IS NOT NULL;
```

For stall detection, check coordinator logs for `"coordinator stalled, restarting"` messages.

### Task queue depth

```sql
-- Pending tasks by table (high depth = workers falling behind)
SELECT table_name,
       state,
       COUNT(*) AS count,
       MIN(FROM_UNIXTIME(created_at DIV 1000000000)) AS oldest
FROM _task_queue
GROUP BY table_name, state
ORDER BY table_name, state;
```

Healthy steady-state: `pending` count near 0 (workers consuming as fast as coordinator produces). A growing `pending` count means workers are undersized — see [Scale Workers](../guides/scale-workers.md).

### Dead-letter tasks

```sql
-- Tasks that exceeded max retries
SELECT * FROM _task_queue WHERE state = 'dead_letter';
```

Any `dead_letter` rows require manual investigation. They indicate repeated worker failures (storage errors, corrupt IR files, CLP binary crashes).

### Kafka consumer lag

Monitor via standard Kafka tooling:

```bash
# Check consumer group lag (replace <table_id> with the UUID from _table.table_id)
kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group clp-coordinator-clp_spark-<table_id>
```

Consumer group IDs follow the pattern `clp-coordinator-{table_name}-{table_id}`, where `table_id` is the UUID from `_table.table_id`. The UUID suffix ensures uniqueness across environments sharing the same Kafka cluster. Lag > 0 during steady state is normal (batch window); lag growing continuously indicates the coordinator is not keeping up.

### Node liveness (heartbeat mode)

```sql
-- All nodes and heartbeat freshness
SELECT node_id,
       FROM_UNIXTIME(last_heartbeat_at DIV 1000000000) AS last_heartbeat,
       (UNIX_TIMESTAMP() * 1000000000 - last_heartbeat_at) DIV 1000000000 AS seconds_stale
FROM _node_registry;
```

Alert if `seconds_stale > deadNodeThresholdSeconds` (default 180) — this is when peers begin claiming the node's tables.

---

## Alerting Thresholds

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| Coordinator stalled | Log message: `"coordinator stalled, restarting"` | Warning | Check coordinator logs; node auto-restarts stalled goroutines (in-memory detection, not SQL-queryable) |
| Node dead | `last_heartbeat_at < NOW() - 180s` (heartbeat mode) | Critical | HA failover should be in progress; verify reconciliation is running |
| Task queue growing | `pending count increasing over 10 min` | Warning | Scale up workers; check for storage errors |
| Dead-letter tasks | `dead_letter count > 0` | Warning | Investigate worker logs; may indicate corrupt files or storage issues |
| No recent ingestion | `files created in last 5 min = 0` (when traffic expected) | Critical | Check producer, Kafka consumer lag, coordinator logs |
| Health endpoint down | `/health/live` returns non-200 | Critical | Process crash or OOM; restart pod |

---

## Visibility Queries

For HA-specific monitoring queries (coordinator assignment, stalled coordinator detection, node liveness), see [Deploy HA: Monitoring](../guides/deploy-ha.md#monitoring).

### Index monitoring

```sql
-- Check which indexes are being used (MariaDB performance_schema)
SELECT OBJECT_NAME, INDEX_NAME, COUNT_STAR, COUNT_READ, COUNT_WRITE
FROM performance_schema.table_io_waits_summary_by_index_usage
WHERE OBJECT_SCHEMA = 'metalog_metastore'
ORDER BY COUNT_STAR DESC;
```

Indexes with `COUNT_READ = 0` after 30 days of production traffic are candidates for removal.

### Slow queries

```sql
-- Queries averaging > 1 second
SELECT DIGEST_TEXT,
       COUNT_STAR,
       AVG_TIMER_WAIT / 1000000000 AS avg_ms
FROM performance_schema.events_statements_summary_by_digest
WHERE AVG_TIMER_WAIT > 1000000000000
ORDER BY COUNT_STAR DESC
LIMIT 10;
```

Persistent slow queries indicate missing indexes — see [Performance Tuning: Dimension Index Strategy](performance-tuning.md#dimension-index-strategy).

---

## See Also

- [Deploy HA](../guides/deploy-ha.md) — HA monitoring and visibility queries
- [Performance Tuning](performance-tuning.md) — Benchmarks and index monitoring
- [Deployment](deployment.md) — Production deployment patterns
- [Troubleshooting](troubleshooting.md) — Common issues and fixes
