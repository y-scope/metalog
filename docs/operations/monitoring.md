# Monitoring

[← Back to docs](../README.md)

Health checks, metrics, and alerting for production CLP Metastore deployments.

---

## Health Checks

The coordinator node exposes HTTP health endpoints on port `8081` (configurable via `node.health.port` in `node.yaml`). The API server does not currently expose a health endpoint — monitor it via gRPC connectivity instead.

### Endpoints

| Endpoint | Purpose | Healthy response |
|----------|---------|-----------------|
| `GET /health/live` | Liveness: is the process running? | `200 OK` |
| `GET /health/ready` | Readiness: is the service ready to accept work? | `200 READY` |

The readiness endpoint returns `503 NOT READY` when the node has not finished startup. Both return plain text.

### Enable health checks

```yaml
node:
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

## Key Metrics

The service does not currently expose a Prometheus scrape endpoint. Use the database visibility queries below as your primary observability layer — everything is in MariaDB/MySQL.

### Ingestion throughput

```sql
-- Files registered in the last minute, by table
SELECT table_name,
       COUNT(*) AS files_last_minute,
       COUNT(*) * 60 AS projected_hourly_rate
FROM clp_spark
WHERE created_at >= UNIX_TIMESTAMP() - 60
GROUP BY table_name;
```

Expected: matches your producer throughput. A sudden drop to 0 indicates an ingestion or coordinator failure.

### Coordinator progress

The `_table_assignment` table tracks the last time each coordinator made progress (database writes). The watchdog goroutine also monitors this in-memory, but this SQL view is useful for external monitoring:

```sql
-- Table assignments and staleness
SELECT table_name,
       node_id,
       FROM_UNIXTIME(last_progress_at) AS last_progress,
       UNIX_TIMESTAMP() - last_progress_at AS seconds_stale
FROM _table_assignment
WHERE node_id IS NOT NULL;
```

Alert if `seconds_stale > 100` (2× the default watchdog stall threshold of 50s).

### Task queue depth

```sql
-- Pending tasks by table (high depth = workers falling behind)
SELECT table_name,
       state,
       COUNT(*) AS count,
       MIN(FROM_UNIXTIME(created_at)) AS oldest
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
  --group metalog-coordinator-clp_spark
```

Consumer group IDs follow the pattern `metalog-coordinator-{table_name}`. Lag > 0 during steady state is normal (batch window); lag growing continuously indicates the coordinator is not keeping up.

### Node liveness (heartbeat mode)

```sql
-- All nodes and heartbeat freshness
SELECT node_id,
       FROM_UNIXTIME(last_heartbeat_at) AS last_heartbeat,
       UNIX_TIMESTAMP() - last_heartbeat_at AS seconds_stale
FROM _node_registry;
```

Alert if `seconds_stale > deadNodeThresholdSeconds` (default 180) — this is when peers begin claiming the node's tables.

---

## Alerting Thresholds

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| Coordinator stalled | `last_progress_at < NOW() - 100s` | Warning | Check coordinator logs; watchdog restarts stalled goroutines |
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
