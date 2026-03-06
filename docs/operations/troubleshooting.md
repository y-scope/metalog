# Troubleshooting

[← Back to docs](../README.md)

Common issues, failure modes, and fixes for the CLP Metastore Service.

---

## Startup Issues

### Database connection failure

**Symptom:**
```
ERROR Node - Failed to connect to database: dial tcp <host>:3306: connect: connection refused
```

**Causes and fixes:**

| Cause | Fix |
|-------|-----|
| Database not running | `docker compose -f docker/docker-compose.yml ps mariadb` — verify healthy |
| Wrong host/port | Check `node.database.host` / `node.database.port` in `node.yaml` |
| Wrong credentials | Check `node.database.user` / `node.database.password` |
| Database not yet created | Ensure `createDatabaseIfNotExist` is set in the DSN (default) |
| Connection pool exhausted | Increase `node.database.poolSize` (default 20) |
| Firewall | Verify `3306` is reachable from the coordinator host |

Test connectivity directly:

```bash
mariadb -h <host> -P 3306 -u root -p -e "SELECT 1"
```

### Schema creation error

**Symptom:**
```
ERROR Node - Failed to initialize schema: Table 'metalog_metastore.clp_spark' doesn't exist
```

Schema is created automatically on startup. If it fails:

- Verify the database user has `CREATE TABLE`, `CREATE INDEX`, and `ALTER TABLE` privileges.
- Check for leftover partial schema from a previous failed run: `SHOW TABLES IN metalog_metastore`.
- Partition creation requires `PARTITION` privilege on MariaDB.

### Port conflicts

**Symptom:**
```
ERROR HealthCheckServer - Address already in use: 8081
ERROR GrpcServer - Address already in use: 9090
```

Change ports in config:

```yaml
node:
  health:
    port: 8082        # health check HTTP port
```

```bash
API_GRPC_PORT=9091    # API server gRPC port
```

See [Port Configuration](port-configuration.md) for all configurable ports.

### Startup blocked: "Waiting for lookahead partitions"

**Symptom:** Node hangs for several seconds or minutes during startup.

Per-coordinator startup has a **blocking** step that ensures lookahead partitions exist before any coordinator goroutines start. With a brand-new database this is fast; with a badly clock-skewed database or very large `partition.lookahead.days`, it may take longer.

If it never unblocks, check for DDL lock contention on the metadata table:

```sql
SHOW PROCESSLIST;
-- Look for ALTER TABLE ... PARTITION statements stuck in "waiting for lock"
```

---

## Ingestion Issues

### Kafka consumer lag growing

**Symptom:** `kafka-consumer-groups.sh --describe` shows lag increasing over time.

**Causes:**

1. **BatchingWriter queue full** — coordinator is receiving records faster than it can write to the DB. Check logs for:
   ```
   WARN TableWriter - BatchingWriter queue full; blocking producer
   ```
   Fix: increase `node.database.poolSize`, or add a second coordinator for the table.

2. **Database write speed insufficient** — check slow query log and verify `interpolateParams=true` in the DSN. Without it, each parameter requires a separate protocol round-trip.

3. **Coordinator not owning the table** — verify the table has an active assignment:
   ```sql
   SELECT table_name, node_id FROM _table_assignment WHERE table_name = 'clp_spark';
   ```
   If `node_id` is NULL, no coordinator has claimed the table. Check reconciliation logs.

### gRPC ingestion returns RESOURCE_EXHAUSTED

**Symptom:** gRPC ingestion clients receive `StatusRuntimeException: RESOURCE_EXHAUSTED`.

The gRPC BatchingWriter queue is full. Default capacity: 10,000 queued records (configurable via `node.ingestion.grpc.maxQueuedRecords`).

**Fix:**
- Reduce producer send rate temporarily.
- Check database write throughput — slow DB writes back-pressure the whole pipeline.
- Verify `interpolateParams=true` in the DSN.

### Records ingested but metadata table empty

**Symptom:** gRPC returns success but `SELECT COUNT(*) FROM clp_spark` shows 0.

1. The coordinator may not be claiming the table — verify `_table_assignment`.
2. The BatchingWriter may be writing to a different table name — check `WORKER_TABLE_NAME` and `tables[].name` in `node.yaml`.
3. Check coordinator logs for UPSERT errors.

---

## Coordinator Issues

### Table not being claimed

**Symptom:** `_table_assignment.node_id IS NULL` for a table that should be active.

```sql
SELECT table_name, node_id, last_progress_at FROM _table_assignment;
```

**Causes:**

1. **No coordinator running** — verify at least one coordinator process is up.
2. **Reconciliation not running** — check logs for reconciliation messages. Reconciliation runs every `reconciliationIntervalSeconds` (default 60s). Wait one cycle.
3. **All nodes at capacity** — each node claims tables up to its fair share. Add more coordinator nodes.
4. **Table not registered** — the table must exist in `_table_assignment`. Declare it in `node.yaml` under `tables:` for auto-registration on startup.

### Stalled coordinator

**Symptom:** `last_progress_at` is old; `seconds_stale > 100` but node is alive.

```sql
SELECT table_name, node_id,
       UNIX_TIMESTAMP() - last_progress_at AS seconds_stale
FROM _table_assignment
WHERE node_id IS NOT NULL;
```

The watchdog goroutine detects stalled per-coordinator goroutines and restarts them (or releases the table assignment). Look for:

```
WARN Watchdog - Coordinator for table=clp_spark has not made progress in 50s — restarting
```

If the watchdog itself has stopped, the process may be in an unhealthy state. Check liveness: `curl http://coordinator:8081/health/live`.

### Watchdog restarting a goroutine repeatedly

**Symptom:** Logs show repeated coordinator restarts for the same table.

Indicates a persistent error in one of the per-coordinator goroutines. Check for:
- Kafka Consumer: Kafka broker unreachable, topic deleted
- Planner: database connectivity, task queue corruption
- Storage Deletion: object storage unreachable
- Retention Cleanup: slow `DELETE` queries, lock contention

```bash
grep "ERROR\|WARN" coordinator.log | grep -i "table=clp_spark"
```

---

## Worker Issues

### Workers not claiming tasks

**Check and fix:**

```sql
-- Are there pending tasks?
SELECT COUNT(*) FROM _task_queue WHERE state = 'pending' AND table_name = 'clp_spark';

-- Are tasks stuck in processing (possibly orphaned)?
SELECT *, UNIX_TIMESTAMP() - claimed_at AS age_seconds
FROM _task_queue
WHERE state = 'processing';
```

If `age_seconds > coordinator.task.timeout.ms / 1000`, the Planner will reclaim them on its next cycle. If no tasks exist at all, the Planner may not be running — check coordinator logs.

```bash
# Verify worker database connectivity
mariadb -h db -u root -p -e "SELECT 1"

# Check worker logs for backoff messages
docker compose -f docker/docker-compose.yml logs worker | grep -i "backoff\|error"
```

### Consolidation failures (dead-letter tasks)

**Symptom:** `SELECT * FROM _task_queue WHERE state = 'dead_letter'` returns rows.

Dead-letter tasks indicate repeated worker failures. Common causes:

| Cause | Symptom in logs | Fix |
|-------|-----------------|-----|
| IR file deleted from storage | `NoSuchKeyException` on download | Verify IR bucket retention policy |
| Archive bucket permissions | `Access Denied` on upload | Grant worker `s3:PutObject` on archive bucket |
| CLP binary not found | `clp-s: No such file` | Set correct `clpBinaryPath` in `node.yaml` |
| Corrupt IR file | CLP binary exits non-zero | Manually inspect file; delete dead-letter row after investigation |
| Out of disk space | `No space left on device` | Free scratch space on worker node |

To retry a dead-letter task manually, reset its state:

```sql
UPDATE _task_queue SET state = 'pending', retry_count = 0 WHERE id = <task_id>;
```

---

## Database Issues

### Slow queries / lock contention

**Symptom:** Ingestion latency spikes; `SHOW PROCESSLIST` shows many `Waiting for lock`.

1. **Missing `interpolateParams=true`** — verify it is in the DSN. Without it, each parameter requires a separate protocol round-trip.
2. **Too many indexes** — each `ALTER TABLE ADD INDEX` (from `DynamicIndexManager`) locks the table briefly. Monitor with:
   ```sql
   SHOW PROCESSLIST;
   -- Look for: ALTER TABLE ... with "waiting for metadata lock"
   ```
3. **Partition maintenance overlap** — `ALTER TABLE ... ADD/DROP PARTITION` during peak ingestion can cause brief stalls. The partition maintenance goroutine runs hourly; schedule coordinator restarts outside maintenance windows if needed.

### Unsigned integer scan errors

**Cause:** Go's `database/sql` `Scan()` may fail when scanning `UNSIGNED INT` or `UNSIGNED BIGINT` columns into signed Go types if the value exceeds the signed range.

**Fix:** Use the appropriate scan target type. For `UNSIGNED BIGINT` columns, scan into `uint64`. The `SplitQueryEngine` handles this correctly — follow its scan patterns for new code.

### Partition not found for timestamp

**Symptom:**
```
ERROR - Table has no partition for value 1234567890 (min_timestamp)
```

The daily partition for the file's `min_timestamp` does not exist. Causes:

1. **Future-dated records** — records with `min_timestamp` beyond the lookahead window. Default: 7 days ahead. Increase `partition.lookahead.days` if ingesting pre-aggregated or future-dated data.
2. **Partition maintenance not running** — verify `PARTITION_MANAGER_ENABLED=true`.
3. **Very old records** — records older than the oldest partition. Check `SHOW CREATE TABLE clp_spark` for the partition range.

---

## Common Error Messages

### Kafka consumer errors during shutdown

**Cause:** The Kafka consumer receives errors during shutdown if the context is cancelled while a `Poll()` call is in progress.

**Where it appears:** During coordinator shutdown — typically a benign log message indicating the poll was interrupted.

**Fix:** Ensure graceful shutdown by sending `SIGTERM` (not `SIGKILL`). The coordinator shutdown sequence cancels the consumer's context, which causes `Poll()` to return. The consumer then drains remaining flushes and commits final offsets before exiting. If you see unexpected errors in production, check for:
- `kill -9` being used by your container orchestrator
- `terminationGracePeriodSeconds` too short in Kubernetes (increase to at least 60s)

### `sort column "x" is not indexed`

**Cause:** A `StreamSplits` request uses `order_by` on an unindexed column without setting `allow_unindexed_sort=true`.

**Fix:** Either add `"allow_unindexed_sort": true` to the request (causes a full table scan per page), or use an indexed column (`min_timestamp` ASC, `max_timestamp` DESC). See [gRPC API: Indexed Columns](../reference/grpc-api.md#indexed-columns-and-sorting).

### `filter_expression contains unknown column`

**Cause:** The filter uses a dimension or aggregate key that is not registered in the column registry.

**Fix:** Use `MetadataService/ListDimensions` and `MetadataService/ListAggs` to enumerate valid keys:

```bash
grpcurl -plaintext -d '{"table": "clp_spark"}' localhost:9090 \
  com.yscope.metalog.query.api.proto.grpc.MetadataService/ListDimensions
```

### `lock wait timeout exceeded`

**Cause:** A metadata query is waiting for a row-level lock held by a slow write transaction.

**Fix:**
1. Check `SHOW PROCESSLIST` for long-running write transactions.
2. Increase `innodb_lock_wait_timeout` on the database (default 50s).
3. If caused by `DynamicIndexManager` running an online DDL, wait for the ALTER to complete — it will not block reads indefinitely.

---

## See Also

- [Quickstart](../getting-started/quickstart.md) — Setup and verification steps
- [Performance Tuning](performance-tuning.md) — Performance gotchas and DSN settings
- [Deploy HA](../guides/deploy-ha.md) — HA recovery and failover
- [Monitoring](monitoring.md) — Health checks and alerting
- [Scale Workers](../guides/scale-workers.md) — Worker troubleshooting
