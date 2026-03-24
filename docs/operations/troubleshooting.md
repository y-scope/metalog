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
| Wrong host/port | Check `database.host` / `database.port` in `node.yaml` |
| Wrong credentials | Check `database.user` / `database.password` |
| Database not yet created | Ensure `createDatabaseIfNotExist` is set in the DSN (default) |
| Connection pool exhausted | Increase `database.poolSize` (default 5) |
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
"health server starting" ... listen tcp :8081: bind: address already in use
"gRPC server starting"   ... listen tcp :9090: bind: address already in use
```

Change ports in config YAML or via env var override:

```yaml
health:
  port: 8082          # health check HTTP port
grpc:
  port: 9091          # gRPC port
```

Or via environment variables: `HEALTH_PORT=8082`, `GRPC_PORT=9091`.

See [Port Configuration](port-configuration.md) for all configurable ports.

### Startup blocked: "Waiting for lookahead partitions"

**Symptom:** Node hangs for several seconds or minutes during startup.

Per-coordinator startup has a **blocking** step that ensures lookahead partitions exist before any coordinator goroutines start. With a brand-new database this is fast; with a badly clock-skewed database it may take longer. The lookahead is hardcoded at 7 days.

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

1. **BatchingWriter channel full** — coordinator is receiving records faster than it can write to the DB. The Kafka consumer blocks (`SubmitWait`) until space opens, stopping the poll loop and propagating backpressure. No messages are dropped, but lag increases. Fix: increase `database.poolSize`, or reduce Kafka topic throughput.

2. **Database write speed insufficient** — check slow query log and verify `interpolateParams=true` in the DSN. Without it, each parameter requires a separate protocol round-trip.

3. **Coordinator not owning the table** — verify the table has an active assignment:
   ```sql
   SELECT table_name, node_id FROM _table_assignment WHERE table_name = 'clp_spark';
   ```
   If `node_id` is NULL, no coordinator has claimed the table. Check reconciliation logs.

### gRPC ingestion returns RESOURCE_EXHAUSTED

**Symptom:** gRPC ingestion clients receive `StatusRuntimeException: RESOURCE_EXHAUSTED`.

The gRPC BatchingWriter per-table channel is full. Default capacity: 5,000 records (`DefaultBatchSize`).

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
4. **Table not registered** — the table must exist in `_table_assignment`. Register it via the admin gRPC API (`AdminService/RegisterTable`) or direct SQL insert.

### Stalled coordinator

**Symptom:** A coordinator is assigned but not making progress. Stall detection is in-memory (not visible via SQL).

The reconciliation loop checks each coordinator's `ProgressTracker` every 60s. If no progress for 5 minutes (`DefaultProgressStallTimeout`), the coordinator is restarted. If restart fails, the table assignment is released. Look for these log messages:

```
WARN  coordinator stalled, restarting    {"table": "clp_spark"}
WARN  restarting stalled coordinator
```

### Reconciliation restarting a coordinator repeatedly

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
SELECT *, (UNIX_TIMESTAMP() * 1000000000 - claimed_at) DIV 1000000000 AS age_seconds
FROM _task_queue
WHERE state = 'processing';
```

If `age_seconds > 300` (5 min, the `DefaultTaskStaleTimeout`), the Planner will reclaim them on its next cycle. If no tasks exist at all, the Planner may not be running — check coordinator logs.

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
UPDATE _task_queue SET state = 'pending', retry_count = 0 WHERE task_id = <task_id>;
```

---

## Database Issues

### Slow queries / lock contention

**Symptom:** Ingestion latency spikes; `SHOW PROCESSLIST` shows many `Waiting for lock`.

1. **Missing `interpolateParams=true`** — verify it is in the DSN. Without it, each parameter requires a separate protocol round-trip.
2. **Too many indexes** — each `ALTER TABLE ADD INDEX` (from `IndexManager`) locks the table briefly. Monitor with:
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

1. **Future-dated records** — records with `min_timestamp` beyond the lookahead window (hardcoded at 7 days ahead). If ingesting pre-aggregated or future-dated data beyond 7 days, a code change is required.
2. **Partition maintenance not running** — the partition manager runs automatically on every coordinator (always on, no toggle). Verify the coordinator is running and check logs for DDL lock contention.
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
3. If caused by `IndexManager` running an online DDL, wait for the ALTER to complete — it will not block reads indefinitely.

---

## See Also

- [Quickstart](../getting-started/quickstart.md) — Setup and verification steps
- [Performance Tuning](performance-tuning.md) — Performance gotchas and DSN settings
- [Deploy HA](../guides/deploy-ha.md) — HA recovery and failover
- [Monitoring](monitoring.md) — Health checks and alerting
- [Scale Workers](../guides/scale-workers.md) — Worker troubleshooting
