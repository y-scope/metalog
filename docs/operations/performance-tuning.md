# Performance Tuning

[← Back to docs](../README.md)

**Last Updated:** 2026-02-11

> **Note:** Throughput figures in this document were measured using local Testcontainers (MariaDB in Docker). Production numbers depend on network latency, disk IOPS, server load, and connection pool configuration. Re-run `mvn test -Dtest=FileRecordsBenchmark` to get metadata ingestion numbers for your hardware; use `./integration-tests/benchmarks/task-queue-scalability/run.py` for task queue throughput.

## Summary

Rough measured characteristics on a local Testcontainers setup:

| Metric | Ballpark | Notes |
|--------|----------|-------|
| **batch-UPSERT** | 14,000–19,000 records/sec | Default mode (batch size 500) |
| **batch INSERT IGNORE** | 16,000–22,000 records/sec | Fast mode for bulk loading |
| **Kafka Consumption** | 30,000+ msg/sec | Kafka path (bounded by DB write speed) |
| **Query (Pending Files)** | < 50ms | With proper indexing |

> **Critical:** These numbers require proper JDBC configuration. See [Performance Gotchas](#performance-gotchas).

## Benchmarks

Run the benchmark suite:

```bash
# In metalog directory
mvn test -Dtest=FileRecordsBenchmark
```

The benchmark measures:
1. **batch-UPSERT throughput** (multiple batch sizes, 1 warmup + 3 measured iterations)
2. **Realistic ingestion** (5K first-time inserts + 5K state-transition updates, reports both rates)
3. **Query performance** for pending files
4. **Concurrent upsert throughput** with 4 threads
5. **IR-only lifecycle** (PENDING → BUFFERING → IR_CLOSED, 3 upserts/file)
6. **Full archive lifecycle** (5 state transitions, full consolidation workflow)
7. **Mixed workload** (existing data + new inserts + state updates)

### Lifecycle Benchmark Results

In production, each file goes through multiple state transitions, requiring multiple upserts. These figures are from local Testcontainers runs — re-run the benchmark for production-representative numbers:

| Scenario | Upserts/File | Throughput | Effective Files/sec | 25M files/day Headroom |
|----------|--------------|------------|---------------------|------------------------|
| IR-Only Lifecycle | 3 | ~15,000 ups/sec | ~5,000 files/sec | **17x** |
| Full Archive Lifecycle | 5 | ~14,000 ups/sec | ~2,800 files/sec | **9.7x** |
| Mixed Workload | varies | ~18,000 ops/sec | — | — |

---

## Architecture Throughput Analysis

### Single Coordinator Constraints

For the Kafka ingestion path, effective throughput is bounded by the slower stage: Kafka consumption (30K+ msg/sec) vs database batch-UPSERT (10-20K rec/sec). The bottleneck is database write throughput (index maintenance), giving an effective rate of ~10-20K records/sec per coordinator. The gRPC ingestion path shares the same database write bottleneck but uses a separate `BatchingWriter` instance, so the two paths do not contend.

### Thread Architecture Benefits

**Per-coordinator threads (up to 4 per table, each independently toggleable):**

| Thread | Purpose | Throughput Impact |
|--------|---------|-------------------|
| **Thread 1** | Kafka Poller | Hides Kafka latency, sustains 30K+ msg/sec (Kafka path) |
| **Thread 3** | Planner | Task creation, policy evaluation, completion processing |
| **Thread 4** | Storage Deletion | Non-blocking cleanup, rate-limited |
| **Thread 5** | Retention Cleanup | Background retention policy enforcement |

**Node-level data path threads:**

| Thread | Purpose | Throughput Impact |
|--------|---------|-------------------|
| **BatchingWriter** | 1 `TableWriter` per active table per ingestion path — batch-UPSERT to database | 10-20K upserts/sec per thread |

**Node-level HA & maintenance threads:**

| Thread | Purpose | Throughput Impact |
|--------|---------|-------------------|
| **Watchdog** | Monitor coordinator thread health | Negligible (60s interval) |
| **HA** | Heartbeat or lease renewal | Negligible (30s interval) |
| **Reconciliation** | Claim unassigned tables, reconcile units | Negligible (60s interval) |
| **Partition Maint.** | Lookahead creation and cleanup for all tables | Negligible (1h interval) |

### Batch Size Impact

Optimal batch sizes for database batch-UPSERT (with `rewriteBatchedStatements=true`):

| Batch Size | Throughput | Latency | Notes |
|------------|------------|---------|-------|
| 500 | ~14,500 rec/sec | ~35ms | **Default** (benchmark-optimized) |
| 1,000 | ~8,000 rec/sec | 125ms | Moderate overhead |
| 2,000 | ~12,000 rec/sec | 170ms | Good balance |
| 5,000 | ~15,000 rec/sec | 350ms | Higher throughput, more memory |
| 10,000 | ~18,000 rec/sec | 550ms | Maximum throughput, highest memory |

**Recommendation:** The default of 500 is benchmark-optimized. Increase if you need higher sustained throughput and can accept more memory usage per batch. See [Performance Gotchas](#performance-gotchas) for critical JDBC settings.

---

## Scalability Options

### 1. Horizontal Scaling (Multiple Coordinators)

Each coordinator is responsible for an **independent** database table (and optionally a Kafka topic). This provides workload isolation rather than shared scaling.

Each coordinator owns an independent database table (one-to-one mapping). For Kafka ingestion, each coordinator also owns a dedicated Kafka topic. Total throughput scales linearly: N coordinators × 15K records/sec. There is no contention between coordinators — this provides workload isolation (high-volume services don't impact others) rather than shared pool scaling.

### 2. Vertical Scaling

| Configuration | Impact |
|--------------|--------|
| Larger batch size | +50-100% throughput |
| Connection pool size | +30-50% concurrent throughput |
| More CPU cores | Helps worker thread pool |
| SSD storage | Faster database commits |

### 3. Database Scaling

| Strategy | When to Use |
|----------|-------------|
| **Read replicas** | High query load |
| **Sharding by app_id** | > 100M rows |
| **Table partitioning** | Already implemented (daily) |

---

## Production Projections

Given measured throughput of 15,000 records/sec per coordinator (UPSERT mode):

| Metric | Per Coordinator | 3 Coordinators | 10 Coordinators |
|--------|-----------------|-----------------|------------------|
| Records/minute | 900,000 | 2,700,000 | 9,000,000 |
| Records/hour | 54,000,000 | 162,000,000 | 540,000,000 |
| Records/day | 1.3 billion | 3.9 billion | 13 billion |

---

## Bottleneck Analysis

### Current Design Bottlenecks

1. **Database Write Throughput** (most common)
   - Single thread batch-UPSERT: 14K-19K records/sec (measured)
   - Mitigation: Larger batches, horizontal scaling, fast insert mode for bulk loads

2. **Network Latency**
   - Kafka polling: Mitigated by background poller thread
   - Database commits: Batch operations reduce round trips

3. **Memory**
   - Event queue: 10,000 records max per ingestion path (bounded, set via `Defaults.EVENT_QUEUE_CAPACITY`)
   - Deletion queue: 10,000 paths max (bounded, set via `Defaults.DELETION_QUEUE_CAPACITY`)

### Not Bottlenecks

- **CPU**: Metadata processing is I/O bound
- **Database polling**: `UPDATE LIMIT` scales with workers
- **Kafka**: Can sustain 50K+ msg/sec easily

---

## Recommendations

1. **Start with 1 coordinator** - sufficient for < 50M records/hour
2. **Scale horizontally** when approaching 80% capacity
3. **Monitor database slow query log** for index optimization opportunities
4. **Keep default batch size of 500** unless you need maximum throughput (increase to 5,000+ for bulk loads)
5. **Enable Kafka consumer backpressure** (gRPC path returns `RESOURCE_EXHAUSTED` when queue is full; Kafka path uses built-in consumer backpressure)
6. **Verify JDBC settings** - see [Performance Gotchas](#performance-gotchas) for critical configuration

---

## Running Performance Tests

```bash
# Run benchmark suite
# In metalog directory
mvn test -Dtest=FileRecordsBenchmark

# Expected output:
# === batch-UPSERT Benchmark: 1000 records ===
# Average: 14,XXX-19,XXX records/sec
# Min: 12,XXX records/sec, Max: 22,XXX records/sec
# Latency: ~70-85ms per batch
```

---

## Performance Gotchas

This section documents critical configuration and design decisions that significantly impact throughput. These are common pitfalls that can cause 10-50x performance degradation if not addressed.

### 1. JDBC Batch Settings (Critical)

**Problem:** The JDBC driver does NOT batch statements by default, even when using `addBatch()`/`executeBatch()`.

**Impact:** Without proper settings, 1000-record batch takes ~15-20 seconds instead of ~1 second.

**Solution:** Add these parameters to the JDBC URL:

```java
// In DatabaseConfig / JdbcUrlBuilder
jdbc:mariadb://host:port/database
    ?rewriteBatchedStatements=true    // CRITICAL: Rewrites batch to multi-row INSERT
    &cachePrepStmts=true              // Cache prepared statements
    &prepStmtCacheSize=250            // Prepared statement cache size
    &prepStmtCacheSqlLimit=2048       // Max SQL length to cache
```

**Why it matters:**

| Setting | Without | With | Improvement |
|---------|---------|------|-------------|
| `rewriteBatchedStatements=true` | 50 rec/sec | 15,000+ rec/sec | **300x** |

The driver transforms:
```sql
-- Without rewriteBatchedStatements (1000 round trips)
INSERT INTO t VALUES (1, 'a');
INSERT INTO t VALUES (2, 'b');
INSERT INTO t VALUES (3, 'c');
...

-- With rewriteBatchedStatements (1 round trip)
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), ...;
```

### 2. Batch Size Tuning

**Problem:** Small batch sizes increase per-batch overhead; large sizes increase memory and latency.

**Configuration** (in `application.properties` or via env override):
```properties
coordinator.insert.batch.size=500
```

**Measured performance (with `rewriteBatchedStatements=true`):**

| Batch Size | Throughput | Latency per Batch |
|------------|------------|-------------------|
| 500 | ~14,500 rec/sec | ~35ms | ← Default |
| 1,000 | ~8,000 rec/sec | ~125ms |
| 5,000 | ~15,000 rec/sec | ~350ms |
| 10,000 | ~18,000 rec/sec | ~550ms |

**Recommendation:** Use the default of 500 for balanced throughput and latency. Increase to 5,000+ only if you need maximum throughput and can tolerate higher per-batch latency.

### 3. Fast Insert Mode (Bulk Loading)

**Problem:** `INSERT ... ON DUPLICATE KEY UPDATE` has overhead from:
- Uniqueness checking (MD5 hash computation on functional index)
- Update logic for duplicates

**Solution:** For initial bulk loads where duplicates are rare, enable fast insert:

```bash
export COORDINATOR_INSERT_FAST_ENABLED=true
```

**Comparison:**

| Mode | SQL | Use Case | Throughput |
|------|-----|----------|------------|
| Safe (default) | `INSERT ... ON DUPLICATE KEY UPDATE` | Production | ~15,000 rec/sec |
| Fast | `INSERT IGNORE ...` | Bulk loading | ~19,000 rec/sec |

**When to use fast mode:**
- Initial data migration
- Backfill operations
- Benchmark testing

**When NOT to use fast mode:**
- Production (state updates would be silently skipped)
- Re-processing scenarios

### 4. Kafka Consumer Thread Safety (Shutdown)

**Problem:** `KafkaConsumer` is NOT thread-safe. Accessing it from multiple threads during shutdown causes `ConcurrentModificationException`.

**Symptom:**
```
java.util.ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access
    at org.apache.kafka.clients.consumer.KafkaConsumer.acquire(...)
```

**Root cause:** Multiple threads accessing KafkaConsumer:
- Kafka Poller thread: `consumer.poll()`
- TableWriter callback: `consumer.commitOffset()` (after successful DB write)
- Shutdown Hook: `consumer.commitOffset()`, `consumer.close()`

**Solution:** Stop threads in correct order during shutdown:

```
Phase 1: Set running = false (signal all threads)
Phase 2: Stop Kafka Poller, wait for exit
Phase 3: Stop BatchingWriter/TableWriter, wait for drain
Phase 4: Stop Planner, wait for exit
Phase 5: Wait for in-flight tasks
Phase 6: Commit final Kafka offset (safe — all threads stopped)
Phase 7: Stop remaining threads (Storage Deletion, Retention Cleanup)
Phase 8: Close resources
```

**Key insight:** The thread that calls `poll()` and the thread that calls `commitOffset()` must be coordinated. Either:
1. Same thread does both (simplest)
2. Strict ordering during shutdown (current approach)
3. Synchronization wrapper (adds latency)

### 5. Index Overhead

**Problem:** The `clp_spark` table has 7 base indexes, each updated on every INSERT. Dimension indexes are added dynamically by `DynamicIndexManager` as new `dim_*` columns are discovered.

**Base indexes:**
1. PRIMARY KEY (min_timestamp, id)
2. idx_id (id) — required for AUTO_INCREMENT on partitioned tables
3. idx_clp_ir_hash (clp_ir_path_hash, min_timestamp) — UNIQUE, virtual MD5 column
4. idx_clp_archive_hash (clp_archive_path_hash) — virtual MD5 column
5. idx_consolidation (state, min_timestamp) — Planner query
6. idx_expiration (expires_at) — retention cleanup
7. idx_max_timestamp (max_timestamp DESC) — time-range queries

**Impact:** Each additional index adds ~5-10% overhead to INSERT throughput. Dynamic dimension indexes added at runtime have the same cost.

**Mitigation:**
- Batch inserts amortize index update cost
- Consider disabling non-essential dimension indexes during bulk loads
- The virtual MD5 indexes add computation overhead per row

### 6. Table Partitioning Considerations

**Current:** Daily partitions by `min_timestamp`

**Benefits:**
- Partition pruning for time-range queries
- Efficient data expiration (drop partition)

**Gotcha:** Inserts spanning multiple partitions in one batch are less efficient. The benchmark generates records with similar timestamps to stay within one partition.

---

## End-to-End Benchmark

Two benchmark scripts exercise the full ingestion pipeline against real Docker infrastructure (MariaDB, Kafka, ZooKeeper). Both build the JAR, start containers, produce records, and report coordinator throughput.

**Kafka ingestion benchmark:**
```bash
# From the metalog directory
integration-tests/benchmarks/kafka-ingestion/run.py -r 100000 -t 300

# Wire format options (default: json)
integration-tests/benchmarks/kafka-ingestion/run.py --mode json
integration-tests/benchmarks/kafka-ingestion/run.py --mode proto-structured   # DimEntry/AggEntry
integration-tests/benchmarks/kafka-ingestion/run.py --mode proto-compat       # SelfDescribingEntry /keys
```

**gRPC ingestion benchmark:**
```bash
integration-tests/benchmarks/grpc-ingestion/run.py -r 100000

# Wire format options (default: structured)
integration-tests/benchmarks/grpc-ingestion/run.py --mode structured   # DimEntry/AggEntry
integration-tests/benchmarks/grpc-ingestion/run.py --mode compat        # SelfDescribingEntry /keys
```

See the [Summary](#summary) table for expected throughput ranges. Results vary by hardware — run the benchmark to get numbers for your environment.

---

## Task Queue Performance

The database-backed task queue uses a single atomic `UPDATE ... ORDER BY task_id LIMIT ?` for distributed task claiming. Performance was benchmarked using Testcontainers (MariaDB).

### Claim Throughput

With the prefetch model, a single `TaskPrefetcher` thread issues one DB batch-claim per queue refill regardless of how many worker threads are running. DB round-trips scale with task throughput, not with worker count.

| Prefetch batch size | DB claims/sec (steady state) | Workers supported |
|---------------------|------------------------------|-------------------|
| 1 | ~50-60/sec | any |
| 5 | ~250-300/sec | any |
| 10 | ~400-500/sec | any |

**Key observations:**
- Worker count no longer drives DB polling frequency — the prefetcher claims in batches, workers drain the queue
- Default `prefetchQueueSize=5` is sufficient for most deployments
- Increase `prefetchQueueSize` if workers are frequently starved (queue drains faster than the prefetcher refills)
- Production database (dedicated, not Docker) will be faster

### Multi-Coordinator Isolation

Each coordinator's workers poll only their assigned table. Multiple coordinator deployments don't compete for the same row locks, enabling independent scaling. This is workload isolation, not shared pool scaling.

### Running the Benchmarks

**Scalability benchmark** — sweeps worker counts and batch sizes to find the concurrency ceiling:

```bash
# Default: workers=1,25,50,75,100,125  batch=1,5,10
integration-tests/benchmarks/task-queue-scalability/run.py

# Custom configuration
integration-tests/benchmarks/task-queue-scalability/run.py -w 50,100 -b 1,5 -t 50

# Options:
#   -w, --workers <csv>    Worker counts (default: 1,25,50,75,100,125)
#   -b, --batch <csv>      Batch sizes (default: 1,5,10)
#   -t, --tasks <n>        Tasks per worker (default: 50)
#   -T, --tables <n>       Number of tables (default: 10; reflects typical multi-table deployment)
#   -J, --jitter <ms>      Deadlock retry jitter cap in ms (default: 50)
```

### Task Queue Bottlenecks

1. **Transaction commit (fsync)** - Main bottleneck, batching helps
2. **Row lock contention** - Multi-table eliminates (workers on different tables never contend)
3. **Network round-trips** - Batching reduces

**Not bottlenecks:**
- ENUM vs TINYINT for state (both stored as integers)
- MEDIUMBLOB payload (stored off-row with ROW_FORMAT=DYNAMIC)

### Replication Considerations

By default, keep `_task_queue` replicated for simpler failover (tasks resume exactly where they left off).

For edge cases with extremely high task churn, `_task_queue` can optionally be excluded from semi-sync replication since it's recoverable from Kafka:

```ini
# Replica my.cnf (optional, not recommended for most deployments)
[mysqld]
replicate-ignore-table=metalog_metastore._task_queue
```

On failover, the new coordinator reuses the same Kafka consumer group ID and resumes from the last committed offset. No checkpoint table is needed. See [Coordinator HA Design: Resumption State](../design/coordinator-ha.md#a3-resumption-state) for details.

---

## Task Queue IOPS Analysis

### Per-Operation Cost

| Operation | SQL Operations | Index Lookups | Writes |
|-----------|----------------|---------------|--------|
| Create Task | 1 INSERT | 0 | 3 (data + 3 indexes) |
| Claim Task | 1 SELECT + 1 UPDATE | 1 (idx_claim) | 2 (data + idx_stale) |
| Complete Task | 1 UPDATE | 1 (PK) | 2 (data + idx_cleanup) |
| Fail Task | 1 UPDATE | 1 (PK) | 2 (data + idx_cleanup) |
| Stale Reclaim | 1 SELECT + N*(1 INSERT + 1 UPDATE) | 1 (idx_stale) | varies |
| Cleanup | 3 SELECT + 3 DELETE (per state) | 3 (idx_cleanup) | varies |

### Steady-State Throughput (100 workers, 10 tasks/sec)

With prefetch, one `TaskPrefetcher` issues DB claims; worker count no longer multiplies claim IOPS. Frequency below is the **steady-state rate needed** to sustain 10 tasks/sec throughput, not the maximum capacity (which is much higher — see [Claim Throughput](#claim-throughput) above).

| Operation | Frequency | Read IOPS | Write IOPS |
|-----------|-----------|-----------|------------|
| Create Task | 10/sec | 0 | 30/sec |
| Claim Task (prefetcher, batch=5) | 2/sec (= 10 tasks ÷ 5 batch) | 2/sec | 4/sec |
| Complete Task | 10/sec | 10/sec | 20/sec |
| Stale Reclaim | 0.1/sec (rare) | ~0 | ~0 |
| Cleanup | 0.02/sec (batch) | ~0 | ~0 |
| **Total** | | **~12/sec** | **~54/sec** |

**Conclusion:** ~66 IOPS total (vs ~120 in the per-worker model). A single MariaDB/MySQL instance handles 10,000-50,000 IOPS depending on hardware.

### Scalability Limits

| Bottleneck | Limit | Mitigation |
|------------|-------|------------|
| Single coordinator | ~1,000 tasks/sec | Shard by `table_name` (already supported) |
| Worker poll rate | ~10,000 polls/sec | Backoff reduces idle polling |
| Index contention | ~50,000 claims/sec | Multi-table eliminates cross-table contention |
| Disk IOPS | Hardware-dependent | SSD recommended; data fits in RAM anyway |

### Comparison with Message Queues

| Aspect | Database Queue | Redis | SQS |
|--------|----------------|-------|-----|
| Latency (claim) | 1-5 ms | 0.5-1 ms | 10-50 ms |
| Throughput | 10,000+ ops/sec | 100,000+ ops/sec | 3,000 ops/sec |
| Durability | Fully durable | Configurable | Fully durable |
| Cost | Included (shared DB) | Separate cluster | Per-request |
| Exactly-once | Via transactions | No | No (at-least-once) |

**For this use case:** Database queue provides sufficient throughput (40 ops/sec actual, 10,000+ capacity), full durability, and zero additional infrastructure cost.

---

## Dimension Index Strategy

This section covers general principles for indexing `dim_*` columns on metadata tables. The primary optimization lever for storage is indexing strategy — it can range from 13% to 34% of total table size.

### Dimension Selection Criteria

Prioritize dimensions for indexing based on:

1. **Query frequency**: How often is this dimension used in WHERE clauses?
2. **Selectivity**: How much does it reduce the result set? (high cardinality = high selectivity)
3. **Cardinality**: Affects index size and effectiveness
4. **Query pattern**: Range queries, equality filters, or aggregations?

### Index Size Estimation

**Formula:** Index Size = Row Count x (Key Size + Overhead) x Compression Factor

**Compression factors by cardinality (InnoDB B-tree, LZ4 page compression):**

| Cardinality | Values | Compression | Effective bytes/row |
|-------------|--------|-------------|---------------------|
| Low | 10-100 | 50-60% | 2-2.5 |
| Medium | 100-10K | 30-40% | 18-25 |
| High | 10K-1M | 20-30% | 28-40 |
| Very high | 1M+ | 10-20% | 40-50 |

Base assumptions: InnoDB B-tree overhead 1.5x key size, LZ4 page compression enabled.

### Time Range Selectivity

Time range filters are highly selective due to the RANGE-partitioned primary key and sequential insertion order:

| Time Range | Rows Scanned | % of 30-day Table |
|------------|-------------|-------------------|
| 1 hour | ~0.14% | Negligible |
| 1 day | ~3.3% | Small |
| 7 days | ~23% | Moderate |
| No time filter | 100% | Full scan |

**Critical insight:** Dimension filters are often MORE selective than time filters. A 1-day time range might reduce 180M rows to 6M, but adding `region + service_name` can further reduce 6M to a few thousand. This means dimension indexes (or composite indexes including time) provide significant query speedups even when time-range partition pruning is already in play.

### Separate vs Composite Indexes

**Separate indexes** (one per dimension):
- MySQL picks ONE index per query (the most selective). Other conditions are filtered via table lookup.
- Storage: each index is full-size, but total is the sum of all individual indexes
- Flexible: any single-dimension query can use its dedicated index

**Composite indexes** (time + 2-3 dimensions):
- MySQL uses the composite index to seek directly to the matching tuple
- Storage: ~40% smaller than equivalent separate indexes
- Performance: 10-100x faster for multi-dimension queries
- Trade-off: single-dimension queries without a time filter cannot use the composite index

**Recommendation:** Use 2-3 composite indexes per table covering the most common query patterns. Always include `min_timestamp` as the first column (enables partition pruning and time-range filtering in one seek).

**Design principles:**
1. Analyze query logs to find the most common dimension combinations
2. Create a composite index for each common pattern
3. Always include time (`min_timestamp`) as the first column
4. Order remaining columns by selectivity (most selective first)
5. Start lean — indexes can be added online (`ALTER TABLE ... ALGORITHM=INPLACE`)

### Index Monitoring

After deployment, monitor index usage and adjust:

```sql
-- Index usage statistics
SELECT OBJECT_NAME, INDEX_NAME, COUNT_STAR, COUNT_READ
FROM performance_schema.table_io_waits_summary_by_index_usage
WHERE OBJECT_SCHEMA = 'metalog_metastore';

-- Slow queries (> 1s average)
SELECT DIGEST_TEXT, COUNT_STAR, AVG_TIMER_WAIT/1000000000 AS avg_ms
FROM performance_schema.events_statements_summary_by_digest
WHERE AVG_TIMER_WAIT > 1000000000000
ORDER BY COUNT_STAR DESC LIMIT 10;
```

**Actions:**
- Drop unused indexes (`COUNT_READ = 0` after 30 days)
- Add targeted indexes for hot queries identified in slow query logs
- Monitor write performance — if p99 exceeds 100ms, consider reducing index count

---

## See Also

- [Architecture Overview](../concepts/overview.md) — System overview and data lifecycle
- [Metadata Schema](../concepts/metadata-schema.md) — Table design, UPSERT strategy, partitioning
- [Metadata Tables Reference](../reference/metadata-tables.md) — DDL, column reference, index reference
- [Query Execution](../concepts/query-execution.md) — Index design and query patterns
- [Task Queue](../concepts/task-queue.md) — Task queue protocol and state machine
- [Configuration Reference](../reference/configuration.md) — JDBC settings, batch size tuning
