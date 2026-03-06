# Task Queue Design

[← Back to docs](../README.md)

## Overview

The task queue uses the same database that stores metadata as the work distribution mechanism. A single **Prefetcher** goroutine per node batch-claims tasks from `_task_queue` into a buffered channel; worker goroutines receive from that channel instead of hitting the database directly. No message broker, no gRPC coordination, no additional infrastructure. The database already exists, already handles failover, and already provides the transactional guarantees that task distribution requires.

---

## Table of Contents

1. [Design Goals](#design-goals)
2. [Schema](#schema)
3. [Operations](#operations)
4. [Recovery](#recovery)
5. [Self-Healing](#self-healing)
6. [Performance](#performance)
7. [Design Decisions](#design-decisions)
8. [Appendix A: Database Compatibility](#appendix-a-database-compatibility)

---

## Design Goals

| Goal | Solution |
|------|----------|
| **Recoverable coordinators** | All coordination state in database; coordinator can restart anytime |
| **No direct communication** | Workers poll database; no gRPC/push needed |
| **Crash recovery** | Rewind to lowest incomplete offset on restart |
| **Self-healing** | Workers clean up their own orphaned archives |
| **Simplicity** | Single table, minimal indexes, opaque payload |

---

## Schema

Compatible with MariaDB 10.6+ and MySQL 8.0+. See [Appendix A](#appendix-a-database-compatibility) for version requirements.

```sql
CREATE TABLE _task_queue (
    -- ========================================================================
    -- IDENTITY
    -- ========================================================================
    task_id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_name          VARCHAR(64) NOT NULL,           -- Logical coordinator identity

    -- ========================================================================
    -- STATE
    -- ========================================================================
    state               ENUM('pending', 'processing', 'completed', 'failed', 'timed_out', 'dead_letter')
                        NOT NULL DEFAULT 'pending',
    worker_id           VARCHAR(64) NULL,

    -- ========================================================================
    -- TIMESTAMPS (epoch seconds, from database clock)
    -- ========================================================================
    created_at          INT UNSIGNED NOT NULL DEFAULT (UNIX_TIMESTAMP()),
    claimed_at          INT UNSIGNED NULL,
    completed_at        INT UNSIGNED NULL,

    -- ========================================================================
    -- RETRY TRACKING
    -- ========================================================================
    retry_count         TINYINT UNSIGNED NOT NULL DEFAULT 0,

    -- ========================================================================
    -- PAYLOAD (opaque, application-defined)
    -- ========================================================================
    input               MEDIUMBLOB NOT NULL,    -- Task input (LZ4+msgpack TaskPayload)
    output              MEDIUMBLOB NULL,        -- Task output (LZ4+msgpack TaskResult, set on completion)

    -- ========================================================================
    -- INDEXES
    -- ========================================================================
    -- Note: ORDER BY task_id uses implicit PK appended to secondary index
    INDEX idx_claim (table_name, state),
    INDEX idx_stale (table_name, state, claimed_at),
    INDEX idx_cleanup (table_name, state, completed_at),

    FOREIGN KEY (table_name) REFERENCES _table(table_name)

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_bin;

```

### Column Reference

| Column | Type | Purpose |
|--------|------|---------|
| `task_id` | BIGINT AUTO_INCREMENT | Unique identifier, sequential for InnoDB efficiency |
| `table_name` | VARCHAR(64) | Logical coordinator identity (matches MariaDB/MySQL table name limit) |
| `state` | ENUM | Task lifecycle state |
| `worker_id` | VARCHAR(64) | Worker that claimed the task |
| `created_at` | INT UNSIGNED | Task creation time (DB clock, epoch seconds) |
| `claimed_at` | INT UNSIGNED | When worker claimed task |
| `completed_at` | INT UNSIGNED | When task reached terminal state |
| `retry_count` | TINYINT UNSIGNED | Number of previous attempts (for dead-letter threshold) |
| `input` | MEDIUMBLOB | Task input — LZ4-compressed msgpack `TaskPayload` (IR paths, archive path, buckets) |
| `output` | MEDIUMBLOB | Task output — LZ4-compressed msgpack `TaskResult` (actual archive size, timestamp); NULL until worker sets it on completion |

### State Machine

```
pending ──► processing ──► completed
               │
               ├─► (timeout, retry_count < max) ──► timed_out ──► NEW pending task
               │
               ├─► (timeout, retry_count >= max) ──► dead_letter
               │
               ├─► (worker error) ──► failed ──► (deleted by cleanup)
               │
               └─► (graceful shutdown, task queued but not yet executed) ──► pending
```

**Note:** On timeout (`timed_out`), the coordinator inserts a NEW row with `state='pending'` and `retry_count=old+1`. The old row stays as `timed_out`. Worker-reported `failed` tasks are terminal — they are not retried automatically. This ensures unique archive paths per attempt.

On graceful node shutdown, the Prefetcher context is cancelled first, closing the task channel. Worker goroutines drain remaining tasks from the channel and finish their current work. After a drain timeout (30s), the worker context is force-cancelled. Tasks still in `processing` state are reclaimed by the Planner as stale after `TASK_STALE_TIMEOUT_SECONDS` (5 min).

| State | Description |
|-------|-------------|
| `pending` | Waiting for worker to claim |
| `processing` | Worker is executing |
| `completed` | Successfully finished |
| `failed` | Worker reported failure |
| `timed_out` | Worker didn't respond in time; coordinator creates a new retry task |
| `dead_letter` | Max retries exceeded on `timed_out` reclaim; kept for investigation |

### Index Justification

| Index | Query Pattern | Purpose |
|-------|---------------|---------|
| `idx_claim (table_name, state)` | `WHERE table_name=? AND state='pending' ORDER BY task_id` | Worker claim (ORDER BY uses implicit PK) |
| `idx_stale (table_name, state, claimed_at)` | `WHERE table_name=? AND state='processing' AND claimed_at<?` | Stale task detection |
| `idx_cleanup (table_name, state, completed_at)` | `WHERE table_name=? AND state=? AND completed_at<?` | Cleanup old tasks (per-state) |

**Why ORDER BY task_id instead of created_at:**
- `task_id` (AUTO_INCREMENT) is guaranteed monotonically increasing
- Immune to clock skew (unlike `UNIX_TIMESTAMP()`)
- Uses implicit PK appended to secondary index (smaller index)
- Sub-second ordering precision

---

## Operations

**Recommended isolation level: READ COMMITTED**
- No gap locking — only the rows being updated are locked, not the surrounding range
- Standard recommendation for queue-like patterns

### 1. Create Task (Coordinator)

```sql
INSERT INTO _task_queue (table_name, state, retry_count, input)
VALUES (?, 'pending', ?, ?)
```

### 2. Claim Task (Prefetcher)

A single `Prefetcher` goroutine per node batch-claims tasks from the DB into a buffered channel (`chan *Task`, capacity `batchSize*2`). Worker goroutines receive from this channel instead of hitting the database directly — one DB claim transaction per channel refill instead of one per worker per poll.

```go
// Prefetcher.Run loop (single goroutine per node)
for {
    select {
    case <-ctx.Done():
        return
    default:
    }

    claimed, err := pf.taskQueue.ClaimTasks(ctx, "", pf.workerID, pf.batchSize)
    if err != nil {
        sleep(ctx, backoff)
        continue
    }

    if len(claimed) == 0 {
        sleep(ctx, backoff)
        backoff = min(backoff*2, maxBackoff) // 1s → 2s → … → 32s
        continue
    }

    backoff = pollInterval
    for _, task := range claimed {
        select {
        case pf.tasks <- task:
        case <-ctx.Done():
            return
        }
    }
}
```

The underlying DB claim uses `SELECT ... FOR UPDATE SKIP LOCKED` + `UPDATE` in a READ COMMITTED transaction (see `Queue.ClaimTasks`):

```sql
-- Lock pending rows. SKIP LOCKED avoids blocking on rows claimed by other nodes.
-- ORDER BY task_id: monotonic (immune to clock skew), uses implicit PK.
SELECT task_id, table_name, state, retry_count, input
FROM _task_queue
WHERE state = 'pending'
ORDER BY task_id LIMIT ?
FOR UPDATE SKIP LOCKED

-- Then update claimed rows:
UPDATE _task_queue
SET state = 'processing', worker_id = ?, claimed_at = UNIX_TIMESTAMP()
WHERE task_id IN (...)
```

**Why `FOR UPDATE SKIP LOCKED`:**
- Multiple nodes may run Prefetcher goroutines concurrently — `SKIP LOCKED` avoids blocking on rows already claimed by another node
- READ COMMITTED re-evaluates `WHERE state='pending'` after lock release — no cascading blocks

**Why READ COMMITTED isolation:**
- Row locks only, no gap locks on the surrounding index range

**Graceful shutdown:** The `WorkerUnit` performs a two-phase shutdown: (1) cancel the Prefetcher context, which closes the task channel; (2) worker goroutines drain remaining tasks and finish their current work; (3) after 30s drain timeout, force-cancel the worker context. Tasks still in `processing` state are reclaimed by the Planner as stale after `TASK_STALE_TIMEOUT_SECONDS` (5 min).

### 3. Complete Task (Worker)

```sql
-- Worker serializes TaskResult (archive size, timestamp) into output column
UPDATE _task_queue
SET state = 'completed', completed_at = UNIX_TIMESTAMP(), output = ?
WHERE task_id = ? AND state = 'processing'
```

If the UPDATE affects 0 rows, the task was already reclaimed by the coordinator (`timed_out` + new retry task created). The worker logs a warning but does NOT delete the archive — the coordinator's retry logic handles cleanup.

### 3b. Process Completed Tasks (Coordinator)

The coordinator periodically polls for completed tasks to update metadata and schedule IR file deletion:

```sql
-- Fetch both input and output in one query (avoids N+1)
SELECT task_id, input, output FROM _task_queue
WHERE table_name = ? AND state = 'completed' AND output IS NOT NULL
LIMIT 100
```

For each completed task, the Planner reads the `TaskResult` from the output column (archive path, size), updates the metadata table to `ARCHIVE_CLOSED`, removes IR paths from the InFlightSet, and queues source IR files for storage deletion.

### 4. Fail Task (Worker)

When the worker encounters an error during execution:

```sql
UPDATE _task_queue
SET state = 'failed', completed_at = UNIX_TIMESTAMP()
WHERE task_id = ? AND state = 'processing'
```

The worker also deletes the archive just created by this (now-failed) attempt.

**Note:** Workers should retry internally before marking as `failed`. A `failed` task is terminal — the coordinator does not automatically retry it. It remains in `failed` state until the cleanup grace period expires, then is deleted. If manual retry is needed, create a new task row.

### 5. Stale Task Reclaim (Coordinator)

When a task has been `processing` too long (worker assumed dead):

```sql
-- Find tasks in 'processing' state beyond the timeout threshold
SELECT * FROM _task_queue
WHERE table_name = ?
  AND state = 'processing'
  AND claimed_at < UNIX_TIMESTAMP() - ?

-- For each stale task:
-- If retry_count + 1 > maxRetries → move to dead_letter
UPDATE _task_queue SET state = 'dead_letter', completed_at = UNIX_TIMESTAMP()
WHERE task_id = ?

-- Otherwise → mark timed_out and create new pending retry task
UPDATE _task_queue SET state = 'timed_out', completed_at = UNIX_TIMESTAMP()
WHERE task_id = ?
INSERT INTO _task_queue (table_name, state, retry_count, input)
VALUES (?, 'pending', ?, ?)
```

**`failed` tasks are not reclaimed.** They are left in `failed` state until cleanup removes them after the grace period. Only stale `processing` tasks (assumed worker crash) are reclaimed for retry.

**Key insight:** Each retry attempt is a NEW row with its own `task_id` and `created_at`. This ensures:
- Unique archive paths: `archives/{table_name}/{created_at}/task-{task_id}.clp`
- Workers can self-clean orphaned archives
- Full history of attempts preserved

**Race condition (accepted):** A small window exists between SELECT and INSERT where the worker could complete, creating a duplicate task. This is accepted because:
- Extremely rare (two rare events must coincide)
- Impact is duplicate work, not data corruption
- Storage cost is negligible
- Avoiding additional complexity reduces bug risk

### 6. Cleanup (Coordinator)

```sql
-- Delete old completed/failed/timed_out tasks past the grace period (up to 1000 at a time)
DELETE FROM _task_queue
WHERE table_name = ?
  AND state IN ('completed', 'failed', 'timed_out')
  AND completed_at < UNIX_TIMESTAMP() - ?
LIMIT 1000
-- Note: dead_letter tasks are NOT deleted here — kept for manual inspection
```

---

## Recovery

### Coordinator Startup Flow

1. Get database timestamp (avoid clock skew with coordinator): `SELECT UNIX_TIMESTAMP()`
2. Wait for next second (ensure clean boundary)
3. Use the OLD timestamp as cutoff — tasks with `created_at <= dbTime` are from the previous coordinator
4. Delete all tasks except `dead_letter`:
   ```sql
   DELETE FROM _task_queue WHERE table_name = ? AND state != 'dead_letter'
   ```
5. Kafka consumer group resumes from last committed offset (broker tracks per group ID)
6. Resume normal operation — only process completions where `created_at > startupTime`

### Why This Works

| Scenario | What Happens |
|----------|--------------|
| Coordinator crashes mid-operation | On restart: deletes tasks, Kafka consumer group resumes from last committed offset |
| Worker completes task from previous coordinator | UPDATE returns 0 (row deleted), worker deletes orphan archive |
| Worker still processing when coordinator restarts | Worker eventually completes, finds no row, deletes orphan archive |

**Duplicate work on recovery (accepted):** When the coordinator restarts, workers may still be processing old tasks. These workers complete their work, fail to UPDATE (row deleted), and delete their archives. Meanwhile, the new coordinator reprocesses the same source data. This is accepted because:
- Rare (only during coordinator restarts)
- No correctness issues (self-healing cleans up orphans)
- Cost is minimal (small amount of redundant compute)
- Simpler than tracking in-flight workers across coordinator restarts

### Clock Skew Handling

**Problem:** Coordinator's clock might differ from database clock.

**Solution:** Query database for timestamp:
```sql
SELECT UNIX_TIMESTAMP()
```

**Edge case:** Startup within same second as old tasks.

**Solution:** Wait for next second (poll `SELECT UNIX_TIMESTAMP()` until it advances), then use the old value as the cutoff.

This ensures `created_at > startupTime` cleanly separates old vs new tasks.

### Replication Strategy

The `_task_queue` table is recoverable (gRPC clients re-send on timeout; Kafka consumer group resumes from committed offsets) and can optionally be excluded from replication for high task churn scenarios:

```ini
# Replica my.cnf (optional)
[mysqld]
replicate-ignore-table=metalog_metastore._task_queue
```

**Failover behavior:**
- On restart: delete tasks, Kafka consumer group resumes from last committed offset
- Group ID derived as `clp-coordinator-{table_name}-{table_id}` — no offset storage in DB

---

## Self-Healing

Workers clean up the archive they just created when their task fails. This prevents orphaned archives from accumulating on storage when a worker encounters an error mid-execution:

On success, the worker marks the task `completed` with the archive output. If the UPDATE affects 0 rows, the task was reclaimed — the worker leaves the archive in place for the coordinator's retry logic.

On failure, the worker marks the task `failed` and deletes the archive it just created from this attempt.

**Archive path uniqueness:**

Each attempt has unique `task_id` and `created_at`, so archive path is always unique:
```
archives/{table_name}/{created_at}/task-{task_id}.clp
```

This prevents overwrite conflicts between retry attempts.

---

## Performance

### Expected IOPS

One `Prefetcher` per node issues all DB claims; worker count no longer multiplies claim queries.

| Operation | Frequency | IOPS |
|-----------|-----------|------|
| Prefetcher claim queries (batch=5) | ~2/sec | ~2/sec |
| Task creates | ~10/sec | ~10/sec |
| Completions | ~10/sec | ~10/sec |
| **Total** | | **~22/sec** |

**Conclusion:** Trivial load for MariaDB/MySQL, which handles 10,000+ simple queries/sec.

### Comparison

| Table | Throughput |
|-------|------------|
| Metadata table | 20-22K rows/sec (batch-UPSERT) |
| Task queue | ~40 ops/sec |

Task queue is ~250x lighter than metadata table.

### Contention

- **READ COMMITTED + FOR UPDATE** — only the Prefetcher goroutine issues claims per node; contention between multiple nodes is minimal since each node's Prefetcher is a single claimer
- Multiple nodes spread across different `table_name` values naturally via task ordering

### Polling Strategy

```
Prefetcher (1 goroutine per node)
  ClaimTasks(batchSize) ──► got tasks? ──► yes ──► send to channel
                                              │
                                              no
                                              ▼
                                     sleep with backoff (1s → 32s)

Worker goroutine (N per node)
  loop ──► <-pf.Tasks() [blocks] ──► got task ──► execute ──► (loop)
```

- **High load:** Prefetcher keeps channel fed; workers receive instantly
- **Low load:** Prefetcher backs off (1 s → 32 s), reducing DB polling automatically; workers block on channel receive at zero CPU cost
- **One DB claim transaction per batch** (SELECT FOR UPDATE + UPDATE) regardless of worker count

---

## Design Decisions

### Why Opaque Payload (MEDIUMBLOB)

| Approach | Pros | Cons |
|----------|------|------|
| Explicit columns | Queryable, typed | Schema changes for new fields |
| JSON column | Flexible, partially queryable | Can't index efficiently |
| **MEDIUMBLOB** | Simple, no schema changes | Can't query contents |

**Decision:** Payload is opaque because:
- Only workers need to read it
- Coordinator doesn't need to query payload contents
- Schema stays simple, no migrations for new fields

### Why No Separate Offset Table

Kafka consumer groups track committed offsets server-side (per group ID: `clp-coordinator-{table_name}-{table_id}`). On restart or failover, the new coordinator reuses the same group ID and resumes automatically. No offset storage in the database.

### Why BIGINT (Signed) for task_id

| Type | Max Value | Go Compatibility |
|------|-----------|-----------------|
| BIGINT UNSIGNED | 18.4 quintillion | Needs `uint64` |
| **BIGINT (signed)** | 9.2 quintillion | Maps to `int64` directly |

**Decision:** Signed for clean Go/SQL integration. 9.2 quintillion is plenty.

### Why table_name Instead of coordinator_id

**Observation:** Coordinators are identified by which table they manage.

**Simplification:** Use `table_name` column (matches MariaDB/MySQL's 64-char limit) instead of separate `coordinator_id`.

### Why New Row Per Retry (Not In-Place Update)

| Approach | Archive Path | Cleanup |
|----------|--------------|---------|
| In-place retry_count++ | Need retry_count in path | Complex |
| **New row per retry** | task_id + created_at = unique | Simple |

**Decision:** New row because:
- Each attempt has unique `task_id` — archive path always unique
- Workers self-clean by checking if row exists
- Full history preserved

### Why created_at From Database Clock

**Problem:** Coordinator clock might differ from database.

**Solution:** Use `DEFAULT (UNIX_TIMESTAMP())` — database sets the value.

**Startup:** Query `SELECT UNIX_TIMESTAMP()` to get database time for filtering.

---

## Verification

### Unit Tests

1. **Claim concurrency:** Multiple workers claim different tasks without overlap
2. **Stale reclaim:** Task times out, new row created, old marked timed_out
3. **Self-healing:** Worker finds no row, returns gracefully

### Integration Tests

1. **Full flow:** Create task → worker claims → worker completes
2. **Coordinator restart:** Kill coordinator → restart → verify Kafka consumer group resumption
3. **Worker self-clean:** Worker completes after coordinator restart → archive deleted

### Manual Testing

1. Docker compose with database mode
2. Kill coordinator mid-operation, verify recovery
3. Kill worker mid-processing, verify stale reclaim

---

## Appendix A: Database Compatibility

This schema is compatible with **MariaDB 10.4+** and **MySQL 8.0+**.

| Feature | MariaDB | MySQL | Notes |
|---------|---------|-------|-------|
| `SELECT ... FOR UPDATE` + `UPDATE ... WHERE ... IN (...)` | All | All | Batch claim (two-step: lock then update) |
| `DEFAULT (expression)` | 10.2.1+ | 8.0.13+ | For `UNIX_TIMESTAMP()` default |
| `COMPRESSION='lz4'` | 10.1+ | 8.0+ | Page compression |
| `BIGINT AUTO_INCREMENT` | All | All | Standard |
| `ENUM` | All | All | Standard |
| `MEDIUMBLOB` | All | All | Standard |
| `READ COMMITTED` isolation | All | All | Standard |

---

## See Also

- [Architecture Overview](overview.md) — Planner creates tasks
- [Scale Workers](../guides/scale-workers.md) — Workers claim and execute tasks
- [Consolidation](consolidation.md) — IR→Archive pipeline that tasks drive
- [Performance Tuning](../operations/performance-tuning.md) — Task queue benchmarks and claim throughput
- [Coordinator HA](../design/coordinator-ha.md) — Task recovery on failover
