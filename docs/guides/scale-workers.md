# Scale Workers

[← Back to docs](../README.md)

How to configure, scale, and troubleshoot the consolidation worker pool. Workers execute consolidation tasks by claiming from `_task_queue` and accessing object storage directly without going through the coordinator.

There are two worker deployment modes with different claiming strategies:

- **In-process workers (`WorkerUnit`)** — run as goroutines inside the Node process. A single `Prefetcher` goroutine batch-claims tasks from any table into a buffered channel; worker goroutines receive from the channel instead of hitting the database directly. Used for development and testing.
- **Standalone workers (`metalog-worker`)** — run as separate processes on dedicated machines. Each process runs its own Prefetcher + worker goroutines with the same architecture. Used in production for fault isolation and independent scaling.

## Overview

| Component | Role |
|-----------|------|
| **Coordinator** | Per-table goroutines + Node-level goroutines (BatchingWriter, watchdog, HA, reconciliation) |
| **Workers (in-process)** | Goroutines inside the Node process, sharing resources (dev/test) |
| **Workers (standalone)** | Separate processes on dedicated machines (production) |
| **Communication** | Database polling (`_task_queue` table) |
| **Storage** | Workers access object storage directly (no coordinator bottleneck) |

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| In-process mode | Simple deployment for dev/test, shared connection pool, single config |
| Standalone mode | Fault isolation, independent scaling (production) |
| Prefetcher + channel | One DB claim transaction per batch instead of one per worker; workers wake via channel receive at zero CPU cost |
| `FOR UPDATE` + `UPDATE` | Transactional task claiming in READ COMMITTED isolation; correct fan-out without SKIP LOCKED |
| Direct storage access | Workers bypass coordinator for data transfer |
| Self-healing | Workers delete the archive they just created when their task fails; if a task was reclaimed, the coordinator's retry logic handles cleanup |

## Task Lifecycle

### State Transitions

| State | Description | Transition |
|-------|-------------|------------|
| **pending** | Task created by Planner, waiting for worker | → processing (on claim) |
| **processing** | Worker executing task | → completed (success) or failed (error) |
| **completed** | Archive created successfully | Deleted after grace period |
| **failed** | Execution failed | Deleted after grace period |
| **timed_out** | Worker took too long, task reclaimed | New pending task created |
| **dead_letter** | Max retries exceeded | Preserved for investigation |

### Detailed Flow

1. **Task Creation** (Planner goroutine)
   - Queries database for files in `IR_ARCHIVE_CONSOLIDATION_PENDING` state
   - Groups files by policy (SparkJob, TimeWindow, Audit)
   - Creates `TaskPayload` with IR paths and archive path
   - Inserts to `_task_queue` table with state = `pending`

2. **Task Claiming**
   - A single `Prefetcher` goroutine executes `SELECT ... FOR UPDATE` + `UPDATE` in a READ COMMITTED transaction to batch-claim tasks. Claimed tasks are sent to a buffered channel. Worker goroutines receive from the channel (blocking). Prefetcher backs off (1 s → 32 s) when no tasks are available.
   - Both in-process (`WorkerUnit`) and standalone (`metalog-worker`) use the same Prefetcher + channel architecture.

3. **Task Execution** (Worker)
   - Deserializes `TaskPayload` from task
   - Downloads IR files from object storage
   - Creates compressed archive (CLP compression)
   - Uploads archive to object storage
   - Measures actual archive size

4. **Task Completion** (Worker via Database)
   - Worker calls `completeTask(taskId, result)` - UPDATE with state = `completed`
   - Returns number of rows affected (0 = task was already reclaimed by coordinator)
   - If rows affected = 0, worker logs a warning and leaves the archive in place — the coordinator's reclaim/retry logic handles cleanup

5. **Failure Handling**
   - Worker calls `failTask(taskId)` - UPDATE with state = `failed`
   - Worker also deletes the archive it just created (orphan cleanup on failure)
   - Planner reclaims stale tasks (processing > timeout)
   - Retry creates new pending task with incremented retry_count
   - After max retries, task moves to dead_letter

## Database Protocol

Workers interact with the `_task_queue` table using `SELECT ... FOR UPDATE` + `UPDATE` (READ COMMITTED isolation) for transactional task claiming. For the complete task queue schema, claiming protocol, state machine, recovery model, and performance analysis, see [Task Queue](../concepts/task-queue.md).

### TaskPayload Fields

| Field | Type | Description |
|-------|------|-------------|
| `archivePath` | String | Target archive object key in storage |
| `archiveBucket` | String | Target archive storage bucket |
| `irFilePaths` | List<String> | Source IR file object keys to consolidate |
| `irBucket` | String | Source IR storage bucket |
| `tableName` | String | Target metadata table |

## Scaling with Docker Compose

```yaml
worker:
  build:
    context: ..
    dockerfile: docker/Dockerfile
  environment:
    DB_HOST: db
    DB_PORT: 3306
    DB_DATABASE: metalog_metastore
    DB_USER: root
    DB_PASSWORD: password
    MINIO_ENDPOINT: http://minio:9000
    MINIO_ACCESS_KEY: minioadmin
    MINIO_SECRET_KEY: minioadmin
  entrypoint: >
    sh -c "sleep 5 && /app/metalog-worker"
  deploy:
    replicas: 2  # Scale here
```

### Scaling Commands

```bash
# Scale to 5 workers
docker compose -f docker/docker-compose.yml up -d --scale worker=5

# Check worker status
docker compose -f docker/docker-compose.yml ps | grep worker
```

### Scaling Guidelines

| Workload | Workers | Notes |
|----------|--------:|-------|
| Development | 1-2 | Minimal resources |
| Testing | 2-4 | Parallel task execution |
| Production (small) | 4-8 | ~1000 archives/hour |
| Production (large) | 10-20 | ~5000 archives/hour |

**Scaling considerations:**
- Archive creation is I/O bound (storage throughput)
- More workers = more parallel downloads/uploads
- Diminishing returns beyond storage bandwidth
- Monitor object storage request rates for bottlenecks
- Database polling scales well with `FOR UPDATE` transactional claims

## Configuration

### Worker

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | Database hostname |
| `DB_PORT` | `3306` | Database port |
| `DB_DATABASE` | `metalog_metastore` | Database name |
| `DB_USER` | `root` | Database user |
| `DB_PASSWORD` | `password` | Database password |
| `WORKER_TABLE_NAME` | `clp_spark` | Target metadata table |
| `WORKER_POLL_MIN_BACKOFF_MS` | `1000` | Minimum Prefetcher backoff |
| `WORKER_POLL_MAX_BACKOFF_MS` | `32000` | Maximum Prefetcher backoff |
| `WORKER_ID` | auto-generated | Unique worker identifier |

### Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO URL |
| `MINIO_ACCESS_KEY` | `minioadmin` | Access key |
| `MINIO_SECRET_KEY` | `minioadmin` | Secret key |
| `MINIO_IR_BUCKET` | `logs` | IR files bucket |
| `MINIO_ARCHIVE_BUCKET` | `logs` | Archives bucket |

## Error Handling

### Worker-Side Errors

| Error | Behavior | Recovery |
|-------|----------|----------|
| No task available | Prefetcher backs off (1s → 32s); workers block on channel receive at zero CPU cost | Automatic |
| Database connection error | Log and retry | Automatic retry |
| Task execution failure | Report via `failTask()` | Task marked failed |
| Storage download error | Report via `failTask()` | Task marked failed |
| Storage upload error | Report via `failTask()` | Task marked failed |
| Completion returns 0 rows | Log warning, leave archive (coordinator handles reclaim) | N/A |

### Coordinator-Side Errors (Planner)

| Error | Behavior | Recovery |
|-------|----------|----------|
| Task timeout | Mark timed_out, create retry | Automatic |
| Max retries exceeded | Move to dead_letter | Manual investigation |
| Database error | Log and continue | Next planning cycle |

### Retry Semantics

- **Limited retries** — tasks retry up to `max_retries` (default: 3)
- **Exponential backoff** — workers back off when no tasks are available
- **Idempotent execution** — archive creation overwrites existing files
- **At-least-once delivery** — tasks may execute multiple times on failure
- **Dead letter** — tasks that exceed max retries are preserved for investigation

## Troubleshooting

### Workers Not Claiming Tasks

| Check | Command |
|-------|---------|
| Tasks available? | `SELECT COUNT(*) FROM _task_queue WHERE state = 'pending'` |
| Database connectivity? | `mariadb -h db -u root -p -e "SELECT 1"` |
| Worker logs | `docker compose -f docker/docker-compose.yml logs worker` |
| Index exists? | `SHOW INDEX FROM _task_queue` |

### Tasks Stuck in Processing

| Check | Solution |
|-------|----------|
| Worker health | `docker compose -f docker/docker-compose.yml ps \| grep worker` |
| Task age | `SELECT *, UNIX_TIMESTAMP() - claimed_at as age FROM _task_queue WHERE state = 'processing'` |
| Planner running? | Check coordinator logs for "planner" |
| Timeout config | Ensure `coordinator.task.timeout.ms` is set |

### Archive Creation Failures

| Check | Solution |
|-------|----------|
| Object storage connectivity | Workers need direct object storage access |
| Bucket permissions | IR and archive buckets must be accessible |
| Disk space | Object storage volume may be full |
| Worker logs | `docker compose -f docker/docker-compose.yml logs worker \| grep -i error` |

### Orphan Archives

| Symptom | Cause | Solution |
|---------|-------|----------|
| Archives without metadata | Worker failed mid-execution; delete-on-failure did not run | Check worker logs, manual cleanup |
| Growing storage usage | Archive delete-on-failure repeatedly failing | Verify storage connectivity; check worker logs |

## See Also

- [Task Queue](../concepts/task-queue.md) — Full task queue protocol, schema, recovery, performance
- [CLP Integration](integrate-clp.md) — Worker-CLP binary integration for IR→Archive
- [Consolidation](../concepts/consolidation.md) — Consolidation pipeline and policies
- [Architecture Overview](../concepts/overview.md) — System overview and data lifecycle
- [Performance Tuning](../operations/performance-tuning.md) — Benchmarks and tuning
