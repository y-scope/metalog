# IR-Archive Consolidation

[← Back to docs](../README.md)

**Related:** [Architecture Overview](overview.md) · [Metadata Schema](metadata-schema.md) · [Task Queue Design](../design/task-queue.md) · [Scale Workers](../guides/scale-workers.md) · [Naming Conventions](../reference/naming-conventions.md)

## Table of Contents

- [Overview](#overview)
  - [What Consolidation Means](#what-consolidation-means)
  - [Benefits](#benefits)
- [File Lifecycle](#file-lifecycle)
  - [Entry Types](#entry-types)
  - [State Transitions](#state-transitions)
  - [IR to Archive Relationship](#ir-to-archive-relationship)
- [Consolidation Policy](#consolidation-policy)
  - [Policy Types](#policy-types)
  - [Archive Size Feedback Loop](#archive-size-feedback-loop)
- [Worker Workflow](#worker-workflow)
  - [Consolidation Pipeline](#consolidation-pipeline)
  - [Worker Lifecycle](#worker-lifecycle)
- [Planner Loop](#planner-loop)
  - [Stuck-File Promotion](#stuck-file-promotion)
- [Query Layer](#query-layer)
  - [Search Modes](#search-modes)
  - [Query Routing](#query-routing)
- [Appendix: Policy Configuration Examples](#appendix-policy-configuration-examples)

---

## Overview

Consolidation transforms **CLP-IR files** (a streaming, row-based format) into **CLP-Archives** (a columnar format optimized for analytics and long-term storage). A single archive replaces hundreds to thousands of IR files — reducing file count overhead, enabling columnar access patterns, and unlocking semantic variable extraction. This is one of several workflows managed by the coordinator. For the goroutine model, task queue, and reliability model, see [Architecture Overview](overview.md).

### What Consolidation Means

Consolidation is a full data transformation pipeline — not just compression or file merging.

**Pipeline stages:**

| Stage | Description |
|-------|-------------|
| **Semantic extraction** ([log-surgeon](https://github.com/y-scope/log-surgeon)) | Variable-first parsing with automatic template inference; domain-specific labels consistent across services |
| **PII detection** _(planned)_ | Identify sensitive data via patterns, NER, and ML models |
| **PII obfuscation** _(planned)_ | Mask, redact, or tokenize detected sensitive fields |
| **Encryption** _(planned)_ | Encrypt sensitive content for compliance requirements |
| **Filter computation** | Build bloom filters for search acceleration |
| **Grouping** | Combine files by dimensions (app_id, service, time window) |
| **Compression** (CLP encoding) | Columnar encoding, cross-file deduplication, entropy coding |

At scale (billions of files, tens to hundreds of petabytes), CLP-IR alone is insufficient for analytical workloads. Point queries (debugging a single job) work well with CLP-IR, but analytical queries (e.g., "all auth failures across all services in 7 days") hit bottlenecks: file count overhead dominates query time, row-based scans read entire records, and there is no semantic variable extraction.

### Benefits

| Benefit | Impact |
|---------|--------|
| **2-3x further compression** | Columnar layout + cross-file deduplication beyond CLP-IR |
| **Faster analytical queries** | 1 archive replaces 100-1000 IR files; column pruning; format string indexing |
| **Semantic variable extraction** | Auto-extracted trace IDs, user IDs power cross-service correlation (see [Research Papers](../reference/research.md)) |
| **Queryable data model** | Merged Parse Trees (MPTs) enable columnar access and pattern search |

---

## File Lifecycle

### Entry Types

The metastore supports multiple entry types, each with a distinct lifecycle:

| Entry Type | Description | Consolidation? |
|------------|-------------|----------------|
| **IR-only** | IR file that expires without consolidation | No |
| **Archive-only** | Archive created directly (no source IR) | N/A |
| **IR+Archive** | IR file that will be consolidated into an archive | Yes (via workers) |

### State Transitions

**Consolidation Lifecycle (IR+Archive entries):**

```
┌─────────────────────────┐
│  IR_ARCHIVE_BUFFERING   │  IR file actively being written
│  (clp_archive_path='')  │
└───────────┬─────────────┘
            │ file closed (rotation/timeout)
            │   — OR —
            │ planner promotes stuck file (max_timestamp stale)
            ▼
┌─────────────────────────────────┐
│  IR_ARCHIVE_CONSOLIDATION_      │  Awaiting consolidation
│  PENDING                        │
│  (clp_archive_path='')          │
└───────────┬─────────────────────┘
            │ worker consolidates
            ▼
┌─────────────────────────┐
│     ARCHIVE_CLOSED      │  Successfully consolidated
│  (clp_archive_path set) │
└─────────────────────────┘
```

After successful consolidation the source IR file in object storage is scheduled for deletion — the metastore row transitions to `ARCHIVE_CLOSED` with the archive path set, and a background purge removes the now-redundant IR object.

Normally, the producer sends a second UPSERT with `state=IR_ARCHIVE_CONSOLIDATION_PENDING` when the IR file is finalized. If the producer crashes or the update is lost, the planner's [stuck-file promotion](#stuck-file-promotion) advances these files automatically.

**Other Entry Types:**
- **IR-only:** `IR_BUFFERING` → `IR_CLOSED` (never consolidated, deleted after retention)
- **Archive-only:** Created directly as `ARCHIVE_CLOSED`

### IR to Archive Relationship

A **many-to-one** relationship exists: one CLP-Archive contains multiple CLP-IR files.

```
┌───────────────────────────────────────────────────────────────────────┐
│ clp_spark                                                          │
├────────────────────┬───────────┬──────────────────────────────────────┤
│ clp_ir_path        │ service   │ clp_archive_path                     │
├────────────────────┼───────────┼──────────────────────────────────────┤
│ /ir/host-a/1.clp   │ payments  │ /archive/2024/01/15/abc.clp          │
│ /ir/host-b/2.clp   │ payments  │ /archive/2024/01/15/abc.clp          │ ◄─ Same archive
│ /ir/host-c/3.clp   │ payments  │ /archive/2024/01/15/abc.clp          │
│ /ir/host-d/4.clp   │ users     │ ''                                   │ ◄─ Not yet consolidated
└────────────────────┴───────────┴──────────────────────────────────────┘
```

- Tracked via `clp_archive_path` column
- Archive metadata (size, timestamps) denormalized onto each IR row
- Queries can still operate at IR file granularity within an archive

---

## Consolidation Policy

The policy determines how IR files are grouped into archives. Policies are **domain-specific** and configured per-table based on query patterns.

### Policy Types

Two built-in policy types are registered in the `consolidation` package:

**`time_window`** — Time-window grouping (e.g., microservices):
- Groups IR files by configurable time windows (e.g., 15 min, 1 hour)
- Optimized for time-range filtering
- This is the default policy when no type is specified

**`spark_job`** — Dimension-based grouping (e.g., Spark logs):
- Groups IR files by a dimension key (e.g., `application_id` via `groupingDimKey`)
- All logs from a single Spark job end up in the same archive(s)
- Enables efficient job-level queries

Both policies support `minFiles` (minimum files to form a group) and `maxFiles` (maximum files per task) thresholds. `spark_job` also supports a `jobTimeout` duration for forcing consolidation of incomplete groups.

### Archive Size Feedback Loop _(planned)_

> **Not yet implemented.** The planner currently groups files by count (`min_files`/`max_files`) without size estimation. The following describes the planned design.

The system will target 32–64 MB archives using a learned compression ratio per table.

**Planned design:**

1. **Estimate** — before consolidation, estimate archive size from total IR size divided by the learned ratio (default: 2.5x, meaning IR is approximately 2.5x the size of the resulting archive)
2. **Split if needed** — if the estimate exceeds the target, split the task into smaller tasks
3. **Update ratio** — after consolidation, compute the actual ratio (IR size / archive size) and blend it with the learned ratio using exponential moving average (alpha = 0.1)
4. **Converge** — the ratio stabilizes after a few consolidation cycles without manual tuning

---

## Worker Workflow

### Consolidation Pipeline

Workers execute the full consolidation pipeline:

| Step | Action | Details |
|------|--------|---------|
| 1 | Read IR files from object storage | Download source files for this task |
| 2 | Semantic extraction _(planned)_ | Extract trace IDs, user IDs, etc. via [log-surgeon](https://github.com/y-scope/log-surgeon) |
| 3 | PII detection and obfuscation _(planned)_ | If configured — mask, redact, or tokenize sensitive fields |
| 4 | Build CLP-Archive | Columnar layout, cross-file deduplication, entropy encoding |
| 5 | Write archive to object storage | Atomic upload of the finished archive |
| 6 | Report completion | Archive path, size, and metadata written back to the database |

### Worker Lifecycle

1. **Claim task** via the `Prefetcher`, which batch-claims tasks from `_task_queue` using `SELECT ... FOR UPDATE` + `UPDATE` (READ COMMITTED isolation) and feeds them into a buffered channel; worker goroutines receive from the channel
2. **Read IR files** from object storage
3. **Transform** data through consolidation pipeline
4. **Write archive** atomically to object storage
5. **Mark complete** by updating task state in database

**On failure:** Workers report errors via `CompleteTask` (with error result) or `FailTask`. The coordinator's stale task detection finds stuck tasks (processing beyond timeout), marks them `timed_out`, and creates retry tasks. See [Task Queue Design](../design/task-queue.md).

---

## Planner Loop

The Planner runs on a configurable interval (default 60s) per table. Each cycle executes `planOnce`, a 7-step pipeline organized in three phases:

**Task queue maintenance:**

| Step | Action | Details |
|------|--------|---------|
| 1 | Finalize completed tasks | Apply results (mark files `ARCHIVE_CLOSED`, delete source IR from storage), then delete the task row. Also cleans up leaked terminal rows older than 24h as a catch-all. |
| 2 | Re-queue abandoned tasks | Find `processing` tasks that exceeded the stale timeout (worker crashed mid-task) and create new `pending` retry tasks. |
| 3 | Backpressure check | If the in-memory active task count ≥ 100, skip the rest of the cycle to prevent unbounded queue growth. The counter is seeded from the DB at startup and maintained via increment (on task creation) / decrement (on task finalization). |

**Candidate discovery:**

| Step | Action | Details |
|------|--------|---------|
| 4 | Promote stuck files | Transition `IR_ARCHIVE_BUFFERING` files whose `max_timestamp` is stale to `CONSOLIDATION_PENDING`. See [Stuck-File Promotion](#stuck-file-promotion). |
| 5 | Find candidates | Resolve the policy's required dimension/aggregation keys to physical columns, then query all `CONSOLIDATION_PENDING` files (including any just-promoted). |

**Task creation:**

| Step | Action | Details |
|------|--------|---------|
| 6 | Apply policy | Group candidate files using the configured policy (time window, spark job). |
| 7 | Create tasks | For each group, build an LZ4+msgpack payload and insert a `pending` task into `_task_queue`. Files are tracked in an in-flight set to prevent duplicate tasks. |

### Stuck-File Promotion

Files enter the hybrid lifecycle as `IR_ARCHIVE_BUFFERING` (set by the producer). Normally, the producer sends a second UPSERT with `state=IR_ARCHIVE_CONSOLIDATION_PENDING` when the IR file is finalized. If the producer crashes or the update is lost, the file stays in `IR_ARCHIVE_BUFFERING` indefinitely and never becomes a consolidation candidate.

The planner detects these stuck files by comparing `max_timestamp` against a configurable threshold (default: 60 minutes). If a file's data is older than the threshold, it is unlikely to receive further writes, and the planner promotes it to `CONSOLIDATION_PENDING` via a direct `UPDATE`. Files that raced ahead (producer sent the update between detection and promotion) are naturally skipped by the `WHERE state = 'IR_ARCHIVE_BUFFERING'` clause.

**Configuration** (`stale_buffering_mins` in table config):
- Positive value: threshold in minutes (default: 60)
- Negative value: disables stuck-file promotion
- Zero/omitted: uses the 60-minute default

---

## Task Distribution

The task queue uses the same database that stores metadata — no message broker, no gRPC coordination, no additional infrastructure. The `_task_queue` table holds all pending, in-progress, and completed tasks with LZ4+msgpack payloads.

### State Machine

```
pending ──► processing ──► completed
               │
               ├─► (timeout, retry_count < max) ──► timed_out ──► NEW pending task
               │
               ├─► (timeout, retry_count >= max) ──► dead_letter
               │
               └─► (worker error) ──► failed ──► (deleted by cleanup)
```

On timeout, the coordinator inserts a NEW row with incremented `retry_count`. Each retry attempt has a unique `task_id`, ensuring unique archive paths and enabling self-cleaning.

### Claim Protocol

A single **Prefetcher** goroutine per node batch-claims tasks into a buffered channel; worker goroutines receive from the channel instead of hitting the database directly.

| Step | Component | Action |
|------|-----------|--------|
| 1 | Prefetcher | `SELECT ... FOR UPDATE SKIP LOCKED` — lock pending rows without blocking other nodes |
| 2 | Prefetcher | `UPDATE ... SET state = 'processing'` — claim locked rows in same transaction |
| 3 | Prefetcher | Send claimed tasks to buffered channel (`batchSize × 2` capacity) |
| 4 | Worker goroutines | Receive from channel, execute, mark completed or failed |

`SKIP LOCKED` is the key: concurrent Prefetchers on different nodes skip each other's locked rows instead of blocking. Exponential backoff (2s → 30s) reduces DB polling during idle periods.

### Recovery

| Scenario | Detection | Response |
|----------|-----------|----------|
| Worker dies mid-task | `processing` task exceeds stale timeout (5 min) | Coordinator creates new `pending` retry task |
| Max retries exceeded | `retry_count >= maxRetries` on reclaim | Task moves to `dead_letter` (kept for investigation) |
| Coordinator restarts | Startup flow | Stale tasks reclaimed via normal cycle; Kafka resumes from last committed offset |

On failure, workers report the error via `CompleteTask` (with error result) or `FailTask`. On success with a missing task row (reclaimed, 0 rows affected), they log a warning and leave the archive for the coordinator's retry logic.

### Backpressure

The Planner tracks an in-memory active task counter (seeded from the DB at startup, incremented on task creation, decremented on finalization) and skips creating new tasks when it reaches `maxBackpressureDepth` (100), preventing unbounded growth during worker stalls.

For the full design — schema DDL, SQL operations, design decisions, performance analysis — see [Task Queue Design](../design/task-queue.md).

---

## Query Layer

The query layer abstracts IR/Archive complexity from users — queries return results regardless of whether data is in IR or Archive format.

### Search Modes

| Mode | When to Use | What's Searched |
|------|-------------|-----------------|
| **Archives only** | Fresh data not required | Only `clp_archive_path != ''` |
| **Hybrid** | Need freshest data | Archives + unconsolidated IR files |

### Query Routing

**Example:** "errors in service=payments, last 2 hours"

| Step | Action | Details |
|------|--------|---------|
| 1. Query metastore | `SELECT clp_ir_path, clp_archive_path FROM clp_spark WHERE ...` | Filter by time range and dimensions against a read replica |
| 2. Generate splits | Group results by `clp_archive_path` | Multiple IR files in the same archive become a single split; standalone IRs become individual splits |
| 3. Execute splits | Workers open each split | Archive splits: open archive, scan only the specified files within. IR splits: open IR file directly |
| 4. Return results | Union split results | No duplication — `clp_archive_path` determines the search target |

**Routing rule:** `clp_archive_path` is non-empty → search the archive. `clp_archive_path` is empty → search the IR file. Never both.

**Key properties:**
- CLP archives support **selective file access** — workers read only the specified files within an archive, not the entire archive
- Split grouping by archive path minimizes I/O (3 rows may produce 2 splits)
- **Replication lag**: if a replica is behind, `clp_archive_path` may appear empty, routing the query to the IR file instead of the archive. Results remain correct with slightly reduced efficiency.

---

## Appendix: Policy Configuration Examples

Policy configuration is stored as JSON in `_table_config.config` under the `consolidation.policies` array. Each entry has a `type` and a policy-specific `config` object. See [Configure Tables](../guides/configure-tables.md) for the full config schema.

### Example 1: Spark Logs (spark_job)

Group all logs from a single Spark application by `application_id`:

```json
{
  "consolidation": {
    "enabled": true,
    "policies": [
      {
        "type": "spark_job",
        "config": {
          "grouping_dim_key": "application_id",
          "min_files": 1,
          "max_files": 100,
          "job_timeout": "4h"
        }
      }
    ]
  }
}
```

### Example 2: Microservices (time_window)

Group by 1-hour time windows:

```json
{
  "consolidation": {
    "enabled": true,
    "policies": [
      {
        "type": "time_window",
        "config": {
          "window_size": "1h",
          "min_files": 2,
          "max_files": 100
        }
      }
    ]
  }
}
```

### Example 3: Audit Logs (strict daily windows)

```json
{
  "consolidation": {
    "enabled": true,
    "policies": [
      {
        "type": "time_window",
        "config": {
          "window_size": "24h",
          "min_files": 1,
          "max_files": 500
        }
      }
    ]
  },
  "retention": {
    "enabled": true,
    "type": "default"
  }
}
```

---

## See Also

- [Architecture Overview](overview.md) — Goroutine model, Planner creates consolidation tasks
- [Task Queue Design](../design/task-queue.md) — Task claiming protocol, schema, recovery, design decisions
- [Scale Workers](../guides/scale-workers.md) — Worker scaling and troubleshooting
- [Metadata Schema](metadata-schema.md) — Metadata table design and state columns
- [Semantic Extraction](semantic-extraction.md) — MPT, ERT, log-surgeon, and LLM-powered variable labeling
- [CLP Integration](../guides/integrate-clp.md) — Worker-CLP binary integration for IR→Archive
- [Research Papers](../reference/research.md) — Academic papers behind CLP-S schema inference
