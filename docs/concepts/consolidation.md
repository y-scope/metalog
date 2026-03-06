# IR-Archive Consolidation

[← Back to docs](../README.md)

**Related:** [Architecture Overview](overview.md) · [Metadata Schema](metadata-schema.md) · [Task Queue](task-queue.md) · [Scale Workers](../guides/scale-workers.md) · [Naming Conventions](../reference/naming-conventions.md)

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
- [Query Layer](#query-layer)
  - [Search Modes](#search-modes)
  - [Query Routing](#query-routing)
- [Appendices](#appendices)
  - [Appendix A: Policy Configuration Examples](#appendix-a-policy-configuration-examples)
  - [Appendix B: Policy Configuration Schema](#appendix-b-policy-configuration-schema)

---

## Overview

Consolidation transforms **CLP-IR files** (a streaming, row-based format) into **CLP-Archives** (a columnar format optimized for analytics and long-term storage). A single archive replaces hundreds to thousands of IR files — reducing file count overhead, enabling columnar access patterns, and unlocking semantic variable extraction. This is one of several workflows managed by the coordinator. For the goroutine model, task queue, and reliability model, see [Architecture Overview](overview.md).

### What Consolidation Means

Consolidation is a full data transformation pipeline — not just compression or file merging.

**Pipeline stages:**

| Stage | Description |
|-------|-------------|
| **Semantic extraction** ([log-surgeon](https://github.com/y-scope/log-surgeon)) | Variable-first parsing with automatic template inference; domain-specific labels consistent across services |
| **PII detection** _(if configured)_ | Identify sensitive data via patterns, NER, and ML models |
| **PII obfuscation** _(if configured)_ | Mask, redact, or tokenize detected sensitive fields |
| **Encryption** _(if configured)_ | Encrypt sensitive content for compliance requirements |
| **Filter computation** | Build probabilistic filters (bloom and cuckoo) for search |
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

**Dimension-Based** (e.g., Spark logs):
- Group by `application_id`
- All logs from a single Spark job in the same archive(s)
- Enables efficient job-level queries

**Time-Window** (e.g., microservices):
- Group by 15-minute or 1-hour windows per service
- Optimized for time-range filtering

**Hybrid Triggers:**
- Job completion detection
- Size limits
- Timeout fallbacks (e.g., consolidate after 4 hours regardless)

### Archive Size Feedback Loop

The system targets 32–64 MB archives using a learned compression ratio per table.

**How it works:**

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
| 2 | Semantic extraction | Extract trace IDs, user IDs, etc. via [log-surgeon](https://github.com/y-scope/log-surgeon) |
| 3 | PII detection and obfuscation | If configured — mask, redact, or tokenize sensitive fields |
| 4 | Build CLP-Archive | Columnar layout, cross-file deduplication, entropy encoding |
| 5 | Write archive to object storage | Atomic upload of the finished archive |
| 6 | Report completion | Archive path, size, and metadata written back to the database |

### Worker Lifecycle

1. **Claim task** via the `Prefetcher`, which batch-claims tasks from `_task_queue` using `SELECT ... FOR UPDATE` + `UPDATE` (READ COMMITTED isolation) and feeds them into a buffered channel; worker goroutines receive from the channel
2. **Read IR files** from object storage
3. **Transform** data through consolidation pipeline
4. **Write archive** atomically to object storage
5. **Mark complete** by updating task state in database

**On failure:** Coordinator's stale task detection finds stuck tasks, marks them `timed_out`, and creates retry tasks. Workers self-heal by deleting orphan outputs when task row is missing. See [Task Queue](task-queue.md).

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

## Appendices

### Appendix A: Policy Configuration Examples

> **Column naming note:** `grouping_key` values below use descriptive logical names (e.g., `dim_str128_application_id`, `dim_str128_service`) for readability. In the actual database, dimension columns use opaque placeholder names (`dim_f01`, `dim_f02`, ...) mapped via `_dim_registry`. The wire-format keys that producers send use slash-delimited notation (`dim/str128/application_id`, `dim/str128/service`). See [Naming Conventions](../reference/naming-conventions.md).

#### Example 1: Spark Logs (Dimension-Based)

Group all logs from a single Spark application:

```yaml
# spark-policy.yaml
table: spark
policy:
  type: dimension
  grouping_key: dim_str128_application_id
  triggers:
    - type: job_completion
      detection: state_change_to_closed
    - type: timeout
      hours: 4
  constraints:
    target_archive_size_mb: 64
    max_archive_size_mb: 128
```

#### Example 2: Microservices (Time-Window)

Group by 15-minute windows per service:

```yaml
# microservices-policy.yaml
table: microservices
policy:
  type: time_window
  window_size_minutes: 15
  grouping_key: dim_str128_service
  triggers:
    - type: window_close
    - type: size_threshold
      raw_size_mb: 256
  constraints:
    target_archive_size_mb: 48
    max_archive_size_mb: 96
```

#### Example 3: Audit Logs (Compliance)

Strict per-day archives:

```yaml
# audit-policy.yaml
table: audit
policy:
  type: time_window
  window_size_minutes: 1440  # 24 hours
  grouping_key: null
  triggers:
    - type: window_close
  constraints:
    target_archive_size_mb: 128
    max_archive_size_mb: 256
  retention:
    days: 395  # 13 months
```

### Appendix B: Policy Configuration Schema

```yaml
# Full schema reference
table: string                    # Target table name
policy:
  type: dimension | time_window  # Grouping strategy
  grouping_key: string | null    # Dimension column (null = no grouping)
  window_size_minutes: int       # Only for time_window type
  triggers:
    - type: job_completion | window_close | timeout | size_threshold
      hours: int                 # For timeout trigger
      raw_size_mb: int           # For size_threshold trigger
  constraints:
    target_archive_size_mb: int  # Feedback loop target
    max_archive_size_mb: int     # Hard limit (triggers splitting)
  retention:
    days: int                    # Override default retention
```

---

## See Also

- [Architecture Overview](overview.md) — Goroutine model, Planner creates consolidation tasks
- [Task Queue](task-queue.md) — Task claiming protocol and recovery
- [Scale Workers](../guides/scale-workers.md) — Worker scaling and troubleshooting
- [Metadata Schema](metadata-schema.md) — Metadata table design and state columns
- [Semantic Extraction](semantic-extraction.md) — MPT, ERT, log-surgeon, and LLM-powered variable labeling
- [CLP Integration](../guides/integrate-clp.md) — Worker-CLP binary integration for IR→Archive
- [Research Papers](../reference/research.md) — Academic papers behind CLP-S schema inference
