# Rust consolidation

Rust implements the Go IR-to-archive pipeline: policy grouping, database queue, download,
`clp-s` compression, upload, metadata publication, source cleanup, and bounded retries. It also
handles `FILE_ARCHIVE_*` records with JSON/other inputs accepted by `clp-s`. `IR_CLOSED` records
remain in the IR-only lifecycle; enabling a policy does not silently convert them.

## Enable it

The Docker image includes CLP 0.14.1 `clp-s`, pinned by image digest. Compose enables one worker
per node. For a native deployment install a compatible `clp-s` executable and configure:

```yaml
database:
  primary:
    # Existing database settings ...
    pool_size: 8
worker:
  concurrency: 2
  clp_binary_path: /usr/local/bin/clp-s
  clp_process_timeout_secs: 300
storage:
  default_backend: minio
  backends:
    minio:
      type: minio
      endpoint: http://minio:9000
      bucket: logs
      access_key: ${MINIO_ACCESS_KEY}
      secret_key: ${MINIO_SECRET_KEY}
      force_path_style: true
```

The destination is `storage.default_backend` and that backend's configured `bucket`. All nodes
must agree on backend identities and have access to both sources and archives. Use one stable
backend name for each object namespace; aliases for the same storage are not independent identities. Worker-only nodes
can set `coordinator.enabled: false` and disable ingestion; their global worker pool claims tasks
for any table. Coordinators can run with `worker.concurrency: 0` when workers run separately.
Each active worker pins one SQL connection. The primary pool must have at least `concurrency + 4`
connections; coordinator-only nodes running consolidation need at least four. Provision more for
concurrent ingestion and many table coordinators. ProxySQL's writer
route must preserve sessions, as described in the [deployment guide](rust-production.md).

Set table configuration through `AdminService/RegisterTable` or `metalog admin register-table`:

```json
{
  "consolidation": {
    "enabled": true,
    "stale_buffering_mins": 60,
    "policies": [
      {
        "type": "spark_job",
        "config": {
          "grouping_dim_key": "application_id",
          "min_files": 2,
          "max_files": 100,
          "job_timeout": "24h"
        }
      },
      {
        "type": "time_window",
        "config": {"window_size": "1h", "min_files": 2, "max_files": 100}
      }
    ]
  }
}
```

Producers send `IR_ARCHIVE_BUFFERING` while writing, then
`IR_ARCHIVE_CONSOLIDATION_PENDING` once the uploaded source is immutable. JSON/file producers use
the corresponding `FILE_ARCHIVE_*` states. Source backend, bucket, and object key must be explicit.
A source must remain immutable and available until Metalog finishes consolidation.

Policies run in order; each consumes files before the next policy sees the remainder. `time_window`
groups by minimum timestamp. `spark_job` groups by the current string dimension value; missing,
empty, and non-string values form an ungrouped bucket. Any positive, sufficiently old maximum
timestamp forces that job group through even below `min_files`; future and zero timestamps do not.
An empty policy list defaults to `time_window(1h)`, minimum 2 and maximum 100 files. Invalid policy
names, options, durations, and file counts are rejected. Counts must satisfy
`1 <= min_files <= max_files <= 10000`. Different source backends are grouped separately; a task
can span buckets within one backend. Repeated references to the same physical source are downloaded
once, while each metadata row is preserved.

Buffering promotion uses maximum timestamp, with zero meaning uninitialized. A
`stale_buffering_mins` value of zero selects Go's 60-minute default; negative disables promotion.
Only enable promotion if producers cannot continue overwriting objects after that interval.

## Recovery and retention

Planning runs every 10 seconds, with a per-table limit of 100 pending/processing tasks. Workers
claim only when they have capacity. Successful completion requires a nonempty, single archive
file; a zero-byte placeholder cannot close metadata. Uploads stream from disk.

`_consolidation_file` reserves each complete `(table_name, min_timestamp, file_id)` identity in the
same transaction that creates its task. This survives coordinator handoff and prevents duplicate
planning. Active reservations protect inputs from retention. Workers heartbeat every 10 seconds;
coordinators check for processing tasks stale for 5 minutes. A dedicated task advisory lock also
prevents reclaim while a connected worker is executing. Losing that connection interrupts work.

Each retry receives a new task ID and archive object key. A late result cannot overwrite or complete
its replacement. Three automatic retries follow the initial attempt. Exhausted tasks remain
`dead_letter`, with their source reservations retained to prevent an endless replan loop. Dead
letters do not prevent normal expiration/retention of their sources.

After upload, the coordinator atomically updates the exact pending rows to `ARCHIVE_CLOSED` and
marks their reservations published. Dimensions, Bloom snapshots, timestamps, and original source
paths remain on each row. The source path is then provenance; reads use the archive. Several rows
can reference one archive; readers deduplicate archive locations or use grouped split queries. Object
retirement checks references in all tables and durable task reservations before recording a
storage tombstone. Source objects shared with live IR rows remain available.

A completed task stays durable until required deletions succeed. Restarting after publication
resumes cleanup without deleting a live archive. Failed attempt objects are cleaned under the task
lock, with retirement tombstones preventing reuse. Timed-out attempt rows remain for 24 hours so
cleanup can catch a disconnected upload that finishes late. Cleanup scans use bounded pages.
Disabling table consolidation stops new planning but leaves finalization and recovery running on
nodes with storage configured.

SIGTERM stops claims and allows up to 30 seconds to drain workers. Remaining work is canceled,
subprocesses are killed on cancellation, and unfinished attempts become eligible for recovery.
SIGKILL or host loss can leave local scratch directories; use ephemeral worker scratch storage or
remove abandoned `metalog-archive-*` directories after confirming their workers are gone.

## Limits and troubleshooting

Each task has an 8 GiB total downloaded-input limit and a 5 GiB archive limit (S3 single PUT).
Allocate scratch space for inputs plus output per concurrent worker. Large groups should use a
smaller `max_files`. The configured timeout covers download, compression, and upload together and
must be 1–3600 seconds. A timeout or failed upload leaves sources intact and retries the task. If an uploaded archive
vanishes before publication, it is rebuilt under a new attempt ID.

Inspect `_task_queue` state/retry counts and worker/coordinator logs. Input and output are Go v1
named MessagePack maps inside LZ4 frames. A completed task that persists generally needs storage
access or cleanup repaired. Do not delete completed tasks to clear a backlog: they retain recovery
state. Fix missing input objects, backend configuration, capacity, or compressor failures before
retrying dead letters.

An operator can explicitly requeue a dead letter using a new task ID and moving its reservations
in one transaction on the primary database. Stop automatic actions for that task while doing so:

```sql
START TRANSACTION;
SET @old_task = 123; -- inspected dead-letter task
SELECT task_id FROM _task_queue WHERE task_id = @old_task AND state = 'dead_letter' FOR UPDATE;
INSERT INTO _task_queue (table_name, state, input, version, retry_count, created_at)
SELECT table_name, 'pending', input, version, 0, UNIX_TIMESTAMP(NOW(6)) * 1000000000
FROM _task_queue WHERE task_id = @old_task AND state = 'dead_letter';
-- Continue only if the INSERT affected exactly one row.
SET @new_task = LAST_INSERT_ID();
UPDATE _consolidation_file SET task_id = @new_task WHERE task_id = @old_task;
UPDATE _task_queue SET state = 'timed_out', completed_at = UNIX_TIMESTAMP(NOW(6)) * 1000000000
WHERE task_id = @old_task AND state = 'dead_letter';
COMMIT;
```

Do not reset a dead letter to pending under its original ID: its output key may already be retired.
Sources that have expired or been retired cannot be revived by requeuing.

## Go migration and verification

Drain and stop Go workers/coordinators before switching. Mixed Go/Rust consolidation executors
are not supported: Go does not participate in Rust's durable reservations and fencing. Queued Go
v1 inputs can be adopted when their IDs and storage identities resolve unambiguously; Go results
with an omitted `error` field are readable. Drain existing Go work before cutover where possible.

Integration tests use real `clp-s` compression and extraction, filesystem and MinIO storage,
partitioned file identities, Spark dimensions, cross-table references, upload/deletion failures,
timeouts, late completion fencing, bounded retries, and process death with coordinator takeover.
They also run through ProxySQL. A separate worker-only node test covers six table coordinators
sharing a five-connection pool. Set `METALOG_TEST_CLP_BINARY` to an executable, or let the test
helper extract the pinned CLP package image using Docker:

```bash
cargo test --locked -p metalog-integration-tests --test test_consolidation
cargo test --locked -p metalog --test production_smoke consolidation_recovers
METALOG_TEST_PROXYSQL=1 cargo test --locked -p metalog-integration-tests --test test_consolidation
```

These are correctness and recovery tests. Benchmark and soak your own input sizes, object store,
and concurrency settings before replacing Go production traffic.

For a running Compose stack, the reproducible smoke client uploads real IR, ingests through both
nodes, queries closed archive metadata, extracts all four records, and checks source deletion:

```bash
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  cargo run --locked -p metalog-integration-tests --example consolidation_smoke -- \
  http://127.0.0.1:GRPC_PORT_1 http://127.0.0.1:MINIO_PORT http://127.0.0.1:GRPC_PORT_2
```

Use the mapped ports reported by `docker compose port`. This creates an isolated test table and
objects in the configured `logs` bucket; use a disposable integration stack.
