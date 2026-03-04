# CLP Integration

[← Back to docs](../README.md)

How the consolidation worker integrates with the CLP binary (`clp-s`) to transform schema-less row-based IR files into schema-less columnar archives.

## Worker Architecture

Each worker node runs a single JVM with N worker threads sharing a connection pool:

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Worker Node (1 JVM)                            │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────┐ ┌──────────┐ ┌──────────┐        ┌──────────┐        │
│  │ Thread 1 │ │ Thread 2 │ │ Thread 3 │  ...   │ Thread N │        │
│  │  claim   │ │  claim   │ │  claim   │        │  claim   │        │
│  │  work    │ │  work    │ │  work    │        │  work    │        │
│  │  commit  │ │  commit  │ │  commit  │        │  commit  │        │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘        └────┬─────┘        │
│       │            │            │                   │               │
│       └────────────┴─────┬──────┴───────────────────┘               │
│                          │                                          │
│               ┌──────────▼──────────┐                               │
│               │    HikariCP Pool    │                               │
│               │  (1-3 connections)  │                               │
│               └──────────┬──────────┘                               │
│                          │                                          │
│               ┌──────────▼──────────┐                               │
│               │     S3 Client       │                               │
│               │     (shared)        │                               │
│               └─────────────────────┘                               │
└─────────────────────────────────────────────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         ┌────────┐  ┌──────────┐  ┌─────────┐
         │Database│  │  MinIO   │  │ clp-s   │
         └────────┘  └──────────┘  └─────────┘
```

**Benefits:**
- Shared connection pool limits database connections (N+2 instead of N×connections)
- Single S3 client with connection reuse
- Centralized configuration and shutdown handling
- Thread count tunable per node based on CPU/memory

## Worker Thread Workflow

Each thread independently executes:

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Worker Thread Loop                             │
├─────────────────────────────────────────────────────────────────────┤
│  1. Claim task from _task_queue (SELECT ... FOR UPDATE + UPDATE)     │
│  2. Deserialize payload (LZ4+msgpack) → TaskPayload                │
│  3. Download IR files to temp staging directory (parallel)          │
│  4. Execute: clp-s c --single-file-archive <outputDir> <stagingDir> │
│  5. Upload archive to archiveBucket/archivePath                     │
│  6. Mark task complete (write TaskResult to output column)          │
│  7. Cleanup local temp dirs                                         │
│  8. Loop back to step 1                                             │
└─────────────────────────────────────────────────────────────────────┘
```

## Task Payload Format

The `input` column in `_task_queue` is a **LZ4-compressed msgpack blob** (not JSON).

**Wire format:**

```
[4 bytes big-endian: uncompressed length] [LZ4-compressed msgpack]
```

**Msgpack layout** — a 7-element array (positional, field names are not encoded):

```
Index  Field                  Type
─────  ─────────────────────  ──────────────
  0    archivePath            string   — object key for the target archive
  1    tableName              string   — metadata table name
  2    irFilePaths            string[] — list of IR file object keys
  3    irStorageBackend       string   — backend type for IR files (e.g. "minio", "s3")
  4    irBucket               string   — bucket where IR files live
  5    archiveStorageBackend  string   — backend type for archives
  6    archiveBucket          string   — bucket where archive should be written
```

Serialization and deserialization is handled by `TaskPayload.serialize()` / `TaskPayload.deserialize(byte[])` in `metastore/model/TaskPayload.java`.

## Workflow Steps

### Step 1: Claim Task

Worker claims a task using a `SELECT ... FOR UPDATE` + `UPDATE` transaction (READ COMMITTED isolation) that transitions `state` from `pending` to `processing`. See [Task Queue](../concepts/task-queue.md) for the full claiming protocol.

### Step 2: Deserialize Payload

```java
// From WorkerCore.executeTask():
TaskPayload payload = task.getParsedInput();
// Internally: reads `input` MEDIUMBLOB, calls TaskPayload.deserialize(bytes)
// LZ4 decompression and msgpack unpacking handled by TaskPayload
```

### Step 3: Download IR Files

IR files are downloaded to a JVM-managed temp directory. Downloads run in parallel (semaphore-gated to limit concurrency):

```java
Path stagingDir = Files.createTempDirectory("clp-staging-");
// parallel download of payload.getIrFilePaths() into stagingDir
storageClient.downloadIrFilesParallel(payload.getIrBucket(), payload.getIrFilePaths(), stagingDir);
```

### Step 4: Execute clp-s

Run `clp-s c` in single-file-archive mode. Arguments are positional — `outputDir` comes before `stagingDir`:

```bash
clp-s c --single-file-archive <outputDir> <stagingDir>
```

```java
Path outputDir = Files.createTempDirectory("clp-output-");
// From ClpCompressor.compress():
ProcessBuilder pb = new ProcessBuilder(
    binaryPath, "c", "--single-file-archive",
    outputDir.toString(),   // positional: archive output directory
    stagingDir.toString()   // positional: input directory with IR files
);
// stdout/stderr read in background threads to prevent deadlock
```

`clp-s` writes a UUID-named archive file into `outputDir`.

### Step 5: Upload Archive

The UUID-named archive from `outputDir` is uploaded directly to the destination from the payload. No renaming is needed — the destination path is already known:

```java
Path archiveFile = findSingleArchive(outputDir);  // the UUID-named file clp-s created
long archiveSize = storageClient.uploadFromFile(
    payload.getArchiveBucket(), payload.getArchivePath(), archiveFile);
```

### Step 6: Mark Complete

```java
TaskResult result = new TaskResult(archiveSize, System.currentTimeMillis() / 1000);
int updated = taskQueue.completeTask(taskId, result.serialize());
// result.serialize() goes into the `output` MEDIUMBLOB column
if (updated == 0) {
    // Task was reclaimed — do NOT delete the archive (coordinator handles cleanup)
    logger.warn("Task {} not found in processing state (possibly reclaimed)", taskId);
}
```

## Error Handling

| Error Type | Handling |
|------------|----------|
| Download failure | Exception thrown → `failTask(taskId)` → delete orphan archive |
| clp-s non-zero exit | `CompressorException` thrown → `failTask(taskId)` → delete orphan archive |
| Upload failure | `StorageException` thrown → `failTask(taskId)` → delete orphan archive |
| Worker crash mid-task | Task stays in `processing`; Planner reclaims after stale timeout (default 10 min, set via `coordinator.task.timeout.ms`) |
| `completeTask` returns 0 | Task reclaimed by coordinator — log warning, leave archive in place |

## Compression Metrics

After compression, `storageClient.createArchive()` logs the result:

```
Created archive via clp-s: bucket=logs, path=tenant/table/partition/archive-001.clp,
    ir_files=3, archive_size=1048576
```

The actual archive size (bytes) is returned by `createArchive()` and stored in the `TaskResult` output column for later reporting.

## Configuration

### Worker Thread Pool

| Property | Default | Description |
|----------|---------|-------------|
| `worker.threads` | `4` | Number of worker threads per JVM |
| `worker.poll.interval.ms` | `5000` | Interval between task polls when idle |

Backoff on empty polls uses exponential backoff (1s → 30s) with a 1.5× multiplier, hardcoded in `Timeouts`.

Note: In containerized environments, set `worker.threads` explicitly based on the container's CPU limit (cgroup).

### Database Connection Pool (HikariCP)

| Property | Default | Description |
|----------|---------|-------------|
| `worker.db.pool.min` | `1` | Minimum idle DB connections |
| `worker.db.pool.max` | `3` | Maximum DB connections |
| `worker.db.pool.idle.timeout.ms` | `600000` | Idle connection timeout (10 min) |

Note: DB operations (claim task, mark complete) are brief. Worker threads spend most time
on I/O operations (download IR files, run clp-s, upload archive), so a small pool (1-3
connections) is sufficient even with many worker threads.

### CLP Binary

| Property | Default | Description |
|----------|---------|-------------|
| `clp.binary.path` | `/usr/bin/clp-s` | Path to clp-s binary |
| `clp.process.timeout.seconds` | `300` | Max seconds to wait for clp-s before killing it |

Staging and output directories are JVM temp dirs created per invocation (`Files.createTempDirectory`) and deleted automatically after upload.

## Object Storage Integration

For local development, MinIO (S3-compatible) is used via Docker Compose:

```yaml
# Already configured in docker compose.yml
minio:
  image: minio/minio:latest
  ports:
    - "9000:9000"   # S3 API
    - "9001:9001"   # Console UI
  environment:
    MINIO_ROOT_USER: minioadmin
    MINIO_ROOT_PASSWORD: minioadmin
```

### Worker Environment Variables

```bash
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_IR_BUCKET=logs
MINIO_ARCHIVE_BUCKET=logs
```

### S3 Client Configuration

`ObjectStorageBackend` uses **AWS SDK v2 `S3AsyncClient`** (not the synchronous `S3Client`). It supports multiple named backends and routes requests based on which backend holds the IR files or archives:

```java
S3AsyncClient s3 = S3AsyncClient.builder()
    .endpointOverride(URI.create(minioEndpoint))
    .credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create(accessKey, secretKey)))
    .region(Region.US_EAST_1)   // MinIO ignores region but SDK requires it
    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
    .build();
```

Downloads and uploads use `AsyncResponseTransformer` / `AsyncRequestBody` for non-blocking I/O. Up to 16 IR file downloads run in parallel (semaphore-gated).

### Buckets

| Bucket | Purpose |
|--------|---------|
| `logs` (default) | IR files (input) — configured via `MINIO_IR_BUCKET` |
| `logs` (default) | Archives (output) — configured via `MINIO_ARCHIVE_BUCKET` |

Access MinIO console at http://localhost:9001 (minioadmin/minioadmin)

## Directory Structure

```
Object Storage (bucket: logs by default)
  └── tenant/table/partition/
      ├── file1.ir             # Input: IR files
      ├── file2.ir
      └── archive-001.clp      # Output: Consolidated archive

Worker Container:
  /usr/bin/
    └── clp-s                  # CLP binary

  /tmp/clp-staging-XXXX/       # JVM temp dir: downloaded IR files (auto-deleted)
    ├── file1.ir
    └── file2.ir

  /tmp/clp-output-XXXX/        # JVM temp dir: clp-s generated archive (auto-deleted)
    └── <uuid>                 # UUID-named archive, uploaded then deleted
```

## Building clp-s

The clp-s binary is built from the [CLP repository](https://github.com/y-scope/clp). See the CLP build documentation for instructions.

## Testing the Integration

### 1. Start Infrastructure

```bash
cd clp-service
./docker/start.sh -d
```

### 2. Upload Test IR Files

```bash
# Using mc (MinIO client)
mc alias set local http://localhost:9000 minioadmin minioadmin
mc cp test-data/*.ir local/logs/tenant/table/partition/
```

### 3. Trigger Consolidation

The system creates tasks automatically when files reach `IR_ARCHIVE_CONSOLIDATION_PENDING` state. The Planner groups pending files and inserts `TaskPayload`-encoded tasks into `_task_queue`. You can also trigger consolidation manually via the coordinator's planning cycle.

Note: Tasks use LZ4+msgpack binary payload (see `TaskPayload.serialize()`). Do not insert raw SQL — use the coordinator API or wait for the Planner.

### 4. Monitor Worker

```bash
docker compose -f docker/docker-compose.yml logs -f worker
```

### 5. Verify Output

```bash
mc ls local/logs/tenant/table/partition/
# Should show the consolidated archive
```

## See Also

- [Scale Workers](scale-workers.md) — Worker scaling and troubleshooting
- [Consolidation](../concepts/consolidation.md) — IR→Archive pipeline and policies
- [Task Queue](../concepts/task-queue.md) — Task claiming and recovery
- [Architecture Overview](../concepts/overview.md) — System overview and data lifecycle
