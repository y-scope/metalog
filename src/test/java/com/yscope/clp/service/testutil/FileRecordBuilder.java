package com.yscope.clp.service.testutil;

import com.yscope.clp.service.common.Timestamps;
import com.yscope.clp.service.metastore.model.FileRecord;
import com.yscope.clp.service.metastore.model.FileState;
import java.util.UUID;

/**
 * Fluent builder for creating FileRecord test fixtures.
 *
 * <p>Provides sensible defaults for all fields and named presets for common states.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * FileRecord file = new FileRecordBuilder()
 *     .withApplicationId("spark-app-001")
 *     .pending()
 *     .build();
 * }</pre>
 */
public class FileRecordBuilder {
  // Default test constants
  private static final long DEFAULT_MIN_TIMESTAMP = 1704067200_000_000_000L; // 2024-01-01 00:00:00 UTC (nanos)
  private static final long DEFAULT_IR_SIZE = 10_485_760L; // 10 MB
  private static final long DEFAULT_ARCHIVE_SIZE = 3_495_253L; // ~3 MB (3x compression)
  private static final long DEFAULT_EVENT_COUNT = 100_000L;
  private static final int DEFAULT_RETENTION_DAYS = 30;

  // IR location
  private String irStorageBackend = "";
  private String irBucket = "";
  private String irPath = null; // Will be auto-generated if null

  // Archive location
  private String archiveStorageBackend = "";
  private String archiveBucket = "";
  private String archivePath = "";

  // State
  private FileState state = FileState.IR_ARCHIVE_BUFFERING;

  // Event time bounds
  private long minTimestamp = DEFAULT_MIN_TIMESTAMP;
  private long maxTimestamp = DEFAULT_MIN_TIMESTAMP;
  private long archiveCreatedAt = 0L; // Archive lifecycle timestamp

  // Record count (for early termination)
  private long recordCount = DEFAULT_EVENT_COUNT;

  // Size metrics (nullable)
  private Long rawSizeBytes = null; // Source file size, can exceed 4GB
  private Long irSizeBytes = DEFAULT_IR_SIZE;
  private Long archiveSizeBytes = null;

  // Event counts (cumulative _plus pattern)
  // Total count uses recordCount field
  private long countLevelDebugPlus = 0L;
  private long countLevelInfoPlus = 0L;
  private long countLevelWarnPlus = 0L;
  private long countLevelErrorPlus = 0L;
  private long countLevelFatalPlus = 0L;

  // Spark dimensions
  private String applicationId = "test-app-001";
  private String executorId = "executor-1";
  private String host = "spark-worker-01.example.com";

  // Retention
  private int retentionDays = DEFAULT_RETENTION_DAYS;
  private long expiresAt = DEFAULT_MIN_TIMESTAMP + (DEFAULT_RETENTION_DAYS * Timestamps.NANOS_PER_DAY);

  // Kafka source tracking
  private long sourceOffset = -1L; // -1 indicates not set (e.g., loaded from DB)
  private int sourcePartition = -1; // -1 indicates not set

  /** Set IR storage backend. */
  public FileRecordBuilder withIrStorageBackend(String backend) {
    this.irStorageBackend = backend;
    return this;
  }

  /** Set IR bucket. */
  public FileRecordBuilder withIrBucket(String bucket) {
    this.irBucket = bucket;
    return this;
  }

  /** Set IR path (auto-generated if not provided). */
  public FileRecordBuilder withIrPath(String path) {
    this.irPath = path;
    return this;
  }

  /** Set archive storage backend. */
  public FileRecordBuilder withArchiveStorageBackend(String backend) {
    this.archiveStorageBackend = backend;
    return this;
  }

  /** Set archive bucket. */
  public FileRecordBuilder withArchiveBucket(String bucket) {
    this.archiveBucket = bucket;
    return this;
  }

  /** Set archive path. */
  public FileRecordBuilder withArchivePath(String path) {
    this.archivePath = path;
    return this;
  }

  /** Set file state. */
  public FileRecordBuilder withState(FileState state) {
    this.state = state;
    return this;
  }

  /** Set min timestamp (earliest event in file, epoch nanoseconds). */
  public FileRecordBuilder withMinTimestamp(long timestamp) {
    this.minTimestamp = timestamp;
    // Update expiresAt to maintain consistency
    this.expiresAt = timestamp + (retentionDays * Timestamps.NANOS_PER_DAY);
    return this;
  }

  /** Set max timestamp (latest event in file, epoch nanoseconds). */
  public FileRecordBuilder withMaxTimestamp(long timestamp) {
    this.maxTimestamp = timestamp;
    return this;
  }

  /** Set record count (for early termination). */
  public FileRecordBuilder withRecordCount(long count) {
    this.recordCount = count;
    return this;
  }

  /** Set IR file size in bytes (nullable). */
  public FileRecordBuilder withIrSizeBytes(Long size) {
    this.irSizeBytes = size;
    return this;
  }

  /** Set archive size in bytes (nullable). */
  public FileRecordBuilder withArchiveSizeBytes(Long size) {
    this.archiveSizeBytes = size;
    return this;
  }

  /** Set Spark application ID. */
  public FileRecordBuilder withApplicationId(String appId) {
    this.applicationId = appId;
    return this;
  }

  /** Set Spark executor ID. */
  public FileRecordBuilder withExecutorId(String executorId) {
    this.executorId = executorId;
    return this;
  }

  /** Set host. */
  public FileRecordBuilder withHost(String host) {
    this.host = host;
    return this;
  }

  /** Set retention days. */
  public FileRecordBuilder withRetentionDays(int days) {
    this.retentionDays = days;
    // Update expiresAt to maintain consistency
    this.expiresAt = minTimestamp + (days * Timestamps.NANOS_PER_DAY);
    return this;
  }

  /** Set expires at timestamp (epoch nanoseconds). */
  public FileRecordBuilder withExpiresAt(long timestamp) {
    this.expiresAt = timestamp;
    return this;
  }

  /** Set Kafka source offset (for watermark tracking tests). */
  public FileRecordBuilder withSourceOffset(long offset) {
    this.sourceOffset = offset;
    return this;
  }

  /** Set Kafka source partition (for per-partition watermark tracking tests). */
  public FileRecordBuilder withSourcePartition(int partition) {
    this.sourcePartition = partition;
    return this;
  }

  // Named presets for common states

  /** Configure as a buffering IR file (actively receiving data). */
  public FileRecordBuilder buffering() {
    this.state = FileState.IR_ARCHIVE_BUFFERING;
    this.maxTimestamp = minTimestamp + 300L * Timestamps.NANOS_PER_SECOND; // 5 minutes after start
    this.archivePath = "";
    this.archiveStorageBackend = "";
    this.archiveBucket = "";
    this.archiveCreatedAt = 0L;
    this.archiveSizeBytes = null;
    return this;
  }

  /** Configure as a pending file (ready for consolidation). */
  public FileRecordBuilder pending() {
    this.state = FileState.IR_ARCHIVE_CONSOLIDATION_PENDING;
    this.maxTimestamp = minTimestamp + 600L * Timestamps.NANOS_PER_SECOND; // 10 minutes after start
    this.archivePath = "";
    this.archiveStorageBackend = "";
    this.archiveBucket = "";
    this.archiveCreatedAt = 0L;
    this.archiveSizeBytes = null;
    return this;
  }

  /**
   * Configure as a consolidated file (archive created). Uses ARCHIVE_CLOSED state (consolidation
   * complete, IR file may still exist for cleanup).
   */
  public FileRecordBuilder consolidated() {
    this.state = FileState.ARCHIVE_CLOSED;
    this.maxTimestamp = minTimestamp + 900L * Timestamps.NANOS_PER_SECOND; // 15 minutes after start
    this.archiveStorageBackend = "s3";
    this.archiveBucket = "clp-archives";
    this.archivePath = String.format("/clp-archives/spark/2024-01-01/%s.clp", applicationId);
    this.archiveCreatedAt = minTimestamp + 900L * Timestamps.NANOS_PER_SECOND;
    this.archiveSizeBytes = (long) DEFAULT_ARCHIVE_SIZE;
    return this;
  }

  /**
   * Build the FileRecord instance.
   *
   * <p>If irPath is not set, generates a unique path using UUID.
   */
  public FileRecord build() {
    // Auto-generate IR path if not provided
    if (irPath == null) {
      String uuid = UUID.randomUUID().toString();
      irPath = String.format("/spark/%s/%s/%s.clp.zst", applicationId, executorId, uuid);
    }

    FileRecord file = new FileRecord();
    file.setIrStorageBackend(irStorageBackend);
    file.setIrBucket(irBucket);
    file.setIrPath(irPath);
    file.setArchiveStorageBackend(archiveStorageBackend);
    file.setArchiveBucket(archiveBucket);
    file.setArchivePath(archivePath);
    file.setState(state);
    file.setMinTimestamp(minTimestamp);
    file.setMaxTimestamp(maxTimestamp);
    file.setArchiveCreatedAt(archiveCreatedAt);
    file.setRecordCount(recordCount);
    file.setRawSizeBytes(rawSizeBytes);
    file.setIrSizeBytes(irSizeBytes);
    file.setArchiveSizeBytes(archiveSizeBytes);
    file.setCount("agg_int/gte/level/debug", countLevelDebugPlus);
    file.setCount("agg_int/gte/level/info", countLevelInfoPlus);
    file.setCount("agg_int/gte/level/warn", countLevelWarnPlus);
    file.setCount("agg_int/gte/level/error", countLevelErrorPlus);
    file.setCount("agg_int/gte/level/fatal", countLevelFatalPlus);
    // Set dimensions using dynamic dimension map
    if (applicationId != null) {
      file.setDimension("dim/str128/application_id", applicationId);
    }
    if (executorId != null) {
      file.setDimension("dim/str128/executor_id", executorId);
    }
    if (host != null) {
      file.setDimension("dim/str128/host", host);
    }
    file.setRetentionDays(retentionDays);
    file.setExpiresAt(expiresAt);

    // Set source offset and partition for watermark tracking
    if (sourceOffset >= 0) {
      file.setSourceOffset(sourceOffset);
    }
    if (sourcePartition >= 0) {
      file.setSourcePartition(sourcePartition);
    }

    return file;
  }
}
