package com.yscope.clp.service.metastore.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yscope.clp.service.common.Timestamps;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a CLP-IR file metadata entry in the database.
 *
 * <p>Maps to a row in the metadata table (table name is configurable). See:
 * docs/architecture/metadata-tables.md
 */
public class FileRecord {
  // Primary key components (IR identifiers)
  private String irStorageBackend;
  private String irBucket;
  private String irPath;

  // Primary key components (Archive identifiers)
  private String archiveStorageBackend;
  private String archiveBucket;
  private String archivePath;

  // State
  private FileState state;

  // Event time bounds (epoch nanoseconds, 0 = unknown)
  private long minTimestamp;
  private long maxTimestamp;
  private long archiveCreatedAt; // Archive lifecycle timestamp (not event bounds)

  // Record count (for early termination)
  private long recordCount;

  // Size metrics (bytes)
  private Long rawSizeBytes; // Nullable - can exceed 4GB
  private Long irSizeBytes; // Nullable - max 4GB per file
  private Long archiveSizeBytes; // Nullable - max 4GB per file

  // Dynamic dimensions (key -> value)
  // Keys use NUL-separated composite format (e.g., "service\0str\064") or self-describing
  // format (e.g., "dim/str64/service"). BatchingWriter resolves these to placeholder columns.
  private final Map<String, Object> dimensions = new HashMap<>();

  // Dynamic integer agg values (original key -> value)
  // Keys use composite format (e.g., "GTE\0level\0error") or self-describing format
  // (e.g., "agg_int/gte/level/error"). BatchingWriter resolves these to placeholder columns.
  private final Map<String, Long> counts = new HashMap<>();

  // Dynamic float agg values (original key -> value)
  // Keys use composite format (e.g., "AVG\0latency\0") or self-describing format
  // (e.g., "agg_float/avg/latency"). BatchingWriter resolves these to placeholder columns.
  private final Map<String, Double> floatCounts = new HashMap<>();

  // Retention
  private int retentionDays;
  private long expiresAt;

  // Kafka source tracking (for offset watermark)
  private long sourceOffset = -1L; // -1 indicates not set (e.g., loaded from DB)
  private int sourcePartition = -1; // -1 indicates not set

  // Constructors
  public FileRecord() {
    // Default constructor for frameworks
  }

  /**
   * Create an FileRecord with just the IR path. Other fields should be set via setters or populated
   * from JSON/database.
   */
  public FileRecord(String irPath) {
    this.irStorageBackend = "";
    this.irBucket = "";
    this.irPath = irPath;
    this.archiveStorageBackend = "";
    this.archiveBucket = "";
    this.archivePath = "";
    this.state = FileState.IR_ARCHIVE_BUFFERING;
    this.minTimestamp = Timestamps.nowNanos();
    this.maxTimestamp = this.minTimestamp;
    this.retentionDays = 30;
    this.expiresAt = this.minTimestamp + (retentionDays * Timestamps.NANOS_PER_DAY);
  }

  // Getters and Setters
  public String getIrStorageBackend() {
    return irStorageBackend;
  }

  public void setIrStorageBackend(String irStorageBackend) {
    this.irStorageBackend = irStorageBackend;
  }

  public String getIrBucket() {
    return irBucket;
  }

  public void setIrBucket(String irBucket) {
    this.irBucket = irBucket;
  }

  public String getIrPath() {
    return irPath;
  }

  public void setIrPath(String irPath) {
    this.irPath = irPath;
  }

  public String getArchiveStorageBackend() {
    return archiveStorageBackend;
  }

  public void setArchiveStorageBackend(String archiveStorageBackend) {
    this.archiveStorageBackend = archiveStorageBackend;
  }

  public String getArchiveBucket() {
    return archiveBucket;
  }

  public void setArchiveBucket(String archiveBucket) {
    this.archiveBucket = archiveBucket;
  }

  public String getArchivePath() {
    return archivePath;
  }

  public void setArchivePath(String archivePath) {
    this.archivePath = archivePath;
  }

  public FileState getState() {
    return state;
  }

  public void setState(FileState state) {
    this.state = state;
  }

  public long getMinTimestamp() {
    return minTimestamp;
  }

  public void setMinTimestamp(long minTimestamp) {
    this.minTimestamp = minTimestamp;
  }

  public long getMaxTimestamp() {
    return maxTimestamp;
  }

  public void setMaxTimestamp(long maxTimestamp) {
    this.maxTimestamp = maxTimestamp;
  }

  public long getArchiveCreatedAt() {
    return archiveCreatedAt;
  }

  public void setArchiveCreatedAt(long archiveCreatedAt) {
    this.archiveCreatedAt = archiveCreatedAt;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public void setRecordCount(long recordCount) {
    this.recordCount = recordCount;
  }

  public Long getRawSizeBytes() {
    return rawSizeBytes;
  }

  public void setRawSizeBytes(Long rawSizeBytes) {
    this.rawSizeBytes = rawSizeBytes;
  }

  public Long getIrSizeBytes() {
    return irSizeBytes;
  }

  public void setIrSizeBytes(Long irSizeBytes) {
    this.irSizeBytes = irSizeBytes;
  }

  public Long getArchiveSizeBytes() {
    return archiveSizeBytes;
  }

  public void setArchiveSizeBytes(Long archiveSizeBytes) {
    this.archiveSizeBytes = archiveSizeBytes;
  }

  // Dynamic dimension accessors

  /**
   * Get all dimensions as an unmodifiable map.
   *
   * @return Map of dimension column names to values
   */
  public Map<String, Object> getDimensions() {
    return Collections.unmodifiableMap(dimensions);
  }

  /**
   * Get a dimension value.
   *
   * @param columnName The dimension key (e.g., "service\0str\064" or "dim/str64/service")
   * @return The dimension value, or null if not set
   */
  public Object getDimension(String columnName) {
    return dimensions.get(columnName);
  }

  /**
   * Get a string dimension value.
   *
   * @param columnName The dimension key (e.g., "app_id\0str\0128" or "dim/str128/app_id")
   * @return The string value, or null if not set or not a string
   */
  public String getStringDimension(String columnName) {
    Object value = dimensions.get(columnName);
    return value instanceof String ? (String) value : null;
  }

  /**
   * Set a dimension value.
   *
   * @param columnName The dimension key (e.g., "service\0str\064" or "dim/str64/service")
   * @param value The dimension value
   */
  public void setDimension(String columnName, Object value) {
    if (value != null) {
      dimensions.put(columnName, value);
    } else {
      dimensions.remove(columnName);
    }
  }

  // Dynamic count accessors

  /**
   * Get all dynamic counts as an unmodifiable map.
   *
   * @return Map of count column names to values
   */
  public Map<String, Long> getCounts() {
    return Collections.unmodifiableMap(counts);
  }

  /**
   * Get a dynamic count value.
   *
   * @param columnName The count key (e.g., "GTE\0level\0error" or "agg_int/gte/level/error")
   * @return The count value, or null if not set
   * @deprecated Use {@link #getIntAgg(String)} instead. "count" is misleading - this stores any
   *     integer aggregation (sum, avg, min, max, etc.), not just counts.
   */
  @Deprecated
  public Long getCount(String columnName) {
    return counts.get(columnName);
  }

  /**
   * Set a dynamic count value.
   *
   * @param columnName The count key (e.g., "GTE\0level\0error" or "agg_int/gte/level/error")
   * @param value The count value
   * @deprecated Use {@link #setIntAgg(String, long)} instead. "count" is misleading - this stores
   *     any integer aggregation (sum, avg, min, max, etc.), not just counts.
   */
  @Deprecated
  public void setCount(String columnName, long value) {
    counts.put(columnName, value);
  }

  /**
   * Get a count value, returning 0 if not set.
   *
   * @param columnName The count column name
   * @return The count value, or 0 if not set
   * @deprecated Use {@link #getIntAggOrZero(String)} instead. "count" is misleading - this stores
   *     any integer aggregation (sum, avg, min, max, etc.), not just counts.
   */
  @Deprecated
  public long getCountOrZero(String columnName) {
    Long value = counts.get(columnName);
    return value != null ? value : 0L;
  }

  // Preferred integer aggregation accessors (replaces "count" naming)

  /**
   * Get an integer aggregation value.
   *
   * @param key The aggregation key (e.g., "agg_int/gte/level/error", "agg_int/sum/bytes")
   * @return The aggregation value, or null if not set
   */
  public Long getIntAgg(String key) {
    return counts.get(key);
  }

  /**
   * Set an integer aggregation value.
   *
   * @param key The aggregation key (e.g., "agg_int/gte/level/error", "agg_int/sum/bytes")
   * @param value The aggregation value
   */
  public void setIntAgg(String key, long value) {
    counts.put(key, value);
  }

  /**
   * Get an integer aggregation value, returning 0 if not set.
   *
   * @param key The aggregation key
   * @return The aggregation value, or 0 if not set
   */
  public long getIntAggOrZero(String key) {
    Long value = counts.get(key);
    return value != null ? value : 0L;
  }

  // Dynamic float count accessors (deprecated - use float agg accessors instead)

  @JsonProperty("float_counts")
  public Map<String, Double> getFloatCounts() {
    return Collections.unmodifiableMap(floatCounts);
  }

  /**
   * @deprecated Use {@link #getFloatAgg(String)} instead. "count" is misleading - this stores any
   *     float aggregation (sum, avg, min, max, etc.), not just counts.
   */
  @Deprecated
  public Double getFloatCount(String columnName) {
    return floatCounts.get(columnName);
  }

  /**
   * @deprecated Use {@link #getFloatAggOrZero(String)} instead. "count" is misleading - this
   *     stores any float aggregation (sum, avg, min, max, etc.), not just counts.
   */
  @Deprecated
  public double getFloatCountOrZero(String columnName) {
    Double value = floatCounts.get(columnName);
    return value != null ? value : 0.0;
  }

  /**
   * @deprecated Use {@link #setFloatAgg(String, double)} instead. "count" is misleading - this
   *     stores any float aggregation (sum, avg, min, max, etc.), not just counts.
   */
  @Deprecated
  public void setFloatCount(String columnName, double value) {
    if (!Double.isFinite(value)) {
      throw new IllegalArgumentException(
          "Float count value must be finite: " + columnName + "=" + value);
    }
    floatCounts.put(columnName, value);
  }

  @JsonProperty("float_counts")
  public void setFloatCounts(Map<String, Double> values) {
    if (values != null) {
      values.forEach(this::setFloatCount);
    }
  }

  // Preferred float aggregation accessors (replaces "float count" naming)

  /**
   * Get a float aggregation value.
   *
   * @param key The aggregation key (e.g., "agg_float/avg/latency", "agg_float/sum/cpu_usage")
   * @return The aggregation value, or null if not set
   */
  public Double getFloatAgg(String key) {
    return floatCounts.get(key);
  }

  /**
   * Set a float aggregation value.
   *
   * @param key The aggregation key (e.g., "agg_float/avg/latency", "agg_float/sum/cpu_usage")
   * @param value The aggregation value
   */
  public void setFloatAgg(String key, double value) {
    if (!Double.isFinite(value)) {
      throw new IllegalArgumentException(
          "Float aggregation value must be finite: " + key + "=" + value);
    }
    floatCounts.put(key, value);
  }

  /**
   * Get a float aggregation value, returning 0.0 if not set.
   *
   * @param key The aggregation key
   * @return The aggregation value, or 0.0 if not set
   */
  public double getFloatAggOrZero(String key) {
    Double value = floatCounts.get(key);
    return value != null ? value : 0.0;
  }

  public int getRetentionDays() {
    return retentionDays;
  }

  public void setRetentionDays(int retentionDays) {
    this.retentionDays = retentionDays;
  }

  public long getExpiresAt() {
    return expiresAt;
  }

  public void setExpiresAt(long expiresAt) {
    this.expiresAt = expiresAt;
  }

  /**
   * Get the Kafka source offset this record came from.
   *
   * @return The Kafka offset, or -1 if not set (e.g., record loaded from database)
   */
  public long getSourceOffset() {
    return sourceOffset;
  }

  /**
   * Set the Kafka source offset this record came from.
   *
   * @param sourceOffset The Kafka offset
   */
  public void setSourceOffset(long sourceOffset) {
    this.sourceOffset = sourceOffset;
  }

  /**
   * Check if this record has a valid source offset.
   *
   * @return true if sourceOffset is set (>= 0)
   */
  public boolean hasSourceOffset() {
    return sourceOffset >= 0;
  }

  /**
   * Get the Kafka source partition this record came from.
   *
   * @return The Kafka partition, or -1 if not set
   */
  public int getSourcePartition() {
    return sourcePartition;
  }

  /**
   * Set the Kafka source partition this record came from.
   *
   * @param sourcePartition The Kafka partition
   */
  public void setSourcePartition(int sourcePartition) {
    this.sourcePartition = sourcePartition;
  }

  /**
   * Check if this record has a valid source partition.
   *
   * @return true if sourcePartition is set (>= 0)
   */
  public boolean hasSourcePartition() {
    return sourcePartition >= 0;
  }

  // Utility methods

  /** Get the composite primary key as a string for logging/debugging. */
  public String getCompositeKeyString() {
    return String.format(
        "%s|%s|%s|%s|%s|%s",
        irStorageBackend, irBucket, irPath, archiveStorageBackend, archiveBucket, archivePath);
  }

  /** Check if this file has been consolidated (has archive path). */
  public boolean isConsolidated() {
    return archivePath != null && !archivePath.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FileRecord irFile = (FileRecord) o;
    return Objects.equals(irStorageBackend, irFile.irStorageBackend)
        && Objects.equals(irBucket, irFile.irBucket)
        && Objects.equals(irPath, irFile.irPath)
        && Objects.equals(archiveStorageBackend, irFile.archiveStorageBackend)
        && Objects.equals(archiveBucket, irFile.archiveBucket)
        && Objects.equals(archivePath, irFile.archivePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        irStorageBackend, irBucket, irPath, archiveStorageBackend, archiveBucket, archivePath);
  }

  @Override
  public String toString() {
    return String.format(
        "FileRecord{state=%s, path=%s, dims=%d, offset=%d}",
        state, irPath, dimensions.size(), sourceOffset);
  }
}
