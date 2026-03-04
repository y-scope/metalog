package com.yscope.clp.service.query.core.splits;

import java.util.List;
import java.util.Map;

/**
 * Represents a queryable unit (file or archive segment) for split pruning.
 *
 * <p>A split contains metadata that enables pruning decisions:
 *
 * <ul>
 *   <li>Dimension values for filtering
 *   <li>Aggregate values for filtering
 *   <li>Time bounds for range queries
 *   <li>Sketch columns for probabilistic filtering
 * </ul>
 *
 * <p>Splits can represent:
 *
 * <ul>
 *   <li>A single CLP-IR file
 *   <li>A single CLP-Archive
 *   <li>A subset of files within an archive (for selective access)
 * </ul>
 */
public record Split(
    long id,
    String objectPath,
    String objectStorageBackend,
    String objectBucket,
    String archivePath,
    String archiveStorageBackend,
    String archiveBucket,
    long minTimestamp,
    long maxTimestamp,
    String state,
    Map<String, String> dimensions,
    List<SplitAgg> aggs,
    long recordCount,
    long sizeBytes) {

  /** Check if this split is an archive (vs standalone IR file). */
  public boolean isArchive() {
    return archivePath != null && !archivePath.isEmpty();
  }

  /** Get the effective path to read (archive path if consolidated, IR path otherwise). */
  public String effectivePath() {
    return isArchive() ? archivePath : objectPath;
  }

  /** Get a dimension value, returning null if not present. */
  public String getDimension(String dimColumn) {
    return dimensions.get(dimColumn);
  }

  /** Check if this split matches dimension filters. */
  public boolean matchesDimensions(Map<String, String> filters) {
    for (var entry : filters.entrySet()) {
      String value = dimensions.get(entry.getKey());
      if (value == null || !value.equals(entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  /** Check if this split overlaps with a time range. */
  public boolean overlapsTimeRange(Long queryMinTimestamp, Long queryMaxTimestamp) {
    if (queryMinTimestamp != null && maxTimestamp < queryMinTimestamp) {
      return false;
    }
    if (queryMaxTimestamp != null && minTimestamp > queryMaxTimestamp) {
      return false;
    }
    return true;
  }
}
