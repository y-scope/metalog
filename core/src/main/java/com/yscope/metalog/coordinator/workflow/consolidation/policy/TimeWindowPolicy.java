package com.yscope.metalog.coordinator.workflow.consolidation.policy;

import com.yscope.metalog.common.Timestamps;
import com.yscope.metalog.coordinator.workflow.consolidation.Policy;
import com.yscope.metalog.metastore.model.ConsolidationTask;
import com.yscope.metalog.metastore.model.FileRecord;
import com.yscope.metalog.metastore.model.FileState;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aggregation policy that groups files by dimension value and time window.
 *
 * <p>Grouping strategy: By grouping key dimension + time bucket (e.g., 15-minute windows)
 *
 * <p>Triggers:
 *
 * <ul>
 *   <li>Window close: Wall clock passes the window end time
 *   <li>Timeout: Files older than configured timeout
 * </ul>
 *
 * <p>Archive path format: /clp-archives/{dim-value}/YYYY-MM-DD/HH-mm.clp
 *
 * <p>Use case: Microservices logs where logs are grouped by service name and aggregated every 15
 * minutes.
 */
@PolicyType("timewindow")
public class TimeWindowPolicy implements Policy {
  private static final Logger logger = LoggerFactory.getLogger(TimeWindowPolicy.class);
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH-mm");

  private final String policyName;
  private final String groupingKey;
  private final int windowIntervalMinutes;
  private final boolean alignToWallClock;
  private final int timeoutHours;
  private final long targetArchiveSizeMb;
  private final long maxArchiveSizeMb;
  private final double learningRate;
  private final Map<String, Double> compressionRatios;
  private double defaultCompressionRatio;

  /** Create a TimeWindowPolicy with explicit parameters. */
  public TimeWindowPolicy(
      String policyName,
      String groupingKey,
      int windowIntervalMinutes,
      boolean alignToWallClock,
      int timeoutHours,
      long targetArchiveSizeMb) {
    if (windowIntervalMinutes <= 0) {
      throw new IllegalArgumentException(
          "windowIntervalMinutes must be positive, got: " + windowIntervalMinutes);
    }
    this.policyName = policyName;
    this.groupingKey = groupingKey;
    this.windowIntervalMinutes = windowIntervalMinutes;
    this.alignToWallClock = alignToWallClock;
    this.timeoutHours = timeoutHours;
    this.targetArchiveSizeMb = targetArchiveSizeMb;
    this.maxArchiveSizeMb = targetArchiveSizeMb * 2;
    this.learningRate = 0.1;
    this.compressionRatios = new ConcurrentHashMap<>();
    this.defaultCompressionRatio = 3.0;
  }

  /** Create a TimeWindowPolicy from YAML configuration. */
  public TimeWindowPolicy(PolicyConfig.PolicyDefinition definition) {
    this.policyName = definition.getName();
    this.groupingKey = definition.getGroupingKey();

    PolicyConfig.TimeWindowTrigger windowTrigger = definition.getTriggers().getTimeWindow();
    int interval = windowTrigger != null ? windowTrigger.getIntervalMinutes() : 15;
    if (interval <= 0) {
      throw new IllegalArgumentException(
          "windowIntervalMinutes must be positive, got: " + interval);
    }
    this.windowIntervalMinutes = interval;
    this.alignToWallClock = windowTrigger != null && windowTrigger.isAlignToWallClock();

    PolicyConfig.TimeoutTrigger timeoutTrigger = definition.getTriggers().getTimeout();
    this.timeoutHours = timeoutTrigger != null ? timeoutTrigger.getHours() : 1;

    this.targetArchiveSizeMb = definition.getArchiveSize().getTargetMb();
    this.maxArchiveSizeMb = definition.getArchiveSize().getMaxMb();
    this.learningRate = definition.getCompressionRatio().getLearningRate();
    this.defaultCompressionRatio = definition.getCompressionRatio().getInitialRatio();
    this.compressionRatios = new ConcurrentHashMap<>();
  }

  @Override
  public List<ConsolidationTask> shouldAggregate(List<FileRecord> files) {
    // Filter to CONSOLIDATION_PENDING files
    List<FileRecord> pendingFiles =
        files.stream()
            .filter(f -> f.getState() == FileState.IR_ARCHIVE_CONSOLIDATION_PENDING)
            .collect(Collectors.toList());

    if (pendingFiles.isEmpty()) {
      return Collections.emptyList();
    }

    // Group by dimension value + time window
    Map<String, List<FileRecord>> filesByGroup = new HashMap<>();
    for (FileRecord file : pendingFiles) {
      String groupKey = createGroupKey(file);
      filesByGroup.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(file);
    }

    List<ConsolidationTask> tasks = new ArrayList<>();

    for (Map.Entry<String, List<FileRecord>> entry : filesByGroup.entrySet()) {
      String groupKey = entry.getKey();
      List<FileRecord> groupFiles = entry.getValue();

      if (shouldTriggerAggregation(groupKey, groupFiles)) {
        tasks.addAll(createTasksForGroup(groupKey, groupFiles));
      }
    }

    return tasks;
  }

  /** Create a group key combining dimension value and time window. */
  private String createGroupKey(FileRecord file) {
    String dimValue = getDimensionValue(file);
    long windowStart = getWindowStart(file.getMinTimestamp());
    return dimValue + ":" + windowStart;
  }

  /** Extract the dimension value from the file based on grouping key. */
  private String getDimensionValue(FileRecord file) {
    // Look up the grouping key in the dynamic dimensions map
    String value = file.getStringDimension(groupingKey);
    return value != null ? value : "unknown";
  }

  /** Calculate the window start timestamp for a given event timestamp. */
  long getWindowStart(long timestamp) {
    // Always align to interval boundaries for consistent grouping
    long intervalNanos = windowIntervalMinutes * Timestamps.NANOS_PER_MINUTE;
    return (timestamp / intervalNanos) * intervalNanos;
  }

  /** Calculate the window end timestamp. */
  long getWindowEnd(long windowStart) {
    return windowStart + (windowIntervalMinutes * Timestamps.NANOS_PER_MINUTE);
  }

  /** Determine if aggregation should be triggered for this group. */
  private boolean shouldTriggerAggregation(String groupKey, List<FileRecord> files) {
    if (files.isEmpty()) {
      return false;
    }

    long now = Timestamps.nowNanos();

    // Parse window start from group key (dim value may contain ':')
    int lastColon = groupKey.lastIndexOf(':');
    long windowStart = Long.parseLong(groupKey.substring(lastColon + 1));
    long windowEnd = getWindowEnd(windowStart);

    // Trigger 1: Window has closed (wall clock passed window end)
    if (now >= windowEnd) {
      logger.info(
          "Triggering aggregation for group={} due to window close (windowEnd={}, now={})",
          groupKey,
          Timestamps.toInstant(windowEnd),
          Timestamps.toInstant(now));
      return true;
    }

    // Trigger 2: Timeout - oldest file exceeds timeout threshold
    long timeoutNanos = timeoutHours * Timestamps.NANOS_PER_HOUR;
    long oldestFileTime = files.stream().mapToLong(FileRecord::getMinTimestamp).min().orElse(now);

    if (now - oldestFileTime >= timeoutNanos) {
      logger.info(
          "Triggering aggregation for group={} due to timeout (age={}h)",
          groupKey,
          (now - oldestFileTime) / (double) Timestamps.NANOS_PER_HOUR);
      return true;
    }

    return false;
  }

  /** Create aggregation tasks for a group of files. */
  private List<ConsolidationTask> createTasksForGroup(String groupKey, List<FileRecord> files) {
    List<ConsolidationTask> tasks = new ArrayList<>();

    // Estimate total IR size
    long totalIrSize =
        files.stream().mapToLong(f -> f.getIrSizeBytes() != null ? f.getIrSizeBytes() : 0L).sum();

    if (totalIrSize == 0) {
      totalIrSize = files.size() * 1024L * 1024;
    }

    // Estimate archive size
    double compressionRatio = compressionRatios.getOrDefault(policyName, defaultCompressionRatio);
    long estimatedArchiveSize = (long) (totalIrSize / compressionRatio);
    long targetSizeBytes = targetArchiveSizeMb * 1024L * 1024;

    // Parse group key (dim value may contain ':')
    int lastColon = groupKey.lastIndexOf(':');
    String dimValue = lastColon > 0 ? groupKey.substring(0, lastColon) : "unknown";
    long windowStart = Long.parseLong(groupKey.substring(lastColon + 1));

    if (estimatedArchiveSize <= targetSizeBytes) {
      // Single archive
      ConsolidationTask task = createTask(dimValue, windowStart, files, 1, 1);
      task.setEstimatedArchiveSizeBytes(estimatedArchiveSize);
      tasks.add(task);
      logger.info(
          "Created single archive task for group={}, files={}, estimated_size={}MB",
          groupKey,
          files.size(),
          estimatedArchiveSize / 1024 / 1024);
    } else {
      // Split into multiple archives
      int numArchives = (int) Math.ceil((double) estimatedArchiveSize / targetSizeBytes);
      int filesPerArchive = (int) Math.ceil((double) files.size() / numArchives);

      logger.info(
          "Splitting group={} into {} archives (estimated_size={}MB, target={}MB)",
          groupKey,
          numArchives,
          estimatedArchiveSize / 1024 / 1024,
          targetArchiveSizeMb);

      for (int i = 0; i < numArchives; i++) {
        int start = i * filesPerArchive;
        if (start >= files.size()) break;
        int end = Math.min((i + 1) * filesPerArchive, files.size());
        List<FileRecord> chunk = files.subList(start, end);

        ConsolidationTask task = createTask(dimValue, windowStart, chunk, i + 1, numArchives);
        task.setEstimatedArchiveSizeBytes(estimatedArchiveSize / numArchives);
        tasks.add(task);
      }
    }

    return tasks;
  }

  /** Create an aggregation task. */
  private ConsolidationTask createTask(
      String dimValue, long windowStart, List<FileRecord> files, int partNum, int totalParts) {
    String archivePath = generateArchivePath(dimValue, windowStart, partNum, totalParts);

    List<String> irPaths = files.stream().map(FileRecord::getIrPath).collect(Collectors.toList());

    return new ConsolidationTask(archivePath, irPaths, dimValue);
  }

  /** Generate archive path: /clp-archives/{dim-value}/YYYY-MM-DD/HH-mm[-partN].clp */
  String generateArchivePath(String dimValue, long windowStart, int partNum, int totalParts) {
    LocalDateTime dateTime =
        LocalDateTime.ofInstant(Timestamps.toInstant(windowStart), ZoneOffset.UTC);

    String dateStr = dateTime.format(DATE_FORMATTER);
    String timeStr = dateTime.format(TIME_FORMATTER);

    // Sanitize dimension value for file path
    String safeDimValue = sanitizePathComponent(dimValue);

    String filename = timeStr;
    if (totalParts > 1) {
      filename += String.format("-part%d", partNum);
    }
    filename += ".clp";

    return String.format("/clp-archives/%s/%s/%s", safeDimValue, dateStr, filename);
  }

  /** Sanitize a string for use in file paths. */
  private String sanitizePathComponent(String value) {
    if (value == null || value.isEmpty()) {
      return "unknown";
    }
    // Replace unsafe characters with underscores
    return value.replaceAll("[^a-zA-Z0-9._-]", "_");
  }

  @Override
  public void learnFromResult(ConsolidationTask task, long totalIrSizeBytes) {
    if (task.getActualArchiveSizeBytes() == 0 || totalIrSizeBytes == 0) {
      return;
    }

    double actualRatio = (double) totalIrSizeBytes / task.getActualArchiveSizeBytes();
    double currentRatio = compressionRatios.getOrDefault(policyName, defaultCompressionRatio);
    double newRatio = (1 - learningRate) * currentRatio + learningRate * actualRatio;

    compressionRatios.put(policyName, newRatio);

    logger.info(
        "Updated compression ratio for {}: {}x (previous={}x, observed={}x)",
        policyName,
        String.format("%.2f", newRatio),
        String.format("%.2f", currentRatio),
        String.format("%.2f", actualRatio));
  }

  @Override
  public String getPolicyName() {
    return "TimeWindowPolicy:" + policyName;
  }

  @Override
  public void transferStateFrom(Policy oldPolicy) {
    if (oldPolicy instanceof TimeWindowPolicy oldWindow) {
      transferState(oldWindow);
      logger.debug("Transferred state from TimeWindowPolicy");
    }
  }

  /** Transfer learned state from another TimeWindowPolicy instance. */
  public void transferState(TimeWindowPolicy other) {
    this.compressionRatios.clear();
    this.compressionRatios.putAll(other.compressionRatios);
    // Transfer the effective compression ratio as the new default
    this.defaultCompressionRatio = other.getCompressionRatio();
    logger.debug("Transferred state from previous TimeWindowPolicy instance");
  }

  // Getters for testing
  public int getWindowIntervalMinutes() {
    return windowIntervalMinutes;
  }

  public boolean isAlignToWallClock() {
    return alignToWallClock;
  }

  public int getTimeoutHours() {
    return timeoutHours;
  }

  public String getGroupingKey() {
    return groupingKey;
  }

  public double getCompressionRatio() {
    return compressionRatios.getOrDefault(policyName, defaultCompressionRatio);
  }
}
