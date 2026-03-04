package com.yscope.metalog.coordinator.workflow.consolidation.policy;

import com.yscope.metalog.common.Timestamps;
import com.yscope.metalog.coordinator.workflow.consolidation.Policy;
import com.yscope.metalog.metastore.model.ConsolidationTask;
import com.yscope.metalog.metastore.model.FileRecord;
import com.yscope.metalog.metastore.model.FileState;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aggregation policy for Spark jobs.
 *
 * <p>Grouping strategy: By application_id (one archive per Spark job)
 *
 * <p>Triggers: - Job complete: All executor files are closed (PENDING state) - Timeout: Files older
 * than configured timeout (e.g., 4 hours)
 *
 * <p>Archive size management: - Learn compression ratios from completed tasks - Estimate archive
 * size before aggregation - Split large jobs into multiple archives if needed
 *
 * <p>See: docs/design/ir-archive-consolidation.md - Aggregation Policy section
 */
@PolicyType("spark")
public class SparkJobPolicy implements Policy {
  private static final Logger logger = LoggerFactory.getLogger(SparkJobPolicy.class);

  private final String policyName;
  private final long timeoutHours;
  private final long targetArchiveSizeMb;
  private final int minFilesForJobComplete;
  private final Map<String, Double> compressionRatios; // table -> ratio
  private final double learningRate;
  private double defaultCompressionRatio;

  public SparkJobPolicy(long timeoutHours, long targetArchiveSizeMb) {
    this.policyName = "spark";
    this.timeoutHours = timeoutHours;
    this.targetArchiveSizeMb = targetArchiveSizeMb;
    this.minFilesForJobComplete = 3;
    this.compressionRatios = new ConcurrentHashMap<>();
    this.learningRate = 0.1; // Exponential moving average weight
    this.defaultCompressionRatio = 3.0; // Conservative initial estimate
  }

  /** Create a SparkJobPolicy from YAML configuration. */
  public SparkJobPolicy(PolicyConfig.PolicyDefinition definition) {
    this.policyName = definition.getName();

    PolicyConfig.TimeoutTrigger timeoutTrigger = definition.getTriggers().getTimeout();
    this.timeoutHours = timeoutTrigger != null ? timeoutTrigger.getHours() : 4;

    PolicyConfig.JobCompleteTrigger jobTrigger = definition.getTriggers().getJobComplete();
    this.minFilesForJobComplete = jobTrigger != null ? jobTrigger.getMinFiles() : 3;

    this.targetArchiveSizeMb = definition.getArchiveSize().getTargetMb();
    this.learningRate = definition.getCompressionRatio().getLearningRate();
    this.defaultCompressionRatio = definition.getCompressionRatio().getInitialRatio();
    this.compressionRatios = new ConcurrentHashMap<>();
  }

  private static final String DIM_APPLICATION_ID = "dim/str128/application_id";

  @Override
  public List<ConsolidationTask> shouldAggregate(List<FileRecord> files) {
    // Group files by application_id dimension
    Map<String, List<FileRecord>> filesByApp =
        files.stream()
            .filter(f -> f.getState() == FileState.IR_ARCHIVE_CONSOLIDATION_PENDING)
            .collect(
                Collectors.groupingBy(
                    f -> {
                      String appId = f.getStringDimension(DIM_APPLICATION_ID);
                      return appId != null ? appId : "unknown";
                    }));

    List<ConsolidationTask> tasks = new ArrayList<>();

    for (Map.Entry<String, List<FileRecord>> entry : filesByApp.entrySet()) {
      String appId = entry.getKey();
      List<FileRecord> appFiles = entry.getValue();

      // Check triggers
      if (shouldTriggerAggregation(appId, appFiles)) {
        tasks.addAll(createTasksForApp(appId, appFiles));
      }
    }

    return tasks;
  }

  /** Determine if aggregation should be triggered for this application. */
  private boolean shouldTriggerAggregation(String appId, List<FileRecord> files) {
    if (files.isEmpty()) {
      return false;
    }

    // Trigger 1: Timeout - if oldest file exceeds timeout
    long now = Timestamps.nowNanos();
    long timeoutNanos = timeoutHours * Timestamps.NANOS_PER_HOUR;

    long oldestFileAge = files.stream().mapToLong(FileRecord::getMinTimestamp).min().orElse(now);

    if (now - oldestFileAge >= timeoutNanos) {
      logger.info(
          "Triggering aggregation for app={} due to timeout (age={}h)",
          appId,
          (now - oldestFileAge) / (double) Timestamps.NANOS_PER_HOUR);
      return true;
    }

    // Trigger 2: Job complete detection
    // In a real system, we'd check if all expected executors have reported.
    // For POC, we assume if files are in PENDING state, the job is complete.
    // This is a simplification - production would need more sophisticated logic.
    boolean jobComplete = checkJobComplete(appId, files);
    if (jobComplete) {
      logger.info(
          "Triggering aggregation for app={} due to job completion (files={})",
          appId,
          files.size());
      return true;
    }

    return false;
  }

  /**
   * Check if a Spark job is complete.
   *
   * <p>In this POC, we simplify by assuming that if we have multiple files in PENDING state, the
   * job is complete. Production would check: - No files in BUFFERING state for this app - Expected
   * executor count reached - Job status from Spark metadata
   */
  private boolean checkJobComplete(String appId, List<FileRecord> files) {
    // Simplified: If we have minFilesForJobComplete+ files in PENDING, consider job complete
    // Real implementation would query for BUFFERING files and check if count is 0
    return files.size() >= minFilesForJobComplete;
  }

  /**
   * Create aggregation tasks for an application.
   *
   * <p>May create multiple tasks if the estimated archive size exceeds target.
   */
  private List<ConsolidationTask> createTasksForApp(String appId, List<FileRecord> files) {
    List<ConsolidationTask> tasks = new ArrayList<>();

    // Estimate total IR size
    long totalIrSize =
        files.stream().mapToLong(f -> f.getIrSizeBytes() != null ? f.getIrSizeBytes() : 0L).sum();

    // If no size info, estimate based on file count (POC fallback)
    if (totalIrSize == 0) {
      totalIrSize = files.size() * 1024L * 1024; // 1MB per file estimate
    }

    // Estimate archive size
    double compressionRatio = compressionRatios.getOrDefault("spark", defaultCompressionRatio);
    long estimatedArchiveSize = (long) (totalIrSize / compressionRatio);

    long targetSizeBytes = targetArchiveSizeMb * 1024L * 1024;

    if (estimatedArchiveSize <= targetSizeBytes) {
      // Single archive
      ConsolidationTask task = createTask(appId, files, 1, 1);
      task.setEstimatedArchiveSizeBytes(estimatedArchiveSize);
      tasks.add(task);

      logger.info(
          "Created single archive task for app={}, files={}, estimated_size={}MB",
          appId,
          files.size(),
          estimatedArchiveSize / 1024 / 1024);
    } else {
      // Split into multiple archives
      int numArchives = (int) Math.ceil((double) estimatedArchiveSize / targetSizeBytes);
      int filesPerArchive = (int) Math.ceil((double) files.size() / numArchives);

      logger.info(
          "Splitting app={} into {} archives (estimated_size={}MB, target={}MB)",
          appId,
          numArchives,
          estimatedArchiveSize / 1024 / 1024,
          targetArchiveSizeMb);

      for (int i = 0; i < numArchives; i++) {
        int start = i * filesPerArchive;
        if (start >= files.size()) {
          break; // All files have been assigned to archives
        }
        int end = Math.min((i + 1) * filesPerArchive, files.size());
        List<FileRecord> chunk = files.subList(start, end);

        ConsolidationTask task = createTask(appId, chunk, i + 1, numArchives);
        task.setEstimatedArchiveSizeBytes(estimatedArchiveSize / numArchives);
        tasks.add(task);
      }
    }

    return tasks;
  }

  /** Create an aggregation task. */
  private ConsolidationTask createTask(
      String appId, List<FileRecord> files, int partNum, int totalParts) {
    // Generate archive path: /clp-archives/spark/YYYY-MM-DD/app-id[-partN].clp
    String archivePath = generateArchivePath(appId, partNum, totalParts);

    List<String> irPaths = files.stream().map(FileRecord::getIrPath).collect(Collectors.toList());

    return new ConsolidationTask(archivePath, irPaths, appId);
  }

  /**
   * Generate archive path following the pattern: /clp-archives/spark/YYYY-MM-DD/app-id[-partN].clp
   */
  private String generateArchivePath(String appId, int partNum, int totalParts) {
    LocalDate date = LocalDate.now(ZoneOffset.UTC);
    String dateStr = date.format(DateTimeFormatter.ISO_LOCAL_DATE);

    String filename = appId;
    if (totalParts > 1) {
      filename += String.format("-part%d", partNum);
    }
    filename += ".clp";

    return String.format("/clp-archives/spark/%s/%s", dateStr, filename);
  }

  @Override
  public void learnFromResult(ConsolidationTask task, long totalIrSizeBytes) {
    if (task.getActualArchiveSizeBytes() == 0 || totalIrSizeBytes == 0) {
      return; // No data to learn from
    }

    double actualRatio = (double) totalIrSizeBytes / task.getActualArchiveSizeBytes();

    // Exponential moving average
    double currentRatio = compressionRatios.getOrDefault("spark", defaultCompressionRatio);
    double newRatio = (1 - learningRate) * currentRatio + learningRate * actualRatio;

    compressionRatios.put("spark", newRatio);

    logger.info(
        "Updated compression ratio: {}x (previous={}x, observed={}x)",
        String.format("%.2f", newRatio),
        String.format("%.2f", currentRatio),
        String.format("%.2f", actualRatio));
  }

  @Override
  public String getPolicyName() {
    return "SparkJobPolicy:" + policyName;
  }

  @Override
  public void transferStateFrom(Policy oldPolicy) {
    if (oldPolicy instanceof SparkJobPolicy oldSpark) {
      this.defaultCompressionRatio = oldSpark.getDefaultCompressionRatio();
      logger.debug(
          "Transferred state from SparkJobPolicy: compressionRatio={}",
          oldSpark.getDefaultCompressionRatio());
    }
  }

  // Getters for testing
  public double getCompressionRatio(String table) {
    return compressionRatios.getOrDefault(table, defaultCompressionRatio);
  }

  public double getDefaultCompressionRatio() {
    return defaultCompressionRatio;
  }

  public void setDefaultCompressionRatio(double ratio) {
    this.defaultCompressionRatio = ratio;
  }
}
