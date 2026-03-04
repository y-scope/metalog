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
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aggregation policy for audit logs.
 *
 * <p>Grouping strategy: No grouping by dimension - all files are candidates for batching
 *
 * <p>Triggers:
 *
 * <ul>
 *   <li>Size threshold: Total IR size exceeds configured minimum
 *   <li>Timeout: Files older than configured timeout
 * </ul>
 *
 * <p>Archive path format: /clp-archives/audit/YYYY-MM-DD/{timestamp}-{uuid}.clp
 *
 * <p>Key characteristic: Immutable compression ratio (no learning). This ensures audit archives
 * have predictable, reproducible compression for compliance and forensic purposes.
 *
 * <p>Use case: Security audit logs that need deterministic archival behavior.
 */
@PolicyType("audit")
public class AuditPolicy implements Policy {
  private static final Logger logger = LoggerFactory.getLogger(AuditPolicy.class);
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

  private final String policyName;
  private final long sizeThresholdBytes;
  private final int timeoutHours;
  private final long targetArchiveSizeMb;
  private final double compressionRatio; // Immutable - no learning
  private final boolean immutable;

  /** Create an AuditPolicy with explicit parameters. */
  public AuditPolicy(
      String policyName,
      long sizeThresholdBytes,
      int timeoutHours,
      long targetArchiveSizeMb,
      double compressionRatio) {
    this.policyName = policyName;
    this.sizeThresholdBytes = sizeThresholdBytes;
    this.timeoutHours = timeoutHours;
    this.targetArchiveSizeMb = targetArchiveSizeMb;
    this.compressionRatio = compressionRatio;
    this.immutable = true;
  }

  /** Create an AuditPolicy from YAML configuration. */
  public AuditPolicy(PolicyConfig.PolicyDefinition definition) {
    this.policyName = definition.getName();

    PolicyConfig.SizeThresholdTrigger sizeTrigger = definition.getTriggers().getSizeThreshold();
    this.sizeThresholdBytes = sizeTrigger != null ? sizeTrigger.getMinBytes() : 10 * 1024 * 1024;

    PolicyConfig.TimeoutTrigger timeoutTrigger = definition.getTriggers().getTimeout();
    this.timeoutHours = timeoutTrigger != null ? timeoutTrigger.getHours() : 24;

    this.targetArchiveSizeMb = definition.getArchiveSize().getTargetMb();
    this.compressionRatio = definition.getCompressionRatio().getInitialRatio();
    this.immutable = definition.isImmutable();
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

    List<ConsolidationTask> tasks = new ArrayList<>();

    // Calculate total size
    long totalIrSize =
        pendingFiles.stream()
            .mapToLong(f -> f.getIrSizeBytes() != null ? f.getIrSizeBytes() : 0L)
            .sum();

    // Check timeout trigger first
    long now = Timestamps.nowNanos();
    long timeoutNanos = timeoutHours * Timestamps.NANOS_PER_HOUR;
    long oldestFileTime =
        pendingFiles.stream().mapToLong(FileRecord::getMinTimestamp).min().orElse(now);

    boolean timeoutTriggered = (now - oldestFileTime >= timeoutNanos);

    // Check size threshold trigger
    boolean sizeTriggered = totalIrSize >= sizeThresholdBytes;

    if (!timeoutTriggered && !sizeTriggered) {
      logger.debug(
          "Audit policy not triggered: totalSize={}MB (threshold={}MB), age={}h (timeout={}h)",
          totalIrSize / 1024.0 / 1024.0,
          sizeThresholdBytes / 1024.0 / 1024.0,
          (now - oldestFileTime) / (double) Timestamps.NANOS_PER_HOUR,
          timeoutHours);
      return Collections.emptyList();
    }

    String triggerReason = timeoutTriggered ? "timeout" : "size_threshold";
    logger.info(
        "Audit policy triggered by {}: {} files, {}MB total",
        triggerReason,
        pendingFiles.size(),
        totalIrSize / 1024.0 / 1024.0);

    // Create tasks, potentially splitting large batches
    tasks.addAll(createTasks(pendingFiles, totalIrSize));

    return tasks;
  }

  /** Create aggregation tasks, splitting if necessary. */
  private List<ConsolidationTask> createTasks(List<FileRecord> files, long totalIrSize) {
    List<ConsolidationTask> tasks = new ArrayList<>();

    // Estimate archive size
    long estimatedArchiveSize = (long) (totalIrSize / compressionRatio);
    long targetSizeBytes = targetArchiveSizeMb * 1024 * 1024;

    if (estimatedArchiveSize <= targetSizeBytes) {
      // Single archive
      ConsolidationTask task = createTask(files, 1, 1);
      task.setEstimatedArchiveSizeBytes(estimatedArchiveSize);
      tasks.add(task);
      logger.info(
          "Created single audit archive task: {} files, estimated {}MB",
          files.size(),
          estimatedArchiveSize / 1024 / 1024);
    } else {
      // Split into multiple archives
      int numArchives = (int) Math.ceil((double) estimatedArchiveSize / targetSizeBytes);
      int filesPerArchive = (int) Math.ceil((double) files.size() / numArchives);

      logger.info(
          "Splitting audit logs into {} archives (estimated {}MB, target {}MB)",
          numArchives,
          estimatedArchiveSize / 1024 / 1024,
          targetArchiveSizeMb);

      for (int i = 0; i < numArchives; i++) {
        int start = i * filesPerArchive;
        if (start >= files.size()) break;
        int end = Math.min((i + 1) * filesPerArchive, files.size());
        List<FileRecord> chunk = files.subList(start, end);

        ConsolidationTask task = createTask(chunk, i + 1, numArchives);
        task.setEstimatedArchiveSizeBytes(estimatedArchiveSize / numArchives);
        tasks.add(task);
      }
    }

    return tasks;
  }

  /** Create an aggregation task. */
  private ConsolidationTask createTask(List<FileRecord> files, int partNum, int totalParts) {
    String archivePath = generateArchivePath(partNum, totalParts);

    List<String> irPaths = files.stream().map(FileRecord::getIrPath).collect(Collectors.toList());

    return new ConsolidationTask(archivePath, irPaths, "audit");
  }

  /** Generate archive path: /clp-archives/audit/YYYY-MM-DD/{timestamp}-{uuid}[-partN].clp */
  String generateArchivePath(int partNum, int totalParts) {
    LocalDate date = LocalDate.now(ZoneOffset.UTC);
    String dateStr = date.format(DATE_FORMATTER);

    long timestamp = System.currentTimeMillis();
    String uuid = UUID.randomUUID().toString().substring(0, 8);

    String filename = String.format("%d-%s", timestamp, uuid);
    if (totalParts > 1) {
      filename += String.format("-part%d", partNum);
    }
    filename += ".clp";

    return String.format("/clp-archives/audit/%s/%s", dateStr, filename);
  }

  @Override
  public void learnFromResult(ConsolidationTask task, long totalIrSizeBytes) {
    // Audit policy does NOT learn from results to maintain deterministic behavior
    if (immutable) {
      logger.debug("Audit policy is immutable, skipping compression ratio learning");
      return;
    }

    // For non-immutable audit policies (if ever needed)
    if (task.getActualArchiveSizeBytes() > 0 && totalIrSizeBytes > 0) {
      double actualRatio = (double) totalIrSizeBytes / task.getActualArchiveSizeBytes();
      logger.info(
          "Audit archive compression ratio observed: {}x (configured: {}x)",
          String.format("%.2f", actualRatio),
          String.format("%.2f", compressionRatio));
    }
  }

  @Override
  public String getPolicyName() {
    return "AuditPolicy:" + policyName;
  }

  // Getters for testing
  public long getSizeThresholdBytes() {
    return sizeThresholdBytes;
  }

  public int getTimeoutHours() {
    return timeoutHours;
  }

  public double getCompressionRatio() {
    return compressionRatio;
  }

  public boolean isImmutable() {
    return immutable;
  }

  public long getTargetArchiveSizeMb() {
    return targetArchiveSizeMb;
  }
}
