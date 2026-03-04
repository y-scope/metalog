package com.yscope.clp.service.coordinator.workflow.consolidation;

import com.yscope.clp.service.common.config.Defaults;
import com.yscope.clp.service.common.config.ServiceConfig;
import com.yscope.clp.service.common.config.Timeouts;
import com.yscope.clp.service.common.storage.StorageRegistry;
import com.yscope.clp.service.coordinator.ProgressTracker;
import com.yscope.clp.service.metastore.FileRecords;
import com.yscope.clp.service.metastore.TaskQueue;
import com.yscope.clp.service.metastore.model.ConsolidationTask;
import com.yscope.clp.service.metastore.model.FileRecord;
import com.yscope.clp.service.metastore.model.TaskPayload;
import com.yscope.clp.service.metastore.model.TaskResult;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Planner thread for the database-backed task queue architecture.
 *
 * <p>Responsibilities:
 *
 * <ul>
 *   <li>Timer-based planning cycles
 *   <li>Query pending files from MySQL
 *   <li>Filter out files already in InFlightSet
 *   <li>Apply aggregation policies to create tasks
 *   <li>Add files to InFlightSet before task creation
 *   <li>Insert tasks into the _task_queue table
 *   <li>Process completed tasks (update metadata to ARCHIVE_CLOSED, queue IR deletion)
 *   <li>Reclaim stale tasks that have timed out
 *   <li>Clean up old completed/failed/timed_out tasks
 * </ul>
 *
 * <p>Design principles:
 *
 * <ul>
 *   <li>Writes to metadata table only for completed task processing (ARCHIVE_CLOSED transition)
 *   <li>Write to _task_queue table for task management
 *   <li>InFlightSet prevents duplicate task creation
 *   <li>Self-healing via stale task reclaim
 * </ul>
 */
public class Planner implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(Planner.class);

  // Configuration
  private final ServiceConfig config;
  private final String tableName;

  // Components
  private final FileRecords repository;
  private final TaskQueue taskQueue;
  private final InFlightSet inFlightSet;
  private final ArchivePathGenerator archivePathGenerator;
  private final StorageRegistry storageRegistry;

  // Deletion queue for IR files after consolidation (null when not wired)
  private final BlockingQueue<String> deletionQueue;

  // Progress tracking (null when used outside CoordinatorUnit)
  private final ProgressTracker progressTracker;

  // Policies
  private volatile List<Policy> policies;

  // Task queue configuration
  private final int staleTimeoutSeconds;
  private final int cleanupGraceSeconds;
  private final int maxRetries;

  // Running state
  private volatile boolean running = true;
  private volatile Thread thread;

  /**
   * Create a Planner.
   *
   * @param config Application configuration
   * @param tableName Target metadata table name
   * @param repository Metadata repository for DB queries
   * @param taskQueue Database task queue for task management
   * @param inFlightSet In-flight set for deduplication
   * @param archivePathGenerator Generator for archive paths
   * @param storageRegistry Storage registry for backend/namespace info
   * @param policies Aggregation policies
   */
  public Planner(
      ServiceConfig config,
      String tableName,
      FileRecords repository,
      TaskQueue taskQueue,
      InFlightSet inFlightSet,
      ArchivePathGenerator archivePathGenerator,
      StorageRegistry storageRegistry,
      List<Policy> policies) {
    this(
        config,
        tableName,
        repository,
        taskQueue,
        inFlightSet,
        archivePathGenerator,
        storageRegistry,
        policies,
        null,
        null);
  }

  /**
   * Create a Planner with progress tracking and deletion queue.
   *
   * @param config Application configuration
   * @param tableName Target metadata table name
   * @param repository Metadata repository for DB queries
   * @param taskQueue Database task queue for task management
   * @param inFlightSet In-flight set for deduplication
   * @param archivePathGenerator Generator for archive paths
   * @param storageRegistry Storage registry for backend/namespace info
   * @param policies Aggregation policies
   * @param deletionQueue Queue for IR file paths to delete from storage (may be null)
   * @param progressTracker Progress tracker for health monitoring (may be null)
   */
  public Planner(
      ServiceConfig config,
      String tableName,
      FileRecords repository,
      TaskQueue taskQueue,
      InFlightSet inFlightSet,
      ArchivePathGenerator archivePathGenerator,
      StorageRegistry storageRegistry,
      List<Policy> policies,
      BlockingQueue<String> deletionQueue,
      ProgressTracker progressTracker) {
    this.config = config;
    this.tableName = tableName;
    this.repository = repository;
    this.taskQueue = taskQueue;
    this.inFlightSet = inFlightSet;
    this.archivePathGenerator = archivePathGenerator;
    this.storageRegistry = storageRegistry;
    this.policies = List.copyOf(policies);
    this.deletionQueue = deletionQueue;
    this.progressTracker = progressTracker;

    // Configure task queue parameters from config or defaults
    this.staleTimeoutSeconds =
        config.getTaskTimeoutMs() > 0
            ? (int) (config.getTaskTimeoutMs() / 1000)
            : Timeouts.TASK_STALE_TIMEOUT_SECONDS;
    this.cleanupGraceSeconds = Timeouts.TASK_CLEANUP_GRACE_SECONDS;
    this.maxRetries = Defaults.TASK_MAX_RETRIES;
  }

  @Override
  public void run() {
    thread = Thread.currentThread();
    logger.info(
        "Planner started (table={}, interval={}ms, staleTimeout={}s)",
        tableName,
        config.getPlannerIntervalMs(),
        staleTimeoutSeconds);

    while (running) {
      try {
        // Sleep first to avoid immediate execution on startup
        Thread.sleep(config.getPlannerIntervalMs());

        if (!running) break;

        // Run planning cycle
        runPlanningCycle();

        if (progressTracker != null) {
          progressTracker.recordProgress("planner");
        }
      } catch (InterruptedException e) {
        logger.info("Planner interrupted, shutting down");
        Thread.currentThread().interrupt();
        break;
      } catch (RuntimeException e) {
        logger.error("Error in Planner", e);
      }
    }

    logger.info("Planner stopped");
  }

  /**
   * Execute a single planning cycle.
   *
   * <p>Steps:
   *
   * <ol>
   *   <li>Reclaim stale tasks
   *   <li>Process completed tasks (update metadata, queue IR deletion)
   *   <li>Clean up old tasks
   *   <li>Check backpressure
   *   <li>Query pending files from DB
   *   <li>Filter by InFlightSet
   *   <li>Apply policies to create tasks
   *   <li>Submit tasks to database
   * </ol>
   */
  void runPlanningCycle() {
    // Step 1: Reclaim stale tasks
    reclaimStaleTasks();

    // Step 2: Process completed tasks (must run before cleanup to consume results)
    processCompletedTasks();

    // Step 3: Clean up old tasks
    cleanupOldTasks();

    // Step 4: Check backpressure
    if (shouldApplyBackpressure()) {
      logger.debug("Backpressure active, skipping task creation");
      return;
    }

    // Step 5: Query pending files from DB
    List<FileRecord> pendingFiles;
    try {
      pendingFiles = repository.findConsolidationPendingFiles();
    } catch (SQLException e) {
      logger.error("Failed to query pending files", e);
      return;
    }

    if (pendingFiles.isEmpty()) {
      logger.debug("No pending files found");
      return;
    }

    logger.debug("Found {} pending files", pendingFiles.size());

    // Step 6: Filter by InFlightSet
    List<FileRecord> eligibleFiles =
        pendingFiles.stream()
            .filter(f -> !inFlightSet.contains(f.getIrPath()))
            .collect(Collectors.toList());

    if (eligibleFiles.isEmpty()) {
      logger.debug("All {} pending files are already in flight", pendingFiles.size());
      return;
    }

    int filtered = pendingFiles.size() - eligibleFiles.size();
    if (filtered > 0) {
      logger.debug(
          "Filtered {} files already in flight, {} eligible", filtered, eligibleFiles.size());
    }

    // Step 7: Apply policies to create tasks (snapshot volatile field for safe iteration)
    List<Policy> currentPolicies = this.policies;
    List<ConsolidationTask> allTasks = new ArrayList<>();
    for (Policy policy : currentPolicies) {
      try {
        List<ConsolidationTask> tasks = policy.shouldAggregate(eligibleFiles);
        allTasks.addAll(tasks);
      } catch (RuntimeException e) {
        logger.error("Error applying policy {}", policy.getClass().getSimpleName(), e);
      }
    }

    if (allTasks.isEmpty()) {
      logger.debug("No tasks created from {} eligible files", eligibleFiles.size());
      return;
    }

    // Step 8: Submit tasks to database
    int submitted = submitTasks(allTasks);
    logger.info(
        "Submitted {}/{} tasks ({} eligible files, {} policies)",
        submitted,
        allTasks.size(),
        eligibleFiles.size(),
        currentPolicies.size());
  }

  /** Reclaim stale tasks that have been processing for too long. */
  private void reclaimStaleTasks() {
    try {
      List<TaskQueue.Task> staleTasks = taskQueue.findStaleTasks(tableName, staleTimeoutSeconds);

      for (TaskQueue.Task staleTask : staleTasks) {
        try {
          // Reclaim the task in DB first (creates new pending task or moves to dead letter)
          long newTaskId = taskQueue.reclaimTask(staleTask, maxRetries);

          // Only remove from InFlightSet after successful DB reclaim
          List<String> irPaths = staleTask.getIrFilePaths();
          if (!irPaths.isEmpty()) {
            inFlightSet.removeAll(irPaths);
            logger.debug(
                "Removed {} files from InFlightSet for stale task {}",
                irPaths.size(),
                staleTask.getTaskId());
          }

          if (newTaskId > 0) {
            logger.info(
                "Reclaimed stale task {} as {} (retry {})",
                staleTask.getTaskId(),
                newTaskId,
                staleTask.getRetryCount() + 1);
          }
        } catch (SQLException e) {
          logger.error("Failed to reclaim stale task {}", staleTask.getTaskId(), e);
        }
      }
    } catch (SQLException e) {
      logger.error("Error finding stale tasks", e);
    }
  }

  /**
   * Process completed tasks: update metadata to ARCHIVE_CLOSED, remove from InFlightSet, and queue
   * IR files for storage deletion.
   *
   * <p>Each task is processed individually so that failures on one task don't block others. Failed
   * tasks remain in the queue and will be retried on the next planning cycle.
   *
   * <p><b>Crash safety:</b> {@code markArchiveClosed()} and {@code deleteTask()} are deliberately
   * separate transactions (metadata update first, task deletion second). See the inline comment in
   * the loop body for the ordering rationale.
   *
   * <p><b>Idempotency:</b> {@link FileRecords#markArchiveClosed} uses {@code WHERE
   * clp_archive_path = ''}, so re-processing a task after crash is safe (already-updated rows
   * won't match).
   */
  private void processCompletedTasks() {
    List<TaskQueue.Task> completedTasks;
    try {
      completedTasks = taskQueue.findCompletedTasks(tableName);
    } catch (SQLException e) {
      logger.error("Failed to find completed tasks", e);
      return;
    }

    if (completedTasks.isEmpty()) {
      return;
    }

    int processed = 0;
    int totalFiles = 0;

    for (TaskQueue.Task task : completedTasks) {
      try {
        TaskPayload input = task.getParsedInput();
        TaskResult output = task.getParsedOutput();

        if (input == null) {
          logger.warn("Task {} has no input, skipping", task.getTaskId());
          taskQueue.deleteTask(task.getTaskId());
          continue;
        }

        List<String> irPaths = input.getIrFilePaths();
        String archivePath = input.getArchivePath();

        // Default values when output is missing (backward compatibility)
        long archiveSizeBytes = output != null ? output.getArchiveSizeBytes() : 0;
        long archiveCreatedAt = output != null ? output.getArchiveCreatedAt() : 0;

        // IMPORTANT: These two operations are deliberately NOT in a single transaction.
        // The order matters for crash safety:
        //   1. markArchiveClosed() — updates file metadata (the critical operation)
        //   2. deleteTask() — removes the consumed task row
        // If we crash between (1) and (2), the orphaned task row is harmless:
        //   - Cleaned up on coordinator restart (deletes all non-dead-letter tasks)
        //   - Or by TaskQueue.cleanup() after the grace period (24h default)
        // The reverse order would be unsafe: deleting the task first, then crashing,
        // leaves files in CONSOLIDATION_PENDING with an already-created archive.
        repository.markArchiveClosed(irPaths, archivePath, archiveSizeBytes, archiveCreatedAt);

        // Remove IR paths from InFlightSet
        inFlightSet.removeAll(irPaths);

        // Queue IR paths for storage deletion
        if (deletionQueue != null) {
          for (String irPath : irPaths) {
            deletionQueue.offer(irPath);
          }
        }

        taskQueue.deleteTask(task.getTaskId());

        processed++;
        totalFiles += irPaths.size();

      } catch (SQLException | IllegalStateException e) {
        logger.error("Failed to process completed task {}", task.getTaskId(), e);
        // Task remains in queue for retry on next cycle
      }
    }

    if (processed > 0) {
      logger.info(
          "Processed {} completed tasks ({} files transitioned to ARCHIVE_CLOSED)",
          processed,
          totalFiles);
    }
  }

  /** Clean up old completed/failed/timed_out tasks. */
  private void cleanupOldTasks() {
    try {
      int deleted = taskQueue.cleanup(tableName, cleanupGraceSeconds);
      if (deleted > 0) {
        logger.debug("Cleaned up {} old tasks", deleted);
      }
    } catch (SQLException e) {
      logger.error("Error cleaning up old tasks", e);
    }
  }

  /**
   * Check if backpressure should be applied.
   *
   * @return true if task creation should be paused
   */
  private boolean shouldApplyBackpressure() {
    try {
      TaskQueue.TaskCounts counts = taskQueue.getTaskCounts(tableName);
      int incomplete = counts.getIncomplete();
      int highWatermark = config.getBackpressureHighWatermark();

      if (incomplete >= highWatermark) {
        logger.debug(
            "Backpressure: {} incomplete tasks >= {} watermark", incomplete, highWatermark);
        return true;
      }
    } catch (SQLException e) {
      logger.warn("Failed to get task counts for backpressure check", e);
    }
    return false;
  }

  /**
   * Submit tasks to the database task queue.
   *
   * <p>For each task:
   *
   * <ol>
   *   <li>Generate archive path
   *   <li>Add files to InFlightSet BEFORE submission
   *   <li>Create TaskPayload and insert to database
   * </ol>
   *
   * @param tasks Tasks to submit
   * @return Number of tasks successfully submitted
   */
  private int submitTasks(List<ConsolidationTask> tasks) {
    int submitted = 0;

    for (ConsolidationTask task : tasks) {
      try {
        // Generate archive path
        String archivePath = archivePathGenerator.generate(task.getTaskId());
        task.setArchivePath(archivePath);

        // Add files to InFlightSet BEFORE submission
        List<String> irPaths = task.getIrFilePaths();
        int added = inFlightSet.addAll(irPaths, task.getTaskId());

        if (added < irPaths.size()) {
          // Some files were already in flight (race condition)
          logger.warn(
              "Race condition: {} files already in flight, skipping task {}",
              irPaths.size() - added,
              task.getTaskId());
          inFlightSet.removeAllForTask(irPaths, task.getTaskId());
          continue;
        }

        // Create payload and insert to database
        TaskPayload payload =
            new TaskPayload(
                archivePath,
                irPaths,
                tableName,
                storageRegistry.getIrBackend().name(),
                storageRegistry.getIrBackend().namespace(),
                storageRegistry.getArchiveBackend().name(),
                storageRegistry.getArchiveBackend().namespace());

        long taskId = taskQueue.createTask(tableName, payload.serialize());
        submitted++;

        logger.debug(
            "Created task {} with {} files, archive={}", taskId, irPaths.size(), archivePath);

      } catch (SQLException e) {
        logger.error("Error submitting task {}", task.getTaskId(), e);
        // Clean up InFlightSet on error (task-scoped to avoid removing other tasks' entries)
        inFlightSet.removeAllForTask(task.getIrFilePaths(), task.getTaskId());
      }
    }

    return submitted;
  }

  /**
   * Update the policies (for hot-reload support).
   *
   * @param newPolicies New list of policies
   */
  public void setPolicies(List<Policy> newPolicies) {
    this.policies = List.copyOf(newPolicies);
    logger.debug("Updated policies: {} policies", newPolicies.size());
  }

  /** Stop the planner thread gracefully. */
  public void stop() {
    running = false;
    Thread t = thread;
    if (t != null) {
      t.interrupt();
    }
  }

  /** Check if the planner thread is running. */
  public boolean isRunning() {
    return running;
  }

  /** Get metrics for monitoring. */
  public Metrics getMetrics() {
    try {
      TaskQueue.TaskCounts counts = taskQueue.getTaskCounts(tableName);
      return new Metrics(inFlightSet.size(), counts.pending, counts.processing, counts.completed);
    } catch (SQLException e) {
      logger.warn("Failed to get task counts for metrics", e);
      return new Metrics(inFlightSet.size(), 0, 0, 0);
    }
  }

  /** Metrics for monitoring the planner. */
  public static class Metrics {
    private final int inFlightSize;
    private final int pendingCount;
    private final int processingCount;
    private final int completedCount;

    public Metrics(int inFlightSize, int pendingCount, int processingCount, int completedCount) {
      this.inFlightSize = inFlightSize;
      this.pendingCount = pendingCount;
      this.processingCount = processingCount;
      this.completedCount = completedCount;
    }

    public int getInFlightSize() {
      return inFlightSize;
    }

    public int getPendingCount() {
      return pendingCount;
    }

    public int getProcessingCount() {
      return processingCount;
    }

    public int getCompletedCount() {
      return completedCount;
    }

    @Override
    public String toString() {
      return String.format(
          "PlannerMetrics{inFlight=%d, pending=%d, processing=%d, completed=%d}",
          inFlightSize, pendingCount, processingCount, completedCount);
    }
  }
}
