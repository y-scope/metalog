package com.yscope.metalog.worker;

import com.yscope.metalog.common.Timestamps;
import com.yscope.metalog.common.config.Timeouts;
import com.yscope.metalog.common.storage.ArchiveCreator;
import com.yscope.metalog.common.storage.StorageBackend;
import com.yscope.metalog.common.storage.StorageException;
import com.yscope.metalog.metastore.TaskQueue;
import com.yscope.metalog.metastore.model.TaskPayload;
import com.yscope.metalog.metastore.model.TaskResult;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared worker logic for the poll/execute/backoff loop.
 *
 * <p>Used by both {@link Worker} (standalone process, table-scoped claiming) and {@link
 * com.yscope.metalog.node.WorkerUnit WorkerUnit} (node-hosted, table-agnostic claiming).
 *
 * <p>Callers supply a {@link TaskClaimer} lambda to control how tasks are claimed.
 */
public class WorkerCore {
  private static final Logger logger = LoggerFactory.getLogger(WorkerCore.class);

  /** Strategy for claiming tasks from the queue. */
  @FunctionalInterface
  public interface TaskClaimer {
    Optional<TaskQueue.Task> claim(String workerId) throws SQLException, InterruptedException;
  }

  private final TaskQueue taskQueue;
  private final ArchiveCreator archiveCreator;
  private final StorageBackend archiveBackend;
  private final TaskClaimer claimer;
  private final String logPrefix;

  /**
   * @param taskQueue queue for completing/failing tasks
   * @param archiveCreator orchestrator for creating archives from IR files
   * @param archiveBackend archive storage backend for orphan cleanup
   * @param claimer strategy for claiming tasks (table-scoped or table-agnostic)
   * @param logPrefix prefix for log messages ({@code ""} for Worker, {@code "[unitName] "} for
   *     WorkerUnit)
   */
  public WorkerCore(
      TaskQueue taskQueue,
      ArchiveCreator archiveCreator,
      StorageBackend archiveBackend,
      TaskClaimer claimer,
      String logPrefix) {
    this.taskQueue = taskQueue;
    this.archiveCreator = archiveCreator;
    this.archiveBackend = archiveBackend;
    this.claimer = claimer;
    this.logPrefix = logPrefix;
  }

  /**
   * Run the poll/backoff/execute loop until {@code running} becomes false or the thread is
   * interrupted.
   */
  public void runLoop(String workerId, AtomicBoolean running) {
    logger.info("{}[{}] Worker thread started", logPrefix, workerId);

    long currentBackoff = Timeouts.BACKOFF_MIN_MS;

    while (running.get()) {
      try {
        Optional<TaskQueue.Task> claimedTask = claimer.claim(workerId);

        if (claimedTask.isEmpty()) {
          logger.debug(
              "{}[{}] No tasks available, sleeping {}ms", logPrefix, workerId, currentBackoff);
          Thread.sleep(currentBackoff);
          currentBackoff =
              Math.min(
                  (long) (currentBackoff * Timeouts.BACKOFF_MULTIPLIER), Timeouts.BACKOFF_MAX_MS);
          continue;
        }

        // Reset backoff on successful claim
        currentBackoff = Timeouts.BACKOFF_MIN_MS;

        TaskQueue.Task task = claimedTask.get();
        logger.info("{}[{}] Claimed task: {}", logPrefix, workerId, task.getTaskId());
        executeTask(workerId, task);

      } catch (InterruptedException e) {
        logger.info("{}[{}] Interrupted, shutting down", logPrefix, workerId);
        Thread.currentThread().interrupt();
        break;
      } catch (SQLException | RuntimeException e) {
        logger.error("{}[{}] Error in worker loop", logPrefix, workerId, e);
        try {
          Thread.sleep(Timeouts.BACKOFF_MIN_MS);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  /** Execute a single task: create archive, then complete or fail. */
  void executeTask(String workerId, TaskQueue.Task task) {
    long taskId = task.getTaskId();
    TaskPayload payload = task.getParsedInput();
    String archivePath = payload.getArchivePath();

    try {
      logger.info(
          "{}[{}] Executing task {}: {} IR files → {}/{}",
          logPrefix,
          workerId,
          taskId,
          payload.getIrFilePaths().size(),
          payload.getArchiveBucket(),
          archivePath);

      long archiveSizeBytes;
      try {
        archiveSizeBytes =
            archiveCreator.createArchive(archivePath, payload.getIrFilePaths());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Archive creation interrupted", e);
      } catch (StorageException e) {
        throw new RuntimeException("Failed to create archive", e);
      }

      TaskResult result = new TaskResult(archiveSizeBytes, Timestamps.nowNanos());
      int updated = taskQueue.completeTask(taskId, result.serialize());

      if (updated > 0) {
        logger.info(
            "{}[{}] Task {} completed successfully (size={})",
            logPrefix,
            workerId,
            taskId,
            archiveSizeBytes);
      } else {
        // Task was likely reclaimed by coordinator and may be assigned to another worker.
        // Do NOT delete the archive — the coordinator's reclaim/retry logic handles cleanup.
        logger.warn(
            "{}[{}] Task {} not found in processing state (possibly reclaimed), "
                + "skipping archive deletion for {}",
            logPrefix,
            workerId,
            taskId,
            archivePath);
      }

    } catch (SQLException | RuntimeException e) {
      logger.error("{}[{}] Task {} failed", logPrefix, workerId, taskId, e);

      try {
        int updated = taskQueue.failTask(taskId);
        if (updated == 0) {
          logger.warn(
              "{}[{}] Task {} already not in processing state", logPrefix, workerId, taskId);
        }
      } catch (SQLException reportError) {
        logger.error("{}[{}] Failed to report task failure", logPrefix, workerId, reportError);
      }

      if (archivePath != null) {
        deleteOrphanArchive(workerId, archivePath);
      }
    }
  }

  /** Delete an orphan archive created by a reclaimed task. */
  void deleteOrphanArchive(String workerId, String archivePath) {
    try {
      archiveBackend.deleteFile(archivePath);
      logger.info("{}[{}] Deleted orphan archive: {}", logPrefix, workerId, archivePath);
    } catch (StorageException e) {
      logger.warn("{}[{}] Failed to delete orphan archive {}", logPrefix, workerId, archivePath, e);
    }
  }
}
