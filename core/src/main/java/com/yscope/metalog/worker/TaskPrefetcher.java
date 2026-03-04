package com.yscope.metalog.worker;

import com.yscope.metalog.metastore.TaskQueue;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-threaded prefetch loop that batch-claims tasks from the DB into an in-memory queue.
 *
 * <p>Worker threads call {@link #poll} instead of hitting the database directly, reducing DB
 * round-trips from N (one per worker) to 1 (one prefetch batch).
 *
 * <p>Backoff: when no tasks are available the prefetch thread sleeps with exponential backoff
 * (1 s → 2 s → … → 32 s). Backoff resets on the first successful claim.
 *
 * <p>Shutdown: {@link #shutdown()} interrupts the prefetch thread, drains the queue, and resets
 * any unclaimed tasks back to {@code pending} so they are not lost.
 */
public class TaskPrefetcher {
  private static final Logger logger = LoggerFactory.getLogger(TaskPrefetcher.class);

  static final long BACKOFF_MIN_MS = 1_000;
  static final long BACKOFF_MAX_MS = 32_000;
  static final long BACKOFF_FACTOR = 2;
  static final long QUEUE_POLL_TIMEOUT_MS = 1_000;

  private final TaskQueue taskQueue;
  private final String prefetcherId;
  private final int queueCapacity;

  private final LinkedBlockingQueue<TaskQueue.Task> queue;
  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private Thread prefetchThread;

  /**
   * @param taskQueue source of tasks and reset target on shutdown
   * @param prefetcherId ID used when claiming tasks in the DB (e.g. {@code "prefetch-abc123"})
   * @param queueCapacity maximum tasks to hold in the in-memory queue
   */
  public TaskPrefetcher(TaskQueue taskQueue, String prefetcherId, int queueCapacity) {
    this.taskQueue = taskQueue;
    this.prefetcherId = prefetcherId;
    this.queueCapacity = queueCapacity;
    this.queue = new LinkedBlockingQueue<>(queueCapacity);
  }

  /** Start the background prefetch thread. */
  public void start() {
    shutdown.set(false);
    prefetchThread = new Thread(this::prefetchLoop, "task-prefetcher-" + prefetcherId);
    prefetchThread.setDaemon(false);
    prefetchThread.start();
    logger.info("TaskPrefetcher started (id={}, capacity={})", prefetcherId, queueCapacity);
  }

  /**
   * Block until a task is available and return it.
   *
   * @return the next task
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public TaskQueue.Task take() throws InterruptedException {
    return queue.take();
  }

  /**
   * Poll a task from the in-memory queue with a timeout. Prefer {@link #take()} for worker
   * threads; use this for tests that need a bounded wait.
   *
   * @param timeoutMs max time to wait in milliseconds
   * @return the next task, or empty if none arrived within the timeout
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public Optional<TaskQueue.Task> poll(long timeoutMs) throws InterruptedException {
    return Optional.ofNullable(queue.poll(timeoutMs, TimeUnit.MILLISECONDS));
  }

  /**
   * Stop the prefetch thread and reset any queued (unclaimed-by-workers) tasks to {@code pending}.
   */
  public void shutdown() {
    shutdown.set(true);
    if (prefetchThread != null) {
      prefetchThread.interrupt();
      try {
        prefetchThread.join(5_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    List<TaskQueue.Task> drained = new ArrayList<>();
    queue.drainTo(drained);

    if (!drained.isEmpty()) {
      List<Long> ids = drained.stream().map(TaskQueue.Task::getTaskId).toList();
      logger.info("Resetting {} queued tasks to pending on shutdown", ids.size());
      try {
        taskQueue.resetTasksToPending(ids);
      } catch (SQLException e) {
        logger.warn("Failed to reset queued tasks to pending on shutdown", e);
      }
    }

    logger.info("TaskPrefetcher shut down (id={})", prefetcherId);
  }

  private void prefetchLoop() {
    long currentBackoffMs = BACKOFF_MIN_MS;

    while (!shutdown.get()) {
      try {
        if (queue.size() >= queueCapacity / 2) {
          // Queue still has enough tasks; check again soon.
          Thread.sleep(QUEUE_POLL_TIMEOUT_MS);
          continue;
        }

        List<TaskQueue.Task> tasks = taskQueue.claimTasks(prefetcherId, queueCapacity);

        if (tasks.isEmpty()) {
          logger.debug("No tasks available, backing off {}ms", currentBackoffMs);
          Thread.sleep(currentBackoffMs);
          currentBackoffMs = Math.min(currentBackoffMs * BACKOFF_FACTOR, BACKOFF_MAX_MS);
        } else {
          currentBackoffMs = BACKOFF_MIN_MS;
          for (TaskQueue.Task task : tasks) {
            queue.put(task); // bounded put; capacity guarantees this won't grow unbounded
          }
          logger.debug("Prefetched {} tasks", tasks.size());
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        logger.warn("Error in prefetch loop, backing off {}ms", BACKOFF_MIN_MS, e);
        try {
          Thread.sleep(BACKOFF_MIN_MS);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    logger.info("Prefetch loop exited (id={})", prefetcherId);
  }
}
