package com.yscope.metalog.coordinator.workflow.consolidation;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe tracking of IR files currently scheduled for consolidation.
 *
 * <p>The InFlightSet prevents duplicate task creation by tracking files from task creation until
 * the database commit is complete.
 *
 * <p>Lifecycle:
 *
 * <ol>
 *   <li>Planner adds files when creating a task (before submitting to TaskQueue)
 *   <li>Writer removes files after successful database commit
 * </ol>
 *
 * <p>This prevents the scenario where:
 *
 * <ol>
 *   <li>Planner queries DB, finds file A in PENDING state
 *   <li>Planner creates task T1 containing file A
 *   <li>Before T1 completes, Planner queries DB again
 *   <li>File A is still PENDING (DB not updated yet)
 *   <li>Without InFlightSet, Planner would create duplicate task T2
 * </ol>
 *
 * <p>This set is <b>in-memory only</b> and is lost on process crash. This is intentional: the
 * Planner's stale-task reclaim mechanism recovers from crashes by scanning the task queue for
 * timed-out tasks, so persistence is unnecessary. On startup, the set is {@link #clear() cleared}
 * to avoid stale entries from a previous run.
 *
 * <p>Thread safety: Uses ConcurrentHashMap for lock-free reads and writes.
 */
public class InFlightSet {
  private static final Logger logger = LoggerFactory.getLogger(InFlightSet.class);

  // Map: IR file path -> task ID
  private final ConcurrentHashMap<String, String> inFlight;

  public InFlightSet() {
    this.inFlight = new ConcurrentHashMap<>();
  }

  /**
   * Add a file to the in-flight set.
   *
   * @param irFilePath The IR file path
   * @param taskId The task ID that includes this file
   * @return true if added, false if already present
   */
  public boolean add(String irFilePath, String taskId) {
    String existing = inFlight.putIfAbsent(irFilePath, taskId);
    if (existing != null) {
      logger.debug("File {} already in flight (task {})", irFilePath, existing);
      return false;
    }
    return true;
  }

  /**
   * Add multiple files to the in-flight set.
   *
   * @param irFilePaths Collection of IR file paths
   * @param taskId The task ID that includes these files
   * @return Number of files actually added (excludes already present)
   */
  public int addAll(Collection<String> irFilePaths, String taskId) {
    int added = 0;
    for (String path : irFilePaths) {
      if (add(path, taskId)) {
        added++;
      }
    }
    return added;
  }

  /**
   * Remove a file from the in-flight set.
   *
   * @param irFilePath The IR file path
   * @return true if removed, false if not present
   */
  public boolean remove(String irFilePath) {
    return inFlight.remove(irFilePath) != null;
  }

  /**
   * Remove multiple files from the in-flight set.
   *
   * @param irFilePaths Collection of IR file paths
   * @return Number of files actually removed
   */
  public int removeAll(Collection<String> irFilePaths) {
    int removed = 0;
    for (String path : irFilePaths) {
      if (remove(path)) {
        removed++;
      }
    }
    return removed;
  }

  /**
   * Remove files only if they belong to the specified task. Avoids removing entries owned by other
   * tasks during rollback of a partially-added batch.
   *
   * @param irFilePaths Collection of IR file paths
   * @param taskId Only remove entries matching this task ID
   * @return Number of files actually removed
   */
  public int removeAllForTask(Collection<String> irFilePaths, String taskId) {
    int removed = 0;
    for (String path : irFilePaths) {
      if (inFlight.remove(path, taskId)) {
        removed++;
      }
    }
    return removed;
  }

  /**
   * Check if a file is in the in-flight set.
   *
   * @param irFilePath The IR file path
   * @return true if the file is in flight
   */
  public boolean contains(String irFilePath) {
    return inFlight.containsKey(irFilePath);
  }

  /**
   * Get the task ID for an in-flight file.
   *
   * @param irFilePath The IR file path
   * @return The task ID, or null if not in flight
   */
  public String getTaskId(String irFilePath) {
    return inFlight.get(irFilePath);
  }

  /**
   * Get the current size of the in-flight set.
   *
   * @return Number of files currently in flight
   */
  public int size() {
    return inFlight.size();
  }

  /**
   * Check if the in-flight set is empty.
   *
   * @return true if no files are in flight
   */
  public boolean isEmpty() {
    return inFlight.isEmpty();
  }

  /**
   * Clear all entries from the in-flight set.
   *
   * <p>Used during crash recovery to reset ephemeral state.
   */
  public void clear() {
    int size = inFlight.size();
    inFlight.clear();
    if (size > 0) {
      logger.info("Cleared {} entries from InFlightSet", size);
    }
  }

  /**
   * Get a snapshot of all in-flight file paths.
   *
   * <p>Returns a copy to avoid ConcurrentModificationException.
   *
   * @return Set of IR file paths currently in flight
   */
  public Set<String> getInFlightPaths() {
    return Set.copyOf(inFlight.keySet());
  }

  /**
   * Get metrics for monitoring.
   *
   * @return Metrics object with current stats
   */
  public Metrics getMetrics() {
    return new Metrics(inFlight.size());
  }

  /** Metrics for monitoring the InFlightSet. */
  public static class Metrics {
    private final int size;

    public Metrics(int size) {
      this.size = size;
    }

    public int getSize() {
      return size;
    }

    @Override
    public String toString() {
      return "InFlightSet.Metrics{size=" + size + "}";
    }
  }
}
