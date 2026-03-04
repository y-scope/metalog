package com.yscope.clp.service.common.config;

/**
 * Standard timeout constants used throughout the Metalog service.
 *
 * <p>All timeouts are in milliseconds unless otherwise noted. These values are tuned for typical
 * production deployments and can be overridden via configuration where applicable.
 */
public final class Timeouts {
  private Timeouts() {}

  // ==================== Database Connection ====================

  /**
   * Maximum time to wait when acquiring a connection from the HikariCP pool. If no connection is
   * available within this time, a SQLException is thrown. 30 seconds balances responsiveness with
   * allowing time for pool recovery.
   */
  public static final long CONNECTION_TIMEOUT_MS = 30_000;

  /**
   * Maximum time a connection can sit idle in the pool before being retired. 10 minutes allows
   * connection reuse while preventing stale connections. Must be less than MySQL's wait_timeout
   * (default 8 hours).
   */
  public static final long IDLE_TIMEOUT_MS = 600_000;

  // ==================== Thread Lifecycle ====================

  /**
   * Short wait time for auxiliary threads (e.g., storage deletion, partition manager) that can be
   * safely interrupted. Used during graceful shutdown.
   */
  public static final long THREAD_JOIN_SHORT_MS = 2_000;

  /**
   * Longer wait time for critical threads (e.g., BatchingWriter, Planner) that may need to complete
   * in-flight operations before stopping.
   */
  public static final long THREAD_JOIN_LONG_MS = 5_000;

  /**
   * Maximum time to wait for ExecutorService shutdown (in seconds). After this, remaining tasks are
   * forcibly terminated.
   */
  public static final long SHUTDOWN_TIMEOUT_SECONDS = 60;

  // ==================== Polling Intervals ====================

  /**
   * Default interval for background loops (Planner, PartitionManager). 5 seconds provides
   * reasonable responsiveness without excessive polling.
   */
  public static final long LOOP_INTERVAL_DEFAULT_MS = 5_000;

  // ==================== Backoff Configuration ====================

  /**
   * Initial backoff delay when a worker encounters a transient error. 1 second prevents tight retry
   * loops while allowing quick recovery.
   */
  public static final long BACKOFF_MIN_MS = 1_000;

  /**
   * Maximum backoff delay after repeated failures. 30 seconds caps the wait time to maintain
   * throughput.
   */
  public static final long BACKOFF_MAX_MS = 30_000;

  /** Multiplier for exponential backoff between retries. */
  public static final double BACKOFF_MULTIPLIER = 1.5;

  // ==================== Task Queue Timeouts ====================

  /**
   * Maximum jitter added to the sleep before retrying a deadlocked task claim. A random value in
   * [1, CLAIM_DEADLOCK_JITTER_MAX_MS] is chosen to break synchronisation between concurrent
   * claimers. 50 ms is imperceptible at production task timescales (seconds) while providing a
   * wide spread window to desynchronise retrying threads.
   */
  public static final long CLAIM_DEADLOCK_JITTER_MAX_MS = 50;

  /**
   * Default task processing timeout in seconds (5 minutes). If a worker takes longer than this to
   * process a task, the task is considered stale and will be reclaimed by the Planner.
   */
  public static final int TASK_STALE_TIMEOUT_SECONDS = 300;

  /**
   * Default cleanup grace period in seconds (1 hour). Completed/failed/timed_out tasks older than
   * this are deleted from the _task_queue table during cleanup cycles.
   */
  public static final int TASK_CLEANUP_GRACE_SECONDS = 3600;

  // ==================== Schema Evolution ====================

}
