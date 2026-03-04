package com.yscope.metalog.common.config;

/**
 * Default configuration values used throughout the Metalog service.
 *
 * <p>These constants provide sensible defaults for queue sizes, batch sizes, and pool
 * configurations. They can be overridden via configuration files or environment variables where
 * applicable.
 */
public final class Defaults {
  private Defaults() {}

  // ==================== Queue Sizes ====================

  /**
   * Default capacity for the event queue that buffers FileRecord metadata from Kafka before
   * database insertion.
   */
  public static final int EVENT_QUEUE_CAPACITY = 10_000;

  /**
   * Default capacity for the deletion queue that buffers file paths for rate-limited storage
   * deletion.
   */
  public static final int DELETION_QUEUE_CAPACITY = 10_000;

  // ==================== Batch Sizes ====================

  /**
   * Default batch size for deletion operations. Limits the number of files deleted per cycle to
   * avoid long-running transactions.
   */
  public static final int DELETION_BATCH_SIZE = 1_000;

  /**
   * Default limit for database queries returning pending files. Prevents memory issues with very
   * large result sets.
   */
  public static final int QUERY_LIMIT = 10_000;

  // ==================== Pool Sizes ====================

  /** Default minimum idle connections in the database pool. */
  public static final int DB_POOL_MIN_IDLE = 2;

  /** Default maximum connections in the database pool. */
  public static final int DB_POOL_MAX_SIZE = 10;

  // ==================== Retry Configuration ====================

  /** Default maximum retries before moving a task to the dead letter queue. */
  public static final int TASK_MAX_RETRIES = 3;

  // ==================== Schema Evolution ====================

  /** Default maximum dimension columns allowed per table. Prevents unbounded schema growth. */
  public static final int SCHEMA_MAX_DIMENSION_COLUMNS = 50;

  /** Default maximum count columns allowed per table. */
  public static final int SCHEMA_MAX_COUNT_COLUMNS = 20;
}
