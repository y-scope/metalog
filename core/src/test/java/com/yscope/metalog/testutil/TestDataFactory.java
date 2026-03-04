package com.yscope.metalog.testutil;

import com.yscope.metalog.common.Timestamps;
import com.yscope.metalog.metastore.model.ConsolidationTask;
import com.yscope.metalog.metastore.model.FileRecord;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for creating common test data fixtures.
 *
 * <p>Provides pre-configured test scenarios and datasets for consistent testing.
 */
public class TestDataFactory {
  // Test timestamps (epoch nanoseconds)
  public static final long TEST_EPOCH = 1704067200_000_000_000L; // 2024-01-01 00:00:00 UTC (nanos)

  // Test application IDs
  public static final String SPARK_APP_001 = "spark-app-001";
  public static final String SPARK_APP_002 = "spark-app-002";
  public static final String SPARK_APP_003 = "spark-app-003";

  // Test executor IDs
  public static final String EXECUTOR_1 = "executor-1";
  public static final String EXECUTOR_2 = "executor-2";
  public static final String EXECUTOR_3 = "executor-3";

  // Test hosts
  public static final String HOST_1 = "spark-worker-01.example.com";

  /** Create a single buffering IR file with default values. */
  public static FileRecord createBufferingFile() {
    return new FileRecordBuilder()
        .withApplicationId(SPARK_APP_001)
        .withExecutorId(EXECUTOR_1)
        .withHost(HOST_1)
        .buffering()
        .build();
  }

  /** Create a single pending IR file with default values. */
  public static FileRecord createPendingFile() {
    return new FileRecordBuilder()
        .withApplicationId(SPARK_APP_001)
        .withExecutorId(EXECUTOR_1)
        .withHost(HOST_1)
        .pending()
        .build();
  }

  /** Create a single consolidated IR file with default values. */
  public static FileRecord createConsolidatedFile() {
    return new FileRecordBuilder()
        .withApplicationId(SPARK_APP_001)
        .withExecutorId(EXECUTOR_1)
        .withHost(HOST_1)
        .consolidated()
        .build();
  }

  /**
   * Create a Spark job with multiple pending files (typical consolidation scenario).
   *
   * @param appId Application ID
   * @param executorCount Number of executors
   * @param filesPerExecutor Number of files per executor
   * @return List of pending IR files
   */
  public static List<FileRecord> createSparkJobFiles(
      String appId, int executorCount, int filesPerExecutor) {
    List<FileRecord> files = new ArrayList<>();

    for (int e = 1; e <= executorCount; e++) {
      String executorId = "executor-" + e;
      String host = "spark-worker-" + String.format("%02d", e) + ".example.com";

      for (int f = 1; f <= filesPerExecutor; f++) {
        FileRecord file =
            new FileRecordBuilder()
                .withApplicationId(appId)
                .withExecutorId(executorId)
                .withHost(host)
                .withMinTimestamp(TEST_EPOCH + (e * 60L + f * 10L) * Timestamps.NANOS_PER_SECOND) // Stagger creation times
                .withIrSizeBytes(10_485_760L + (f * 1024L)) // Vary sizes slightly
                .withRecordCount(100_000L + (f * 1000L))
                .pending()
                .build();
        files.add(file);
      }
    }

    return files;
  }

  /** Create a small Spark job (3 executors, 1 file each) - typical quick job. */
  public static List<FileRecord> createSmallSparkJob() {
    return createSparkJobFiles(SPARK_APP_001, 3, 1);
  }

  /** Create a job with exactly 5 files (5 executors, 1 file each). */
  public static List<FileRecord> createFiveFileJob() {
    return createSparkJobFiles(SPARK_APP_001, 5, 1);
  }

  /** Create files with mixed states (for state transition testing). */
  public static List<FileRecord> createMixedStateFiles(String appId) {
    List<FileRecord> files = new ArrayList<>();

    // 2 buffering files
    files.add(
        new FileRecordBuilder()
            .withApplicationId(appId)
            .withExecutorId(EXECUTOR_1)
            .buffering()
            .build());
    files.add(
        new FileRecordBuilder()
            .withApplicationId(appId)
            .withExecutorId(EXECUTOR_2)
            .buffering()
            .build());

    // 3 pending files
    files.add(
        new FileRecordBuilder()
            .withApplicationId(appId)
            .withExecutorId(EXECUTOR_1)
            .pending()
            .build());
    files.add(
        new FileRecordBuilder()
            .withApplicationId(appId)
            .withExecutorId(EXECUTOR_2)
            .pending()
            .build());
    files.add(
        new FileRecordBuilder()
            .withApplicationId(appId)
            .withExecutorId(EXECUTOR_3)
            .pending()
            .build());

    // 1 consolidated file
    files.add(
        new FileRecordBuilder()
            .withApplicationId(appId)
            .withExecutorId(EXECUTOR_1)
            .consolidated()
            .build());

    return files;
  }

  /** Create files across multiple applications (for grouping tests). */
  public static List<FileRecord> createMultiAppFiles() {
    List<FileRecord> files = new ArrayList<>();

    // App 1: 3 pending files
    files.addAll(createSparkJobFiles(SPARK_APP_001, 3, 1));

    // App 2: 5 pending files
    files.addAll(createSparkJobFiles(SPARK_APP_002, 5, 1));

    // App 3: 2 pending files
    files.addAll(createSparkJobFiles(SPARK_APP_003, 2, 1));

    return files;
  }

  /** Create expired files (for retention/deletion testing). */
  public static List<FileRecord> createExpiredFiles(int count) {
    List<FileRecord> files = new ArrayList<>();
    long expiredTime = TEST_EPOCH - (31L * Timestamps.NANOS_PER_DAY); // 31 days ago (default retention is 30)

    for (int i = 0; i < count; i++) {
      FileRecord file =
          new FileRecordBuilder()
              .withApplicationId(SPARK_APP_001)
              .withExecutorId("executor-" + (i + 1))
              .withMinTimestamp(expiredTime)
              .withRetentionDays(30)
              .consolidated()
              .build();
      files.add(file);
    }

    return files;
  }

  /** Create a simple aggregation task for testing. */
  public static ConsolidationTask createSimpleTask() {
    return new ConsolidationTaskBuilder()
        .withAppId(SPARK_APP_001)
        .withFileCount(3)
        .submitted()
        .build();
  }

  /** Create a completed aggregation task. */
  public static ConsolidationTask createCompletedTask() {
    return new ConsolidationTaskBuilder()
        .withAppId(SPARK_APP_001)
        .withFileCount(5)
        .completed()
        .build();
  }

  /**
   * Create files with specific sizes for compression ratio testing.
   *
   * @param irSizeBytes IR file size
   * @param count Number of files
   * @return List of files with specified sizes
   */
  public static List<FileRecord> createFilesWithSize(long irSizeBytes, int count) {
    List<FileRecord> files = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      FileRecord file =
          new FileRecordBuilder()
              .withApplicationId(SPARK_APP_001)
              .withExecutorId("executor-" + (i + 1))
              .withIrSizeBytes(irSizeBytes)
              .pending()
              .build();
      files.add(file);
    }

    return files;
  }

  /** Private constructor to prevent instantiation. */
  private TestDataFactory() {
    throw new UnsupportedOperationException("Utility class");
  }
}
