package com.yscope.metalog.testutil;

import com.yscope.metalog.metastore.model.ConsolidationTask;
import java.util.ArrayList;
import java.util.List;

/**
 * Fluent builder for creating ConsolidationTask test fixtures.
 *
 * <p>Provides methods for creating tasks in various states with realistic timing.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ConsolidationTask task = new ConsolidationTaskBuilder()
 *     .withAppId("spark-app-001")
 *     .withFileCount(5)
 *     .processing()
 *     .build();
 * }</pre>
 */
public class ConsolidationTaskBuilder {
  // Default test constants
  private static final long DEFAULT_MIN_TIMESTAMP = 1704067200000L; // 2024-01-01 00:00:00 UTC
  private static final String DEFAULT_APP_ID = "test-app-001";
  private static final String DEFAULT_ARCHIVE_PATH =
      "/clp-archives/spark/2024-01-01/test-app-001.clp";

  private String applicationId = DEFAULT_APP_ID;
  private String archivePath = DEFAULT_ARCHIVE_PATH;
  private List<String> irFilePaths = new ArrayList<>();
  private long submittedAt = DEFAULT_MIN_TIMESTAMP;
  private long processingStartedAt = 0L;
  private long completedAt = 0L;
  private long actualArchiveSizeBytes = 0L;
  private String workerId = null;
  private String errorMessage = null;
  private ConsolidationTask.TaskStatus targetStatus = ConsolidationTask.TaskStatus.SUBMITTED;

  /** Set the application ID. */
  public ConsolidationTaskBuilder withAppId(String appId) {
    this.applicationId = appId;
    this.archivePath = String.format("/clp-archives/spark/2024-01-01/%s.clp", appId);
    return this;
  }

  /** Generate N unique IR file paths. */
  public ConsolidationTaskBuilder withFileCount(int count) {
    this.irFilePaths.clear();
    for (int i = 0; i < count; i++) {
      String path =
          String.format("/spark/%s/executor-%d/file-%03d.clp.zst", applicationId, (i % 3) + 1, i);
      this.irFilePaths.add(path);
    }
    return this;
  }

  /** Set submitted timestamp. */
  public ConsolidationTaskBuilder withSubmittedAt(long timestamp) {
    this.submittedAt = timestamp;
    return this;
  }

  /** Set error message. */
  public ConsolidationTaskBuilder withErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
    return this;
  }

  // State presets

  /** Configure task as submitted (initial state). */
  public ConsolidationTaskBuilder submitted() {
    this.targetStatus = ConsolidationTask.TaskStatus.SUBMITTED;
    this.processingStartedAt = 0L;
    this.completedAt = 0L;
    this.workerId = null;
    return this;
  }

  /** Configure task as processing (worker claimed). */
  public ConsolidationTaskBuilder processing() {
    this.targetStatus = ConsolidationTask.TaskStatus.PROCESSING;
    this.processingStartedAt = submittedAt + 5000L; // 5 seconds after submission
    this.completedAt = 0L;
    this.workerId = "worker-1";
    return this;
  }

  /** Configure task as completed successfully. */
  public ConsolidationTaskBuilder completed() {
    this.targetStatus = ConsolidationTask.TaskStatus.COMPLETED;
    this.processingStartedAt = submittedAt + 5000L;
    this.completedAt = processingStartedAt + 10000L; // 10 seconds to complete
    this.workerId = "worker-1";
    // Set realistic archive size if not already set
    if (actualArchiveSizeBytes == 0L) {
      this.actualArchiveSizeBytes = irFilePaths.size() * 1024 * 1024 / 3; // ~3x compression
    }
    return this;
  }

  /** Configure task as failed. */
  public ConsolidationTaskBuilder failed() {
    this.targetStatus = ConsolidationTask.TaskStatus.FAILED;
    this.processingStartedAt = submittedAt + 5000L;
    this.completedAt = processingStartedAt + 3000L; // Failed after 3 seconds
    this.workerId = "worker-1";
    if (errorMessage == null) {
      this.errorMessage = "Simulated failure for testing";
    }
    return this;
  }

  /**
   * Build the ConsolidationTask instance.
   *
   * <p>If no IR file paths are set, generates 3 default paths.
   */
  public ConsolidationTask build() {
    // Generate default file paths if none provided
    if (irFilePaths.isEmpty()) {
      withFileCount(3);
    }

    // Create task using constructor (which generates taskId and sets initial state)
    ConsolidationTask task = new ConsolidationTask(archivePath, irFilePaths, applicationId);

    // Set timing fields
    task.setSubmittedAt(submittedAt);
    if (processingStartedAt > 0) {
      task.setProcessingStartedAt(processingStartedAt);
    }
    if (completedAt > 0) {
      task.setCompletedAt(completedAt);
    }

    // Set optional fields
    if (actualArchiveSizeBytes > 0) {
      task.setActualArchiveSizeBytes(actualArchiveSizeBytes);
    }
    if (workerId != null) {
      task.setWorkerId(workerId);
    }
    if (errorMessage != null) {
      task.setErrorMessage(errorMessage);
    }

    // Set final status
    task.setStatus(targetStatus);

    return task;
  }

  /**
   * Build and execute task lifecycle to PROCESSING state. Uses actual ConsolidationTask methods for
   * realistic testing.
   */
  public ConsolidationTask buildAndStart() {
    ConsolidationTask task = submitted().build();
    task.markStarted();
    return task;
  }

  /**
   * Build and execute task lifecycle to COMPLETED state. Uses actual ConsolidationTask methods for
   * realistic testing.
   */
  public ConsolidationTask buildAndComplete() {
    ConsolidationTask task = submitted().build();
    task.markStarted();
    long archiveSize = actualArchiveSizeBytes > 0 ? actualArchiveSizeBytes : 3_000_000L;
    task.markCompleted(archiveSize);
    return task;
  }

  /**
   * Build and execute task lifecycle to FAILED state. Uses actual ConsolidationTask methods for
   * realistic testing.
   */
  public ConsolidationTask buildAndFail() {
    ConsolidationTask task = submitted().build();
    task.markStarted();
    String error = errorMessage != null ? errorMessage : "Test failure";
    task.markFailed(error);
    return task;
  }
}
