package com.yscope.clp.service.metastore.model;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.clp.service.testutil.ConsolidationTaskBuilder;
import com.yscope.clp.service.testutil.TestDataFactory;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for ConsolidationTask domain model.
 *
 * <p>Validates: - Task lifecycle: SUBMITTED→PROCESSING→COMPLETED/FAILED - Timing metrics:
 * getDurationMs(), getTotalLatencyMs() - Immutability of irFilePaths list - UUID uniqueness of
 * taskId - Equality based on taskId only
 */
class ConsolidationTaskTest {

  // Constructor tests

  @Test
  void testConstructor_generatesUniqueTaskId() {
    ConsolidationTask task1 =
        new ConsolidationTask("/archive1.clp", Arrays.asList("/file1.clp"), "app-001");

    ConsolidationTask task2 =
        new ConsolidationTask("/archive1.clp", Arrays.asList("/file1.clp"), "app-001");

    assertNotEquals(task1.getTaskId(), task2.getTaskId());
  }

  @Test
  void testConstructor_initializesFieldsCorrectly() {
    List<String> paths = Arrays.asList("/file1.clp", "/file2.clp");
    long beforeCreate = System.currentTimeMillis();

    ConsolidationTask task = new ConsolidationTask("/archive.clp", paths, "app-001");

    long afterCreate = System.currentTimeMillis();

    assertNotNull(task.getTaskId());
    assertEquals("/archive.clp", task.getArchivePath());
    assertEquals(paths, task.getIrFilePaths());
    assertEquals("app-001", task.getApplicationId());
    assertEquals(ConsolidationTask.TaskStatus.SUBMITTED, task.getStatus());
    assertTrue(task.getCreatedAt() >= beforeCreate);
    assertTrue(task.getCreatedAt() <= afterCreate);
  }

  @Test
  void testConstructor_irFilePathsImmutable() {
    List<String> originalPaths = Arrays.asList("/file1.clp", "/file2.clp");

    ConsolidationTask task = new ConsolidationTask("/archive.clp", originalPaths, "app-001");

    // Try to modify returned list - should throw UnsupportedOperationException
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          task.getIrFilePaths().add("/file3.clp");
        });

    // Verify list size unchanged
    assertEquals(2, task.getIrFilePaths().size());
  }

  // Lifecycle transition tests

  @Test
  void testMarkStarted_transitionsToProcessing() {
    ConsolidationTask task = TestDataFactory.createSimpleTask();
    long beforeStart = System.currentTimeMillis();

    task.markStarted();

    long afterStart = System.currentTimeMillis();

    assertEquals(ConsolidationTask.TaskStatus.PROCESSING, task.getStatus());
    assertTrue(task.getProcessingStartedAt() >= beforeStart);
    assertTrue(task.getProcessingStartedAt() <= afterStart);
  }

  @Test
  void testMarkCompleted_transitionsToCompleted() {
    ConsolidationTask task = TestDataFactory.createSimpleTask();
    task.markStarted();
    long beforeComplete = System.currentTimeMillis();

    task.markCompleted(1024 * 1024);

    long afterComplete = System.currentTimeMillis();

    assertEquals(ConsolidationTask.TaskStatus.COMPLETED, task.getStatus());
    assertTrue(task.getCompletedAt() >= beforeComplete);
    assertTrue(task.getCompletedAt() <= afterComplete);
    assertEquals(1024 * 1024, task.getActualArchiveSizeBytes());
  }

  @Test
  void testMarkFailed_transitionsToFailed() {
    ConsolidationTask task = TestDataFactory.createSimpleTask();
    task.markStarted();
    long beforeFail = System.currentTimeMillis();

    task.markFailed("Storage unavailable");

    long afterFail = System.currentTimeMillis();

    assertEquals(ConsolidationTask.TaskStatus.FAILED, task.getStatus());
    assertTrue(task.getCompletedAt() >= beforeFail);
    assertTrue(task.getCompletedAt() <= afterFail);
    assertEquals("Storage unavailable", task.getErrorMessage());
  }

  // Timing metrics tests

  @Test
  void testGetDurationMs_notStarted_returnsZero() {
    ConsolidationTask task = TestDataFactory.createSimpleTask();

    assertEquals(0L, task.getDurationMs());
  }

  @Test
  void testGetDurationMs_inProgress_returnsElapsedTime() throws InterruptedException {
    ConsolidationTask task = new ConsolidationTaskBuilder().buildAndStart();

    Thread.sleep(10); // Wait 10ms

    long duration = task.getDurationMs();
    assertTrue(duration >= 10, "Duration should be at least 10ms");
  }

  @Test
  void testGetDurationMs_completed_returnsTotalDuration() {
    // Build task with explicit timestamps, don't use completed() preset
    ConsolidationTask task =
        new ConsolidationTaskBuilder().withSubmittedAt(1000L).submitted().build();

    // Manually set timestamps to control the duration
    task.setProcessingStartedAt(2000L);
    task.setCompletedAt(5000L);
    task.setStatus(ConsolidationTask.TaskStatus.COMPLETED);

    assertEquals(3000L, task.getDurationMs()); // 5000 - 2000
  }

  @Test
  void testGetTotalLatencyMs_notSubmitted_returnsZero() {
    ConsolidationTask task =
        new ConsolidationTask("/archive.clp", Arrays.asList("/file.clp"), "app-001");
    task.setSubmittedAt(0L);

    assertEquals(0L, task.getTotalLatencyMs());
  }

  @Test
  void testGetTotalLatencyMs_inProgress_returnsElapsedTime() throws InterruptedException {
    ConsolidationTask task =
        new ConsolidationTaskBuilder().withSubmittedAt(System.currentTimeMillis()).buildAndStart();

    Thread.sleep(10); // Wait 10ms

    long latency = task.getTotalLatencyMs();
    assertTrue(latency >= 10, "Latency should be at least 10ms");
  }

  @Test
  void testGetTotalLatencyMs_completed_returnsTotalLatency() {
    // Build task with explicit timestamps, don't use completed() preset
    ConsolidationTask task =
        new ConsolidationTaskBuilder().withSubmittedAt(1000L).submitted().build();

    // Manually set timestamps to control the latency
    task.setSubmittedAt(1000L);
    task.setProcessingStartedAt(2000L);
    task.setCompletedAt(5000L);
    task.setStatus(ConsolidationTask.TaskStatus.COMPLETED);

    assertEquals(4000L, task.getTotalLatencyMs()); // 5000 - 1000
  }

  // File count tests

  @Test
  void testGetFileCount_returnsCorrectCount() {
    ConsolidationTask task = new ConsolidationTaskBuilder().withFileCount(5).build();

    assertEquals(5, task.getFileCount());
    assertEquals(5, task.getIrFilePaths().size());
  }

  // Equality tests (based on taskId only)

  @Test
  void testEquals_differentTaskId_returnsFalse() {
    ConsolidationTask task1 = new ConsolidationTaskBuilder().withAppId("app-001").build();
    ConsolidationTask task2 = new ConsolidationTaskBuilder().withAppId("app-002").build();
    assertNotEquals(task1, task2);
  }
}
