package com.yscope.clp.service.metastore;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.clp.service.metastore.TaskQueue.Task;
import com.yscope.clp.service.metastore.TaskQueue.TaskCounts;
import com.yscope.clp.service.metastore.model.TaskPayload;
import com.yscope.clp.service.testutil.AbstractMariaDBTest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link TaskQueue} against a real MariaDB instance.
 *
 * <p>Validates SQL correctness for task lifecycle operations including SELECT FOR UPDATE claiming,
 * state transitions, dead-letter handling, and cleanup.
 */
class TaskQueueIT extends AbstractMariaDBTest {

  private static final String TABLE_NAME = "clp_table";
  private static final String WORKER_ID = "worker-1";

  private TaskQueue taskQueue;

  @BeforeEach
  void setUp() throws Exception {
    registerTable(TABLE_NAME);
    taskQueue = new TaskQueue(getDsl());
  }

  private byte[] testPayload() {
    return new TaskPayload(
            "/archives/test.clp",
            List.of("/ir/file1.zst", "/ir/file2.zst"),
            TABLE_NAME,
            "s3",
            "ir-bucket",
            "s3",
            "archive-bucket")
        .serialize();
  }

  // ========================================================================
  // Create
  // ========================================================================

  @Test
  void createTask_returnsPositiveId() throws SQLException {
    long taskId = taskQueue.createTask(TABLE_NAME, testPayload());
    assertTrue(taskId > 0);
  }

  @Test
  void createTask_multipleTasksGetSequentialIds() throws SQLException {
    long id1 = taskQueue.createTask(TABLE_NAME, testPayload());
    long id2 = taskQueue.createTask(TABLE_NAME, testPayload());
    assertTrue(id2 > id1);
  }

  // ========================================================================
  // Claim
  // ========================================================================

  @Test
  void claimTask_returnsPendingTask() throws SQLException {
    long taskId = taskQueue.createTask(TABLE_NAME, testPayload());

    Optional<Task> claimed = taskQueue.claimTask(TABLE_NAME, WORKER_ID);

    assertTrue(claimed.isPresent());
    assertEquals(taskId, claimed.get().getTaskId());
    assertEquals(WORKER_ID, claimed.get().getWorkerId());
    assertEquals(TABLE_NAME, claimed.get().getTableName());
  }

  @Test
  void claimTask_emptyQueueReturnsEmpty() throws SQLException {
    Optional<Task> claimed = taskQueue.claimTask(TABLE_NAME, WORKER_ID);
    assertTrue(claimed.isEmpty());
  }

  @Test
  void claimTask_fromAnyTable() throws SQLException {
    taskQueue.createTask(TABLE_NAME, testPayload());

    Optional<Task> claimed = taskQueue.claimTask(WORKER_ID);

    assertTrue(claimed.isPresent());
    assertEquals(TABLE_NAME, claimed.get().getTableName());
  }

  @Test
  void claimTasks_batchClaimRespectsLimit() throws SQLException {
    for (int i = 0; i < 5; i++) {
      taskQueue.createTask(TABLE_NAME, testPayload());
    }

    List<Task> claimed = taskQueue.claimTasks(TABLE_NAME, WORKER_ID, 3);

    assertEquals(3, claimed.size());
    // Verify remaining 2 are still pending
    TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    assertEquals(2, counts.pending);
    assertEquals(3, counts.processing);
  }

  @Test
  void claimTask_byTableFiltersCorrectly() throws SQLException {
    // Register a second table
    registerTable("other_table");
    taskQueue.createTask(TABLE_NAME, testPayload());
    taskQueue.createTask("other_table", testPayload());

    Optional<Task> claimed = taskQueue.claimTask("other_table", WORKER_ID);

    assertTrue(claimed.isPresent());
    assertEquals("other_table", claimed.get().getTableName());

    // clp_table task should still be pending
    TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    assertEquals(1, counts.pending);
  }

  @Test
  void claimTask_alreadyClaimedNotClaimedAgain() throws SQLException {
    taskQueue.createTask(TABLE_NAME, testPayload());

    taskQueue.claimTask(TABLE_NAME, WORKER_ID);
    Optional<Task> second = taskQueue.claimTask(TABLE_NAME, "worker-2");

    assertTrue(second.isEmpty());
  }

  // ========================================================================
  // Complete
  // ========================================================================

  @Test
  void completeTask_setsCompletedState() throws SQLException {
    long taskId = taskQueue.createTask(TABLE_NAME, testPayload());
    taskQueue.claimTask(TABLE_NAME, WORKER_ID);

    int updated = taskQueue.completeTask(taskId);

    assertEquals(1, updated);
    TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    assertEquals(1, counts.completed);
    assertEquals(0, counts.processing);
  }

  @Test
  void completeTask_withOutput() throws SQLException {
    long taskId = taskQueue.createTask(TABLE_NAME, testPayload());
    taskQueue.claimTask(TABLE_NAME, WORKER_ID);
    byte[] output = "result-data".getBytes();

    int updated = taskQueue.completeTask(taskId, output);

    assertEquals(1, updated);
    List<Task> completed = taskQueue.findCompletedTasks(TABLE_NAME);
    assertEquals(1, completed.size());
    assertArrayEquals(output, completed.get(0).getOutput());
  }

  @Test
  void completeTask_pendingTaskNotCompleted() throws SQLException {
    long taskId = taskQueue.createTask(TABLE_NAME, testPayload());
    // Don't claim — task is still pending

    int updated = taskQueue.completeTask(taskId);

    assertEquals(0, updated);
  }

  @Test
  void completeTasks_batchComplete() throws SQLException {
    long id1 = taskQueue.createTask(TABLE_NAME, testPayload());
    long id2 = taskQueue.createTask(TABLE_NAME, testPayload());
    taskQueue.claimTasks(TABLE_NAME, WORKER_ID, 2);

    int updated = taskQueue.completeTasks(List.of(id1, id2));

    assertEquals(2, updated);
    TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    assertEquals(2, counts.completed);
  }

  // ========================================================================
  // Fail
  // ========================================================================

  @Test
  void failTask_setsFailedState() throws SQLException {
    long taskId = taskQueue.createTask(TABLE_NAME, testPayload());
    taskQueue.claimTask(TABLE_NAME, WORKER_ID);

    int updated = taskQueue.failTask(taskId);

    assertEquals(1, updated);
    TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    assertEquals(1, counts.failed);
    assertEquals(0, counts.processing);
  }

  // ========================================================================
  // Stale detection and reclaim
  // ========================================================================

  @Test
  void findStaleTasks_findsOldProcessingTasks() throws SQLException {
    long taskId = taskQueue.createTask(TABLE_NAME, testPayload());
    taskQueue.claimTask(TABLE_NAME, WORKER_ID);

    // Manually backdate claimed_at to simulate a stale task
    backdateClaimedAt(taskId, 600);

    List<Task> stale = taskQueue.findStaleTasks(TABLE_NAME, 300);

    assertEquals(1, stale.size());
    assertEquals(taskId, stale.get(0).getTaskId());
    assertEquals(WORKER_ID, stale.get(0).getWorkerId());
  }

  @Test
  void findStaleTasks_recentTasksNotStale() throws SQLException {
    taskQueue.createTask(TABLE_NAME, testPayload());
    taskQueue.claimTask(TABLE_NAME, WORKER_ID);

    List<Task> stale = taskQueue.findStaleTasks(TABLE_NAME, 300);

    assertTrue(stale.isEmpty());
  }

  @Test
  void reclaimTask_createsNewPendingTask() throws SQLException {
    long taskId = taskQueue.createTask(TABLE_NAME, testPayload());
    Task claimed = taskQueue.claimTask(TABLE_NAME, WORKER_ID).orElseThrow();

    long newTaskId = taskQueue.reclaimTask(claimed, 3);

    assertTrue(newTaskId > 0);
    assertTrue(newTaskId > taskId);
    // Original task should be timed_out
    TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    assertEquals(1, counts.pending);
    assertEquals(1, counts.timedOut);
  }

  @Test
  void reclaimTask_deadLetterWhenMaxRetriesExceeded() throws SQLException {
    taskQueue.createTask(TABLE_NAME, testPayload());
    Task claimed = taskQueue.claimTask(TABLE_NAME, WORKER_ID).orElseThrow();
    claimed.setRetryCount(3); // Already at max

    long result = taskQueue.reclaimTask(claimed, 3);

    assertEquals(-1L, result);
    TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    assertEquals(1, counts.deadLetter);
    assertEquals(0, counts.pending);
  }

  // ========================================================================
  // Find completed
  // ========================================================================

  @Test
  void findCompletedTasks_returnsCompletedOnly() throws SQLException {
    long id1 = taskQueue.createTask(TABLE_NAME, testPayload());
    long id2 = taskQueue.createTask(TABLE_NAME, testPayload());
    taskQueue.claimTasks(TABLE_NAME, WORKER_ID, 2);
    taskQueue.completeTask(id1);
    // id2 stays in processing

    List<Task> completed = taskQueue.findCompletedTasks(TABLE_NAME);

    assertEquals(1, completed.size());
    assertEquals(id1, completed.get(0).getTaskId());
  }

  // ========================================================================
  // Delete
  // ========================================================================

  @Test
  void deleteTask_removesTask() throws SQLException {
    long taskId = taskQueue.createTask(TABLE_NAME, testPayload());

    int deleted = taskQueue.deleteTask(taskId);

    assertEquals(1, deleted);
    TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    assertEquals(0, counts.getTotal());
  }

  // ========================================================================
  // Cleanup
  // ========================================================================

  @Test
  void cleanup_deletesOldCompletedAndFailed() throws SQLException {
    long id1 = taskQueue.createTask(TABLE_NAME, testPayload());
    long id2 = taskQueue.createTask(TABLE_NAME, testPayload());
    taskQueue.claimTasks(TABLE_NAME, WORKER_ID, 2);
    taskQueue.completeTask(id1);
    taskQueue.failTask(id2);

    // Backdate completed_at
    backdateCompletedAt(id1, 3600);
    backdateCompletedAt(id2, 3600);

    int cleaned = taskQueue.cleanup(TABLE_NAME, 1800);

    assertEquals(2, cleaned);
    TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    assertEquals(0, counts.getTotal());
  }

  @Test
  void cleanup_preservesRecentTasks() throws SQLException {
    long taskId = taskQueue.createTask(TABLE_NAME, testPayload());
    taskQueue.claimTask(TABLE_NAME, WORKER_ID);
    taskQueue.completeTask(taskId);
    // completed_at is now (just completed), grace period 1800s hasn't passed

    int cleaned = taskQueue.cleanup(TABLE_NAME, 1800);

    assertEquals(0, cleaned);
  }

  // ========================================================================
  // Reset to pending
  // ========================================================================

  @Test
  void resetTasksToPending_resetsProcessingTasks() throws SQLException {
    long id1 = taskQueue.createTask(TABLE_NAME, testPayload());
    long id2 = taskQueue.createTask(TABLE_NAME, testPayload());
    taskQueue.claimTasks(TABLE_NAME, WORKER_ID, 2);

    int reset = taskQueue.resetTasksToPending(List.of(id1, id2));

    assertEquals(2, reset);
    TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    assertEquals(2, counts.pending);
    assertEquals(0, counts.processing);
  }

  // ========================================================================
  // Delete all except dead letter
  // ========================================================================

  @Test
  void deleteAllExceptDeadLetter_preservesDeadLetterOnly() throws SQLException {
    // Create tasks in various states
    long id1 = taskQueue.createTask(TABLE_NAME, testPayload());
    long id2 = taskQueue.createTask(TABLE_NAME, testPayload());
    long id3 = taskQueue.createTask(TABLE_NAME, testPayload());
    taskQueue.claimTasks(TABLE_NAME, WORKER_ID, 2);
    taskQueue.completeTask(id1);
    // Manually set id2 to dead_letter
    Task stale = taskQueue.claimTask(TABLE_NAME, WORKER_ID).orElseThrow();
    // id3 is now claimed
    taskQueue.claimTask(TABLE_NAME, WORKER_ID); // claim id3

    // Move id2 to dead_letter by reclaiming with max retries exceeded
    Task task2 = new Task();
    task2.setTaskId(id2);
    task2.setTableName(TABLE_NAME);
    task2.setInput(testPayload());
    task2.setRetryCount(4);
    taskQueue.reclaimTask(task2, 3);

    int deleted = taskQueue.deleteAllExceptDeadLetter(TABLE_NAME);

    assertTrue(deleted > 0);
    TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    assertEquals(1, counts.deadLetter);
    assertEquals(counts.deadLetter, counts.getTotal());
  }

  // ========================================================================
  // Task counts
  // ========================================================================

  @Test
  void getTaskCounts_reflectsAllStates() throws SQLException {
    long id1 = taskQueue.createTask(TABLE_NAME, testPayload());
    long id2 = taskQueue.createTask(TABLE_NAME, testPayload());
    long id3 = taskQueue.createTask(TABLE_NAME, testPayload());
    taskQueue.claimTasks(TABLE_NAME, WORKER_ID, 2);
    taskQueue.completeTask(id1);
    // id2 = processing, id3 = pending

    TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);

    assertEquals(1, counts.pending);
    assertEquals(1, counts.processing);
    assertEquals(1, counts.completed);
    assertEquals(3, counts.getTotal());
    assertEquals(2, counts.getIncomplete());
  }

  // ========================================================================
  // Database timestamp
  // ========================================================================

  @Test
  void getDatabaseTimestamp_returnsReasonableValue() throws SQLException {
    long ts = taskQueue.getDatabaseTimestamp();

    // Should be a reasonable epoch (after 2024-01-01 and before 2100-01-01)
    assertTrue(ts > 1704067200L, "Timestamp should be after 2024-01-01");
    assertTrue(ts < 4102444800L, "Timestamp should be before 2100-01-01");
  }

  // ========================================================================
  // Task inner class - parsed input
  // ========================================================================

  @Test
  void task_getParsedInput_lazilyDeserializes() throws SQLException {
    byte[] payload = testPayload();
    taskQueue.createTask(TABLE_NAME, payload);
    Task claimed = taskQueue.claimTask(TABLE_NAME, WORKER_ID).orElseThrow();

    TaskPayload parsed = claimed.getParsedInput();

    assertNotNull(parsed);
    assertEquals("/archives/test.clp", parsed.getArchivePath());
    assertEquals(2, parsed.getIrFilePaths().size());
    assertEquals(TABLE_NAME, parsed.getTableName());
  }

  // ========================================================================
  // Helpers
  // ========================================================================

  private void backdateClaimedAt(long taskId, int secondsAgo) throws SQLException {
    try (Connection conn = getDataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "UPDATE _task_queue SET claimed_at = UNIX_TIMESTAMP() - ? WHERE task_id = ?")) {
      ps.setInt(1, secondsAgo);
      ps.setLong(2, taskId);
      ps.executeUpdate();
    }
  }

  private void backdateCompletedAt(long taskId, int secondsAgo) throws SQLException {
    try (Connection conn = getDataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "UPDATE _task_queue SET completed_at = UNIX_TIMESTAMP() - ? WHERE task_id = ?")) {
      ps.setInt(1, secondsAgo);
      ps.setLong(2, taskId);
      ps.executeUpdate();
    }
  }
}
