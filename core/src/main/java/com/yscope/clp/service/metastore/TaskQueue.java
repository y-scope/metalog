package com.yscope.clp.service.metastore;

import static com.yscope.clp.service.metastore.JooqFields.*;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.val;

import com.yscope.clp.service.common.config.Timeouts;
import com.yscope.clp.service.metastore.model.TaskPayload;
import com.yscope.clp.service.metastore.model.TaskResult;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Database-backed task queue for worker coordination.
 *
 * <p>A {@link com.yscope.clp.service.worker.TaskPrefetcher} per node batch-claims tasks using
 * {@code SELECT ... FOR UPDATE} + {@code UPDATE} in READ COMMITTED isolation. READ COMMITTED is
 * required so that a claimer waiting on a locked row re-evaluates {@code WHERE state='pending'}
 * after the lock releases — the row is now {@code processing}, so it is skipped and the claimer
 * advances to the next pending row. This gives correct fan-out across nodes without SKIP LOCKED.
 *
 * <p>Task lifecycle:
 *
 * <ol>
 *   <li>Planner inserts tasks with state='pending'
 *   <li>Workers claim tasks via SELECT ... FOR UPDATE + UPDATE
 *   <li>Workers update state to 'processing', then 'completed' or 'failed'
 *   <li>Planner processes completed tasks (updates metadata, queues IR deletion)
 *   <li>Planner reclaims stale tasks (processing too long)
 *   <li>Planner cleans up old completed/failed/timed_out tasks
 * </ol>
 *
 * <p>State transitions:
 *
 * <pre>
 *   pending → processing → completed
 *                       → failed
 *                       → timed_out (reclaimed) → pending (new task)
 *                       → dead_letter (max retries exceeded)
 * </pre>
 */
public class TaskQueue {
  private static final Logger logger = LoggerFactory.getLogger(TaskQueue.class);

  /** Default maximum retry count before moving to dead letter. */
  public static final int DEFAULT_MAX_RETRIES = 3;

  private final DSLContext dsl;

  public TaskQueue(DSLContext dsl) {
    this.dsl = dsl;
  }



  /**
   * Create a new pending task in the queue.
   *
   * @param tableName The target metadata table
   * @param input Serialized task input (TaskPayload)
   * @return The generated task ID
   * @throws SQLException if database operation fails
   */
  public long createTask(String tableName, byte[] input) throws SQLException {
    try {
      return dsl.connectionResult(
          conn -> {
            DSLContext cDsl = DSL.using(conn, dsl.dialect(), dsl.settings());
            cDsl.insertInto(TASK_QUEUE).set(TABLE_NAME, tableName).set(INPUT, input).execute();
            long taskId =
                cDsl.select(DSL.field("LAST_INSERT_ID()", Long.class)).fetchOne().value1();
            logger.debug("Created task {} for table {}", taskId, tableName);
            return taskId;
          });
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Claim a pending task from any table.
   *
   * @param workerId The worker claiming the task
   * @return The claimed task, or empty if no tasks available
   * @throws SQLException if database operation fails
   */
  public Optional<Task> claimTask(String workerId) throws SQLException {
    List<Task> tasks = claimTasks(workerId, 1);
    return tasks.isEmpty() ? Optional.empty() : Optional.of(tasks.get(0));
  }

  /**
   * Claim a pending task for processing.
   *
   * @param tableName The target metadata table
   * @param workerId The worker claiming the task
   * @return The claimed task, or empty if no tasks available
   * @throws SQLException if database operation fails
   */
  public Optional<Task> claimTask(String tableName, String workerId) throws SQLException {
    List<Task> tasks = claimTasks(tableName, workerId, 1);
    return tasks.isEmpty() ? Optional.empty() : Optional.of(tasks.get(0));
  }

  /**
   * Claim multiple pending tasks from any table (batch claim).
   *
   * <p>Uses {@code SELECT ... FOR UPDATE} + {@code UPDATE} in READ COMMITTED isolation. Retries
   * automatically on deadlock (InnoDB error 1213).
   *
   * @param workerId The worker claiming the tasks
   * @param batchSize Maximum number of tasks to claim
   * @return List of claimed tasks (may be empty or smaller than batchSize)
   * @throws SQLException if database operation fails
   */
  public List<Task> claimTasks(String workerId, int batchSize) throws SQLException {
    return claimTasksWithRetry(null, workerId, batchSize);
  }

  /**
   * Claim multiple pending tasks for processing (batch claim).
   *
   * <p>Uses {@code SELECT ... FOR UPDATE} + {@code UPDATE} in READ COMMITTED isolation. Retries
   * automatically on deadlock (InnoDB error 1213).
   *
   * @param tableName The target metadata table
   * @param workerId The worker claiming the tasks
   * @param batchSize Maximum number of tasks to claim
   * @return List of claimed tasks (may be empty or smaller than batchSize)
   * @throws SQLException if database operation fails
   */
  public List<Task> claimTasks(String tableName, String workerId, int batchSize)
      throws SQLException {
    return claimTasksWithRetry(tableName, workerId, batchSize);
  }

  /** Maximum number of deadlock retries before giving up. */
  private static final int MAX_DEADLOCK_RETRIES = 10;

  private List<Task> claimTasksWithRetry(String tableName, String workerId, int batchSize)
      throws SQLException {
    int attempts = 0;
    while (true) {
      try {
        return dsl.connectionResult(
            conn -> claimTasksWithConnection(conn, tableName, workerId, batchSize));
      } catch (DataAccessException e) {
        SQLException cause = e.getCause() instanceof SQLException s ? s : null;
        if (cause != null && cause.getErrorCode() == 1213) {
          attempts++;
          if (attempts >= MAX_DEADLOCK_RETRIES) {
            logger.error(
                "Deadlock retry limit ({}) exceeded for claim by worker {}",
                MAX_DEADLOCK_RETRIES,
                workerId);
            throw cause;
          }
          // Deadlock — retryable. Sleep 1–CLAIM_DEADLOCK_JITTER_MAX_MS ms
          // of random jitter to desynchronise concurrent claimers before retrying.
          logger.debug("Deadlock on claim (attempt {}), retrying", attempts);
          try {
            Thread.sleep(1 + (long) (Math.random() * Timeouts.CLAIM_DEADLOCK_JITTER_MAX_MS));
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw cause;
          }
          continue;
        }
        throw cause != null ? cause : new SQLException(e.getMessage(), e);
      }
    }
  }

  /**
   * Claim tasks using {@code SELECT ... FOR UPDATE} + {@code UPDATE} in a transaction.
   *
   * <p>READ COMMITTED isolation is required so that when a claimer waits on a locked row and the
   * lock is released, the {@code WHERE state='pending'} condition is re-evaluated against the
   * now-committed value. The row is {@code processing} → skip it and advance to the next pending
   * row. This is how multiple nodes fan out correctly without SKIP LOCKED.
   *
   * @param conn Raw JDBC connection (from jOOQ's connection pool)
   * @param tableName Optional table name filter (null = all tables)
   * @param workerId Worker claiming the tasks
   * @param batchSize Maximum number of tasks to claim
   * @return List of claimed tasks
   */
  private List<Task> claimTasksWithConnection(
      Connection conn, String tableName, String workerId, int batchSize) throws SQLException {
    int originalIsolation = conn.getTransactionIsolation();
    boolean originalAutoCommit = conn.getAutoCommit();
    conn.setAutoCommit(false);
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    try {
      DSLContext txDsl = DSL.using(conn, dsl.dialect(), dsl.settings());

      var select =
          txDsl
              .select(TASK_ID, TABLE_NAME, STATE, RETRY_COUNT, INPUT)
              .from(TASK_QUEUE)
              .where(STATE.eq("pending"));
      if (tableName != null) {
        select = select.and(TABLE_NAME.eq(tableName));
      }
      var limited = select.orderBy(TASK_ID.asc()).limit(batchSize);
      var rows = txDsl.fetch(limited.getSQL() + " FOR UPDATE", limited.getBindValues().toArray());

      List<Task> tasks = new ArrayList<>();
      for (var r : rows) {
        tasks.add(mapRecordToTask(r));
      }
      if (tasks.isEmpty()) {
        conn.commit();
        return tasks;
      }

      List<Long> taskIds = tasks.stream().map(Task::getTaskId).toList();
      txDsl
          .update(TASK_QUEUE)
          .set(STATE, "processing")
          .set(WORKER_ID, workerId)
          .set(CLAIMED_AT, unixTimestamp())
          .where(TASK_ID.in(taskIds))
          .execute();

      conn.commit();

      for (Task task : tasks) {
        task.setWorkerId(workerId);
      }
      logger.debug("Worker {} claimed {} tasks", workerId, tasks.size());
      return tasks;
    } catch (Exception e) {
      try {
        conn.rollback();
      } catch (SQLException rollbackEx) {
        logger.warn("Rollback failed after claim error", rollbackEx);
      }
      throw e;
    } finally {
      conn.setTransactionIsolation(originalIsolation);
      conn.setAutoCommit(originalAutoCommit);
    }
  }

  /**
   * Mark a task as completed (without output).
   *
   * @param taskId The task ID
   * @return Number of rows affected (0 or 1)
   * @throws SQLException if database operation fails
   */
  public int completeTask(long taskId) throws SQLException {
    try {
      int updated =
          dsl.update(TASK_QUEUE)
              .set(STATE, "completed")
              .set(COMPLETED_AT, unixTimestamp())
              .where(TASK_ID.eq(taskId))
              .and(STATE.eq("processing"))
              .execute();

      if (updated > 0) {
        logger.debug("Task {} completed", taskId);
      } else {
        logger.warn("Task {} not found or not in processing state", taskId);
      }
      return updated;
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Mark a task as completed with output data.
   *
   * @param taskId The task ID
   * @param output Serialized task output (TaskResult)
   * @return Number of rows affected (0 or 1)
   * @throws SQLException if database operation fails
   */
  public int completeTask(long taskId, byte[] output) throws SQLException {
    try {
      int updated =
          dsl.update(TASK_QUEUE)
              .set(STATE, "completed")
              .set(COMPLETED_AT, unixTimestamp())
              .set(OUTPUT, output)
              .where(TASK_ID.eq(taskId))
              .and(STATE.eq("processing"))
              .execute();

      if (updated > 0) {
        logger.debug("Task {} completed with output", taskId);
      } else {
        logger.warn("Task {} not found or not in processing state", taskId);
      }
      return updated;
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Mark multiple tasks as completed (batch complete).
   *
   * @param taskIds The task IDs to complete
   * @return Number of rows affected
   * @throws SQLException if database operation fails
   */
  public int completeTasks(List<Long> taskIds) throws SQLException {
    if (taskIds == null || taskIds.isEmpty()) {
      return 0;
    }
    if (taskIds.size() == 1) {
      return completeTask(taskIds.get(0));
    }

    try {
      int updated =
          dsl.update(TASK_QUEUE)
              .set(STATE, "completed")
              .set(COMPLETED_AT, unixTimestamp())
              .where(TASK_ID.in(taskIds))
              .and(STATE.eq("processing"))
              .execute();

      if (updated > 0) {
        logger.debug("Completed {} tasks", updated);
      }
      if (updated < taskIds.size()) {
        logger.warn(
            "{} of {} tasks not found or not in processing state",
            taskIds.size() - updated,
            taskIds.size());
      }
      return updated;
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Mark a task as failed.
   *
   * @param taskId The task ID
   * @return Number of rows affected
   * @throws SQLException if database operation fails
   */
  public int failTask(long taskId) throws SQLException {
    try {
      int updated =
          dsl.update(TASK_QUEUE)
              .set(STATE, "failed")
              .set(COMPLETED_AT, unixTimestamp())
              .where(TASK_ID.eq(taskId))
              .and(STATE.eq("processing"))
              .execute();

      if (updated > 0) {
        logger.info("Task {} marked as failed", taskId);
      }
      return updated;
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Find tasks that have been processing for longer than the timeout.
   *
   * @param tableName The target metadata table
   * @param timeoutSeconds Processing timeout in seconds
   * @return List of stale tasks
   * @throws SQLException if database operation fails
   */
  public List<Task> findStaleTasks(String tableName, int timeoutSeconds) throws SQLException {
    try {
      List<Task> tasks = new ArrayList<>();
      var rows =
          dsl.select(TASK_ID, TABLE_NAME, STATE, RETRY_COUNT, INPUT, WORKER_ID)
              .from(TASK_QUEUE)
              .where(TABLE_NAME.eq(tableName))
              .and(STATE.eq("processing"))
              .and(CLAIMED_AT.le(unixTimestamp().minus(val(timeoutSeconds))))
              .fetch();

      for (var r : rows) {
        Task task = mapRecordToTask(r);
        task.setWorkerId(r.get(WORKER_ID));
        tasks.add(task);
      }

      if (!tasks.isEmpty()) {
        logger.warn("Found {} stale tasks for table {}", tasks.size(), tableName);
      }
      return tasks;
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Find completed tasks for a table.
   *
   * @param tableName The target metadata table
   * @return List of completed tasks (up to 100)
   * @throws SQLException if database operation fails
   */
  public List<Task> findCompletedTasks(String tableName) throws SQLException {
    try {
      List<Task> tasks = new ArrayList<>();
      var rows =
          dsl.select(TASK_ID, TABLE_NAME, STATE, RETRY_COUNT, INPUT, OUTPUT)
              .from(TASK_QUEUE)
              .where(TABLE_NAME.eq(tableName))
              .and(STATE.eq("completed"))
              .orderBy(TASK_ID.asc())
              .limit(100)
              .fetch();

      for (var r : rows) {
        Task task = mapRecordToTask(r);
        task.setOutput(r.get(OUTPUT));
        tasks.add(task);
      }
      return tasks;
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Reclaim a stale task by creating a new pending task and marking the old one as timed_out.
   *
   * <p>If the task has exceeded max retries, it is moved to dead_letter instead.
   *
   * @param staleTask The stale task to reclaim
   * @param maxRetries Maximum retry count before dead letter
   * @return The new task ID, or -1 if moved to dead letter
   * @throws SQLException if database operation fails
   */
  public long reclaimTask(Task staleTask, int maxRetries) throws SQLException {
    try {
      return dsl.transactionResult(
          ctx -> {
            DSLContext txDsl = ctx.dsl();
            int newRetryCount = staleTask.getRetryCount() + 1;

            if (newRetryCount > maxRetries) {
              txDsl
                  .update(TASK_QUEUE)
                  .set(STATE, "dead_letter")
                  .set(COMPLETED_AT, unixTimestamp())
                  .where(TASK_ID.eq(staleTask.getTaskId()))
                  .execute();

              logger.warn(
                  "Task {} moved to dead letter after {} retries",
                  staleTask.getTaskId(),
                  newRetryCount);
              return -1L;
            }

            txDsl
                .update(TASK_QUEUE)
                .set(STATE, "timed_out")
                .set(COMPLETED_AT, unixTimestamp())
                .where(TASK_ID.eq(staleTask.getTaskId()))
                .execute();

            txDsl
                .insertInto(TASK_QUEUE)
                .set(TABLE_NAME, staleTask.getTableName())
                .set(INPUT, staleTask.getInput())
                .set(RETRY_COUNT, newRetryCount)
                .execute();

            long newTaskId =
                txDsl.select(DSL.field("LAST_INSERT_ID()", Long.class)).fetchOne().value1();
            logger.debug(
                "Reclaimed task {} as {} (retry {})",
                staleTask.getTaskId(),
                newTaskId,
                newRetryCount);
            return newTaskId;
          });
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Clean up old completed/failed/timed_out tasks.
   *
   * @param tableName The target metadata table
   * @param gracePeriodSeconds Seconds since completion before deletion
   * @return Number of tasks deleted
   * @throws SQLException if database operation fails
   */
  public int cleanup(String tableName, int gracePeriodSeconds) throws SQLException {
    try {
      var delete =
          dsl.deleteFrom(TASK_QUEUE)
              .where(TABLE_NAME.eq(tableName))
              .and(STATE.in("completed", "failed", "timed_out"))
              .and(COMPLETED_AT.le(unixTimestamp().minus(val(gracePeriodSeconds))));
      int deleted = dsl.execute(delete.getSQL() + " LIMIT 1000", delete.getBindValues().toArray());

      if (deleted > 0) {
        logger.info("Cleaned up {} old tasks for table {}", deleted, tableName);
      }
      return deleted;
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Delete a single task by ID.
   *
   * @param taskId The task ID to delete
   * @return Number of rows deleted (0 or 1)
   * @throws SQLException if database operation fails
   */
  public int deleteTask(long taskId) throws SQLException {
    try {
      return dsl.deleteFrom(TASK_QUEUE).where(TASK_ID.eq(taskId)).execute();
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Reset claimed-but-unexecuted tasks back to pending.
   *
   * <p>Called during graceful shutdown to return tasks that were claimed into the prefetch queue but
   * never handed to a worker. Does not increment {@code retry_count}.
   *
   * @param taskIds Task IDs to reset
   * @return Number of rows updated
   * @throws SQLException if database operation fails
   */
  public int resetTasksToPending(List<Long> taskIds) throws SQLException {
    if (taskIds == null || taskIds.isEmpty()) {
      return 0;
    }
    try {
      int updated =
          dsl.update(TASK_QUEUE)
              .set(STATE, "pending")
              .set(WORKER_ID, (String) null)
              .set(CLAIMED_AT, (Long) null)
              .where(TASK_ID.in(taskIds))
              .and(STATE.eq("processing"))
              .execute();
      logger.info("Reset {} tasks to pending during shutdown", updated);
      return updated;
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Delete all tasks except dead_letter for coordinator restart.
   *
   * @param tableName The target metadata table
   * @return Number of tasks deleted
   * @throws SQLException if database operation fails
   */
  public int deleteAllExceptDeadLetter(String tableName) throws SQLException {
    try {
      int deleted =
          dsl.deleteFrom(TASK_QUEUE)
              .where(TABLE_NAME.eq(tableName))
              .and(STATE.ne("dead_letter"))
              .execute();

      if (deleted > 0) {
        logger.info(
            "Deleted {} non-dead-letter tasks for table {} during recovery", deleted, tableName);
      }
      return deleted;
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Get current database timestamp.
   *
   * @return Current database UNIX timestamp
   * @throws SQLException if database operation fails
   */
  public long getDatabaseTimestamp() throws SQLException {
    try {
      return dsl.select(unixTimestamp()).fetchOne().value1();
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Get counts of tasks by state for monitoring.
   *
   * @param tableName The target metadata table
   * @return TaskCounts with counts per state
   * @throws SQLException if database operation fails
   */
  public TaskCounts getTaskCounts(String tableName) throws SQLException {
    try {
      TaskCounts counts = new TaskCounts();

      var rows =
          dsl.select(STATE, count(asterisk()).as("cnt"))
              .from(TASK_QUEUE)
              .where(TABLE_NAME.eq(tableName))
              .groupBy(STATE)
              .fetch();

      for (var r : rows) {
        String state = r.get(STATE);
        int cnt = r.get("cnt", int.class);
        switch (state) {
          case "pending" -> counts.pending = cnt;
          case "processing" -> counts.processing = cnt;
          case "completed" -> counts.completed = cnt;
          case "failed" -> counts.failed = cnt;
          case "timed_out" -> counts.timedOut = cnt;
          case "dead_letter" -> counts.deadLetter = cnt;
        }
      }
      return counts;
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  private Task mapRecordToTask(Record r) {
    Task task = new Task();
    task.setTaskId(r.get(TASK_ID.getName(), Long.class));
    task.setTableName(r.get(TABLE_NAME.getName(), String.class));
    task.setState(r.get(STATE.getName(), String.class));
    task.setRetryCount(r.get(RETRY_COUNT.getName(), Integer.class));
    task.setInput(r.get(INPUT.getName(), byte[].class));
    return task;
  }

  /** Unwrap jOOQ DataAccessException to SQLException for API compatibility. */
  private static SQLException translated(DataAccessException e) {
    if (e.getCause() instanceof SQLException sqlEx) {
      return sqlEx;
    }
    return new SQLException(e.getMessage(), e);
  }

  /** Represents a task from the _task_queue table. */
  public static class Task {
    private long taskId;
    private String tableName;
    private String state;
    private String workerId;
    private int retryCount;
    private byte[] input;
    private byte[] output;

    // Transient fields - parsed lazily
    private TaskPayload parsedInput;
    private TaskResult parsedOutput;

    public long getTaskId() {
      return taskId;
    }

    public void setTaskId(long taskId) {
      this.taskId = taskId;
    }

    public String getTableName() {
      return tableName;
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public String getState() {
      return state;
    }

    public void setState(String state) {
      this.state = state;
    }

    public String getWorkerId() {
      return workerId;
    }

    public void setWorkerId(String workerId) {
      this.workerId = workerId;
    }

    public int getRetryCount() {
      return retryCount;
    }

    public void setRetryCount(int retryCount) {
      this.retryCount = retryCount;
    }

    public byte[] getInput() {
      return input;
    }

    public void setInput(byte[] input) {
      this.input = input;
      this.parsedInput = null;
    }

    public byte[] getOutput() {
      return output;
    }

    public void setOutput(byte[] output) {
      this.output = output;
      this.parsedOutput = null;
    }

    public TaskPayload getParsedInput() {
      if (parsedInput == null && input != null) {
        parsedInput = TaskPayload.deserialize(input);
      }
      return parsedInput;
    }

    public TaskResult getParsedOutput() {
      if (parsedOutput == null && output != null) {
        parsedOutput = TaskResult.deserialize(output);
      }
      return parsedOutput;
    }

    public String getArchivePath() {
      TaskPayload p = getParsedInput();
      return p != null ? p.getArchivePath() : null;
    }

    public List<String> getIrFilePaths() {
      TaskPayload p = getParsedInput();
      return p != null ? p.getIrFilePaths() : List.of();
    }

    @Override
    public String toString() {
      return String.format(
          "Task{id=%d, table='%s', state='%s', worker='%s', retry=%d}",
          taskId, tableName, state, workerId, retryCount);
    }
  }

  /** Task counts by state for monitoring. */
  public static class TaskCounts {
    public int pending;
    public int processing;
    public int completed;
    public int failed;
    public int timedOut;
    public int deadLetter;

    public int getTotal() {
      return pending + processing + completed + failed + timedOut + deadLetter;
    }

    public int getIncomplete() {
      return pending + processing;
    }

    @Override
    public String toString() {
      return String.format(
          "TaskCounts{pending=%d, processing=%d, completed=%d, failed=%d, timedOut=%d, deadLetter=%d}",
          pending, processing, completed, failed, timedOut, deadLetter);
    }
  }
}
