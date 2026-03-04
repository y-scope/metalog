package com.yscope.metalog.tools.benchmark;

import com.yscope.metalog.common.config.db.DSLContextFactory;
import com.yscope.metalog.metastore.TaskQueue;
import com.yscope.metalog.metastore.model.TaskPayload;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;

/**
 * Performance benchmark for TaskQueue.
 *
 * <p>Measures:
 *
 * <ul>
 *   <li>Task creation throughput (ops/sec)
 *   <li>Task claim throughput
 *   <li>Task completion throughput
 *   <li>Concurrent worker simulation
 *   <li>Full task lifecycle (create → claim → complete)
 *   <li>Stale task detection and reclaim
 *   <li>Cleanup throughput
 *   <li>Durability tuning impact
 *   <li>Batch vs single-row insert comparison
 * </ul>
 *
 * <p>Run:
 *
 * <pre>
 *   ./benchmarks/task-queue/run.sh
 * </pre>
 *
 * <p>Connection environment variables (all optional):
 *
 * <ul>
 *   <li>{@code DB_HOST} — MariaDB host (default: localhost)
 *   <li>{@code DB_PORT} — MariaDB port (default: 3306)
 *   <li>{@code DB_NAME} — database name (default: metalog_metastore)
 *   <li>{@code DB_USER} — username (default: root)
 *   <li>{@code DB_PASSWORD} — password (default: password)
 * </ul>
 */
public class TaskQueueBenchmark {

  private static final String TABLE_NAME = "clp_table";
  private static final int BENCHMARK_ITERATIONS = 2;

  private static DataSource dataSource;
  private static TaskQueue taskQueue;

  public static void main(String[] args) throws Exception {
    setupAll();
    try {
      runBenchmarks();
    } finally {
      tearDownAll();
    }
  }

  // ========================================================================
  // Setup / teardown
  // ========================================================================

  private static void setupAll() {
    String host = env("DB_HOST", "localhost");
    String port = env("DB_PORT", "3306");
    String db = env("DB_NAME", "metalog_metastore");
    String user = env("DB_USER", "root");
    String password = env("DB_PASSWORD", "password");

    System.out.println("\n" + "=".repeat(60));
    System.out.println("Task Queue Benchmark - Database: MariaDB");
    System.out.println("  Host: " + host + ":" + port + "/" + db);
    System.out.println("=".repeat(60));
    System.out.println("\nDurability Settings (default):");
    System.out.println("  innodb_flush_log_at_trx_commit=1 means fsync on EVERY commit");
    System.out.println("  This is the main throughput bottleneck\n");

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:mariadb://" + host + ":" + port + "/" + db);
    config.setUsername(user);
    config.setPassword(password);
    config.setMaximumPoolSize(20);
    config.setMinimumIdle(5);
    dataSource = new HikariDataSource(config);
  }

  private static void tearDownAll() {
    if (dataSource instanceof HikariDataSource) {
      ((HikariDataSource) dataSource).close();
    }
  }

  private static void setUp() throws SQLException {
    executeSql("DELETE FROM _task_queue");
    taskQueue = new TaskQueue(DSLContextFactory.create(dataSource));
  }

  // ========================================================================
  // Benchmark runner
  // ========================================================================

  private static void runBenchmarks() throws Exception {
    setUp();
    benchmarkTaskCreation();
    setUp();
    benchmarkSingleWorkerClaim();
    setUp();
    benchmarkFullLifecycle();
    setUp();
    benchmarkConcurrentWorkers(4, 200);
    setUp();
    benchmarkConcurrentWorkers(8, 400);
    setUp();
    benchmarkConcurrentWorkers(16, 500);
    setUp();
    benchmarkSkipLockedContention();
    setUp();
    benchmarkStaleTaskDetection();
    setUp();
    benchmarkCleanup();
    setUp();
    benchmarkDurabilityTuning();
    setUp();
    benchmarkBatchOperations();
  }

  // ========================================================================
  // Task creation throughput
  // ========================================================================

  private static void benchmarkTaskCreation() throws SQLException {
    int taskCount = 200;
    List<Double> throughputs = new ArrayList<>();

    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
      long startTime = System.nanoTime();
      createTasks(taskCount, "bench-" + i);
      long duration = System.nanoTime() - startTime;
      throughputs.add((taskCount * 1_000_000_000.0) / duration);
      executeSql("DELETE FROM _task_queue");
    }

    double avg = avg(throughputs);
    System.out.printf("%n=== Task Creation Benchmark: %d tasks ===%n", taskCount);
    System.out.printf("Average: %,.0f tasks/sec%n", avg);
    System.out.printf("Min: %,.0f, Max: %,.0f tasks/sec%n", min(throughputs), max(throughputs));
    System.out.printf("Latency: %.2f ms per task%n", 1000.0 / avg);
    assertAtLeast(avg, 10, "Task creation should achieve at least 10 tasks/sec");
  }

  // ========================================================================
  // Single-worker claim throughput
  // ========================================================================

  private static void benchmarkSingleWorkerClaim() throws SQLException {
    int taskCount = 200;
    List<Double> throughputs = new ArrayList<>();

    System.out.printf("%n=== Single Worker Claim Benchmark ===%n");
    System.out.printf("Tasks: %d%n", taskCount);

    for (int iter = 0; iter < BENCHMARK_ITERATIONS; iter++) {
      createTasks(taskCount, "claim-" + iter);
      long startTime = System.nanoTime();
      int claimed = 0;
      while (taskQueue.claimTask(TABLE_NAME, "worker-1").isPresent()) {
        claimed++;
      }
      long duration = System.nanoTime() - startTime;
      throughputs.add((claimed * 1_000_000_000.0) / duration);
      executeSql("DELETE FROM _task_queue");
    }

    double avg = avg(throughputs);
    System.out.printf("Average claim throughput: %,.0f tasks/sec%n", avg);
    System.out.printf("Latency per claim: %.2f ms%n", 1000.0 / avg);
    assertAtLeast(avg, 10, "Single worker claim should achieve at least 10 tasks/sec");
  }

  // ========================================================================
  // Full lifecycle (create → claim → complete)
  // ========================================================================

  private static void benchmarkFullLifecycle() throws SQLException {
    int taskCount = 100;
    List<Double> throughputs = new ArrayList<>();

    System.out.printf("%n=== Full Task Lifecycle Benchmark ===%n");
    System.out.println("Lifecycle: create -> claim -> complete");
    System.out.printf("Tasks: %d%n", taskCount);

    for (int iter = 0; iter < BENCHMARK_ITERATIONS; iter++) {
      long startTime = System.nanoTime();
      createTasks(taskCount, "lifecycle-" + iter);
      int processed = 0;
      Optional<TaskQueue.Task> task;
      while ((task = taskQueue.claimTask(TABLE_NAME, "worker-1")).isPresent()) {
        taskQueue.completeTask(task.get().getTaskId());
        processed++;
      }
      long duration = System.nanoTime() - startTime;
      throughputs.add((processed * 1_000_000_000.0) / duration);
      executeSql("DELETE FROM _task_queue");
    }

    double avg = avg(throughputs);
    System.out.printf("%nFull lifecycle throughput: %,.0f tasks/sec%n", avg);
    System.out.printf("(3 DB operations per task: INSERT + UPDATE + UPDATE)%n");
    System.out.printf("Effective DB ops: %,.0f ops/sec%n", avg * 3);
    assertAtLeast(avg, 5, "Full lifecycle should achieve at least 5 tasks/sec");
  }

  // ========================================================================
  // Concurrent workers
  // ========================================================================

  private static void benchmarkConcurrentWorkers(int numWorkers, int taskCount) throws Exception {
    System.out.printf("%n=== Concurrent Workers Benchmark ===%n");
    System.out.printf("Workers: %d, Tasks: %d%n", numWorkers, taskCount);

    createTasks(taskCount, "concurrent");

    ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numWorkers);
    AtomicInteger totalProcessed = new AtomicInteger(0);
    AtomicLong totalWorkerNanos = new AtomicLong(0);

    for (int w = 0; w < numWorkers; w++) {
      final String workerId = "worker-" + w;
      executor.submit(
          () -> {
            try {
              startLatch.await();
              int processed = 0;
              long workerStart = System.nanoTime();
              Optional<TaskQueue.Task> task;
              while ((task = taskQueue.claimTask(TABLE_NAME, workerId)).isPresent()) {
                taskQueue.completeTask(task.get().getTaskId());
                processed++;
              }
              totalWorkerNanos.addAndGet(System.nanoTime() - workerStart);
              totalProcessed.addAndGet(processed);
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    long wallClockStart = System.nanoTime();
    startLatch.countDown();
    boolean completed = doneLatch.await(120, TimeUnit.SECONDS);
    long wallClockNanos = System.nanoTime() - wallClockStart;
    executor.shutdown();

    if (!completed) {
      throw new RuntimeException("Workers did not complete within timeout");
    }

    int processed = totalProcessed.get();
    double wallThroughput = (processed * 1_000_000_000.0) / wallClockNanos;
    double avgWorkerThroughput =
        (processed * 1_000_000_000.0) / (totalWorkerNanos.get() / numWorkers);

    System.out.printf("%nTasks processed: %d%n", processed);
    System.out.printf("Wall-clock throughput: %,.0f tasks/sec%n", wallThroughput);
    System.out.printf("Avg per-worker throughput: %,.0f tasks/sec%n", avgWorkerThroughput);
    System.out.printf("Wall-clock time: %.2f sec%n", wallClockNanos / 1_000_000_000.0);

    TaskQueue.TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    if (counts.completed != taskCount) {
      throw new RuntimeException(
          "Expected all " + taskCount + " tasks completed, got " + counts.completed);
    }

    executeSql("DELETE FROM _task_queue");
    assertAtLeast(wallThroughput, 10, "Concurrent workers should achieve at least 10 tasks/sec");
  }

  // ========================================================================
  // FOR UPDATE concurrent contention
  // ========================================================================

  private static void benchmarkSkipLockedContention() throws Exception {
    int numWorkers = 4;
    int claimsPerWorker = 25;
    int taskCount = numWorkers * claimsPerWorker;

    System.out.printf("%n=== FOR UPDATE Contention Benchmark ===%n");
    System.out.printf("Workers: %d, Claims per worker: %d%n", numWorkers, claimsPerWorker);

    createTasks(taskCount, "contention");

    ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numWorkers);
    AtomicInteger totalClaimed = new AtomicInteger(0);

    for (int w = 0; w < numWorkers; w++) {
      final String workerId = "worker-" + w;
      executor.submit(
          () -> {
            try {
              startLatch.await();
              int claimed = 0;
              int emptyPolls = 0;
              for (int i = 0; i < claimsPerWorker * 2; i++) {
                Optional<TaskQueue.Task> task = taskQueue.claimTask(TABLE_NAME, workerId);
                if (task.isPresent()) {
                  claimed++;
                  taskQueue.completeTask(task.get().getTaskId());
                  if (claimed >= claimsPerWorker) break;
                } else if (++emptyPolls > 10) {
                  break;
                }
              }
              totalClaimed.addAndGet(claimed);
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    long startTime = System.nanoTime();
    startLatch.countDown();
    boolean completed = doneLatch.await(60, TimeUnit.SECONDS);
    long duration = System.nanoTime() - startTime;
    executor.shutdown();

    if (!completed) {
      throw new RuntimeException("Workers did not complete within timeout");
    }

    int claimed = totalClaimed.get();
    double throughput = (claimed * 1_000_000_000.0) / duration;

    System.out.printf("%nTotal tasks claimed: %d%n", claimed);
    System.out.printf("Throughput under contention: %,.0f tasks/sec%n", throughput);
    System.out.printf("Duration: %.2f sec%n", duration / 1_000_000_000.0);

    TaskQueue.TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    if (counts.completed != claimed) {
      throw new RuntimeException(
          "Completed count " + counts.completed + " != claimed " + claimed + " (duplicates!)");
    }

    executeSql("DELETE FROM _task_queue");
    assertAtLeast(throughput, 10, "Should maintain throughput under contention");
  }

  // ========================================================================
  // Stale task detection
  // ========================================================================

  private static void benchmarkStaleTaskDetection() throws SQLException, InterruptedException {
    int taskCount = 100;

    System.out.printf("%n=== Stale Task Detection Benchmark ===%n");
    System.out.printf("Processing tasks: %d%n", taskCount);

    createTasks(taskCount, "stale");
    for (int i = 0; i < taskCount; i++) {
      taskQueue.claimTask(TABLE_NAME, "crashed-worker-" + (i % 10));
    }
    Thread.sleep(100);

    long startTime = System.nanoTime();
    List<TaskQueue.Task> staleTasks = taskQueue.findStaleTasks(TABLE_NAME, 0);
    long duration = System.nanoTime() - startTime;
    System.out.printf(
        "Found %d stale tasks in %.2f ms%n", staleTasks.size(), duration / 1_000_000.0);

    if (staleTasks.size() != taskCount) {
      throw new RuntimeException(
          "Expected " + taskCount + " stale tasks, got " + staleTasks.size());
    }

    startTime = System.nanoTime();
    int reclaimed = 0;
    for (TaskQueue.Task stale : staleTasks) {
      if (taskQueue.reclaimTask(stale, 3) > 0) reclaimed++;
    }
    duration = System.nanoTime() - startTime;
    double reclaimThroughput = (reclaimed * 1_000_000_000.0) / duration;

    System.out.printf("Reclaimed %d tasks at %,.0f tasks/sec%n", reclaimed, reclaimThroughput);
    executeSql("DELETE FROM _task_queue");
    assertAtLeast(reclaimThroughput, 5, "Reclaim should achieve at least 5 tasks/sec");
  }

  // ========================================================================
  // Cleanup throughput
  // ========================================================================

  private static void benchmarkCleanup() throws SQLException {
    int taskCount = 200;

    System.out.printf("%n=== Cleanup Performance Benchmark ===%n");
    System.out.printf("Completed tasks: %d%n", taskCount);

    createTasks(taskCount, "cleanup");
    Optional<TaskQueue.Task> task;
    while ((task = taskQueue.claimTask(TABLE_NAME, "worker-1")).isPresent()) {
      taskQueue.completeTask(task.get().getTaskId());
    }

    TaskQueue.TaskCounts counts = taskQueue.getTaskCounts(TABLE_NAME);
    if (counts.completed != taskCount) {
      throw new RuntimeException("Expected " + taskCount + " completed, got " + counts.completed);
    }

    long startTime = System.nanoTime();
    int deleted = taskQueue.cleanup(TABLE_NAME, 0);
    long duration = System.nanoTime() - startTime;
    double deleteThroughput = (deleted * 1_000_000_000.0) / duration;

    System.out.printf("Deleted %d tasks in %.2f ms%n", deleted, duration / 1_000_000.0);
    System.out.printf("Cleanup throughput: %,.0f tasks/sec%n", deleteThroughput);

    if (deleted != taskCount) {
      throw new RuntimeException("Expected to delete " + taskCount + ", deleted " + deleted);
    }
    assertAtLeast(deleteThroughput, 100, "Cleanup should achieve at least 100 tasks/sec");
  }

  // ========================================================================
  // Durability tuning impact
  // ========================================================================

  private static void benchmarkDurabilityTuning() throws Exception {
    int numWorkers = 8;
    int taskCount = 500;

    System.out.println("\n=== Durability Tuning Impact ===");
    System.out.println("Testing throughput with relaxed durability settings\n");

    double defaultThroughput = runConcurrentTest(numWorkers, taskCount, "default");
    System.out.printf("Default (fsync every commit):    %,.0f tasks/sec%n", defaultThroughput);

    executeSql("SET GLOBAL innodb_flush_log_at_trx_commit = 2");
    executeSql("SET GLOBAL sync_binlog = 0");

    double relaxedThroughput = runConcurrentTest(numWorkers, taskCount, "relaxed");
    System.out.printf("Relaxed (fsync once per second): %,.0f tasks/sec%n", relaxedThroughput);
    System.out.printf(
        "%nImprovement: +%.0f%%%n", (relaxedThroughput / defaultThroughput - 1) * 100);
    System.out.println("\nNote: Relaxed settings trade durability for speed.");
    System.out.println(
        "      With relaxed settings, up to 1 second of data could be lost on crash.");

    executeSql("SET GLOBAL innodb_flush_log_at_trx_commit = 1");
    executeSql("SET GLOBAL sync_binlog = 1");
  }

  private static double runConcurrentTest(int numWorkers, int taskCount, String prefix)
      throws Exception {
    executeSql("DELETE FROM _task_queue");
    taskQueue = new TaskQueue(DSLContextFactory.create(dataSource));
    createTasks(taskCount, prefix);

    ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numWorkers);
    AtomicInteger totalProcessed = new AtomicInteger(0);

    for (int w = 0; w < numWorkers; w++) {
      final String workerId = "worker-" + w;
      executor.submit(
          () -> {
            try {
              startLatch.await();
              Optional<TaskQueue.Task> task;
              while ((task = taskQueue.claimTask(TABLE_NAME, workerId)).isPresent()) {
                taskQueue.completeTask(task.get().getTaskId());
                totalProcessed.incrementAndGet();
              }
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    long start = System.nanoTime();
    startLatch.countDown();
    doneLatch.await(120, TimeUnit.SECONDS);
    long duration = System.nanoTime() - start;
    executor.shutdown();
    executeSql("DELETE FROM _task_queue");
    return (totalProcessed.get() * 1_000_000_000.0) / duration;
  }

  // ========================================================================
  // Batch vs single-row insert comparison
  // ========================================================================

  private static void benchmarkBatchOperations() throws Exception {
    int taskCount = 1000;

    System.out.println("\n=== Batch vs Single-Row Operations ===");
    System.out.println("Comparing throughput of different operation patterns\n");

    executeSql("DELETE FROM _task_queue");
    long start = System.nanoTime();
    createTasks(taskCount, "single");
    double singleTps = (taskCount * 1_000_000_000.0) / (System.nanoTime() - start);

    executeSql("DELETE FROM _task_queue");
    start = System.nanoTime();
    createTasksBatch(taskCount, "batch", 100);
    double batchTps = (taskCount * 1_000_000_000.0) / (System.nanoTime() - start);

    System.out.printf("INSERT (single-row):  %,.0f tasks/sec%n", singleTps);
    System.out.printf("INSERT (batch 100):   %,.0f tasks/sec%n", batchTps);
    System.out.printf("Batch improvement:    %.1fx%n", batchTps / singleTps);
    System.out.println("\nNote: Batch operations dramatically improve throughput.");

    executeSql("DELETE FROM _task_queue");
  }

  // ========================================================================
  // Helpers
  // ========================================================================

  private static void createTasks(int count, String prefix) throws SQLException {
    for (int i = 0; i < count; i++) {
      taskQueue.createTask(TABLE_NAME, makePayload(prefix, i));
    }
  }

  private static void createTasksBatch(int count, String prefix, int batchSize)
      throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try (PreparedStatement stmt =
          conn.prepareStatement("INSERT INTO _task_queue (table_name, input) VALUES (?, ?)")) {
        byte[] payload = makePayload(prefix, 0);
        for (int i = 0; i < count; i++) {
          stmt.setString(1, TABLE_NAME);
          stmt.setBytes(2, payload);
          stmt.addBatch();
          if ((i + 1) % batchSize == 0) {
            stmt.executeBatch();
            conn.commit();
          }
        }
        stmt.executeBatch();
        conn.commit();
      }
    }
  }

  private static byte[] makePayload(String prefix, int i) {
    return new TaskPayload(
            "/archive/" + prefix + "/task-" + i + ".clp",
            List.of("/ir/" + prefix + "/file-" + i + ".clp"),
            TABLE_NAME,
            "minio",
            "clp-ir",
            "minio",
            "clp-archives")
        .serialize();
  }

  private static void executeSql(String sql) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
    }
  }

  private static String env(String key, String defaultValue) {
    String v = System.getenv(key);
    return (v != null && !v.isEmpty()) ? v : defaultValue;
  }

  private static double avg(List<Double> list) {
    return list.stream().mapToDouble(d -> d).average().orElse(0);
  }

  private static double min(List<Double> list) {
    return list.stream().mapToDouble(d -> d).min().orElse(0);
  }

  private static double max(List<Double> list) {
    return list.stream().mapToDouble(d -> d).max().orElse(0);
  }

  private static void assertAtLeast(double actual, double min, String message) {
    if (actual < min) {
      System.out.printf("[WARN] %s (actual: %,.0f, required: %,.0f)%n", message, actual, min);
    }
  }
}
