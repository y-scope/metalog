package com.yscope.clp.service.tools.benchmark;

import com.yscope.clp.service.metastore.model.TaskPayload;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

/**
 * Scalability benchmark for the database-backed task queue.
 *
 * <p>Sweeps worker counts × batch sizes to find the throughput ceiling of the task queue under
 * concurrent load. Useful for capacity planning.
 *
 * <p>Claim strategy: {@code SELECT ... FOR UPDATE} + {@code UPDATE} in a READ COMMITTED
 * transaction, matching production {@code TaskQueue.claimTasksWithConnection}.
 *
 * <p>Run:
 *
 * <pre>
 *   ./benchmarks/scalability/run.py
 * </pre>
 *
 * <p>Connection environment variables (all optional):
 *
 * <ul>
 *   <li>{@code DB_HOST} — MariaDB host (default: localhost)
 *   <li>{@code DB_PORT} — MariaDB port (default: 3306)
 *   <li>{@code DB_NAME} — database name (default: clp_metastore)
 *   <li>{@code DB_USER} — username (default: root)
 *   <li>{@code DB_PASSWORD} — password (default: password)
 * </ul>
 */
public class TaskQueueScalabilityBenchmark {

  private static final String TABLE_NAME = "benchmark";
  private static final String TABLE_PREFIX = "table_";

  private HikariDataSource dataSource;

  public static void main(String[] args) {
    try {
      runBenchmark(args);
    } catch (Exception e) {
      System.err.println("Benchmark error: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
    System.exit(0);
  }

  private static void runBenchmark(String[] args) throws Exception {
    String workersStr = System.getProperty("workers", "1,25,50,75,100,125");
    String batchSizesStr = System.getProperty("batchSizes", "1,5,10");
    int tasksPerWorker = Integer.parseInt(System.getProperty("tasksPerWorker", "50"));
    int maxConnections = Integer.parseInt(System.getProperty("maxConnections", "2000"));
    int numTables = Integer.parseInt(System.getProperty("tables", "10"));
    int jitterMaxMs = Integer.parseInt(System.getProperty("jitterMaxMs", "50"));

    int[] workerCounts = parseIntArray(workersStr);
    int[] batchSizes = parseIntArray(batchSizesStr);

    TaskQueueScalabilityBenchmark benchmark = new TaskQueueScalabilityBenchmark();
    benchmark.run(workerCounts, batchSizes, tasksPerWorker, maxConnections, numTables, jitterMaxMs);
  }

  private static int[] parseIntArray(String csv) {
    return Arrays.stream(csv.split(",")).map(String::trim).mapToInt(Integer::parseInt).toArray();
  }

  public void run(
      int[] workerCounts,
      int[] batchSizes,
      int tasksPerWorker,
      int maxConnections,
      int numTables,
      int jitterMaxMs)
      throws Exception {
    try {
      setup(maxConnections);
      runBenchmarks(workerCounts, batchSizes, tasksPerWorker, numTables, jitterMaxMs);
    } finally {
      teardown();
    }
  }

  private void setup(int maxConnections) {
    String host = env("DB_HOST", "localhost");
    String port = env("DB_PORT", "3306");
    String db = env("DB_NAME", "clp_metastore");
    String user = env("DB_USER", "root");
    String password = env("DB_PASSWORD", "password");

    String jdbcUrl = "jdbc:mariadb://" + host + ":" + port + "/" + db;

    // Try to raise max_connections on the server (docker-compose may already set it via command)
    try {
      HikariConfig adminCfg = new HikariConfig();
      adminCfg.setJdbcUrl(jdbcUrl);
      adminCfg.setUsername(user);
      adminCfg.setPassword(password);
      adminCfg.setMaximumPoolSize(1);
      try (HikariDataSource admin = new HikariDataSource(adminCfg);
          Connection conn = admin.getConnection();
          Statement stmt = conn.createStatement()) {
        stmt.execute("SET GLOBAL max_connections = " + maxConnections);
        System.out.println("MariaDB max_connections set to " + maxConnections);
      }
    } catch (Exception e) {
      System.out.println("Warning: Could not set max_connections (" + e.getMessage() + ")");
    }

    int poolSize = Math.min(maxConnections - 50, 1500);
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(jdbcUrl);
    config.setUsername(user);
    config.setPassword(password);
    config.setMaximumPoolSize(poolSize);
    config.setConnectionTimeout(120000);
    dataSource = new HikariDataSource(config);
    System.out.println("Connection pool size: " + poolSize);
  }

  private void teardown() {
    if (dataSource != null) {
      dataSource.close();
    }
  }

  private void runBenchmarks(
      int[] workerCounts, int[] batchSizes, int tasksPerWorker, int numTables, int jitterMaxMs)
      throws Exception {
    System.out.println();
    System.out.println("╔════════════════════════════════════════════════════════════════════╗");
    System.out.println("║           TASK QUEUE SCALABILITY BENCHMARK                         ║");
    System.out.println("╚════════════════════════════════════════════════════════════════════╝");
    System.out.println();
    System.out.println("Tasks per worker: " + tasksPerWorker);
    System.out.println("Deadlock jitter: 1–" + jitterMaxMs + " ms");
    System.out.println("Worker counts: " + Arrays.toString(workerCounts));
    System.out.println("Batch sizes: " + Arrays.toString(batchSizes));
    System.out.println(
        "Tables: "
            + numTables
            + (numTables > 1 ? " (workers distributed across tables)" : " (single table)"));
    System.out.println();

    double[][] claimResults = new double[workerCounts.length][batchSizes.length];
    double[][] completeResults = new double[workerCounts.length][batchSizes.length];

    int total = workerCounts.length * batchSizes.length;
    int run = 0;
    for (int w = 0; w < workerCounts.length; w++) {
      for (int b = 0; b < batchSizes.length; b++) {
        int totalTasks = workerCounts[w] * tasksPerWorker;
        System.out.printf(
            "  [%d/%d] workers=%-4d batch=%-4d tasks=%-6d ... ",
            ++run, total, workerCounts[w], batchSizes[b], totalTasks);
        System.out.flush();
        try {
          double[] t =
              runSingleBenchmark(workerCounts[w], batchSizes[b], totalTasks, numTables, jitterMaxMs);
          claimResults[w][b] = t[0];
          completeResults[w][b] = t[1];
          System.out.printf("claim=%.0f/s  complete=%.0f/s%n", t[0], t[1]);
        } catch (Exception e) {
          claimResults[w][b] = -1;
          completeResults[w][b] = -1;
          System.out.println("FAILED (" + e.getMessage() + ")");
          if (e.getMessage() != null && e.getMessage().contains("Too many connections")) {
            System.out.println("  Stopping (max_connections limit)");
            break;
          }
        }
      }
    }
    System.out.println();

    System.out.println("═══════════════════════════════════════════════════════════════════════");
    System.out.println("CLAIM THROUGHPUT (tasks/sec):");
    System.out.println("═══════════════════════════════════════════════════════════════════════");
    System.out.println();
    printResultsTable(workerCounts, batchSizes, claimResults);

    System.out.println("═══════════════════════════════════════════════════════════════════════");
    System.out.println("COMPLETE THROUGHPUT (tasks/sec):");
    System.out.println("═══════════════════════════════════════════════════════════════════════");
    System.out.println();
    printResultsTable(workerCounts, batchSizes, completeResults);

    printCsvFormat("claim", workerCounts, batchSizes, claimResults);
    printCsvFormat("complete", workerCounts, batchSizes, completeResults);

    printAnalysis(workerCounts, batchSizes, claimResults, completeResults);
  }

  /** Runs claim + complete phases, returns {claimThroughput, completeThroughput}. */
  private double[] runSingleBenchmark(
      int numWorkers, int batchSize, int totalTasks, int numTables, int jitterMaxMs)
      throws Exception {
    resetQueue(totalTasks, numTables);

    BatchClaimQueue[] queues = buildQueues(numTables, batchSize, jitterMaxMs);

    // Phase 1: claim
    AtomicInteger totalClaimed = new AtomicInteger(0);
    ConcurrentLinkedQueue<Long> claimedTaskIds = new ConcurrentLinkedQueue<>();
    long claimDuration =
        runWorkers(
            numWorkers,
            workerId -> {
              int w = Integer.parseInt(workerId.split("-")[1]);
              int claimed = queues[w % queues.length].claimTasks(workerId);
              totalClaimed.addAndGet(claimed);
              return claimed == 0;
            });
    double claimThroughput = (totalClaimed.get() * 1_000_000_000.0) / claimDuration;

    // Phase 2: complete — read back all claimed task IDs first
    List<Long> allClaimed = new ArrayList<>(claimedTaskIds);
    // Re-query claimed IDs from DB since claimTasks (UPDATE LIMIT) doesn't return them
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt =
            conn.prepareStatement(
                "SELECT task_id FROM _task_queue WHERE state='processing' ORDER BY task_id")) {
      try (java.sql.ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) allClaimed.add(rs.getLong(1));
      }
    }
    final List<Long> finalClaimed = allClaimed;
    int tasksPerThread = (finalClaimed.size() + numWorkers - 1) / numWorkers;
    long completeDuration =
        runWorkers(
            numWorkers,
            workerId -> {
              int w = Integer.parseInt(workerId.split("-")[1]);
              int start = w * tasksPerThread;
              int end = Math.min(start + tasksPerThread, finalClaimed.size());
              if (start >= finalClaimed.size()) return true;
              for (int i = start; i < end; i += batchSize) {
                queues[w % queues.length].completeTasks(
                    finalClaimed.subList(i, Math.min(i + batchSize, end)));
              }
              return true;
            });
    double completeThroughput =
        finalClaimed.isEmpty() ? 0 : (finalClaimed.size() * 1_000_000_000.0) / completeDuration;

    return new double[] {claimThroughput, completeThroughput};
  }

  private void resetQueue(int totalTasks, int numTables) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM _task_queue");
    }
    String[] tableNames = tableNames(numTables);
    int tasksPerTable = totalTasks / numTables;
    for (int t = 0; t < numTables; t++) {
      int count = (t == numTables - 1) ? (totalTasks - tasksPerTable * t) : tasksPerTable;
      createTasksBatch(tableNames[t], count);
    }
  }

  private BatchClaimQueue[] buildQueues(int numTables, int batchSize, int jitterMaxMs) {
    String[] names = tableNames(numTables);
    BatchClaimQueue[] queues = new BatchClaimQueue[numTables];
    for (int t = 0; t < numTables; t++) {
      queues[t] = new BatchClaimQueue(dataSource, names[t], batchSize, jitterMaxMs);
    }
    return queues;
  }

  private String[] tableNames(int numTables) {
    String[] names = new String[numTables];
    for (int t = 0; t < numTables; t++) {
      names[t] = numTables == 1 ? TABLE_NAME : TABLE_PREFIX + t;
    }
    return names;
  }

  /** Runs numWorkers threads; each calls work(workerId) in a loop until work returns true. */
  private long runWorkers(int numWorkers, WorkerTask task) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numWorkers);

    for (int w = 0; w < numWorkers; w++) {
      final String workerId = "worker-" + w;
      executor.submit(
          () -> {
            try {
              startLatch.await();
              boolean done = false;
              while (!done) {
                done = task.run(workerId);
              }
            } catch (Exception e) {
              // worker finished or error
            } finally {
              doneLatch.countDown();
            }
          });
    }

    long start = System.nanoTime();
    startLatch.countDown();
    doneLatch.await(300, TimeUnit.SECONDS);
    long duration = System.nanoTime() - start;
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
    return duration;
  }

  @FunctionalInterface
  interface WorkerTask {
    /** @return true when this worker has no more work to do */
    boolean run(String workerId) throws Exception;
  }

  private void printResultsTable(int[] workerCounts, int[] batchSizes, double[][] results) {
    StringBuilder header = new StringBuilder(String.format("%-12s", "Workers"));
    for (int batchSize : batchSizes) {
      header.append(String.format("%15s", "Batch=" + batchSize));
    }
    System.out.println(header);
    System.out.println("-".repeat(12 + 15 * batchSizes.length));
    for (int w = 0; w < workerCounts.length; w++) {
      StringBuilder row = new StringBuilder(String.format("%-12d", workerCounts[w]));
      for (int b = 0; b < batchSizes.length; b++) {
        row.append(
            results[w][b] < 0
                ? String.format("%15s", "FAIL")
                : String.format("%15.0f", results[w][b]));
      }
      System.out.println(row);
    }
    System.out.println();
  }

  private void createTasksBatch(String tableName, int count) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      String sql = "INSERT INTO _task_queue (table_name, input) VALUES (?, ?)";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        byte[] payload =
            new TaskPayload(
                    "/a/t", List.of("/f"), tableName, "minio", "clp-ir", "minio", "clp-archives")
                .serialize();
        for (int i = 0; i < count; i++) {
          stmt.setString(1, tableName);
          stmt.setBytes(2, payload);
          stmt.addBatch();
          if ((i + 1) % 500 == 0) {
            stmt.executeBatch();
            conn.commit();
          }
        }
        stmt.executeBatch();
        conn.commit();
      }
    }
  }

  private void printCsvFormat(
      String label, int[] workerCounts, int[] batchSizes, double[][] results) {
    System.out.println("CSV FORMAT - " + label.toUpperCase() + " (copy/paste to spreadsheet):");
    System.out.println("----------------------------------------");
    StringBuilder csvHeader = new StringBuilder("workers");
    for (int batchSize : batchSizes) {
      csvHeader.append(",").append(label).append("_batch_").append(batchSize);
    }
    System.out.println(csvHeader);
    for (int w = 0; w < workerCounts.length; w++) {
      StringBuilder csvRow = new StringBuilder().append(workerCounts[w]);
      for (int b = 0; b < batchSizes.length; b++) {
        csvRow.append(",").append(String.format("%.0f", results[w][b]));
      }
      System.out.println(csvRow);
    }
    System.out.println();
  }

  private void printAnalysis(
      int[] workerCounts,
      int[] batchSizes,
      double[][] claimResults,
      double[][] completeResults) {
    System.out.println("═══════════════════════════════════════════════════════════════════════");
    System.out.println("ANALYSIS:");
    System.out.println("═══════════════════════════════════════════════════════════════════════");
    System.out.println();

    double[] maxClaim = findMax(workerCounts, batchSizes, claimResults);
    double[] maxComplete = findMax(workerCounts, batchSizes, completeResults);

    System.out.println("CLAIM (SELECT ... FOR UPDATE + UPDATE, READ COMMITTED):");
    System.out.printf(
        "  Peak: %.0f tasks/sec (%d workers, batch=%d)%n",
        maxClaim[0], (int) maxClaim[1], (int) maxClaim[2]);

    System.out.println("\nCOMPLETE (UPDATE state to 'completed'):");
    System.out.printf(
        "  Peak: %.0f tasks/sec (%d workers, batch=%d)%n",
        maxComplete[0], (int) maxComplete[1], (int) maxComplete[2]);

    double effective = Math.min(maxClaim[0], maxComplete[0]);
    System.out.println("\nEFFECTIVE THROUGHPUT (limited by slower phase):");
    System.out.printf("  Per second: %,.0f tasks%n", effective);
    System.out.printf("  Per minute: %,.0f tasks%n", effective * 60);
    System.out.printf("  Per hour:   %,.0f tasks%n", effective * 3600);
    System.out.printf("  Per day:    %,.0f tasks%n", effective * 86400);

    System.out.println("\nNOTE: This benchmark uses Docker Compose (external MariaDB).");
    System.out.println("      Results reflect an isolated, dedicated database instance.");
    System.out.println();
  }

  private double[] findMax(int[] workerCounts, int[] batchSizes, double[][] results) {
    double maxThroughput = 0;
    int bestWorkers = 0;
    int bestBatch = 0;
    for (int w = 0; w < workerCounts.length; w++) {
      for (int b = 0; b < batchSizes.length; b++) {
        if (results[w][b] > maxThroughput) {
          maxThroughput = results[w][b];
          bestWorkers = workerCounts[w];
          bestBatch = batchSizes[b];
        }
      }
    }
    return new double[] {maxThroughput, bestWorkers, bestBatch};
  }

  private static String env(String key, String defaultValue) {
    String v = System.getenv(key);
    return (v != null && !v.isEmpty()) ? v : defaultValue;
  }

  /** Task queue claim operations for benchmarking. */
  static class BatchClaimQueue {
    private final DataSource dataSource;
    private final String tableName;
    private final int batchSize;
    private final int jitterMaxMs;

    BatchClaimQueue(DataSource dataSource, String tableName, int batchSize, int jitterMaxMs) {
      this.dataSource = dataSource;
      this.tableName = tableName;
      this.batchSize = batchSize;
      this.jitterMaxMs = jitterMaxMs;
    }

    /**
     * Claim up to batchSize tasks using {@code SELECT ... FOR UPDATE} + {@code UPDATE} in a READ
     * COMMITTED transaction, matching production {@code TaskQueue.claimTasksWithConnection}.
     *
     * <p>Retries automatically on deadlock (InnoDB error 1213). Deadlocks are transient — they
     * occur when concurrent workers modify adjacent secondary-index entries; a retry always
     * succeeds.
     *
     * @return number of tasks claimed (0 = queue empty for this table)
     */
    int claimTasks(String workerId) throws SQLException {
      while (true) {
        try {
          return doClaimTasks(workerId);
        } catch (SQLException e) {
          if (e.getErrorCode() != 1213) {
            throw e;
          }
          // Deadlock — always retryable. Sleep briefly to desynchronise
          // concurrent claimers before retrying.
          try {
            Thread.sleep(1 + (long) (Math.random() * jitterMaxMs));
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw e;
          }
        }
      }
    }

    private int doClaimTasks(String workerId) throws SQLException {
      try (Connection conn = dataSource.getConnection()) {
        int originalIsolation = conn.getTransactionIsolation();
        boolean originalAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        try {
          String selectSql =
              "SELECT task_id FROM _task_queue"
                  + " WHERE table_name = ? AND state = 'pending'"
                  + " ORDER BY task_id LIMIT ? FOR UPDATE";
          List<Long> taskIds = new ArrayList<>();
          try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
            stmt.setString(1, tableName);
            stmt.setInt(2, batchSize);
            try (java.sql.ResultSet rs = stmt.executeQuery()) {
              while (rs.next()) taskIds.add(rs.getLong(1));
            }
          }
          if (taskIds.isEmpty()) {
            conn.commit();
            return 0;
          }
          String placeholders = String.join(",", Collections.nCopies(taskIds.size(), "?"));
          String updateSql =
              "UPDATE _task_queue SET state='processing', worker_id=?, claimed_at=UNIX_TIMESTAMP()"
                  + " WHERE task_id IN ("
                  + placeholders
                  + ")";
          try (PreparedStatement stmt = conn.prepareStatement(updateSql)) {
            stmt.setString(1, workerId);
            for (int i = 0; i < taskIds.size(); i++) stmt.setLong(i + 2, taskIds.get(i));
            stmt.executeUpdate();
          }
          conn.commit();
          return taskIds.size();
        } catch (Exception e) {
          try {
            conn.rollback();
          } catch (SQLException ignored) {
          }
          throw e;
        } finally {
          conn.setTransactionIsolation(originalIsolation);
          conn.setAutoCommit(originalAutoCommit);
        }
      }
    }

    void completeTasks(List<Long> taskIds) throws SQLException {
      if (taskIds.isEmpty()) return;
      String placeholders = String.join(",", Collections.nCopies(taskIds.size(), "?"));
      String sql =
          String.format(
              "UPDATE _task_queue SET state = 'completed', completed_at = UNIX_TIMESTAMP()"
                  + " WHERE task_id IN (%s) AND state = 'processing'",
              placeholders);
      try (Connection conn = dataSource.getConnection();
          PreparedStatement stmt = conn.prepareStatement(sql)) {
        int idx = 1;
        for (long id : taskIds) stmt.setLong(idx++, id);
        stmt.executeUpdate();
      }
    }
  }
}
