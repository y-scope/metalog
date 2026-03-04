package com.yscope.metalog.tools.benchmark;

import com.yscope.metalog.common.config.ServiceConfig;
import com.yscope.metalog.metastore.SqlIdentifiers;
import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitor that tracks MySQL row count and calculates coordinator throughput.
 *
 * <p>Usage:
 *
 * <pre>
 * java -cp coordinator.jar com.yscope.consolidation.benchmark.BenchmarkMonitor \
 *   --expected 10000 \
 *   --timeout 60
 * </pre>
 *
 * <p>The monitor: 1. Polls MySQL for row count 2. Waits until expected records are inserted or
 * timeout 3. Reports throughput metrics
 */
public class BenchmarkMonitor {
  private static final Logger logger = LoggerFactory.getLogger(BenchmarkMonitor.class);

  private final String jdbcUrl;
  private final String username;
  private final String password;
  private final String tableName;

  public BenchmarkMonitor(ServiceConfig config) {
    this.jdbcUrl = config.getJdbcUrl();
    this.username = config.getDatabaseUser();
    this.password = config.getDatabasePassword();
    this.tableName = SqlIdentifiers.requireValidTableName(config.getDatabaseTable());
  }

  /**
   * Monitor until expected records are inserted or timeout.
   *
   * @param expectedRecords Expected number of records
   * @param timeoutSeconds Maximum wait time
   * @param pollIntervalMs Poll interval in milliseconds
   * @return Benchmark results
   */
  public BenchmarkResult monitor(int expectedRecords, int timeoutSeconds, int pollIntervalMs) {
    logger.info(
        "Starting monitor: expected={}, timeout={}s, poll={}ms",
        expectedRecords,
        timeoutSeconds,
        pollIntervalMs);

    // Get initial count
    long initialCount = getRowCount();
    logger.info("Initial row count: {}", initialCount);

    long startTime = System.currentTimeMillis();
    long timeoutMs = timeoutSeconds * 1000L;
    long lastCount = initialCount;
    long lastReportTime = startTime;

    while (true) {
      try {
        Thread.sleep(pollIntervalMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }

      long currentCount = getRowCount();
      long insertedSoFar = currentCount - initialCount;
      long elapsed = System.currentTimeMillis() - startTime;

      // Progress report every 5 seconds
      if (System.currentTimeMillis() - lastReportTime >= 5000) {
        long intervalRecords = currentCount - lastCount;
        long intervalTime = System.currentTimeMillis() - lastReportTime;
        double intervalThroughput = (intervalRecords * 1000.0) / intervalTime;
        double overallThroughput = elapsed > 0 ? (insertedSoFar * 1000.0) / elapsed : 0;
        double percentComplete = (insertedSoFar * 100.0) / expectedRecords;

        logger.info(
            "Progress: {}/{} records ({}%), interval: {} rec/sec, overall: {} rec/sec",
            insertedSoFar,
            expectedRecords,
            String.format("%.1f", percentComplete),
            String.format("%.0f", intervalThroughput),
            String.format("%.0f", overallThroughput));

        lastCount = currentCount;
        lastReportTime = System.currentTimeMillis();
      }

      // Check completion
      if (insertedSoFar >= expectedRecords) {
        long duration = System.currentTimeMillis() - startTime;
        double throughput = (insertedSoFar * 1000.0) / duration;

        return new BenchmarkResult(expectedRecords, insertedSoFar, duration, throughput, true);
      }

      // Check timeout
      if (elapsed >= timeoutMs) {
        long duration = System.currentTimeMillis() - startTime;
        double throughput = (insertedSoFar * 1000.0) / duration;

        logger.warn("Timeout reached. Only {}/{} records inserted", insertedSoFar, expectedRecords);

        return new BenchmarkResult(expectedRecords, insertedSoFar, duration, throughput, false);
      }
    }

    return new BenchmarkResult(expectedRecords, 0, 0, 0, false);
  }

  /** Get current row count from MySQL. */
  public long getRowCount() {
    String sql = "SELECT COUNT(*) FROM " + tableName;

    try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {

      if (rs.next()) {
        return rs.getLong(1);
      }
      return 0;

    } catch (SQLException e) {
      logger.error("Failed to get row count", e);
      return -1;
    }
  }

  /** Clear all records from the table. */
  public void clearTable() {
    String sql = "DELETE FROM " + tableName;

    try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
        Statement stmt = conn.createStatement()) {

      int deleted = stmt.executeUpdate(sql);
      logger.info("Cleared {} records from {}", deleted, tableName);

    } catch (SQLException e) {
      logger.error("Failed to clear table", e);
    }
  }

  public static class BenchmarkResult {
    public final int expectedRecords;
    public final long actualRecords;
    public final long durationMs;
    public final double throughput;
    public final boolean completed;

    public BenchmarkResult(
        int expectedRecords,
        long actualRecords,
        long durationMs,
        double throughput,
        boolean completed) {
      this.expectedRecords = expectedRecords;
      this.actualRecords = actualRecords;
      this.durationMs = durationMs;
      this.throughput = throughput;
      this.completed = completed;
    }

    @Override
    public String toString() {
      return String.format(
          "BenchmarkResult{expected=%d, actual=%d, duration=%dms, throughput=%.0f rec/sec, completed=%s}",
          expectedRecords, actualRecords, durationMs, throughput, completed);
    }
  }

  public static void main(String[] args) {
    // Parse arguments
    int expectedRecords = 10000;
    int timeoutSeconds = 120;
    int pollIntervalMs = 500;
    boolean clearFirst = false;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--expected":
        case "-e":
          expectedRecords = Integer.parseInt(args[++i]);
          break;
        case "--timeout":
        case "-t":
          timeoutSeconds = Integer.parseInt(args[++i]);
          break;
        case "--poll":
        case "-p":
          pollIntervalMs = Integer.parseInt(args[++i]);
          break;
        case "--clear":
        case "-c":
          clearFirst = true;
          break;
        case "--help":
        case "-h":
          System.out.println("Usage: BenchmarkMonitor [options]");
          System.out.println(
              "  --expected, -e <num>   Expected number of records (default: 10000)");
          System.out.println("  --timeout, -t <sec>    Timeout in seconds (default: 120)");
          System.out.println("  --poll, -p <ms>        Poll interval in ms (default: 500)");
          System.out.println("  --clear, -c            Clear table before monitoring");
          System.out.println("Environment variables:");
          System.out.println("  DB_HOST, DB_PORT, DB_DATABASE");
          System.out.println("  DB_USER, DB_PASSWORD");
          System.exit(0);
          break;
      }
    }

    ServiceConfig config = new ServiceConfig();
    BenchmarkMonitor monitor = new BenchmarkMonitor(config);

    if (clearFirst) {
      monitor.clearTable();
    }

    logger.info("Waiting for {} records (timeout: {}s)...", expectedRecords, timeoutSeconds);

    BenchmarkResult result = monitor.monitor(expectedRecords, timeoutSeconds, pollIntervalMs);

    // Output results
    logger.info("=== COORDINATOR THROUGHPUT RESULTS ===");
    logger.info("Expected records: {}", result.expectedRecords);
    logger.info("Actual records inserted: {}", result.actualRecords);
    logger.info(
        "Duration: {} ms ({} sec)",
        result.durationMs,
        String.format("%.1f", result.durationMs / 1000.0));
    logger.info("Throughput: {} records/sec", String.format("%.0f", result.throughput));
    logger.info("Completed: {}", result.completed ? "YES" : "NO (timeout)");
    logger.info("=======================================");

    System.exit(result.completed ? 0 : 1);
  }
}
