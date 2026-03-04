package com.yscope.clp.service.metastore.schema;

import com.yscope.clp.service.common.Timestamps;
import com.yscope.clp.service.common.config.ServiceConfig;
import com.yscope.clp.service.metastore.SqlIdentifiers;
import java.sql.*;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages MySQL table partitions for the metadata table.
 *
 * <p>Responsibilities:
 *
 * <ul>
 *   <li>Create lookahead partitions (e.g., N days ahead)
 *   <li>Cleanup old partitions (merge sparse, drop empty)
 *   <li>Monitor partition health
 * </ul>
 *
 * <p>Uses MySQL RANGE partitioning on expires_at column for efficient cleanup of expired data.
 *
 * <p>Thread safety: Multiple threads and nodes may call {@link #runMaintenance()} concurrently.
 * Coordination uses MySQL advisory locks ({@code GET_LOCK('pm_<table>', ...)}) to ensure only one
 * caller runs DDL at a time. Losers skip immediately (non-blocking) or wait briefly (blocking
 * startup). Advisory locks are connection-scoped and auto-released if the holder crashes.
 *
 * <p>Connection pooling note: The advisory lock is held on a dedicated connection obtained from the
 * pool, kept open (via try-with-resources) for the duration of maintenance, and released before the
 * connection is returned. DDL operations use separate pooled connections internally.
 */
public class PartitionManager {
  private static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);
  private static final String PARTITION_PREFIX = "p_";
  private static final DateTimeFormatter PARTITION_NAME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd");

  // INFORMATION_SCHEMA fields
  private static final org.jooq.Field<String> PARTITION_NAME =
      DSL.field(DSL.name("PARTITION_NAME"), SQLDataType.VARCHAR);
  private static final org.jooq.Field<String> PARTITION_DESCRIPTION =
      DSL.field(DSL.name("PARTITION_DESCRIPTION"), SQLDataType.VARCHAR);
  private static final org.jooq.Field<Long> TABLE_ROWS =
      DSL.field(DSL.name("TABLE_ROWS"), SQLDataType.BIGINT);
  private static final org.jooq.Field<Long> DATA_LENGTH =
      DSL.field(DSL.name("DATA_LENGTH"), SQLDataType.BIGINT);
  private static final org.jooq.Field<String> TABLE_SCHEMA =
      DSL.field(DSL.name("TABLE_SCHEMA"), SQLDataType.VARCHAR);
  private static final org.jooq.Field<String> TABLE_NAME_FIELD =
      DSL.field(DSL.name("TABLE_NAME"), SQLDataType.VARCHAR);

  private static final org.jooq.Table<?> INFO_PARTITIONS =
      DSL.table(DSL.name("INFORMATION_SCHEMA", "PARTITIONS"));

  private final DSLContext dsl;
  private final String tableName;
  private final int lookaheadDays;
  private final int cleanupAgeDays;
  private final long sparseRowThreshold;

  /**
   * Create a PartitionManager.
   *
   * @param dsl jOOQ DSL context
   * @param tableName Name of the partitioned table
   * @param lookaheadDays Number of days to create partitions ahead
   * @param cleanupAgeDays Age in days after which empty partitions are dropped
   * @param sparseRowThreshold Row count below which partitions are considered sparse for merging
   */
  public PartitionManager(
      DSLContext dsl,
      String tableName,
      int lookaheadDays,
      int cleanupAgeDays,
      long sparseRowThreshold) {
    this.dsl = dsl;
    this.tableName = SqlIdentifiers.requireValidTableName(tableName);
    this.lookaheadDays = lookaheadDays;
    this.cleanupAgeDays = cleanupAgeDays;
    this.sparseRowThreshold = sparseRowThreshold;
  }

  /** Create a PartitionManager with configuration. */
  public PartitionManager(DSLContext dsl, ServiceConfig config) {
    this(
        dsl,
        config.getDatabaseTable(),
        config.getPartitionLookaheadDays(),
        config.getPartitionCleanupAgeDays(),
        config.getPartitionSparseRowThreshold());
  }

  /**
   * Run partition maintenance under an advisory lock.
   *
   * <p>Acquires {@code GET_LOCK('pm_<table>', 0)} (non-blocking). If the lock is already held by
   * another node/thread, this call returns immediately. Otherwise it creates lookahead partitions
   * and cleans up old partitions.
   */
  public void runMaintenance() {
    String lockName = "pm_" + tableName;
    try {
      // Advisory locks need a dedicated connection held open for the duration
      dsl.connection(
          conn -> {
            if (!acquireLock(conn, lockName, 0)) {
              logger.debug(
                  "Partition maintenance lock held by another node for {}, skipping", tableName);
              return;
            }
            try {
              logger.debug("Starting partition maintenance for {}", tableName);
              createLookaheadPartitions();
              cleanupOldPartitions();
              logger.debug("Partition maintenance completed for {}", tableName);
            } finally {
              releaseLock(conn, lockName);
            }
          });
    } catch (DataAccessException e) {
      logger.error("Error during partition maintenance for {}", tableName, e);
    }
  }

  /**
   * Ensure lookahead partitions exist (BLOCKING).
   *
   * <p>This method BLOCKS until all required lookahead partitions are created. It should be called
   * during coordinator startup BEFORE accepting any data, to ensure no data lands in p_future
   * (which would require expensive REORGANIZE to move data later).
   *
   * <p>Uses a short advisory lock timeout (5 seconds) so startup can wait briefly for another node
   * to finish, rather than skipping immediately.
   *
   * @return number of partitions created
   * @throws SQLException if partition creation fails or lock cannot be acquired
   */
  public int ensureLookaheadPartitions() throws SQLException {
    logger.info("Ensuring lookahead partitions exist (blocking startup)...");

    String lockName = "pm_" + tableName;
    try {
      return dsl.connectionResult(
          conn -> {
            boolean lockAcquired = acquireLock(conn, lockName, 5);
            if (!lockAcquired) {
              logger.warn(
                  "Advisory lock held by another node for {}, "
                      + "proceeding without lock (partitions may already be created)",
                  tableName);
            }
            try {
              return doEnsureLookaheadPartitions();
            } finally {
              if (lockAcquired) {
                releaseLock(conn, lockName);
              }
            }
          });
    } catch (DataAccessException e) {
      if (e.getCause() instanceof SQLException sqlEx) {
        throw sqlEx;
      }
      throw new SQLException(e.getMessage(), e);
    }
  }

  private int doEnsureLookaheadPartitions() throws SQLException {
    List<PartitionInfo> existing = getExistingPartitions();
    LocalDate today = LocalDate.now(ZoneOffset.UTC);

    int created = 0;
    for (int i = 0; i <= lookaheadDays; i++) {
      LocalDate partitionDate = today.plusDays(i);
      String partitionName = PARTITION_PREFIX + partitionDate.format(PARTITION_NAME_FORMAT);

      // Check if partition already exists
      boolean exists = existing.stream().anyMatch(p -> p.name.equals(partitionName));

      if (!exists) {
        createPartition(partitionName, partitionDate);
        created++;
        logger.info(
            "Created partition {} ({} of {} lookahead days)", partitionName, i, lookaheadDays);
      }
    }

    if (created > 0) {
      logger.info("Startup partition creation complete: {} partitions created", created);
    } else {
      logger.info("All {} lookahead partitions already exist", lookaheadDays + 1);
    }

    return created;
  }

  /**
   * Create partitions for the next N days.
   *
   * <p>Creates daily partitions based on expires_at column. Uses ADD PARTITION for tables that are
   * already partitioned.
   */
  public void createLookaheadPartitions() throws SQLException {
    logger.debug("Creating lookahead partitions for {} days", lookaheadDays);

    List<PartitionInfo> existing = getExistingPartitions();
    LocalDate today = LocalDate.now(ZoneOffset.UTC);

    int created = 0;
    for (int i = 0; i <= lookaheadDays; i++) {
      LocalDate partitionDate = today.plusDays(i);
      String partitionName = PARTITION_PREFIX + partitionDate.format(PARTITION_NAME_FORMAT);

      // Check if partition already exists
      boolean exists = existing.stream().anyMatch(p -> p.name.equals(partitionName));

      if (!exists) {
        createPartition(partitionName, partitionDate);
        created++;
      }
    }

    if (created > 0) {
      logger.info("Created {} lookahead partitions", created);
    }
  }

  /**
   * Acquire a MySQL advisory lock.
   *
   * @param conn Connection to hold the lock (must stay open)
   * @param lockName Lock name (max 64 chars)
   * @param timeoutSeconds 0 for non-blocking, >0 to wait
   * @return true if lock acquired, false otherwise
   */
  private boolean acquireLock(Connection conn, String lockName, int timeoutSeconds)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("SELECT GET_LOCK(?, ?)")) {
      ps.setString(1, lockName);
      ps.setInt(2, timeoutSeconds);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getInt(1) == 1;
      }
    }
  }

  /**
   * Release a MySQL advisory lock. Logs a warning on failure but does not throw.
   *
   * @param conn Connection that holds the lock
   * @param lockName Lock name to release
   */
  private void releaseLock(Connection conn, String lockName) {
    try (PreparedStatement ps = conn.prepareStatement("SELECT RELEASE_LOCK(?)")) {
      ps.setString(1, lockName);
      try (ResultSet rs = ps.executeQuery()) {
        // ResultSet consumed and closed
      }
    } catch (SQLException e) {
      logger.warn("Failed to release lock {}", lockName, e);
    }
  }

  /**
   * Create a single partition for a specific date.
   *
   * <p>If a MAXVALUE partition (p_future) exists, uses REORGANIZE PARTITION to split it. Otherwise,
   * uses ADD PARTITION.
   *
   * <p>Partition DDL stays as plain SQL — jOOQ has no RANGE partition DDL support.
   */
  private void createPartition(String partitionName, LocalDate partitionDate) throws SQLException {
    // Calculate the LESS THAN value (start of next day in epoch nanoseconds)
    long lessThan = Timestamps.startOfDayNanos(partitionDate.plusDays(1));

    // Check if MAXVALUE partition exists
    String maxValuePartition = findMaxValuePartition();

    String sql;
    if (maxValuePartition != null) {
      SqlIdentifiers.requireValid(maxValuePartition, "partition name from INFORMATION_SCHEMA");
      // REORGANIZE: split MAXVALUE partition into new partition + MAXVALUE
      sql =
          String.format(
              "ALTER TABLE %s REORGANIZE PARTITION %s INTO ("
                  + "PARTITION %s VALUES LESS THAN (%d), "
                  + "PARTITION %s VALUES LESS THAN MAXVALUE)",
              tableName, maxValuePartition, partitionName, lessThan, maxValuePartition);
    } else {
      // ADD PARTITION: no MAXVALUE partition exists
      sql =
          String.format(
              "ALTER TABLE %s ADD PARTITION (PARTITION %s VALUES LESS THAN (%d))",
              tableName, partitionName, lessThan);
    }

    try {
      dsl.execute(sql);
      logger.debug(
          "Created partition {} (LESS THAN {}) via {}",
          partitionName,
          lessThan,
          maxValuePartition != null ? "REORGANIZE" : "ADD");
    } catch (DataAccessException e) {
      Throwable cause = e.getCause();
      String causeMsg = cause != null ? cause.getMessage() : null;
      if (causeMsg != null
          && (causeMsg.contains("already exists")
              || causeMsg.contains("Duplicate partition name"))) {
        logger.debug("Partition {} already exists", partitionName);
      } else if (causeMsg != null && causeMsg.contains("partitioned")) {
        logger.warn("Table {} is not partitioned, skipping partition creation", tableName);
      } else {
        if (cause instanceof SQLException sqlEx) {
          throw sqlEx;
        }
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  /**
   * Find the MAXVALUE partition name if one exists.
   *
   * @return Partition name with MAXVALUE, or null if none exists
   */
  private String findMaxValuePartition() {
    var row =
        dsl.select(PARTITION_NAME)
            .from(INFO_PARTITIONS)
            .where(TABLE_SCHEMA.eq(DSL.field("DATABASE()", SQLDataType.VARCHAR)))
            .and(TABLE_NAME_FIELD.eq(tableName))
            .and(PARTITION_DESCRIPTION.eq("MAXVALUE"))
            .fetchOne();

    return row != null ? row.value1() : null;
  }

  /**
   * Cleanup old and sparse partitions.
   *
   * <p>Operations:
   *
   * <ul>
   *   <li>Empty partitions older than cleanupAgeDays: DROP
   *   <li>Sparse partitions (rows < threshold): MERGE into first partition
   * </ul>
   *
   * <p>Safety: Partitions with data are NEVER dropped - they are merged into the first partition
   * (historical catch-all) using REORGANIZE PARTITION.
   */
  public void cleanupOldPartitions() throws SQLException {
    logger.debug("Cleaning up old partitions (age > {} days)", cleanupAgeDays);

    List<PartitionInfo> partitions = getExistingPartitions();
    if (partitions.isEmpty()) {
      logger.debug("No partitions found");
      return;
    }

    LocalDate cutoffDate = LocalDate.now(ZoneOffset.UTC).minusDays(cleanupAgeDays);
    String cutoffName = PARTITION_PREFIX + cutoffDate.format(PARTITION_NAME_FORMAT);

    // Find the first partition (merge target)
    PartitionInfo firstPartition =
        partitions.stream()
            .filter(p -> p.name.startsWith(PARTITION_PREFIX) && p.name.length() >= 10)
            .min((a, b) -> a.name.compareTo(b.name))
            .orElse(null);

    if (firstPartition == null) {
      logger.warn("No date-based partitions found, skipping cleanup");
      return;
    }

    int dropped = 0;
    int merged = 0;
    for (PartitionInfo partition : partitions) {
      // Skip partitions that are not date-based (e.g., p_future, pMAXVALUE)
      if (!partition.name.startsWith(PARTITION_PREFIX) || partition.name.length() < 10) {
        continue;
      }

      // Skip the first partition (can't merge into itself)
      if (partition.name.equals(firstPartition.name)) {
        continue;
      }

      // Check if partition is older than cutoff
      if (partition.name.compareTo(cutoffName) < 0) {
        if (partition.rowCount == 0) {
          // Empty partition: safe to drop
          dropPartition(partition.name);
          dropped++;
        } else if (partition.rowCount < sparseRowThreshold) {
          // Sparse partition: merge into first partition
          mergePartitionIntoFirst(partition.name, firstPartition.name);
          merged++;
        }
      }
    }

    if (dropped > 0 || merged > 0) {
      logger.info("Partition cleanup: dropped {} empty, merged {} sparse", dropped, merged);
    }
  }

  /**
   * Merge a partition into the first partition using REORGANIZE PARTITION.
   *
   * <p>This moves all rows from the source partition into the first partition, then removes the
   * source partition boundary. Data is NEVER lost.
   *
   * <p>Partition DDL stays as plain SQL — jOOQ has no RANGE partition DDL support.
   */
  private void mergePartitionIntoFirst(String sourcePartition, String targetPartition)
      throws SQLException {
    SqlIdentifiers.requireValid(sourcePartition, "source partition name");
    SqlIdentifiers.requireValid(targetPartition, "target partition name");
    long targetLessThan = getPartitionLessThanValue(targetPartition);

    if (targetLessThan == -1) {
      logger.warn("Could not determine LESS THAN value for {}, skipping merge", targetPartition);
      return;
    }

    String sql =
        String.format(
            "ALTER TABLE %s REORGANIZE PARTITION %s, %s INTO ("
                + "PARTITION %s VALUES LESS THAN (%d)"
                + ")",
            tableName, targetPartition, sourcePartition, targetPartition, targetLessThan);

    try {
      dsl.execute(sql);
      logger.info("Merged partition {} into {}", sourcePartition, targetPartition);
    } catch (DataAccessException e) {
      Throwable cause = e.getCause();
      String causeMsg = cause != null ? cause.getMessage() : null;
      if (causeMsg != null
          && (causeMsg.contains("doesn't exist") || causeMsg.contains("Unknown partition"))) {
        logger.debug("Partition {} already merged/dropped", sourcePartition);
      } else {
        if (cause instanceof SQLException sqlEx) {
          throw sqlEx;
        }
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  /** Get the LESS THAN value for a partition from INFORMATION_SCHEMA. */
  private long getPartitionLessThanValue(String partitionName) {
    var row =
        dsl.select(PARTITION_DESCRIPTION)
            .from(INFO_PARTITIONS)
            .where(TABLE_SCHEMA.eq(DSL.field("DATABASE()", SQLDataType.VARCHAR)))
            .and(TABLE_NAME_FIELD.eq(tableName))
            .and(PARTITION_NAME.eq(partitionName))
            .fetchOne();

    if (row != null) {
      String desc = row.value1();
      if (desc != null && !desc.equals("MAXVALUE")) {
        return Long.parseLong(desc);
      }
    }
    return -1;
  }

  /** Drop a partition. Partition DDL stays as plain SQL. */
  private void dropPartition(String partitionName) throws SQLException {
    SqlIdentifiers.requireValid(partitionName, "partition name");
    String sql = String.format("ALTER TABLE %s DROP PARTITION %s", tableName, partitionName);

    try {
      dsl.execute(sql);
      logger.debug("Dropped partition {}", partitionName);
    } catch (DataAccessException e) {
      Throwable cause = e.getCause();
      String causeMsg = cause != null ? cause.getMessage() : null;
      if (causeMsg != null
          && (causeMsg.contains("doesn't exist") || causeMsg.contains("Unknown partition"))) {
        logger.debug("Partition {} already dropped", partitionName);
      } else {
        if (cause instanceof SQLException sqlEx) {
          throw sqlEx;
        }
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  /** Get information about existing partitions. */
  public List<PartitionInfo> getExistingPartitions() throws SQLException {
    try {
      var rows =
          dsl.select(PARTITION_NAME, TABLE_ROWS, DATA_LENGTH)
              .from(INFO_PARTITIONS)
              .where(TABLE_SCHEMA.eq(DSL.field("DATABASE()", SQLDataType.VARCHAR)))
              .and(TABLE_NAME_FIELD.eq(tableName))
              .and(PARTITION_NAME.isNotNull())
              .orderBy(DSL.field(DSL.name("PARTITION_ORDINAL_POSITION")))
              .fetch();

      List<PartitionInfo> partitions = new ArrayList<>();
      for (var r : rows) {
        PartitionInfo info = new PartitionInfo();
        info.name = r.value1();
        info.rowCount = r.value2() != null ? r.value2() : 0;
        info.dataLength = r.value3() != null ? r.value3() : 0;
        partitions.add(info);
      }
      return partitions;
    } catch (DataAccessException e) {
      if (e.getCause() instanceof SQLException sqlEx) {
        throw sqlEx;
      }
      throw new SQLException(e.getMessage(), e);
    }
  }

  /** Check if the table is partitioned. */
  public boolean isTablePartitioned() throws SQLException {
    try {
      int count =
          dsl.selectCount()
              .from(INFO_PARTITIONS)
              .where(TABLE_SCHEMA.eq(DSL.field("DATABASE()", SQLDataType.VARCHAR)))
              .and(TABLE_NAME_FIELD.eq(tableName))
              .and(PARTITION_NAME.isNotNull())
              .fetchOne(0, int.class);

      return count > 0;
    } catch (DataAccessException e) {
      if (e.getCause() instanceof SQLException sqlEx) {
        throw sqlEx;
      }
      throw new SQLException(e.getMessage(), e);
    }
  }

  /** Get partition statistics. */
  public PartitionStats getStats() throws SQLException {
    PartitionStats stats = new PartitionStats();
    List<PartitionInfo> partitions = getExistingPartitions();

    stats.totalPartitions = partitions.size();
    stats.totalRows = partitions.stream().mapToLong(p -> p.rowCount).sum();
    stats.emptyPartitions = (int) partitions.stream().filter(p -> p.rowCount == 0).count();
    stats.sparsePartitions =
        (int)
            partitions.stream()
                .filter(p -> p.rowCount > 0 && p.rowCount < sparseRowThreshold)
                .count();

    return stats;
  }

  /** Information about a single partition. */
  public static class PartitionInfo {
    public String name;
    public long rowCount;
    public long dataLength;

    @Override
    public String toString() {
      return String.format(
          "PartitionInfo{name='%s', rows=%d, size=%d}", name, rowCount, dataLength);
    }
  }

  /** Aggregate statistics about partitions. */
  public static class PartitionStats {
    public int totalPartitions;
    public long totalRows;
    public int emptyPartitions;
    public int sparsePartitions;

    @Override
    public String toString() {
      return String.format(
          "PartitionStats{total=%d, rows=%d, empty=%d, sparse=%d}",
          totalPartitions, totalRows, emptyPartitions, sparsePartitions);
    }
  }
}
