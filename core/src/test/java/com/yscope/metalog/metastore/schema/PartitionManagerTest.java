package com.yscope.metalog.metastore.schema;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Test;

/**
 * Tests for PartitionManager partition lifecycle management.
 *
 * <p>Uses jOOQ's MockDataProvider to intercept SQL at the query level.
 */
class PartitionManagerTest {

  private static final String TABLE_NAME = "clp_ir_spark";
  private static final int LOOKAHEAD_DAYS = 7;
  private static final int CLEANUP_AGE_DAYS = 30;
  private static final long SPARSE_ROW_THRESHOLD = 1000;

  // --- Partition info tests ---

  @Test
  void testGetExistingPartitions_returnsPartitionList() throws SQLException {
    PartitionManager mgr =
        managerWith(
            ctx -> {
              if (ctx.sql().contains("INFORMATION_SCHEMA")) {
                return partitionRows(
                    partition("p_20240101", 1000, 1024000), partition("p_20240102", 500, 512000));
              }
              return rowsAffected(0);
            });

    List<PartitionManager.PartitionInfo> partitions = mgr.getExistingPartitions();

    assertEquals(2, partitions.size());
    assertEquals("p_20240101", partitions.get(0).name);
    assertEquals(1000L, partitions.get(0).rowCount);
    assertEquals("p_20240102", partitions.get(1).name);
  }

  @Test
  void testGetExistingPartitions_emptyTable_returnsEmptyList() throws SQLException {
    PartitionManager mgr = managerWith(ctx -> emptyPartitionResult());

    List<PartitionManager.PartitionInfo> partitions = mgr.getExistingPartitions();

    assertTrue(partitions.isEmpty());
  }

  // --- Lookahead partition tests ---

  @Test
  void testCreateLookaheadPartitions_createsNewPartitions() throws SQLException {
    List<String> executedDDL = new ArrayList<>();
    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA") && sql.contains("PARTITION")) {
                if (sql.contains("MAXVALUE")) {
                  return emptyPartitionDescResult();
                }
                return emptyPartitionResult();
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    mgr.createLookaheadPartitions();

    // Should create partitions (today + 7 lookahead days)
    assertTrue(executedDDL.size() >= 1);
    assertTrue(executedDDL.stream().allMatch(sql -> sql.contains("ADD PARTITION")));
  }

  @Test
  void testCreateLookaheadPartitions_skipsExisting() throws SQLException {
    String today =
        "p_" + LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    List<String> executedDDL = new ArrayList<>();

    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA") && sql.contains("PARTITION")) {
                if (sql.contains("MAXVALUE")) {
                  return emptyPartitionDescResult();
                }
                return partitionRows(partition(today, 100, 1024));
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    mgr.createLookaheadPartitions();

    // Should skip today's partition but create future ones
    assertEquals(LOOKAHEAD_DAYS, executedDDL.size());
  }

  @Test
  void testCreateLookaheadPartitions_handlesDuplicateError() throws SQLException {
    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA") && sql.contains("PARTITION")) {
                if (sql.contains("MAXVALUE")) {
                  return emptyPartitionDescResult();
                }
                return emptyPartitionResult();
              } else if (sql.contains("ALTER TABLE")) {
                throw new SQLException("Duplicate partition name");
              }
              return rowsAffected(0);
            });

    assertDoesNotThrow(() -> mgr.createLookaheadPartitions());
  }

  @Test
  void testCreateLookaheadPartitions_usesReorganizeWhenMaxValueExists() throws SQLException {
    List<String> executedDDL = new ArrayList<>();
    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA") && sql.contains("PARTITION_DESCRIPTION")) {
                // findMaxValuePartition query — returns p_future
                return partitionDescResult("p_future");
              } else if (sql.contains("INFORMATION_SCHEMA") && sql.contains("PARTITION")) {
                // getExistingPartitions — no existing partitions
                return emptyPartitionResult();
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    mgr.createLookaheadPartitions();

    assertTrue(executedDDL.stream().allMatch(sql -> sql.contains("REORGANIZE PARTITION p_future")));
    assertTrue(
        executedDDL.stream()
            .noneMatch(sql -> sql.contains("ADD PARTITION") && !sql.contains("REORGANIZE")));
  }

  @Test
  void testCreateLookaheadPartitions_usesAddWhenNoMaxValue() throws SQLException {
    List<String> executedDDL = new ArrayList<>();
    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA") && sql.contains("PARTITION")) {
                if (sql.contains("MAXVALUE")) {
                  return emptyPartitionDescResult();
                }
                return emptyPartitionResult();
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    mgr.createLookaheadPartitions();

    assertTrue(executedDDL.stream().allMatch(sql -> sql.contains("ADD PARTITION")));
    assertTrue(executedDDL.stream().noneMatch(sql -> sql.contains("REORGANIZE")));
  }

  // --- Cleanup tests ---

  @Test
  void testCleanupOldPartitions_dropsEmptyOldPartitions() throws SQLException {
    String firstDate = "p_20240101";
    String oldDate =
        "p_"
            + LocalDate.now(ZoneOffset.UTC)
                .minusDays(31)
                .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    List<String> executedDDL = new ArrayList<>();

    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA")) {
                return partitionRows(partition(firstDate, 1000, 1024), partition(oldDate, 0, 0));
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    mgr.cleanupOldPartitions();

    assertTrue(executedDDL.stream().anyMatch(sql -> sql.contains("DROP PARTITION")));
  }

  @Test
  void testCleanupOldPartitions_mergesSparsePartitions() throws SQLException {
    String firstDate = "p_20240101";
    String oldDate =
        "p_"
            + LocalDate.now(ZoneOffset.UTC)
                .minusDays(31)
                .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    List<String> executedDDL = new ArrayList<>();

    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA")
                  && sql.contains("PARTITION_DESCRIPTION")
                  && sql.contains("PARTITION_NAME")) {
                // Partition LESS THAN value query
                return partitionLessThanResult("1704067200");
              } else if (sql.contains("INFORMATION_SCHEMA")) {
                return partitionRows(
                    partition(firstDate, 5000, 1024), partition(oldDate, 500, 512));
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    mgr.cleanupOldPartitions();

    assertTrue(executedDDL.stream().anyMatch(sql -> sql.contains("REORGANIZE PARTITION")));
    assertTrue(executedDDL.stream().noneMatch(sql -> sql.contains("DROP PARTITION")));
  }

  @Test
  void testCleanupOldPartitions_keepsRecentPartitions() throws SQLException {
    String firstDate = "p_20240101";
    String recentDate =
        "p_"
            + LocalDate.now(ZoneOffset.UTC)
                .minusDays(5)
                .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    List<String> executedDDL = new ArrayList<>();

    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA")) {
                return partitionRows(partition(firstDate, 1000, 1024), partition(recentDate, 0, 0));
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    mgr.cleanupOldPartitions();

    assertTrue(executedDDL.isEmpty());
  }

  // --- ensureLookaheadPartitions tests ---

  @Test
  void testEnsureLookaheadPartitions_createsAllMissingPartitions() throws SQLException {
    List<String> executedDDL = new ArrayList<>();
    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("GET_LOCK")) {
                return lockResult(true);
              } else if (sql.contains("RELEASE_LOCK")) {
                return lockResult(true);
              } else if (sql.contains("INFORMATION_SCHEMA") && sql.contains("PARTITION")) {
                if (sql.contains("MAXVALUE")) {
                  return emptyPartitionDescResult();
                }
                return emptyPartitionResult();
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    int created = mgr.ensureLookaheadPartitions();

    assertEquals(LOOKAHEAD_DAYS + 1, created);
  }

  @Test
  void testEnsureLookaheadPartitions_skipsExistingPartitions() throws SQLException {
    String today =
        "p_" + LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    List<String> executedDDL = new ArrayList<>();

    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("GET_LOCK")) {
                return lockResult(true);
              } else if (sql.contains("RELEASE_LOCK")) {
                return lockResult(true);
              } else if (sql.contains("INFORMATION_SCHEMA") && sql.contains("PARTITION")) {
                if (sql.contains("MAXVALUE")) {
                  return emptyPartitionDescResult();
                }
                return partitionRows(partition(today, 100, 1024));
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    int created = mgr.ensureLookaheadPartitions();

    assertEquals(LOOKAHEAD_DAYS, created);
  }

  // --- Statistics tests ---

  @Test
  void testGetStats_calculatesCorrectly() throws SQLException {
    PartitionManager mgr =
        managerWith(
            ctx -> {
              if (ctx.sql().contains("INFORMATION_SCHEMA")) {
                return partitionRows(
                    partition("p_20240101", 0, 0),
                    partition("p_20240102", 500, 512000),
                    partition("p_20240103", 2000, 2048000));
              }
              return rowsAffected(0);
            });

    PartitionManager.PartitionStats stats = mgr.getStats();

    assertEquals(3, stats.totalPartitions);
    assertEquals(2500L, stats.totalRows);
    assertEquals(1, stats.emptyPartitions);
    assertEquals(1, stats.sparsePartitions);
  }

  // --- Table check tests ---

  @Test
  void testIsTablePartitioned_true() throws SQLException {
    PartitionManager mgr =
        managerWith(
            ctx -> {
              if (ctx.sql().contains("count(*)") || ctx.sql().contains("COUNT(*)")) {
                return countResult(5);
              } else if (ctx.sql().contains("INFORMATION_SCHEMA")) {
                return countResult(5);
              }
              return rowsAffected(0);
            });

    assertTrue(mgr.isTablePartitioned());
  }

  @Test
  void testIsTablePartitioned_false() throws SQLException {
    PartitionManager mgr =
        managerWith(
            ctx -> {
              if (ctx.sql().contains("count(*)") || ctx.sql().contains("COUNT(*)")) {
                return countResult(0);
              } else if (ctx.sql().contains("INFORMATION_SCHEMA")) {
                return countResult(0);
              }
              return rowsAffected(0);
            });

    assertFalse(mgr.isTablePartitioned());
  }

  // --- Tier 2: runMaintenance tests ---

  @Test
  void testRunMaintenance_lockAcquired_createsAndCleansUp() throws SQLException {
    List<String> executedDDL = new ArrayList<>();

    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("GET_LOCK")) {
                return lockResult(true);
              } else if (sql.contains("RELEASE_LOCK")) {
                return lockResult(true);
              } else if (sql.contains("INFORMATION_SCHEMA") && sql.contains("PARTITION")) {
                if (sql.contains("MAXVALUE")) {
                  return emptyPartitionDescResult();
                }
                return emptyPartitionResult();
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    assertDoesNotThrow(() -> mgr.runMaintenance());
    // Should have created lookahead partitions
    assertFalse(executedDDL.isEmpty());
  }

  @Test
  void testRunMaintenance_lockNotAcquired_skips() {
    List<String> executedDDL = new ArrayList<>();

    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("GET_LOCK")) {
                return lockResult(false); // Lock held by another node
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    assertDoesNotThrow(() -> mgr.runMaintenance());
    // Should NOT have executed any DDL since lock was not acquired
    assertTrue(executedDDL.isEmpty());
  }

  // --- Tier 2: ensureLookaheadPartitions lock failure ---

  @Test
  void testEnsureLookaheadPartitions_lockNotAcquired_stillCreates() throws SQLException {
    List<String> executedDDL = new ArrayList<>();

    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("GET_LOCK")) {
                return lockResult(false); // Lock not acquired
              } else if (sql.contains("INFORMATION_SCHEMA") && sql.contains("PARTITION")) {
                if (sql.contains("MAXVALUE")) {
                  return emptyPartitionDescResult();
                }
                return emptyPartitionResult();
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    int created = mgr.ensureLookaheadPartitions();

    // Should still create partitions even without lock (warns but proceeds)
    assertEquals(LOOKAHEAD_DAYS + 1, created);
  }

  // --- Tier 2: cleanupOldPartitions edge cases ---

  @Test
  void testCleanupOldPartitions_emptyPartitionsList_noDdlExecuted() throws SQLException {
    List<String> executedDDL = new ArrayList<>();

    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA")) {
                return emptyPartitionResult();
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    mgr.cleanupOldPartitions();

    assertTrue(executedDDL.isEmpty());
  }

  @Test
  void testCleanupOldPartitions_noDateBasedPartitions_noDdl() throws SQLException {
    List<String> executedDDL = new ArrayList<>();

    PartitionManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA")) {
                // Only short names that don't match date-based pattern
                return partitionRows(partition("p_fut", 100, 1024));
              } else if (sql.contains("ALTER TABLE")) {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
              return rowsAffected(0);
            });

    mgr.cleanupOldPartitions();

    assertTrue(executedDDL.isEmpty());
  }

  // --- Tier 2: getExistingPartitions null TABLE_ROWS ---

  @Test
  void testGetExistingPartitions_nullTableRows_defaultsToZero() throws SQLException {
    PartitionManager mgr =
        managerWith(
            ctx -> {
              if (ctx.sql().contains("INFORMATION_SCHEMA")) {
                // Build a result where TABLE_ROWS is null
                var nameField = DSL.field("PARTITION_NAME", SQLDataType.VARCHAR);
                var rowsField = DSL.field("TABLE_ROWS", SQLDataType.BIGINT);
                var dataLenField = DSL.field("DATA_LENGTH", SQLDataType.BIGINT);
                var result =
                    DSL.using(SQLDialect.MYSQL).newResult(nameField, rowsField, dataLenField);
                var record =
                    DSL.using(SQLDialect.MYSQL).newRecord(nameField, rowsField, dataLenField);
                record.set(nameField, "p_20240101");
                record.set(rowsField, (Long) null);
                record.set(dataLenField, (Long) null);
                result.add(record);
                return new MockResult[] {new MockResult(1, result)};
              }
              return rowsAffected(0);
            });

    List<PartitionManager.PartitionInfo> partitions = mgr.getExistingPartitions();

    assertEquals(1, partitions.size());
    assertEquals(0L, partitions.get(0).rowCount);
    assertEquals(0L, partitions.get(0).dataLength);
  }

  // --- Tier 2: Constructor validation ---

  @Test
  void testConstructor_invalidTableName_throws() {
    DSLContext dsl = DSL.using(new MockConnection(ctx -> rowsAffected(0)), SQLDialect.MYSQL);

    assertThrows(
        IllegalArgumentException.class,
        () -> new PartitionManager(dsl, "Robert'; DROP TABLE--", 7, 30, 1000));
  }

  // --- Tier 2: getStats with all-empty partitions ---

  @Test
  void testGetStats_allEmptyPartitions_correctCounts() throws SQLException {
    PartitionManager mgr =
        managerWith(
            ctx -> {
              if (ctx.sql().contains("INFORMATION_SCHEMA")) {
                return partitionRows(
                    partition("p_20240101", 0, 0),
                    partition("p_20240102", 0, 0),
                    partition("p_20240103", 0, 0));
              }
              return rowsAffected(0);
            });

    PartitionManager.PartitionStats stats = mgr.getStats();

    assertEquals(3, stats.totalPartitions);
    assertEquals(0L, stats.totalRows);
    assertEquals(3, stats.emptyPartitions);
    assertEquals(0, stats.sparsePartitions);
  }

  // ==========================================
  // Helper Methods
  // ==========================================

  private PartitionManager managerWith(MockDataProvider provider) {
    DSLContext dsl = DSL.using(new MockConnection(provider), SQLDialect.MYSQL);
    return new PartitionManager(
        dsl, TABLE_NAME, LOOKAHEAD_DAYS, CLEANUP_AGE_DAYS, SPARSE_ROW_THRESHOLD);
  }

  private record PartitionData(String name, long rows, long dataLength) {}

  private PartitionData partition(String name, long rows, long dataLength) {
    return new PartitionData(name, rows, dataLength);
  }

  private MockResult[] partitionRows(PartitionData... partitions) {
    var nameField = DSL.field("PARTITION_NAME", SQLDataType.VARCHAR);
    var rowsField = DSL.field("TABLE_ROWS", SQLDataType.BIGINT);
    var dataLenField = DSL.field("DATA_LENGTH", SQLDataType.BIGINT);
    var result = DSL.using(SQLDialect.MYSQL).newResult(nameField, rowsField, dataLenField);
    for (var p : partitions) {
      var record = DSL.using(SQLDialect.MYSQL).newRecord(nameField, rowsField, dataLenField);
      record.set(nameField, p.name);
      record.set(rowsField, p.rows);
      record.set(dataLenField, p.dataLength);
      result.add(record);
    }
    return new MockResult[] {new MockResult(result.size(), result)};
  }

  private MockResult[] emptyPartitionResult() {
    var nameField = DSL.field("PARTITION_NAME", SQLDataType.VARCHAR);
    var rowsField = DSL.field("TABLE_ROWS", SQLDataType.BIGINT);
    var dataLenField = DSL.field("DATA_LENGTH", SQLDataType.BIGINT);
    var result = DSL.using(SQLDialect.MYSQL).newResult(nameField, rowsField, dataLenField);
    return new MockResult[] {new MockResult(0, result)};
  }

  private MockResult[] partitionDescResult(String partitionName) {
    var field = DSL.field("PARTITION_NAME", SQLDataType.VARCHAR);
    var result = DSL.using(SQLDialect.MYSQL).newResult(field);
    var record = DSL.using(SQLDialect.MYSQL).newRecord(field);
    record.set(field, partitionName);
    result.add(record);
    return new MockResult[] {new MockResult(1, result)};
  }

  private MockResult[] emptyPartitionDescResult() {
    var field = DSL.field("PARTITION_NAME", SQLDataType.VARCHAR);
    var result = DSL.using(SQLDialect.MYSQL).newResult(field);
    return new MockResult[] {new MockResult(0, result)};
  }

  private MockResult[] partitionLessThanResult(String lessThan) {
    var field = DSL.field("PARTITION_DESCRIPTION", SQLDataType.VARCHAR);
    var result = DSL.using(SQLDialect.MYSQL).newResult(field);
    var record = DSL.using(SQLDialect.MYSQL).newRecord(field);
    record.set(field, lessThan);
    result.add(record);
    return new MockResult[] {new MockResult(1, result)};
  }

  private MockResult[] lockResult(boolean acquired) {
    var field = DSL.field("lock_result", SQLDataType.INTEGER);
    var result = DSL.using(SQLDialect.MYSQL).newResult(field);
    var record = DSL.using(SQLDialect.MYSQL).newRecord(field);
    record.set(field, acquired ? 1 : 0);
    result.add(record);
    return new MockResult[] {new MockResult(1, result)};
  }

  private MockResult[] countResult(int count) {
    var field = DSL.field("cnt", SQLDataType.INTEGER);
    var result = DSL.using(SQLDialect.MYSQL).newResult(field);
    var record = DSL.using(SQLDialect.MYSQL).newRecord(field);
    record.set(field, count);
    result.add(record);
    return new MockResult[] {new MockResult(1, result)};
  }

  private MockResult[] rowsAffected(int count) {
    return new MockResult[] {new MockResult(count)};
  }
}
