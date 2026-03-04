package com.yscope.metalog.metastore.schema;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.metalog.common.config.IndexConfig;
import java.sql.SQLException;
import java.util.*;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Test;

/**
 * Tests for DynamicIndexManager index reconciliation.
 *
 * <p>Uses jOOQ's MockDataProvider to intercept SQL at the query level.
 */
class DynamicIndexManagerTest {

  private static final String TABLE_NAME = "clp_ir_spark";

  // --- Get existing indexes tests ---

  @Test
  void testGetExistingIndexes_returnsIndexSet() {
    DynamicIndexManager mgr =
        managerWith(infoSchemaIndexes("PRIMARY", "idx_consolidation", "idx_custom"));

    Set<String> indexes = mgr.getExistingIndexes();

    assertEquals(3, indexes.size());
    assertTrue(indexes.contains("PRIMARY"));
    assertTrue(indexes.contains("idx_custom"));
  }

  @Test
  void testGetExistingIndexes_emptyTable_returnsEmptySet() {
    DynamicIndexManager mgr = managerWith(infoSchemaIndexes());

    Set<String> indexes = mgr.getExistingIndexes();

    assertTrue(indexes.isEmpty());
  }

  // --- Reconcile tests ---

  @Test
  void testReconcile_createsEnabledMissingIndexes() {
    List<String> executedDDL = new ArrayList<>();
    DynamicIndexManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA") && sql.contains("STATISTICS")) {
                return noRows();
              } else if (sql.contains("INFORMATION_SCHEMA") && sql.contains("COLUMNS")) {
                return columnRows("col1");
              } else {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
            });

    IndexConfig config =
        createIndexConfig(createIndexDefinition("idx_test", List.of("col1"), true));

    DynamicIndexManager.ReconcileResult result = mgr.reconcile(config);

    assertEquals(1, result.created.size());
    assertTrue(result.created.contains("idx_test"));
    assertTrue(executedDDL.stream().anyMatch(s -> s.contains("CREATE INDEX idx_test")));
  }

  @Test
  void testReconcile_skipsExistingIndexes() {
    List<String> executedDDL = new ArrayList<>();
    DynamicIndexManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA")) {
                return indexRows("idx_test");
              } else {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
            });

    IndexConfig config =
        createIndexConfig(createIndexDefinition("idx_test", List.of("col1"), true));

    DynamicIndexManager.ReconcileResult result = mgr.reconcile(config);

    assertTrue(result.created.isEmpty());
    assertTrue(executedDDL.isEmpty());
  }

  @Test
  void testReconcile_dropsDisabledIndexes() {
    List<String> executedDDL = new ArrayList<>();
    DynamicIndexManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA")) {
                return indexRows("idx_to_drop");
              } else {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
            });

    IndexConfig config =
        createIndexConfig(createIndexDefinition("idx_to_drop", List.of("col1"), false));

    DynamicIndexManager.ReconcileResult result = mgr.reconcile(config);

    assertEquals(1, result.dropped.size());
    assertTrue(executedDDL.stream().anyMatch(s -> s.contains("DROP INDEX idx_to_drop")));
  }

  @Test
  void testReconcile_protectsSystemIndexes() {
    DynamicIndexManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA")) {
                return indexRows("PRIMARY", "idx_consolidation");
              }
              return rowsAffected(0);
            });

    IndexConfig config =
        createIndexConfig(
            createIndexDefinition("PRIMARY", List.of("id"), false),
            createIndexDefinition("idx_consolidation", List.of("state", "min_timestamp"), false));

    DynamicIndexManager.ReconcileResult result = mgr.reconcile(config);

    assertEquals(2, result.skipped.size());
    assertTrue(result.dropped.isEmpty());
  }

  @Test
  void testReconcile_handlesCreateError() {
    DynamicIndexManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA") && sql.contains("STATISTICS")) {
                return noRows();
              } else if (sql.contains("INFORMATION_SCHEMA") && sql.contains("COLUMNS")) {
                return columnRows("nonexistent_col");
              } else if (sql.contains("CREATE INDEX")) {
                throw new SQLException("Column not found");
              }
              return rowsAffected(0);
            });

    IndexConfig config =
        createIndexConfig(createIndexDefinition("idx_bad", List.of("nonexistent_col"), true));

    DynamicIndexManager.ReconcileResult result = mgr.reconcile(config);

    assertTrue(result.created.isEmpty());
    assertTrue(result.hasErrors());
    assertTrue(result.errors.containsKey("idx_bad"));
  }

  @Test
  void testReconcile_handlesDuplicateKeyError() {
    DynamicIndexManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA") && sql.contains("STATISTICS")) {
                return noRows();
              } else if (sql.contains("INFORMATION_SCHEMA") && sql.contains("COLUMNS")) {
                return columnRows("col1");
              } else {
                throw new SQLException("Duplicate key name");
              }
            });

    IndexConfig config =
        createIndexConfig(createIndexDefinition("idx_test", List.of("col1"), true));

    DynamicIndexManager.ReconcileResult result = mgr.reconcile(config);

    // Should not report as error (index already exists, caught by createIndex)
    assertTrue(result.errors.isEmpty());
  }

  // --- Online DDL tests ---

  @Test
  void testReconcile_usesOnlineDDL() {
    List<String> executedDDL = new ArrayList<>();
    DynamicIndexManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA") && sql.contains("STATISTICS")) {
                return noRows();
              } else if (sql.contains("INFORMATION_SCHEMA") && sql.contains("COLUMNS")) {
                return columnRows("col1");
              } else {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
            });

    IndexConfig config =
        createIndexConfig(createIndexDefinition("idx_test", List.of("col1"), true));

    mgr.reconcile(config);

    assertTrue(
        executedDDL.stream()
            .anyMatch(sql -> sql.contains("ALGORITHM=INPLACE") && sql.contains("LOCK=NONE")));
  }

  @Test
  void testReconcile_fallsBackOnUnsupportedDDL() {
    List<String> executedDDL = new ArrayList<>();
    DynamicIndexManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA") && sql.contains("STATISTICS")) {
                return noRows();
              } else if (sql.contains("INFORMATION_SCHEMA") && sql.contains("COLUMNS")) {
                return columnRows("col1");
              } else if (sql.contains("ALGORITHM=INPLACE")) {
                throw new SQLException("ALGORITHM=INPLACE not supported");
              } else {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
            });

    IndexConfig config =
        createIndexConfig(createIndexDefinition("idx_test", List.of("col1"), true));

    DynamicIndexManager.ReconcileResult result = mgr.reconcile(config);

    assertEquals(1, result.created.size());
    // Fallback DDL should not contain ALGORITHM
    assertTrue(
        executedDDL.stream()
            .anyMatch(sql -> sql.contains("CREATE INDEX") && !sql.contains("ALGORITHM")));
  }

  // --- Unique index tests ---

  @Test
  void testReconcile_createsUniqueIndex() {
    List<String> executedDDL = new ArrayList<>();
    DynamicIndexManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA") && sql.contains("STATISTICS")) {
                return noRows();
              } else if (sql.contains("INFORMATION_SCHEMA") && sql.contains("COLUMNS")) {
                return columnRows("col1");
              } else {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
            });

    IndexConfig.IndexDefinition uniqueIdx =
        createIndexDefinition("idx_unique", List.of("col1"), true);
    uniqueIdx.setUnique(true);
    IndexConfig config = createIndexConfig(uniqueIdx);

    mgr.reconcile(config);

    assertTrue(executedDDL.stream().anyMatch(sql -> sql.contains("UNIQUE INDEX")));
  }

  // --- Composite index tests ---

  @Test
  void testReconcile_createsCompositeIndex() {
    List<String> executedDDL = new ArrayList<>();
    DynamicIndexManager mgr =
        managerWith(
            ctx -> {
              String sql = ctx.sql();
              if (sql.contains("INFORMATION_SCHEMA") && sql.contains("STATISTICS")) {
                return noRows();
              } else if (sql.contains("INFORMATION_SCHEMA") && sql.contains("COLUMNS")) {
                return columnRows("col1", "col2", "col3");
              } else {
                executedDDL.add(sql);
                return rowsAffected(0);
              }
            });

    IndexConfig config =
        createIndexConfig(
            createIndexDefinition("idx_composite", List.of("col1", "col2", "col3"), true));

    mgr.reconcile(config);

    assertTrue(executedDDL.stream().anyMatch(sql -> sql.contains("col1, col2, col3")));
  }

  // --- Statistics tests ---

  @Test
  void testGetStats_calculatesCorrectly() {
    DynamicIndexManager mgr =
        managerWith(
            infoSchemaIndexes("PRIMARY", "idx_consolidation", "idx_custom1", "idx_custom2"));

    DynamicIndexManager.IndexStats stats = mgr.getStats();

    assertEquals(4, stats.totalIndexes);
    assertEquals(3, stats.managedIndexes); // idx_consolidation, idx_custom1, idx_custom2
    assertEquals(2, stats.protectedIndexes); // PRIMARY, idx_consolidation
  }

  // ==========================================
  // Helper Methods
  // ==========================================

  private DynamicIndexManager managerWith(MockDataProvider provider) {
    DSLContext dsl = DSL.using(new MockConnection(provider), SQLDialect.MYSQL);
    return new DynamicIndexManager(dsl, TABLE_NAME);
  }

  /** Provider that returns index names from INFORMATION_SCHEMA.STATISTICS. */
  private MockDataProvider infoSchemaIndexes(String... indexNames) {
    return ctx -> {
      String sql = ctx.sql();
      if (sql.contains("INFORMATION_SCHEMA")) {
        return indexRows(indexNames);
      }
      return rowsAffected(0);
    };
  }

  private MockResult[] indexRows(String... names) {
    var field = DSL.field("INDEX_NAME", SQLDataType.VARCHAR);
    var result = DSL.using(SQLDialect.MYSQL).newResult(field);
    for (String name : names) {
      var record = DSL.using(SQLDialect.MYSQL).newRecord(field);
      record.set(field, name);
      result.add(record);
    }
    return new MockResult[] {new MockResult(result.size(), result)};
  }

  private MockResult[] columnRows(String... names) {
    var field = DSL.field("COLUMN_NAME", SQLDataType.VARCHAR);
    var result = DSL.using(SQLDialect.MYSQL).newResult(field);
    for (String name : names) {
      var record = DSL.using(SQLDialect.MYSQL).newRecord(field);
      record.set(field, name);
      result.add(record);
    }
    return new MockResult[] {new MockResult(result.size(), result)};
  }

  private MockResult[] noRows() {
    var field = DSL.field("dummy", SQLDataType.VARCHAR);
    var result = DSL.using(SQLDialect.MYSQL).newResult(field);
    return new MockResult[] {new MockResult(0, result)};
  }

  private MockResult[] rowsAffected(int count) {
    return new MockResult[] {new MockResult(count)};
  }

  private IndexConfig createIndexConfig(IndexConfig.IndexDefinition... definitions) {
    IndexConfig config = new IndexConfig();
    config.setIndexes(Arrays.asList(definitions));
    return config;
  }

  private IndexConfig.IndexDefinition createIndexDefinition(
      String name, List<String> columns, boolean enabled) {
    IndexConfig.IndexDefinition def = new IndexConfig.IndexDefinition();
    def.setName(name);
    def.setColumns(columns);
    def.setEnabled(enabled);
    return def;
  }
}
