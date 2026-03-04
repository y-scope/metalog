package com.yscope.clp.service.metastore.schema;

import com.yscope.clp.service.common.config.IndexConfig;
import com.yscope.clp.service.common.config.IndexConfig.IndexDefinition;
import com.yscope.clp.service.metastore.SqlIdentifiers;
import java.sql.*;
import java.util.*;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages dynamic indexes on the metadata table based on YAML configuration.
 *
 * <p>Responsibilities:
 *
 * <ul>
 *   <li>Create indexes that are enabled in config but missing from database
 *   <li>Drop indexes that are disabled in config
 *   <li>Uses online DDL (ALGORITHM=INPLACE, LOCK=NONE) for minimal disruption
 *   <li>Tracks managed indexes to avoid dropping hardcoded/system indexes
 * </ul>
 *
 * <p>Thread safety: This class is designed to be called from the main thread during configuration
 * reload or from a maintenance thread.
 */
public class DynamicIndexManager {
  private static final Logger logger = LoggerFactory.getLogger(DynamicIndexManager.class);

  // Prefix for indexes managed by this class
  private static final String MANAGED_INDEX_PREFIX = "idx_";

  // Indexes that should never be dropped (hardcoded/system indexes)
  private static final Set<String> PROTECTED_INDEXES =
      Set.of(
          "PRIMARY",
          "idx_consolidation", // Core index for pending file queries
          "idx_expiration" // Core index for retention/deletion queries
          );

  private final DSLContext dsl;
  private final String tableName;
  private final Set<String> managedIndexes;

  /**
   * Create a DynamicIndexManager.
   *
   * @param dsl jOOQ DSL context
   * @param tableName Name of the table to manage indexes for
   */
  public DynamicIndexManager(DSLContext dsl, String tableName) {
    this.dsl = dsl;
    this.tableName = SqlIdentifiers.requireValidTableName(tableName);
    this.managedIndexes = new HashSet<>();
  }



  /**
   * Reconcile indexes with the given configuration.
   *
   * <p>This method:
   *
   * <ol>
   *   <li>Creates indexes that are enabled but don't exist
   *   <li>Drops indexes that are disabled and exist
   *   <li>Leaves protected indexes untouched
   * </ol>
   *
   * @param indexConfig The desired index configuration
   * @return Result of the reconciliation
   */
  public ReconcileResult reconcile(IndexConfig indexConfig) {
    ReconcileResult result = new ReconcileResult();

    try {
      // Get current indexes from database
      Set<String> existingIndexes = getExistingIndexes();

      // Get existing columns for validation
      Set<String> existingColumns = getExistingColumns();

      // Process enabled indexes (create if missing)
      for (IndexDefinition definition : indexConfig.getEnabledIndexes()) {
        if (!existingIndexes.contains(definition.getName())) {
          // Check if all columns exist before creating index
          List<String> missingColumns =
              definition.getColumns().stream()
                  .filter(col -> !existingColumns.contains(col.toLowerCase()))
                  .toList();

          if (!missingColumns.isEmpty()) {
            logger.warn(
                "Skipping index {} - columns not yet created: {}",
                definition.getName(),
                missingColumns);
            result.skipped.add(definition.getName());
            continue;
          }

          try {
            createIndex(definition);
            managedIndexes.add(definition.getName());
            result.created.add(definition.getName());
          } catch (DataAccessException e) {
            logger.error("Failed to create index {}", definition.getName(), e);
            result.errors.put(definition.getName(), e.getMessage());
          }
        } else {
          // Index exists, mark as managed
          managedIndexes.add(definition.getName());
        }
      }

      // Process disabled indexes (drop if exists and managed)
      for (IndexDefinition definition : indexConfig.getDisabledIndexes()) {
        if (existingIndexes.contains(definition.getName())) {
          if (isProtectedIndex(definition.getName())) {
            logger.warn("Cannot drop protected index {}", definition.getName());
            result.skipped.add(definition.getName());
          } else {
            try {
              dropIndex(definition.getName());
              managedIndexes.remove(definition.getName());
              result.dropped.add(definition.getName());
            } catch (DataAccessException e) {
              logger.error("Failed to drop index {}", definition.getName(), e);
              result.errors.put(definition.getName(), e.getMessage());
            }
          }
        }
      }

      if (!result.created.isEmpty() || !result.dropped.isEmpty() || !result.errors.isEmpty()) {
        logger.info(
            "Index reconciliation: created={}, dropped={}, skipped={}, errors={}",
            result.created.size(),
            result.dropped.size(),
            result.skipped.size(),
            result.errors.size());
      } else {
        logger.debug("Index reconciliation: no changes (skipped={})", result.skipped.size());
      }

    } catch (DataAccessException e) {
      logger.error("Error during index reconciliation", e);
      result.errors.put("_general", e.getMessage());
    }

    return result;
  }

  /** Validate that an index name contains only safe characters for SQL identifiers. */
  private static void requireValidIndexName(String name) {
    if (name == null || !name.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
      throw new IllegalArgumentException("Invalid index name: " + name);
    }
  }

  /** Create an index using online DDL. DDL stays as plain SQL (jOOQ has no ALGORITHM/LOCK). */
  private void createIndex(IndexDefinition definition) {
    requireValidIndexName(definition.getName());

    StringBuilder sql = new StringBuilder();
    sql.append("CREATE ");

    if (definition.isUnique()) {
      sql.append("UNIQUE ");
    }

    sql.append("INDEX ");
    sql.append(definition.getName());
    sql.append(" ON ");
    sql.append(tableName);
    sql.append(" (");
    sql.append(definition.getColumnList());
    sql.append(")");

    // Use online DDL for minimal disruption
    sql.append(" ALGORITHM=INPLACE, LOCK=NONE");

    try {
      logger.info("Creating index: {}", definition.getName());
      dsl.execute(sql.toString());
      logger.debug("Index {} created successfully", definition.getName());
    } catch (DataAccessException e) {
      if (causeContains(e, "ALGORITHM=INPLACE") || causeContains(e, "LOCK=NONE")) {
        logger.warn(
            "Online DDL not supported for index {}, falling back to default", definition.getName());
        createIndexWithoutOnlineDDL(definition);
      } else if (causeContains(e, "Duplicate key name")) {
        logger.debug("Index {} already exists", definition.getName());
      } else {
        throw e;
      }
    }
  }

  /** Create an index without online DDL (fallback). */
  private void createIndexWithoutOnlineDDL(IndexDefinition definition) {
    StringBuilder sql = new StringBuilder();
    sql.append("CREATE ");

    if (definition.isUnique()) {
      sql.append("UNIQUE ");
    }

    sql.append("INDEX ");
    sql.append(definition.getName());
    sql.append(" ON ");
    sql.append(tableName);
    sql.append(" (");
    sql.append(definition.getColumnList());
    sql.append(")");

    dsl.execute(sql.toString());
    logger.debug("Index {} created (without online DDL)", definition.getName());
  }

  /** Drop an index using online DDL. DDL stays as plain SQL (jOOQ has no ALGORITHM/LOCK). */
  private void dropIndex(String indexName) {
    requireValidIndexName(indexName);
    String sql =
        String.format("DROP INDEX %s ON %s ALGORITHM=INPLACE, LOCK=NONE", indexName, tableName);

    try {
      logger.info("Dropping index: {}", indexName);
      dsl.execute(sql);
      logger.debug("Index {} dropped successfully", indexName);
    } catch (DataAccessException e) {
      if (causeContains(e, "ALGORITHM=INPLACE") || causeContains(e, "LOCK=NONE")) {
        logger.warn("Online DDL not supported for dropping index {}, using default", indexName);
        dropIndexWithoutOnlineDDL(indexName);
      } else if (causeContains(e, "doesn't exist") || causeContains(e, "Can't DROP")) {
        logger.debug("Index {} already dropped", indexName);
      } else {
        throw e;
      }
    }
  }

  /** Drop an index without online DDL (fallback). */
  private void dropIndexWithoutOnlineDDL(String indexName) {
    String sql = String.format("DROP INDEX %s ON %s", indexName, tableName);
    dsl.execute(sql);
    logger.debug("Index {} dropped (without online DDL)", indexName);
  }

  /** Get all existing indexes on the table. */
  public Set<String> getExistingIndexes() {
    var indexName = DSL.field(DSL.name("INDEX_NAME"), SQLDataType.VARCHAR);
    var tableSchema = DSL.field(DSL.name("TABLE_SCHEMA"), SQLDataType.VARCHAR);
    var tblName = DSL.field(DSL.name("TABLE_NAME"), SQLDataType.VARCHAR);

    var rows =
        dsl.selectDistinct(indexName)
            .from(DSL.table(DSL.name("INFORMATION_SCHEMA", "STATISTICS")))
            .where(tableSchema.eq(DSL.field("DATABASE()", SQLDataType.VARCHAR)))
            .and(tblName.eq(tableName))
            .fetch();

    Set<String> indexes = new HashSet<>();
    for (var r : rows) {
      indexes.add(r.value1());
    }
    return indexes;
  }

  /** Get all existing columns on the table (lowercase for case-insensitive matching). */
  public Set<String> getExistingColumns() {
    var columnName = DSL.field(DSL.name("COLUMN_NAME"), SQLDataType.VARCHAR);
    var tableSchema = DSL.field(DSL.name("TABLE_SCHEMA"), SQLDataType.VARCHAR);
    var tblName = DSL.field(DSL.name("TABLE_NAME"), SQLDataType.VARCHAR);

    var rows =
        dsl.select(columnName)
            .from(DSL.table(DSL.name("INFORMATION_SCHEMA", "COLUMNS")))
            .where(tableSchema.eq(DSL.field("DATABASE()", SQLDataType.VARCHAR)))
            .and(tblName.eq(tableName))
            .fetch();

    Set<String> columns = new HashSet<>();
    for (var r : rows) {
      columns.add(r.value1().toLowerCase());
    }
    return columns;
  }

  /**
   * Check if the root database error message contains a substring. Checks the cause's message first
   * (the original SQLException), falling back to the wrapper message. This avoids false matches
   * against SQL text that jOOQ includes in the wrapper DataAccessException message.
   */
  private static boolean causeContains(DataAccessException e, String substring) {
    Throwable cause = e.getCause();
    if (cause != null) {
      String causeMsg = cause.getMessage();
      if (causeMsg != null) {
        return causeMsg.contains(substring);
      }
    }
    // Fallback: check wrapper message (for cases where there is no cause)
    String msg = e.getMessage();
    return msg != null && msg.contains(substring);
  }

  /** Check if an index is protected (should not be dropped). */
  private boolean isProtectedIndex(String indexName) {
    return PROTECTED_INDEXES.contains(indexName);
  }

  /** Get the set of managed indexes. */
  public Set<String> getManagedIndexes() {
    return Collections.unmodifiableSet(managedIndexes);
  }

  /** Get index statistics. */
  public IndexStats getStats() {
    IndexStats stats = new IndexStats();
    Set<String> existing = getExistingIndexes();

    stats.totalIndexes = existing.size();
    stats.managedIndexes =
        (int) existing.stream().filter(name -> name.startsWith(MANAGED_INDEX_PREFIX)).count();
    stats.protectedIndexes = (int) existing.stream().filter(this::isProtectedIndex).count();

    return stats;
  }

  /** Result of index reconciliation. */
  public static class ReconcileResult {
    public List<String> created = new ArrayList<>();
    public List<String> dropped = new ArrayList<>();
    public List<String> skipped = new ArrayList<>();
    public Map<String, String> errors = new HashMap<>();

    public boolean hasErrors() {
      return !errors.isEmpty();
    }

    @Override
    public String toString() {
      return String.format(
          "ReconcileResult{created=%d, dropped=%d, skipped=%d, errors=%d}",
          created.size(), dropped.size(), skipped.size(), errors.size());
    }
  }

  /** Index statistics. */
  public static class IndexStats {
    public int totalIndexes;
    public int managedIndexes;
    public int protectedIndexes;

    @Override
    public String toString() {
      return String.format(
          "IndexStats{total=%d, managed=%d, protected=%d}",
          totalIndexes, managedIndexes, protectedIndexes);
    }
  }
}
