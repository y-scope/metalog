package com.yscope.clp.service.metastore.schema;

import com.yscope.clp.service.common.config.db.DSLContextFactory;
import com.yscope.clp.service.common.config.db.DatabaseTypeDetector;
import com.yscope.clp.service.metastore.SqlIdentifiers;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Idempotently provisions a metadata table and its registry rows on first contact.
 *
 * <p>Called by {@link com.yscope.clp.service.coordinator.ingestion.BatchingWriter} when an ingest
 * request arrives for an unknown table. All operations are idempotent (CREATE TABLE IF NOT EXISTS,
 * INSERT IGNORE) so concurrent provisioners on multiple nodes are safe.
 *
 * <p>After provisioning the table, the normal coordinator fight-for-master mechanism will claim the
 * {@code _table_assignment} row and start the lifecycle (consolidation, partition maintenance,
 * etc.).
 *
 * <p>Schema reconciliation is also performed: old-style named columns (e.g., {@code
 * dim_str128_application_id}) left over from pre-registry schema versions are dropped, and stale
 * registry entries pointing to non-existent columns are freed back to AVAILABLE.
 */
public final class TableProvisioner {
  private static final Logger logger = LoggerFactory.getLogger(TableProvisioner.class);

  private static final int SKETCH_SLOT_COUNT = 32;
  private static final int PARTITION_LOOKAHEAD_DAYS = 7;
  private static final int PARTITION_CLEANUP_AGE_DAYS = 180;
  private static final long PARTITION_SPARSE_ROW_THRESHOLD = 1_000;

  /**
   * Old-style dim column names before the placeholder (dim_f01) naming convention. These columns
   * are dropped during schema reconciliation. Matches: dim_str{N}_, dim_str{N}utf8_, dim_int_,
   * dim_uint_, dim_bool_, dim_float_
   */
  private static final Pattern OLD_STYLE_DIM_COLUMN =
      Pattern.compile("^dim_(str\\d+(?:utf8)?|int|uint|bool|float)_.*");

  /**
   * Old-style agg column names before the placeholder (agg_f01) naming convention. These columns
   * are dropped during schema reconciliation. Matches: agg_int_, agg_float_, agg_{aggtype}_
   */
  private static final Pattern OLD_STYLE_AGG_COLUMN =
      Pattern.compile("^agg_(int|float|gt|gte|lt|lte|eq|sum|avg|min|max)_.*");

  private TableProvisioner() {}

  /**
   * Ensure that the physical table and all registry rows exist for the given table name.
   *
   * <p>Steps (all idempotent):
   *
   * <ol>
   *   <li>Validate the table name via {@link SqlIdentifiers#requireValidTableName(String)}
   *   <li>Create the physical table by cloning the {@code _clp_template} DDL from schema.sql
   *   <li>Reconcile schema: drop old-style columns, free stale registry entries
   *   <li>INSERT IGNORE into {@code _table}, {@code _table_config}, {@code _table_assignment}
   *   <li>Pre-populate 32 sketch slots in {@code _sketch_registry}
   *   <li>Create lookahead partitions via {@link PartitionManager#ensureLookaheadPartitions()}
   * </ol>
   *
   * @param dataSource shared connection pool
   * @param tableName name of the table to provision
   * @throws IllegalArgumentException if the table name fails validation
   * @throws SQLException if any DDL or DML statement fails
   */
  public static void ensureTable(DataSource dataSource, String tableName) throws SQLException {
    SqlIdentifiers.requireValidTableName(tableName);

    createPhysicalTable(dataSource, tableName);
    reconcileSchema(dataSource, tableName);
    insertRegistryRows(dataSource, tableName);
    prepopulateSketchSlots(dataSource, tableName);
    ensureLookaheadPartitions(dataSource, tableName);

    logger.info("Table '{}' provisioned (or already existed)", tableName);
  }

  /**
   * Clone the {@code _clp_template} CREATE TABLE statement from schema.sql, substituting the target
   * table name and adding compression for the detected database engine.
   *
   * <p>The base DDL in schema.sql has no compression clause — this method appends the
   * engine-specific syntax: {@code COMPRESSION='lz4'} for MySQL or {@code PAGE_COMPRESSED=1} for
   * MariaDB. This avoids maintaining a separate schema file per engine.
   */
  private static void createPhysicalTable(DataSource dataSource, String tableName)
      throws SQLException {
    boolean isMariaDb =
        DatabaseTypeDetector.detect(dataSource) == DatabaseTypeDetector.DatabaseType.MARIADB;
    String ddl = loadTemplateDdl();
    String compressionClause = isMariaDb ? "\n  PAGE_COMPRESSED=1" : "\n  COMPRESSION='lz4'";
    ddl = ddl.replace("COLLATE=utf8mb4_bin", "COLLATE=utf8mb4_bin" + compressionClause);
    String targetDdl = ddl.replace("_clp_template", tableName);

    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute(targetDdl);
    }

    logger.debug("Physical table '{}' ensured", tableName);
  }

  /**
   * Reconcile the physical table schema and registry entries.
   *
   * <p>Handles the transient period when a table already existed with an older schema:
   *
   * <ul>
   *   <li>Drops old-style named columns (e.g., {@code dim_str128_application_id}) that predate the
   *       placeholder ({@code dim_f01}) naming convention. These columns are unknown to the column
   *       registry and will never be used by the query engine.
   *   <li>Frees stale {@code _dim_registry} and {@code _agg_registry} entries (ACTIVE or
   *       INVALIDATED status) whose physical columns no longer exist in the table, setting them
   *       back to AVAILABLE so they can be reallocated.
   * </ul>
   */
  private static void reconcileSchema(DataSource dataSource, String tableName) throws SQLException {
    Set<String> physicalColumns = getTableColumns(dataSource, tableName);

    dropOldStyleColumns(dataSource, tableName, physicalColumns);

    // Re-fetch after drops so stale-entry check reflects the current physical state
    physicalColumns = getTableColumns(dataSource, tableName);
    freeStaleRegistryEntries(dataSource, tableName, physicalColumns);
  }

  /** Query all column names for the given table from INFORMATION_SCHEMA. */
  private static Set<String> getTableColumns(DataSource dataSource, String tableName)
      throws SQLException {
    Set<String> columns = new HashSet<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?")) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          columns.add(rs.getString(1));
        }
      }
    }
    return columns;
  }

  /**
   * Drop any old-style dim/agg columns that predate the placeholder naming convention.
   *
   * <p>Old-style columns (e.g., {@code dim_str128_application_id}) are never registered in {@code
   * _dim_registry} and are not used by the query engine. Dropping them reclaims storage and
   * prevents confusion during schema evolution.
   */
  private static void dropOldStyleColumns(
      DataSource dataSource, String tableName, Set<String> physicalColumns) throws SQLException {
    List<String> toDrop = new ArrayList<>();
    for (String col : physicalColumns) {
      if (OLD_STYLE_DIM_COLUMN.matcher(col).matches()
          || OLD_STYLE_AGG_COLUMN.matcher(col).matches()) {
        toDrop.add(col);
      }
    }

    if (toDrop.isEmpty()) {
      return;
    }

    // Build a single ALTER TABLE with multiple DROP COLUMN clauses for efficiency.
    // Column names are validated against physical table column names, safe to interpolate.
    StringBuilder sql = new StringBuilder("ALTER TABLE ").append(tableName);
    for (int i = 0; i < toDrop.size(); i++) {
      sql.append(i == 0 ? " DROP COLUMN " : ", DROP COLUMN ").append(toDrop.get(i));
    }

    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute(sql.toString());
    }

    logger.info("Dropped {} old-style column(s) from '{}': {}", toDrop.size(), tableName, toDrop);
  }

  /**
   * Mark ACTIVE/INVALIDATED registry entries as AVAILABLE when the physical column is absent.
   *
   * <p>This can happen when:
   *
   * <ul>
   *   <li>A table was recreated with a fresh schema but the registry still has old entries
   *   <li>A column was manually dropped outside the normal registry lifecycle
   * </ul>
   *
   * <p>Freeing these slots allows the column registry to reallocate them for new fields without
   * attempting ALTER TABLE ADD COLUMN on already-missing columns.
   */
  private static void freeStaleRegistryEntries(
      DataSource dataSource, String tableName, Set<String> physicalColumns) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      int dimFreed = freeStaleEntries(conn, "_dim_registry", tableName, physicalColumns);
      int aggFreed = freeStaleEntries(conn, "_agg_registry", tableName, physicalColumns);
      if (dimFreed + aggFreed > 0) {
        logger.info(
            "Freed {} stale dim + {} stale agg registry slots for '{}'",
            dimFreed,
            aggFreed,
            tableName);
      }
    }
  }

  private static int freeStaleEntries(
      Connection conn, String registryTable, String tableName, Set<String> physicalColumns)
      throws SQLException {
    // Fetch all non-AVAILABLE entries for this table
    List<String> staleColumns = new ArrayList<>();
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT column_name FROM "
                + registryTable
                + " WHERE table_name = ? AND status != 'AVAILABLE'")) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String col = rs.getString(1);
          if (!physicalColumns.contains(col)) {
            staleColumns.add(col);
          }
        }
      }
    }

    if (staleColumns.isEmpty()) {
      return 0;
    }

    // Reset stale entries to AVAILABLE
    String placeholders = staleColumns.stream().map(c -> "?").collect(Collectors.joining(", "));
    String updateSql =
        "UPDATE "
            + registryTable
            + " SET status = 'AVAILABLE', invalidated_at = NULL"
            + " WHERE table_name = ? AND column_name IN ("
            + placeholders
            + ")";
    try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
      ps.setString(1, tableName);
      for (int i = 0; i < staleColumns.size(); i++) {
        ps.setString(i + 2, staleColumns.get(i));
      }
      return ps.executeUpdate();
    }
  }

  /** Load the _clp_template CREATE TABLE statement from schema.sql on the classpath. */
  private static String loadTemplateDdl() throws SQLException {
    String schemaFile = "schema/schema.sql";
    try (InputStream is = TableProvisioner.class.getClassLoader().getResourceAsStream(schemaFile)) {
      if (is == null) {
        throw new SQLException(schemaFile + " not found on classpath");
      }

      String sql;
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        // Strip SQL line comments to avoid splitting on semicolons inside comments
        sql =
            reader
                .lines()
                .filter(line -> !line.stripLeading().startsWith("--"))
                .collect(Collectors.joining("\n"));
      }

      // Split into individual statements and find the _clp_template CREATE TABLE
      String[] statements = sql.split(";");
      for (String stmt : statements) {
        String trimmed = stmt.trim();
        if (trimmed.contains("CREATE TABLE IF NOT EXISTS _clp_template")) {
          return trimmed;
        }
      }

      throw new SQLException("CREATE TABLE IF NOT EXISTS _clp_template not found in " + schemaFile);
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException("Failed to load _clp_template DDL from " + schemaFile, e);
    }
  }

  /** INSERT IGNORE into _table, _table_config, and _table_assignment. */
  private static void insertRegistryRows(DataSource dataSource, String tableName)
      throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      // _table: identity row
      try (PreparedStatement ps =
          conn.prepareStatement(
              "INSERT IGNORE INTO _table (table_name, display_name) VALUES (?, ?)")) {
        ps.setString(1, tableName);
        ps.setString(2, tableName);
        ps.executeUpdate();
      }

      // _table_config: DB defaults apply; kafka_poller_enabled set to FALSE for gRPC-only tables
      try (PreparedStatement ps =
          conn.prepareStatement(
              "INSERT IGNORE INTO _table_config (table_name, kafka_poller_enabled)"
                  + " VALUES (?, FALSE)")) {
        ps.setString(1, tableName);
        ps.executeUpdate();
      }

      // _table_assignment: node_id NULL so fight-for-master can claim it
      try (PreparedStatement ps =
          conn.prepareStatement("INSERT IGNORE INTO _table_assignment (table_name) VALUES (?)")) {
        ps.setString(1, tableName);
        ps.executeUpdate();
      }
    }

    logger.debug("Registry rows ensured for table '{}'", tableName);
  }

  /** Pre-populate 32 AVAILABLE sketch slots in _sketch_registry. */
  private static void prepopulateSketchSlots(DataSource dataSource, String tableName)
      throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "INSERT IGNORE INTO _sketch_registry (table_name, set_member, status)"
                    + " VALUES (?, ?, 'AVAILABLE')")) {
      for (int i = 1; i <= SKETCH_SLOT_COUNT; i++) {
        ps.setString(1, tableName);
        ps.setString(2, String.format("s%02d", i));
        ps.addBatch();
      }
      ps.executeBatch();
    }

    logger.debug("Sketch slots ensured for table '{}'", tableName);
  }

  /** Create lookahead partitions so new data lands in the right partition immediately. */
  private static void ensureLookaheadPartitions(DataSource dataSource, String tableName)
      throws SQLException {
    PartitionManager pm =
        new PartitionManager(
            DSLContextFactory.create(dataSource),
            tableName,
            PARTITION_LOOKAHEAD_DAYS,
            PARTITION_CLEANUP_AGE_DAYS,
            PARTITION_SPARSE_ROW_THRESHOLD);
    int created = pm.ensureLookaheadPartitions();
    logger.debug("Ensured {} lookahead partition(s) for table '{}'", created, tableName);
  }
}
