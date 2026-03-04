package com.yscope.clp.service.metastore.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates that the database schema contains all required base columns.
 *
 * <p>On startup, discovers the actual schema from INFORMATION_SCHEMA and verifies it is a superset
 * of the required base columns. If any required columns are missing, logs an error and throws an
 * exception to prevent the application from starting with an incompatible schema.
 *
 * <p>This ensures the base DDL has been applied before the application starts, while allowing
 * additional dimension columns to be added dynamically via {@link SchemaEvolution}.
 */
public class BaseSchemaValidator {
  private static final Logger logger = LoggerFactory.getLogger(BaseSchemaValidator.class);

  /**
   * Required base columns that must exist in the schema. These are defined in schema.sql and are
   * essential for core functionality.
   */
  public static final List<String> REQUIRED_COLUMNS =
      List.of(
          // Identity and time
          "id",
          "min_timestamp",
          "max_timestamp",
          "clp_archive_created_at",

          // IR storage location
          "clp_ir_storage_backend",
          "clp_ir_bucket",
          "clp_ir_path",

          // Archive storage location
          "clp_archive_storage_backend",
          "clp_archive_bucket",
          "clp_archive_path",

          // Lifecycle state
          "state",

          // Metrics
          "record_count",
          "raw_size_bytes",
          "clp_ir_size_bytes",
          "clp_archive_size_bytes",

          // Retention
          "retention_days",
          "expires_at");

  private final DataSource dataSource;
  private final String tableName;

  public BaseSchemaValidator(DataSource dataSource, String tableName) {
    this.dataSource = dataSource;
    this.tableName = tableName;
  }

  /**
   * Validates that the database schema contains all required base columns.
   *
   * @return Set of all discovered columns (superset of required columns)
   * @throws SchemaValidationException if required columns are missing
   * @throws SQLException if database access fails
   */
  public Set<String> validate() throws SchemaValidationException, SQLException {
    Set<String> discoveredColumns = discoverColumns();

    // Check for missing required columns
    Set<String> missingColumns = new HashSet<>();
    for (String required : REQUIRED_COLUMNS) {
      if (!discoveredColumns.contains(required.toLowerCase())) {
        missingColumns.add(required);
      }
    }

    if (!missingColumns.isEmpty()) {
      String errorMsg =
          String.format(
              "Schema validation failed for table '%s'. Missing required columns: %s. "
                  + "Please ensure the base schema (schema.sql) has been applied.",
              tableName, missingColumns);
      logger.error(errorMsg);
      throw new SchemaValidationException(errorMsg, missingColumns);
    }

    logger.info(
        "Schema validation passed for table '{}': {} columns discovered, {} required",
        tableName,
        discoveredColumns.size(),
        REQUIRED_COLUMNS.size());

    return discoveredColumns;
  }

  /** Discovers all columns in the table from INFORMATION_SCHEMA. */
  private Set<String> discoverColumns() throws SQLException {
    Set<String> columns = new HashSet<>();

    String sql =
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";

    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {

      stmt.setString(1, tableName);

      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          columns.add(rs.getString("COLUMN_NAME").toLowerCase());
        }
      }
    }

    if (columns.isEmpty()) {
      throw new SQLException("Table '" + tableName + "' not found or has no columns");
    }

    logger.debug("Discovered {} columns in table '{}'", columns.size(), tableName);
    return columns;
  }

  /** Exception thrown when schema validation fails. */
  public static class SchemaValidationException extends Exception {
    private final Set<String> missingColumns;

    public SchemaValidationException(String message, Set<String> missingColumns) {
      super(message);
      this.missingColumns = Set.copyOf(missingColumns);
    }

    public Set<String> getMissingColumns() {
      return missingColumns;
    }
  }
}
