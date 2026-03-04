package com.yscope.metalog.metastore.schema;

import com.yscope.metalog.metastore.schema.BaseSchemaValidator.SchemaValidationException;
import java.sql.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.impl.DataSourceConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Automatic schema evolution for dimension and aggregate columns.
 *
 * <p>When enabled, detects unrecognized {@code dim_*} and {@code agg_*} fields in metadata records
 * and automatically adds new columns to the table via the {@link ColumnRegistry}.
 *
 * <p>The column registry maps opaque placeholder column names ({@code dim_f01}, {@code agg_f01}) to
 * real field metadata via registry tables, solving special character limitations, column name
 * length limits, and enabling column recycling.
 *
 * <h3>Safety Features:</h3>
 *
 * <ul>
 *   <li>Disabled by default (explicit opt-in)
 *   <li>Uses MySQL online DDL with database-specific lock modes
 *   <li>MySQL: LOCK=NONE (zero-impact, allows concurrent reads and writes)
 *   <li>MariaDB: LOCK=SHARED (brief write pauses, allows concurrent reads)
 * </ul>
 */
public class SchemaEvolution {
  private static final Logger logger = LoggerFactory.getLogger(SchemaEvolution.class);

  // Configuration
  private final DSLContext dsl;
  private final String tableName;
  private final boolean enabled;

  // Column registry for placeholder-based schema evolution
  private final ColumnRegistry columnRegistry;

  // Cache of known columns (populated on startup)
  private final Set<String> knownColumns = ConcurrentHashMap.newKeySet();

  /**
   * Create a schema evolution service with a column registry.
   *
   * @param dsl jOOQ DSL context (can be null for testing)
   * @param tableName Target table name
   * @param enabled Whether auto-evolution is enabled
   * @param columnRegistry Column registry for placeholder names
   * @throws SchemaValidationException if required base columns are missing
   * @throws SQLException if database access fails
   */
  public SchemaEvolution(
      DSLContext dsl, String tableName, boolean enabled, ColumnRegistry columnRegistry)
      throws SchemaValidationException, SQLException {
    this.dsl = dsl;
    this.tableName = tableName;
    this.enabled = enabled;
    this.columnRegistry = columnRegistry;

    // Skip validation if no dsl (testing mode)
    if (dsl == null) {
      logger.info("Schema evolution service created without database connection (testing mode)");
      return;
    }

    // Validate base schema and discover existing columns
    // BaseSchemaValidator still uses DataSource — extract from jOOQ's connection provider
    DataSource dataSource =
        ((DataSourceConnectionProvider) dsl.configuration().connectionProvider()).dataSource();
    BaseSchemaValidator validator = new BaseSchemaValidator(dataSource, tableName);
    Set<String> discoveredColumns = validator.validate();

    // Populate known columns
    knownColumns.addAll(discoveredColumns);

    if (enabled) {
      logger.info(
          "Schema evolution enabled for table={}, discovered {} columns",
          tableName,
          discoveredColumns.size());
    } else {
      logger.info(
          "Schema evolution disabled for table={}, discovered {} columns",
          tableName,
          discoveredColumns.size());
    }
  }


  /** Check if a column exists in the schema. */
  public boolean columnExists(String columnName) {
    return knownColumns.contains(columnName.toLowerCase());
  }

  /**
   * Evolve schema using the column registry and return field-to-column mappings.
   *
   * <p>All new columns from the batch are registered and added in a single ALTER TABLE statement.
   *
   * @param dimSpecs map of dimKey -> DimSpec (baseType, width)
   * @return map of dimKey -> placeholder column name (e.g., "@timestamp" -> "dim_f01")
   * @throws SQLException if database operation fails
   * @throws IllegalStateException if no column registry is configured
   */
  public Map<String, String> evolveSchemaWithRegistry(Map<String, ColumnRegistry.DimSpec> dimSpecs)
      throws SQLException {
    if (columnRegistry == null) {
      throw new IllegalStateException("Column registry not configured");
    }
    return columnRegistry.resolveOrAllocateDims(dimSpecs);
  }

  /** Check if this service has a column registry configured. */
  public boolean hasColumnRegistry() {
    return columnRegistry != null;
  }

  /** Get the column registry (may be null). */
  public ColumnRegistry getColumnRegistry() {
    return columnRegistry;
  }

  /**
   * Evolve schema for aggregate fields using the column registry.
   *
   * <p>All new columns from the batch are registered and added in a single ALTER TABLE statement.
   *
   * @param aggSpecs map of compositeKey -> AggSpec
   * @return map of compositeKey -> placeholder column name (e.g., "GTE\0level\0error" -> "agg_f01")
   * @throws SQLException if database operation fails
   * @throws IllegalStateException if no column registry is configured
   */
  public Map<String, String> evolveAggsWithRegistry(Map<String, ColumnRegistry.AggSpec> aggSpecs)
      throws SQLException {
    if (columnRegistry == null) {
      throw new IllegalStateException("Column registry not configured");
    }
    return columnRegistry.resolveOrAllocateAggs(aggSpecs);
  }

  /**
   * Get all known dimension columns (columns starting with "dim_").
   *
   * @return Set of dimension column names that exist in the schema
   */
  public Set<String> getDimensionColumns() {
    Set<String> dimColumns = new HashSet<>();
    for (String col : knownColumns) {
      if (col.startsWith("dim_")) {
        dimColumns.add(col);
      }
    }
    return dimColumns;
  }

  /**
   * Get all known aggregate columns (columns starting with "agg_").
   *
   * @return Set of aggregate column names that exist in the schema
   */
  public Set<String> getAggColumns() {
    Set<String> aggColumns = new HashSet<>();
    for (String col : knownColumns) {
      if (col.startsWith("agg_")) {
        aggColumns.add(col);
      }
    }
    return aggColumns;
  }

  /** Check if schema evolution is enabled. */
  public boolean isEnabled() {
    return enabled;
  }

  /** Get all known column names. */
  public Set<String> getKnownColumns() {
    return Set.copyOf(knownColumns);
  }
}
