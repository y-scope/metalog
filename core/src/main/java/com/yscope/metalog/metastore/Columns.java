package com.yscope.metalog.metastore;

import java.util.List;
import java.util.Locale;

/**
 * Column name constants for IR file metadata tables.
 *
 * <p>Centralizes all column names to prevent typos and simplify refactoring. Used by {@link
 * JooqFields} and repository classes.
 *
 * <h2>Key Formats</h2>
 *
 * <p>Dimension and aggregation keys use slash-delimited format:
 *
 * <ul>
 *   <li>Dimensions: {@code dim/{baseType}{width}/{field}} (e.g., {@code dim/str128/application_id})
 *   <li>Integer aggregations: {@code agg_int/{aggType}/{field}/{qualifier}} (e.g., {@code
 *       agg_int/gte/level/debug})
 *   <li>Float aggregations: {@code agg_float/{aggType}/{field}/{qualifier}} (e.g., {@code
 *       agg_float/avg/latency/p99})
 * </ul>
 *
 * <h2>Benefits</h2>
 *
 * <ul>
 *   <li>Compile-time checking (typos caught by compiler)
 *   <li>IDE autocomplete
 *   <li>Single source of truth for schema
 *   <li>Easy refactoring (rename column everywhere)
 *   <li>Automatic lowercase normalization prevents duplicate columns
 * </ul>
 *
 * @see #dim(String, int, String)
 * @see #aggInt(String, String, String)
 * @see #aggFloat(String, String, String)
 */
public final class Columns {

  private Columns() {
    // Utility class, prevent instantiation
  }

  // ===================
  // IR Location
  // ===================

  public static final String IR_STORAGE_BACKEND = "clp_ir_storage_backend";
  public static final String IR_BUCKET = "clp_ir_bucket";
  public static final String IR_PATH = "clp_ir_path";

  public static final List<String> IR_LOCATION = List.of(IR_STORAGE_BACKEND, IR_BUCKET, IR_PATH);

  // ===================
  // Archive Location
  // ===================

  public static final String ARCHIVE_STORAGE_BACKEND = "clp_archive_storage_backend";
  public static final String ARCHIVE_BUCKET = "clp_archive_bucket";
  public static final String ARCHIVE_PATH = "clp_archive_path";

  public static final List<String> ARCHIVE_LOCATION =
      List.of(ARCHIVE_STORAGE_BACKEND, ARCHIVE_BUCKET, ARCHIVE_PATH);

  // ===================
  // VIRTUAL Hash Columns (for indexed lookups)
  // These are VIRTUAL (generated) columns: computed on read, stored only in index.
  // Compatible with both MySQL 5.7+ and MariaDB 10.2+.
  // ===================

  /**
   * VIRTUAL column for IR path hash (MD5). Use for indexed lookups instead of clp_ir_path directly.
   * Query pattern: WHERE clp_ir_path_hash = UNHEX(MD5(?))
   */
  public static final String IR_PATH_HASH = "clp_ir_path_hash";

  /**
   * VIRTUAL column for archive path hash (MD5). Use for indexed lookups instead of clp_archive_path
   * directly. Query pattern: WHERE clp_archive_path_hash = UNHEX(MD5(?))
   */
  public static final String ARCHIVE_PATH_HASH = "clp_archive_path_hash";

  // ===================
  // State
  // ===================

  public static final String STATE = "state";

  // ===================
  // Event Time Bounds
  // ===================

  public static final String MIN_TIMESTAMP = "min_timestamp";
  public static final String MAX_TIMESTAMP = "max_timestamp";
  public static final String ARCHIVE_CREATED_AT = "clp_archive_created_at";

  public static final List<String> TIME_BOUNDS =
      List.of(MIN_TIMESTAMP, MAX_TIMESTAMP, ARCHIVE_CREATED_AT);

  // ===================
  // Record Count
  // ===================

  public static final String RECORD_COUNT = "record_count";

  // ===================
  // Sizes
  // ===================

  public static final String RAW_SIZE_BYTES = "raw_size_bytes";
  public static final String IR_SIZE_BYTES = "clp_ir_size_bytes";
  public static final String ARCHIVE_SIZE_BYTES = "clp_archive_size_bytes";

  public static final List<String> SIZES =
      List.of(RAW_SIZE_BYTES, IR_SIZE_BYTES, ARCHIVE_SIZE_BYTES);

  // ===================
  // Level aggregation keys (GTE = "greater than or equal" = this level and above).
  // These are FileRecord.counts map keys, not DB column names — level counts are
  // dynamically allocated as agg_fNN placeholders via _agg_registry.
  public static final String AGG_INT_GTE_LEVEL_DEBUG = "agg_int/gte/level/debug";
  public static final String AGG_INT_GTE_LEVEL_INFO = "agg_int/gte/level/info";
  public static final String AGG_INT_GTE_LEVEL_WARN = "agg_int/gte/level/warn";
  public static final String AGG_INT_GTE_LEVEL_ERROR = "agg_int/gte/level/error";
  public static final String AGG_INT_GTE_LEVEL_FATAL = "agg_int/gte/level/fatal";

  // ===================
  // Retention
  // ===================

  public static final String RETENTION_DAYS = "retention_days";
  public static final String EXPIRES_AT = "expires_at";

  public static final List<String> RETENTION = List.of(RETENTION_DAYS, EXPIRES_AT);

  // ===================
  // Sketches and Extension Data (Optional, NULL for most rows)
  // Used for split pruning only, NOT for early termination
  // ===================

  /**
   * SET column indicating which fields have data sketches (bloom/cuckoo) in ext. NULL for most rows
   * (no sketches); non-NULL when sketches are available. Query pattern: sketches IS NOT NULL AND
   * FIND_IN_SET('field', sketches) > 0
   */
  public static final String SKETCHES = "sketches";

  /**
   * Msgpack-encoded extension data. Use sparingly—NULL for most rows. Primary use case: sketch data
   * for split pruning. Cross-database compatible (MySQL and MariaDB).
   */
  public static final String EXT = "ext";

  // ===================
  // Column Registry Placeholder Helpers
  // Used by ColumnRegistry to generate opaque column names (dim_f01, agg_f01)
  // ===================

  /**
   * Generate a dimension placeholder column name for the given slot number.
   *
   * @param slot 1-based slot number
   * @return placeholder name (e.g., "dim_f01", "dim_f12")
   */
  public static String dimPlaceholder(int slot) {
    if (slot < 1 || slot > 999) {
      throw new IllegalArgumentException("Slot must be 1-999, got: " + slot);
    }
    return String.format("dim_f%02d", slot);
  }

  /**
   * Generate an agg placeholder column name for the given slot number.
   *
   * @param slot 1-based slot number
   * @return placeholder name (e.g., "agg_f01", "agg_f12")
   */
  public static String aggPlaceholder(int slot) {
    if (slot < 1 || slot > 999) {
      throw new IllegalArgumentException("Slot must be 1-999, got: " + slot);
    }
    return String.format("agg_f%02d", slot);
  }

  // ===================
  // Dimension Helpers
  // Naming convention: dim/{baseType}{width}/{field}
  // ===================

  /**
   * Build dimension key from base type, width, and field name.
   *
   * <p>Input is normalized to lowercase to ensure consistent keys regardless of input casing.
   *
   * @param baseType Base type: "str", "int", "float", "bool", etc.
   * @param width Type width (e.g., 128 for str128, 32 for int32)
   * @param field Field name (e.g., "application_id", "region")
   * @return Slash-delimited key (e.g., "dim/str128/application_id")
   */
  public static String dim(String baseType, int width, String field) {
    baseType = baseType.toLowerCase(Locale.ROOT);
    field = field.toLowerCase(Locale.ROOT);

    if (!baseType.matches("str|int|uint|float")) {
      throw new IllegalArgumentException(
          "Invalid dimension base type: "
              + baseType
              + " (must be str, int, uint, or float)");
    }
    if (width <= 0) {
      throw new IllegalArgumentException("Width must be > 0, got: " + width);
    }
    if (!field.matches("[a-z][a-z0-9_]{0,62}")) {
      throw new IllegalArgumentException(
          "Invalid dimension field: " + field + " (must match [a-z][a-z0-9_]{0,62})");
    }
    return "dim/" + baseType + width + "/" + field;
  }

  /**
   * Build string dimension key with width.
   *
   * @param maxLength Maximum string length (e.g., 128 for VARCHAR(128))
   * @param field Field name
   * @return Slash-delimited key (e.g., "dim/str128/application_id")
   */
  public static String dimStr(int maxLength, String field) {
    return dim("str", maxLength, field);
  }

  // ===================
  // Aggregation Helpers
  // Naming convention: agg_int/{aggType}/{field}/{qualifier}
  //                    agg_float/{aggType}/{field}/{qualifier}
  // ===================

  /**
   * Build integer aggregation key.
   *
   * <p>Input is normalized to lowercase to ensure consistent keys.
   *
   * @param aggType Aggregation type: "gte", "gt", "lte", "lt", "eq", "sum", "avg", "min", "max"
   * @param field Field being aggregated (e.g., "level")
   * @param qualifier Specific qualifier (e.g., "debug", "error")
   * @return Slash-delimited key (e.g., "agg_int/gte/level/debug")
   */
  public static String aggInt(String aggType, String field, String qualifier) {
    aggType = aggType.toLowerCase(Locale.ROOT);
    field = field.toLowerCase(Locale.ROOT);
    qualifier = qualifier.toLowerCase(Locale.ROOT);
    return "agg_int/" + aggType + "/" + field + "/" + qualifier;
  }

  /**
   * Build float aggregation key.
   *
   * <p>Input is normalized to lowercase to ensure consistent keys.
   *
   * @param aggType Aggregation type: "avg", "sum", "min", "max", etc.
   * @param field Field being aggregated (e.g., "latency")
   * @param qualifier Specific qualifier (e.g., "p99", "avg")
   * @return Slash-delimited key (e.g., "agg_float/avg/latency/p99")
   */
  public static String aggFloat(String aggType, String field, String qualifier) {
    aggType = aggType.toLowerCase(Locale.ROOT);
    field = field.toLowerCase(Locale.ROOT);
    qualifier = qualifier.toLowerCase(Locale.ROOT);
    return "agg_float/" + aggType + "/" + field + "/" + qualifier;
  }

  // ===================
  // All Columns
  // ===================

  /**
   * All standard database columns (excludes dynamic dim/agg placeholders and incoming-data map
   * keys). The {@code AGG_INT_GTE_LEVEL_*} constants above are FileRecord map keys, not DB
   * columns, and are therefore excluded here.
   */
  public static final List<String> ALL_STANDARD =
      List.of(
          IR_STORAGE_BACKEND,
          IR_BUCKET,
          IR_PATH,
          ARCHIVE_STORAGE_BACKEND,
          ARCHIVE_BUCKET,
          ARCHIVE_PATH,
          STATE,
          MIN_TIMESTAMP,
          MAX_TIMESTAMP,
          ARCHIVE_CREATED_AT,
          RECORD_COUNT,
          RAW_SIZE_BYTES,
          IR_SIZE_BYTES,
          ARCHIVE_SIZE_BYTES,
          RETENTION_DAYS,
          EXPIRES_AT);
}
