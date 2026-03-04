package com.yscope.metalog.metastore.model;

/**
 * An aggregate column registry entry mapping a placeholder column name to field metadata.
 *
 * @param tableName the metadata table this entry belongs to
 * @param columnName placeholder column name (e.g., "agg_f01") or aliased column (e.g.,
 *     "record_count")
 * @param aggKey field name being aggregated (e.g., "level")
 * @param aggValue value qualifier (e.g., "error"), null for total aggregates
 * @param aggregationType the aggregation comparison/function type
 * @param valueType physical column value type (INT or FLOAT)
 * @param aliasColumn if non-null, this entry aliases an existing column instead of allocating a new
 *     placeholder
 * @param status lifecycle state: ACTIVE, INVALIDATED, or AVAILABLE
 * @param createdAt epoch seconds when this entry was created
 * @param invalidatedAt epoch seconds when invalidated (null if active/available)
 */
public record AggRegistryEntry(
    String tableName,
    String columnName,
    String aggKey,
    String aggValue,
    AggregationType aggregationType,
    AggValueType valueType,
    String aliasColumn,
    String status,
    Long createdAt,
    Long invalidatedAt) {

  public boolean isActive() {
    return "ACTIVE".equals(status);
  }

  /** Returns true if this entry aliases an existing column rather than an allocated placeholder. */
  public boolean isAlias() {
    return aliasColumn != null;
  }

  /** Returns the effective physical column name (alias target or placeholder). */
  public String effectiveColumn() {
    return aliasColumn != null ? aliasColumn : columnName;
  }
}
