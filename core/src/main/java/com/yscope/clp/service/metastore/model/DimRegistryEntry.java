package com.yscope.clp.service.metastore.model;

/**
 * A dimension column registry entry mapping a placeholder column name to field metadata.
 *
 * @param tableName the metadata table this entry belongs to
 * @param columnName placeholder column name (e.g., "dim_f01"), or for alias entries the target
 *     column (e.g., "clp_ir_storage_backend"); equals {@code aliasColumn} when this is an alias
 * @param baseType simplified type: str, str_utf8, bool, int, float
 * @param width current VARCHAR width (str/str_utf8 only, null for others)
 * @param dimKey original field name (may contain special chars like @, ., -)
 * @param aliasColumn if non-null, this entry aliases an existing column instead of allocating a new
 *     placeholder
 * @param status lifecycle state: ACTIVE, INVALIDATED, or AVAILABLE
 * @param createdAt epoch seconds when this entry was created
 * @param invalidatedAt epoch seconds when invalidated (null if active/available)
 */
public record DimRegistryEntry(
    String tableName,
    String columnName,
    String baseType,
    Integer width,
    String dimKey,
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
