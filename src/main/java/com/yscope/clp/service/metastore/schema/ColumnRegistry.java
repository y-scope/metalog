package com.yscope.clp.service.metastore.schema;

import static com.yscope.clp.service.metastore.JooqFields.*;

import com.yscope.clp.service.metastore.Columns;
import com.yscope.clp.service.metastore.model.AggRegistryEntry;
import com.yscope.clp.service.metastore.model.AggValueType;
import com.yscope.clp.service.metastore.model.AggregationType;
import com.yscope.clp.service.metastore.model.DimRegistryEntry;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Column registry that maps opaque placeholder column names ({@code dim_f01}, {@code agg_f01}) to
 * field metadata (logical key, base type, width, aggregation type, etc.).
 *
 * <p>Metadata is stored in per-table registry tables ({@code _dim_registry}, {@code _agg_registry})
 * and cached in-memory for fast lookup during ingestion and query resolution.
 *
 * <h3>Thread Safety</h3>
 *
 * <p>The in-memory caches use {@link ConcurrentHashMap}. Allocation of new slots is synchronized on
 * the registry instance to prevent duplicate slot assignment.
 */
public class ColumnRegistry {
  private static final Logger logger = LoggerFactory.getLogger(ColumnRegistry.class);

  // jOOQ field references for registry tables
  private static final org.jooq.Field<String> REG_TABLE_NAME =
      DSL.field(DSL.name("table_name"), String.class);
  private static final org.jooq.Field<String> REG_COLUMN_NAME =
      DSL.field(DSL.name("column_name"), String.class);
  private static final org.jooq.Field<String> REG_BASE_TYPE =
      DSL.field(DSL.name("base_type"), String.class);
  private static final org.jooq.Field<Integer> REG_WIDTH =
      DSL.field(DSL.name("width"), Integer.class);
  private static final org.jooq.Field<String> REG_DIM_KEY =
      DSL.field(DSL.name("dim_key"), String.class);
  private static final org.jooq.Field<String> REG_STATUS =
      DSL.field(DSL.name("status"), String.class);
  private static final org.jooq.Field<Long> REG_CREATED_AT =
      DSL.field(DSL.name("created_at"), Long.class);
  private static final org.jooq.Field<Long> REG_INVALIDATED_AT =
      DSL.field(DSL.name("invalidated_at"), Long.class);
  private static final org.jooq.Field<String> REG_AGG_KEY =
      DSL.field(DSL.name("agg_key"), String.class);
  private static final org.jooq.Field<String> REG_AGG_VALUE =
      DSL.field(DSL.name("agg_value"), String.class);
  private static final org.jooq.Field<String> REG_AGGREGATION_TYPE =
      DSL.field(DSL.name("aggregation_type"), String.class);
  private static final org.jooq.Field<String> REG_VALUE_TYPE =
      DSL.field(DSL.name("value_type"), String.class);
  private static final org.jooq.Field<String> REG_ALIAS_COLUMN =
      DSL.field(DSL.name("alias_column"), String.class);

  private final DSLContext dsl;
  private final String tableName;

  // dimKey -> DimRegistryEntry (ACTIVE entries only)
  private final ConcurrentHashMap<String, DimRegistryEntry> dimByKey = new ConcurrentHashMap<>();
  // columnName -> DimRegistryEntry (ACTIVE entries only)
  private final ConcurrentHashMap<String, DimRegistryEntry> dimByColumn = new ConcurrentHashMap<>();

  // composite key "aggregationType\0aggKey\0aggValue" -> AggRegistryEntry
  private final ConcurrentHashMap<String, AggRegistryEntry> aggByKey = new ConcurrentHashMap<>();
  // columnName -> AggRegistryEntry
  private final ConcurrentHashMap<String, AggRegistryEntry> aggByColumn = new ConcurrentHashMap<>();

  // Next available slot numbers (updated on load and allocation)
  private int nextDimSlot;
  private int nextAggSlot;

  // Read-only view over the registry caches (shares the same map instances)
  private final ColumnRegistryReader reader;

  /**
   * Create a ColumnRegistry and load all ACTIVE entries from the database.
   *
   * @param dsl jOOQ DSLContext
   * @param tableName the metadata table name
   * @throws SQLException if loading fails
   */
  public ColumnRegistry(DSLContext dsl, String tableName) throws SQLException {
    this.dsl = dsl;
    this.tableName = tableName;
    this.reader = new ColumnRegistryReader(dimByKey, dimByColumn, aggByKey, aggByColumn);
    try {
      loadActiveEntries();
    } catch (DataAccessException e) {
      throw translated(e);
    }
    logger.info(
        "ColumnRegistry loaded for table={}: {} dim entries, {} agg entries",
        tableName,
        dimByKey.size(),
        aggByKey.size());
  }

  /**
   * @deprecated Use {@link #ColumnRegistry(DSLContext, String)} instead.
   */
  @Deprecated
  public ColumnRegistry(DataSource dataSource, String tableName) throws SQLException {
    this(com.yscope.clp.service.common.config.db.DSLContextFactory.create(dataSource), tableName);
  }

  /**
   * Resolve an existing dimension mapping or allocate a new slot.
   *
   * @param dimKey the original field name (may contain special chars)
   * @param baseType simplified type: str, str_utf8, bool, int, float
   * @param width requested width for str/str_utf8 (ignored for other types)
   * @return the column name to use for this dimension
   */
  public String resolveOrAllocateDim(String dimKey, String baseType, int width)
      throws SQLException {
    DimRegistryEntry existing = dimByKey.get(dimKey);
    if (existing != null) {
      if (!existing.baseType().equals(baseType)) {
        logger.warn(
            "Type conflict for dim '{}': registered as '{}', requested as '{}' — using registered type",
            dimKey,
            existing.baseType(),
            baseType);
      }
      if (needsWidthExpansion(existing, width)) {
        return expandDimWidth(existing, width);
      }
      return existing.columnName();
    }
    return allocateNewDimSlot(dimKey, baseType, width);
  }

  /**
   * Resolve an existing agg mapping or allocate a new slot.
   *
   * @param aggKey field name (e.g., "level")
   * @param aggValue value qualifier (e.g., "error"), null for total
   * @param aggType the aggregation type
   * @param valueType physical column value type (INT or FLOAT)
   * @return the placeholder column name (e.g., "agg_f01")
   */
  public String resolveOrAllocateAgg(
      String aggKey, String aggValue, AggregationType aggType, AggValueType valueType)
      throws SQLException {
    String cacheKey = aggCacheKey(aggKey, aggValue, aggType);
    AggRegistryEntry existing = aggByKey.get(cacheKey);
    if (existing != null) {
      if (existing.valueType() != valueType) {
        logger.warn(
            "Value type conflict for agg '{}:{}' ({}): registered as {}, requested as {} — using registered type",
            aggKey,
            aggValue,
            aggType,
            existing.valueType(),
            valueType);
      }
      return existing.columnName();
    }
    return allocateNewAggSlot(aggKey, aggValue, aggType, valueType);
  }

  /** Specification for a dimension field. */
  public record DimSpec(String baseType, int width) {}

  /** Specification for an aggregate field. */
  public record AggSpec(
      String aggKey,
      String aggValue,
      AggregationType aggregationType,
      AggValueType valueType,
      String aliasColumn) {
    public AggSpec(
        String aggKey, String aggValue, AggregationType aggregationType, AggValueType valueType) {
      this(aggKey, aggValue, aggregationType, valueType, null);
    }
  }

  /**
   * Resolve or allocate dim columns for a batch of fields in one operation, issuing at most one
   * ALTER TABLE for all newly allocated columns.
   *
   * @param specs map of dimKey -> DimSpec
   * @return map of dimKey -> placeholder column name (e.g., "service" -> "dim_f01")
   * @throws SQLException if a database operation fails
   */
  public Map<String, String> resolveOrAllocateDims(Map<String, DimSpec> specs) throws SQLException {
    Map<String, String> result = new LinkedHashMap<>();
    Map<String, DimSpec> unknowns = new LinkedHashMap<>();
    for (Map.Entry<String, DimSpec> e : specs.entrySet()) {
      String dimKey = e.getKey();
      DimSpec spec = e.getValue();
      DimRegistryEntry existing = dimByKey.get(dimKey);
      if (existing != null) {
        if (!existing.baseType().equals(spec.baseType())) {
          logger.warn(
              "Type conflict for dim '{}': registered as '{}', requested as '{}' — using registered type",
              dimKey,
              existing.baseType(),
              spec.baseType());
        }
        if (needsWidthExpansion(existing, spec.width())) {
          result.put(dimKey, expandDimWidth(existing, spec.width()));
        } else {
          result.put(dimKey, existing.columnName());
        }
      } else {
        unknowns.put(dimKey, spec);
      }
    }
    if (!unknowns.isEmpty()) {
      result.putAll(batchAllocateDimSlots(unknowns));
    }
    return result;
  }

  /**
   * Resolve or allocate agg columns for a batch of fields in one operation, issuing at most one
   * ALTER TABLE for all newly allocated columns.
   *
   * @param specs map of compositeKey -> AggSpec
   * @return map of compositeKey -> placeholder column name (e.g., "GTE\0level\0error" -> "agg_f01")
   * @throws SQLException if a database operation fails
   */
  public Map<String, String> resolveOrAllocateAggs(Map<String, AggSpec> specs) throws SQLException {
    Map<String, String> result = new LinkedHashMap<>();
    Map<String, AggSpec> unknowns = new LinkedHashMap<>();
    for (Map.Entry<String, AggSpec> e : specs.entrySet()) {
      String compositeKey = e.getKey();
      AggRegistryEntry existing = aggByKey.get(compositeKey);
      if (existing != null) {
        AggSpec spec = e.getValue();
        if (existing.valueType() != spec.valueType()) {
          logger.warn(
              "Value type conflict for agg '{}:{}' ({}): registered as {}, requested as {} — using registered type",
              spec.aggKey(),
              spec.aggValue(),
              spec.aggregationType(),
              existing.valueType(),
              spec.valueType());
        }
        result.put(compositeKey, existing.columnName());
      } else {
        unknowns.put(compositeKey, e.getValue());
      }
    }
    if (!unknowns.isEmpty()) {
      Map<String, AggSpec> aliases = new LinkedHashMap<>();
      Map<String, AggSpec> allocations = new LinkedHashMap<>();
      for (Map.Entry<String, AggSpec> e : unknowns.entrySet()) {
        if (e.getValue().aliasColumn() != null) {
          aliases.put(e.getKey(), e.getValue());
        } else {
          allocations.put(e.getKey(), e.getValue());
        }
      }
      for (Map.Entry<String, AggSpec> e : aliases.entrySet()) {
        AggSpec spec = e.getValue();
        registerAggAlias(
            spec.aggKey(), spec.aggValue(), spec.aggregationType(),
            spec.valueType(), spec.aliasColumn());
        result.put(e.getKey(), spec.aliasColumn());
      }
      if (!allocations.isEmpty()) {
        result.putAll(batchAllocateAggSlots(allocations));
      }
    }
    return result;
  }

  /**
   * Register an agg field as an alias for an existing column in the metadata table. No physical
   * column is allocated; targetColumn must already exist (e.g., "record_count").
   *
   * @param aggKey field name (e.g., "level")
   * @param aggValue value qualifier (e.g., "info"), null for total
   * @param aggType the aggregation type
   * @param valueType physical column value type
   * @param targetColumn existing column name to alias (e.g., "record_count")
   */
  public synchronized void registerAggAlias(
      String aggKey,
      String aggValue,
      AggregationType aggType,
      AggValueType valueType,
      String targetColumn)
      throws SQLException {
    String cacheKey = aggCacheKey(aggKey, aggValue, aggType);
    AggRegistryEntry existing = aggByKey.get(cacheKey);
    if (existing != null) {
      return;
    }

    try {
      dsl.insertInto(AGG_REGISTRY)
          .set(REG_TABLE_NAME, tableName)
          .set(REG_COLUMN_NAME, targetColumn)
          .set(REG_AGG_KEY, aggKey)
          .set(REG_AGG_VALUE, aggValue)
          .set(REG_AGGREGATION_TYPE, aggType.name())
          .set(REG_VALUE_TYPE, valueType.name())
          .set(REG_ALIAS_COLUMN, targetColumn)
          .set(REG_STATUS, "ACTIVE")
          .execute();
    } catch (DataAccessException e) {
      if (isDuplicateKeyError(e)) {
        logger.debug("Agg alias race for aggKey={}, reloading", aggKey);
        reloadAggEntry(aggKey, aggValue, aggType);
        return;
      }
      throw translated(e);
    }

    AggRegistryEntry entry =
        new AggRegistryEntry(
            tableName,
            targetColumn,
            aggKey,
            aggValue,
            aggType,
            valueType,
            targetColumn,
            "ACTIVE",
            null,
            null);
    aggByKey.put(cacheKey, entry);
    aggByColumn.put(targetColumn, entry);

    logger.info("Registered agg alias: {}:{} ({}) -> {}", aggKey, aggValue, aggType, targetColumn);
  }

  /**
   * Register a dim field as an alias for an existing column in the metadata table. No physical
   * column is allocated; targetColumn must already exist (e.g., "clp_ir_storage_backend").
   *
   * @param dimKey logical field name (e.g., "backend")
   * @param baseType simplified type: str, str_utf8, bool, int, float
   * @param width VARCHAR width (str/str_utf8 only, null for non-string types; callers must pass
   *     null rather than 0 when width is not applicable)
   * @param targetColumn existing column name to alias (e.g., "clp_ir_storage_backend")
   */
  public synchronized void registerDimAlias(
      String dimKey, String baseType, Integer width, String targetColumn) throws SQLException {
    if (dimByKey.containsKey(dimKey)) {
      return;
    }

    try {
      dsl.insertInto(DIM_REGISTRY)
          .set(REG_TABLE_NAME, tableName)
          .set(REG_COLUMN_NAME, targetColumn)
          .set(REG_BASE_TYPE, baseType)
          .set(REG_WIDTH, width)
          .set(REG_DIM_KEY, dimKey)
          .set(REG_ALIAS_COLUMN, targetColumn)
          .set(REG_STATUS, "ACTIVE")
          .execute();
    } catch (DataAccessException e) {
      if (isDuplicateKeyError(e)) {
        logger.debug("Dim alias race for dimKey={}, reloading", dimKey);
        reloadDimEntry(dimKey);
        return;
      }
      throw translated(e);
    }

    DimRegistryEntry entry =
        new DimRegistryEntry(
            tableName, targetColumn, baseType, width, dimKey, targetColumn, "ACTIVE", null, null);
    dimByKey.put(dimKey, entry);
    dimByColumn.put(targetColumn, entry);

    logger.info("Registered dim alias: {} -> {}", dimKey, targetColumn);
  }

  /** Returns the read-only view over the registry caches. */
  public ColumnRegistryReader reader() {
    return reader;
  }

  /**
   * Look up the effective column name for an agg (agg_fXX or aliased column like record_count).
   *
   * @param aggKey field name (e.g., "level")
   * @param aggValue value qualifier (e.g., "warn"), null for total
   * @param aggType the aggregation type
   * @return the effective physical column name, or null if not found
   */
  public String aggLookup(String aggKey, String aggValue, AggregationType aggType) {
    return reader.aggLookup(aggKey, aggValue, aggType);
  }

  /** Get all active dimension column names, sorted alphabetically. */
  public List<String> getActiveDimColumns() {
    return reader.getActiveDimColumns();
  }

  /** Get all active dimension entries. */
  public Collection<DimRegistryEntry> getActiveDimEntries() {
    return reader.getActiveDimEntries();
  }

  /** Get all active agg entries. */
  public Collection<AggRegistryEntry> getActiveAggEntries() {
    return reader.getActiveAggEntries();
  }

  /** Get all active agg column names in slot order. */
  public List<String> getActiveAggColumns() {
    return reader.getActiveAggColumns();
  }

  /** Reverse lookup: column name -> dim key. Returns null if not found. */
  public String dimColumnToKey(String columnName) {
    return reader.dimColumnToKey(columnName);
  }

  /** Reverse lookup: column name -> full agg registry entry. Returns null if not found. */
  public AggRegistryEntry aggColumnToEntry(String columnName) {
    return reader.aggColumnToEntry(columnName);
  }

  /** Reverse lookup: column name -> agg key. Returns null if not found. */
  public String aggColumnToKey(String columnName) {
    return reader.aggColumnToKey(columnName);
  }

  /**
   * Get the dim key -> effective column name mapping for all active dimensions. For regular entries
   * the effective column is the placeholder (e.g., "dim_f01"); for alias entries it is the target
   * column (e.g., "clp_ir_storage_backend").
   */
  public Map<String, String> getDimKeyToColumnMap() {
    return reader.getDimKeyToColumnMap();
  }

  /**
   * Get the agg key -> column name mapping for all active aggs. Lowest-numbered slot wins for
   * duplicate aggKeys.
   */
  public Map<String, String> getAggKeyToColumnMap() {
    return reader.getAggKeyToColumnMap();
  }

  /** Get the DimRegistryEntry for a given dim key. Returns null if not found. */
  public DimRegistryEntry getDimEntry(String dimKey) {
    return reader.getDimEntry(dimKey);
  }

  // ========================================================================
  // Private: Loading
  // ========================================================================

  private void loadActiveEntries() {
    int maxDimSlot = 0;
    int maxAggSlot = 0;

    // Load dim entries
    var dimRows =
        dsl.select(
                REG_TABLE_NAME,
                REG_COLUMN_NAME,
                REG_BASE_TYPE,
                REG_WIDTH,
                REG_DIM_KEY,
                REG_ALIAS_COLUMN,
                REG_STATUS,
                REG_CREATED_AT,
                REG_INVALIDATED_AT)
            .from(DIM_REGISTRY)
            .where(REG_TABLE_NAME.eq(tableName))
            .and(REG_STATUS.eq("ACTIVE"))
            .fetch();

    for (var r : dimRows) {
      DimRegistryEntry entry =
          new DimRegistryEntry(
              r.get(REG_TABLE_NAME),
              r.get(REG_COLUMN_NAME),
              r.get(REG_BASE_TYPE),
              r.get(REG_WIDTH),
              r.get(REG_DIM_KEY),
              r.get(REG_ALIAS_COLUMN),
              r.get(REG_STATUS),
              r.get(REG_CREATED_AT),
              r.get(REG_INVALIDATED_AT));
      dimByKey.put(entry.dimKey(), entry);
      dimByColumn.put(entry.columnName(), entry);
      if (!entry.isAlias()) {
        int slot = parseSlotNumber(entry.columnName(), "dim_f");
        if (slot > maxDimSlot) {
          maxDimSlot = slot;
        }
      }
    }

    // Load agg entries
    var aggRows =
        dsl.select(
                REG_TABLE_NAME,
                REG_COLUMN_NAME,
                REG_AGG_KEY,
                REG_AGG_VALUE,
                REG_AGGREGATION_TYPE,
                REG_VALUE_TYPE,
                REG_ALIAS_COLUMN,
                REG_STATUS,
                REG_CREATED_AT,
                REG_INVALIDATED_AT)
            .from(AGG_REGISTRY)
            .where(REG_TABLE_NAME.eq(tableName))
            .and(REG_STATUS.eq("ACTIVE"))
            .fetch();

    for (var r : aggRows) {
      AggRegistryEntry entry =
          new AggRegistryEntry(
              r.get(REG_TABLE_NAME),
              r.get(REG_COLUMN_NAME),
              r.get(REG_AGG_KEY),
              r.get(REG_AGG_VALUE),
              AggregationType.valueOf(r.get(REG_AGGREGATION_TYPE)),
              AggValueType.valueOf(r.get(REG_VALUE_TYPE)),
              r.get(REG_ALIAS_COLUMN),
              r.get(REG_STATUS),
              r.get(REG_CREATED_AT),
              r.get(REG_INVALIDATED_AT));
      String ck = aggCacheKey(entry.aggKey(), entry.aggValue(), entry.aggregationType());
      aggByKey.put(ck, entry);
      aggByColumn.put(entry.columnName(), entry);
      if (!entry.isAlias()) {
        int slot = parseSlotNumber(entry.columnName(), "agg_f");
        if (slot > maxAggSlot) {
          maxAggSlot = slot;
        }
      }
    }

    this.nextDimSlot = maxDimSlot + 1;
    this.nextAggSlot = maxAggSlot + 1;
  }

  // ========================================================================
  // Private: Allocation
  // ========================================================================

  private synchronized String allocateNewDimSlot(String dimKey, String baseType, int width)
      throws SQLException {
    DimRegistryEntry existing = dimByKey.get(dimKey);
    if (existing != null) {
      return existing.columnName();
    }

    int slot = nextDimSlot++;
    String columnName = Columns.dimPlaceholder(slot);
    String sqlType = toSqlType(baseType, width);

    try {
      dsl.insertInto(DIM_REGISTRY)
          .set(REG_TABLE_NAME, tableName)
          .set(REG_COLUMN_NAME, columnName)
          .set(REG_BASE_TYPE, baseType)
          .set(REG_WIDTH, isStringType(baseType) ? width : null)
          .set(REG_DIM_KEY, dimKey)
          .set(REG_STATUS, "ACTIVE")
          .execute();

      // ALTER TABLE — plain SQL (jOOQ has no ALGORITHM/LOCK support)
      addPhysicalColumn(columnName, sqlType + " NULL");
    } catch (DataAccessException e) {
      if (isDuplicateKeyError(e)) {
        nextDimSlot--; // reclaim the abandoned slot number
        logger.debug("Dim slot race for dimKey={}, reloading", dimKey);
        reloadDimEntry(dimKey);
        DimRegistryEntry winner = dimByKey.get(dimKey);
        if (winner != null) {
          return winner.columnName();
        }
      }
      throw translated(e);
    }

    Integer widthVal = isStringType(baseType) ? width : null;
    DimRegistryEntry entry =
        new DimRegistryEntry(
            tableName, columnName, baseType, widthVal, dimKey, null, "ACTIVE", null, null);
    dimByKey.put(dimKey, entry);
    dimByColumn.put(columnName, entry);

    logger.info(
        "Allocated dim slot: {} -> {} (type={}, width={})", dimKey, columnName, baseType, width);
    return columnName;
  }

  private synchronized String allocateNewAggSlot(
      String aggKey, String aggValue, AggregationType aggType, AggValueType valueType)
      throws SQLException {
    String cacheKey = aggCacheKey(aggKey, aggValue, aggType);
    AggRegistryEntry existing = aggByKey.get(cacheKey);
    if (existing != null) {
      return existing.columnName();
    }

    int slot = nextAggSlot++;
    String columnName = Columns.aggPlaceholder(slot);
    String columnDef = valueType == AggValueType.FLOAT ? "DOUBLE NULL" : "BIGINT NULL";

    try {
      dsl.insertInto(AGG_REGISTRY)
          .set(REG_TABLE_NAME, tableName)
          .set(REG_COLUMN_NAME, columnName)
          .set(REG_AGG_KEY, aggKey)
          .set(REG_AGG_VALUE, aggValue)
          .set(REG_AGGREGATION_TYPE, aggType.name())
          .set(REG_VALUE_TYPE, valueType.name())
          .set(REG_STATUS, "ACTIVE")
          .execute();

      addPhysicalColumn(columnName, columnDef);
    } catch (DataAccessException e) {
      if (isDuplicateKeyError(e)) {
        nextAggSlot--; // reclaim the abandoned slot number
        logger.debug("Agg slot race for aggKey={}, reloading", aggKey);
        reloadAggEntry(aggKey, aggValue, aggType);
        AggRegistryEntry winner = aggByKey.get(cacheKey);
        if (winner != null) {
          return winner.columnName();
        }
      }
      throw translated(e);
    }

    AggRegistryEntry entry =
        new AggRegistryEntry(
            tableName,
            columnName,
            aggKey,
            aggValue,
            aggType,
            valueType,
            null,
            "ACTIVE",
            null,
            null);
    aggByKey.put(cacheKey, entry);
    aggByColumn.put(columnName, entry);

    logger.info(
        "Allocated agg slot: {}:{} -> {} (type={}, valueType={})",
        aggKey,
        aggValue,
        columnName,
        aggType,
        valueType);
    return columnName;
  }

  private synchronized Map<String, String> batchAllocateDimSlots(Map<String, DimSpec> specs)
      throws SQLException {
    Map<String, String> result = new LinkedHashMap<>();
    Map<String, String> dimKeyToColumn = new LinkedHashMap<>();
    Map<String, String> colToSqlDef = new LinkedHashMap<>();

    // Re-check under lock — another thread may have allocated between outer check and this lock
    for (Map.Entry<String, DimSpec> e : specs.entrySet()) {
      String dimKey = e.getKey();
      DimRegistryEntry existing = dimByKey.get(dimKey);
      if (existing != null) {
        result.put(dimKey, existing.columnName());
        continue;
      }
      DimSpec spec = e.getValue();
      String columnName = Columns.dimPlaceholder(nextDimSlot++);
      dimKeyToColumn.put(dimKey, columnName);
      colToSqlDef.put(columnName, toSqlType(spec.baseType(), spec.width()) + " NULL");
    }

    if (dimKeyToColumn.isEmpty()) {
      return result;
    }

    // Batch INSERT IGNORE — concurrent allocators for the same dimKey: only one row wins
    batchInsertIgnoreDims(dimKeyToColumn, specs);

    // SELECT back actual assignments — covers our wins and any race losses
    Map<String, DimRegistryEntry> actuals = selectDimEntries(dimKeyToColumn.keySet());

    List<String> newPhysicalCols = new ArrayList<>();
    for (Map.Entry<String, String> e : dimKeyToColumn.entrySet()) {
      String dimKey = e.getKey();
      String ourColumn = e.getValue();
      DimRegistryEntry actual = actuals.get(dimKey);
      if (actual != null) {
        dimByKey.put(dimKey, actual);
        dimByColumn.put(actual.columnName(), actual);
        result.put(dimKey, actual.columnName());
        if (actual.columnName().equals(ourColumn)) {
          newPhysicalCols.add(ourColumn);
        }
        // If we lost the race, the winner is responsible for the physical column
      }
    }

    if (!newPhysicalCols.isEmpty()) {
      batchAddPhysicalColumns(newPhysicalCols, colToSqlDef);
    }
    return result;
  }

  private synchronized Map<String, String> batchAllocateAggSlots(Map<String, AggSpec> specs)
      throws SQLException {
    Map<String, String> result = new LinkedHashMap<>();
    Map<String, String> compositeKeyToColumn = new LinkedHashMap<>();
    Map<String, String> colToSqlDef = new LinkedHashMap<>();

    for (Map.Entry<String, AggSpec> e : specs.entrySet()) {
      String compositeKey = e.getKey();
      AggRegistryEntry existing = aggByKey.get(compositeKey);
      if (existing != null) {
        result.put(compositeKey, existing.columnName());
        continue;
      }
      AggSpec spec = e.getValue();
      String columnName = Columns.aggPlaceholder(nextAggSlot++);
      String columnDef = spec.valueType() == AggValueType.FLOAT ? "DOUBLE NULL" : "BIGINT NULL";
      compositeKeyToColumn.put(compositeKey, columnName);
      colToSqlDef.put(columnName, columnDef);
    }

    if (compositeKeyToColumn.isEmpty()) {
      return result;
    }

    batchInsertIgnoreAggs(compositeKeyToColumn, specs);

    Map<String, AggRegistryEntry> actuals = selectAggEntries(compositeKeyToColumn.keySet(), specs);

    List<String> newPhysicalCols = new ArrayList<>();
    for (Map.Entry<String, String> e : compositeKeyToColumn.entrySet()) {
      String compositeKey = e.getKey();
      String ourColumn = e.getValue();
      AggRegistryEntry actual = actuals.get(compositeKey);
      if (actual != null) {
        String ck = aggCacheKey(actual.aggKey(), actual.aggValue(), actual.aggregationType());
        aggByKey.put(ck, actual);
        aggByColumn.put(actual.columnName(), actual);
        result.put(compositeKey, actual.columnName());
        if (actual.columnName().equals(ourColumn)) {
          newPhysicalCols.add(ourColumn);
        }
      }
    }

    if (!newPhysicalCols.isEmpty()) {
      batchAddPhysicalColumns(newPhysicalCols, colToSqlDef);
    }
    return result;
  }

  // ========================================================================
  // Private: Width expansion
  // ========================================================================

  private boolean needsWidthExpansion(DimRegistryEntry entry, int requestedWidth) {
    if (!isStringType(entry.baseType())) {
      return false;
    }
    return entry.width() != null && requestedWidth > entry.width();
  }

  private String expandDimWidth(DimRegistryEntry entry, int newWidth) throws SQLException {
    int oldWidth = entry.width() != null ? entry.width() : 0;

    if (oldWidth <= 255 && newWidth <= 255) {
      // Free INPLACE resize within 1-byte length prefix zone
      String sqlType = toSqlType(entry.baseType(), newWidth);
      modifyPhysicalColumn(entry.columnName(), sqlType + " NULL");

      try {
        dsl.update(DIM_REGISTRY)
            .set(REG_WIDTH, newWidth)
            .where(REG_TABLE_NAME.eq(tableName))
            .and(REG_COLUMN_NAME.eq(entry.columnName()))
            .execute();
      } catch (DataAccessException e) {
        throw translated(e);
      }

      DimRegistryEntry updated =
          new DimRegistryEntry(
              entry.tableName(),
              entry.columnName(),
              entry.baseType(),
              newWidth,
              entry.dimKey(),
              entry.aliasColumn(),
              entry.status(),
              entry.createdAt(),
              entry.invalidatedAt());
      dimByKey.put(updated.dimKey(), updated);
      dimByColumn.put(updated.columnName(), updated);

      logger.info(
          "Expanded dim width (INPLACE): {} ({} -> {})", entry.columnName(), oldWidth, newWidth);
      return entry.columnName();
    } else {
      invalidateDimSlot(entry);
      logger.info(
          "Invalidated dim slot {} for width expansion ({} -> {})",
          entry.columnName(),
          oldWidth,
          newWidth);
      return allocateNewDimSlot(entry.dimKey(), entry.baseType(), newWidth);
    }
  }

  private void invalidateDimSlot(DimRegistryEntry entry) throws SQLException {
    try {
      dsl.update(DIM_REGISTRY)
          .set(REG_STATUS, "INVALIDATED")
          .set(REG_INVALIDATED_AT, unixTimestamp())
          .where(REG_TABLE_NAME.eq(tableName))
          .and(REG_COLUMN_NAME.eq(entry.columnName()))
          .execute();
    } catch (DataAccessException e) {
      throw translated(e);
    }
    dimByKey.remove(entry.dimKey());
    dimByColumn.remove(entry.columnName());
  }

  // ========================================================================
  // Private: SQL type mapping
  // ========================================================================

  static String toSqlType(String baseType, int width) {
    switch (baseType) {
      case "str":
        return "VARCHAR(" + width + ") CHARACTER SET ascii COLLATE ascii_bin";
      case "str_utf8":
        return "VARCHAR(" + width + ")";
      case "bool":
        return "TINYINT(1)";
      case "int":
        return "BIGINT";
      case "float":
        return "DOUBLE";
      default:
        throw new IllegalArgumentException("Unknown base type: " + baseType);
    }
  }

  private static boolean isStringType(String baseType) {
    return "str".equals(baseType) || "str_utf8".equals(baseType);
  }

  // ========================================================================
  // Private: Physical column operations (plain SQL — jOOQ has no ALGORITHM/LOCK)
  // ========================================================================

  private void addPhysicalColumn(String columnName, String columnDef) throws SQLException {
    String lockMode = dsl.dialect() == SQLDialect.MARIADB ? "SHARED" : "NONE";
    String sql =
        String.format(
            "ALTER TABLE %s ADD COLUMN %s %s, ALGORITHM=INPLACE, LOCK=%s",
            tableName, columnName, columnDef, lockMode);
    try {
      dsl.execute(sql);
    } catch (DataAccessException e) {
      Throwable cause = e.getCause();
      String causeMsg = cause != null ? cause.getMessage() : null;
      if (causeMsg != null && causeMsg.contains("Duplicate column")) {
        logger.debug("Column {} already exists (race condition)", columnName);
        return;
      }
      throw translated(e);
    }
  }

  private void modifyPhysicalColumn(String columnName, String columnDef) throws SQLException {
    String lockMode = dsl.dialect() == SQLDialect.MARIADB ? "SHARED" : "NONE";
    String sql =
        String.format(
            "ALTER TABLE %s MODIFY COLUMN %s %s, ALGORITHM=INPLACE, LOCK=%s",
            tableName, columnName, columnDef, lockMode);
    try {
      dsl.execute(sql);
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  // ========================================================================
  // Private: Batch helpers
  // ========================================================================

  private void batchInsertIgnoreDims(Map<String, String> dimKeyToColumn, Map<String, DimSpec> specs)
      throws SQLException {
    StringBuilder sql =
        new StringBuilder(
            "INSERT IGNORE INTO _dim_registry"
                + " (table_name, column_name, base_type, width, dim_key, status) VALUES ");
    List<Object> binds = new ArrayList<>();
    boolean first = true;
    for (Map.Entry<String, String> e : dimKeyToColumn.entrySet()) {
      String dimKey = e.getKey();
      String columnName = e.getValue();
      DimSpec spec = specs.get(dimKey);
      if (!first) {
        sql.append(", ");
      }
      sql.append("(?,?,?,?,?,?)");
      binds.add(tableName);
      binds.add(columnName);
      binds.add(spec.baseType());
      binds.add(isStringType(spec.baseType()) ? spec.width() : null);
      binds.add(dimKey);
      binds.add("ACTIVE");
      first = false;
    }
    try {
      dsl.execute(sql.toString(), binds.toArray());
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  private void batchInsertIgnoreAggs(
      Map<String, String> compositeKeyToColumn, Map<String, AggSpec> specs) throws SQLException {
    StringBuilder sql =
        new StringBuilder(
            "INSERT IGNORE INTO _agg_registry"
                + " (table_name, column_name, agg_key, agg_value,"
                + " aggregation_type, value_type, status) VALUES ");
    List<Object> binds = new ArrayList<>();
    boolean first = true;
    for (Map.Entry<String, String> e : compositeKeyToColumn.entrySet()) {
      String compositeKey = e.getKey();
      String columnName = e.getValue();
      AggSpec spec = specs.get(compositeKey);
      if (!first) {
        sql.append(", ");
      }
      sql.append("(?,?,?,?,?,?,?)");
      binds.add(tableName);
      binds.add(columnName);
      binds.add(spec.aggKey());
      binds.add(spec.aggValue());
      binds.add(spec.aggregationType().name());
      binds.add(spec.valueType().name());
      binds.add("ACTIVE");
      first = false;
    }
    try {
      dsl.execute(sql.toString(), binds.toArray());
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  private Map<String, DimRegistryEntry> selectDimEntries(Set<String> dimKeys) throws SQLException {
    Map<String, DimRegistryEntry> result = new LinkedHashMap<>();
    try {
      var rows =
          dsl.select(
                  REG_TABLE_NAME,
                  REG_COLUMN_NAME,
                  REG_BASE_TYPE,
                  REG_WIDTH,
                  REG_DIM_KEY,
                  REG_ALIAS_COLUMN,
                  REG_STATUS,
                  REG_CREATED_AT,
                  REG_INVALIDATED_AT)
              .from(DIM_REGISTRY)
              .where(REG_TABLE_NAME.eq(tableName))
              .and(REG_DIM_KEY.in(dimKeys))
              .and(REG_STATUS.eq("ACTIVE"))
              .fetch();
      for (var r : rows) {
        DimRegistryEntry entry =
            new DimRegistryEntry(
                r.get(REG_TABLE_NAME),
                r.get(REG_COLUMN_NAME),
                r.get(REG_BASE_TYPE),
                r.get(REG_WIDTH),
                r.get(REG_DIM_KEY),
                r.get(REG_ALIAS_COLUMN),
                r.get(REG_STATUS),
                r.get(REG_CREATED_AT),
                r.get(REG_INVALIDATED_AT));
        result.put(entry.dimKey(), entry);
      }
    } catch (DataAccessException e) {
      throw translated(e);
    }
    return result;
  }

  private Map<String, AggRegistryEntry> selectAggEntries(
      Set<String> compositeKeys, Map<String, AggSpec> specs) throws SQLException {
    Set<String> aggKeys = new HashSet<>();
    for (AggSpec spec : specs.values()) {
      aggKeys.add(spec.aggKey());
    }
    Map<String, AggRegistryEntry> result = new LinkedHashMap<>();
    try {
      var rows =
          dsl.select(
                  REG_TABLE_NAME,
                  REG_COLUMN_NAME,
                  REG_AGG_KEY,
                  REG_AGG_VALUE,
                  REG_AGGREGATION_TYPE,
                  REG_VALUE_TYPE,
                  REG_ALIAS_COLUMN,
                  REG_STATUS,
                  REG_CREATED_AT,
                  REG_INVALIDATED_AT)
              .from(AGG_REGISTRY)
              .where(REG_TABLE_NAME.eq(tableName))
              .and(REG_AGG_KEY.in(aggKeys))
              .and(REG_STATUS.eq("ACTIVE"))
              .fetch();
      for (var r : rows) {
        AggRegistryEntry entry =
            new AggRegistryEntry(
                r.get(REG_TABLE_NAME),
                r.get(REG_COLUMN_NAME),
                r.get(REG_AGG_KEY),
                r.get(REG_AGG_VALUE),
                AggregationType.valueOf(r.get(REG_AGGREGATION_TYPE)),
                AggValueType.valueOf(r.get(REG_VALUE_TYPE)),
                r.get(REG_ALIAS_COLUMN),
                r.get(REG_STATUS),
                r.get(REG_CREATED_AT),
                r.get(REG_INVALIDATED_AT));
        String ck = aggCacheKey(entry.aggKey(), entry.aggValue(), entry.aggregationType());
        if (compositeKeys.contains(ck)) {
          result.put(ck, entry);
        }
      }
    } catch (DataAccessException e) {
      throw translated(e);
    }
    return result;
  }

  private void batchAddPhysicalColumns(List<String> columnNames, Map<String, String> colToSqlDef)
      throws SQLException {
    String lockMode = dsl.dialect() == SQLDialect.MARIADB ? "SHARED" : "NONE";
    StringBuilder sql = new StringBuilder("ALTER TABLE ").append(tableName);
    for (int i = 0; i < columnNames.size(); i++) {
      String col = columnNames.get(i);
      if (i > 0) {
        sql.append(",");
      }
      sql.append(" ADD COLUMN ").append(col).append(" ").append(colToSqlDef.get(col));
    }
    sql.append(", ALGORITHM=INPLACE, LOCK=").append(lockMode);
    try {
      dsl.execute(sql.toString());
      logger.info("Batch-added {} physical column(s): {}", columnNames.size(), columnNames);
    } catch (DataAccessException e) {
      Throwable cause = e.getCause();
      String causeMsg = cause != null ? cause.getMessage() : "";
      if (causeMsg.contains("Duplicate column") || causeMsg.contains("1060")) {
        // A previous crashed run may have inserted the registry row but not finished the ALTER.
        // Fall back to individual adds which silently skip existing columns.
        logger.debug("Batch ADD COLUMN hit duplicate; falling back to individual adds");
        for (String col : columnNames) {
          addPhysicalColumn(col, colToSqlDef.get(col));
        }
      } else {
        throw translated(e);
      }
    }
  }

  // ========================================================================
  // Private: Helpers
  // ========================================================================

  private void reloadDimEntry(String dimKey) throws SQLException {
    try {
      var rows =
          dsl.select(
                  REG_TABLE_NAME,
                  REG_COLUMN_NAME,
                  REG_BASE_TYPE,
                  REG_WIDTH,
                  REG_DIM_KEY,
                  REG_ALIAS_COLUMN,
                  REG_STATUS,
                  REG_CREATED_AT,
                  REG_INVALIDATED_AT)
              .from(DIM_REGISTRY)
              .where(REG_TABLE_NAME.eq(tableName))
              .and(REG_DIM_KEY.eq(dimKey))
              .and(REG_STATUS.eq("ACTIVE"))
              .fetch();

      for (var r : rows) {
        DimRegistryEntry entry =
            new DimRegistryEntry(
                r.get(REG_TABLE_NAME),
                r.get(REG_COLUMN_NAME),
                r.get(REG_BASE_TYPE),
                r.get(REG_WIDTH),
                r.get(REG_DIM_KEY),
                r.get(REG_ALIAS_COLUMN),
                r.get(REG_STATUS),
                r.get(REG_CREATED_AT),
                r.get(REG_INVALIDATED_AT));
        dimByKey.put(entry.dimKey(), entry);
        dimByColumn.put(entry.columnName(), entry);
      }
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  private void reloadAggEntry(String aggKey, String aggValue, AggregationType aggType)
      throws SQLException {
    try {
      var rows =
          dsl.select(
                  REG_TABLE_NAME,
                  REG_COLUMN_NAME,
                  REG_AGG_KEY,
                  REG_AGG_VALUE,
                  REG_AGGREGATION_TYPE,
                  REG_VALUE_TYPE,
                  REG_ALIAS_COLUMN,
                  REG_STATUS,
                  REG_CREATED_AT,
                  REG_INVALIDATED_AT)
              .from(AGG_REGISTRY)
              .where(REG_TABLE_NAME.eq(tableName))
              .and(REG_AGG_KEY.eq(aggKey))
              .and(REG_STATUS.eq("ACTIVE"))
              .fetch();

      for (var r : rows) {
        AggRegistryEntry entry =
            new AggRegistryEntry(
                r.get(REG_TABLE_NAME),
                r.get(REG_COLUMN_NAME),
                r.get(REG_AGG_KEY),
                r.get(REG_AGG_VALUE),
                AggregationType.valueOf(r.get(REG_AGGREGATION_TYPE)),
                AggValueType.valueOf(r.get(REG_VALUE_TYPE)),
                r.get(REG_ALIAS_COLUMN),
                r.get(REG_STATUS),
                r.get(REG_CREATED_AT),
                r.get(REG_INVALIDATED_AT));
        String ck = aggCacheKey(entry.aggKey(), entry.aggValue(), entry.aggregationType());
        aggByKey.put(ck, entry);
        aggByColumn.put(entry.columnName(), entry);
      }
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  private static String aggCacheKey(
      String aggKey, String aggValue, AggregationType aggregationType) {
    return ColumnRegistryReader.aggCacheKey(aggKey, aggValue, aggregationType);
  }

  private static int parseSlotNumber(String columnName, String prefix) {
    try {
      return Integer.parseInt(columnName.substring(prefix.length()));
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static boolean isDuplicateKeyError(DataAccessException e) {
    Throwable cause = e.getCause();
    if (cause != null) {
      String causeMsg = cause.getMessage();
      if (causeMsg != null && (causeMsg.contains("Duplicate entry") || causeMsg.contains("1062"))) {
        return true;
      }
    }
    String msg = e.getMessage();
    return msg != null && (msg.contains("Duplicate entry") || msg.contains("1062"));
  }

  private static SQLException translated(DataAccessException e) {
    if (e.getCause() instanceof SQLException sqlEx) {
      return sqlEx;
    }
    return new SQLException(e.getMessage(), e);
  }
}
