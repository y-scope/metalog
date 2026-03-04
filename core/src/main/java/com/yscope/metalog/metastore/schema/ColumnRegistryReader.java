package com.yscope.metalog.metastore.schema;

import com.yscope.metalog.metastore.model.AggRegistryEntry;
import com.yscope.metalog.metastore.model.AggregationType;
import com.yscope.metalog.metastore.model.DimRegistryEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Read-only view over the column registry caches.
 *
 * <p>Provides all resolution (lookup) operations without any database access or synchronization.
 * Shares the same {@link ConcurrentHashMap} instances as the owning {@link ColumnRegistry}, so
 * allocations on the write path are immediately visible here.
 *
 * <p>This class is safe for use from any thread — all underlying maps are concurrent.
 */
public class ColumnRegistryReader {

  private final ConcurrentHashMap<String, DimRegistryEntry> dimByKey;
  private final ConcurrentHashMap<String, DimRegistryEntry> dimByColumn;
  private final ConcurrentHashMap<String, AggRegistryEntry> aggByKey;
  private final ConcurrentHashMap<String, AggRegistryEntry> aggByColumn;

  ColumnRegistryReader(
      ConcurrentHashMap<String, DimRegistryEntry> dimByKey,
      ConcurrentHashMap<String, DimRegistryEntry> dimByColumn,
      ConcurrentHashMap<String, AggRegistryEntry> aggByKey,
      ConcurrentHashMap<String, AggRegistryEntry> aggByColumn) {
    this.dimByKey = dimByKey;
    this.dimByColumn = dimByColumn;
    this.aggByKey = aggByKey;
    this.aggByColumn = aggByColumn;
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
    AggRegistryEntry entry = aggByKey.get(aggCacheKey(aggKey, aggValue, aggType));
    return entry != null ? entry.effectiveColumn() : null;
  }

  /** Get all active dimension column names, sorted alphabetically. */
  public List<String> getActiveDimColumns() {
    List<String> columns = new ArrayList<>(dimByColumn.keySet());
    Collections.sort(columns);
    return columns;
  }

  /** Get all active dimension entries. */
  public Collection<DimRegistryEntry> getActiveDimEntries() {
    return Collections.unmodifiableCollection(dimByColumn.values());
  }

  /** Get all active agg entries. */
  public Collection<AggRegistryEntry> getActiveAggEntries() {
    return Collections.unmodifiableCollection(aggByColumn.values());
  }

  /** Get all active agg column names in slot order. */
  public List<String> getActiveAggColumns() {
    List<String> columns = new ArrayList<>(aggByColumn.keySet());
    Collections.sort(columns);
    return columns;
  }

  /** Reverse lookup: column name -> dim key. Returns null if not found. */
  public String dimColumnToKey(String columnName) {
    DimRegistryEntry entry = dimByColumn.get(columnName);
    return entry != null ? entry.dimKey() : null;
  }

  /** Reverse lookup: column name -> full agg registry entry. Returns null if not found. */
  public AggRegistryEntry aggColumnToEntry(String columnName) {
    return aggByColumn.get(columnName);
  }

  /** Reverse lookup: column name -> agg key. Returns null if not found. */
  public String aggColumnToKey(String columnName) {
    AggRegistryEntry entry = aggByColumn.get(columnName);
    return entry != null ? entry.aggKey() : null;
  }

  /**
   * Get the dim key -> effective column name mapping for all active dimensions. For regular entries
   * the effective column is the placeholder (e.g., "dim_f01"); for alias entries it is the target
   * column (e.g., "clp_ir_storage_backend").
   */
  public Map<String, String> getDimKeyToColumnMap() {
    Map<String, String> map = new LinkedHashMap<>();
    for (Map.Entry<String, DimRegistryEntry> e : dimByKey.entrySet()) {
      map.put(e.getKey(), e.getValue().effectiveColumn());
    }
    return map;
  }

  /**
   * Get the agg key -> column name mapping for all active aggs. Lowest-numbered slot wins for
   * duplicate aggKeys.
   */
  public Map<String, String> getAggKeyToColumnMap() {
    Map<String, String> map = new LinkedHashMap<>();
    aggByColumn.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(e -> map.putIfAbsent(e.getValue().aggKey(), e.getValue().columnName()));
    return map;
  }

  /** Get the DimRegistryEntry for a given dim key. Returns null if not found. */
  public DimRegistryEntry getDimEntry(String dimKey) {
    return dimByKey.get(dimKey);
  }

  /**
   * Build the composite cache key for an agg entry: {@code "aggregationType\0aggKey\0aggValue"}.
   */
  static String aggCacheKey(String aggKey, String aggValue, AggregationType aggregationType) {
    return aggregationType.name() + "\0" + aggKey + "\0" + (aggValue != null ? aggValue : "");
  }
}
