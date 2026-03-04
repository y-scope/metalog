package com.yscope.metalog.metastore.schema;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.metalog.metastore.model.AggRegistryEntry;
import com.yscope.metalog.metastore.model.AggValueType;
import com.yscope.metalog.metastore.model.AggregationType;
import com.yscope.metalog.metastore.model.DimRegistryEntry;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ColumnRegistryReader Unit Tests")
class ColumnRegistryReaderTest {

  private ConcurrentHashMap<String, DimRegistryEntry> dimByKey;
  private ConcurrentHashMap<String, DimRegistryEntry> dimByColumn;
  private ConcurrentHashMap<String, AggRegistryEntry> aggByKey;
  private ConcurrentHashMap<String, AggRegistryEntry> aggByColumn;

  @BeforeEach
  void setUp() {
    dimByKey = new ConcurrentHashMap<>();
    dimByColumn = new ConcurrentHashMap<>();
    aggByKey = new ConcurrentHashMap<>();
    aggByColumn = new ConcurrentHashMap<>();
  }

  private ColumnRegistryReader createReader() {
    return new ColumnRegistryReader(dimByKey, dimByColumn, aggByKey, aggByColumn);
  }

  // ==========================================
  // Helper factories
  // ==========================================

  private static DimRegistryEntry dimEntry(
      String columnName, String baseType, Integer width, String dimKey, String aliasColumn) {
    return new DimRegistryEntry(
        "test_table", columnName, baseType, width, dimKey, aliasColumn, "ACTIVE", 1000L, null);
  }

  private static AggRegistryEntry aggEntry(
      String columnName,
      String aggKey,
      String aggValue,
      AggregationType aggType,
      AggValueType valueType,
      String aliasColumn) {
    return new AggRegistryEntry(
        "test_table",
        columnName,
        aggKey,
        aggValue,
        aggType,
        valueType,
        aliasColumn,
        "ACTIVE",
        1000L,
        null);
  }

  private void addDim(String dimKey, String columnName, String baseType, Integer width) {
    addDim(dimKey, columnName, baseType, width, null);
  }

  private void addDim(
      String dimKey, String columnName, String baseType, Integer width, String aliasColumn) {
    DimRegistryEntry entry = dimEntry(columnName, baseType, width, dimKey, aliasColumn);
    dimByKey.put(dimKey, entry);
    dimByColumn.put(entry.effectiveColumn(), entry);
  }

  private void addAgg(
      String aggKey,
      String aggValue,
      AggregationType aggType,
      AggValueType valueType,
      String columnName) {
    addAgg(aggKey, aggValue, aggType, valueType, columnName, null);
  }

  private void addAgg(
      String aggKey,
      String aggValue,
      AggregationType aggType,
      AggValueType valueType,
      String columnName,
      String aliasColumn) {
    AggRegistryEntry entry = aggEntry(columnName, aggKey, aggValue, aggType, valueType, aliasColumn);
    String cacheKey = ColumnRegistryReader.aggCacheKey(aggKey, aggValue, aggType);
    aggByKey.put(cacheKey, entry);
    aggByColumn.put(entry.effectiveColumn(), entry);
  }

  // ==========================================
  // Empty registry
  // ==========================================

  @Test
  @DisplayName("empty registry: collections are empty and lookups return null")
  void emptyRegistry_emptyCollectionsAndNullLookups() {
    ColumnRegistryReader reader = createReader();
    assertTrue(reader.getActiveDimColumns().isEmpty());
    assertTrue(reader.getActiveDimEntries().isEmpty());
    assertTrue(reader.getActiveAggColumns().isEmpty());
    assertTrue(reader.getActiveAggEntries().isEmpty());
    assertTrue(reader.getDimKeyToColumnMap().isEmpty());
    assertTrue(reader.getAggKeyToColumnMap().isEmpty());
    assertNull(reader.aggLookup("x", null, AggregationType.EQ));
    assertNull(reader.dimColumnToKey("dim_f01"));
    assertNull(reader.aggColumnToEntry("agg_f01"));
    assertNull(reader.aggColumnToKey("agg_f01"));
    assertNull(reader.getDimEntry("x"));
  }

  // ==========================================
  // Agg lookup
  // ==========================================

  @Nested
  @DisplayName("aggLookup")
  class AggLookup {

    @Test
    @DisplayName("returns column for existing entry")
    void existingEntry() {
      addAgg("level", "warn", AggregationType.EQ, AggValueType.INT, "agg_f01");
      ColumnRegistryReader reader = createReader();
      assertEquals("agg_f01", reader.aggLookup("level", "warn", AggregationType.EQ));
    }

    @Test
    @DisplayName("returns null for missing entry")
    void missingEntry() {
      addAgg("level", "warn", AggregationType.EQ, AggValueType.INT, "agg_f01");
      ColumnRegistryReader reader = createReader();
      assertNull(reader.aggLookup("level", "error", AggregationType.EQ));
    }

    @Test
    @DisplayName("null aggValue is handled correctly")
    void nullAggValue() {
      addAgg("level", null, AggregationType.EQ, AggValueType.INT, "agg_f02");
      ColumnRegistryReader reader = createReader();
      assertEquals("agg_f02", reader.aggLookup("level", null, AggregationType.EQ));
    }

    @Test
    @DisplayName("returns aliased column for alias entry")
    void aliasedEntry() {
      addAgg("count", null, AggregationType.EQ, AggValueType.INT, "agg_f01", "record_count");
      ColumnRegistryReader reader = createReader();
      assertEquals("record_count", reader.aggLookup("count", null, AggregationType.EQ));
    }
  }

  // ==========================================
  // Dim columns
  // ==========================================

  @Nested
  @DisplayName("getActiveDimColumns")
  class ActiveDimColumns {

    @Test
    @DisplayName("returns sorted list")
    void sortedColumns() {
      addDim("host", "dim_f02", "str", 255);
      addDim("app", "dim_f01", "str", 128);
      addDim("zone", "dim_f03", "str", 64);
      ColumnRegistryReader reader = createReader();

      List<String> columns = reader.getActiveDimColumns();
      assertEquals(List.of("dim_f01", "dim_f02", "dim_f03"), columns);
    }
  }

  // ==========================================
  // getDimKeyToColumnMap
  // ==========================================

  @Nested
  @DisplayName("getDimKeyToColumnMap")
  class DimKeyToColumnMap {

    @Test
    @DisplayName("maps dimKey to effectiveColumn")
    void regularEntries() {
      addDim("host", "dim_f01", "str", 255);
      addDim("app", "dim_f02", "str", 128);
      ColumnRegistryReader reader = createReader();

      Map<String, String> map = reader.getDimKeyToColumnMap();
      assertEquals("dim_f01", map.get("host"));
      assertEquals("dim_f02", map.get("app"));
    }

    @Test
    @DisplayName("alias entries use effectiveColumn (aliasColumn)")
    void aliasEntries() {
      addDim("storage", "dim_f01", "str", 255, "clp_ir_storage_backend");
      ColumnRegistryReader reader = createReader();

      Map<String, String> map = reader.getDimKeyToColumnMap();
      assertEquals("clp_ir_storage_backend", map.get("storage"));
    }
  }

  // ==========================================
  // getAggKeyToColumnMap
  // ==========================================

  @Nested
  @DisplayName("getAggKeyToColumnMap")
  class AggKeyToColumnMap {

    @Test
    @DisplayName("lowest slot wins for duplicate aggKeys")
    void lowestSlotWins() {
      // agg_f01 < agg_f03 alphabetically, so agg_f01 should win
      addAgg("level", "warn", AggregationType.EQ, AggValueType.INT, "agg_f03");
      addAgg("level", "error", AggregationType.EQ, AggValueType.INT, "agg_f01");
      ColumnRegistryReader reader = createReader();

      Map<String, String> map = reader.getAggKeyToColumnMap();
      // Both have aggKey="level", sorted by column name, agg_f01 comes first -> wins
      assertEquals("agg_f01", map.get("level"));
      assertEquals(1, map.size());
    }
  }

  // ==========================================
  // Reverse lookups
  // ==========================================

  @Nested
  @DisplayName("Reverse lookups")
  class ReverseLookups {

    @Test
    @DisplayName("dimColumnToKey returns correct key")
    void dimColumnToKey() {
      addDim("host", "dim_f01", "str", 255);
      ColumnRegistryReader reader = createReader();
      assertEquals("host", reader.dimColumnToKey("dim_f01"));
    }

    @Test
    @DisplayName("dimColumnToKey returns null for unknown column")
    void dimColumnToKeyMissing() {
      ColumnRegistryReader reader = createReader();
      assertNull(reader.dimColumnToKey("dim_f99"));
    }

    @Test
    @DisplayName("aggColumnToEntry returns full entry")
    void aggColumnToEntry() {
      addAgg("level", "warn", AggregationType.EQ, AggValueType.INT, "agg_f01");
      ColumnRegistryReader reader = createReader();

      AggRegistryEntry entry = reader.aggColumnToEntry("agg_f01");
      assertNotNull(entry);
      assertEquals("level", entry.aggKey());
      assertEquals("warn", entry.aggValue());
      assertEquals(AggregationType.EQ, entry.aggregationType());
    }

    @Test
    @DisplayName("aggColumnToKey returns agg key for known column")
    void aggColumnToKey() {
      addAgg("level", "warn", AggregationType.EQ, AggValueType.INT, "agg_f01");
      ColumnRegistryReader reader = createReader();
      assertEquals("level", reader.aggColumnToKey("agg_f01"));
    }

    @Test
    @DisplayName("aggColumnToKey returns null for unknown column")
    void aggColumnToKeyMissing() {
      ColumnRegistryReader reader = createReader();
      assertNull(reader.aggColumnToKey("agg_f99"));
    }
  }

  // ==========================================
  // getDimEntry
  // ==========================================

  @Nested
  @DisplayName("getDimEntry")
  class GetDimEntry {

    @Test
    @DisplayName("returns entry for known dim key")
    void knownKey() {
      addDim("host", "dim_f01", "str", 255);
      ColumnRegistryReader reader = createReader();

      DimRegistryEntry entry = reader.getDimEntry("host");
      assertNotNull(entry);
      assertEquals("dim_f01", entry.columnName());
      assertEquals("str", entry.baseType());
      assertEquals(255, entry.width());
    }

    @Test
    @DisplayName("returns null for unknown dim key")
    void unknownKey() {
      ColumnRegistryReader reader = createReader();
      assertNull(reader.getDimEntry("nonexistent"));
    }
  }

  // ==========================================
  // aggCacheKey
  // ==========================================

  @Nested
  @DisplayName("aggCacheKey")
  class AggCacheKeyTest {

    @Test
    @DisplayName("produces deterministic composite key")
    void compositeKey() {
      String key = ColumnRegistryReader.aggCacheKey("level", "warn", AggregationType.EQ);
      assertEquals("EQ\0level\0warn", key);
    }

    @Test
    @DisplayName("null aggValue maps to empty string")
    void nullAggValue() {
      String key = ColumnRegistryReader.aggCacheKey("level", null, AggregationType.GTE);
      assertEquals("GTE\0level\0", key);
    }
  }
}
