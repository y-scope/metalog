package com.yscope.clp.service.metastore.schema;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.clp.service.metastore.model.AggRegistryEntry;
import com.yscope.clp.service.metastore.model.AggValueType;
import com.yscope.clp.service.metastore.model.AggregationType;
import com.yscope.clp.service.metastore.model.DimRegistryEntry;
import com.yscope.clp.service.testutil.AbstractMariaDBTest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link ColumnRegistry} against a real MariaDB instance.
 *
 * <p>Validates the full cycle of column allocation: registry INSERT + ALTER TABLE ADD COLUMN for
 * dimension and aggregation placeholders. Each test runs in a fresh database to ensure DDL isolation
 * (ALTER TABLE operations cannot be rolled back).
 */
class ColumnRegistryIT extends AbstractMariaDBTest {

  private static final String TABLE_NAME = "clp_table";

  @BeforeEach
  void setUp() throws Exception {
    registerTable(TABLE_NAME);
  }

  // ========================================================================
  // Constructor loading
  // ========================================================================

  @Test
  void constructor_loadsEmptyRegistry() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    assertTrue(registry.getActiveDimColumns().isEmpty());
    assertTrue(registry.getActiveAggColumns().isEmpty());
  }

  @Test
  void constructor_loadsExistingActiveEntries() throws SQLException {
    // Pre-insert ACTIVE dim and agg entries
    addDimColumn("dim_f01", "VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL");
    insertDimRegistryEntry(TABLE_NAME, "dim_f01", "str", 128, "dim/str128/app_id");
    insertAggRegistryEntry(
        TABLE_NAME, "agg_f01", "level", "debug", "GTE", "INT");

    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    assertEquals(1, registry.getActiveDimColumns().size());
    assertEquals(1, registry.getActiveAggColumns().size());
    assertEquals("dim_f01", registry.getActiveDimColumns().get(0));
    assertEquals("agg_f01", registry.getActiveAggColumns().get(0));
  }

  // ========================================================================
  // Resolve or allocate dim
  // ========================================================================

  @Test
  void resolveOrAllocateDim_allocatesNewSlot() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    String colName = registry.resolveOrAllocateDim("dim/str128/service", "str", 128);

    assertEquals("dim_f01", colName);
    assertTrue(columnExists(TABLE_NAME, "dim_f01"));
  }

  @Test
  void resolveOrAllocateDim_returnsCachedOnSecondCall() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    String first = registry.resolveOrAllocateDim("dim/str128/service", "str", 128);
    String second = registry.resolveOrAllocateDim("dim/str128/service", "str", 128);

    assertEquals(first, second);
    assertEquals("dim_f01", first);
  }

  @Test
  void resolveOrAllocateDim_strType() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateDim("dim/str64/region", "str", 64);
    // Verify the column has ASCII charset
    String colType = getColumnType(TABLE_NAME, "dim_f01");
    assertTrue(colType.toLowerCase().contains("varchar"), "Expected VARCHAR, got: " + colType);
  }

  @Test
  void resolveOrAllocateDim_strUtf8Type() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateDim("dim/str_utf8_128/label", "str_utf8", 128);
    assertTrue(columnExists(TABLE_NAME, "dim_f01"));
  }

  @Test
  void resolveOrAllocateDim_boolType() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateDim("dim/bool/is_active", "bool", 1);
    assertTrue(columnExists(TABLE_NAME, "dim_f01"));
    String colType = getColumnType(TABLE_NAME, "dim_f01");
    String lowerType = colType.toLowerCase();
    assertTrue(
        lowerType.contains("tinyint") || lowerType.contains("boolean") || lowerType.contains("bit"),
        "Expected TINYINT, BOOLEAN, or BIT, got: " + colType);
  }

  @Test
  void resolveOrAllocateDim_intType() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateDim("dim/int/thread_id", "int", 8);
    assertTrue(columnExists(TABLE_NAME, "dim_f01"));
    String colType = getColumnType(TABLE_NAME, "dim_f01");
    assertTrue(colType.toLowerCase().contains("bigint"), "Expected BIGINT, got: " + colType);
  }

  @Test
  void resolveOrAllocateDim_floatType() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateDim("dim/float/score", "float", 8);
    assertTrue(columnExists(TABLE_NAME, "dim_f01"));
    String colType = getColumnType(TABLE_NAME, "dim_f01");
    assertTrue(colType.toLowerCase().contains("double"), "Expected DOUBLE, got: " + colType);
  }

  // ========================================================================
  // Resolve or allocate dims batch
  // ========================================================================

  @Test
  void resolveOrAllocateDims_batchAllocatesMultiple() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    Map<String, ColumnRegistry.DimSpec> specs = new LinkedHashMap<>();
    specs.put("dim/str128/service", new ColumnRegistry.DimSpec("str", 128));
    specs.put("dim/str64/region", new ColumnRegistry.DimSpec("str", 64));
    specs.put("dim/int/thread_id", new ColumnRegistry.DimSpec("int", 8));

    Map<String, String> result = registry.resolveOrAllocateDims(specs);

    assertEquals(3, result.size());
    assertTrue(columnExists(TABLE_NAME, result.get("dim/str128/service")));
    assertTrue(columnExists(TABLE_NAME, result.get("dim/str64/region")));
    assertTrue(columnExists(TABLE_NAME, result.get("dim/int/thread_id")));
  }

  // ========================================================================
  // Resolve or allocate agg
  // ========================================================================

  @Test
  void resolveOrAllocateAgg_allocatesNewSlot() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    String colName =
        registry.resolveOrAllocateAgg("level", "debug", AggregationType.GTE, AggValueType.INT);

    assertEquals("agg_f01", colName);
    assertTrue(columnExists(TABLE_NAME, "agg_f01"));
  }

  @Test
  void resolveOrAllocateAgg_returnsCachedOnSecondCall() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    String first =
        registry.resolveOrAllocateAgg("level", "debug", AggregationType.GTE, AggValueType.INT);
    String second =
        registry.resolveOrAllocateAgg("level", "debug", AggregationType.GTE, AggValueType.INT);

    assertEquals(first, second);
  }

  @Test
  void resolveOrAllocateAgg_floatType() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    String colName =
        registry.resolveOrAllocateAgg("latency", "p99", AggregationType.AVG, AggValueType.FLOAT);

    assertTrue(columnExists(TABLE_NAME, colName));
    String colType = getColumnType(TABLE_NAME, colName);
    assertTrue(colType.toLowerCase().contains("double"), "Expected DOUBLE, got: " + colType);
  }

  // ========================================================================
  // Resolve or allocate aggs batch
  // ========================================================================

  @Test
  void resolveOrAllocateAggs_batchAllocatesMultiple() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    Map<String, ColumnRegistry.AggSpec> specs = new LinkedHashMap<>();
    specs.put(
        "GTE\0level\0debug",
        new ColumnRegistry.AggSpec("level", "debug", AggregationType.GTE, AggValueType.INT));
    specs.put(
        "GTE\0level\0info",
        new ColumnRegistry.AggSpec("level", "info", AggregationType.GTE, AggValueType.INT));

    Map<String, String> result = registry.resolveOrAllocateAggs(specs);

    assertEquals(2, result.size());
    assertTrue(columnExists(TABLE_NAME, result.get("GTE\0level\0debug")));
    assertTrue(columnExists(TABLE_NAME, result.get("GTE\0level\0info")));
  }

  // ========================================================================
  // Dim alias registration
  // ========================================================================

  @Test
  void registerDimAlias_mapsToExistingColumn() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    // clp_ir_storage_backend already exists in the schema
    registry.registerDimAlias(
        "dim/str128/ir_storage_backend", "str", 128, "clp_ir_storage_backend");

    DimRegistryEntry entry = registry.getDimEntry("dim/str128/ir_storage_backend");
    assertNotNull(entry);
    assertTrue(entry.isAlias());
    assertEquals("clp_ir_storage_backend", entry.effectiveColumn());
  }

  @Test
  void registerDimAlias_idempotent() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    registry.registerDimAlias(
        "dim/str128/ir_storage_backend", "str", 128, "clp_ir_storage_backend");
    // Second call should not throw
    registry.registerDimAlias(
        "dim/str128/ir_storage_backend", "str", 128, "clp_ir_storage_backend");

    assertEquals(1, registry.getActiveDimColumns().size());
  }

  // ========================================================================
  // Agg alias registration
  // ========================================================================

  @Test
  void registerAggAlias_mapsToExistingColumn() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    // record_count already exists in the schema
    registry.registerAggAlias(
        "record_count", null, AggregationType.EQ, AggValueType.INT, "record_count");

    String col = registry.aggLookup("record_count", null, AggregationType.EQ);
    assertEquals("record_count", col);
  }

  // ========================================================================
  // Agg lookup
  // ========================================================================

  @Test
  void aggLookup_returnsNullWhenNotRegistered() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    String col = registry.aggLookup("nonexistent", "value", AggregationType.EQ);

    assertNull(col);
  }

  @Test
  void aggLookup_returnsPlaceholderWhenRegistered() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateAgg("level", "error", AggregationType.GTE, AggValueType.INT);

    String col = registry.aggLookup("level", "error", AggregationType.GTE);

    assertEquals("agg_f01", col);
  }

  // ========================================================================
  // Width expansion
  // ========================================================================

  @Test
  void resolveOrAllocateDim_expandsWidthInplace() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);

    // Allocate with width=64
    String col1 = registry.resolveOrAllocateDim("dim/str64/service", "str", 64);
    assertEquals("dim_f01", col1);

    // Request wider — should expand in-place (both <= 255)
    String col2 = registry.resolveOrAllocateDim("dim/str64/service", "str", 128);
    assertEquals("dim_f01", col2);

    // Verify the updated entry
    DimRegistryEntry entry = registry.getDimEntry("dim/str64/service");
    assertNotNull(entry);
    assertEquals(128, entry.width());
  }

  // ========================================================================
  // Access methods
  // ========================================================================

  @Test
  void getActiveDimColumns_returnsSortedList() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateDim("dim/str128/zebra", "str", 128);
    registry.resolveOrAllocateDim("dim/str64/apple", "str", 64);

    List<String> columns = registry.getActiveDimColumns();

    assertEquals(2, columns.size());
    // Sorted alphabetically
    assertTrue(columns.get(0).compareTo(columns.get(1)) <= 0);
  }

  @Test
  void getActiveAggColumns_returnsList() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateAgg("level", "debug", AggregationType.GTE, AggValueType.INT);
    registry.resolveOrAllocateAgg("level", "info", AggregationType.GTE, AggValueType.INT);

    List<String> columns = registry.getActiveAggColumns();

    assertEquals(2, columns.size());
  }

  @Test
  void dimColumnToKey_reverseLookup() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateDim("dim/str128/service", "str", 128);

    String key = registry.dimColumnToKey("dim_f01");

    assertEquals("dim/str128/service", key);
  }

  @Test
  void aggColumnToKey_reverseLookup() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateAgg("level", "error", AggregationType.GTE, AggValueType.INT);

    String key = registry.aggColumnToKey("agg_f01");

    assertEquals("level", key);
  }

  @Test
  void getDimKeyToColumnMap_returnsFullMap() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateDim("dim/str128/service", "str", 128);
    registry.resolveOrAllocateDim("dim/str64/region", "str", 64);

    Map<String, String> map = registry.getDimKeyToColumnMap();

    assertEquals(2, map.size());
    assertNotNull(map.get("dim/str128/service"));
    assertNotNull(map.get("dim/str64/region"));
  }

  @Test
  void getAggKeyToColumnMap_returnsFullMap() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateAgg("level", "debug", AggregationType.GTE, AggValueType.INT);

    Map<String, String> map = registry.getAggKeyToColumnMap();

    assertFalse(map.isEmpty());
  }

  // ========================================================================
  // Reader view
  // ========================================================================

  @Test
  void reader_returnsReadOnlyViewWithSameData() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateDim("dim/str128/service", "str", 128);
    registry.resolveOrAllocateAgg("level", "debug", AggregationType.GTE, AggValueType.INT);

    ColumnRegistryReader reader = registry.reader();

    assertEquals(registry.getActiveDimColumns(), reader.getActiveDimColumns());
    assertEquals(registry.getActiveAggColumns(), reader.getActiveAggColumns());
    assertEquals(
        registry.dimColumnToKey("dim_f01"), reader.dimColumnToKey("dim_f01"));
    assertEquals(
        registry.aggLookup("level", "debug", AggregationType.GTE),
        reader.aggLookup("level", "debug", AggregationType.GTE));
  }

  @Test
  void getActiveDimEntries_returnsEntries() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateDim("dim/str128/service", "str", 128);

    Collection<DimRegistryEntry> entries = registry.getActiveDimEntries();

    assertEquals(1, entries.size());
    DimRegistryEntry entry = entries.iterator().next();
    assertEquals("dim/str128/service", entry.dimKey());
    assertEquals("str", entry.baseType());
    assertEquals(128, entry.width());
  }

  @Test
  void getActiveAggEntries_returnsEntries() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateAgg("level", "debug", AggregationType.GTE, AggValueType.INT);

    Collection<AggRegistryEntry> entries = registry.getActiveAggEntries();

    assertEquals(1, entries.size());
    AggRegistryEntry entry = entries.iterator().next();
    assertEquals("level", entry.aggKey());
    assertEquals("debug", entry.aggValue());
    assertEquals(AggregationType.GTE, entry.aggregationType());
    assertEquals(AggValueType.INT, entry.valueType());
  }

  @Test
  void aggColumnToEntry_returnsFullEntry() throws SQLException {
    ColumnRegistry registry = new ColumnRegistry(getDsl(), TABLE_NAME);
    registry.resolveOrAllocateAgg("level", "error", AggregationType.GTE, AggValueType.INT);

    AggRegistryEntry entry = registry.aggColumnToEntry("agg_f01");

    assertNotNull(entry);
    assertEquals("level", entry.aggKey());
    assertEquals("error", entry.aggValue());
    assertEquals(AggregationType.GTE, entry.aggregationType());
  }

  // ========================================================================
  // Helpers
  // ========================================================================

  private boolean columnExists(String tableName, String columnName) throws SQLException {
    try (Connection conn = getDataSource().getConnection();
        ResultSet rs =
            conn.getMetaData()
                .getColumns(getTestDbName(), null, tableName, columnName)) {
      return rs.next();
    }
  }

  private String getColumnType(String tableName, String columnName) throws SQLException {
    try (Connection conn = getDataSource().getConnection();
        ResultSet rs =
            conn.getMetaData()
                .getColumns(getTestDbName(), null, tableName, columnName)) {
      if (rs.next()) {
        return rs.getString("TYPE_NAME");
      }
      throw new SQLException("Column not found: " + tableName + "." + columnName);
    }
  }

  private void addDimColumn(String columnName, String sqlType) throws SQLException {
    try (Connection conn = getDataSource().getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("ALTER TABLE " + TABLE_NAME + " ADD COLUMN " + columnName + " " + sqlType);
    }
  }

  private void insertDimRegistryEntry(
      String tableName, String columnName, String baseType, int width, String dimKey)
      throws SQLException {
    try (Connection conn = getDataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "INSERT INTO _dim_registry (table_name, column_name, base_type, width, dim_key, status) "
                    + "VALUES (?, ?, ?, ?, ?, 'ACTIVE')")) {
      ps.setString(1, tableName);
      ps.setString(2, columnName);
      ps.setString(3, baseType);
      ps.setInt(4, width);
      ps.setString(5, dimKey);
      ps.executeUpdate();
    }
  }

  private void insertAggRegistryEntry(
      String tableName,
      String columnName,
      String aggKey,
      String aggValue,
      String aggregationType,
      String valueType)
      throws SQLException {
    // First add the physical column if it looks like a placeholder
    if (columnName.startsWith("agg_f")) {
      String colDef = "FLOAT".equals(valueType) ? "DOUBLE NULL" : "BIGINT NULL";
      try (Connection conn = getDataSource().getConnection();
          Statement stmt = conn.createStatement()) {
        stmt.execute("ALTER TABLE " + TABLE_NAME + " ADD COLUMN " + columnName + " " + colDef);
      }
    }
    try (Connection conn = getDataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "INSERT INTO _agg_registry (table_name, column_name, agg_key, agg_value, "
                    + "aggregation_type, value_type, status) "
                    + "VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')")) {
      ps.setString(1, tableName);
      ps.setString(2, columnName);
      ps.setString(3, aggKey);
      ps.setString(4, aggValue);
      ps.setString(5, aggregationType);
      ps.setString(6, valueType);
      ps.executeUpdate();
    }
  }
}
