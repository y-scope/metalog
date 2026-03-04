package com.yscope.clp.service.metastore.schema;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.clp.service.metastore.Columns;
import com.yscope.clp.service.metastore.model.AggRegistryEntry;
import com.yscope.clp.service.metastore.model.AggValueType;
import com.yscope.clp.service.metastore.model.AggregationType;
import com.yscope.clp.service.metastore.model.DimRegistryEntry;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@DisplayName("ColumnRegistry Unit Tests")
class ColumnRegistryTest {

  private static final String TABLE = "clp_ir_spark";

  // ==========================================
  // Placeholder Name Format Tests
  // ==========================================

  @Test
  @DisplayName("dimPlaceholder generates correct format")
  void testDimPlaceholder_format() {
    assertEquals("dim_f01", Columns.dimPlaceholder(1));
    assertEquals("dim_f02", Columns.dimPlaceholder(2));
    assertEquals("dim_f10", Columns.dimPlaceholder(10));
    assertEquals("dim_f99", Columns.dimPlaceholder(99));
    assertEquals("dim_f100", Columns.dimPlaceholder(100));
  }

  @Test
  @DisplayName("aggPlaceholder generates correct format")
  void testAggPlaceholder_format() {
    assertEquals("agg_f01", Columns.aggPlaceholder(1));
    assertEquals("agg_f02", Columns.aggPlaceholder(2));
    assertEquals("agg_f10", Columns.aggPlaceholder(10));
    assertEquals("agg_f99", Columns.aggPlaceholder(99));
  }

  @Test
  @DisplayName("dimPlaceholder rejects invalid slots")
  void testDimPlaceholder_rejectsInvalid() {
    assertThrows(IllegalArgumentException.class, () -> Columns.dimPlaceholder(0));
    assertThrows(IllegalArgumentException.class, () -> Columns.dimPlaceholder(-1));
    assertThrows(IllegalArgumentException.class, () -> Columns.dimPlaceholder(1000));
  }

  @Test
  @DisplayName("aggPlaceholder rejects invalid slots")
  void testAggPlaceholder_rejectsInvalid() {
    assertThrows(IllegalArgumentException.class, () -> Columns.aggPlaceholder(0));
    assertThrows(IllegalArgumentException.class, () -> Columns.aggPlaceholder(-1));
    assertThrows(IllegalArgumentException.class, () -> Columns.aggPlaceholder(1000));
  }

  // ==========================================
  // SQL Type Mapping Tests
  // ==========================================

  @ParameterizedTest
  @CsvSource({
    "str, 255, VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin",
    "str, 1024, VARCHAR(1024) CHARACTER SET ascii COLLATE ascii_bin",
    "str_utf8, 255, VARCHAR(255)",
    "str_utf8, 768, VARCHAR(768)",
    "bool, 0, TINYINT(1)",
    "int, 0, BIGINT",
    "float, 0, DOUBLE"
  })
  @DisplayName("toSqlType maps correctly")
  void testToSqlType(String baseType, int width, String expectedSql) {
    assertEquals(expectedSql, ColumnRegistry.toSqlType(baseType, width));
  }

  @Test
  @DisplayName("toSqlType rejects unknown type")
  void testToSqlType_rejectsUnknown() {
    assertThrows(IllegalArgumentException.class, () -> ColumnRegistry.toSqlType("varchar", 128));
  }

  // ==========================================
  // Constructor and Load Tests
  // ==========================================

  @Nested
  @DisplayName("Constructor and Load")
  class ConstructorAndLoad {

    @Test
    void testConstructor_emptyRegistry() throws SQLException {
      ColumnRegistry reg = registryWith(ctx -> {
        if (isDimSelect(ctx.sql())) return emptyDimResult();
        if (isAggSelect(ctx.sql())) return emptyAggResult();
        return rowsAffected(0);
      });

      assertTrue(reg.getActiveDimColumns().isEmpty());
      assertTrue(reg.getActiveAggColumns().isEmpty());
      assertNull(reg.getDimEntry("any"));
      assertNull(reg.aggColumnToEntry("any"));
    }

    @Test
    void testConstructor_loadsDimEntries() throws SQLException {
      ColumnRegistry reg = registryWith(ctx -> {
        if (isDimSelect(ctx.sql())) {
          return dimRows(
              dimRow("dim_f01", "str", 128, "service.name"),
              dimRow("dim_f02", "int", null, "status_code"));
        }
        if (isAggSelect(ctx.sql())) return emptyAggResult();
        return rowsAffected(0);
      });

      assertEquals(2, reg.getActiveDimColumns().size());
      assertNotNull(reg.getDimEntry("service.name"));
      assertEquals("dim_f01", reg.getDimEntry("service.name").columnName());
      assertNotNull(reg.getDimEntry("status_code"));
      assertEquals("dim_f02", reg.getDimEntry("status_code").columnName());
    }

    @Test
    void testConstructor_loadsAggEntries() throws SQLException {
      ColumnRegistry reg = registryWith(ctx -> {
        if (isDimSelect(ctx.sql())) return emptyDimResult();
        if (isAggSelect(ctx.sql())) {
          return aggRows(
              aggRow("agg_f01", "level", "warn", "EQ", "INT"),
              aggRow("agg_f02", "cpu", null, "SUM", "FLOAT"));
        }
        return rowsAffected(0);
      });

      assertEquals(2, reg.getActiveAggColumns().size());
      assertNotNull(reg.aggColumnToEntry("agg_f01"));
      assertEquals("level", reg.aggColumnToEntry("agg_f01").aggKey());
      assertEquals(AggregationType.SUM, reg.aggColumnToEntry("agg_f02").aggregationType());
    }

    @Test
    void testConstructor_aliasEntries_skipSlotCounting() throws SQLException {
      // An alias dim entry should not advance the slot counter. Next allocated slot should be 1.
      ColumnRegistry reg = registryWith(ctx -> {
        if (isDimSelect(ctx.sql())) {
          return dimRows(dimAliasRow("clp_ir_storage_backend", "str", 128, "backend"));
        }
        if (isAggSelect(ctx.sql())) {
          return aggRows(aggAliasRow("record_count", "count", null, "EQ", "INT"));
        }
        return rowsAffected(0);
      });

      // Alias entries are in the cache
      assertNotNull(reg.getDimEntry("backend"));
      assertEquals("clp_ir_storage_backend", reg.getDimEntry("backend").effectiveColumn());
      assertNotNull(reg.aggColumnToEntry("record_count"));
    }

    @Test
    void testConstructor_dbFailure_throwsSQLException() {
      assertThrows(SQLException.class, () -> registryWith(ctx -> {
        if (isDimSelect(ctx.sql())) {
          throw new SQLException("Connection refused");
        }
        return rowsAffected(0);
      }));
    }
  }

  // ==========================================
  // ResolveOrAllocateDim Tests
  // ==========================================

  @Nested
  @DisplayName("ResolveOrAllocateDim")
  class ResolveOrAllocateDimTests {

    @Test
    void testResolveOrAllocateDim_existingKey_returnsCachedColumn() throws SQLException {
      List<String> executedSql = new ArrayList<>();
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        executedSql.add(sql);
        if (isDimSelect(sql)) return dimRows(dimRow("dim_f01", "str", 128, "service.name"));
        if (isAggSelect(sql)) return emptyAggResult();
        return rowsAffected(0);
      });
      executedSql.clear();

      String col = reg.resolveOrAllocateDim("service.name", "str", 128);

      assertEquals("dim_f01", col);
      // No INSERT or ALTER should have been executed
      assertTrue(executedSql.isEmpty());
    }

    @Test
    void testResolveOrAllocateDim_typeConflict_returnsCachedColumn() throws SQLException {
      ColumnRegistry reg = registryWith(ctx -> {
        if (isDimSelect(ctx.sql())) return dimRows(dimRow("dim_f01", "str", 128, "myfield"));
        if (isAggSelect(ctx.sql())) return emptyAggResult();
        return rowsAffected(0);
      });

      // Request as "int" instead of "str" — should warn but return existing column
      String col = reg.resolveOrAllocateDim("myfield", "int", 0);
      assertEquals("dim_f01", col);
    }

    @Test
    void testResolveOrAllocateDim_newKey_insertsAndAlters() throws SQLException {
      List<String> executedSql = new ArrayList<>();
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) return emptyDimResult();
        if (isAggSelect(sql)) return emptyAggResult();
        executedSql.add(sql);
        return rowsAffected(1);
      });

      String col = reg.resolveOrAllocateDim("service.name", "str", 128);

      assertEquals("dim_f01", col);
      assertTrue(executedSql.stream().anyMatch(s -> s.contains("_dim_registry")));
      assertTrue(executedSql.stream().anyMatch(s -> s.contains("ALTER TABLE")));
    }

    @Test
    void testResolveOrAllocateDim_widthExpansion_inplace() throws SQLException {
      List<String> executedSql = new ArrayList<>();
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) return dimRows(dimRow("dim_f01", "str", 128, "myfield"));
        if (isAggSelect(sql)) return emptyAggResult();
        executedSql.add(sql);
        return rowsAffected(1);
      });
      executedSql.clear();

      // Expand width from 128 -> 200 (both <= 255, so INPLACE resize)
      String col = reg.resolveOrAllocateDim("myfield", "str", 200);

      assertEquals("dim_f01", col);
      // Should have MODIFY COLUMN + UPDATE width
      assertTrue(executedSql.stream().anyMatch(s -> s.contains("MODIFY COLUMN")));
      assertTrue(executedSql.stream().anyMatch(s -> s.contains("_dim_registry")));
    }

    @Test
    void testResolveOrAllocateDim_widthExpansion_crossBoundary() throws SQLException {
      List<String> executedSql = new ArrayList<>();
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) return dimRows(dimRow("dim_f01", "str", 200, "myfield"));
        if (isAggSelect(sql)) return emptyAggResult();
        executedSql.add(sql);
        return rowsAffected(1);
      });
      executedSql.clear();

      // Expand width from 200 -> 300 (crosses 255 boundary → invalidate + new slot)
      String col = reg.resolveOrAllocateDim("myfield", "str", 300);

      // Should get a new slot (dim_f02 since dim_f01 was highest)
      assertEquals("dim_f02", col);
      // Should have UPDATE (invalidate status) and then INSERT + ALTER for new slot
      // "INVALIDATED" is a bind param, so check for update on _dim_registry instead
      assertTrue(executedSql.stream().anyMatch(s ->
          s.contains("_dim_registry") && s.contains("update")));
      assertTrue(executedSql.stream().anyMatch(s -> s.contains("ADD COLUMN")));
    }

    @Test
    void testResolveOrAllocateDim_duplicateKeyRace() throws SQLException {
      // First INSERT throws duplicate → reloads from DB → returns winner.
      // Discriminator: loadActiveEntries has no WHERE on dim_key;
      // reloadDimEntry has "dim_key` = ?" in WHERE.
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) {
          if (isDimKeyFilter(sql)) {
            // This is the reload query — return the "winning" entry
            return dimRows(dimRow("dim_f99", "str", 128, "service.name"));
          }
          return emptyDimResult();
        }
        if (isAggSelect(sql)) return emptyAggResult();
        if (sql.contains("insert into") || sql.contains("INSERT INTO")) {
          throw new DataAccessException("Duplicate entry",
              new SQLException("Duplicate entry 'service.name' for key 'PRIMARY'"));
        }
        return rowsAffected(0);
      });

      String col = reg.resolveOrAllocateDim("service.name", "str", 128);

      assertEquals("dim_f99", col);
    }
  }

  // ==========================================
  // ResolveOrAllocateAgg Tests
  // ==========================================

  @Nested
  @DisplayName("ResolveOrAllocateAgg")
  class ResolveOrAllocateAggTests {

    @Test
    void testResolveOrAllocateAgg_existingKey_returnsCachedColumn() throws SQLException {
      ColumnRegistry reg = registryWith(ctx -> {
        if (isDimSelect(ctx.sql())) return emptyDimResult();
        if (isAggSelect(ctx.sql())) {
          return aggRows(aggRow("agg_f01", "level", "warn", "EQ", "INT"));
        }
        return rowsAffected(0);
      });

      String col = reg.resolveOrAllocateAgg("level", "warn", AggregationType.EQ, AggValueType.INT);
      assertEquals("agg_f01", col);
    }

    @Test
    void testResolveOrAllocateAgg_valueTypeConflict_returnsCachedColumn() throws SQLException {
      ColumnRegistry reg = registryWith(ctx -> {
        if (isDimSelect(ctx.sql())) return emptyDimResult();
        if (isAggSelect(ctx.sql())) {
          return aggRows(aggRow("agg_f01", "cpu", null, "SUM", "FLOAT"));
        }
        return rowsAffected(0);
      });

      // Request as INT when registered as FLOAT — should warn but return existing
      String col = reg.resolveOrAllocateAgg("cpu", null, AggregationType.SUM, AggValueType.INT);
      assertEquals("agg_f01", col);
    }

    @Test
    void testResolveOrAllocateAgg_newKey_floatType() throws SQLException {
      List<String> executedSql = new ArrayList<>();
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) return emptyDimResult();
        if (isAggSelect(sql)) return emptyAggResult();
        executedSql.add(sql);
        return rowsAffected(1);
      });

      String col = reg.resolveOrAllocateAgg("cpu", null, AggregationType.AVG, AggValueType.FLOAT);

      assertEquals("agg_f01", col);
      assertTrue(executedSql.stream().anyMatch(s -> s.contains("ALTER TABLE")));
    }

    @Test
    void testResolveOrAllocateAgg_newKey_intType() throws SQLException {
      List<String> executedSql = new ArrayList<>();
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) return emptyDimResult();
        if (isAggSelect(sql)) return emptyAggResult();
        executedSql.add(sql);
        return rowsAffected(1);
      });

      String col =
          reg.resolveOrAllocateAgg("level", "warn", AggregationType.EQ, AggValueType.INT);

      assertEquals("agg_f01", col);
      assertTrue(executedSql.stream().anyMatch(s -> s.contains("_agg_registry")));
    }

    @Test
    void testResolveOrAllocateAgg_duplicateKeyRace() throws SQLException {
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) return emptyDimResult();
        if (isAggSelect(sql)) {
          if (isAggKeyFilter(sql)) {
            // Reload query — return winning entry
            return aggRows(aggRow("agg_f99", "level", "warn", "EQ", "INT"));
          }
          return emptyAggResult();
        }
        if (sql.contains("insert into") || sql.contains("INSERT INTO")) {
          throw new DataAccessException("Duplicate entry",
              new SQLException("Duplicate entry for key 'PRIMARY'"));
        }
        return rowsAffected(0);
      });

      String col = reg.resolveOrAllocateAgg("level", "warn", AggregationType.EQ, AggValueType.INT);

      assertEquals("agg_f99", col);
    }
  }

  // ==========================================
  // Batch Allocate Dims Tests
  // ==========================================

  @Nested
  @DisplayName("BatchAllocateDims")
  class BatchAllocateDimsTests {

    @Test
    void testResolveOrAllocateDims_allExisting() throws SQLException {
      List<String> executedSql = new ArrayList<>();
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) {
          return dimRows(
              dimRow("dim_f01", "str", 128, "svc"),
              dimRow("dim_f02", "int", null, "code"));
        }
        if (isAggSelect(sql)) return emptyAggResult();
        executedSql.add(sql);
        return rowsAffected(0);
      });
      executedSql.clear();

      Map<String, ColumnRegistry.DimSpec> specs = new LinkedHashMap<>();
      specs.put("svc", new ColumnRegistry.DimSpec("str", 128));
      specs.put("code", new ColumnRegistry.DimSpec("int", 0));

      Map<String, String> result = reg.resolveOrAllocateDims(specs);

      assertEquals(2, result.size());
      assertEquals("dim_f01", result.get("svc"));
      assertEquals("dim_f02", result.get("code"));
      assertTrue(executedSql.isEmpty());
    }

    @Test
    void testResolveOrAllocateDims_mixedExistingAndNew() throws SQLException {
      List<String> executedSql = new ArrayList<>();
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) {
          if (isDimKeyFilter(sql)) {
            // SELECT back after INSERT IGNORE — return the new entry
            return dimRows(dimRow("dim_f02", "str", 255, "region"));
          }
          return dimRows(dimRow("dim_f01", "str", 128, "svc"));
        }
        if (isAggSelect(sql)) return emptyAggResult();
        executedSql.add(sql);
        return rowsAffected(1);
      });

      Map<String, ColumnRegistry.DimSpec> specs = new LinkedHashMap<>();
      specs.put("svc", new ColumnRegistry.DimSpec("str", 128));
      specs.put("region", new ColumnRegistry.DimSpec("str", 255));

      Map<String, String> result = reg.resolveOrAllocateDims(specs);

      assertEquals(2, result.size());
      assertEquals("dim_f01", result.get("svc"));
      assertNotNull(result.get("region"));
    }

    @Test
    void testResolveOrAllocateDims_allNew() throws SQLException {
      List<String> executedSql = new ArrayList<>();
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) {
          if (isDimKeyFilter(sql)) {
            return dimRows(
                dimRow("dim_f01", "str", 128, "svc"),
                dimRow("dim_f02", "int", null, "code"));
          }
          return emptyDimResult();
        }
        if (isAggSelect(sql)) return emptyAggResult();
        executedSql.add(sql);
        return rowsAffected(1);
      });

      Map<String, ColumnRegistry.DimSpec> specs = new LinkedHashMap<>();
      specs.put("svc", new ColumnRegistry.DimSpec("str", 128));
      specs.put("code", new ColumnRegistry.DimSpec("int", 0));

      Map<String, String> result = reg.resolveOrAllocateDims(specs);

      assertEquals(2, result.size());
      // Should have done INSERT IGNORE and ALTER TABLE
      assertTrue(executedSql.stream().anyMatch(s -> s.contains("INSERT IGNORE")));
      assertTrue(executedSql.stream().anyMatch(s -> s.contains("ALTER TABLE")));
    }
  }

  // ==========================================
  // Batch Allocate Aggs Tests
  // ==========================================

  @Nested
  @DisplayName("BatchAllocateAggs")
  class BatchAllocateAggsTests {

    @Test
    void testResolveOrAllocateAggs_withAliases() throws SQLException {
      List<String> executedSql = new ArrayList<>();
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) return emptyDimResult();
        if (isAggSelect(sql)) return emptyAggResult();
        executedSql.add(sql);
        return rowsAffected(1);
      });

      String compositeKey = ColumnRegistryReader.aggCacheKey("count", null, AggregationType.EQ);
      Map<String, ColumnRegistry.AggSpec> specs = new LinkedHashMap<>();
      specs.put(compositeKey, new ColumnRegistry.AggSpec(
          "count", null, AggregationType.EQ, AggValueType.INT, "record_count"));

      Map<String, String> result = reg.resolveOrAllocateAggs(specs);

      assertEquals(1, result.size());
      assertEquals("record_count", result.get(compositeKey));
      // Should have INSERT into _agg_registry for alias, but no ALTER TABLE
      assertTrue(executedSql.stream().anyMatch(s -> s.contains("_agg_registry")));
      assertTrue(executedSql.stream().noneMatch(s -> s.contains("ALTER TABLE")));
    }

    @Test
    void testResolveOrAllocateAggs_mixedAliasAndRegular() throws SQLException {
      List<String> executedSql = new ArrayList<>();
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) return emptyDimResult();
        if (isAggSelect(sql)) {
          if (isAggKeyFilter(sql)) {
            return aggRows(aggRow("agg_f01", "level", "warn", "EQ", "INT"));
          }
          return emptyAggResult();
        }
        executedSql.add(sql);
        return rowsAffected(1);
      });

      String aliasKey = ColumnRegistryReader.aggCacheKey("count", null, AggregationType.EQ);
      String regularKey =
          ColumnRegistryReader.aggCacheKey("level", "warn", AggregationType.EQ);
      Map<String, ColumnRegistry.AggSpec> specs = new LinkedHashMap<>();
      specs.put(aliasKey, new ColumnRegistry.AggSpec(
          "count", null, AggregationType.EQ, AggValueType.INT, "record_count"));
      specs.put(regularKey, new ColumnRegistry.AggSpec(
          "level", "warn", AggregationType.EQ, AggValueType.INT));

      Map<String, String> result = reg.resolveOrAllocateAggs(specs);

      assertEquals(2, result.size());
      assertEquals("record_count", result.get(aliasKey));
    }
  }

  // ==========================================
  // Register Alias Tests
  // ==========================================

  @Nested
  @DisplayName("RegisterAlias")
  class RegisterAliasTests {

    @Test
    void testRegisterAggAlias_insertsAndCaches() throws SQLException {
      ColumnRegistry reg = registryWith(ctx -> {
        if (isDimSelect(ctx.sql())) return emptyDimResult();
        if (isAggSelect(ctx.sql())) return emptyAggResult();
        return rowsAffected(1);
      });

      reg.registerAggAlias(
          "count", null, AggregationType.EQ, AggValueType.INT, "record_count");

      assertNotNull(reg.aggColumnToEntry("record_count"));
      assertEquals("count", reg.aggColumnToEntry("record_count").aggKey());
    }

    @Test
    void testRegisterAggAlias_alreadyExists_noop() throws SQLException {
      List<String> executedSql = new ArrayList<>();
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) return emptyDimResult();
        if (isAggSelect(sql)) {
          return aggRows(aggAliasRow("record_count", "count", null, "EQ", "INT"));
        }
        executedSql.add(sql);
        return rowsAffected(0);
      });
      executedSql.clear();

      reg.registerAggAlias(
          "count", null, AggregationType.EQ, AggValueType.INT, "record_count");

      // Should not issue any INSERT since the entry already exists
      assertTrue(executedSql.isEmpty());
    }

    @Test
    void testRegisterDimAlias_insertsAndCaches() throws SQLException {
      ColumnRegistry reg = registryWith(ctx -> {
        if (isDimSelect(ctx.sql())) return emptyDimResult();
        if (isAggSelect(ctx.sql())) return emptyAggResult();
        return rowsAffected(1);
      });

      reg.registerDimAlias("backend", "str", 128, "clp_ir_storage_backend");

      assertNotNull(reg.getDimEntry("backend"));
      assertEquals(
          "clp_ir_storage_backend", reg.getDimEntry("backend").effectiveColumn());
    }

    @Test
    void testRegisterDimAlias_duplicateRace() throws SQLException {
      ColumnRegistry reg = registryWith(ctx -> {
        String sql = ctx.sql();
        if (isDimSelect(sql)) {
          if (isDimKeyFilter(sql)) {
            return dimRows(dimAliasRow("clp_ir_storage_backend", "str", 128, "backend"));
          }
          return emptyDimResult();
        }
        if (isAggSelect(sql)) return emptyAggResult();
        if (sql.contains("insert into") || sql.contains("INSERT INTO")) {
          throw new DataAccessException("Duplicate entry",
              new SQLException("Duplicate entry 'backend' for key 'PRIMARY'"));
        }
        return rowsAffected(0);
      });

      // Should not throw — falls back to reload
      assertDoesNotThrow(
          () -> reg.registerDimAlias("backend", "str", 128, "clp_ir_storage_backend"));
    }
  }

  // ==========================================
  // Static Helper Tests
  // ==========================================

  @Nested
  @DisplayName("StaticHelpers")
  class StaticHelperTests {

    @Test
    void testIsStringType_strAndStrUtf8() {
      // Verify via toSqlType that string types need width, others don't
      String strResult = ColumnRegistry.toSqlType("str", 100);
      assertTrue(strResult.contains("VARCHAR"));

      String utf8Result = ColumnRegistry.toSqlType("str_utf8", 100);
      assertTrue(utf8Result.contains("VARCHAR"));

      // Non-string types produce fixed types regardless of width
      assertEquals("BIGINT", ColumnRegistry.toSqlType("int", 0));
      assertEquals("DOUBLE", ColumnRegistry.toSqlType("float", 0));
      assertEquals("TINYINT(1)", ColumnRegistry.toSqlType("bool", 0));
    }
  }

  // ==========================================
  // Helper Methods
  // ==========================================

  private ColumnRegistry registryWith(MockDataProvider provider) throws SQLException {
    DSLContext dsl = DSL.using(new MockConnection(provider), SQLDialect.MYSQL);
    return new ColumnRegistry(dsl, TABLE);
  }

  private static boolean isDimSelect(String sql) {
    return sql != null && sql.contains("_dim_registry") && sql.contains("select");
  }

  private static boolean isAggSelect(String sql) {
    return sql != null && sql.contains("_agg_registry") && sql.contains("select");
  }

  /** Detects reload/selectDimEntries queries that have dim_key in the WHERE clause. */
  private static boolean isDimKeyFilter(String sql) {
    // WHERE clause uses: `dim_key` = ? or `dim_key` in (?)
    // vs loadActiveEntries which only has dim_key in SELECT list
    return sql.contains("`dim_key` =") || sql.contains("`dim_key` in");
  }

  /** Detects reload/selectAggEntries queries that have agg_key in the WHERE clause. */
  private static boolean isAggKeyFilter(String sql) {
    return sql.contains("`agg_key` =") || sql.contains("`agg_key` in");
  }

  // --- Dim result builders ---

  private static final org.jooq.Field<String> D_TABLE = DSL.field(DSL.name("table_name"), String.class);
  private static final org.jooq.Field<String> D_COLUMN = DSL.field(DSL.name("column_name"), String.class);
  private static final org.jooq.Field<String> D_BASE_TYPE = DSL.field(DSL.name("base_type"), String.class);
  private static final org.jooq.Field<Integer> D_WIDTH = DSL.field(DSL.name("width"), Integer.class);
  private static final org.jooq.Field<String> D_DIM_KEY = DSL.field(DSL.name("dim_key"), String.class);
  private static final org.jooq.Field<String> D_ALIAS = DSL.field(DSL.name("alias_column"), String.class);
  private static final org.jooq.Field<String> D_STATUS = DSL.field(DSL.name("status"), String.class);
  private static final org.jooq.Field<Long> D_CREATED = DSL.field(DSL.name("created_at"), Long.class);
  private static final org.jooq.Field<Long> D_INVALIDATED = DSL.field(DSL.name("invalidated_at"), Long.class);

  private record DimRowData(
      String column, String baseType, Integer width, String dimKey, String alias) {}

  private static DimRowData dimRow(String column, String baseType, Integer width, String dimKey) {
    return new DimRowData(column, baseType, width, dimKey, null);
  }

  private static DimRowData dimAliasRow(
      String column, String baseType, Integer width, String dimKey) {
    return new DimRowData(column, baseType, width, dimKey, column);
  }

  private MockResult[] dimRows(DimRowData... rows) {
    var result = DSL.using(SQLDialect.MYSQL).newResult(
        D_TABLE, D_COLUMN, D_BASE_TYPE, D_WIDTH, D_DIM_KEY, D_ALIAS, D_STATUS, D_CREATED,
        D_INVALIDATED);
    for (var r : rows) {
      var rec = DSL.using(SQLDialect.MYSQL).newRecord(
          D_TABLE, D_COLUMN, D_BASE_TYPE, D_WIDTH, D_DIM_KEY, D_ALIAS, D_STATUS, D_CREATED,
          D_INVALIDATED);
      rec.set(D_TABLE, TABLE);
      rec.set(D_COLUMN, r.column());
      rec.set(D_BASE_TYPE, r.baseType());
      rec.set(D_WIDTH, r.width());
      rec.set(D_DIM_KEY, r.dimKey());
      rec.set(D_ALIAS, r.alias());
      rec.set(D_STATUS, "ACTIVE");
      rec.set(D_CREATED, null);
      rec.set(D_INVALIDATED, null);
      result.add(rec);
    }
    return new MockResult[] {new MockResult(result.size(), result)};
  }

  private MockResult[] emptyDimResult() {
    var result = DSL.using(SQLDialect.MYSQL).newResult(
        D_TABLE, D_COLUMN, D_BASE_TYPE, D_WIDTH, D_DIM_KEY, D_ALIAS, D_STATUS, D_CREATED,
        D_INVALIDATED);
    return new MockResult[] {new MockResult(0, result)};
  }

  // --- Agg result builders ---

  private static final org.jooq.Field<String> A_TABLE = DSL.field(DSL.name("table_name"), String.class);
  private static final org.jooq.Field<String> A_COLUMN = DSL.field(DSL.name("column_name"), String.class);
  private static final org.jooq.Field<String> A_AGG_KEY = DSL.field(DSL.name("agg_key"), String.class);
  private static final org.jooq.Field<String> A_AGG_VALUE = DSL.field(DSL.name("agg_value"), String.class);
  private static final org.jooq.Field<String> A_AGG_TYPE = DSL.field(DSL.name("aggregation_type"), String.class);
  private static final org.jooq.Field<String> A_VALUE_TYPE = DSL.field(DSL.name("value_type"), String.class);
  private static final org.jooq.Field<String> A_ALIAS = DSL.field(DSL.name("alias_column"), String.class);
  private static final org.jooq.Field<String> A_STATUS = DSL.field(DSL.name("status"), String.class);
  private static final org.jooq.Field<Long> A_CREATED = DSL.field(DSL.name("created_at"), Long.class);
  private static final org.jooq.Field<Long> A_INVALIDATED = DSL.field(DSL.name("invalidated_at"), Long.class);

  private record AggRowData(
      String column, String aggKey, String aggValue, String aggType, String valueType,
      String alias) {}

  private static AggRowData aggRow(
      String column, String aggKey, String aggValue, String aggType, String valueType) {
    return new AggRowData(column, aggKey, aggValue, aggType, valueType, null);
  }

  private static AggRowData aggAliasRow(
      String column, String aggKey, String aggValue, String aggType, String valueType) {
    return new AggRowData(column, aggKey, aggValue, aggType, valueType, column);
  }

  private MockResult[] aggRows(AggRowData... rows) {
    var result = DSL.using(SQLDialect.MYSQL).newResult(
        A_TABLE, A_COLUMN, A_AGG_KEY, A_AGG_VALUE, A_AGG_TYPE, A_VALUE_TYPE, A_ALIAS, A_STATUS,
        A_CREATED, A_INVALIDATED);
    for (var r : rows) {
      var rec = DSL.using(SQLDialect.MYSQL).newRecord(
          A_TABLE, A_COLUMN, A_AGG_KEY, A_AGG_VALUE, A_AGG_TYPE, A_VALUE_TYPE, A_ALIAS, A_STATUS,
          A_CREATED, A_INVALIDATED);
      rec.set(A_TABLE, TABLE);
      rec.set(A_COLUMN, r.column());
      rec.set(A_AGG_KEY, r.aggKey());
      rec.set(A_AGG_VALUE, r.aggValue());
      rec.set(A_AGG_TYPE, r.aggType());
      rec.set(A_VALUE_TYPE, r.valueType());
      rec.set(A_ALIAS, r.alias());
      rec.set(A_STATUS, "ACTIVE");
      rec.set(A_CREATED, null);
      rec.set(A_INVALIDATED, null);
      result.add(rec);
    }
    return new MockResult[] {new MockResult(result.size(), result)};
  }

  private MockResult[] emptyAggResult() {
    var result = DSL.using(SQLDialect.MYSQL).newResult(
        A_TABLE, A_COLUMN, A_AGG_KEY, A_AGG_VALUE, A_AGG_TYPE, A_VALUE_TYPE, A_ALIAS, A_STATUS,
        A_CREATED, A_INVALIDATED);
    return new MockResult[] {new MockResult(0, result)};
  }

  private MockResult[] rowsAffected(int count) {
    return new MockResult[] {new MockResult(count)};
  }
}
