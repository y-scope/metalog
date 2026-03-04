package com.yscope.metalog.query.core.splits;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.yscope.metalog.metastore.model.AggregationType;
import com.yscope.metalog.metastore.schema.ColumnRegistry;
import java.util.LinkedHashMap;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

class FilterExpressionResolverTest {

  private ColumnRegistry registry;

  @BeforeEach
  void setUp() {
    registry = mock(ColumnRegistry.class);

    // Dim mappings: logical key -> physical column
    Map<String, String> dimMap = new LinkedHashMap<>();
    dimMap.put("service.name", "dim_f01");
    dimMap.put("k8s.pod.name", "dim_f02");
    dimMap.put("region", "dim_f03");
    dimMap.put("host-name", "dim_f04");
    when(registry.getDimKeyToColumnMap()).thenReturn(dimMap);

    // Agg mappings: logical key -> physical column
    Map<String, String> aggMap = new LinkedHashMap<>();
    aggMap.put("level", "agg_f01");
    aggMap.put("error.count", "agg_f02");
    when(registry.getAggKeyToColumnMap()).thenReturn(aggMap);

    // aggLookup for namespace-prefixed resolution
    when(registry.aggLookup("level", "warn", AggregationType.EQ)).thenReturn("agg_f01_warn_eq");
    when(registry.aggLookup("level", "warn", AggregationType.GTE)).thenReturn("agg_f01_warn_gte");
    when(registry.aggLookup("level", null, AggregationType.EQ)).thenReturn("agg_f01_eq");
  }

  @Nested
  class PassthroughCases {

    @ParameterizedTest
    @NullAndEmptySource
    void nullAndEmpty_passThrough(String input) {
      assertEquals(input, FilterExpressionResolver.resolve(input, registry));
    }

    @Test
    void nullRegistry_passThrough() {
      String expr = "service.name = 'foo'";
      assertEquals(expr, FilterExpressionResolver.resolve(expr, null));
    }

    @Test
    void unparseable_deferredToValidator() {
      // Resolver cannot parse this, so it returns unchanged.
      // The downstream FilterExpressionValidator MUST reject it.
      String expr = "AND AND AND";
      assertEquals(expr, FilterExpressionResolver.resolve(expr, registry));
    }
  }

  @Nested
  class DimResolution {

    @Test
    void simpleName() {
      String result = FilterExpressionResolver.resolve("region = 'us-east-1'", registry);
      assertEquals("dim_f03 = 'us-east-1'", result);
    }

    @Test
    void dottedName() {
      String result = FilterExpressionResolver.resolve("service.name = 'foo'", registry);
      assertEquals("dim_f01 = 'foo'", result);
    }

    @Test
    void deeplyDottedName() {
      String result = FilterExpressionResolver.resolve("k8s.pod.name = 'my-pod'", registry);
      assertEquals("dim_f02 = 'my-pod'", result);
    }

    @Test
    void backtickQuoted() {
      String result = FilterExpressionResolver.resolve("`service.name` = 'bar'", registry);
      assertEquals("dim_f01 = 'bar'", result);
    }
  }

  @Nested
  class AggResolution {

    @Test
    void simpleAggKey() {
      String result = FilterExpressionResolver.resolve("level > 100", registry);
      assertEquals("agg_f01 > 100", result);
    }

    @Test
    void dottedAggKey() {
      String result = FilterExpressionResolver.resolve("error.count >= 5", registry);
      assertEquals("agg_f02 >= 5", result);
    }
  }

  @Nested
  class FilePassthrough {

    @Test
    void stateUnchanged() {
      String result = FilterExpressionResolver.resolve("state = 'IR_CLOSED'", registry);
      assertEquals("state = 'IR_CLOSED'", result);
    }

    @Test
    void minTimestampUnchanged() {
      String result = FilterExpressionResolver.resolve("min_timestamp >= 1679711330", registry);
      assertEquals("min_timestamp >= 1679711330", result);
    }

    @Test
    void maxTimestampUnchanged() {
      String result = FilterExpressionResolver.resolve("max_timestamp <= 1679976000", registry);
      assertEquals("max_timestamp <= 1679976000", result);
    }
  }

  @Nested
  class MixedExpressions {

    @Test
    void dimAndFile() {
      String result =
          FilterExpressionResolver.resolve(
              "service.name = 'foo' AND state = 'IR_CLOSED'", registry);
      assertEquals("dim_f01 = 'foo' AND state = 'IR_CLOSED'", result);
    }

    @Test
    void dimAndAgg() {
      String result =
          FilterExpressionResolver.resolve("region = 'us-east-1' AND level > 100", registry);
      assertEquals("dim_f03 = 'us-east-1' AND agg_f01 > 100", result);
    }

    @Test
    void dimAggAndFile() {
      String result =
          FilterExpressionResolver.resolve(
              "service.name = 'foo' AND level > 100 AND state = 'IR_CLOSED'", registry);
      assertEquals("dim_f01 = 'foo' AND agg_f01 > 100 AND state = 'IR_CLOSED'", result);
    }
  }

  @Nested
  class BooleanOperators {

    @Test
    void notExpression() {
      String result = FilterExpressionResolver.resolve("NOT (service.name = 'foo')", registry);
      assertEquals("NOT (dim_f01 = 'foo')", result);
    }

    @Test
    void inExpression() {
      String result =
          FilterExpressionResolver.resolve("region IN ('us-east-1', 'eu-west-1')", registry);
      assertEquals("dim_f03 IN ('us-east-1', 'eu-west-1')", result);
    }

    @Test
    void isNull() {
      String result = FilterExpressionResolver.resolve("service.name IS NULL", registry);
      assertEquals("dim_f01 IS NULL", result);
    }

    @Test
    void isNotNull() {
      String result = FilterExpressionResolver.resolve("service.name IS NOT NULL", registry);
      assertEquals("dim_f01 IS NOT NULL", result);
    }

    @Test
    void between() {
      String result = FilterExpressionResolver.resolve("level BETWEEN 1 AND 100", registry);
      assertEquals("agg_f01 BETWEEN 1 AND 100", result);
    }

    @Test
    void orExpression() {
      String result =
          FilterExpressionResolver.resolve(
              "service.name = 'foo' OR service.name = 'bar'", registry);
      assertEquals("dim_f01 = 'foo' OR dim_f01 = 'bar'", result);
    }
  }

  @Nested
  class UnknownColumns {

    @Test
    void unknownColumn_passThrough() {
      String result = FilterExpressionResolver.resolve("unknown_col = 'value'", registry);
      assertEquals("unknown_col = 'value'", result);
    }

    @Test
    void unknownDottedColumn_passThrough() {
      String result = FilterExpressionResolver.resolve("unknown.field = 'value'", registry);
      assertEquals("unknown.field = 'value'", result);
    }
  }

  @Nested
  class ReconstructLogicalKey {

    @Test
    void simpleColumn() {
      Column col = new Column("state");
      assertEquals("state", FilterExpressionResolver.reconstructLogicalKey(col));
    }

    @Test
    void nullColumnName() {
      Column col = new Column((String) null);
      assertNull(FilterExpressionResolver.reconstructLogicalKey(col));
    }

    @Test
    void threePartName_reassemblesSchemaTableColumn() {
      // k8s.pod.name → schema="k8s", table="pod", col="name"
      Column col = new Column();
      col.setColumnName("name");
      Table table = new Table("k8s", "pod");
      col.setTable(table);

      assertEquals("k8s.pod.name", FilterExpressionResolver.reconstructLogicalKey(col));
    }
  }

  // ========================================================================
  // resolveColumnName — namespace-prefixed single-column resolution
  // ========================================================================

  @Nested
  class ResolveColumnName {

    @Test
    void testFilePrefix_state_returnsState() {
      String result = FilterExpressionResolver.resolveColumnName("__FILE.state", registry);
      assertEquals("state", result);
    }

    @Test
    void testFilePrefix_unknownColumn_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () -> FilterExpressionResolver.resolveColumnName("__FILE.unknown_col", registry));
    }

    @Test
    void testDimPrefix_resolves() {
      String result = FilterExpressionResolver.resolveColumnName("__DIM.service.name", registry);
      assertEquals("dim_f01", result);
    }

    @Test
    void testDimPrefix_unknown_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () -> FilterExpressionResolver.resolveColumnName("__DIM.unknown", registry));
    }

    @Test
    void testDimPrefix_nullRegistry_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () -> FilterExpressionResolver.resolveColumnName("__DIM.service.name", null));
    }

    @Test
    void testAggEqPrefix_resolves() {
      String result =
          FilterExpressionResolver.resolveColumnName("__AGG_EQ.level.warn", registry);
      assertEquals("agg_f01_warn_eq", result);
    }

    @Test
    void testAggGtePrefix_resolves() {
      String result =
          FilterExpressionResolver.resolveColumnName("__AGG_GTE.level.warn", registry);
      assertEquals("agg_f01_warn_gte", result);
    }

    @Test
    void testAggDefaultPrefix_usesEqType() {
      String result = FilterExpressionResolver.resolveColumnName("__AGG.level.warn", registry);
      assertEquals("agg_f01_warn_eq", result);
    }

    @Test
    void testAggPrefix_noDotInRemainder_aggValueNull() {
      String result = FilterExpressionResolver.resolveColumnName("__AGG.level", registry);
      assertEquals("agg_f01_eq", result);
    }

    @Test
    void testAggPrefix_nullRegistry_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () -> FilterExpressionResolver.resolveColumnName("__AGG.level.warn", null));
    }

    @Test
    void testUnqualifiedFileColumn_resolves() {
      String result = FilterExpressionResolver.resolveColumnName("state", registry);
      assertEquals("state", result);
    }

    @Test
    void testUnqualifiedDimKey_resolves() {
      String result = FilterExpressionResolver.resolveColumnName("service.name", registry);
      assertEquals("dim_f01", result);
    }

    @Test
    void testUnqualifiedAggKey_resolves() {
      String result = FilterExpressionResolver.resolveColumnName("level", registry);
      assertEquals("agg_f01", result);
    }

    @Test
    void testUnknownColumn_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () -> FilterExpressionResolver.resolveColumnName("totally_unknown", registry));
    }
  }

  // ========================================================================
  // resolveToExpression — parse + resolve, returning AST
  // ========================================================================

  @Nested
  class ResolveToExpression {

    @Test
    void testSemicolon_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () -> FilterExpressionResolver.resolveToExpression("state = 1; DROP TABLE", registry));
    }

    @Test
    void testUnparseable_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () -> FilterExpressionResolver.resolveToExpression("AND AND AND", registry));
    }

    @Test
    void testValidExpressionWithRegistry_resolves() {
      Expression expr =
          FilterExpressionResolver.resolveToExpression("service.name = 'foo'", registry);

      assertNotNull(expr);
      assertTrue(expr.toString().contains("dim_f01"));
    }

    @Test
    void testNullRegistry_noResolution() {
      Expression expr =
          FilterExpressionResolver.resolveToExpression("service.name = 'foo'", null);

      assertNotNull(expr);
      // Columns pass through unresolved
      assertTrue(expr.toString().contains("service"));
    }
  }

  // ========================================================================
  // resolveToExpressionStrict — strict mode with unknown column errors
  // ========================================================================

  @Nested
  class ResolveToExpressionStrict {

    @Test
    void testSimpleComparison_resolvesColumns() {
      Expression expr =
          FilterExpressionResolver.resolveToExpressionStrict(
              "service.name = 'foo'", registry);

      assertNotNull(expr);
      assertTrue(expr.toString().contains("dim_f01"));
    }

    @Test
    void testUnknownColumn_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              FilterExpressionResolver.resolveToExpressionStrict(
                  "unknown_xyz = 'val'", registry));
    }

    @Test
    void testNullRegistry_fallsThrough() {
      // Strict with null registry logs a warning but doesn't throw
      Expression expr =
          FilterExpressionResolver.resolveToExpressionStrict("state = 'IR_CLOSED'", null);

      assertNotNull(expr);
      assertTrue(expr.toString().contains("state"));
    }

    @Test
    void testSemicolon_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              FilterExpressionResolver.resolveToExpressionStrict(
                  "state = 1; DROP TABLE foo", registry));
    }
  }
}
