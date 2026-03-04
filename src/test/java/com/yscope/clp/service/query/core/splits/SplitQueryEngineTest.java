package com.yscope.clp.service.query.core.splits;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.yscope.clp.service.metastore.model.AggRegistryEntry;
import com.yscope.clp.service.metastore.model.AggValueType;
import com.yscope.clp.service.metastore.model.AggregationType;
import com.yscope.clp.service.metastore.model.DimRegistryEntry;
import com.yscope.clp.service.metastore.schema.ColumnRegistry;
import com.yscope.clp.service.metastore.schema.ColumnRegistryProvider;
import com.yscope.clp.service.metastore.schema.ColumnRegistryReader;
import com.yscope.clp.service.query.core.CacheService;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.SortField;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("SplitQueryEngine Unit Tests")
class SplitQueryEngineTest {

  private static final String TABLE = "clp_ir_spark";

  private ColumnRegistry mockRegistry;
  private ColumnRegistryProvider mockProvider;
  private CacheService mockCacheService;
  private SketchEvaluator mockSketchEvaluator;
  private DSLContext dsl;
  private SplitQueryEngine engine;

  @BeforeEach
  void setUp() {
    mockRegistry = mock(ColumnRegistry.class);
    mockProvider = mock(ColumnRegistryProvider.class);
    when(mockProvider.get(TABLE)).thenReturn(mockRegistry);

    mockCacheService = mock(CacheService.class);
    // Pass-through caching: just call the supplier immediately
    when(mockCacheService.getQuery(anyString(), any())).thenAnswer(inv -> {
      var supplier = inv.getArgument(1, java.util.function.Supplier.class);
      return supplier.get();
    });

    mockSketchEvaluator = mock(SketchEvaluator.class);
    dsl = DSL.using(
        new MockConnection(ctx -> new MockResult[] {new MockResult(0)}), SQLDialect.MYSQL);
    engine = new SplitQueryEngine(dsl, mockProvider, mockCacheService, mockSketchEvaluator);

    // Default dim and agg mappings
    Map<String, String> dimMap = new LinkedHashMap<>();
    dimMap.put("service.name", "dim_f01");
    dimMap.put("region", "dim_f02");
    when(mockRegistry.getDimKeyToColumnMap()).thenReturn(dimMap);

    Map<String, String> aggMap = new LinkedHashMap<>();
    aggMap.put("level", "agg_f01");
    when(mockRegistry.getAggKeyToColumnMap()).thenReturn(aggMap);

    when(mockRegistry.aggLookup("level", "warn", AggregationType.EQ))
        .thenReturn("agg_f01");
  }

  // ==========================================
  // Existing buildCursorCondition tests
  // ==========================================

  @Test
  void singleDescColumn() {
    var orderBy = List.of(new OrderBySpec("max_timestamp", Order.DESC));
    var cursor = new KeysetCursor(List.of(1704067200L), 42L);
    Condition cond = SplitQueryEngine.buildCursorCondition(orderBy, cursor);
    String sql = cond.toString();
    assertTrue(sql.contains("max_timestamp < 1704067200"), "DESC comparison missing: " + sql);
    assertTrue(sql.contains("id > 42"), "id tiebreaker missing: " + sql);
  }

  @Test
  void singleAscColumn() {
    var orderBy = List.of(new OrderBySpec("min_timestamp", Order.ASC));
    var cursor = new KeysetCursor(List.of(1704000000L), 7L);
    Condition cond = SplitQueryEngine.buildCursorCondition(orderBy, cursor);
    String sql = cond.toString();
    assertTrue(sql.contains("min_timestamp > 1704000000"), "ASC comparison missing: " + sql);
    assertTrue(sql.contains("id > 7"), "id tiebreaker missing: " + sql);
  }

  @Test
  void multiColumnMixedSort() {
    var orderBy =
        List.of(
            new OrderBySpec("record_count", Order.DESC),
            new OrderBySpec("max_timestamp", Order.ASC));
    var cursor = new KeysetCursor(List.of(1000L, 1704067200L), 42L);
    Condition cond = SplitQueryEngine.buildCursorCondition(orderBy, cursor);
    String sql = cond.toString();
    assertTrue(sql.contains("record_count < 1000"), "f1 DESC comparison missing: " + sql);
    assertTrue(sql.contains("max_timestamp > 1704067200"), "f2 ASC comparison missing: " + sql);
    assertTrue(sql.contains("id > 42"), "id tiebreaker missing: " + sql);
  }

  @Test
  void cursorSizeMismatch() {
    var orderBy = List.of(new OrderBySpec("max_timestamp", Order.DESC));
    var cursor = new KeysetCursor(List.of(100L, 200L), 1L);
    assertThrows(
        IllegalArgumentException.class,
        () -> SplitQueryEngine.buildCursorCondition(orderBy, cursor));
  }

  // ==========================================
  // PrepareSketchPredicates Tests
  // ==========================================

  @Nested
  @DisplayName("PrepareSketchPredicates")
  class PrepareSketchPredicatesTests {

    @Test
    void testPrepareSketchPredicates_nullExpression() {
      SketchExtractionResult result = engine.prepareSketchPredicates(null);
      assertNull(result.cleanedExpression());
      assertTrue(result.predicates().isEmpty());
    }

    @Test
    void testPrepareSketchPredicates_emptyExpression() {
      SketchExtractionResult result = engine.prepareSketchPredicates("");
      assertEquals("", result.cleanedExpression());
      assertTrue(result.predicates().isEmpty());
    }

    @Test
    void testPrepareSketchPredicates_noSketchTerms() {
      SketchExtractionResult result =
          engine.prepareSketchPredicates("max_timestamp > 100");
      assertEquals("max_timestamp > 100", result.cleanedExpression());
      assertTrue(result.predicates().isEmpty());
    }

    @Test
    void testPrepareSketchPredicates_withSketchTerm() {
      SketchExtractionResult result =
          engine.prepareSketchPredicates(
              "__SKETCH.service = 'web' AND max_timestamp > 100");
      // Sketch term should be extracted; cleaned expression should not contain it
      assertFalse(result.predicates().isEmpty());
      assertFalse(result.cleanedExpression().contains("__SKETCH"));
    }
  }

  // ==========================================
  // PrepareFilterCondition Tests
  // ==========================================

  @Nested
  @DisplayName("PrepareFilterCondition")
  class PrepareFilterConditionTests {

    @Test
    void testPrepareFilterCondition_null() {
      Condition result = engine.prepareFilterCondition(TABLE, null);
      assertNull(result);
    }

    @Test
    void testPrepareFilterCondition_empty() {
      Condition result = engine.prepareFilterCondition(TABLE, "");
      assertNull(result);
    }

    @Test
    void testPrepareFilterCondition_validExpression() {
      Condition result = engine.prepareFilterCondition(TABLE, "max_timestamp > 100");
      assertNotNull(result);
    }

    @Test
    void testPrepareFilterCondition_invalidExpression() {
      assertThrows(Exception.class,
          () -> engine.prepareFilterCondition(TABLE, "AND AND AND"));
    }

    @Test
    void testPrepareFilterCondition_caching() {
      // Configure a counting cache service to verify caching behavior
      AtomicInteger supplierCalls = new AtomicInteger(0);
      CacheService countingCache = mock(CacheService.class);
      when(countingCache.getQuery(anyString(), any())).thenAnswer(inv -> {
        supplierCalls.incrementAndGet();
        var supplier = inv.getArgument(1, java.util.function.Supplier.class);
        return supplier.get();
      });
      SplitQueryEngine cachedEngine =
          new SplitQueryEngine(dsl, mockProvider, countingCache, mockSketchEvaluator);

      cachedEngine.prepareFilterCondition(TABLE, "max_timestamp > 100");
      cachedEngine.prepareFilterCondition(TABLE, "max_timestamp > 100");

      // Cache service should be called twice (once per invocation) but each with the same key
      assertEquals(2, supplierCalls.get());
      // Verify same cache key used
      verify(countingCache, times(2))
          .getQuery(eq("filter:" + TABLE + ":max_timestamp > 100"), any());
    }
  }

  // ==========================================
  // PrepareOrderBy Tests
  // ==========================================

  @Nested
  @DisplayName("PrepareOrderBy")
  class PrepareOrderByTests {

    @Test
    void testPrepareOrderBy_fileColumn() {
      List<OrderBySpec> result = engine.prepareOrderBy(
          TABLE, List.of(new OrderBySpec("__FILE.max_timestamp", Order.DESC)));

      assertEquals(1, result.size());
      assertEquals("max_timestamp", result.get(0).column());
      assertEquals(Order.DESC, result.get(0).order());
    }

    @Test
    void testPrepareOrderBy_dimColumn() {
      List<OrderBySpec> result = engine.prepareOrderBy(
          TABLE, List.of(new OrderBySpec("__DIM.service.name", Order.ASC)));

      assertEquals(1, result.size());
      assertEquals("dim_f01", result.get(0).column());
    }

    @Test
    void testPrepareOrderBy_unknownColumn() {
      assertThrows(IllegalArgumentException.class,
          () -> engine.prepareOrderBy(
              TABLE, List.of(new OrderBySpec("__FILE.nonexistent", Order.ASC))));
    }
  }

  // ==========================================
  // PrepareProjection Tests
  // ==========================================

  @Nested
  @DisplayName("PrepareProjection")
  class PrepareProjectionTests {

    @Test
    void testPrepareProjection_nullTerms() {
      ResolvedProjection result = engine.prepareProjection(TABLE, null);
      assertTrue(result.isAll());
    }

    @Test
    void testPrepareProjection_emptyTerms() {
      ResolvedProjection result = engine.prepareProjection(TABLE, List.of());
      assertTrue(result.isAll());
    }

    @Test
    void testPrepareProjection_star() {
      ResolvedProjection result = engine.prepareProjection(TABLE, List.of("*"));
      assertTrue(result.isAll());
    }

    @Test
    void testPrepareProjection_fileWildcard() {
      ResolvedProjection result = engine.prepareProjection(TABLE, List.of("__FILE.*"));
      assertNull(result.fileColumns()); // null = all file columns
      assertNotNull(result.dimColumns()); // non-null = specific set (empty)
    }

    @Test
    void testPrepareProjection_specificFileColumn() {
      ResolvedProjection result =
          engine.prepareProjection(TABLE, List.of("__FILE.max_timestamp"));
      assertNotNull(result.fileColumns());
      assertTrue(result.fileColumns().contains("max_timestamp"));
    }

    @Test
    void testPrepareProjection_unknownFileColumn() {
      assertThrows(IllegalArgumentException.class,
          () -> engine.prepareProjection(TABLE, List.of("__FILE.nonexistent_col")));
    }

    @Test
    void testPrepareProjection_dimWildcard() {
      ResolvedProjection result = engine.prepareProjection(TABLE, List.of("__DIM.*"));
      assertNull(result.dimColumns()); // null = all dim columns
    }

    @Test
    void testPrepareProjection_specificDimColumn() {
      ResolvedProjection result =
          engine.prepareProjection(TABLE, List.of("__DIM.service.name"));
      assertNotNull(result.dimColumns());
      assertTrue(result.dimColumns().contains("dim_f01"));
    }

    @Test
    void testPrepareProjection_unknownDimKey() {
      assertThrows(IllegalArgumentException.class,
          () -> engine.prepareProjection(TABLE, List.of("__DIM.nonexistent")));
    }

    @Test
    void testPrepareProjection_aggWildcard() {
      ResolvedProjection result = engine.prepareProjection(TABLE, List.of("__AGG.*"));
      assertNull(result.aggColumns()); // null = all agg columns
    }

    @Test
    void testPrepareProjection_specificAggColumn() {
      when(mockRegistry.aggLookup("level", "warn", AggregationType.EQ))
          .thenReturn("agg_f01");

      ResolvedProjection result =
          engine.prepareProjection(TABLE, List.of("__AGG_EQ.level.warn"));
      assertNotNull(result.aggColumns());
      assertTrue(result.aggColumns().contains("agg_f01"));
    }

    @Test
    void testPrepareProjection_unknownAggTerm() {
      when(mockRegistry.aggLookup("unknown", null, AggregationType.EQ))
          .thenReturn(null);

      assertThrows(IllegalArgumentException.class,
          () -> engine.prepareProjection(TABLE, List.of("__AGG_EQ.unknown")));
    }

    @Test
    void testPrepareProjection_nullRegistry_dimTerm() {
      when(mockProvider.get(TABLE)).thenReturn(null);
      SplitQueryEngine nullRegEngine =
          new SplitQueryEngine(dsl, mockProvider, mockCacheService, mockSketchEvaluator);

      assertThrows(IllegalArgumentException.class,
          () -> nullRegEngine.prepareProjection(TABLE, List.of("__DIM.service.name")));
    }

    @Test
    void testPrepareProjection_aggPrefixes_allTypesResolved() {
      // Verifies GTE, SUM, AVG, MIN, MAX, GT, LT, LTE prefixes all resolve correctly
      when(mockRegistry.aggLookup("level", "warn", AggregationType.GTE)).thenReturn("agg_gte");
      when(mockRegistry.aggLookup("cpu", null, AggregationType.SUM)).thenReturn("agg_sum");
      when(mockRegistry.aggLookup("latency", null, AggregationType.AVG)).thenReturn("agg_avg");
      when(mockRegistry.aggLookup("temp", null, AggregationType.MIN)).thenReturn("agg_min");
      when(mockRegistry.aggLookup("temp", null, AggregationType.MAX)).thenReturn("agg_max");
      when(mockRegistry.aggLookup("score", "high", AggregationType.GT)).thenReturn("agg_gt");
      when(mockRegistry.aggLookup("score", "low", AggregationType.LT)).thenReturn("agg_lt");
      when(mockRegistry.aggLookup("score", "mid", AggregationType.LTE)).thenReturn("agg_lte");

      ResolvedProjection result = engine.prepareProjection(TABLE, List.of(
          "__AGG_GTE.level.warn", "__AGG_SUM.cpu", "__AGG_AVG.latency",
          "__AGG_MIN.temp", "__AGG_MAX.temp",
          "__AGG_GT.score.high", "__AGG_LT.score.low", "__AGG_LTE.score.mid"));
      assertNotNull(result.aggColumns());
      assertTrue(result.aggColumns().containsAll(
          Set.of("agg_gte", "agg_sum", "agg_avg", "agg_min", "agg_max",
              "agg_gt", "agg_lt", "agg_lte")));
    }

    @Test
    void testPrepareProjection_aggTypeWildcard() {
      // __AGG_EQ.* should iterate all active agg entries of type EQ
      AggRegistryEntry entry = new AggRegistryEntry(
          TABLE, "agg_f01", "level", "warn", AggregationType.EQ, AggValueType.INT,
          null, "ACTIVE", null, null);
      AggRegistryEntry otherEntry = new AggRegistryEntry(
          TABLE, "agg_f02", "cpu", null, AggregationType.SUM, AggValueType.FLOAT,
          null, "ACTIVE", null, null);
      when(mockRegistry.getActiveAggEntries()).thenReturn(List.of(entry, otherEntry));

      ResolvedProjection result =
          engine.prepareProjection(TABLE, List.of("__AGG_EQ.*"));
      assertNotNull(result.aggColumns());
      assertTrue(result.aggColumns().contains("agg_f01"));
      // SUM entry should not be included under EQ wildcard
      assertFalse(result.aggColumns().contains("agg_f02"));
    }

    @Test
    void testPrepareProjection_nullRegistry_aggStarWildcard() {
      when(mockProvider.get(TABLE)).thenReturn(null);
      SplitQueryEngine nullRegEngine =
          new SplitQueryEngine(dsl, mockProvider, mockCacheService, mockSketchEvaluator);

      assertThrows(IllegalArgumentException.class,
          () -> nullRegEngine.prepareProjection(TABLE, List.of("__AGG_EQ.*")));
    }

    @Test
    void testPrepareProjection_multipleFileColumns() {
      ResolvedProjection result = engine.prepareProjection(
          TABLE, List.of("__FILE.max_timestamp", "__FILE.min_timestamp", "__FILE.state"));
      assertNotNull(result.fileColumns());
      assertEquals(3, result.fileColumns().size());
      assertTrue(result.fileColumns().contains("max_timestamp"));
      assertTrue(result.fileColumns().contains("min_timestamp"));
      assertTrue(result.fileColumns().contains("state"));
    }

    @Test
    void testPrepareProjection_mixedNamespaces() {
      when(mockRegistry.aggLookup("level", "warn", AggregationType.EQ))
          .thenReturn("agg_f01");

      ResolvedProjection result = engine.prepareProjection(
          TABLE,
          List.of("__FILE.max_timestamp", "__DIM.service.name", "__AGG_EQ.level.warn"));
      assertNotNull(result.fileColumns());
      assertNotNull(result.dimColumns());
      assertNotNull(result.aggColumns());
      assertTrue(result.fileColumns().contains("max_timestamp"));
      assertTrue(result.dimColumns().contains("dim_f01"));
      assertTrue(result.aggColumns().contains("agg_f01"));
    }
  }

  // ==========================================
  // ExtractCursor Tests
  // ==========================================

  @Nested
  @DisplayName("ExtractCursor")
  class ExtractCursorTests {

    @Test
    void testExtractCursor_singleColumn() throws Exception {
      var rs = mock(java.sql.ResultSet.class);
      when(rs.getObject("max_timestamp")).thenReturn(1704067200L);
      when(rs.getLong("id")).thenReturn(42L);

      var orderBy = List.of(new OrderBySpec("max_timestamp", Order.DESC));

      // extractCursor is private, but we can test it indirectly through buildCursorCondition
      // by verifying the cursor construction logic. Since extractCursor is private static,
      // we verify its behavior via reflection.
      var method = SplitQueryEngine.class.getDeclaredMethod(
          "extractCursor", java.sql.ResultSet.class, List.class);
      method.setAccessible(true);

      KeysetCursor cursor = (KeysetCursor) method.invoke(null, rs, orderBy);

      assertEquals(42L, cursor.id());
      assertEquals(1, cursor.sortValues().size());
      assertEquals(1704067200L, cursor.sortValues().get(0));
    }

    @Test
    void testExtractCursor_integerNormalization() throws Exception {
      var rs = mock(java.sql.ResultSet.class);
      // JDBC returns Integer for signed INT columns
      when(rs.getObject("record_count")).thenReturn(Integer.valueOf(42));
      when(rs.getLong("id")).thenReturn(7L);

      var orderBy = List.of(new OrderBySpec("record_count", Order.ASC));

      var method = SplitQueryEngine.class.getDeclaredMethod(
          "extractCursor", java.sql.ResultSet.class, List.class);
      method.setAccessible(true);

      KeysetCursor cursor = (KeysetCursor) method.invoke(null, rs, orderBy);

      // Integer should be normalized to Long
      assertEquals(7L, cursor.id());
      assertInstanceOf(Long.class, cursor.sortValues().get(0));
      assertEquals(42L, cursor.sortValues().get(0));
    }
  }
}
