package com.yscope.metalog.query.core.splits;

import static com.yscope.metalog.metastore.JooqFields.*;
import static org.jooq.impl.DSL.val;

import com.yscope.metalog.metastore.SqlIdentifiers;
import com.yscope.metalog.metastore.model.AggRegistryEntry;
import com.yscope.metalog.metastore.model.AggValueType;
import com.yscope.metalog.metastore.model.AggregationType;
import com.yscope.metalog.metastore.schema.ColumnRegistry;
import com.yscope.metalog.metastore.schema.ColumnRegistryProvider;
import com.yscope.metalog.query.core.CacheService;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import net.sf.jsqlparser.expression.Expression;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.SortField;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes split queries against the metadata table.
 *
 * <p>Supports state filtering, raw SQL filter clauses, and multi-column keyset pagination with
 * cursor-based continuation.
 *
 * @see <a href="docs/architecture/pk-keyset-pagination.md">PK Keyset Pagination</a>
 */
public class SplitQueryEngine {

  private static final Logger LOG = LoggerFactory.getLogger(SplitQueryEngine.class);

  /**
   * Columns that have a dedicated index supporting efficient keyset pagination. Sorting on any
   * other column causes a full table scan and requires {@code allow_unindexed_sort=true}.
   */
  public static final Set<String> INDEXED_SORT_COLUMNS = Set.of("min_timestamp", "max_timestamp");

  private final DSLContext dsl;
  private final ColumnRegistryProvider registryProvider;
  private final CacheService cacheService;
  private final SketchEvaluator sketchEvaluator;

  public SplitQueryEngine(
      DSLContext dsl,
      ColumnRegistryProvider registryProvider,
      CacheService cacheService,
      SketchEvaluator sketchEvaluator) {
    this.dsl = dsl;
    this.registryProvider = registryProvider;
    this.cacheService = cacheService;
    this.sketchEvaluator = sketchEvaluator;
  }

  /**
   * Extract {@code __SKETCH.*} predicates from a filter expression, returning the cleaned
   * expression (without sketch terms) and the list of extracted predicates.
   *
   * <p>Call this once per RPC before {@link #prepareFilterCondition} — pass {@code
   * cleanedExpression} to that method so sketch predicates are not forwarded to the DB.
   *
   * <p>If the expression fails to parse (e.g., contains a syntax error), extraction is skipped and
   * the original expression is returned unchanged — the definitive parse error will be raised by
   * the subsequent {@link #prepareFilterCondition} call.
   *
   * @param filterExpression SQL WHERE clause fragment, or null/empty
   * @return extraction result with cleaned expression and predicates
   * @throws IllegalArgumentException if sketch predicates appear inside OR or NOT, or use
   *     unsupported operators
   */
  public SketchExtractionResult prepareSketchPredicates(String filterExpression) {
    if (filterExpression == null || filterExpression.isEmpty()) {
      return new SketchExtractionResult(filterExpression, List.of());
    }
    return SketchPredicateExtractor.extract(filterExpression);
  }

  /**
   * Result of a single DB page fetch.
   *
   * <p>{@code splits} contains only rows that passed sketch filtering. {@code dbRowsScanned} is the
   * total number of rows read from the DB before sketch filtering — use this (not {@code
   * splits.size()}) to determine whether the DB has more pages remaining.
   *
   * <p>{@code lastDbCursor} is the cursor of the last row read from the DB (regardless of sketch
   * filtering). When all rows are sketch-pruned, this cursor enables pagination to continue to the
   * next page. It is {@code null} only when zero rows were read from the DB.
   */
  public record PageFetchResult(
      List<SplitWithCursor> splits, long dbRowsScanned, KeysetCursor lastDbCursor) {}

  /**
   * Consumer callback for async streaming.
   *
   * <p>Called for each split during {@link #streamSplitsAsync}. The consumer can throw exceptions,
   * which will be propagated to the caller.
   */
  @FunctionalInterface
  public interface SplitConsumer {
    /**
     * Process a split.
     *
     * @param split the split with cursor
     * @return true to continue streaming, false to stop
     * @throws Exception if processing fails (propagated to caller)
     */
    boolean accept(SplitWithCursor split) throws Exception;
  }

  /**
   * Result of an async streaming operation.
   *
   * @param splitsScanned total number of splits read from the database (before sketch filtering)
   * @param splitsMatched total number of splits that passed filters and were sent to consumer
   */
  public record StreamingResult(long splitsScanned, long splitsMatched) {}

  /**
   * Parse, resolve, and validate a filter expression once, returning a reusable jOOQ Condition.
   *
   * <p>Call this once per RPC before the pagination loop, then pass the result to {@link
   * #fetchPage} or {@link #streamPage} on each page iteration.
   *
   * @param table table name, used to look up the column registry for name resolution
   * @param filterExpression SQL WHERE clause fragment, or null/empty for no extra filtering
   * @return a pre-built filter, or {@code null} if filterExpression is blank
   * @throws IllegalArgumentException if the filter expression is invalid
   */
  public PreparedFilter prepareFilterCondition(String table, String filterExpression) {
    if (filterExpression == null || filterExpression.isEmpty()) {
      return null;
    }
    String cacheKey = "filter:" + table + ":" + filterExpression;
    return new PreparedFilter(
        cacheService.getQuery(cacheKey, () -> resolveFilterCondition(table, filterExpression)));
  }

  /**
   * Resolve and validate order_by column names using the same namespace prefix rules as
   * filter_expression (e.g., {@code __FILE.}, {@code __DIM.}, {@code __AGG_GTE.}).
   *
   * @param table table name, used to look up the column registry
   * @param orderBy raw order_by spec from the request
   * @return list of order_by specs with physical column names
   * @throws IllegalArgumentException if any column name cannot be resolved
   */
  public List<OrderBySpec> prepareOrderBy(String table, List<OrderBySpec> orderBy) {
    ColumnRegistry registry = registryProvider.get(table);
    List<OrderBySpec> resolved = new ArrayList<>(orderBy.size());
    for (OrderBySpec spec : orderBy) {
      String physical = FilterExpressionResolver.resolveColumnName(spec.column(), registry);
      resolved.add(new OrderBySpec(physical, spec.order()));
    }
    return resolved;
  }

  /**
   * Parse and validate a projection term list, returning a {@link ResolvedProjection} that
   * identifies the physical columns to SELECT.
   *
   * <p>Call this once per RPC before the pagination loop, then pass the result to {@link
   * #fetchPage} or {@link #streamPage}.
   *
   * @param table table name, used to look up the column registry for name resolution
   * @param terms projection terms from the request (empty = all columns)
   * @return resolved projection describing which column groups to include
   * @throws IllegalArgumentException if any term is invalid or references an unknown column
   */
  public ResolvedProjection prepareProjection(String table, List<String> terms) {
    if (terms == null || terms.isEmpty()) {
      return ResolvedProjection.ALL;
    }

    ColumnRegistry registry = registryProvider.get(table);

    Set<String> fileColumns = new HashSet<>();
    Set<String> dimColumns = new HashSet<>();
    Set<String> aggColumns = new HashSet<>();
    boolean allFile = false;
    boolean allDim = false;
    boolean allAgg = false;

    for (String term : terms) {
      String upper = term.toUpperCase();

      if (upper.equals("*")) {
        return ResolvedProjection.ALL;
      } else if (upper.equals("__FILE.*")) {
        allFile = true;
      } else if (upper.startsWith("__FILE.")) {
        String col = term.substring("__FILE.".length());
        if (!FilterExpressionValidator.FILE_FILTER_COLUMNS.contains(col)) {
          throw new IllegalArgumentException("Unknown __FILE column: " + col);
        }
        if (!allFile) {
          fileColumns.add(col);
        }
      } else if (upper.equals("__DIM.*")) {
        allDim = true;
      } else if (upper.startsWith("__DIM.")) {
        String key = term.substring("__DIM.".length());
        if (registry == null) {
          throw new IllegalArgumentException(
              "Column registry unavailable; cannot resolve __DIM." + key);
        }
        String physical = registry.getDimKeyToColumnMap().get(key);
        if (physical == null) {
          throw new IllegalArgumentException("Unknown dimension key: " + key);
        }
        if (!allDim) {
          dimColumns.add(physical);
        }
      } else if (upper.equals("__AGG.*")) {
        allAgg = true;
      } else {
        AggPrefixMatch match = parseAggProjectionPrefix(upper);
        if (match == null) {
          throw new IllegalArgumentException("Unknown projection term: " + term);
        }
        String rest = term.substring(match.prefixLength());
        if (rest.equals("*")) {
          if (!allAgg) {
            if (registry == null) {
              throw new IllegalArgumentException(
                  "Column registry unavailable; cannot resolve __AGG_* projection");
            }
            for (AggRegistryEntry entry : registry.getActiveAggEntries()) {
              if (entry.aggregationType() == match.type()) {
                aggColumns.add(entry.effectiveColumn());
              }
            }
          }
        } else {
          if (registry == null) {
            throw new IllegalArgumentException(
                "Column registry unavailable; cannot resolve aggregate projection: " + term);
          }
          int lastDot = rest.lastIndexOf('.');
          String aggKey;
          String aggValue;
          if (lastDot >= 0) {
            aggKey = rest.substring(0, lastDot);
            aggValue = rest.substring(lastDot + 1);
          } else {
            aggKey = rest;
            aggValue = null;
          }
          String physical = registry.aggLookup(aggKey, aggValue, match.type());
          if (physical == null) {
            throw new IllegalArgumentException("Unknown aggregate projection term: " + term);
          }
          if (!allAgg) {
            aggColumns.add(physical);
          }
        }
      }
    }

    return new ResolvedProjection(
        allFile ? null : fileColumns, allDim ? null : dimColumns, allAgg ? null : aggColumns);
  }

  private record AggPrefixMatch(AggregationType type, int prefixLength) {}

  private static AggPrefixMatch parseAggProjectionPrefix(String upper) {
    if (upper.startsWith("__AGG_EQ.")) return new AggPrefixMatch(AggregationType.EQ, 9);
    if (upper.startsWith("__AGG_GTE.")) return new AggPrefixMatch(AggregationType.GTE, 10);
    if (upper.startsWith("__AGG_GT.")) return new AggPrefixMatch(AggregationType.GT, 9);
    if (upper.startsWith("__AGG_LTE.")) return new AggPrefixMatch(AggregationType.LTE, 10);
    if (upper.startsWith("__AGG_LT.")) return new AggPrefixMatch(AggregationType.LT, 9);
    if (upper.startsWith("__AGG_SUM.")) return new AggPrefixMatch(AggregationType.SUM, 10);
    if (upper.startsWith("__AGG_AVG.")) return new AggPrefixMatch(AggregationType.AVG, 10);
    if (upper.startsWith("__AGG_MIN.")) return new AggPrefixMatch(AggregationType.MIN, 10);
    if (upper.startsWith("__AGG_MAX.")) return new AggPrefixMatch(AggregationType.MAX, 10);
    return null;
  }

  private Condition resolveFilterCondition(String table, String filterExpression) {
    ColumnRegistry registry = registryProvider.get(table);
    if (registry == null) {
      LOG.warn(
          "Column registry unavailable; unknown columns in filter will not be caught: {}",
          filterExpression);
    }
    Expression resolved =
        FilterExpressionResolver.resolveToExpressionStrict(filterExpression, registry);
    FilterExpressionValidator.validate(resolved);
    return DSL.condition(resolved.toString());
  }

  /** Internal result from {@link #streamPage} carrying both the row count and last DB cursor. */
  private record StreamResult(long dbRowsScanned, KeysetCursor lastDbCursor) {}

  /**
   * Stream rows directly from the database to a consumer, one page at a time.
   *
   * <p>The consumer receives each row as a {@link SplitWithCursor}. The cursor encodes the row's
   * position in the sort order and can be passed back in a subsequent request to resume from there.
   *
   * <p>If {@code sketchPredicates} is non-empty, each row is post-filtered via the {@link
   * SketchEvaluator} before being passed to the consumer. Rows that the sketch definitively
   * excludes are skipped; all others are forwarded.
   *
   * @param table Target table name
   * @param stateFilter State values for WHERE IN clause (empty = no filter)
   * @param filterCondition Pre-built filter from {@link #prepareFilterCondition}, or null
   * @param limit DB page size — maximum rows to fetch per page (0 = no limit)
   * @param orderBy Sort specification. "id" must not appear — it is always the implicit final
   *     tiebreaker (ASC).
   * @param cursor Keyset cursor from previous page, or null for first page
   * @param sketchPredicates Sketch predicates to post-filter rows (empty = no sketch filtering)
   * @param projection resolved projection describing which columns to SELECT
   * @param consumer Called for each row; return false to stop
   * @return stream result with the DB row count and the last row's cursor (for advancing past
   *     fully sketch-pruned pages)
   */
  StreamResult streamPage(
      String table,
      List<String> stateFilter,
      PreparedFilter filterCondition,
      long limit,
      List<OrderBySpec> orderBy,
      KeysetCursor cursor,
      List<SketchPredicate> sketchPredicates,
      ResolvedProjection projection,
      Predicate<SplitWithCursor> consumer) {

    Condition condition = filterCondition != null ? filterCondition.condition : null;
    SqlIdentifiers.requireValidTableName(table);
    ColumnRegistry registry = registryProvider.get(table);
    Condition where = buildWhereCondition(stateFilter, condition);
    if (cursor != null) {
      where = where.and(buildCursorCondition(orderBy, cursor));
    }

    SortField<?>[] sortFields = buildOrderByClause(orderBy);

    Query limited;
    if (projection.isAll()) {
      var q = dsl.selectFrom(table(table)).where(where).orderBy(sortFields);
      limited = limit > 0 ? q.limit((int) limit) : q;
    } else {
      // Build explicit column list — always include id (cursor tiebreaker) and all sort columns,
      // plus sketch columns when sketch predicates are active.
      Set<String> cols = new LinkedHashSet<>();
      cols.add("id");
      for (OrderBySpec ob : orderBy) {
        cols.add(ob.column());
      }
      if (!sketchPredicates.isEmpty()) {
        cols.add("sketches");
        cols.add("ext");
      }
      // File columns
      if (projection.fileColumns() == null) {
        cols.addAll(FilterExpressionValidator.FILE_FILTER_COLUMNS);
      } else {
        cols.addAll(projection.fileColumns());
      }
      // Dim columns
      if (projection.dimColumns() == null) {
        if (registry != null) {
          cols.addAll(registry.getActiveDimColumns());
        }
      } else {
        cols.addAll(projection.dimColumns());
      }
      // Agg columns
      if (projection.aggColumns() == null) {
        if (registry != null) {
          cols.addAll(registry.getActiveAggColumns());
        }
      } else {
        cols.addAll(projection.aggColumns());
      }

      Field<?>[] fields = cols.stream().map(DSL::field).toArray(Field[]::new);
      var q = dsl.select(fields).from(table(table)).where(where).orderBy(sortFields);
      limited = limit > 0 ? q.limit((int) limit) : q;
    }
    LOG.debug("Direct streaming SQL: {}", limited.getSQL());

    StreamResult result;

    try {
      result =
          executeQuery(
              limited,
              rs -> {
                // Check sketch column availability once — avoids per-row metadata calls
                // and prevents broad SQLException catch inside the hot loop.
                boolean hasSketchCols = !sketchPredicates.isEmpty() && hasSketchColumns(rs);
                long n = 0;
                KeysetCursor lastCursor = null;
                while (rs.next()) {
                  n++;
                  // Extract cursor BEFORE sketch filtering so we can advance past
                  // fully-pruned pages (all rows sketch-filtered on a single page).
                  KeysetCursor rowCursor = extractCursor(rs, orderBy);
                  lastCursor = rowCursor;
                  if (!sketchPredicates.isEmpty()
                      && !sketchMatches(rs, sketchPredicates, hasSketchCols)) {
                    continue;
                  }
                  Split split = mapToSplit(rs, registry);
                  if (!consumer.test(new SplitWithCursor(split, rowCursor))) {
                    break;
                  }
                }
                return new StreamResult(n, lastCursor);
              });
    } catch (DataAccessException | SQLException e) {
      LOG.error("Direct streaming query failed", e);
      throw new RuntimeException("Query failed: " + e.getMessage(), e);
    }

    return result;
  }

  /**
   * Fetch a single page of splits from the database.
   *
   * <p>Collects all rows into a list and releases the DB connection before returning. This is
   * useful for prefetch patterns where the connection should not be held during downstream
   * processing.
   *
   * @param table Target table name
   * @param stateFilter State values for WHERE IN clause (empty = no filter)
   * @param filterCondition Pre-built filter from {@link #prepareFilterCondition}, or null
   * @param limit DB page size — maximum rows to fetch (0 = no limit)
   * @param orderBy Sort specification. "id" must not appear — it is always the implicit final
   *     tiebreaker (ASC).
   * @param cursor Keyset cursor from previous page, or null for first page
   * @param sketchPredicates Sketch predicates to post-filter rows (empty = no sketch filtering)
   * @param projection resolved projection describing which columns to SELECT
   * @return page result with the filtered split list, the raw DB row count, and the last DB
   *     cursor; use {@link PageFetchResult#dbRowsScanned()} (not {@code splits().size()}) for
   *     exhaustion checks, and {@link PageFetchResult#lastDbCursor()} to advance past fully
   *     sketch-pruned pages
   */
  public PageFetchResult fetchPage(
      String table,
      List<String> stateFilter,
      PreparedFilter filterCondition,
      long limit,
      List<OrderBySpec> orderBy,
      KeysetCursor cursor,
      List<SketchPredicate> sketchPredicates,
      ResolvedProjection projection) {
    List<SplitWithCursor> page = new ArrayList<>();
    StreamResult streamResult =
        streamPage(
            table,
            stateFilter,
            filterCondition,
            limit,
            orderBy,
            cursor,
            sketchPredicates,
            projection,
            item -> {
              page.add(item);
              return true;
            });
    return new PageFetchResult(page, streamResult.dbRowsScanned(), streamResult.lastDbCursor());
  }

  // --- Query helpers ---

  /** Build a WHERE condition from optional state filter and pre-built filter condition. */
  private Condition buildWhereCondition(List<String> stateFilter, Condition filterCondition) {
    Condition where = DSL.trueCondition();
    if (stateFilter != null && !stateFilter.isEmpty()) {
      where = where.and(STATE.in(stateFilter));
    }
    if (filterCondition != null) {
      where = where.and(filterCondition);
    }
    return where;
  }

  /**
   * Build a keyset WHERE condition from an orderBy spec and cursor.
   *
   * <p>For {@code N} sort fields plus the implicit {@code id} tiebreaker, produces:
   *
   * <pre>
   *   (f1 {op1} v1)
   *   OR (f1 = v1 AND f2 {op2} v2)
   *   OR (f1 = v1 AND f2 = v2 AND f3 {op3} v3)
   *   ...
   *   OR (f1 = v1 AND ... AND fN = vN AND id > id_val)
   * </pre>
   *
   * <p>Where {@code {opI}} is {@code <} for DESC fields and {@code >} for ASC fields.
   */
  static Condition buildCursorCondition(List<OrderBySpec> orderBy, KeysetCursor cursor) {
    int n = orderBy.size();
    List<Object> sortValues = cursor.sortValues();
    if (sortValues.size() != n) {
      throw new IllegalArgumentException(
          "cursor.values has "
              + sortValues.size()
              + " entries but order_by has "
              + n
              + " — cursor is not valid for this order_by");
    }

    Condition result = DSL.falseCondition();
    for (int i = 0; i <= n; i++) {
      // Equality prefix: f0=v0 AND f1=v1 AND ... AND f_{i-1}=v_{i-1}
      Condition prefix = DSL.trueCondition();
      for (int j = 0; j < i; j++) {
        String col = j < n ? orderBy.get(j).column() : "id";
        Object v = j < n ? sortValues.get(j) : cursor.id();
        prefix = prefix.and(DSL.field(col).eq(val(v)));
      }

      // Strict comparison for field i
      String col = i < n ? orderBy.get(i).column() : "id";
      Object v = i < n ? sortValues.get(i) : cursor.id();
      Order order = i < n ? orderBy.get(i).order() : Order.ASC;
      Condition comparison =
          order == Order.DESC ? DSL.field(col).lt(val(v)) : DSL.field(col).gt(val(v));

      result = result.or(prefix.and(comparison));
    }
    return result;
  }

  /** Build an ORDER BY clause from an orderBy spec, appending id ASC as the final tiebreaker. */
  private static SortField<?>[] buildOrderByClause(List<OrderBySpec> orderBy) {
    List<SortField<?>> fields = new ArrayList<>(orderBy.size() + 1);
    for (OrderBySpec spec : orderBy) {
      Field<?> field = DSL.field(spec.column());
      fields.add(spec.order() == Order.DESC ? field.desc() : field.asc());
    }
    fields.add(ID.asc());
    return fields.toArray(new SortField[0]);
  }

  /**
   * Extract cursor values for the current ResultSet row.
   *
   * <p>Reads the sort column values via {@link ResultSet#getObject}, which returns {@link Long} for
   * BIGINT, {@link Double} for DOUBLE, and {@link String} for VARCHAR/ENUM columns.
   */
  private static KeysetCursor extractCursor(ResultSet rs, List<OrderBySpec> orderBy)
      throws SQLException {
    List<Object> values = new ArrayList<>(orderBy.size());
    for (OrderBySpec spec : orderBy) {
      Object raw = rs.getObject(spec.column());
      // Normalize Integer → Long so callers never need to handle both types.
      // JDBC returns Integer for signed INT columns; INT UNSIGNED returns Long directly.
      values.add(raw instanceof Integer i ? i.longValue() : raw);
    }
    return new KeysetCursor(values, rs.getLong("id"));
  }

  /**
   * Execute a jOOQ query via raw PreparedStatement and process the ResultSet with a callback.
   * Handles connection, statement, and result set lifecycle.
   */
  private <T> T executeQuery(Query query, ResultSetHandler<T> handler) throws SQLException {
    String sql = query.getSQL();
    List<?> bindValues = query.getBindValues();
    try (Connection conn = dsl.configuration().connectionProvider().acquire();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      for (int i = 0; i < bindValues.size(); i++) {
        stmt.setObject(i + 1, bindValues.get(i));
      }
      try (ResultSet rs = stmt.executeQuery()) {
        return handler.handle(rs);
      }
    }
  }

  @FunctionalInterface
  private interface ResultSetHandler<T> {
    T handle(ResultSet rs) throws SQLException;
  }

  /**
   * Returns true if both {@code sketches} and {@code ext} columns are present in the ResultSet.
   *
   * <p>Called once per page before the row loop to avoid per-row metadata overhead and to replace
   * the broad {@code SQLException} catch that was previously used to detect missing columns.
   */
  private static boolean hasSketchColumns(ResultSet rs) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    boolean hasSketches = false;
    boolean hasExt = false;
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      String name = meta.getColumnName(i);
      if ("sketches".equals(name)) {
        hasSketches = true;
      } else if ("ext".equals(name)) {
        hasExt = true;
      }
    }
    return hasSketches && hasExt;
  }

  /**
   * Returns true if the row passes all sketch predicates.
   *
   * <p>When {@code hasSketchCols} is false (columns absent — e.g., test stubs without sketch
   * support), the row is kept. A row is pruned only when the evaluator definitively returns false
   * for at least one predicate.
   */
  private boolean sketchMatches(
      ResultSet rs, List<SketchPredicate> sketchPredicates, boolean hasSketchCols)
      throws SQLException {
    if (!hasSketchCols) {
      return true;
    }
    String sketchesSet = rs.getString("sketches");
    byte[] extData = rs.getBytes("ext");
    for (SketchPredicate pred : sketchPredicates) {
      if (!sketchEvaluator.maybeContains(pred.field(), pred.values(), sketchesSet, extData)) {
        return false;
      }
    }
    return true;
  }

  // --- Row mapping ---

  private Split mapToSplit(ResultSet rs, ColumnRegistry registry) throws SQLException {
    Map<String, String> dimensions = new HashMap<>();
    List<SplitAgg> aggs = new ArrayList<>();

    ResultSetMetaData meta = rs.getMetaData();
    Set<String> columnNames = new HashSet<>();
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      String colName = meta.getColumnName(i);
      columnNames.add(colName);
      if (colName.startsWith("dim_")) {
        String value = rs.getString(i);
        if (value != null) {
          dimensions.put(resolveDimName(registry, colName), value);
        }
      } else if (colName.startsWith("agg_")) {
        SplitAgg agg = resolveAggEntry(registry, colName, rs, i);
        if (agg != null) {
          aggs.add(agg);
        }
      }
    }

    return new Split(
        rs.getLong("id"),
        getStringOrDefault(rs, "clp_ir_path", columnNames, null),
        getStringOrDefault(rs, "clp_ir_storage_backend", columnNames, ""),
        getStringOrDefault(rs, "clp_ir_bucket", columnNames, ""),
        getStringOrDefault(rs, "clp_archive_path", columnNames, null),
        getStringOrDefault(rs, "clp_archive_storage_backend", columnNames, ""),
        getStringOrDefault(rs, "clp_archive_bucket", columnNames, ""),
        getLongOrDefault(rs, "min_timestamp", columnNames, 0),
        getLongOrDefault(rs, "max_timestamp", columnNames, 0),
        getStringOrDefault(rs, "state", columnNames, null),
        dimensions,
        aggs,
        getLongOrDefault(rs, "record_count", columnNames, 0),
        getLongOrDefault(rs, "clp_ir_size_bytes", columnNames, 0));
  }

  /** Resolve a dim column name to its logical key, falling back to the raw column name. */
  private static String resolveDimName(ColumnRegistry registry, String colName) {
    if (registry != null) {
      String key = registry.dimColumnToKey(colName);
      if (key != null) {
        return key;
      }
    }
    return colName;
  }

  /**
   * Resolve an agg column to a SplitAgg, reading the value as int or float based on the registry
   * entry's value type. Returns null if the value is SQL NULL.
   */
  private static SplitAgg resolveAggEntry(
      ColumnRegistry registry, String colName, ResultSet rs, int colIndex) throws SQLException {
    if (registry != null) {
      AggRegistryEntry entry = registry.aggColumnToEntry(colName);
      if (entry != null) {
        if (entry.valueType() == AggValueType.FLOAT) {
          double value = rs.getDouble(colIndex);
          if (rs.wasNull()) {
            return null;
          }
          return new SplitAgg(
              entry.aggKey(),
              entry.aggValue(),
              entry.aggregationType(),
              AggValueType.FLOAT,
              0,
              value);
        } else {
          long value = rs.getLong(colIndex);
          if (rs.wasNull()) {
            return null;
          }
          return new SplitAgg(
              entry.aggKey(),
              entry.aggValue(),
              entry.aggregationType(),
              AggValueType.INT,
              value,
              0.0);
        }
      }
    }
    // Fallback: read as long, no registry metadata
    long value = rs.getLong(colIndex);
    if (rs.wasNull()) {
      return null;
    }
    return new SplitAgg(colName, null, AggregationType.EQ, AggValueType.INT, value, 0.0);
  }

  private static String getStringOrDefault(ResultSet rs, String col, Set<String> present, String def)
      throws SQLException {
    if (!present.contains(col)) {
      return def;
    }
    String value = rs.getString(col);
    return value != null ? value : def;
  }

  private static long getLongOrDefault(
      ResultSet rs, String column, Set<String> columnNames, long defaultValue) throws SQLException {
    if (!columnNames.contains(column)) {
      return defaultValue;
    }
    long value = rs.getLong(column);
    return rs.wasNull() ? defaultValue : value;
  }

  /**
   * Stream splits asynchronously with prefetch optimization.
   *
   * <p>Fetches pages from the database in a background thread and pushes splits into a bounded
   * queue. The main thread drains the queue and calls the consumer for each split. DB connections
   * are released after each page fetch, preventing idle connections during consumer processing.
   * This overlaps DB I/O with consumer work for maximum throughput.
   *
   * <p>The background thread respects the cancellation token and stops fetching when cancelled.
   * The consumer can also stop the stream early by returning false.
   *
   * <p><b>Prefetch queue capacity:</b> {@code 2 * pageSize} allows the producer to stay one page
   * ahead of the consumer without blocking.
   *
   * <p><b>Error handling:</b> Exceptions thrown by the consumer are propagated to the caller.
   * Exceptions in the background thread are captured and re-thrown after the consumer completes.
   *
   * @param table table name
   * @param stateFilter state filter list (empty = no filter)
   * @param filterCondition prepared filter condition from {@link #prepareFilterCondition}, or null
   * @param orderBy ordering specification (resolved column names)
   * @param initialCursor optional starting cursor for pagination (null = start from beginning)
   * @param sketchPredicates sketch-based predicates for early filtering
   * @param projection column projection
   * @param pageSize number of splits to fetch per DB query
   * @param limitCount maximum number of results to return (0 = unlimited)
   * @param consumer callback invoked for each split; return false to stop streaming
   * @param cancellation cancellation token checked frequently; set to true to stop streaming
   * @return statistics (total scanned, total matched)
   * @throws InterruptedException if interrupted while waiting on the queue
   * @throws Exception if the consumer throws an exception or the background thread encounters an
   *     error
   */
  public StreamingResult streamSplitsAsync(
      String table,
      List<String> stateFilter,
      @Nullable PreparedFilter filterCondition,
      List<OrderBySpec> orderBy,
      @Nullable KeysetCursor initialCursor,
      List<SketchPredicate> sketchPredicates,
      ResolvedProjection projection,
      int pageSize,
      long limitCount,
      SplitConsumer consumer,
      AtomicBoolean cancellation)
      throws InterruptedException, Exception {

    // Prefetch queue: capacity 2 * pageSize allows producer to stay one page ahead
    ArrayBlockingQueue<SplitWithCursor> splitQueue = new ArrayBlockingQueue<>(2 * pageSize);
    AtomicLong totalScanned = new AtomicLong(0);
    AtomicLong totalMatched = new AtomicLong(0);
    AtomicBoolean producerDone = new AtomicBoolean(false);
    AtomicReference<Exception> producerError = new AtomicReference<>();
    AtomicLong remainingCount = new AtomicLong(limitCount); // 0 = unlimited

    // Sentinel value to signal end of stream
    SplitWithCursor SENTINEL = new SplitWithCursor(null, null);

    // Background producer thread: fetches pages and pushes to queue
    Thread producer =
        new Thread(
            () -> {
              try {
                KeysetCursor cursor = initialCursor;
                while (!cancellation.get()) {
                  long remaining = remainingCount.get();
                  long effectivePageSize =
                      (remaining > 0) ? Math.min(pageSize, remaining) : pageSize;

                  PageFetchResult result =
                      fetchPage(
                          table,
                          stateFilter,
                          filterCondition,
                          effectivePageSize,
                          orderBy,
                          cursor,
                          sketchPredicates,
                          projection);
                  // DB connection released here

                  totalScanned.addAndGet(result.dbRowsScanned());

                  if (result.splits().isEmpty()) {
                    // All rows were sketch-pruned, but the DB may have more pages.
                    // Use lastDbCursor to advance past the pruned page.
                    if (result.dbRowsScanned() >= effectivePageSize
                        && result.lastDbCursor() != null) {
                      cursor = result.lastDbCursor();
                      continue;
                    }
                    break; // DB actually exhausted (fewer rows than page size)
                  }

                  for (SplitWithCursor item : result.splits()) {
                    if (cancellation.get()) {
                      break;
                    }
                    splitQueue.put(item);
                    totalMatched.incrementAndGet();

                    // Check limit
                    if (remainingCount.get() > 0) {
                      long newRemaining = remainingCount.decrementAndGet();
                      if (newRemaining == 0) {
                        break;
                      }
                    }
                  }

                  if (result.dbRowsScanned() < effectivePageSize) {
                    break; // DB returned fewer rows than requested — data exhausted
                  }

                  cursor = result.splits().get(result.splits().size() - 1).cursor();

                  // Stop if limit reached
                  if (remainingCount.get() == 0 && limitCount > 0) {
                    break;
                  }
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } catch (Exception e) {
                if (!cancellation.get()) {
                  producerError.set(e);
                }
              } finally {
                producerDone.set(true);
                // Push sentinel to unblock consumer
                try {
                  splitQueue.put(SENTINEL);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
            },
            "split-prefetch");

    producer.setDaemon(true);
    producer.start();

    // Foreground consumer: drain queue and call consumer
    try {
      while (!cancellation.get()) {
        SplitWithCursor item = splitQueue.take();

        // Check for sentinel
        if (item == SENTINEL) {
          break;
        }

        // Call consumer
        boolean continueStreaming = consumer.accept(item);
        if (!continueStreaming) {
          cancellation.set(true);
          break;
        }
      }
    } finally {
      cancellation.set(true);
      producer.interrupt();
    }

    // Check for producer error after consumer completes
    Exception err = producerError.get();
    if (err != null) {
      throw err;
    }

    return new StreamingResult(totalScanned.get(), totalMatched.get());
  }
}
