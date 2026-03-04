package com.yscope.clp.service.query.core.splits;

import com.yscope.clp.service.metastore.model.AggregationType;
import com.yscope.clp.service.metastore.schema.ColumnRegistry;
import java.util.Locale;
import java.util.Map;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves logical column names in filter expressions to physical placeholder columns.
 *
 * <p>Users write filters with logical field names (e.g., {@code service.name = 'foo'}), but the
 * database uses opaque placeholder columns ({@code dim_f01}, {@code agg_f01}). This class parses
 * the filter expression, walks the AST to find {@link Column} nodes, and replaces logical names
 * with physical column names using the {@link ColumnRegistry}.
 *
 * <p>Resolution runs <b>before</b> validation — logical names contain dots that the validator
 * rejects, but the resolved physical names pass validation.
 *
 * <p>JSqlParser splits dotted names across schema/table/column fields:
 *
 * <ul>
 *   <li>{@code state} → Column(table=null, col="state")
 *   <li>{@code service.name} → Column(table="service", col="name")
 *   <li>{@code k8s.pod.name} → Column(schema="k8s", table="pod", col="name")
 *   <li>{@code `service.name`} → Column(table=null, col="`service.name`")
 * </ul>
 */
public class FilterExpressionResolver {

  private static final Logger LOG = LoggerFactory.getLogger(FilterExpressionResolver.class);

  /**
   * Parse and resolve a filter expression, returning the mutated AST.
   *
   * <p>Like {@link #resolve(String, ColumnRegistry)} but returns the {@link Expression} AST
   * directly, avoiding a round-trip back through string. The caller can validate the AST with
   * {@link FilterExpressionValidator#validate(Expression)} and convert to SQL with {@link
   * Expression#toString()}.
   *
   * <p>The semicolon check that {@link FilterExpressionValidator#validate(String)} performs is done
   * here, since this method is the first consumer of the raw string when used in this path.
   *
   * @param filterExpression SQL WHERE clause fragment; must be non-null and non-empty
   * @param registry column registry for name resolution, or null to skip resolution
   * @return the resolved (mutated) expression AST
   * @throws IllegalArgumentException if the expression contains a semicolon or cannot be parsed
   */
  static Expression resolveToExpression(String filterExpression, ColumnRegistry registry) {
    if (filterExpression.contains(";")) {
      throw new IllegalArgumentException("Semicolons are not allowed in filter expressions");
    }
    Expression expr;
    try {
      expr = CCJSqlParserUtil.parseCondExpression(filterExpression);
    } catch (JSQLParserException e) {
      throw new IllegalArgumentException("Failed to parse filter expression: " + e.getMessage(), e);
    }
    if (registry != null) {
      resolveExpression(expr, registry.getDimKeyToColumnMap(), registry.getAggKeyToColumnMap());
    }
    return expr;
  }

  /**
   * Resolve logical column names in a filter expression to physical column names.
   *
   * @param filterExpression SQL WHERE clause fragment, or null/empty
   * @param registry column registry for name resolution, or null to skip resolution
   * @return the resolved expression string, or the original if no resolution needed
   */
  public static String resolve(String filterExpression, ColumnRegistry registry) {
    if (filterExpression == null || filterExpression.isEmpty() || registry == null) {
      return filterExpression;
    }

    Expression expr;
    try {
      expr = CCJSqlParserUtil.parseCondExpression(filterExpression);
    } catch (JSQLParserException e) {
      // Parse failure — return unchanged; downstream validator will produce the definitive error.
      LOG.debug("Filter parse failed during resolution, deferring to validator", e);
      return filterExpression;
    }

    Map<String, String> dimMap = registry.getDimKeyToColumnMap();
    Map<String, String> aggMap = registry.getAggKeyToColumnMap();

    resolveExpression(expr, dimMap, aggMap);
    return expr.toString();
  }

  // --- Namespace prefix constants ---
  private static final String PREFIX_FILE = "__FILE.";
  private static final String PREFIX_DIM = "__DIM.";
  private static final String PREFIX_AGG = "__AGG.";

  /**
   * Resolve a single column name to its physical column name, applying the same namespace prefix
   * rules as the filter expression resolver.
   *
   * <p>Supports: {@code __FILE.xxx}, {@code __DIM.xxx}, {@code __AGG_<TYPE>.key.value}. Unqualified
   * names fall through: file → dimension → aggregate → error.
   *
   * @param column logical column name, optionally with namespace prefix
   * @param registry column registry for name resolution; may be null for built-in-only resolution
   * @return physical column name
   * @throws IllegalArgumentException if the column cannot be resolved
   */
  public static String resolveColumnName(String column, ColumnRegistry registry) {
    String upperKey = column.toUpperCase(Locale.ROOT);

    if (upperKey.startsWith(PREFIX_FILE)) {
      String name = column.substring(PREFIX_FILE.length()).toLowerCase(Locale.ROOT);
      if (!FilterExpressionValidator.FILE_FILTER_COLUMNS.contains(name)) {
        throw new IllegalArgumentException(
            "Unknown file column: " + column.substring(PREFIX_FILE.length()));
      }
      return name;
    }

    if (upperKey.startsWith(PREFIX_DIM)) {
      String dimKey = stripBackticks(column.substring(PREFIX_DIM.length()));
      if (registry == null) {
        throw new IllegalArgumentException("Cannot resolve __DIM. without a column registry");
      }
      String physical = registry.getDimKeyToColumnMap().get(dimKey);
      if (physical == null) {
        throw new IllegalArgumentException("Unknown dimension: " + dimKey);
      }
      return physical;
    }

    AggPrefixMatch aggMatch = parseAggPrefix(upperKey);
    if (aggMatch != null) {
      String remainder = stripBackticks(column.substring(aggMatch.prefixLength));
      int lastDot = remainder.lastIndexOf('.');
      String aggKey;
      String aggValue;
      if (lastDot >= 0) {
        aggKey = remainder.substring(0, lastDot);
        aggValue = remainder.substring(lastDot + 1);
      } else {
        aggKey = remainder;
        aggValue = null;
      }
      if (registry == null) {
        throw new IllegalArgumentException(
            "Cannot resolve __AGG. prefix without a column registry");
      }
      String physical = registry.aggLookup(aggKey, aggValue, aggMatch.type);
      if (physical == null) {
        throw new IllegalArgumentException(
            "Unknown aggregate: "
                + aggKey
                + (aggValue != null ? "." + aggValue : "")
                + " (type="
                + aggMatch.type
                + ")");
      }
      return physical;
    }

    // Unqualified: file → dimension → aggregate → error
    String lowerKey = column.toLowerCase(Locale.ROOT);
    if (FilterExpressionValidator.FILE_FILTER_COLUMNS.contains(lowerKey)) {
      return lowerKey;
    }

    if (registry != null) {
      String physical = registry.getDimKeyToColumnMap().get(column);
      if (physical != null) {
        return physical;
      }
      physical = registry.getAggKeyToColumnMap().get(column);
      if (physical != null) {
        return physical;
      }
    }

    throw new IllegalArgumentException(
        "Unknown column: \""
            + column
            + "\". Use __FILE., __DIM., or __AGG_<TYPE>. prefix for explicit targeting.");
  }

  /**
   * Parse and resolve a filter expression in strict mode, returning the mutated AST.
   *
   * <p>Strict mode validates that every column reference resolves to a known column. Unknown column
   * names cause an {@link IllegalArgumentException} instead of passing through to the database
   * (which would produce an opaque SQL error).
   *
   * <p>Supports namespace prefixes for unambiguous column targeting:
   *
   * <ul>
   *   <li>{@code __FILE.record_count} — file-level metadata columns
   *   <li>{@code __DIM.zone} — dimension columns via registry
   *   <li>{@code __AGG.level.warn} or {@code __AGG_EQ.level.warn} — exact-match aggregate
   *   <li>{@code __AGG_GTE.level.warn} — cumulative (>=) aggregate
   *   <li>{@code __AGG_GT.level.warn}, {@code __AGG_LTE.level.warn}, {@code __AGG_LT.level.warn}
   *   <li>{@code __AGG_SUM.}, {@code __AGG_AVG.}, {@code __AGG_MIN.}, {@code __AGG_MAX.}
   * </ul>
   *
   * <p>Unqualified names are resolved by trying: file → dimension → aggregate → error.
   *
   * @param filterExpression SQL WHERE clause fragment; must be non-null and non-empty
   * @param registry column registry for name resolution; must be non-null for strict mode
   * @return the resolved (mutated) expression AST
   * @throws IllegalArgumentException if any column name cannot be resolved
   */
  static Expression resolveToExpressionStrict(String filterExpression, ColumnRegistry registry) {
    if (filterExpression.contains(";")) {
      throw new IllegalArgumentException("Semicolons are not allowed in filter expressions");
    }
    Expression expr;
    try {
      expr = CCJSqlParserUtil.parseCondExpression(filterExpression);
    } catch (JSQLParserException e) {
      throw new IllegalArgumentException("Failed to parse filter expression: " + e.getMessage(), e);
    }
    if (registry != null) {
      resolveExpressionStrict(expr, registry);
    } else {
      LOG.warn("Strict resolver called without registry; unknown columns will not be caught");
      resolveExpression(expr, Map.of(), Map.of());
    }
    return expr;
  }

  private static void resolveExpressionStrict(Expression expr, ColumnRegistry registry) {
    if (expr instanceof AndExpression and) {
      resolveExpressionStrict(and.getLeftExpression(), registry);
      resolveExpressionStrict(and.getRightExpression(), registry);
    } else if (expr instanceof OrExpression or) {
      resolveExpressionStrict(or.getLeftExpression(), registry);
      resolveExpressionStrict(or.getRightExpression(), registry);
    } else if (expr instanceof ComparisonOperator comparison) {
      resolveExpressionStrict(comparison.getLeftExpression(), registry);
      resolveExpressionStrict(comparison.getRightExpression(), registry);
    } else if (expr instanceof ParenthesedExpressionList<?> parenList && parenList.size() == 1) {
      resolveExpressionStrict(parenList.get(0), registry);
    } else if (expr instanceof NotExpression not) {
      resolveExpressionStrict(not.getExpression(), registry);
    } else if (expr instanceof InExpression inExpr) {
      resolveExpressionStrict(inExpr.getLeftExpression(), registry);
      Expression rightExpr = inExpr.getRightExpression();
      if (rightExpr instanceof ExpressionList<?> exprList) {
        for (Expression child : exprList) {
          resolveExpressionStrict(child, registry);
        }
      }
    } else if (expr instanceof IsNullExpression isNull) {
      resolveExpressionStrict(isNull.getLeftExpression(), registry);
    } else if (expr instanceof Between between) {
      resolveExpressionStrict(between.getLeftExpression(), registry);
      resolveExpressionStrict(between.getBetweenExpressionStart(), registry);
      resolveExpressionStrict(between.getBetweenExpressionEnd(), registry);
    } else if (expr instanceof Column col) {
      resolveColumnStrict(col, registry);
    }
    // Literals (LongValue, DoubleValue, StringValue, NullValue) — nothing to resolve
  }

  /**
   * Resolve a column reference in strict mode. Handles namespace prefixes and validates that the
   * column name resolves to a known column.
   */
  private static void resolveColumnStrict(Column col, ColumnRegistry registry) {
    String logicalKey = reconstructLogicalKey(col);
    if (logicalKey == null) {
      return;
    }

    String upperKey = logicalKey.toUpperCase(Locale.ROOT);

    // --- Namespace-prefixed resolution ---
    if (upperKey.startsWith(PREFIX_FILE)) {
      String name = logicalKey.substring(PREFIX_FILE.length());
      if (!FilterExpressionValidator.FILE_FILTER_COLUMNS.contains(name.toLowerCase(Locale.ROOT))) {
        throw new IllegalArgumentException("Unknown file column: " + name);
      }
      col.setColumnName(name);
      col.setTable(null);
      return;
    }

    if (upperKey.startsWith(PREFIX_DIM)) {
      String dimKey = logicalKey.substring(PREFIX_DIM.length());
      dimKey = stripBackticks(dimKey);
      Map<String, String> dimMap = registry.getDimKeyToColumnMap();
      String physical = dimMap.get(dimKey);
      if (physical == null) {
        throw new IllegalArgumentException("Unknown dimension: " + dimKey);
      }
      col.setColumnName(physical);
      col.setTable(null);
      return;
    }

    // Check for __AGG* prefixes (with optional aggregation type suffix)
    AggPrefixMatch aggMatch = parseAggPrefix(upperKey);
    if (aggMatch != null) {
      String remainder = logicalKey.substring(aggMatch.prefixLength);
      remainder = stripBackticks(remainder);
      // Split on rightmost dot → (aggKey, aggValue)
      int lastDot = remainder.lastIndexOf('.');
      String aggKey;
      String aggValue;
      if (lastDot >= 0) {
        aggKey = remainder.substring(0, lastDot);
        aggValue = remainder.substring(lastDot + 1);
      } else {
        aggKey = remainder;
        aggValue = null;
      }
      String physical = registry.aggLookup(aggKey, aggValue, aggMatch.type);
      if (physical == null) {
        throw new IllegalArgumentException(
            "Unknown aggregate: "
                + aggKey
                + (aggValue != null ? "." + aggValue : "")
                + " (type="
                + aggMatch.type
                + ")");
      }
      col.setColumnName(physical);
      col.setTable(null);
      return;
    }

    // --- Unqualified resolution: file → dimension → aggregate → error ---
    String lowerKey = logicalKey.toLowerCase(Locale.ROOT);
    if (FilterExpressionValidator.FILE_FILTER_COLUMNS.contains(lowerKey)) {
      // Built-in column — clear any table part from JSqlParser splitting
      col.setColumnName(logicalKey);
      col.setTable(null);
      return;
    }

    Map<String, String> dimMap = registry.getDimKeyToColumnMap();
    String physical = dimMap.get(logicalKey);
    if (physical != null) {
      col.setColumnName(physical);
      col.setTable(null);
      return;
    }

    Map<String, String> aggMap = registry.getAggKeyToColumnMap();
    physical = aggMap.get(logicalKey);
    if (physical != null) {
      col.setColumnName(physical);
      col.setTable(null);
      return;
    }

    throw new IllegalArgumentException(
        "Unknown column in filter expression: "
            + logicalKey
            + ". Use __FILE., __DIM., or __AGG_<TYPE>. prefix for explicit targeting.");
  }

  private record AggPrefixMatch(AggregationType type, int prefixLength) {}

  /**
   * Parse an __AGG* prefix and return the match with its actual length, or null if the key doesn't
   * start with an __AGG prefix. Longer prefixes are checked first to avoid false matches (e.g.,
   * __AGG_GTE before __AGG_GT).
   */
  private static AggPrefixMatch parseAggPrefix(String upperKey) {
    if (upperKey.startsWith("__AGG_EQ.")) return new AggPrefixMatch(AggregationType.EQ, 9);
    if (upperKey.startsWith("__AGG_GTE.")) return new AggPrefixMatch(AggregationType.GTE, 10);
    if (upperKey.startsWith("__AGG_GT.")) return new AggPrefixMatch(AggregationType.GT, 9);
    if (upperKey.startsWith("__AGG_LTE.")) return new AggPrefixMatch(AggregationType.LTE, 10);
    if (upperKey.startsWith("__AGG_LT.")) return new AggPrefixMatch(AggregationType.LT, 9);
    if (upperKey.startsWith("__AGG_SUM.")) return new AggPrefixMatch(AggregationType.SUM, 10);
    if (upperKey.startsWith("__AGG_AVG.")) return new AggPrefixMatch(AggregationType.AVG, 10);
    if (upperKey.startsWith("__AGG_MIN.")) return new AggPrefixMatch(AggregationType.MIN, 10);
    if (upperKey.startsWith("__AGG_MAX.")) return new AggPrefixMatch(AggregationType.MAX, 10);
    if (upperKey.startsWith("__AGG.")) return new AggPrefixMatch(AggregationType.EQ, 6);
    return null;
  }

  private static void resolveExpression(
      Expression expr, Map<String, String> dimMap, Map<String, String> aggMap) {
    if (expr instanceof AndExpression and) {
      resolveExpression(and.getLeftExpression(), dimMap, aggMap);
      resolveExpression(and.getRightExpression(), dimMap, aggMap);
    } else if (expr instanceof OrExpression or) {
      resolveExpression(or.getLeftExpression(), dimMap, aggMap);
      resolveExpression(or.getRightExpression(), dimMap, aggMap);
    } else if (expr instanceof ComparisonOperator comparison) {
      resolveExpression(comparison.getLeftExpression(), dimMap, aggMap);
      resolveExpression(comparison.getRightExpression(), dimMap, aggMap);
    } else if (expr instanceof ParenthesedExpressionList<?> parenList && parenList.size() == 1) {
      resolveExpression(parenList.get(0), dimMap, aggMap);
    } else if (expr instanceof NotExpression not) {
      resolveExpression(not.getExpression(), dimMap, aggMap);
    } else if (expr instanceof InExpression inExpr) {
      resolveExpression(inExpr.getLeftExpression(), dimMap, aggMap);
      Expression rightExpr = inExpr.getRightExpression();
      if (rightExpr instanceof ExpressionList<?> exprList) {
        for (Expression child : exprList) {
          resolveExpression(child, dimMap, aggMap);
        }
      }
    } else if (expr instanceof IsNullExpression isNull) {
      resolveExpression(isNull.getLeftExpression(), dimMap, aggMap);
    } else if (expr instanceof Between between) {
      resolveExpression(between.getLeftExpression(), dimMap, aggMap);
      resolveExpression(between.getBetweenExpressionStart(), dimMap, aggMap);
      resolveExpression(between.getBetweenExpressionEnd(), dimMap, aggMap);
    } else if (expr instanceof Column col) {
      resolveColumn(col, dimMap, aggMap);
    } else if (!(expr instanceof LongValue)
        && !(expr instanceof DoubleValue)
        && !(expr instanceof StringValue)
        && !(expr instanceof NullValue)) {
      // Unknown expression type — columns inside it won't be resolved.
      // If the validator whitelist is expanded to accept this type, add resolution logic here.
      LOG.trace(
          "Skipping resolution for unhandled expression type: {}", expr.getClass().getSimpleName());
    }
  }

  /**
   * Reconstruct the logical key from JSqlParser's split schema/table/column parts and resolve it.
   */
  private static void resolveColumn(
      Column col, Map<String, String> dimMap, Map<String, String> aggMap) {
    String logicalKey = reconstructLogicalKey(col);
    if (logicalKey == null) {
      return;
    }

    String physical = dimMap.get(logicalKey);
    if (physical == null) {
      physical = aggMap.get(logicalKey);
    }
    if (physical != null) {
      col.setColumnName(physical);
      col.setTable(null);
    } else {
      // Strip backticks from unresolved columns so they pass through cleanly
      String colName = col.getColumnName();
      if (colName != null
          && colName.length() >= 2
          && colName.charAt(0) == '`'
          && colName.charAt(colName.length() - 1) == '`') {
        col.setColumnName(colName.substring(1, colName.length() - 1));
      }
    }
  }

  /**
   * Reconstruct the dotted logical key from a JSqlParser Column node.
   *
   * <p>JSqlParser splits dotted identifiers:
   *
   * <ul>
   *   <li>"state" → table=null, col="state" → key="state"
   *   <li>"service.name" → table="service", col="name" → key="service.name"
   *   <li>"k8s.pod.name" → schema="k8s", table="pod", col="name" → key="k8s.pod.name"
   *   <li>"`service.name`" → table=null, col="`service.name`" → key="service.name"
   * </ul>
   */
  static String reconstructLogicalKey(Column col) {
    String colName = col.getColumnName();
    if (colName == null) {
      return null;
    }

    // Strip backtick quoting if present
    colName = stripBackticks(colName);

    Table table = col.getTable();
    if (table == null || table.getName() == null) {
      return colName;
    }

    String tablePart = table.getName();
    String schemaPart = table.getSchemaName();

    if (schemaPart != null) {
      return schemaPart + "." + tablePart + "." + colName;
    }
    return tablePart + "." + colName;
  }

  private static String stripBackticks(String name) {
    if (name.length() >= 2 && name.charAt(0) == '`' && name.charAt(name.length() - 1) == '`') {
      return name.substring(1, name.length() - 1);
    }
    return name;
  }
}
