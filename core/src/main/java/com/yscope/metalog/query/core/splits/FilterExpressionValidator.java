package com.yscope.metalog.query.core.splits;

import java.util.Set;
import java.util.regex.Pattern;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Select;

/**
 * Validates SQL filter expressions by parsing with JSqlParser and walking the AST with an explicit
 * whitelist.
 *
 * <p>Only safe constructs (comparisons, boolean logic, literals, simple column names) are allowed.
 * Anything not explicitly whitelisted — including subqueries, function calls, arithmetic, UNION,
 * CASE, CAST — is rejected. This is safer than a visitor-based approach which silently accepts
 * unknown expression types by default.
 */
public class FilterExpressionValidator {

  private static final Pattern VALID_COLUMN_NAME = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

  /**
   * File-level columns that are always valid in filter expressions. These describe the file as a
   * storage object (location, lifecycle, retention, sizes) and exist in every metadata table
   * regardless of the column registry.
   */
  public static final Set<String> FILE_FILTER_COLUMNS =
      Set.of(
          "id",
          "state",
          "clp_ir_storage_backend",
          "clp_ir_bucket",
          "clp_ir_path",
          "clp_ir_path_hash",
          "clp_archive_storage_backend",
          "clp_archive_bucket",
          "clp_archive_path",
          "clp_archive_path_hash",
          "min_timestamp",
          "max_timestamp",
          "clp_archive_created_at",
          "record_count",
          "raw_size_bytes",
          "clp_ir_size_bytes",
          "clp_archive_size_bytes",
          "retention_days",
          "expires_at",
          "created_at",
          "updated_at");

  /**
   * Validates that a filter expression contains only safe SQL constructs.
   *
   * <p>Use this overload when the expression has already been parsed (e.g. by {@link
   * FilterExpressionResolver#resolveToExpression}) to avoid a redundant parse.
   *
   * @param expr already-parsed expression to validate
   * @throws IllegalArgumentException if the expression contains disallowed constructs
   */
  public static void validate(Expression expr) {
    validateExpression(expr);
  }

  /**
   * Validates that a filter expression contains only safe SQL constructs.
   *
   * @param filterExpression SQL WHERE clause fragment to validate, or null/empty for no filtering
   * @throws IllegalArgumentException if the expression contains disallowed constructs or cannot be
   *     parsed
   */
  public static void validate(String filterExpression) {
    if (filterExpression == null || filterExpression.isEmpty()) {
      return;
    }

    // Reject semicolons upfront: parseCondExpression silently ignores content after ';',
    // but we concatenate the original string, so trailing SQL would pass through unvalidated.
    if (filterExpression.contains(";")) {
      throw new IllegalArgumentException("Semicolons are not allowed in filter expressions");
    }

    Expression expr;
    try {
      expr = CCJSqlParserUtil.parseCondExpression(filterExpression);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse filter expression: " + e.getMessage(), e);
    }

    validateExpression(expr);
  }

  private static void validateExpression(Expression expr) {
    if (expr instanceof AndExpression and) {
      validateExpression(and.getLeftExpression());
      validateExpression(and.getRightExpression());
    } else if (expr instanceof OrExpression or) {
      validateExpression(or.getLeftExpression());
      validateExpression(or.getRightExpression());
    } else if (expr instanceof EqualsTo eq) {
      validateExpression(eq.getLeftExpression());
      validateExpression(eq.getRightExpression());
    } else if (expr instanceof NotEqualsTo neq) {
      validateExpression(neq.getLeftExpression());
      validateExpression(neq.getRightExpression());
    } else if (expr instanceof GreaterThan gt) {
      validateExpression(gt.getLeftExpression());
      validateExpression(gt.getRightExpression());
    } else if (expr instanceof GreaterThanEquals gte) {
      validateExpression(gte.getLeftExpression());
      validateExpression(gte.getRightExpression());
    } else if (expr instanceof MinorThan lt) {
      validateExpression(lt.getLeftExpression());
      validateExpression(lt.getRightExpression());
    } else if (expr instanceof MinorThanEquals lte) {
      validateExpression(lte.getLeftExpression());
      validateExpression(lte.getRightExpression());
    } else if (expr instanceof ParenthesedExpressionList<?> parenList && parenList.size() == 1) {
      validateExpression(parenList.get(0));
    } else if (expr instanceof NotExpression not) {
      validateExpression(not.getExpression());
    } else if (expr instanceof InExpression inExpr) {
      validateExpression(inExpr.getLeftExpression());
      Expression rightExpr = inExpr.getRightExpression();
      if (rightExpr instanceof Select) {
        throw new IllegalArgumentException("Subqueries are not allowed in filter expressions");
      } else if (rightExpr instanceof ExpressionList<?> exprList) {
        for (Expression child : exprList) {
          validateExpression(child);
        }
      } else if (rightExpr != null) {
        throw new IllegalArgumentException(
            "Unsupported IN right-hand side type: " + rightExpr.getClass().getSimpleName());
      }
    } else if (expr instanceof IsNullExpression isNull) {
      validateExpression(isNull.getLeftExpression());
    } else if (expr instanceof LikeExpression like) {
      validateExpression(like.getLeftExpression());
      validateExpression(like.getRightExpression());
    } else if (expr instanceof Between between) {
      validateExpression(between.getLeftExpression());
      validateExpression(between.getBetweenExpressionStart());
      validateExpression(between.getBetweenExpressionEnd());
    } else if (expr instanceof Column col) {
      if (col.getTable() != null && col.getTable().getName() != null) {
        throw new IllegalArgumentException(
            "Table-qualified columns are not allowed: " + col.getFullyQualifiedName());
      }
      String columnName = col.getColumnName();
      if (!VALID_COLUMN_NAME.matcher(columnName).matches()) {
        throw new IllegalArgumentException("Invalid column name: " + columnName);
      }
    } else if (expr instanceof LongValue
        || expr instanceof DoubleValue
        || expr instanceof StringValue
        || expr instanceof NullValue) {
      // Leaf literal values — safe, nothing to recurse into
    } else {
      throw new IllegalArgumentException(
          "Disallowed expression type in filter: "
              + expr.getClass().getSimpleName()
              + " ("
              + expr
              + ")");
    }
  }
}
