package com.yscope.clp.service.query.core.splits;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;

/**
 * Extracts {@code __SKETCH.*} predicates from a filter expression before it is sent to the DB.
 *
 * <p>Sketch predicates reference probabilistic data structures stored in the {@code ext} column
 * that the DB cannot filter on directly. This class parses the expression, removes any top-level
 * {@code __SKETCH.*} conjuncts, and returns them as {@link SketchPredicate}s for in-process
 * evaluation.
 *
 * <p>Sketch predicates must appear as top-level AND conjuncts — embedding them inside OR or NOT is
 * rejected with {@link IllegalArgumentException} (→ gRPC {@code INVALID_ARGUMENT}).
 */
class SketchPredicateExtractor {

  private static final String PREFIX_SKETCH = "__SKETCH.";

  private final List<SketchPredicate> predicates = new ArrayList<>();

  private SketchPredicateExtractor() {}

  /**
   * Parse {@code filterExpression} and separate any {@code __SKETCH.*} predicates from it.
   *
   * @param filterExpression SQL WHERE clause fragment, or null/empty
   * @return extraction result; {@code cleanedExpression} is null when the whole expression was
   *     sketch predicates, or unchanged when no sketch predicates were found
   * @throws IllegalArgumentException if a sketch predicate appears inside OR or NOT, or uses an
   *     unsupported comparison operator, or has a non-string RHS
   */
  static SketchExtractionResult extract(String filterExpression) {
    if (filterExpression == null || filterExpression.isEmpty()) {
      return new SketchExtractionResult(filterExpression, List.of());
    }

    Expression parsed;
    try {
      parsed = CCJSqlParserUtil.parseCondExpression(filterExpression);
    } catch (JSQLParserException e) {
      // Parse failure — return unchanged; downstream resolver will produce the definitive error.
      return new SketchExtractionResult(filterExpression, List.of());
    }

    SketchPredicateExtractor extractor = new SketchPredicateExtractor();
    Optional<Expression> cleaned = extractor.extractExpr(parsed, false);

    String cleanedExpression = cleaned.map(Expression::toString).orElse(null);
    return new SketchExtractionResult(cleanedExpression, List.copyOf(extractor.predicates));
  }

  /**
   * Recursively walk an expression node, extracting sketch predicates.
   *
   * @param expr the expression to process
   * @param inOrOrNot true when inside an OR or NOT — sketch predicates are not allowed here
   * @return {@code Optional.empty()} if the entire sub-expression was sketch predicates (treat as
   *     TRUE and elide from the cleaned expression); {@code Optional.of(e)} otherwise
   */
  private Optional<Expression> extractExpr(Expression expr, boolean inOrOrNot) {
    if (expr instanceof AndExpression and) {
      Optional<Expression> left = extractExpr(and.getLeftExpression(), inOrOrNot);
      Optional<Expression> right = extractExpr(and.getRightExpression(), inOrOrNot);
      if (left.isEmpty() && right.isEmpty()) {
        return Optional.empty();
      }
      if (left.isEmpty()) {
        return right;
      }
      if (right.isEmpty()) {
        return left;
      }
      return Optional.of(new AndExpression(left.get(), right.get()));

    } else if (expr instanceof OrExpression or) {
      // Descend only to detect sketch predicates inside OR (which are rejected).
      extractExpr(or.getLeftExpression(), true);
      extractExpr(or.getRightExpression(), true);
      return Optional.of(or);

    } else if (expr instanceof ParenthesedExpressionList<?> parenList && parenList.size() == 1) {
      Optional<Expression> inner = extractExpr(parenList.get(0), inOrOrNot);
      return inner.map(e -> new ParenthesedExpressionList<>(new ExpressionList<>(e)));

    } else if (expr instanceof NotExpression not) {
      // Descend only to detect sketch predicates inside NOT (which are rejected).
      extractExpr(not.getExpression(), true);
      return Optional.of(not);

    } else if (expr instanceof EqualsTo eq) {
      return handleComparison(eq, eq.getLeftExpression(), eq.getRightExpression(), inOrOrNot);

    } else if (expr instanceof ComparisonOperator cmp) {
      // Non-equals comparisons: check left side for sketch prefix, then reject.
      Expression left = cmp.getLeftExpression();
      if (left instanceof Column col && isSketchColumn(col)) {
        if (inOrOrNot) {
          throw new IllegalArgumentException(
              "__SKETCH predicates must not appear inside OR or NOT");
        }
        throw new IllegalArgumentException(
            "__SKETCH predicates only support = and IN operators, not: "
                + cmp.getStringExpression());
      }
      return Optional.of(expr);

    } else if (expr instanceof InExpression inExpr) {
      return handleInExpression(inExpr, inOrOrNot);

    } else if (expr instanceof Column col && isSketchColumn(col)) {
      // Bare sketch column reference (unusual, but detect it).
      if (inOrOrNot) {
        throw new IllegalArgumentException("__SKETCH predicates must not appear inside OR or NOT");
      }
      // Cannot extract a meaningful predicate from a bare column — fall through unchanged.
      return Optional.of(expr);

    } else {
      return Optional.of(expr);
    }
  }

  private Optional<Expression> handleComparison(
      EqualsTo eq, Expression leftExpr, Expression rightExpr, boolean inOrOrNot) {
    if (!(leftExpr instanceof Column col) || !isSketchColumn(col)) {
      return Optional.of(eq);
    }
    if (inOrOrNot) {
      throw new IllegalArgumentException("__SKETCH predicates must not appear inside OR or NOT");
    }
    if (!(rightExpr instanceof StringValue sv)) {
      throw new IllegalArgumentException(
          "__SKETCH predicate RHS must be a string literal, got: " + rightExpr);
    }
    String field = sketchFieldName(col);
    predicates.add(new SketchPredicate(field, Set.of(sv.getValue())));
    return Optional.empty();
  }

  private Optional<Expression> handleInExpression(InExpression inExpr, boolean inOrOrNot) {
    Expression leftExpr = inExpr.getLeftExpression();
    if (!(leftExpr instanceof Column col) || !isSketchColumn(col)) {
      return Optional.of(inExpr);
    }
    if (inOrOrNot) {
      throw new IllegalArgumentException("__SKETCH predicates must not appear inside OR or NOT");
    }

    Set<String> values = new HashSet<>();
    Expression rightExpr = inExpr.getRightExpression();
    if (rightExpr instanceof ExpressionList<?> exprList) {
      for (Expression item : exprList) {
        if (item instanceof StringValue sv) {
          values.add(sv.getValue());
        } else {
          throw new IllegalArgumentException(
              "__SKETCH IN list must contain only string literals, got: " + item);
        }
      }
    }

    String field = sketchFieldName(col);
    predicates.add(new SketchPredicate(field, Set.copyOf(values)));
    return Optional.empty();
  }

  /** Returns true if the column's reconstructed logical key starts with {@code __SKETCH.}. */
  private static boolean isSketchColumn(Column col) {
    String key = FilterExpressionResolver.reconstructLogicalKey(col);
    return key != null && key.toUpperCase(Locale.ROOT).startsWith(PREFIX_SKETCH);
  }

  /**
   * Extract the logical field name from a {@code __SKETCH.<field>} column reference.
   *
   * <p>For example, {@code __SKETCH.user_id} → {@code "user_id"}.
   */
  private static String sketchFieldName(Column col) {
    String key = FilterExpressionResolver.reconstructLogicalKey(col);
    // key is guaranteed non-null and to start with __SKETCH. (checked by caller)
    return key.substring(PREFIX_SKETCH.length());
  }
}
