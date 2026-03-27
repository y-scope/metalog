package query

import (
	"testing"
)

func TestWalkExpr_ComparisonWithRegexp(t *testing.T) {
	// REGEXP should be valid (it's a comparison operator)
	if err := ValidateFilterExpression("x REGEXP '^test'"); err != nil {
		t.Errorf("REGEXP should be valid: %v", err)
	}
}

func TestWalkExpr_IsTrue(t *testing.T) {
	// IS TRUE / IS FALSE should be allowed
	if err := ValidateFilterExpression("x IS TRUE"); err != nil {
		t.Errorf("IS TRUE should be valid: %v", err)
	}
	if err := ValidateFilterExpression("x IS FALSE"); err != nil {
		t.Errorf("IS FALSE should be valid: %v", err)
	}
}

func TestWalkExpr_ValTupleInComparison(t *testing.T) {
	// Tuple with mixed types
	if err := ValidateFilterExpression("x IN (1, 2, 3)"); err != nil {
		t.Errorf("IN with int tuple should be valid: %v", err)
	}
	if err := ValidateFilterExpression("x IN ('a', 'b', 'c')"); err != nil {
		t.Errorf("IN with string tuple should be valid: %v", err)
	}
}

func TestWalkExpr_NotExpr(t *testing.T) {
	if err := ValidateFilterExpression("NOT (x = 1 AND y = 2)"); err != nil {
		t.Errorf("NOT with nested expr should be valid: %v", err)
	}
}

func TestWalkExpr_NestedOrAnd(t *testing.T) {
	if err := ValidateFilterExpression("(a = 1 OR b = 2) AND (c = 3 OR d = 4)"); err != nil {
		t.Errorf("nested OR/AND should be valid: %v", err)
	}
}

func TestWalkExpr_BetweenExpr(t *testing.T) {
	if err := ValidateFilterExpression("x BETWEEN 1 AND 100"); err != nil {
		t.Errorf("BETWEEN should be valid: %v", err)
	}
}

func TestWalkExpr_NullVal(t *testing.T) {
	if err := ValidateFilterExpression("x = NULL"); err != nil {
		t.Errorf("NULL literal should be valid: %v", err)
	}
}

func TestWalkExpr_UnaryNegative(t *testing.T) {
	if err := ValidateFilterExpression("x > -1"); err != nil {
		t.Errorf("unary negative should be valid: %v", err)
	}
}

func TestWalkExpr_LikeExpr(t *testing.T) {
	if err := ValidateFilterExpression("x LIKE '%test%'"); err != nil {
		t.Errorf("LIKE should be valid: %v", err)
	}
	if err := ValidateFilterExpression("x NOT LIKE '%test%'"); err != nil {
		t.Errorf("NOT LIKE should be valid: %v", err)
	}
}

func TestWalkExpr_BoolVal(t *testing.T) {
	if err := ValidateFilterExpression("x = TRUE"); err != nil {
		t.Errorf("TRUE literal should be valid: %v", err)
	}
	if err := ValidateFilterExpression("x = FALSE"); err != nil {
		t.Errorf("FALSE literal should be valid: %v", err)
	}
}

func TestWalkExpr_FunctionCallRejected(t *testing.T) {
	err := ValidateFilterExpression("NOW() > x")
	if err == nil {
		t.Error("function calls should be rejected")
	}
}

func TestWalkExpr_ComparisonWithEscape(t *testing.T) {
	// LIKE with ESCAPE clause exercises the Escape branch in ComparisonExpr.
	// Vitess parser requires the ESCAPE value as a single char.
	if err := ValidateFilterExpression("x LIKE '%test%' ESCAPE '$'"); err != nil {
		t.Errorf("LIKE with ESCAPE should be valid: %v", err)
	}
}

func TestWalkExpr_BetweenWithColumnRefs(t *testing.T) {
	// BETWEEN with column refs on all positions.
	if err := ValidateFilterExpression("x BETWEEN y AND z"); err != nil {
		t.Errorf("BETWEEN with column refs should be valid: %v", err)
	}
}

func TestWalkExpr_ValTupleWithMixed(t *testing.T) {
	// Tuple with NULL and booleans.
	if err := ValidateFilterExpression("x IN (1, NULL, TRUE)"); err != nil {
		t.Errorf("IN with mixed tuple should be valid: %v", err)
	}
}

// Test walkExpr error paths through ValidateFilterExpression.

func TestWalkExpr_AndLeftUnsafe(t *testing.T) {
	// Left side of AND has function call -> rejected.
	if err := ValidateFilterExpression("NOW() = 1 AND x = 2"); err == nil {
		t.Error("expected error for function call on left side of AND")
	}
}

func TestWalkExpr_OrLeftUnsafe(t *testing.T) {
	// Left side of OR has function call -> rejected.
	if err := ValidateFilterExpression("NOW() = 1 OR x = 2"); err == nil {
		t.Error("expected error for function call on left side of OR")
	}
}

func TestWalkExpr_ComparisonEscapeUnsafe(t *testing.T) {
	// ESCAPE with a function call expression -> rejected.
	if err := ValidateFilterExpression("x LIKE '%test%' ESCAPE CONCAT('a', 'b')"); err == nil {
		t.Error("expected error for function call in ESCAPE")
	}
}

func TestWalkExpr_BetweenFromUnsafe(t *testing.T) {
	// BETWEEN with function call in FROM position.
	if err := ValidateFilterExpression("x BETWEEN NOW() AND 100"); err == nil {
		t.Error("expected error for function call in BETWEEN from")
	}
}

func TestWalkExpr_BetweenToUnsafe(t *testing.T) {
	// BETWEEN with function call in TO position.
	if err := ValidateFilterExpression("x BETWEEN 1 AND NOW()"); err == nil {
		t.Error("expected error for function call in BETWEEN to")
	}
}

func TestWalkExpr_ValTupleUnsafe(t *testing.T) {
	// IN with subquery in tuple position.
	if err := ValidateFilterExpression("x IN (1, NOW())"); err == nil {
		t.Error("expected error for function call in IN tuple")
	}
}

func TestRewriteFilterColumns_BetweenWithVirtual(t *testing.T) {
	reg := newTestRegistry(t)
	rewritten, err := RewriteFilterColumns("__DIM.region BETWEEN 'a' AND 'z'", reg)
	if err != nil {
		t.Fatal(err)
	}
	if rewritten == "" {
		t.Error("expected non-empty rewritten expression")
	}
}

func TestRewriteFilterColumns_NotExpr(t *testing.T) {
	reg := newTestRegistry(t)
	rewritten, err := RewriteFilterColumns("NOT __DIM.region = 'us-east'", reg)
	if err != nil {
		t.Fatal(err)
	}
	if rewritten == "" {
		t.Error("expected non-empty rewritten expression")
	}
}

func TestRewriteFilterColumns_IsNull(t *testing.T) {
	reg := newTestRegistry(t)
	rewritten, err := RewriteFilterColumns("__DIM.region IS NULL", reg)
	if err != nil {
		t.Fatal(err)
	}
	if rewritten == "" {
		t.Error("expected non-empty rewritten expression")
	}
}

func TestRewriteFilterColumns_OrExpr(t *testing.T) {
	reg := newTestRegistry(t)
	rewritten, err := RewriteFilterColumns("__DIM.region = 'us' OR __DIM.region = 'eu'", reg)
	if err != nil {
		t.Fatal(err)
	}
	if rewritten == "" {
		t.Error("expected non-empty rewritten expression")
	}
}

// Error-path tests for rewriteExpr branches — ensure errors propagate correctly
// when a virtual column inside a compound expression cannot be resolved.

func TestRewriteFilterColumns_OrExprErrorLeft(t *testing.T) {
	// Left side of OR has an unresolvable __FILE.bogus column.
	_, err := RewriteFilterColumns("__FILE.bogus = 1 OR __FILE.state = 'IR_CLOSED'", nil)
	if err == nil {
		t.Error("expected error for unknown __FILE.bogus on left side of OR")
	}
}

func TestRewriteFilterColumns_OrExprErrorRight(t *testing.T) {
	// Right side of OR has an unresolvable __FILE.bogus column.
	_, err := RewriteFilterColumns("__FILE.state = 'IR_CLOSED' OR __FILE.bogus = 1", nil)
	if err == nil {
		t.Error("expected error for unknown __FILE.bogus on right side of OR")
	}
}

func TestRewriteFilterColumns_NotExprError(t *testing.T) {
	// NOT wrapping an unresolvable column.
	_, err := RewriteFilterColumns("NOT __FILE.bogus = 1", nil)
	if err == nil {
		t.Error("expected error for unknown __FILE.bogus inside NOT")
	}
}

func TestRewriteFilterColumns_BetweenExprErrorLeft(t *testing.T) {
	// BETWEEN with an unresolvable left column.
	_, err := RewriteFilterColumns("__FILE.bogus BETWEEN 1 AND 100", nil)
	if err == nil {
		t.Error("expected error for unknown __FILE.bogus as BETWEEN left side")
	}
}

func TestRewriteFilterColumns_BetweenExprErrorFrom(t *testing.T) {
	// BETWEEN with a bad DIM reference in the FROM expression.
	// To trigger the "from" error path we use nil registry with a __DIM ref.
	_, err := RewriteFilterColumns("record_count BETWEEN __DIM.x AND 100", nil)
	if err == nil {
		t.Error("expected error for __DIM.x in BETWEEN from without registry")
	}
}

func TestRewriteFilterColumns_BetweenExprErrorTo(t *testing.T) {
	// BETWEEN with a bad DIM reference in the TO expression.
	_, err := RewriteFilterColumns("record_count BETWEEN 1 AND __DIM.x", nil)
	if err == nil {
		t.Error("expected error for __DIM.x in BETWEEN to without registry")
	}
}

func TestRewriteFilterColumns_IsExprError(t *testing.T) {
	// IS NULL with an unresolvable __FILE.bogus column.
	_, err := RewriteFilterColumns("__FILE.bogus IS NULL", nil)
	if err == nil {
		t.Error("expected error for unknown __FILE.bogus in IS NULL")
	}
}

func TestRewriteFilterColumns_ComparisonExprErrorLeft(t *testing.T) {
	// Comparison with unresolvable __FILE.bogus on left.
	_, err := RewriteFilterColumns("__FILE.bogus > 100", nil)
	if err == nil {
		t.Error("expected error for unknown __FILE.bogus on left side of comparison")
	}
}

func TestRewriteFilterColumns_AndExprErrorLeft(t *testing.T) {
	// AND with unresolvable left side.
	_, err := RewriteFilterColumns("__FILE.bogus = 1 AND state = 'IR_CLOSED'", nil)
	if err == nil {
		t.Error("expected error for unknown __FILE.bogus on left side of AND")
	}
}

func TestRewriteFilterColumns_AndExprErrorRight(t *testing.T) {
	// AND with unresolvable right side.
	_, err := RewriteFilterColumns("state = 'IR_CLOSED' AND __FILE.bogus = 1", nil)
	if err == nil {
		t.Error("expected error for unknown __FILE.bogus on right side of AND")
	}
}
