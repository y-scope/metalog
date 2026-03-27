package query

import (
	"testing"
)

func TestParseSketchExpression_RejectsNOT(t *testing.T) {
	_, err := ParseSketchExpression("NOT uuid = 'abc'")
	if err == nil {
		t.Error("expected error for NOT")
	}
	if err != nil {
		if s := err.Error(); s == "" {
			t.Error("error message should not be empty")
		}
	}
}

func TestParseSketchExpression_RejectsINNonStringLiteral(t *testing.T) {
	_, err := ParseSketchExpression("uuid IN (123, 456)")
	if err == nil {
		t.Error("expected error for non-string IN values")
	}
}

func TestParseSketchExpression_RejectsINSubquery(t *testing.T) {
	_, err := ParseSketchExpression("uuid IN (SELECT x FROM t)")
	if err == nil {
		t.Error("expected error for IN subquery")
	}
}

func TestParseSketchExpression_RejectsNonColumnLHS(t *testing.T) {
	_, err := ParseSketchExpression("1 = 'abc'")
	if err == nil {
		t.Error("expected error for non-column LHS")
	}
}

func TestParseSketchExpression_RejectsLessThan(t *testing.T) {
	_, err := ParseSketchExpression("uuid < 'abc'")
	if err == nil {
		t.Error("expected error for < operator")
	}
}

func TestIsValidSketchName_AdditionalCases(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"s10", true},
		{"s32", true},
		{"s01", true},
		{"s64", true},
		{"s00", false},
		{"s65", false},
		{"s99", false},
		{"ab1", false},
		{"s0a", false},
		{"sa0", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidSketchName(tt.name)
			if got != tt.want {
				t.Errorf("isValidSketchName(%q) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestEvaluateSketchPredicatesFromRow_EmptyInputs(t *testing.T) {
	pass, err := evaluateSketchPredicatesFromRow(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !pass {
		t.Error("should pass with no ext data and no predicates")
	}

	pass, err = evaluateSketchPredicatesFromRow([]byte{}, []SketchPredicate{{SketchKey: "k", Values: []string{"v"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !pass {
		t.Error("should pass with empty ext data")
	}
}

func TestEvaluateSketchPredicatesFromRow_InvalidExtData(t *testing.T) {
	// Invalid data should pass through (can't decode -> don't prune)
	pass, err := evaluateSketchPredicatesFromRow(
		[]byte("not valid msgpack"),
		[]SketchPredicate{{SketchKey: "uuid", Values: []string{"abc"}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !pass {
		t.Error("invalid ext data should pass (don't prune)")
	}
}

// --- buildSketchExtExpr tests ---

func TestBuildSketchExtExpr_NoPredicates(t *testing.T) {
	reg := newTestRegistry(t)
	// Empty predicates → raw ext column name.
	result, err := buildSketchExtExpr(nil, reg)
	if err != nil {
		t.Fatal(err)
	}
	if result != "ext" {
		t.Errorf("expected %q, got %q", "ext", result)
	}
}

func TestBuildSketchExtExpr_UnknownSketchKey(t *testing.T) {
	reg := newTestRegistry(t)
	// Predicate whose sketch key doesn't resolve → treated as no-op → plain ext.
	result, err := buildSketchExtExpr([]SketchPredicate{
		{SketchKey: "no_such_sketch", Values: []string{"v"}},
	}, reg)
	if err != nil {
		t.Fatal(err)
	}
	if result != "ext" {
		t.Errorf("expected %q for unknown sketch key, got %q", "ext", result)
	}
}
