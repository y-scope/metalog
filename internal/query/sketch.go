package query

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.uber.org/zap"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/y-scope/metalog/internal/db"
	"github.com/y-scope/metalog/internal/metastore"
)

const sketchPrefix = "__SKETCH."

// SketchEvaluator evaluates sketch (bloom/cuckoo) filters to prune query splits.
type SketchEvaluator struct {
	db  *sql.DB
	log *zap.Logger
}

// NewSketchEvaluator creates a SketchEvaluator.
func NewSketchEvaluator(db *sql.DB, log *zap.Logger) *SketchEvaluator {
	return &SketchEvaluator{db: db, log: log}
}

// MayContain checks whether the sketch column for a table might contain the given value.
// Sketch columns are SET types with up to 32 members representing hash buckets.
// Returns true if the value's hash bucket is present in the sketch (possible match),
// false if definitely not present.
func (e *SketchEvaluator) MayContain(ctx context.Context, tableName, sketchColumn, value string, recordID int64) (bool, error) {
	if err := db.ValidateSQLIdentifier(tableName); err != nil {
		return false, err
	}
	if err := db.ValidateSQLIdentifier(sketchColumn); err != nil {
		return false, err
	}

	// Compute the bucket index for this value (0-31)
	bucket := hashToBucket(value, 32)
	member := fmt.Sprintf("b%02d", bucket)

	// FIND_IN_SET with mixed parameter ordering is awkward in squirrel, use raw SQL.
	rawQuery := fmt.Sprintf(
		"SELECT 1 FROM %s WHERE %s = ? AND FIND_IN_SET(?, %s) > 0 LIMIT 1",
		db.QuoteIdentifier(tableName),
		metastore.ColID,
		db.QuoteIdentifier(sketchColumn),
	)

	var exists int
	err := e.db.QueryRowContext(ctx, rawQuery, recordID, member).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sketch may contain: %w", err)
	}
	return true, nil
}

// SketchPredicate is a sketch-based filter extracted from a WHERE clause.
// __SKETCH.<sketch_col> = '<value>' predicates are extracted from the filter,
// evaluated client-side via MayContain, and removed from the SQL filter.
type SketchPredicate struct {
	SketchColumn string
	Value        string
}

// ExtractSketchPredicates scans a filter expression for __SKETCH.<col> = '<value>'
// predicates. Returns the predicates and the rewritten filter with sketch predicates removed.
func ExtractSketchPredicates(expr string) ([]SketchPredicate, string, error) {
	if expr == "" {
		return nil, "", nil
	}

	stmt, err := sqlParser.Parse("SELECT 1 FROM t WHERE " + expr)
	if err != nil {
		return nil, expr, nil // don't fail; let the SQL engine handle it
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || sel.Where == nil {
		return nil, expr, nil
	}

	var predicates []SketchPredicate
	remaining := extractSketchFromExpr(sel.Where.Expr, &predicates)

	if len(predicates) == 0 {
		return nil, expr, nil
	}

	if remaining == nil {
		return predicates, "", nil
	}
	return predicates, sqlparser.String(remaining), nil
}

// extractSketchFromExpr recursively removes __SKETCH predicates from an expression tree.
// Returns the remaining expression (nil if everything was extracted).
func extractSketchFromExpr(node sqlparser.Expr, out *[]SketchPredicate) sqlparser.Expr {
	switch n := node.(type) {
	case *sqlparser.AndExpr:
		left := extractSketchFromExpr(n.Left, out)
		right := extractSketchFromExpr(n.Right, out)
		if left == nil && right == nil {
			return nil
		}
		if left == nil {
			return right
		}
		if right == nil {
			return left
		}
		n.Left = left
		n.Right = right
		return n

	case *sqlparser.ComparisonExpr:
		if n.Operator == sqlparser.EqualOp {
			if col, ok := n.Left.(*sqlparser.ColName); ok {
				name := colNameToString(col)
				if strings.HasPrefix(name, sketchPrefix) {
					sketchCol := name[len(sketchPrefix):]
					if lit, ok := n.Right.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
						*out = append(*out, SketchPredicate{
							SketchColumn: sketchCol,
							Value:        lit.Val,
						})
						return nil
					}
				}
			}
		}
		return n

	default:
		return n
	}
}

// EvaluateSketchPredicates checks each sketch predicate against the given record ID.
// Returns true if all predicates pass (may contain), false if any definitely don't match.
func (e *SketchEvaluator) EvaluateSketchPredicates(ctx context.Context, tableName string, recordID int64, predicates []SketchPredicate) (bool, error) {
	for _, p := range predicates {
		match, err := e.MayContain(ctx, tableName, p.SketchColumn, p.Value, recordID)
		if err != nil {
			return false, err
		}
		if !match {
			return false, nil
		}
	}
	return true, nil
}

// hashToBucket computes a simple hash bucket index for a string value.
// Uses FNV-1a-like hashing to distribute across numBuckets.
func hashToBucket(value string, numBuckets int) int {
	h := uint32(2166136261) // FNV offset basis
	for i := 0; i < len(value); i++ {
		h ^= uint32(value[i])
		h *= 16777619 // FNV prime
	}
	return int(h % uint32(numBuckets))
}
