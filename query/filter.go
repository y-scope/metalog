package query

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

// sqlParser is a package-level parser instance used for filter expression validation
// and column rewriting. Using sqlparser.New instead of NewTestParser for production use.
var sqlParser *sqlparser.Parser

func init() {
	var err error
	sqlParser, err = sqlparser.New(sqlparser.Options{})
	if err != nil {
		panic(fmt.Sprintf("failed to create SQL parser: %v", err))
	}
}

// ValidateFilterExpression parses a WHERE clause expression into an AST and
// validates that it contains only safe node types (comparisons, boolean logic,
// column refs, and literals). Rejects subqueries, function calls, and other
// potentially dangerous constructs.
func ValidateFilterExpression(expr string) error {
	if expr == "" {
		return nil
	}

	// Parse as a SELECT WHERE clause so vitess can produce an AST.
	stmt, err := sqlParser.Parse("SELECT 1 FROM t WHERE " + expr)
	if err != nil {
		return fmt.Errorf("invalid filter expression: %w", err)
	}

	sel, ok := stmt.(*sqlparser.Select)
	if !ok || sel.Where == nil {
		return fmt.Errorf("invalid filter expression")
	}

	return walkExpr(sel.Where.Expr)
}

// walkExpr recursively validates that an expression node is safe.
func walkExpr(node sqlparser.Expr) error {
	switch n := node.(type) {
	case *sqlparser.AndExpr:
		if err := walkExpr(n.Left); err != nil {
			return err
		}
		return walkExpr(n.Right)

	case *sqlparser.OrExpr:
		if err := walkExpr(n.Left); err != nil {
			return err
		}
		return walkExpr(n.Right)

	case *sqlparser.NotExpr:
		return walkExpr(n.Expr)

	case *sqlparser.ComparisonExpr:
		// Allow: =, !=, <, >, <=, >=, <=>, LIKE, NOT LIKE, IN, NOT IN, REGEXP
		if err := walkExpr(n.Left); err != nil {
			return err
		}
		if err := walkExpr(n.Right); err != nil {
			return err
		}
		if n.Escape != nil {
			if err := walkExpr(n.Escape); err != nil {
				return err
			}
		}
		return nil

	case *sqlparser.BetweenExpr:
		// BETWEEN ... AND ...
		if err := walkExpr(n.Left); err != nil {
			return err
		}
		if err := walkExpr(n.From); err != nil {
			return err
		}
		return walkExpr(n.To)

	case *sqlparser.IsExpr:
		// IS NULL, IS NOT NULL, IS TRUE, IS FALSE
		return walkExpr(n.Left)

	case *sqlparser.ColName:
		// Column references are always safe.
		return nil

	case *sqlparser.Literal:
		// String, int, float, hex literals are safe.
		return nil

	case sqlparser.BoolVal:
		return nil

	case sqlparser.ValTuple:
		// Tuple of values, e.g. IN (1, 2, 3)
		for _, v := range n {
			if err := walkExpr(v); err != nil {
				return err
			}
		}
		return nil

	case *sqlparser.NullVal:
		return nil

	case *sqlparser.UnaryExpr:
		// Unary minus for negative numbers.
		return walkExpr(n.Expr)

	default:
		return fmt.Errorf("unsupported expression type %T in filter", node)
	}
}
