package query

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/schema"
)

// SketchPredicate is a bloom filter check extracted from a sketch expression.
// Multiple predicates are ANDed: all must pass for a file to be kept.
type SketchPredicate struct {
	SketchKey string   // logical key (e.g. "uuid")
	Values    []string // search values (any match → keep file)
}

// ParseSketchExpression parses a SQL WHERE fragment and extracts bloom filter
// predicates. Only equality (=) and IN operators on string literals are
// supported. AND is supported for combining predicates across fields.
//
// Supported:
//
//	field = 'value'
//	field IN ('a', 'b', 'c')
//	field1 = 'x' AND field2 IN ('y', 'z')
//
// Rejected with error:
//
//	!=, NOT IN, <, >, <=, >=, LIKE, OR, NOT
func ParseSketchExpression(expr string, registry *schema.ColumnRegistry) ([]SketchPredicate, error) {
	if expr == "" {
		return nil, nil
	}

	stmt, err := sqlParser.Parse("SELECT 1 FROM t WHERE " + expr)
	if err != nil {
		return nil, fmt.Errorf("parse sketch expression: %w", err)
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || sel.Where == nil {
		return nil, fmt.Errorf("invalid sketch expression")
	}

	var predicates []SketchPredicate
	if err := extractSketchPredicates(sel.Where.Expr, registry, &predicates); err != nil {
		return nil, err
	}
	if len(predicates) == 0 {
		return nil, fmt.Errorf("sketch expression contains no usable predicates")
	}
	return predicates, nil
}

// extractSketchPredicates recursively walks the AST and extracts equality/IN
// predicates. Returns an error for unsupported operators.
func extractSketchPredicates(node sqlparser.Expr, registry *schema.ColumnRegistry, out *[]SketchPredicate) error {
	switch n := node.(type) {
	case *sqlparser.AndExpr:
		if err := extractSketchPredicates(n.Left, registry, out); err != nil {
			return err
		}
		return extractSketchPredicates(n.Right, registry, out)

	case *sqlparser.OrExpr:
		return fmt.Errorf("OR is not supported in sketch expressions (use AND to combine predicates)")

	case *sqlparser.NotExpr:
		return fmt.Errorf("NOT is not supported in sketch expressions")

	case *sqlparser.ComparisonExpr:
		col, ok := n.Left.(*sqlparser.ColName)
		if !ok {
			return fmt.Errorf("unsupported left-hand side in sketch expression: %s", sqlparser.String(n.Left))
		}
		colName := col.Name.String()

		// Resolve logical name to sketch key.
		sketchKey := colName
		if registry != nil {
			if resolved := registry.ResolveDim(colName); resolved != "" {
				// colName is a logical dim key → use it as sketch key.
				sketchKey = colName
			}
		}

		switch n.Operator {
		case sqlparser.EqualOp:
			lit, ok := n.Right.(*sqlparser.Literal)
			if !ok || lit.Type != sqlparser.StrVal {
				return fmt.Errorf("sketch expression: = requires a string literal, got %s", sqlparser.String(n.Right))
			}
			*out = append(*out, SketchPredicate{SketchKey: sketchKey, Values: []string{lit.Val}})
			return nil

		case sqlparser.InOp:
			tuple, ok := n.Right.(sqlparser.ValTuple)
			if !ok {
				return fmt.Errorf("sketch expression: IN requires a value list")
			}
			var values []string
			for _, v := range tuple {
				lit, ok := v.(*sqlparser.Literal)
				if !ok || lit.Type != sqlparser.StrVal {
					return fmt.Errorf("sketch expression: IN values must be string literals, got %s", sqlparser.String(v))
				}
				values = append(values, lit.Val)
			}
			*out = append(*out, SketchPredicate{SketchKey: sketchKey, Values: values})
			return nil

		default:
			return fmt.Errorf("unsupported operator %q in sketch expression (only = and IN are supported)", n.Operator.ToString())
		}

	default:
		return fmt.Errorf("unsupported expression type %T in sketch expression", node)
	}
}

// buildSketchExtExpr resolves sketch keys to SET members via the registry and
// builds a SQL expression that conditionally fetches the ext blob only for rows
// that have at least one of the relevant SET members. This avoids transferring
// MEDIUMBLOB data for rows that can't be pruned.
//
// For a single predicate on "uuid" -> "s03", produces:
//
//	IF(FIND_IN_SET('s03',sketches)>0,ext,NULL) AS `ext`
//
// For multiple predicates (uuid->s03, session_id->s07), produces:
//
//	IF(FIND_IN_SET('s03',sketches)>0 OR FIND_IN_SET('s07',sketches)>0,ext,NULL) AS `ext`
//
// OR is used because any one matching sketch is worth decoding the blob for.
func buildSketchExtExpr(predicates []SketchPredicate, registry *schema.ColumnRegistry) (string, error) {
	var conditions []string
	for _, p := range predicates {
		entry := registry.ResolveSketch(p.SketchKey)
		if entry == nil {
			continue
		}
		if !isValidSketchName(entry.SketchName) {
			return "", fmt.Errorf("invalid sketch name %q", entry.SketchName)
		}
		conditions = append(conditions,
			fmt.Sprintf("FIND_IN_SET('%s',%s)>0", entry.SketchName, metastore.ColSketches))
	}

	if len(conditions) == 0 {
		return metastore.ColExt, nil
	}

	return fmt.Sprintf("IF(%s,%s,NULL) AS `%s`",
		strings.Join(conditions, " OR "),
		metastore.ColExt,
		metastore.ColExt,
	), nil
}

// isValidSketchName checks that a sketch name matches the expected s01..s64 pattern.
func isValidSketchName(name string) bool {
	if len(name) != 3 || name[0] != 's' {
		return false
	}
	if name[1] < '0' || name[1] > '9' || name[2] < '0' || name[2] > '9' {
		return false
	}
	n := int(name[1]-'0')*10 + int(name[2]-'0')
	return n >= 1 && n <= 64
}

// evaluateSketchPredicatesFromRow checks all sketch predicates against a row's
// ext blob data. Returns true if no sketch says "definitely not present".
// For multi-value predicates (IN), at least one value must pass.
// Rows without ext data or without a sketch for a given key pass through —
// the sketch is an acceleration, not a filter requirement.
func evaluateSketchPredicatesFromRow(extData []byte, predicates []SketchPredicate) (bool, error) {
	if len(extData) == 0 || len(predicates) == 0 {
		return true, nil
	}

	ext, err := decodeExtBlob(extData)
	if err != nil {
		return true, nil // can't decode → don't prune
	}

	for _, p := range predicates {
		// For multi-value (IN), any match means the file might contain a match.
		anyMatch := false
		for _, v := range p.Values {
			if evaluateSketchFromExt(ext, p.SketchKey, v) {
				anyMatch = true
				break
			}
		}
		if !anyMatch {
			return false, nil
		}
	}
	return true, nil
}
