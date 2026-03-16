package query

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/schema"
)

// SketchPredicate is a sketch-based filter extracted from a WHERE clause.
// The caller specifies which fields to accelerate; the engine finds equality
// predicates on those fields and evaluates them against bloom filter data
// in the ext column.
type SketchPredicate struct {
	SketchKey string // logical key (e.g. "uuid")
	Value     string // search value
}

// CollectSketchValues scans a filter expression for equality predicates on
// the specified field names and returns the values. The filter expression is
// NOT modified — predicates remain in the SQL for correctness. The bloom filter
// provides additional pruning on top of the DB filter.
func CollectSketchValues(expr string, fields []string, registry *schema.ColumnRegistry) []SketchPredicate {
	if expr == "" || len(fields) == 0 {
		return nil
	}

	// Build a set of physical column names to look for.
	// The filter expression has already been rewritten to physical names,
	// so we need to resolve dim keys to their physical equivalents.
	targetCols := make(map[string]string) // physical col → sketch key
	for _, field := range fields {
		physCol := field
		if registry != nil {
			if resolved := registry.ResolveDim(field); resolved != "" {
				physCol = resolved
			}
		}
		targetCols[physCol] = field
	}

	stmt, err := sqlParser.Parse("SELECT 1 FROM t WHERE " + expr)
	if err != nil {
		return nil
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || sel.Where == nil {
		return nil
	}

	var predicates []SketchPredicate
	collectValuesFromExpr(sel.Where.Expr, targetCols, &predicates)
	return predicates
}

// collectValuesFromExpr recursively finds equality predicates on target columns
// and collects their values. Does not modify the expression tree.
func collectValuesFromExpr(node sqlparser.Expr, targets map[string]string, out *[]SketchPredicate) {
	switch n := node.(type) {
	case *sqlparser.AndExpr:
		collectValuesFromExpr(n.Left, targets, out)
		collectValuesFromExpr(n.Right, targets, out)

	case *sqlparser.ComparisonExpr:
		if n.Operator == sqlparser.EqualOp {
			if col, ok := n.Left.(*sqlparser.ColName); ok {
				colName := col.Name.String()
				if sketchKey, found := targets[colName]; found {
					if lit, ok := n.Right.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
						*out = append(*out, SketchPredicate{
							SketchKey: sketchKey,
							Value:     lit.Val,
						})
					}
				}
			}
		}
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
			// Unknown sketch key — no SET member exists, skip the condition.
			// The predicate still evaluates server-side (ext will be NULL → pass through).
			continue
		}
		// Validate sketch name before interpolating into SQL.
		if !isValidSketchName(entry.SketchName) {
			return "", fmt.Errorf("invalid sketch name %q", entry.SketchName)
		}
		conditions = append(conditions,
			fmt.Sprintf("FIND_IN_SET('%s',%s)>0", entry.SketchName, metastore.ColSketches))
	}

	if len(conditions) == 0 {
		// No resolvable sketch keys — just select raw ext.
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
		if !evaluateSketchFromExt(ext, p.SketchKey, p.Value) {
			return false, nil
		}
	}
	return true, nil
}
