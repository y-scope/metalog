package query

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/schema"
)

// systemColumns are always included in query results.
var systemColumns = []string{
	metastore.ColID,
	metastore.ColMinTimestamp,
	metastore.ColMaxTimestamp,
	metastore.ColState,
}

const (
	prefixFile = "__FILE."
	prefixDim  = "__DIM."
	prefixAgg  = "__AGG."
	prefixAggT = "__AGG_" // __AGG_EQ., __AGG_GTE., etc.
)

// fileColumns are the physical columns exposed via __FILE.<col>.
var fileColumns = map[string]bool{
	metastore.ColID:                       true,
	metastore.ColMinTimestamp:             true,
	metastore.ColMaxTimestamp:             true,
	metastore.ColClpArchiveCreatedAt:      true,
	metastore.ColClpIRStorageBackend:      true,
	metastore.ColClpIRBucket:              true,
	metastore.ColClpIRPath:                true,
	metastore.ColClpArchiveStorageBackend: true,
	metastore.ColClpArchiveBucket:         true,
	metastore.ColClpArchivePath:           true,
	metastore.ColState:                    true,
	metastore.ColRecordCount:              true,
	metastore.ColRawSizeBytes:             true,
	metastore.ColClpIRSizeBytes:           true,
	metastore.ColClpArchiveSizeBytes:      true,
	metastore.ColRetentionDays:            true,
	metastore.ColExpiresAt:                true,
}

// allFileColumns returns all file-level column names in a stable order.
func allFileColumns() []string {
	return []string{
		metastore.ColID,
		metastore.ColMinTimestamp,
		metastore.ColMaxTimestamp,
		metastore.ColClpArchiveCreatedAt,
		metastore.ColClpIRStorageBackend,
		metastore.ColClpIRBucket,
		metastore.ColClpIRPath,
		metastore.ColClpArchiveStorageBackend,
		metastore.ColClpArchiveBucket,
		metastore.ColClpArchivePath,
		metastore.ColState,
		metastore.ColRecordCount,
		metastore.ColRawSizeBytes,
		metastore.ColClpIRSizeBytes,
		metastore.ColClpArchiveSizeBytes,
		metastore.ColRetentionDays,
		metastore.ColExpiresAt,
	}
}

// ResolveProjectionColumns expands prefixed column patterns into physical column names.
// Supported patterns:
//   - __FILE.*           → all file-level columns
//   - __FILE.<col>       → specific file column
//   - __DIM.*            → all active dimension columns
//   - __DIM.<key>        → specific dimension by key
//   - __AGG.*            → all active aggregation columns
//   - __AGG_<TYPE>.*     → all agg columns of given type
//   - __AGG_<TYPE>.<key> → specific agg (no value qualifier)
//   - __AGG_<TYPE>.<key>.<value> → specific agg with value
//
// Returns the list of physical column names to SELECT.
func ResolveProjectionColumns(requested []string, registry *schema.ColumnRegistry) ([]string, error) {
	if len(requested) == 0 {
		return nil, nil // nil means SELECT *
	}

	seen := make(map[string]bool)
	var cols []string
	add := func(c string) {
		if !seen[c] {
			seen[c] = true
			cols = append(cols, c)
		}
	}

	// Always include system columns first.
	for _, c := range systemColumns {
		add(c)
	}

	for _, col := range requested {
		switch {
		case col == "__FILE.*":
			for _, c := range allFileColumns() {
				add(c)
			}

		case strings.HasPrefix(col, prefixFile):
			name := col[len(prefixFile):]
			if !fileColumns[name] {
				return nil, fmt.Errorf("unknown file column: %q", name)
			}
			add(name)

		case col == "__DIM.*":
			if registry == nil {
				continue
			}
			for _, e := range registry.AllDimEntries() {
				add(e.ColumnName)
			}

		case strings.HasPrefix(col, prefixDim):
			key := col[len(prefixDim):]
			if registry == nil {
				return nil, fmt.Errorf("no column registry for dim lookup: %q", key)
			}
			phys := registry.ResolveDim(key)
			if phys == "" {
				return nil, fmt.Errorf("unknown dimension: %q", key)
			}
			add(phys)

		case col == "__AGG.*":
			if registry == nil {
				continue
			}
			for _, e := range registry.AllAggEntries() {
				add(e.ColumnName)
			}

		case strings.HasPrefix(col, prefixAggT):
			resolved, err := resolveAggPattern(col, registry)
			if err != nil {
				return nil, err
			}
			for _, c := range resolved {
				add(c)
			}

		case strings.HasPrefix(col, prefixAgg):
			// __AGG.<something> without a type — treat like __AGG_EQ.
			rest := col[len(prefixAgg):]
			resolved, err := resolveAggPattern("__AGG_EQ."+rest, registry)
			if err != nil {
				return nil, err
			}
			for _, c := range resolved {
				add(c)
			}

		default:
			// Raw column name or physical column.
			if isPhysicalColumn(col) || fileColumns[col] {
				add(col)
			} else {
				return nil, fmt.Errorf("unknown column: %q", col)
			}
		}
	}

	return cols, nil
}

// resolveAggPattern resolves __AGG_<TYPE>.* or __AGG_<TYPE>.<key>[.<value>].
func resolveAggPattern(col string, registry *schema.ColumnRegistry) ([]string, error) {
	// col starts with "__AGG_"
	rest := col[len("__AGG_"):]

	// Split into TYPE and remainder on first dot.
	dotIdx := strings.IndexByte(rest, '.')
	if dotIdx < 0 {
		return nil, fmt.Errorf("invalid agg pattern: %q (expected __AGG_<TYPE>.<key>)", col)
	}
	aggType := rest[:dotIdx]
	remainder := rest[dotIdx+1:]

	if registry == nil {
		return nil, fmt.Errorf("no column registry for agg lookup: %q", col)
	}

	// Wildcard: __AGG_<TYPE>.*
	if remainder == "*" {
		var cols []string
		for _, e := range registry.AllAggEntries() {
			if e.AggregationType == aggType {
				cols = append(cols, e.ColumnName)
			}
		}
		return cols, nil
	}

	// Specific: __AGG_<TYPE>.<key>.<value> — split on rightmost dot.
	var aggKey, aggValue string
	lastDot := strings.LastIndexByte(remainder, '.')
	if lastDot < 0 {
		aggKey = remainder
		aggValue = ""
	} else {
		aggKey = remainder[:lastDot]
		aggValue = remainder[lastDot+1:]
	}

	phys := registry.ResolveAgg(aggKey, aggValue, aggType)
	if phys == "" {
		return nil, fmt.Errorf("unknown aggregation: %q", col)
	}
	return []string{phys}, nil
}

// ResolveColumnRef resolves a single prefixed column reference to a physical column name.
// Used for filter rewriting and ORDER BY resolution.
func ResolveColumnRef(col string, registry *schema.ColumnRegistry) (string, error) {
	switch {
	case strings.HasPrefix(col, prefixFile):
		name := col[len(prefixFile):]
		if !fileColumns[name] {
			return "", fmt.Errorf("unknown file column: %q", name)
		}
		return name, nil

	case strings.HasPrefix(col, prefixDim):
		key := col[len(prefixDim):]
		if registry == nil {
			return "", fmt.Errorf("no column registry for dim lookup: %q", key)
		}
		phys := registry.ResolveDim(key)
		if phys == "" {
			return "", fmt.Errorf("unknown dimension: %q", key)
		}
		return phys, nil

	case strings.HasPrefix(col, prefixAggT):
		resolved, err := resolveAggPattern(col, registry)
		if err != nil {
			return "", err
		}
		if len(resolved) != 1 {
			return "", fmt.Errorf("agg pattern %q must resolve to exactly one column", col)
		}
		return resolved[0], nil

	case strings.HasPrefix(col, prefixAgg):
		rest := col[len(prefixAgg):]
		resolved, err := resolveAggPattern("__AGG_EQ."+rest, registry)
		if err != nil {
			return "", err
		}
		if len(resolved) != 1 {
			return "", fmt.Errorf("agg pattern %q must resolve to exactly one column", col)
		}
		return resolved[0], nil

	default:
		return col, nil
	}
}

// RewriteFilterColumns parses a filter expression and rewrites any __FILE/__DIM/__AGG
// column references to physical column names. Returns the rewritten SQL expression.
func RewriteFilterColumns(expr string, registry *schema.ColumnRegistry) (string, error) {
	if expr == "" {
		return "", nil
	}

	stmt, err := sqlParser.Parse("SELECT 1 FROM t WHERE " + expr)
	if err != nil {
		return "", fmt.Errorf("invalid filter expression: %w", err)
	}

	sel, ok := stmt.(*sqlparser.Select)
	if !ok || sel.Where == nil {
		return "", fmt.Errorf("invalid filter expression")
	}

	rewritten, err := rewriteExpr(sel.Where.Expr, registry)
	if err != nil {
		return "", err
	}

	return sqlparser.String(rewritten), nil
}

// rewriteExpr recursively rewrites column references in an expression.
func rewriteExpr(node sqlparser.Expr, registry *schema.ColumnRegistry) (sqlparser.Expr, error) {
	switch n := node.(type) {
	case *sqlparser.ColName:
		name := colNameToString(n)
		if !hasMagicPrefix(name) {
			return n, nil
		}
		resolved, err := ResolveColumnRef(name, registry)
		if err != nil {
			return nil, err
		}
		return sqlparser.NewColName(resolved), nil

	case *sqlparser.ComparisonExpr:
		left, err := rewriteExpr(n.Left, registry)
		if err != nil {
			return nil, err
		}
		right, err := rewriteExpr(n.Right, registry)
		if err != nil {
			return nil, err
		}
		n.Left = left
		n.Right = right
		return n, nil

	case *sqlparser.AndExpr:
		left, err := rewriteExpr(n.Left, registry)
		if err != nil {
			return nil, err
		}
		right, err := rewriteExpr(n.Right, registry)
		if err != nil {
			return nil, err
		}
		n.Left = left
		n.Right = right
		return n, nil

	case *sqlparser.OrExpr:
		left, err := rewriteExpr(n.Left, registry)
		if err != nil {
			return nil, err
		}
		right, err := rewriteExpr(n.Right, registry)
		if err != nil {
			return nil, err
		}
		n.Left = left
		n.Right = right
		return n, nil

	case *sqlparser.NotExpr:
		inner, err := rewriteExpr(n.Expr, registry)
		if err != nil {
			return nil, err
		}
		n.Expr = inner
		return n, nil

	case *sqlparser.BetweenExpr:
		left, err := rewriteExpr(n.Left, registry)
		if err != nil {
			return nil, err
		}
		from, err := rewriteExpr(n.From, registry)
		if err != nil {
			return nil, err
		}
		to, err := rewriteExpr(n.To, registry)
		if err != nil {
			return nil, err
		}
		n.Left = left
		n.From = from
		n.To = to
		return n, nil

	case *sqlparser.IsExpr:
		left, err := rewriteExpr(n.Left, registry)
		if err != nil {
			return nil, err
		}
		n.Left = left
		return n, nil

	default:
		return n, nil
	}
}

// colNameToString reconstructs the dotted column name from a vitess ColName.
// Vitess parses "__DIM.zone" as qualifier="__DIM", name="zone".
func colNameToString(col *sqlparser.ColName) string {
	q := col.Qualifier.Name.String()
	if q != "" {
		return q + "." + col.Name.String()
	}
	return col.Name.String()
}

// isPhysicalColumn checks if a column name matches dim_fNN or agg_fNN pattern.
func isPhysicalColumn(name string) bool {
	if len(name) < 6 {
		return false
	}
	prefix := name[:4]
	if prefix != "dim_" && prefix != "agg_" {
		return false
	}
	rest := name[4:]
	if len(rest) < 2 || rest[0] != 'f' {
		return false
	}
	for _, c := range rest[1:] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// hasMagicPrefix checks if a column name uses one of the __FILE/__DIM/__AGG prefixes.
func hasMagicPrefix(name string) bool {
	return strings.HasPrefix(name, "__FILE.") ||
		strings.HasPrefix(name, "__DIM.") ||
		strings.HasPrefix(name, "__AGG.") ||
		strings.HasPrefix(name, "__AGG_")
}
