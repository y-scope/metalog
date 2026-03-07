package query

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/db"
	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/schema"
)

// SplitQueryEngine executes paginated queries on metadata tables.
type SplitQueryEngine struct {
	db    *sql.DB
	cache *Cache
	log   *zap.Logger
}

// NewSplitQueryEngine creates a SplitQueryEngine.
func NewSplitQueryEngine(db *sql.DB, log *zap.Logger) *SplitQueryEngine {
	return &SplitQueryEngine{
		db:    db,
		cache: NewCache(5 * time.Minute),
		log:   log,
	}
}

// QueryParams holds the parameters for a split query.
type QueryParams struct {
	TableName      string
	Columns        []string
	StateFilter    []string
	FilterExpr     string
	OrderBy        []OrderBySpec
	Limit          int
	CursorValues   []any
	CursorID       int64
	HasCursor      bool
	AllowUnindexed bool
	Registry       *schema.ColumnRegistry
}

// OrderBySpec defines a sort column and direction.
type OrderBySpec struct {
	Column string
	Desc   bool
}

// SplitRow holds a single query result row.
type SplitRow struct {
	ID     int64
	Values map[string]any
}

// Query executes a paginated query and returns rows.
func (e *SplitQueryEngine) Query(ctx context.Context, params *QueryParams) ([]*SplitRow, error) {
	if err := db.ValidateSQLIdentifier(params.TableName); err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	// Resolve projection columns (__FILE.*, __DIM.*, __AGG_*.*)
	cols, err := ResolveProjectionColumns(params.Columns, params.Registry)
	if err != nil {
		return nil, fmt.Errorf("resolve projection: %w", err)
	}
	if len(cols) == 0 {
		cols = []string{"*"}
	}

	// Resolve ORDER BY columns and validate they are safe identifiers
	for i, ob := range params.OrderBy {
		resolved, err := ResolveColumnRef(ob.Column, params.Registry)
		if err != nil {
			return nil, fmt.Errorf("resolve order by: %w", err)
		}
		if err := db.ValidateSQLIdentifier(resolved); err != nil {
			return nil, fmt.Errorf("order by: %w", err)
		}
		params.OrderBy[i].Column = resolved
	}

	// Validate filter expression (defense-in-depth — gRPC handler also validates)
	if params.FilterExpr != "" {
		if err := ValidateFilterExpression(params.FilterExpr); err != nil {
			return nil, fmt.Errorf("filter validation: %w", err)
		}
	}

	// Rewrite filter expression columns (cached to avoid repeated parsing)
	if params.FilterExpr != "" {
		cacheKey := "filter:" + params.TableName + ":" + params.FilterExpr
		cached, cacheErr := e.cache.GetOrCompute(cacheKey, func() (any, error) {
			return RewriteFilterColumns(params.FilterExpr, params.Registry)
		})
		if cacheErr != nil {
			return nil, fmt.Errorf("rewrite filter: %w", cacheErr)
		}
		params.FilterExpr = cached.(string)
	}

	builder := sq.Select(cols...).From(db.QuoteIdentifier(params.TableName))

	// User filter expression (validated + rewritten upstream)
	if params.FilterExpr != "" {
		builder = builder.Where(params.FilterExpr)
	}

	// State filter
	if len(params.StateFilter) > 0 {
		builder = builder.Where(sq.Eq{metastore.ColState: params.StateFilter})
	}

	// Keyset cursor
	if params.HasCursor && len(params.OrderBy) > 0 {
		cursorWhere := buildKeysetWhere(params.OrderBy, params.CursorValues, params.CursorID)
		builder = builder.Where(cursorWhere)
	}

	// Order by
	orderClauses := make([]string, 0, len(params.OrderBy)+1)
	for _, ob := range params.OrderBy {
		dir := "ASC"
		if ob.Desc {
			dir = "DESC"
		}
		orderClauses = append(orderClauses, ob.Column+" "+dir)
	}
	orderClauses = append(orderClauses, metastore.ColID+" ASC") // implicit tiebreaker
	builder = builder.OrderBy(orderClauses...)

	if params.Limit > 0 {
		builder = builder.Limit(uint64(params.Limit))
	}

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("query: columns: %w", err)
	}

	var results []*SplitRow
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("query: scan: %w", err)
		}

		row := &SplitRow{Values: make(map[string]any, len(columns))}
		for i, col := range columns {
			row.Values[col] = values[i]
			if col == metastore.ColID {
				if id, ok := values[i].(int64); ok {
					row.ID = id
				}
			}
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query: rows: %w", err)
	}
	return results, nil
}

func buildKeysetWhere(orderBy []OrderBySpec, cursorValues []any, cursorID int64) sq.Sqlizer {
	// For keyset pagination: (col1, col2, ..., id) > (val1, val2, ..., cursorID)
	// This creates a row comparison that MySQL can optimize.
	colNames := make([]string, 0, len(orderBy)+1)
	for _, ob := range orderBy {
		colNames = append(colNames, ob.Column)
	}
	colNames = append(colNames, metastore.ColID)

	vals := make([]any, 0, len(cursorValues)+1)
	vals = append(vals, cursorValues...)
	vals = append(vals, cursorID)

	lhs := "(" + strings.Join(colNames, ",") + ")"
	placeholders := "(" + strings.Repeat("?,", len(vals)-1) + "?)"

	return sq.Expr(lhs+" > "+placeholders, vals...)
}
