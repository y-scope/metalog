package query

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/db"
	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/schema"
)

// indexedSortColumns lists columns with dedicated indexes that support efficient
// keyset pagination without a full table scan.
var indexedSortColumns = map[string]bool{
	"min_timestamp": true,
	"max_timestamp": true,
}

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
	TableName        string
	Columns          []string
	FilterExpr       string
	OrderBy          []OrderBySpec
	Limit            int
	CursorValues     []any
	CursorID         int64
	HasCursor        bool
	AllowUnindexed   bool
	Registry         *schema.ColumnRegistry
	SketchAcceleration []string // field names to accelerate via bloom filter sketches
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

// SplitWithCursor pairs a result row with the cursor position needed to
// resume pagination from this row.
type SplitWithCursor struct {
	Row          *SplitRow
	CursorValues []any
	CursorID     int64
}

// StreamingResult reports final statistics from a streaming query.
type StreamingResult struct {
	SplitsScanned int64
	SplitsMatched int64
}

// SplitConsumer is called for each split during streaming.
// Return false to stop iteration early.
type SplitConsumer func(split *SplitWithCursor) (keepGoing bool, err error)

// preparedQuery holds validated and resolved query components that are
// reusable across multiple page fetches within a single streaming RPC.
type preparedQuery struct {
	tableName        string
	cols             []string
	orderBy          []OrderBySpec
	filterExpr       string
	orderClauses     []string
	sketchPredicates []SketchPredicate // sketch predicates for bloom filter evaluation
	sketchExtExpr    string            // e.g. "IF(FIND_IN_SET('s03',sketches)>0,ext,NULL)" or "" if no sketches
}

// Query executes a single-page query and returns rows. This is the original
// interface retained for backward compatibility with existing callers and tests.
func (e *SplitQueryEngine) Query(ctx context.Context, params *QueryParams) ([]*SplitRow, error) {
	prepared, err := e.prepareQuery(params)
	if err != nil {
		return nil, err
	}

	var cursorValues []any
	var cursorID int64
	if params.HasCursor {
		cursorValues = params.CursorValues
		cursorID = params.CursorID
	}

	swcs, err := e.executePage(ctx, prepared, params.Limit, cursorValues, cursorID)
	if err != nil {
		return nil, err
	}

	var rows []*SplitRow
	for _, swc := range swcs {
		if !passesSketchFilter(swc.Row, prepared.sketchPredicates) {
			continue
		}
		rows = append(rows, swc.Row)
	}
	return rows, nil
}

// StreamSplitsAsync fetches all matching splits across multiple internal pages,
// calling consumer for each result. A background goroutine prefetches the next
// page while the consumer processes the current one.
//
// totalLimit caps the total number of results (0 = unlimited). pageSize controls
// the SQL LIMIT per internal page fetch. The method respects ctx cancellation.
func (e *SplitQueryEngine) StreamSplitsAsync(
	ctx context.Context,
	params *QueryParams,
	totalLimit int,
	pageSize int,
	consumer SplitConsumer,
) (*StreamingResult, error) {
	prepared, err := e.prepareQuery(params)
	if err != nil {
		return nil, err
	}

	// Initial cursor from the request (nil = start from beginning).
	var cursorValues []any
	var cursorID int64
	if params.HasCursor {
		cursorValues = params.CursorValues
		cursorID = params.CursorID
	}

	// Prefetch channel: capacity 2*pageSize allows producer to stay one page
	// ahead of the consumer without blocking.
	ch := make(chan *SplitWithCursor, pageSize*2)
	producerCtx, cancelProducer := context.WithCancel(ctx)
	defer cancelProducer()

	producerErr := make(chan error, 1)

	// splitsScanned is tracked by the producer (DB rows fetched). When sketch
	// filtering is added, scanned will exceed matched for pruned rows.
	var splitsScanned atomic.Int64

	// Background producer: fetches pages and pushes to channel.
	go func() {
		defer close(ch)
		cv := cursorValues
		cid := cursorID
		totalSent := 0

		for {
			effectivePageSize := pageSize
			if totalLimit > 0 {
				remaining := totalLimit - totalSent
				if remaining <= 0 {
					return
				}
				// When sketch predicates are active, keep the full page size
				// so the DB returns enough rows to compensate for client-side
				// bloom filter pruning. Without this, the shrinking LIMIT would
				// cause under-returning results.
				if remaining < effectivePageSize && len(prepared.sketchPredicates) == 0 {
					effectivePageSize = remaining
				}
			}

			page, err := e.executePage(producerCtx, prepared, effectivePageSize, cv, cid)
			if err != nil {
				if producerCtx.Err() != nil {
					return // cancelled, not an error
				}
				producerErr <- err
				return
			}

			splitsScanned.Add(int64(len(page)))

			for _, swc := range page {
				if !passesSketchFilter(swc.Row, prepared.sketchPredicates) {
					continue
				}

				select {
				case ch <- swc:
					totalSent++
				case <-producerCtx.Done():
					return
				}
			}

			// Exhausted: DB returned fewer rows than requested.
			if len(page) < effectivePageSize {
				return
			}

			// Advance cursor to last row of this page.
			last := page[len(page)-1]
			cv = last.CursorValues
			cid = last.CursorID

			// Check limit.
			if totalLimit > 0 && totalSent >= totalLimit {
				return
			}
		}
	}()

	// Foreground consumer: drain channel and call consumer callback.
	// splitsMatched counts rows delivered to the consumer.
	var splitsMatched int64
	for swc := range ch {
		splitsMatched++

		keepGoing, err := consumer(swc)
		if err != nil {
			cancelProducer()
			// Drain remaining items so the producer goroutine can exit.
			for range ch {
			}
			return nil, err
		}
		if !keepGoing {
			cancelProducer()
			for range ch {
			}
			break
		}
	}

	// Check for producer error — but only if the consumer didn't initiate
	// the stop. When the consumer cancels (keepGoing=false or send error),
	// a concurrent DB error in the producer is expected collateral and
	// should not override the successful partial result.
	if ctx.Err() == nil {
		select {
		case err := <-producerErr:
			return nil, err
		default:
		}
	}

	return &StreamingResult{
		SplitsScanned: splitsScanned.Load(),
		SplitsMatched: splitsMatched,
	}, nil
}

// prepareQuery validates and resolves all query components once. The result
// is reusable across multiple executePage calls within a streaming RPC.
func (e *SplitQueryEngine) prepareQuery(params *QueryParams) (*preparedQuery, error) {
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
	resolvedOrderBy := make([]OrderBySpec, len(params.OrderBy))
	for i, ob := range params.OrderBy {
		resolved, err := ResolveColumnRef(ob.Column, params.Registry)
		if err != nil {
			return nil, fmt.Errorf("resolve order by: %w", err)
		}
		if err := db.ValidateSQLIdentifier(resolved); err != nil {
			return nil, fmt.Errorf("order by: %w", err)
		}
		if !params.AllowUnindexed && !indexedSortColumns[resolved] {
			return nil, fmt.Errorf("sort column %q is not indexed; use an indexed column (%s) or set allow_unindexed_sort=true",
				ob.Column, "min_timestamp, max_timestamp")
		}
		resolvedOrderBy[i] = OrderBySpec{Column: resolved, Desc: ob.Desc}
	}

	// Validate filter expression (defense-in-depth — gRPC handler also validates)
	filterExpr := params.FilterExpr
	if filterExpr != "" {
		if err := ValidateFilterExpression(filterExpr); err != nil {
			return nil, fmt.Errorf("filter validation: %w", err)
		}
	}

	// Rewrite filter expression columns (cached to avoid repeated parsing).
	// The cache key includes the registry entry count so that a schema change
	// (new dim/agg columns) invalidates stale rewrites.
	if filterExpr != "" {
		regVersion := 0
		if params.Registry != nil {
			regVersion = params.Registry.EntryCount()
		}
		cacheKey := fmt.Sprintf("filter:%s:%d:%s", params.TableName, regVersion, filterExpr)
		cached, cacheErr := e.cache.GetOrCompute(cacheKey, func() (any, error) {
			return RewriteFilterColumns(filterExpr, params.Registry)
		})
		if cacheErr != nil {
			return nil, fmt.Errorf("rewrite filter: %w", cacheErr)
		}
		rewritten, ok := cached.(string)
		if !ok {
			return nil, fmt.Errorf("rewrite filter: unexpected cache type %T", cached)
		}
		filterExpr = rewritten
	}

	// Sketch acceleration: if the caller specified fields to accelerate,
	// find equality predicates on those fields in the rewritten filter and
	// extract their values for bloom filter evaluation. The predicates are
	// NOT removed from the SQL — the DB still filters on them for correctness.
	// The bloom filter provides additional pruning for rows that passed the
	// DB filter but can be rejected before the caller opens the file.
	var sketchPredicates []SketchPredicate
	var sketchExtExpr string
	if len(params.SketchAcceleration) > 0 && filterExpr != "" {
		sketchPredicates = CollectSketchValues(filterExpr, params.SketchAcceleration, params.Registry)
	}
	if len(sketchPredicates) > 0 && params.Registry != nil {
		var err error
		sketchExtExpr, err = buildSketchExtExpr(sketchPredicates, params.Registry)
		if err != nil {
			return nil, fmt.Errorf("resolve sketch predicates: %w", err)
		}
	}

	// When sketch predicates are present, replace any existing ext column with
	// a conditional expression that only fetches the blob for rows that have
	// the relevant SET members, emitting NULL otherwise to save transfer cost.
	if sketchExtExpr != "" && len(cols) > 0 && cols[0] != "*" {
		replaced := false
		for i, c := range cols {
			if c == metastore.ColExt {
				cols[i] = sketchExtExpr
				replaced = true
				break
			}
		}
		if !replaced {
			cols = append(cols, sketchExtExpr)
		}
	}

	// Ensure sort columns and the id tiebreaker are in the projection.
	// Without these, cursor extraction would produce nil values.
	if len(cols) > 0 && cols[0] != "*" {
		colSet := make(map[string]bool, len(cols))
		for _, c := range cols {
			colSet[c] = true
		}
		if !colSet[metastore.ColID] {
			cols = append(cols, metastore.ColID)
		}
		for _, ob := range resolvedOrderBy {
			if !colSet[ob.Column] {
				cols = append(cols, ob.Column)
				colSet[ob.Column] = true
			}
		}
	}

	// Pre-build ORDER BY clauses (reused per page).
	orderClauses := make([]string, 0, len(resolvedOrderBy)+1)
	for _, ob := range resolvedOrderBy {
		dir := "ASC"
		if ob.Desc {
			dir = "DESC"
		}
		orderClauses = append(orderClauses, db.QuoteIdentifier(ob.Column)+" "+dir)
	}
	orderClauses = append(orderClauses, db.QuoteIdentifier(metastore.ColID)+" ASC")

	return &preparedQuery{
		tableName:        params.TableName,
		cols:             cols,
		orderBy:          resolvedOrderBy,
		filterExpr:       filterExpr,
		orderClauses:     orderClauses,
		sketchPredicates: sketchPredicates,
		sketchExtExpr:    sketchExtExpr,
	}, nil
}

// executePage fetches a single page of results from the database.
// cursorValues/cursorID are nil/0 for the first page.
func (e *SplitQueryEngine) executePage(
	ctx context.Context,
	pq *preparedQuery,
	limit int,
	cursorValues []any,
	cursorID int64,
) ([]*SplitWithCursor, error) {
	builder := sq.Select(pq.cols...).From(db.QuoteIdentifier(pq.tableName))

	if pq.filterExpr != "" {
		builder = builder.Where(pq.filterExpr)
	}

	// Keyset cursor
	if cursorValues != nil && len(pq.orderBy) > 0 {
		if len(cursorValues) != len(pq.orderBy) {
			return nil, fmt.Errorf("keyset cursor: got %d values but %d order-by columns",
				len(cursorValues), len(pq.orderBy))
		}
		builder = builder.Where(buildKeysetWhere(pq.orderBy, cursorValues, cursorID))
	}

	builder = builder.OrderBy(pq.orderClauses...)

	if limit > 0 {
		builder = builder.Limit(uint64(limit))
	}

	sqlStr, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := e.db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("query: columns: %w", err)
	}

	var results []*SplitWithCursor
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

		// Extract cursor values from sort columns. Reject NULL values —
		// SQL comparisons with NULL (col > NULL) evaluate to UNKNOWN, which
		// would silently truncate pagination by returning zero subsequent rows.
		cv := make([]any, len(pq.orderBy))
		for i, ob := range pq.orderBy {
			v := row.Values[ob.Column]
			if v == nil {
				return nil, fmt.Errorf("keyset cursor: sort column %q has NULL value in row id=%d; "+
					"NULL sort columns are not supported for keyset pagination", ob.Column, row.ID)
			}
			cv[i] = v
		}

		results = append(results, &SplitWithCursor{
			Row:          row,
			CursorValues: cv,
			CursorID:     row.ID,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query: rows: %w", err)
	}
	return results, nil
}

// passesSketchFilter evaluates bloom filter predicates against a row's ext blob.
// Returns true if the row should be kept (no predicates, or all pass).
func passesSketchFilter(row *SplitRow, predicates []SketchPredicate) bool {
	if len(predicates) == 0 {
		return true
	}
	var extBytes []byte
	if b, ok := row.Values[metastore.ColExt].([]byte); ok {
		extBytes = b
	}
	pass, _ := evaluateSketchPredicatesFromRow(extBytes, predicates)
	return pass
}

// buildKeysetWhere builds a keyset pagination WHERE clause using the
// OR-of-prefix-equalities algorithm. For N sort fields plus the implicit id
// tiebreaker (always ASC), it produces:
//
//	(f1 {op1} v1)
//	OR (f1 = v1 AND f2 {op2} v2)
//	OR (f1 = v1 AND f2 = v2 AND f3 {op3} v3)
//	...
//	OR (f1 = v1 AND ... AND fN = vN AND id > cursorID)
//
// Each {opI} is < for DESC fields and > for ASC fields. This correctly handles
// mixed sort directions, unlike SQL row comparison which is always lexicographic.
func buildKeysetWhere(orderBy []OrderBySpec, cursorValues []any, cursorID int64) sq.Sqlizer {
	n := len(orderBy)

	// Collect all columns and values including the id tiebreaker.
	cols := make([]string, n+1)
	vals := make([]any, n+1)
	descs := make([]bool, n+1)
	for i, ob := range orderBy {
		cols[i] = db.QuoteIdentifier(ob.Column)
		vals[i] = cursorValues[i]
		descs[i] = ob.Desc
	}
	cols[n] = db.QuoteIdentifier(metastore.ColID)
	vals[n] = cursorID
	descs[n] = false // id is always ASC

	// Build OR branches: one for each position 0..n.
	var branches []string
	var allArgs []any
	for i := 0; i <= n; i++ {
		var parts []string
		// Equality prefix: f0=v0 AND f1=v1 AND ... AND f_{i-1}=v_{i-1}
		for j := 0; j < i; j++ {
			parts = append(parts, cols[j]+" = ?")
			allArgs = append(allArgs, vals[j])
		}
		// Strict comparison for field i
		op := ">"
		if descs[i] {
			op = "<"
		}
		parts = append(parts, cols[i]+" "+op+" ?")
		allArgs = append(allArgs, vals[i])

		branches = append(branches, "("+strings.Join(parts, " AND ")+")")
	}

	return sq.Expr("("+strings.Join(branches, " OR ")+")", allArgs...)
}
