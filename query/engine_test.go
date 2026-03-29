package query

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewSplitQueryEngine(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck

	engine := NewSplitQueryEngine(db, zap.NewNop())
	assert.NotNil(t, engine)
	assert.NotNil(t, engine.cache)
}

// --- buildKeysetWhere tests ---

func TestBuildKeysetWhere_SingleASC(t *testing.T) {
	w := buildKeysetWhere(
		[]OrderBySpec{{Column: "max_timestamp", Desc: false}},
		[]any{int64(1000)},
		42,
	)
	sql, args, err := w.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "((`max_timestamp` > ?) OR (`max_timestamp` IS NULL) OR (`max_timestamp` = ? AND `id` > ?))", sql)
	assert.Equal(t, []any{int64(1000), int64(1000), int64(42)}, args)
}

func TestBuildKeysetWhere_SingleDESC(t *testing.T) {
	w := buildKeysetWhere(
		[]OrderBySpec{{Column: "max_timestamp", Desc: true}},
		[]any{int64(5000)},
		99,
	)
	sql, args, err := w.ToSql()
	assert.NoError(t, err)
	// max_timestamp is DESC (<), id tiebreaker matches primary sort direction (<)
	assert.Equal(t, "((`max_timestamp` < ?) OR (`max_timestamp` = ? AND `id` < ?))", sql)
	assert.Equal(t, []any{int64(5000), int64(5000), int64(99)}, args)
}

func TestBuildKeysetWhere_MixedDirections(t *testing.T) {
	w := buildKeysetWhere(
		[]OrderBySpec{
			{Column: "max_timestamp", Desc: true},
			{Column: "min_timestamp", Desc: false},
		},
		[]any{int64(5000), int64(1000)},
		7,
	)
	sql, args, err := w.ToSql()
	assert.NoError(t, err)
	// max_timestamp DESC (<), min_timestamp ASC (>), id matches primary sort DESC (<)
	assert.Equal(t,
		"((`max_timestamp` < ?) OR (`max_timestamp` = ? AND `min_timestamp` > ?) OR (`max_timestamp` = ? AND `min_timestamp` IS NULL) OR (`max_timestamp` = ? AND `min_timestamp` = ? AND `id` < ?))",
		sql,
	)
	assert.Equal(t, []any{int64(5000), int64(5000), int64(1000), int64(5000), int64(5000), int64(1000), int64(7)}, args)
}

func TestBuildKeysetWhere_TwoColumnsAllDESC(t *testing.T) {
	w := buildKeysetWhere(
		[]OrderBySpec{
			{Column: "max_timestamp", Desc: true},
			{Column: "min_timestamp", Desc: true},
		},
		[]any{int64(5000), int64(3000)},
		10,
	)
	sql, args, err := w.ToSql()
	assert.NoError(t, err)
	// Both sort cols DESC (<), id matches primary sort DESC (<)
	assert.Equal(t,
		"((`max_timestamp` < ?) OR (`max_timestamp` = ? AND `min_timestamp` < ?) OR (`max_timestamp` = ? AND `min_timestamp` = ? AND `id` < ?))",
		sql,
	)
	assert.Equal(t, []any{int64(5000), int64(5000), int64(3000), int64(5000), int64(3000), int64(10)}, args)
}

// --- StreamSplitsAsync tests (using sqlmock) ---

// newTestEngine creates a SplitQueryEngine with a sqlmock DB for testing.
func newTestEngine(t *testing.T) (*SplitQueryEngine, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	engine := &SplitQueryEngine{
		db:    db,
		cache: NewCache(5 * time.Minute),
		log:   zap.NewNop(),
	}
	return engine, mock, db
}

// makeRows creates sqlmock.Rows with (id, min_timestamp) columns.
func makeRows(startID, count int, baseTS int64) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"id", "min_timestamp"})
	for i := 0; i < count; i++ {
		rows.AddRow(int64(startID+i), baseTS+int64(i)*1000)
	}
	return rows
}

// collectConsumer returns a SplitConsumer that collects all splits into a slice.
func collectConsumer(results *[]*SplitWithCursor) SplitConsumer {
	return func(swc *SplitWithCursor) (bool, error) {
		*results = append(*results, swc)
		return true, nil
	}
}

func TestStreamSplitsAsync_SinglePage(t *testing.T) {
	engine, mock, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	// Return 3 rows (less than page size 5 → signals last page).
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(1, 3, 1000))

	var results []*SplitWithCursor
	params := &QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	}

	sr, err := engine.StreamSplitsAsync(context.Background(), params, 0, 5, collectConsumer(&results))
	require.NoError(t, err)
	assert.Len(t, results, 3)
	assert.Equal(t, int64(3), sr.SplitsScanned)
	assert.Equal(t, int64(3), sr.SplitsMatched)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStreamSplitsAsync_MultiPage(t *testing.T) {
	engine, mock, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	// Page 1: 3 full rows (= page size → fetch next page)
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(1, 3, 1000))
	// Page 2: 2 rows (< page size → last page)
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(4, 2, 4000))

	var results []*SplitWithCursor
	params := &QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	}

	sr, err := engine.StreamSplitsAsync(context.Background(), params, 0, 3, collectConsumer(&results))
	require.NoError(t, err)
	assert.Len(t, results, 5)
	assert.Equal(t, int64(5), sr.SplitsScanned)
	assert.Equal(t, int64(5), sr.SplitsMatched)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStreamSplitsAsync_TotalLimitCapsResults(t *testing.T) {
	engine, mock, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	// totalLimit=4, pageSize=3: first page returns 3, second page should
	// request only 1 more (remaining = 4-3 = 1).
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(1, 3, 1000))
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(4, 1, 4000))

	var results []*SplitWithCursor
	params := &QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	}

	sr, err := engine.StreamSplitsAsync(context.Background(), params, 4, 3, collectConsumer(&results))
	require.NoError(t, err)
	assert.Len(t, results, 4)
	assert.Equal(t, int64(4), sr.SplitsMatched)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStreamSplitsAsync_TotalLimitSmallerThanPage(t *testing.T) {
	engine, mock, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	// totalLimit=2, pageSize=5: effective page size should be 2.
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(1, 2, 1000))

	var results []*SplitWithCursor
	params := &QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	}

	sr, err := engine.StreamSplitsAsync(context.Background(), params, 2, 5, collectConsumer(&results))
	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, int64(2), sr.SplitsMatched)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStreamSplitsAsync_EmptyResult(t *testing.T) {
	engine, mock, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id", "min_timestamp"}))

	var results []*SplitWithCursor
	params := &QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	}

	sr, err := engine.StreamSplitsAsync(context.Background(), params, 0, 5, collectConsumer(&results))
	require.NoError(t, err)
	assert.Empty(t, results)
	assert.Equal(t, int64(0), sr.SplitsScanned)
	assert.Equal(t, int64(0), sr.SplitsMatched)
}

func TestStreamSplitsAsync_ConsumerStops(t *testing.T) {
	engine, mock, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	// Return fewer rows than pageSize so the producer doesn't attempt a
	// second page fetch that races with cancellation.
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(1, 4, 1000))

	// Consumer stops after 2 results.
	count := 0
	consumer := func(swc *SplitWithCursor) (bool, error) {
		count++
		return count < 2, nil
	}

	params := &QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	}

	sr, err := engine.StreamSplitsAsync(context.Background(), params, 0, 5, consumer)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.Equal(t, int64(2), sr.SplitsMatched)
	// Producer scanned all 4 rows even though consumer stopped after 2.
	assert.Equal(t, int64(4), sr.SplitsScanned)
}

func TestStreamSplitsAsync_ConsumerError(t *testing.T) {
	engine, mock, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(1, 5, 1000))

	consumer := func(swc *SplitWithCursor) (bool, error) {
		return false, fmt.Errorf("consumer failed")
	}

	params := &QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	}

	sr, err := engine.StreamSplitsAsync(context.Background(), params, 0, 5, consumer)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "consumer failed")
	assert.Nil(t, sr)
}

func TestStreamSplitsAsync_ContextCancellation(t *testing.T) {
	engine, mock, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	ctx, cancel := context.WithCancel(context.Background())

	// Return a full page to trigger a second fetch attempt.
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(1, 3, 1000))
	// Cancel context before the second page query.
	mock.ExpectQuery("SELECT").WillReturnError(context.Canceled)

	var results []*SplitWithCursor
	params := &QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	}

	// Cancel after consuming first page.
	consumer := func(swc *SplitWithCursor) (bool, error) {
		results = append(results, swc)
		if len(results) == 3 {
			cancel()
		}
		return true, nil
	}

	sr, err := engine.StreamSplitsAsync(ctx, params, 0, 3, consumer)
	// After cancellation, we may get a partial result (not an error) because
	// the consumer didn't initiate the stop — the context did.
	if err != nil {
		// Context cancellation is acceptable as an error here.
		assert.ErrorIs(t, err, context.Canceled)
	} else {
		assert.NotNil(t, sr)
		assert.GreaterOrEqual(t, len(results), 3)
	}
}

func TestStreamSplitsAsync_ProducerDBError(t *testing.T) {
	engine, mock, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT").WillReturnError(fmt.Errorf("connection refused"))

	var results []*SplitWithCursor
	params := &QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	}

	sr, err := engine.StreamSplitsAsync(context.Background(), params, 0, 5, collectConsumer(&results))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
	assert.Nil(t, sr)
	assert.Empty(t, results)
}

func TestStreamSplitsAsync_CursorAdvancement(t *testing.T) {
	engine, mock, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	// Page 1: returns rows 1-3.
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(1, 3, 1000))
	// Page 2: returns rows 4-5 (cursor from last row of page 1 is used).
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(4, 2, 4000))

	var results []*SplitWithCursor
	params := &QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	}

	sr, err := engine.StreamSplitsAsync(context.Background(), params, 0, 3, collectConsumer(&results))
	require.NoError(t, err)
	assert.Len(t, results, 5)

	// Verify cursor values are populated for each result.
	for i, r := range results {
		assert.NotNil(t, r.CursorValues, "result %d should have cursor values", i)
		assert.Equal(t, r.Row.ID, r.CursorID, "result %d CursorID should match Row.ID", i)
	}

	// Verify IDs are sequential (no overlap).
	for i, r := range results {
		assert.Equal(t, int64(i+1), r.Row.ID, "result %d ID", i)
	}
	assert.Equal(t, int64(5), sr.SplitsScanned)
}

// --- prepareQuery tests ---

func TestPrepareQuery_InvalidTableName(t *testing.T) {
	engine, _, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	_, err := engine.prepareQuery(&QueryParams{
		TableName: "DROP TABLE--",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query:")
}

func TestPrepareQuery_UnindexedSortRejected(t *testing.T) {
	engine, _, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	_, err := engine.prepareQuery(&QueryParams{
		TableName: "test_table",
		OrderBy:   []OrderBySpec{{Column: "record_count", Desc: false}},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not indexed")
}

func TestPrepareQuery_UnindexedSortAllowedWithFlag(t *testing.T) {
	engine, _, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	pq, err := engine.prepareQuery(&QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, pq)
}

func TestPrepareQuery_SortColumnsInjectedIntoProjection(t *testing.T) {
	engine, _, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	pq, err := engine.prepareQuery(&QueryParams{
		TableName:      "test_table",
		Columns:        []string{"state"},
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	})
	require.NoError(t, err)

	// Projection should include: state, id (tiebreaker), min_timestamp (sort col).
	colSet := make(map[string]bool)
	for _, c := range pq.cols {
		colSet[c] = true
	}
	assert.True(t, colSet["state"], "projection should include requested 'state'")
	assert.True(t, colSet["id"], "projection should include 'id' tiebreaker")
	assert.True(t, colSet["min_timestamp"], "projection should include sort column 'min_timestamp'")
}

func TestPrepareQuery_WildcardSkipsSortInjection(t *testing.T) {
	engine, _, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	pq, err := engine.prepareQuery(&QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	})
	require.NoError(t, err)

	// No explicit columns → wildcard.
	assert.Equal(t, []string{"*"}, pq.cols)
}

// --- executePage NULL sort column test ---

func TestExecutePage_NullSortColumnAccepted(t *testing.T) {
	engine, mock, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	// Return a row where min_timestamp is NULL (dim columns are nullable).
	rows := sqlmock.NewRows([]string{"id", "min_timestamp"}).
		AddRow(int64(1), nil)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	pq := &preparedQuery{
		tableName:    "test_table",
		cols:         []string{"*"},
		orderBy:      []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		orderClauses: []string{"`min_timestamp` ASC", "`id` ASC"},
	}

	results, err := engine.executePage(context.Background(), pq, 10, nil, 0)
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Nil(t, results[0].CursorValues[0], "NULL cursor value should be preserved")
}

func TestBuildKeysetWhere_NullCursorASC(t *testing.T) {
	// ASC with NULL cursor: NULL is last, only id tiebreaker produces "after" rows.
	w := buildKeysetWhere(
		[]OrderBySpec{{Column: "dim_f01", Desc: false}},
		[]any{nil},
		42,
	)
	sql, args, err := w.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "((`dim_f01` IS NULL AND `id` > ?))", sql)
	assert.Equal(t, []any{int64(42)}, args)
}

func TestBuildKeysetWhere_NullCursorDESC(t *testing.T) {
	// DESC with NULL cursor: NULL is first, so "after" means IS NOT NULL.
	w := buildKeysetWhere(
		[]OrderBySpec{{Column: "dim_f01", Desc: true}},
		[]any{nil},
		42,
	)
	sql, args, err := w.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "((`dim_f01` IS NOT NULL) OR (`dim_f01` IS NULL AND `id` < ?))", sql)
	assert.Equal(t, []any{int64(42)}, args)
}

func TestBuildKeysetWhere_AllNullASC(t *testing.T) {
	// ASC with all NULL cursors — only id tiebreaker branch.
	w := buildKeysetWhere(
		[]OrderBySpec{{Column: "dim_f01", Desc: false}},
		[]any{nil},
		42,
	)
	sql, _, err := w.ToSql()
	assert.NoError(t, err)
	assert.Contains(t, sql, "`id` > ?")
}

func TestPrepareQuery_FilterValidationError(t *testing.T) {
	engine, _, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	_, err := engine.prepareQuery(&QueryParams{
		TableName:  "test_table",
		FilterExpr: "SLEEP(5)",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "filter validation")
}

func TestPrepareQuery_InvalidSketchExpression(t *testing.T) {
	engine, _, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	_, err := engine.prepareQuery(&QueryParams{
		TableName:      "test_table",
		SketchExpr:     "NOT valid = 'abc'",
		AllowUnindexed: true,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sketch_expression")
}

func TestPrepareQuery_OrderByWithFilterAndColumns(t *testing.T) {
	engine, _, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	pq, err := engine.prepareQuery(&QueryParams{
		TableName:      "test_table",
		Columns:        []string{"state", "record_count"},
		FilterExpr:     "state = 'IR_CLOSED'",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: true}},
		AllowUnindexed: true,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, pq.filterExpr)
	assert.Len(t, pq.orderBy, 1)
	assert.True(t, pq.orderBy[0].Desc)
}

func TestPrepareQuery_InvalidOrderByColumn(t *testing.T) {
	engine, _, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	_, err := engine.prepareQuery(&QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "DROP TABLE;--", Desc: false}},
		AllowUnindexed: true,
	})
	assert.Error(t, err)
}

func TestPassesSketchFilter_NoPredicates(t *testing.T) {
	row := &SplitRow{Values: map[string]any{}}
	assert.True(t, passesSketchFilter(row, nil))
}

func TestPassesSketchFilter_WithPredicates(t *testing.T) {
	// With predicates but no ext data, the bloom filter check should
	// return a result (likely true since no bloom to check against).
	row := &SplitRow{Values: map[string]any{}}
	predicates := []SketchPredicate{{SketchKey: "uuid", Values: []string{"abc"}}}
	// passesSketchFilter returns true when ext is nil (no bloom to reject)
	result := passesSketchFilter(row, predicates)
	// The result depends on evaluateSketchPredicatesFromRow behavior with nil ext.
	// We just want to exercise the code path.
	_ = result
}

func TestPassesSketchFilter_WithExtData(t *testing.T) {
	row := &SplitRow{Values: map[string]any{"ext": []byte{0x01, 0x02, 0x03}}}
	predicates := []SketchPredicate{{SketchKey: "uuid", Values: []string{"abc"}}}
	// Exercise the code path where ext bytes exist.
	_ = passesSketchFilter(row, predicates)
}

func TestExecutePage_CursorValueCountMismatch(t *testing.T) {
	engine, _, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	pq := &preparedQuery{
		tableName:    "test_table",
		cols:         []string{"*"},
		orderBy:      []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		orderClauses: []string{"`min_timestamp` ASC", "`id` ASC"},
	}

	// Provide 2 cursor values but only 1 order-by column.
	_, err := engine.executePage(context.Background(), pq, 10, []any{int64(1), int64(2)}, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "keyset cursor")
}

func TestQuery_WithFilter(t *testing.T) {
	engine, mock, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(1, 2, 1000))

	params := &QueryParams{
		TableName:      "test_table",
		FilterExpr:     "state = 'IR_CLOSED'",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		AllowUnindexed: true,
	}

	rows, err := engine.Query(context.Background(), params)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

func TestQuery_WithCursor(t *testing.T) {
	engine, mock, db := newTestEngine(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(5, 2, 5000))

	params := &QueryParams{
		TableName:      "test_table",
		OrderBy:        []OrderBySpec{{Column: "min_timestamp", Desc: false}},
		HasCursor:      true,
		CursorValues:   []any{int64(4000)},
		CursorID:       4,
		AllowUnindexed: true,
	}

	rows, err := engine.Query(context.Background(), params)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}
