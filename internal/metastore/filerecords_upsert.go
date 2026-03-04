package metastore

import (
	"strings"

	dbutil "github.com/y-scope/metalog/internal/db"
)

// BuildGuardedUpsertSQL generates a multi-row INSERT ... ON DUPLICATE KEY UPDATE
// with guarded column assignments.
//
// The guard condition prevents overwriting rows in protected states and only
// applies updates when VALUES(max_timestamp) > max_timestamp. This ensures
// idempotent, monotonic ingestion:
//
//	guard = state NOT IN ('IR_ARCHIVE_CONSOLIDATION_PENDING','ARCHIVE_CLOSED','ARCHIVE_PURGING')
//	        AND VALUES(max_timestamp) > max_timestamp
//
// Each guarded column uses: col = IF(guard, VALUES(col), col)
// max_timestamp MUST be the last assignment because the guard references it.
//
// Returns the SQL string, an empty args slice (caller fills in), and the
// number of parameters per row.
func BuildGuardedUpsertSQL(
	tableName string,
	dimCols []string,
	aggCols []string,
	floatAggCols map[string]bool,
	rowCount int,
) (string, []any, int) {
	allCols := make([]string, 0, len(BaseCols)+len(dimCols)+len(aggCols))
	allCols = append(allCols, BaseCols...)
	allCols = append(allCols, dimCols...)
	allCols = append(allCols, aggCols...)

	paramsPerRow := len(allCols)
	var b strings.Builder

	// INSERT INTO `table` (col1, col2, ...)
	b.WriteString("INSERT INTO ")
	b.WriteString(dbutil.QuoteIdentifier(tableName))
	b.WriteString(" (")
	for i, col := range allCols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(col)
	}
	b.WriteString(") VALUES ")

	// VALUES (?,?,?...), (?,?,?...), ...
	rowPlaceholder := "(" + strings.Repeat("?,", paramsPerRow-1) + "?)"
	for i := 0; i < rowCount; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(rowPlaceholder)
	}

	// ON DUPLICATE KEY UPDATE with guard
	b.WriteString(" ON DUPLICATE KEY UPDATE ")

	// Guard condition
	guard := "state NOT IN ('IR_ARCHIVE_CONSOLIDATION_PENDING','ARCHIVE_CLOSED','ARCHIVE_PURGING') AND VALUES(max_timestamp) > max_timestamp"

	// Guarded columns: everything except max_timestamp (which goes last)
	first := true
	for _, col := range GuardedUpdateCols {
		if col == ColMaxTimestamp {
			continue // goes last
		}
		if !first {
			b.WriteString(", ")
		}
		writeGuardedAssignment(&b, col, guard)
		first = false
	}

	// Dim columns
	for _, col := range dimCols {
		b.WriteString(", ")
		writeGuardedAssignment(&b, col, guard)
	}

	// Agg columns
	for _, col := range aggCols {
		b.WriteString(", ")
		writeGuardedAssignment(&b, col, guard)
	}

	// max_timestamp LAST (guard references it)
	b.WriteString(", ")
	writeGuardedAssignment(&b, ColMaxTimestamp, guard)

	return b.String(), nil, paramsPerRow
}

// writeGuardedAssignment writes: col = IF(guard, VALUES(col), col)
func writeGuardedAssignment(b *strings.Builder, col string, guard string) {
	b.WriteString(col)
	b.WriteString(" = IF(")
	b.WriteString(guard)
	b.WriteString(", VALUES(")
	b.WriteString(col)
	b.WriteString("), ")
	b.WriteString(col)
	b.WriteString(")")
}
