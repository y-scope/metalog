package metastore

import (
	"strings"

	dbutil "github.com/y-scope/metalog/internal/db"
)

// rowAlias is the alias used for the inserted row in MySQL 8.0+ syntax.
const rowAlias = "new"

// BuildGuardedUpsertSQL generates a multi-row INSERT ... ON DUPLICATE KEY UPDATE
// with guarded column assignments.
//
// The guard condition prevents overwriting rows in protected states and only
// applies updates when the new max_timestamp > existing max_timestamp. This
// ensures idempotent, monotonic ingestion.
//
// Each guarded column uses: col = IF(guard, <new_value>, col)
// max_timestamp MUST be the last assignment because the guard references it.
//
// When useValuesFunc is true, uses VALUES(col) syntax (fully supported in MariaDB).
// Otherwise uses the MySQL 8.0.20+ alias form: INSERT ... AS new ... new.col.
//
// Returns the SQL string, an empty args slice (caller fills in), and the
// number of parameters per row.
func BuildGuardedUpsertSQL(
	tableName string,
	dimCols []string,
	aggCols []string,
	floatAggCols map[string]bool,
	rowCount int,
	useValuesFunc bool,
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

	// MySQL 8.0.20+ alias form: ... VALUES (...) AS new
	if !useValuesFunc {
		b.WriteString(" AS ")
		b.WriteString(rowAlias)
	}

	// ON DUPLICATE KEY UPDATE with guard
	b.WriteString(" ON DUPLICATE KEY UPDATE ")

	// Guard condition — generated from UpsertGuardStates to stay in sync.
	guardStates := make([]string, len(UpsertGuardStates))
	for i, s := range UpsertGuardStates {
		guardStates[i] = "'" + string(s) + "'"
	}

	// newRef returns the reference to the new row's column value.
	newRef := func(col string) string {
		if useValuesFunc {
			return "VALUES(" + col + ")"
		}
		return rowAlias + "." + col
	}

	guard := "state NOT IN (" + strings.Join(guardStates, ",") + ") AND " + newRef(ColMaxTimestamp) + " > " + ColMaxTimestamp

	// Guarded columns: everything except max_timestamp (which goes last)
	first := true
	for _, col := range GuardedUpdateCols {
		if col == ColMaxTimestamp {
			continue // goes last
		}
		if !first {
			b.WriteString(", ")
		}
		writeGuardedAssignment(&b, col, guard, newRef)
		first = false
	}

	// Dim columns
	for _, col := range dimCols {
		b.WriteString(", ")
		writeGuardedAssignment(&b, col, guard, newRef)
	}

	// Agg columns
	for _, col := range aggCols {
		b.WriteString(", ")
		writeGuardedAssignment(&b, col, guard, newRef)
	}

	// max_timestamp LAST (guard references it)
	b.WriteString(", ")
	writeGuardedAssignment(&b, ColMaxTimestamp, guard, newRef)

	return b.String(), nil, paramsPerRow
}

// writeGuardedAssignment writes: col = IF(guard, <newRef(col)>, col)
func writeGuardedAssignment(b *strings.Builder, col string, guard string, newRef func(string) string) {
	b.WriteString(col)
	b.WriteString(" = IF(")
	b.WriteString(guard)
	b.WriteString(", ")
	b.WriteString(newRef(col))
	b.WriteString(", ")
	b.WriteString(col)
	b.WriteString(")")
}
