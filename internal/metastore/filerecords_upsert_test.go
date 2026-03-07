package metastore

import (
	"strings"
	"testing"
)

func TestBuildGuardedUpsertSQL(t *testing.T) {
	dimCols := []string{"dim_f01", "dim_f02"}
	aggCols := []string{"agg_f01"}
	floatAggCols := map[string]bool{}

	sql, _, paramsPerRow := BuildGuardedUpsertSQL("test_table", dimCols, aggCols, floatAggCols, 2)

	// Check INSERT INTO
	if !strings.HasPrefix(sql, "INSERT INTO `test_table` (") {
		t.Errorf("expected INSERT INTO prefix, got: %s", sql[:50])
	}

	// Check all base columns + dim + agg are in the column list
	for _, col := range BaseCols {
		if !strings.Contains(sql, col) {
			t.Errorf("missing column %q in SQL", col)
		}
	}
	if !strings.Contains(sql, "dim_f01") {
		t.Error("missing dim_f01")
	}
	if !strings.Contains(sql, "agg_f01") {
		t.Error("missing agg_f01")
	}

	// Check 2 value rows
	if strings.Count(sql, "(?") != 2 {
		t.Errorf("expected 2 value rows, got %d", strings.Count(sql, "(?,"))
	}

	// Check ON DUPLICATE KEY UPDATE
	if !strings.Contains(sql, "ON DUPLICATE KEY UPDATE") {
		t.Error("missing ON DUPLICATE KEY UPDATE")
	}

	// Check guard condition
	if !strings.Contains(sql, "state NOT IN ('IR_PURGING','IR_ARCHIVE_CONSOLIDATION_PENDING','ARCHIVE_CLOSED','ARCHIVE_PURGING')") {
		t.Error("missing guard condition")
	}

	// Check max_timestamp is last
	lastGuard := strings.LastIndex(sql, "max_timestamp = IF(")
	otherGuard := strings.Index(sql, "state = IF(")
	if lastGuard < otherGuard {
		t.Error("max_timestamp should be the last guarded assignment")
	}

	// Check paramsPerRow = baseCols + dimCols + aggCols
	expectedParams := len(BaseCols) + len(dimCols) + len(aggCols)
	if paramsPerRow != expectedParams {
		t.Errorf("paramsPerRow = %d, want %d", paramsPerRow, expectedParams)
	}
}

func TestBuildGuardedUpsertSQLNoDynamic(t *testing.T) {
	sql, _, paramsPerRow := BuildGuardedUpsertSQL("my_table", nil, nil, nil, 1)

	if !strings.Contains(sql, "INSERT INTO `my_table`") {
		t.Error("wrong table name")
	}
	if paramsPerRow != len(BaseCols) {
		t.Errorf("paramsPerRow = %d, want %d", paramsPerRow, len(BaseCols))
	}
	// Should have exactly 1 value row
	valuesIdx := strings.Index(sql, "VALUES ")
	onDupIdx := strings.Index(sql, " ON DUPLICATE")
	valuesSection := sql[valuesIdx:onDupIdx]
	if strings.Count(valuesSection, "(") != 1 {
		t.Errorf("expected 1 value row in: %s", valuesSection)
	}
}
