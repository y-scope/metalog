package query

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/schema"
)

// newTestRegistry creates a ColumnRegistry with pre-populated dim and agg entries
// using sqlmock to satisfy the constructor's DB queries.
//
// Dim entries:
//
//	"region"       -> dim_f01 (str, width 64)
//	"service.name" -> dim_f02 (str_utf8, width 128)
//	"k8s.pod.name" -> dim_f03 (str, width 255)
//
// Agg entries:
//
//	EQ:  "status_code"           (no value) -> agg_f01 (INT)
//	GTE: "response_time"."p99"              -> agg_f02 (FLOAT)
//	SUM: "bytes_sent"            (no value) -> agg_f03 (INT)
//	AVG: "latency"               (no value) -> agg_f04 (FLOAT)
func newTestRegistry(t *testing.T) *schema.ColumnRegistry {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	// Mock dim registry query
	dimRows := sqlmock.NewRows([]string{"column_name", "base_type", "width", "dim_key", "alias_column"}).
		AddRow("dim_f01", "str", 64, "region", nil).
		AddRow("dim_f02", "str_utf8", 128, "service.name", nil).
		AddRow("dim_f03", "str", 255, "k8s.pod.name", nil)
	mock.ExpectQuery("SELECT").WillReturnRows(dimRows)

	// Mock agg registry query
	aggRows := sqlmock.NewRows([]string{"column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column"}).
		AddRow("agg_f01", "status_code", nil, "EQ", "INT", nil).
		AddRow("agg_f02", "response_time", "p99", "GTE", "FLOAT", nil).
		AddRow("agg_f03", "bytes_sent", nil, "SUM", "INT", nil).
		AddRow("agg_f04", "latency", nil, "AVG", "FLOAT", nil)
	mock.ExpectQuery("SELECT").WillReturnRows(aggRows)

	registry, err := schema.NewColumnRegistry(context.Background(), db, "test_table", false, zap.NewNop())
	require.NoError(t, err)
	return registry
}
