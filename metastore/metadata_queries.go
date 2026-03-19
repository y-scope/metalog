package metastore

import (
	"context"
	"database/sql"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"
)

// MetadataReader provides read-only queries against the column registry tables.
// It is transport-agnostic — gRPC/YARPC handlers map these domain types to proto.
type MetadataReader struct {
	db  *sql.DB
	log *zap.Logger
}

// NewMetadataReader creates a MetadataReader.
func NewMetadataReader(db *sql.DB, log *zap.Logger) *MetadataReader {
	return &MetadataReader{db: db, log: log}
}

// DimensionInfo holds dimension metadata for a single column.
type DimensionInfo struct {
	Name        string
	Type        string
	Width       int32
	AliasColumn string
}

// AggInfo holds aggregation metadata for a single column.
type AggInfo struct {
	Name            string
	Value           string
	AggregationType string // raw DB value, e.g. "EQ", "GTE"
	ValueType       string // "INT" or "FLOAT"
	AliasColumn     string
}

// SketchInfo holds sketch metadata for a single column.
type SketchInfo struct {
	Name string
}

// ListTables returns all registered table names, ordered alphabetically.
func (q *MetadataReader) ListTables(ctx context.Context) ([]string, error) {
	query, args, err := sq.Select("table_name").
		From(TableRegistry).
		OrderBy("table_name").
		ToSql()
	if err != nil {
		return nil, err
	}
	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// ListDimensions returns dimension metadata for ACTIVE columns in a table.
func (q *MetadataReader) ListDimensions(ctx context.Context, tableName string) ([]DimensionInfo, error) {
	query, args, err := sq.Select("column_name", "dim_key", "base_type", "COALESCE(width, 0)", "COALESCE(alias_column, '')").
		From(DimRegistryTable).
		Where(sq.Eq{"table_name": tableName, "state": "ACTIVE"}).
		OrderBy("column_name").
		ToSql()
	if err != nil {
		return nil, err
	}
	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dims []DimensionInfo
	for rows.Next() {
		var colName string
		var d DimensionInfo
		if err := rows.Scan(&colName, &d.Name, &d.Type, &d.Width, &d.AliasColumn); err != nil {
			return nil, err
		}
		dims = append(dims, d)
	}
	return dims, rows.Err()
}

// ListAggs returns aggregation metadata for ACTIVE columns in a table.
func (q *MetadataReader) ListAggs(ctx context.Context, tableName string) ([]AggInfo, error) {
	query, args, err := sq.Select("column_name", "agg_key", "COALESCE(agg_value, '')", "aggregation_type", "value_type", "COALESCE(alias_column, '')").
		From(AggRegistryTable).
		Where(sq.Eq{"table_name": tableName, "state": "ACTIVE"}).
		OrderBy("column_name").
		ToSql()
	if err != nil {
		return nil, err
	}
	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var aggs []AggInfo
	for rows.Next() {
		var colName string
		var a AggInfo
		if err := rows.Scan(&colName, &a.Name, &a.Value, &a.AggregationType, &a.ValueType, &a.AliasColumn); err != nil {
			return nil, err
		}
		aggs = append(aggs, a)
	}
	return aggs, rows.Err()
}

// ListSketches returns sketch metadata for ACTIVE columns in a table.
func (q *MetadataReader) ListSketches(ctx context.Context, tableName string) ([]SketchInfo, error) {
	query, args, err := sq.Select("sketch_name").
		From(SketchRegistryTable).
		Where(sq.Eq{"table_name": tableName, "state": "ACTIVE"}).
		OrderBy("sketch_name").
		ToSql()
	if err != nil {
		return nil, err
	}
	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sketches []SketchInfo
	for rows.Next() {
		var s SketchInfo
		if err := rows.Scan(&s.Name); err != nil {
			return nil, err
		}
		sketches = append(sketches, s)
	}
	return sketches, rows.Err()
}
