package grpc

import (
	"context"
	"database/sql"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	metapb "github.com/y-scope/metalog/gen/proto/metadatapb"
	"github.com/y-scope/metalog/internal/metastore"
)

// MetadataHandler implements the MetadataService gRPC interface.
type MetadataHandler struct {
	metapb.UnimplementedMetadataServiceServer
	db  *sql.DB
	log *zap.Logger
}

// NewMetadataHandler creates a MetadataHandler.
func NewMetadataHandler(db *sql.DB, log *zap.Logger) *MetadataHandler {
	return &MetadataHandler{db: db, log: log}
}

// ListTables returns all registered table names.
func (h *MetadataHandler) ListTables(ctx context.Context, _ *metapb.ListTablesRequest) (*metapb.ListTablesResponse, error) {
	query, args, err := sq.Select("table_name").
		From(metastore.TableRegistry).
		OrderBy("table_name").
		ToSql()
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list tables: build query: %v", err)
	}
	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, grpcstatus.Errorf(codes.Internal, "scan table: %v", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list tables: %v", err)
	}

	return &metapb.ListTablesResponse{Tables: tables}, nil
}

// ListDimensions returns dimension metadata for a table.
func (h *MetadataHandler) ListDimensions(ctx context.Context, req *metapb.ListDimensionsRequest) (*metapb.ListDimensionsResponse, error) {
	tableName := req.GetTable()
	if tableName == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "table is required")
	}

	query, args, err := sq.Select("column_name", "dim_key").
		From(metastore.DimRegistryTable).
		Where(sq.Eq{"table_name": tableName, "state": "ACTIVE"}).
		OrderBy("column_name").
		ToSql()
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list dimensions: build query: %v", err)
	}
	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list dimensions: %v", err)
	}
	defer rows.Close()

	var dims []*metapb.DimensionInfo
	for rows.Next() {
		var colName, dimKey string
		if err := rows.Scan(&colName, &dimKey); err != nil {
			return nil, grpcstatus.Errorf(codes.Internal, "scan dimension: %v", err)
		}
		dims = append(dims, &metapb.DimensionInfo{
			Name:        dimKey,
			AliasColumn: colName,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list dimensions: %v", err)
	}

	return &metapb.ListDimensionsResponse{Dimensions: dims}, nil
}

// ListAggs returns aggregation metadata for a table.
func (h *MetadataHandler) ListAggs(ctx context.Context, req *metapb.ListAggsRequest) (*metapb.ListAggsResponse, error) {
	tableName := req.GetTable()
	if tableName == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "table is required")
	}

	query, args, err := sq.Select("column_name", "agg_key", "value_type").
		From(metastore.AggRegistryTable).
		Where(sq.Eq{"table_name": tableName, "state": "ACTIVE"}).
		OrderBy("column_name").
		ToSql()
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list aggs: build query: %v", err)
	}
	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list aggs: %v", err)
	}
	defer rows.Close()

	var aggs []*metapb.AggInfo
	for rows.Next() {
		var colName, aggKey, valueType string
		if err := rows.Scan(&colName, &aggKey, &valueType); err != nil {
			return nil, grpcstatus.Errorf(codes.Internal, "scan agg: %v", err)
		}
		aggInfo := &metapb.AggInfo{
			Name:        aggKey,
			AliasColumn: colName,
		}
		if valueType == "FLOAT" || valueType == "float" {
			aggInfo.ValueType = metapb.AggValueType_AGG_VALUE_TYPE_FLOAT
		} else {
			aggInfo.ValueType = metapb.AggValueType_AGG_VALUE_TYPE_INT
		}
		aggs = append(aggs, aggInfo)
	}
	if err := rows.Err(); err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list aggs: %v", err)
	}

	return &metapb.ListAggsResponse{Aggs: aggs}, nil
}

// ListSketches returns sketch metadata for a table.
func (h *MetadataHandler) ListSketches(ctx context.Context, req *metapb.ListSketchesRequest) (*metapb.ListSketchesResponse, error) {
	tableName := req.GetTable()
	if tableName == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "table is required")
	}

	query, args, err := sq.Select("sketch_name").
		From(metastore.SketchRegistryTable).
		Where(sq.Eq{"table_name": tableName, "state": "ACTIVE"}).
		OrderBy("sketch_name").
		ToSql()
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list sketches: build query: %v", err)
	}
	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list sketches: %v", err)
	}
	defer rows.Close()

	var sketches []*metapb.SketchInfo
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, grpcstatus.Errorf(codes.Internal, "scan sketch: %v", err)
		}
		sketches = append(sketches, &metapb.SketchInfo{Name: name})
	}
	if err := rows.Err(); err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list sketches: %v", err)
	}

	return &metapb.ListSketchesResponse{Sketches: sketches}, nil
}
