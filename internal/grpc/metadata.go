package grpc

import (
	"context"
	"database/sql"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	rows, err := h.db.QueryContext(ctx, "SELECT table_name FROM "+metastore.TableRegistry+" ORDER BY table_name")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, status.Errorf(codes.Internal, "scan table: %v", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "list tables: %v", err)
	}

	return &metapb.ListTablesResponse{Tables: tables}, nil
}

// ListDimensions returns dimension metadata for a table.
func (h *MetadataHandler) ListDimensions(ctx context.Context, req *metapb.ListDimensionsRequest) (*metapb.ListDimensionsResponse, error) {
	tableName := req.GetTable()
	if tableName == "" {
		return nil, status.Error(codes.InvalidArgument, "table is required")
	}

	rows, err := h.db.QueryContext(ctx,
		"SELECT column_name, dim_key FROM "+metastore.DimRegistryTable+" WHERE table_name = ? AND state = 'ACTIVE' ORDER BY column_name",
		tableName,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list dimensions: %v", err)
	}
	defer rows.Close()

	var dims []*metapb.DimensionInfo
	for rows.Next() {
		var colName, dimKey string
		if err := rows.Scan(&colName, &dimKey); err != nil {
			return nil, status.Errorf(codes.Internal, "scan dimension: %v", err)
		}
		dims = append(dims, &metapb.DimensionInfo{
			Name:        dimKey,
			AliasColumn: colName,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "list dimensions: %v", err)
	}

	return &metapb.ListDimensionsResponse{Dimensions: dims}, nil
}

// ListAggs returns aggregation metadata for a table.
func (h *MetadataHandler) ListAggs(ctx context.Context, req *metapb.ListAggsRequest) (*metapb.ListAggsResponse, error) {
	tableName := req.GetTable()
	if tableName == "" {
		return nil, status.Error(codes.InvalidArgument, "table is required")
	}

	rows, err := h.db.QueryContext(ctx,
		"SELECT column_name, agg_key, value_type FROM "+metastore.AggRegistryTable+" WHERE table_name = ? AND state = 'ACTIVE' ORDER BY column_name",
		tableName,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list aggs: %v", err)
	}
	defer rows.Close()

	var aggs []*metapb.AggInfo
	for rows.Next() {
		var colName, aggKey, valueType string
		if err := rows.Scan(&colName, &aggKey, &valueType); err != nil {
			return nil, status.Errorf(codes.Internal, "scan agg: %v", err)
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
		return nil, status.Errorf(codes.Internal, "list aggs: %v", err)
	}

	return &metapb.ListAggsResponse{Aggs: aggs}, nil
}

// ListSketches returns sketch metadata for a table.
func (h *MetadataHandler) ListSketches(ctx context.Context, req *metapb.ListSketchesRequest) (*metapb.ListSketchesResponse, error) {
	tableName := req.GetTable()
	if tableName == "" {
		return nil, status.Error(codes.InvalidArgument, "table is required")
	}

	rows, err := h.db.QueryContext(ctx,
		"SELECT sketch_name FROM "+metastore.SketchRegistryTable+" WHERE table_name = ? AND state = 'ACTIVE' ORDER BY sketch_name",
		tableName,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list sketches: %v", err)
	}
	defer rows.Close()

	var sketches []*metapb.SketchInfo
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, status.Errorf(codes.Internal, "scan sketch: %v", err)
		}
		sketches = append(sketches, &metapb.SketchInfo{Name: name})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "list sketches: %v", err)
	}

	return &metapb.ListSketchesResponse{Sketches: sketches}, nil
}
