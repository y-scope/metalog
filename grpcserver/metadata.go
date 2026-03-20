package grpcserver

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	metapb "github.com/y-scope/metalog/gen/proto/metadatapb"
	splitspb "github.com/y-scope/metalog/gen/proto/splitspb"
	"github.com/y-scope/metalog/metastore"
)

// MetadataHandler implements the MetadataService gRPC interface.
type MetadataHandler struct {
	metapb.UnimplementedMetadataServiceServer
	querier *metastore.MetadataReader
	log     *zap.Logger
}

// NewMetadataHandler creates a MetadataHandler.
func NewMetadataHandler(querier *metastore.MetadataReader, log *zap.Logger) *MetadataHandler {
	return &MetadataHandler{querier: querier, log: log}
}

// ListTables returns all registered table names.
func (h *MetadataHandler) ListTables(ctx context.Context, _ *metapb.ListTablesRequest) (*metapb.ListTablesResponse, error) {
	tables, err := h.querier.ListTables(ctx)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list tables: %v", err)
	}
	return &metapb.ListTablesResponse{Tables: tables}, nil
}

// ListDimensions returns dimension metadata for a table.
func (h *MetadataHandler) ListDimensions(ctx context.Context, req *metapb.ListDimensionsRequest) (*metapb.ListDimensionsResponse, error) {
	if req.GetTable() == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "table is required")
	}

	dims, err := h.querier.ListDimensions(ctx, req.GetTable())
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list dimensions: %v", err)
	}

	pbDims := make([]*metapb.DimensionInfo, len(dims))
	for i, d := range dims {
		pbDims[i] = &metapb.DimensionInfo{
			Name:        d.Name,
			Type:        d.Type,
			Width:       d.Width,
			AliasColumn: d.AliasColumn,
		}
	}
	return &metapb.ListDimensionsResponse{Dimensions: pbDims}, nil
}

// ListAggs returns aggregation metadata for a table.
func (h *MetadataHandler) ListAggs(ctx context.Context, req *metapb.ListAggsRequest) (*metapb.ListAggsResponse, error) {
	if req.GetTable() == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "table is required")
	}

	aggs, err := h.querier.ListAggs(ctx, req.GetTable())
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list aggs: %v", err)
	}

	pbAggs := make([]*metapb.AggInfo, len(aggs))
	for i, a := range aggs {
		aggTypeName := "AGGREGATION_TYPE_" + a.AggregationType
		aggTypeVal, ok := splitspb.AggregationType_value[aggTypeName]
		if !ok {
			h.log.Warn("unknown aggregation_type, defaulting to UNSPECIFIED",
				zap.String("raw_value", a.AggregationType))
		}
		aggInfo := &metapb.AggInfo{
			Name:            a.Name,
			Value:           a.Value,
			AggregationType: splitspb.AggregationType(aggTypeVal),
			AliasColumn:     a.AliasColumn,
		}
		if a.ValueType == "FLOAT" || a.ValueType == "float" {
			aggInfo.ValueType = metapb.AggValueType_AGG_VALUE_TYPE_FLOAT
		} else {
			aggInfo.ValueType = metapb.AggValueType_AGG_VALUE_TYPE_INT
		}
		pbAggs[i] = aggInfo
	}
	return &metapb.ListAggsResponse{Aggs: pbAggs}, nil
}

// ListSketches returns sketch metadata for a table.
func (h *MetadataHandler) ListSketches(ctx context.Context, req *metapb.ListSketchesRequest) (*metapb.ListSketchesResponse, error) {
	if req.GetTable() == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "table is required")
	}

	sketches, err := h.querier.ListSketches(ctx, req.GetTable())
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "list sketches: %v", err)
	}

	pbSketches := make([]*metapb.SketchInfo, len(sketches))
	for i, s := range sketches {
		pbSketches[i] = &metapb.SketchInfo{Name: s.Name}
	}
	return &metapb.ListSketchesResponse{Sketches: pbSketches}, nil
}
