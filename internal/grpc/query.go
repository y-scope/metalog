package grpc

import (
	"fmt"

	"go.uber.org/zap"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/y-scope/metalog/gen/proto/splitspb"
	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/query"
	"github.com/y-scope/metalog/internal/schema"
)

// RegistryLookup returns the ColumnRegistry for a table, or nil.
type RegistryLookup func(tableName string) *schema.ColumnRegistry

// QueryHandler implements the QuerySplitsService gRPC interface.
type QueryHandler struct {
	pb.UnimplementedQuerySplitsServiceServer
	engine         *query.SplitQueryEngine
	registryLookup RegistryLookup
	log            *zap.Logger
}

// NewQueryHandler creates a QueryHandler.
func NewQueryHandler(engine *query.SplitQueryEngine, lookup RegistryLookup, log *zap.Logger) *QueryHandler {
	return &QueryHandler{engine: engine, registryLookup: lookup, log: log}
}

// StreamSplits handles server-streaming split queries.
func (h *QueryHandler) StreamSplits(req *pb.StreamSplitsRequest, stream gogrpc.ServerStreamingServer[pb.StreamSplitsResponse]) error {
	if req.GetTable() == "" {
		return status.Error(codes.InvalidArgument, "table is required")
	}

	// Validate filter expression
	if err := query.ValidateFilterExpression(req.GetFilterExpression()); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid filter: %v", err)
	}

	// Build query params
	params := &query.QueryParams{
		TableName:      req.GetTable(),
		StateFilter:    req.GetStateFilter(),
		FilterExpr:     req.GetFilterExpression(),
		Limit:          int(req.GetLimit()),
		AllowUnindexed: req.GetAllowUnindexedSort(),
		Registry:       h.registryLookup(req.GetTable()),
	}

	// Columns (projection)
	if len(req.GetProjection()) > 0 {
		params.Columns = req.GetProjection()
	}

	// Order by
	for _, ob := range req.GetOrderBy() {
		params.OrderBy = append(params.OrderBy, query.OrderBySpec{
			Column: ob.GetColumn(),
			Desc:   ob.GetOrder() == pb.Order_ORDER_DESC,
		})
	}

	// Cursor
	if c := req.GetCursor(); c != nil {
		params.HasCursor = true
		params.CursorID = c.GetId()
		for _, cv := range c.GetValues() {
			switch v := cv.GetValue().(type) {
			case *pb.CursorValue_IntVal:
				params.CursorValues = append(params.CursorValues, v.IntVal)
			case *pb.CursorValue_FloatVal:
				params.CursorValues = append(params.CursorValues, v.FloatVal)
			case *pb.CursorValue_StrVal:
				params.CursorValues = append(params.CursorValues, v.StrVal)
			}
		}
	}

	if params.Limit <= 0 {
		params.Limit = config.DefaultQueryLimit
	}

	// Execute query
	rows, err := h.engine.Query(stream.Context(), params)
	if err != nil {
		h.log.Error("query failed", zap.String("table", req.GetTable()), zap.Error(err))
		return status.Errorf(codes.Internal, "query: %v", err)
	}

	// Stream results
	var seq int32
	for _, row := range rows {
		split := rowToProtoSplit(row)

		resp := &pb.StreamSplitsResponse{
			Split:    split,
			Sequence: seq,
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
		seq++
	}

	// Send final response with stats
	return stream.Send(&pb.StreamSplitsResponse{
		Done: true,
		Stats: &pb.QueryStats{
			SplitsScanned: int64(len(rows)),
			SplitsMatched: int64(len(rows)),
		},
		Sequence: seq,
	})
}

func rowToProtoSplit(row *query.SplitRow) *pb.Split {
	split := &pb.Split{
		Id:         row.ID,
		Dimensions: make(map[string]string),
	}

	var irSizeBytes, archiveSizeBytes int64

	for col, val := range row.Values {
		switch col {
		case "clp_ir_path":
			split.ClpIrPath = dbValToString(val)
		case "clp_archive_path":
			split.ClpArchivePath = dbValToString(val)
		case "min_timestamp":
			if v, ok := val.(int64); ok {
				split.MinTimestamp = v
			}
		case "max_timestamp":
			if v, ok := val.(int64); ok {
				split.MaxTimestamp = v
			}
		case "state":
			split.State = dbValToString(val)
		case "record_count":
			if v, ok := val.(int64); ok {
				split.RecordCount = v
			}
		case "clp_ir_size_bytes":
			if v, ok := val.(int64); ok {
				irSizeBytes = v
			}
		case "clp_archive_size_bytes":
			if v, ok := val.(int64); ok {
				archiveSizeBytes = v
			}
		case "clp_ir_storage_backend":
			split.ClpIrStorageBackend = dbValToString(val)
		case "clp_ir_bucket":
			split.ClpIrBucket = dbValToString(val)
		case "clp_archive_storage_backend":
			split.ClpArchiveStorageBackend = dbValToString(val)
		case "clp_archive_bucket":
			split.ClpArchiveBucket = dbValToString(val)
		default:
			// Dimension columns
			if val != nil {
				split.Dimensions[col] = dbValToString(val)
			}
		}
	}

	// Archive size takes precedence over IR size when available.
	if archiveSizeBytes > 0 {
		split.SizeBytes = archiveSizeBytes
	} else {
		split.SizeBytes = irSizeBytes
	}

	return split
}

// dbValToString converts a database value to a string.
// Handles []byte (from MySQL driver) and other types efficiently.
func dbValToString(val any) string {
	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}
