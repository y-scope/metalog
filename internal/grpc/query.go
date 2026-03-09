package grpc

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/y-scope/metalog/gen/proto/splitspb"
	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/metastore"
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
	if len(req.GetOrderBy()) == 0 {
		return status.Error(codes.InvalidArgument, "order_by is required (must have at least one field)")
	}
	for _, ob := range req.GetOrderBy() {
		if ob.GetColumn() == "" {
			return status.Error(codes.InvalidArgument, "order_by column must not be empty")
		}
		if ob.GetColumn() == "id" {
			return status.Error(codes.InvalidArgument, "\"id\" must not appear in order_by — it is the implicit final tiebreaker")
		}
		if ob.GetOrder() == pb.Order_ORDER_UNSPECIFIED {
			return status.Error(codes.InvalidArgument, "order_by order must be ORDER_ASC or ORDER_DESC, not UNSPECIFIED")
		}
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
		if len(c.GetValues()) != len(req.GetOrderBy()) {
			return status.Errorf(codes.InvalidArgument,
				"cursor.values has %d entries but order_by has %d",
				len(c.GetValues()), len(req.GetOrderBy()))
		}
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
			default:
				return status.Error(codes.InvalidArgument, "cursor value has no value set")
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

	// Stream results (sequence is 1-based per proto spec)
	var seq int32
	for _, row := range rows {
		seq++
		split := rowToProtoSplit(row, params.Registry)

		resp := &pb.StreamSplitsResponse{
			Split:    split,
			Sequence: seq,
		}
		if req.GetIncludeCursor() {
			resp.Cursor = buildResponseCursor(row, params.OrderBy)
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
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

func rowToProtoSplit(row *query.SplitRow, registry *schema.ColumnRegistry) *pb.Split {
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
			if val == nil {
				continue
			}
			// Agg columns: reverse-map physical name to structured AggEntry
			if strings.HasPrefix(col, metastore.AggColumnPrefix) && registry != nil {
				if entry := registry.LookupAggByColumn(col); entry != nil {
					aggEntry := &pb.AggEntry{
						Key:             entry.AggKey,
						Value:           entry.AggValue,
						AggregationType: pb.AggregationType(pb.AggregationType_value["AGGREGATION_TYPE_"+entry.AggregationType]),
					}
					if entry.ValueType == "FLOAT" {
						if f, ok := dbValToFloat64(val); ok {
							aggEntry.Result = &pb.AggEntry_FloatValue{FloatValue: f}
						}
					} else {
						if i, ok := dbValToInt64(val); ok {
							aggEntry.Result = &pb.AggEntry_IntValue{IntValue: i}
						}
					}
					split.Aggs = append(split.Aggs, aggEntry)
					continue
				}
			}
			// Dimension columns: reverse-map physical name to semantic dim_key.
			// Falls back to physical column name if no registry entry exists.
			dimKey := col
			if strings.HasPrefix(col, metastore.DimColumnPrefix) && registry != nil {
				if entry := registry.LookupDimByColumn(col); entry != nil {
					dimKey = entry.DimKey
				}
			}
			split.Dimensions[dimKey] = dbValToString(val)
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

// buildResponseCursor creates a KeysetCursor from a result row's sort column values.
func buildResponseCursor(row *query.SplitRow, orderBy []query.OrderBySpec) *pb.KeysetCursor {
	cursor := &pb.KeysetCursor{Id: row.ID}
	for _, ob := range orderBy {
		cursor.Values = append(cursor.Values, toCursorValue(row.Values[ob.Column]))
	}
	return cursor
}

func toCursorValue(val any) *pb.CursorValue {
	switch v := val.(type) {
	case int64:
		return &pb.CursorValue{Value: &pb.CursorValue_IntVal{IntVal: v}}
	case int32:
		return &pb.CursorValue{Value: &pb.CursorValue_IntVal{IntVal: int64(v)}}
	case float64:
		return &pb.CursorValue{Value: &pb.CursorValue_FloatVal{FloatVal: v}}
	case string:
		return &pb.CursorValue{Value: &pb.CursorValue_StrVal{StrVal: v}}
	case []byte:
		return &pb.CursorValue{Value: &pb.CursorValue_StrVal{StrVal: string(v)}}
	default:
		return &pb.CursorValue{Value: &pb.CursorValue_StrVal{StrVal: fmt.Sprintf("%v", v)}}
	}
}

func dbValToInt64(val any) (int64, bool) {
	switch v := val.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

func dbValToFloat64(val any) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	default:
		return 0, false
	}
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
