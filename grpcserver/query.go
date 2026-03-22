package grpcserver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/y-scope/metalog/gen/proto/splitspb"
	"github.com/y-scope/metalog/config"
	"github.com/y-scope/metalog/query"
	"github.com/y-scope/metalog/schema"
)

const (
	// defaultStreamTimeout is the maximum duration for a StreamSplits RPC
	// when the client does not set stream_idle_timeout_ms.
	defaultStreamTimeout = 60 * time.Second

	// minStreamTimeout prevents clients from setting an unreasonably short
	// timeout that would cancel every query before results are returned.
	minStreamTimeout = 1 * time.Second

	// maxStreamTimeout prevents clients from requesting an effectively
	// infinite server-side timeout.
	maxStreamTimeout = 10 * time.Minute
)

// RegistryLookup returns the ColumnRegistry for a table, or nil.
type RegistryLookup func(tableName string) *schema.ColumnRegistry

// QueryHandler implements the SplitQueryService gRPC interface.
type QueryHandler struct {
	pb.UnimplementedSplitQueryServiceServer
	engine         *query.SplitQueryEngine
	registryLookup RegistryLookup
	log            *zap.Logger

	mRequests      metric.Int64Counter
	mDuration      metric.Float64Histogram
	mSplitsMatched metric.Int64Counter
}

// NewQueryHandler creates a QueryHandler.
func NewQueryHandler(engine *query.SplitQueryEngine, lookup RegistryLookup, log *zap.Logger) *QueryHandler {
	h := &QueryHandler{engine: engine, registryLookup: lookup, log: log}
	h.initMetrics(noop.Meter{})
	return h
}

// SetMeter configures OpenTelemetry metrics. Must be called before serving.
func (h *QueryHandler) SetMeter(m metric.Meter) { h.initMetrics(m) }

func (h *QueryHandler) initMetrics(m metric.Meter) {
	h.mRequests, _ = m.Int64Counter("metalog.query.requests",
		metric.WithDescription("Split query requests"), metric.WithUnit("{request}"))
	h.mDuration, _ = m.Float64Histogram("metalog.query.duration_seconds",
		metric.WithDescription("Split query duration"), metric.WithUnit("s"))
	h.mSplitsMatched, _ = m.Int64Counter("metalog.query.splits_matched",
		metric.WithDescription("Splits matched by queries"), metric.WithUnit("{split}"))
}

// StreamSplits handles server-streaming split queries. It fetches all matching
// splits across multiple internal pages using a background prefetch goroutine,
// streaming each result to the client as it becomes available.
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
		TableName:        req.GetTable(),
		FilterExpr:     req.GetFilterExpression(),
		AllowUnindexed: req.GetAllowUnindexedSort(),
		Registry:       h.registryLookup(req.GetTable()),
		SketchExpr:     req.GetSketchExpression(),
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

	// Cursor (initial position for first page)
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

	totalLimit := int(req.GetLimit())
	if totalLimit <= 0 {
		totalLimit = config.DefaultQueryLimit
	}

	pageSize := config.DefaultQueryPageSize
	if totalLimit > 0 && totalLimit < pageSize {
		pageSize = totalLimit
	}

	// Apply stream deadline. The client can set stream_idle_timeout_ms to
	// control how long the server will spend streaming results, clamped to
	// [minStreamTimeout, maxStreamTimeout].
	streamTimeout := defaultStreamTimeout
	if ms := req.GetStreamIdleTimeoutMs(); ms > 0 {
		streamTimeout = time.Duration(ms) * time.Millisecond
		if streamTimeout < minStreamTimeout {
			streamTimeout = minStreamTimeout
		} else if streamTimeout > maxStreamTimeout {
			streamTimeout = maxStreamTimeout
		}
	}
	streamCtx, streamCancel := context.WithTimeout(stream.Context(), streamTimeout)
	defer streamCancel()

	// Stream results via prefetch consumer.
	var seq int32
	registry := params.Registry
	includeCursor := req.GetIncludeCursor()

	consumer := func(swc *query.SplitWithCursor) (bool, error) {
		seq++
		split := rowToProtoSplit(swc.Row, registry, h.log)

		resp := &pb.StreamSplitsResponse{
			Split:    split,
			Sequence: seq,
		}
		if includeCursor {
			resp.Cursor = &pb.KeysetCursor{Id: swc.CursorID}
			for _, v := range swc.CursorValues {
				resp.Cursor.Values = append(resp.Cursor.Values, toCursorValue(v))
			}
		}
		if err := stream.Send(resp); err != nil {
			return false, err
		}
		return true, nil
	}

	queryStart := time.Now()
	tableAttr := attribute.String("table", req.GetTable())

	result, err := h.engine.StreamSplitsAsync(streamCtx, params, totalLimit, pageSize, consumer)
	if err != nil {
		h.mRequests.Add(stream.Context(), 1, metric.WithAttributes(tableAttr, attribute.String("status", "error")))
		h.mDuration.Record(stream.Context(), time.Since(queryStart).Seconds(), metric.WithAttributes(tableAttr))
		if streamCtx.Err() != nil {
			h.log.Debug("stream cancelled", zap.String("table", req.GetTable()), zap.Error(streamCtx.Err()))
			return status.FromContextError(streamCtx.Err()).Err()
		}
		h.log.Error("query failed", zap.String("table", req.GetTable()), zap.Error(err))
		return status.Errorf(codes.Internal, "query: %v", err)
	}
	h.mRequests.Add(stream.Context(), 1, metric.WithAttributes(tableAttr, attribute.String("status", "success")))
	h.mDuration.Record(stream.Context(), time.Since(queryStart).Seconds(), metric.WithAttributes(tableAttr))
	h.mSplitsMatched.Add(stream.Context(), result.SplitsMatched, metric.WithAttributes(tableAttr))

	// Send final response with stats
	seq++
	return stream.Send(&pb.StreamSplitsResponse{
		Done: true,
		Stats: &pb.QueryStats{
			SplitsScanned: result.SplitsScanned,
			SplitsMatched: result.SplitsMatched,
		},
		Sequence: seq,
	})
}

func rowToProtoSplit(row *query.SplitRow, registry *schema.ColumnRegistry, log *zap.Logger) *pb.Split {
	rs := query.ResolveSplit(row, registry)
	split := &pb.Split{
		Id:                       rs.ID,
		MinTimestamp:             rs.MinTimestamp,
		MaxTimestamp:             rs.MaxTimestamp,
		RecordCount:              rs.RecordCount,
		SizeBytes:                rs.SizeBytes,
		ClpIrPath:                rs.ClpIRPath,
		ClpArchivePath:           rs.ClpArchivePath,
		State:                    rs.State,
		ClpIrStorageBackend:      rs.ClpIRStorageBackend,
		ClpIrBucket:              rs.ClpIRBucket,
		ClpArchiveStorageBackend: rs.ClpArchiveStorageBackend,
		ClpArchiveBucket:         rs.ClpArchiveBucket,
		Dimensions:               rs.Dimensions,
	}
	for _, ra := range rs.Aggs {
		aggTypeName := "AGGREGATION_TYPE_" + ra.AggregationType
		aggTypeVal, ok := pb.AggregationType_value[aggTypeName]
		if !ok {
			log.Warn("unknown aggregation_type, defaulting to UNSPECIFIED",
				zap.String("raw_value", ra.AggregationType))
		}
		aggEntry := &pb.AggEntry{
			Key:             ra.Key,
			Value:           ra.Value,
			AggregationType: pb.AggregationType(aggTypeVal),
		}
		if ra.HasResult {
			if strings.EqualFold(ra.ValueType, "FLOAT") {
				aggEntry.Result = &pb.AggEntry_FloatValue{FloatValue: ra.FloatResult}
			} else {
				aggEntry.Result = &pb.AggEntry_IntValue{IntValue: ra.IntResult}
			}
		}
		split.Aggs = append(split.Aggs, aggEntry)
	}
	return split
}

func toCursorValue(val any) *pb.CursorValue {
	if val == nil {
		// NULL sort column values are represented as unset CursorValue (zero
		// oneof). The handler rejects these on inbound cursors, preventing a
		// NULL cursor from silently corrupting pagination.
		return &pb.CursorValue{}
	}
	switch v := val.(type) {
	case int64:
		return &pb.CursorValue{Value: &pb.CursorValue_IntVal{IntVal: v}}
	case int32:
		return &pb.CursorValue{Value: &pb.CursorValue_IntVal{IntVal: int64(v)}}
	case uint64:
		return &pb.CursorValue{Value: &pb.CursorValue_IntVal{IntVal: int64(v)}}
	case uint32:
		return &pb.CursorValue{Value: &pb.CursorValue_IntVal{IntVal: int64(v)}}
	case float64:
		return &pb.CursorValue{Value: &pb.CursorValue_FloatVal{FloatVal: v}}
	case float32:
		return &pb.CursorValue{Value: &pb.CursorValue_FloatVal{FloatVal: float64(v)}}
	case string:
		return &pb.CursorValue{Value: &pb.CursorValue_StrVal{StrVal: v}}
	case []byte:
		return &pb.CursorValue{Value: &pb.CursorValue_StrVal{StrVal: string(v)}}
	default:
		return &pb.CursorValue{Value: &pb.CursorValue_StrVal{StrVal: fmt.Sprintf("%v", v)}}
	}
}

