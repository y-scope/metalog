package grpcserver

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ingestiongrpc "github.com/y-scope/metalog/gen/proto/ingestiongrpc"
	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/ingestion"
)

// IngestionHandler implements the MetadataIngestionService gRPC interface.
type IngestionHandler struct {
	ingestiongrpc.UnimplementedMetadataIngestionServiceServer
	service *ingestion.Service
	log     *zap.Logger
}

// NewIngestionHandler creates an IngestionHandler.
func NewIngestionHandler(svc *ingestion.Service, log *zap.Logger) *IngestionHandler {
	return &IngestionHandler{service: svc, log: log}
}

// Ingest handles a single metadata record ingestion request.
// Backpressure and transport errors are returned as gRPC status codes:
//   - RESOURCE_EXHAUSTED: ingestion channel full (client should retry with backoff)
//   - DEADLINE_EXCEEDED: request context deadline passed
//   - CANCELLED: request context was cancelled
//   - INVALID_ARGUMENT: validation failure (missing fields, invalid state)
//
// Application-level errors (internal processing) are returned via IngestResponse.
func (h *IngestionHandler) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
	rec, err := ingestion.ConvertRecord(req.GetRecord())
	if err != nil {
		return nil, mapIngestionError(err)
	}

	result := h.service.Ingest(ctx, req.GetTableName(), rec)
	if !result.Accepted {
		if grpcErr := h.mapToGRPCError(result); grpcErr != nil {
			return nil, grpcErr
		}
		h.log.Warn("ingest failed",
			zap.String("table", req.GetTableName()),
			zap.String("error", result.Error),
		)
	} else {
		h.log.Debug("record ingested",
			zap.String("table", req.GetTableName()),
		)
	}
	return &pb.IngestResponse{
		Accepted: result.Accepted,
		Error:    result.Error,
	}, nil
}

// mapIngestionError maps a ConvertRecord error to a gRPC status.
func mapIngestionError(err error) error {
	var ve *ingestion.ValidationError
	if errors.As(err, &ve) {
		return status.Error(codes.InvalidArgument, ve.Error())
	}
	return status.Error(codes.InvalidArgument, err.Error())
}

// mapToGRPCError maps known error types to gRPC status codes.
// Returns nil if the error should remain as an application-level IngestResponse.
func (h *IngestionHandler) mapToGRPCError(result *ingestion.IngestionResult) error {
	err := result.Err
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ingestion.ErrChannelFull):
		return status.Error(codes.ResourceExhausted, err.Error())
	case err == context.DeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, err.Error())
	case err == context.Canceled:
		return status.Error(codes.Canceled, err.Error())
	default:
		var ve *ingestion.ValidationError
		if errors.As(err, &ve) {
			return status.Error(codes.InvalidArgument, ve.Error())
		}
		return nil
	}
}
