package grpc

import (
	"context"

	"go.uber.org/zap"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/internal/coordinator/ingestion"
)

// IngestionHandler implements the MetadataIngestionService gRPC interface.
type IngestionHandler struct {
	pb.UnimplementedMetadataIngestionServiceServer
	service *ingestion.Service
	log     *zap.Logger
}

// NewIngestionHandler creates an IngestionHandler.
func NewIngestionHandler(svc *ingestion.Service, log *zap.Logger) *IngestionHandler {
	return &IngestionHandler{service: svc, log: log}
}

// Ingest handles a single metadata record ingestion request.
func (s *IngestionHandler) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
	result := s.service.Ingest(ctx, req.GetTableName(), req.GetRecord())
	if !result.Accepted {
		s.log.Warn("ingest failed",
			zap.String("table", req.GetTableName()),
			zap.String("error", result.Error),
		)
	} else {
		s.log.Debug("record ingested",
			zap.String("table", req.GetTableName()),
		)
	}
	return &pb.IngestResponse{
		Accepted: result.Accepted,
		Error:    result.Error,
	}, nil
}
