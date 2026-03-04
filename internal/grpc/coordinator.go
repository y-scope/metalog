package grpc

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/y-scope/metalog/gen/proto/coordinatorpb"
	"github.com/y-scope/metalog/internal/coordinator"
)

// CoordinatorHandler implements the CoordinatorService gRPC interface.
type CoordinatorHandler struct {
	pb.UnimplementedCoordinatorServiceServer
	registration *coordinator.TableRegistration
	log          *zap.Logger
}

// NewCoordinatorHandler creates a CoordinatorHandler.
func NewCoordinatorHandler(reg *coordinator.TableRegistration, log *zap.Logger) *CoordinatorHandler {
	return &CoordinatorHandler{registration: reg, log: log}
}

// RegisterTable handles runtime table registration requests.
func (s *CoordinatorHandler) RegisterTable(ctx context.Context, req *pb.RegisterTableRequest) (*pb.RegisterTableResponse, error) {
	if req.GetTableName() == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}

	kafka := req.GetKafka()
	var kafkaTopic, kafkaBootstrap, transformer string
	if kafka != nil {
		kafkaTopic = kafka.GetTopic()
		kafkaBootstrap = kafka.GetBootstrapServers()
		transformer = kafka.GetRecordTransformer()
	}

	created, err := s.registration.RegisterTable(ctx,
		req.GetTableName(), req.GetDisplayName(),
		kafkaTopic, kafkaBootstrap, transformer,
	)
	if err != nil {
		s.log.Error("register table failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "register table: %v", err)
	}

	return &pb.RegisterTableResponse{
		TableName: req.GetTableName(),
		Created:   created,
	}, nil
}
