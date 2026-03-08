package grpc

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/y-scope/metalog/gen/proto/coordinatorpb"
	"github.com/y-scope/metalog/internal/coordinator"
)

// AdminHandler implements the AdminService gRPC interface.
type AdminHandler struct {
	pb.UnimplementedAdminServiceServer
	registration *coordinator.TableRegistration
	log          *zap.Logger
}

// NewAdminHandler creates a AdminHandler.
func NewAdminHandler(reg *coordinator.TableRegistration, log *zap.Logger) *AdminHandler {
	return &AdminHandler{registration: reg, log: log}
}

// RegisterTable handles runtime table registration requests.
func (h *AdminHandler) RegisterTable(ctx context.Context, req *pb.RegisterTableRequest) (*pb.RegisterTableResponse, error) {
	if req.GetTableName() == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}

	kafkaCfg := req.GetKafka()
	var kafkaTopic, kafkaBootstrap, transformer string
	if kafkaCfg != nil {
		kafkaTopic = kafkaCfg.GetTopic()
		kafkaBootstrap = kafkaCfg.GetBootstrapServers()
		transformer = kafkaCfg.GetRecordTransformer()
	}

	created, err := h.registration.RegisterTable(ctx,
		req.GetTableName(), req.GetDisplayName(),
		kafkaTopic, kafkaBootstrap, transformer,
	)
	if err != nil {
		h.log.Error("register table failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "register table: %v", err)
	}

	return &pb.RegisterTableResponse{
		TableName: req.GetTableName(),
		Created:   created,
	}, nil
}
