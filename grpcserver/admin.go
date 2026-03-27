package grpcserver

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/y-scope/metalog/gen/proto/coordinatorpb"
	"github.com/y-scope/metalog/coordinator"
	"github.com/y-scope/metalog/metastore"
)

// AdminHandler implements the AdminService gRPC interface.
type AdminHandler struct {
	pb.UnimplementedAdminServiceServer
	registration *coordinator.TableRegistration
	kafkaSources *metastore.KafkaSourceStore
	log          *zap.Logger
}

// NewAdminHandler creates an AdminHandler.
func NewAdminHandler(reg *coordinator.TableRegistration, kafkaSources *metastore.KafkaSourceStore, log *zap.Logger) *AdminHandler {
	return &AdminHandler{registration: reg, kafkaSources: kafkaSources, log: log}
}

// RegisterTable handles runtime table registration requests.
func (h *AdminHandler) RegisterTable(ctx context.Context, req *pb.RegisterTableRequest) (*pb.RegisterTableResponse, error) {
	if req.GetTableName() == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}

	created, err := h.registration.RegisterTable(ctx,
		req.GetTableName(), req.GetDisplayName(),
		coordinator.RegisterTableOpts{
			ConfigJSON: req.ConfigJson,
		},
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

// SetColumnAlias sets or clears the alias for a dimension or aggregation column.
func (h *AdminHandler) SetColumnAlias(ctx context.Context, req *pb.SetColumnAliasRequest) (*pb.SetColumnAliasResponse, error) {
	if req.GetTableName() == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}
	if req.GetColumnName() == "" {
		return nil, status.Error(codes.InvalidArgument, "column_name is required")
	}

	alias, err := h.registration.SetColumnAlias(ctx, req.GetTableName(), req.GetColumnName(), req.GetAliasColumn())
	if err != nil {
		h.log.Error("set column alias failed", zap.String("table", req.GetTableName()), zap.String("column", req.GetColumnName()), zap.Error(err))
		return nil, mapAdminError(err)
	}

	return &pb.SetColumnAliasResponse{
		ColumnName:  req.GetColumnName(),
		AliasColumn: alias,
	}, nil
}

// InvalidateColumn marks a dimension or aggregation column as INVALIDATED.
func (h *AdminHandler) InvalidateColumn(ctx context.Context, req *pb.InvalidateColumnRequest) (*pb.InvalidateColumnResponse, error) {
	if req.GetTableName() == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}
	if req.GetColumnName() == "" {
		return nil, status.Error(codes.InvalidArgument, "column_name is required")
	}

	previousKey, err := h.registration.InvalidateColumn(ctx, req.GetTableName(), req.GetColumnName())
	if err != nil {
		h.log.Error("invalidate column failed", zap.String("table", req.GetTableName()), zap.String("column", req.GetColumnName()), zap.Error(err))
		return nil, mapAdminError(err)
	}

	return &pb.InvalidateColumnResponse{
		ColumnName:  req.GetColumnName(),
		PreviousKey: previousKey,
	}, nil
}

// RegisterKafkaSource registers a Kafka ingestion source for a table.
func (h *AdminHandler) RegisterKafkaSource(ctx context.Context, req *pb.RegisterKafkaSourceRequest) (*pb.RegisterKafkaSourceResponse, error) {
	if req.GetTableName() == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}
	if req.GetSourceName() == "" {
		return nil, status.Error(codes.InvalidArgument, "source_name is required")
	}
	if req.GetTopic() == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	if req.GetBootstrapServers() == "" {
		return nil, status.Error(codes.InvalidArgument, "bootstrap_servers is required")
	}
	if req.GetConsumerGroupId() == "" {
		return nil, status.Error(codes.InvalidArgument, "consumer_group_id is required")
	}

	// Check for duplicate consumer_group_id across all sources for this table.
	existing, err := h.kafkaSources.ListSources(ctx, req.GetTableName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list sources: %v", err)
	}
	for _, s := range existing {
		if s.SourceName != req.GetSourceName() && s.ConsumerGroupID == req.GetConsumerGroupId() {
			return nil, status.Errorf(codes.AlreadyExists,
				"consumer_group_id %q is already used by source %q for table %q",
				req.GetConsumerGroupId(), s.SourceName, req.GetTableName())
		}
	}

	if err := metastore.ValidateRequiredEnv(req.GetRequiredEnv()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	transformer := req.GetRecordTransformer()
	if transformer == "" {
		transformer = "proto"
	}

	src := &metastore.KafkaSource{
		TableName:         req.GetTableName(),
		SourceName:          req.GetSourceName(),
		Topic:             req.GetTopic(),
		BootstrapServers:  req.GetBootstrapServers(),
		RecordTransformer: transformer,
		ConsumerGroupID:   req.GetConsumerGroupId(),
		RequiredEnv:          req.GetRequiredEnv(),
	}

	created, err := h.kafkaSources.Register(ctx, src)
	if err != nil {
		h.log.Error("register kafka source failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "register kafka source: %v", err)
	}

	return &pb.RegisterKafkaSourceResponse{
		TableName: src.TableName,
		SourceName:  src.SourceName,
		Created:   created,
	}, nil
}

// DeleteKafkaSource removes a Kafka source and its assignment.
func (h *AdminHandler) DeleteKafkaSource(ctx context.Context, req *pb.DeleteKafkaSourceRequest) (*pb.DeleteKafkaSourceResponse, error) {
	if req.GetTableName() == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}
	if req.GetSourceName() == "" {
		return nil, status.Error(codes.InvalidArgument, "source_name is required")
	}

	if err := h.kafkaSources.Delete(ctx, req.GetTableName(), req.GetSourceName()); err != nil {
		h.log.Error("delete kafka source failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "delete kafka source: %v", err)
	}

	return &pb.DeleteKafkaSourceResponse{}, nil
}

// mapAdminError converts coordinator sentinel errors to gRPC status errors.
func mapAdminError(err error) error {
	switch {
	case errors.Is(err, coordinator.ErrInvalidColumnPrefix):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	case errors.Is(err, coordinator.ErrInvalidAlias):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	case errors.Is(err, coordinator.ErrColumnNotFound):
		return status.Errorf(codes.NotFound, "%v", err)
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}
