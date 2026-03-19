package grpcserver

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/y-scope/metalog/gen/proto/coordinatorpb"
	"github.com/y-scope/metalog/coordinator"
)

// AdminHandler implements the AdminService gRPC interface.
type AdminHandler struct {
	pb.UnimplementedAdminServiceServer
	registration *coordinator.TableRegistration
	log          *zap.Logger
}

// NewAdminHandler creates an AdminHandler.
func NewAdminHandler(reg *coordinator.TableRegistration, log *zap.Logger) *AdminHandler {
	return &AdminHandler{registration: reg, log: log}
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
