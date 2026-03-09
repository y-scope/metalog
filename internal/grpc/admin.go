package grpc

import (
	"context"
	"database/sql"
	"regexp"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/y-scope/metalog/gen/proto/coordinatorpb"
	"github.com/y-scope/metalog/internal/coordinator"
	"github.com/y-scope/metalog/internal/metastore"
)

// AdminHandler implements the AdminService gRPC interface.
type AdminHandler struct {
	pb.UnimplementedAdminServiceServer
	registration *coordinator.TableRegistration
	db           *sql.DB
	log          *zap.Logger
}

// NewAdminHandler creates a AdminHandler.
func NewAdminHandler(reg *coordinator.TableRegistration, db *sql.DB, log *zap.Logger) *AdminHandler {
	return &AdminHandler{registration: reg, db: db, log: log}
}

// RegisterTable handles runtime table registration requests.
func (h *AdminHandler) RegisterTable(ctx context.Context, req *pb.RegisterTableRequest) (*pb.RegisterTableResponse, error) {
	if req.GetTableName() == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}

	var kafkaTopic, kafkaBootstrap, transformer string
	if kafkaCfg := req.GetKafka(); kafkaCfg != nil {
		if kafkaCfg.GetTopic() == "" {
			return nil, status.Error(codes.InvalidArgument, "kafka.topic is required when kafka is set")
		}
		if kafkaCfg.GetBootstrapServers() == "" {
			return nil, status.Error(codes.InvalidArgument, "kafka.bootstrap_servers is required when kafka is set")
		}
		kafkaTopic = kafkaCfg.GetTopic()
		kafkaBootstrap = kafkaCfg.GetBootstrapServers()
		transformer = kafkaCfg.GetRecordTransformer()
	}

	created, err := h.registration.RegisterTable(ctx,
		req.GetTableName(), req.GetDisplayName(),
		kafkaTopic, kafkaBootstrap, transformer,
		coordinator.RegisterTableOpts{
			KafkaPollerEnabled:   req.KafkaPollerEnabled,
			ConsolidationEnabled: req.ConsolidationEnabled,
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

// maxAliasLength is the maximum length of an alias column value.
const maxAliasLength = 128

// aliasPattern is the allowed pattern for alias values: alphanumeric, underscores, dots, hyphens, slashes.
var aliasPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_./-]*$`)

// SetColumnAlias sets or clears the alias for a dimension or aggregation column.
// Updates the database only — nodes pick up changes via periodic alias refresh.
func (h *AdminHandler) SetColumnAlias(ctx context.Context, req *pb.SetColumnAliasRequest) (*pb.SetColumnAliasResponse, error) {
	tableName := req.GetTableName()
	colName := req.GetColumnName()
	alias := strings.TrimSpace(req.GetAliasColumn())

	if tableName == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}
	if colName == "" {
		return nil, status.Error(codes.InvalidArgument, "column_name is required")
	}
	if alias != "" {
		if len(alias) > maxAliasLength {
			return nil, status.Errorf(codes.InvalidArgument,
				"alias_column exceeds max length of %d characters", maxAliasLength)
		}
		if !aliasPattern.MatchString(alias) {
			return nil, status.Error(codes.InvalidArgument,
				"alias_column must match [a-zA-Z_][a-zA-Z0-9_./-]*")
		}
	}

	// Determine which registry table to update based on column prefix.
	var registryTable string
	if strings.HasPrefix(colName, metastore.DimColumnPrefix) {
		registryTable = metastore.DimRegistryTable
	} else if strings.HasPrefix(colName, metastore.AggColumnPrefix) {
		registryTable = metastore.AggRegistryTable
	} else {
		return nil, status.Errorf(codes.InvalidArgument,
			"column_name must start with %q or %q", metastore.DimColumnPrefix, metastore.AggColumnPrefix)
	}

	// Atomic update — only touches ACTIVE rows, eliminating the TOCTOU race
	// of a separate SELECT followed by UPDATE.
	var aliasVal any
	if alias != "" {
		aliasVal = alias
	}
	updateQuery, updateArgs, _ := sq.Update(registryTable).
		Set("alias_column", aliasVal).
		Where(sq.Eq{"table_name": tableName, "column_name": colName, "state": "ACTIVE"}).
		ToSql()
	res, err := h.db.ExecContext(ctx, updateQuery, updateArgs...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update alias: %v", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "rows affected: %v", err)
	}
	if affected == 0 {
		return nil, status.Errorf(codes.NotFound,
			"no ACTIVE column %s in table %s", colName, tableName)
	}

	h.log.Info("column alias updated",
		zap.String("table", tableName),
		zap.String("column", colName),
		zap.String("alias", alias),
	)

	return &pb.SetColumnAliasResponse{
		ColumnName:  colName,
		AliasColumn: alias,
	}, nil
}
