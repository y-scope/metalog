package coordinator

import (
	"context"
	"database/sql"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/db"
	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/schema"
)

// TableRegistration handles registering new tables at runtime.
type TableRegistration struct {
	db                  *sql.DB
	isMariaDB           bool
	compressionOverride string
	log                 *zap.Logger
}

// NewTableRegistration creates a TableRegistration.
func NewTableRegistration(db *sql.DB, isMariaDB bool, compressionOverride string, log *zap.Logger) *TableRegistration {
	return &TableRegistration{db: db, isMariaDB: isMariaDB, compressionOverride: compressionOverride, log: log}
}

// RegisterTable provisions a table and upserts its Kafka config.
// Returns (created bool, err error). created is true if the table was newly provisioned.
func (s *TableRegistration) RegisterTable(
	ctx context.Context,
	tableName, displayName string,
	kafkaTopic, kafkaBootstrapServers, recordTransformer string,
) (bool, error) {
	if err := db.ValidateSQLIdentifier(tableName); err != nil {
		return false, err
	}

	// Check if table already exists
	existsQuery, existsArgs, _ := sq.Select("COUNT(*) > 0").
		From(metastore.TableRegistry).
		Where(sq.Eq{"table_name": tableName}).
		ToSql()
	var exists bool
	if err := s.db.QueryRowContext(ctx, existsQuery, existsArgs...).Scan(&exists); err != nil {
		return false, err
	}

	// Provision table
	if err := schema.EnsureTable(ctx, s.db, tableName, s.isMariaDB, s.compressionOverride, s.log); err != nil {
		return false, err
	}

	// Update display name if provided
	if displayName != "" {
		updateQuery, updateArgs, _ := sq.Update(metastore.TableRegistry).
			Set("display_name", displayName).
			Where(sq.Eq{"table_name": tableName}).
			ToSql()
		if _, err := s.db.ExecContext(ctx, updateQuery, updateArgs...); err != nil {
			return false, err
		}
	}

	// Upsert Kafka config
	kafkaQuery, kafkaArgs, _ := sq.Insert(metastore.TableRegistryKafka).
		Columns("table_name", "kafka_topic", "kafka_bootstrap_servers", "record_transformer").
		Values(tableName, kafkaTopic, kafkaBootstrapServers, recordTransformer).
		Suffix("ON DUPLICATE KEY UPDATE kafka_topic = VALUES(kafka_topic), " +
			"kafka_bootstrap_servers = VALUES(kafka_bootstrap_servers), " +
			"record_transformer = VALUES(record_transformer)").
		ToSql()
	if _, err := s.db.ExecContext(ctx, kafkaQuery, kafkaArgs...); err != nil {
		return false, err
	}

	created := !exists
	s.log.Info("table registered",
		zap.String("table", tableName),
		zap.Bool("created", created),
	)
	return created, nil
}
