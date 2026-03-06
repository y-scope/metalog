package coordinator

import (
	"context"
	"database/sql"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/db"
	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/schema"
)

// TableRegistration handles registering new tables at runtime.
type TableRegistration struct {
	db                   *sql.DB
	isMariaDB            bool
	compressionOverride  string
	log                  *zap.Logger
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
	var exists bool
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) > 0 FROM "+metastore.TableRegistry+" WHERE table_name = ?",
		tableName,
	).Scan(&exists)
	if err != nil {
		return false, err
	}

	// Provision table
	if err := schema.EnsureTable(ctx, s.db, tableName, s.isMariaDB, s.compressionOverride, s.log); err != nil {
		return false, err
	}

	// Update display name if provided
	if displayName != "" {
		_, err := s.db.ExecContext(ctx,
			"UPDATE "+metastore.TableRegistry+" SET display_name = ? WHERE table_name = ?",
			displayName, tableName,
		)
		if err != nil {
			return false, err
		}
	}

	// Upsert Kafka config
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO "+metastore.TableRegistryKafka+
			" (table_name, kafka_topic, kafka_bootstrap_servers, record_transformer) VALUES (?, ?, ?, ?)"+
			" ON DUPLICATE KEY UPDATE kafka_topic = VALUES(kafka_topic), kafka_bootstrap_servers = VALUES(kafka_bootstrap_servers), record_transformer = VALUES(record_transformer)",
		tableName, kafkaTopic, kafkaBootstrapServers, recordTransformer,
	)
	if err != nil {
		return false, err
	}

	created := !exists
	s.log.Info("table registered",
		zap.String("table", tableName),
		zap.Bool("created", created),
	)
	return created, nil
}
