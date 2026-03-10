package coordinator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

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

// RegisterTableOpts holds optional fields for RegisterTable.
// Nil pointers mean "don't update" (keep DB default or existing value).
type RegisterTableOpts struct {
	KafkaPollerEnabled   *bool
	ConsolidationEnabled *bool
	ConfigJSON           *string // JSON blob to merge into the config
}

// RegisterTable provisions a table and upserts its Kafka config.
// Returns (created bool, err error). created is true if the table was newly provisioned.
func (s *TableRegistration) RegisterTable(
	ctx context.Context,
	tableName, displayName string,
	kafkaTopic, kafkaBootstrapServers, recordTransformer string,
	opts RegisterTableOpts,
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

	// Upsert Kafka config only when Kafka settings were provided.
	// Skipping prevents clobbering existing config on re-registration without Kafka.
	if kafkaTopic != "" {
		kafkaInsert := sq.Insert(metastore.TableRegistryKafka).
			Columns("table_name", "kafka_topic", "kafka_bootstrap_servers", "record_transformer").
			Values(tableName, kafkaTopic, kafkaBootstrapServers, recordTransformer)
		kafkaCols := []string{"kafka_topic", "kafka_bootstrap_servers", "record_transformer"}
		if s.isMariaDB {
			kafkaInsert = kafkaInsert.Suffix(db.OnDuplicateKeyUpdateValues(kafkaCols...))
		} else {
			kafkaInsert = kafkaInsert.Suffix(db.OnDuplicateKeyUpdateAlias("new", kafkaCols...))
		}
		kafkaQuery, kafkaArgs, _ := kafkaInsert.ToSql()
		if _, err := s.db.ExecContext(ctx, kafkaQuery, kafkaArgs...); err != nil {
			return false, err
		}
	}

	// Update table config blob if any config fields are explicitly set.
	if err := s.updateTableConfig(ctx, tableName, opts); err != nil {
		return false, fmt.Errorf("update table config: %w", err)
	}

	created := !exists
	s.log.Info("table registered",
		zap.String("table", tableName),
		zap.Bool("created", created),
	)
	return created, nil
}

// updateTableConfig performs a read-modify-write on the config blob.
// Only fields explicitly set in opts are overridden.
func (s *TableRegistration) updateTableConfig(ctx context.Context, tableName string, opts RegisterTableOpts) error {
	needsUpdate := opts.KafkaPollerEnabled != nil || opts.ConsolidationEnabled != nil || opts.ConfigJSON != nil
	if !needsUpdate {
		return nil
	}

	// Read existing config blob.
	query, args, _ := sq.Select("config").
		From(metastore.TableRegistryConfig).
		Where(sq.Eq{"table_name": tableName}).
		ToSql()
	var blob []byte
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&blob); err != nil && err != sql.ErrNoRows {
		return err
	}

	cfg, err := metastore.DecodeTableConfig(blob)
	if err != nil {
		return err
	}

	// Apply overrides from individual fields.
	if opts.KafkaPollerEnabled != nil {
		cfg.KafkaPollerEnabled = *opts.KafkaPollerEnabled
	}
	if opts.ConsolidationEnabled != nil {
		cfg.ConsolidationEnabled = *opts.ConsolidationEnabled
	}

	// Apply overrides from JSON blob (contains full config including policies).
	if opts.ConfigJSON != nil {
		if err := json.Unmarshal([]byte(*opts.ConfigJSON), &cfg); err != nil {
			return fmt.Errorf("parse config_json: %w", err)
		}
	}

	// Encode and write back.
	newBlob, err := metastore.EncodeTableConfig(cfg)
	if err != nil {
		return err
	}

	updateQuery, updateArgs, _ := sq.Update(metastore.TableRegistryConfig).
		Set("config", newBlob).
		Where(sq.Eq{"table_name": tableName}).
		ToSql()
	_, err = s.db.ExecContext(ctx, updateQuery, updateArgs...)
	return err
}
