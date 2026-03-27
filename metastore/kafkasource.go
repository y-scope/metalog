package metastore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/timeutil"
)

// KafkaSource represents a Kafka ingestion source for a table.
// Sources are first-class entities, decoupled from table config —
// multiple sources can target the same table (e.g., one per region).
type KafkaSource struct {
	TableName         string
	SourceName          string
	Topic             string
	BootstrapServers  string
	RecordTransformer string
	ConsumerGroupID           string
	RequiredEnv          string // "KEY=VALUE,KEY=VALUE" AND semantics; empty = any node
}

// ParseRequiredEnv parses an required_env string into key-value pairs.
// Returns nil for empty strings (no gate).
func ParseRequiredEnv(requiredEnv string) map[string]string {
	if requiredEnv == "" {
		return nil
	}
	result := make(map[string]string)
	for _, pair := range strings.Split(requiredEnv, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) == 2 && parts[0] != "" {
			result[parts[0]] = parts[1]
		}
	}
	return result
}

// ValidateRequiredEnv checks that required_env is well-formed. Returns an error if
// the string is non-empty but contains no valid KEY=VALUE pairs (e.g., a typo
// like "REGION_us-east" would silently match all nodes without this check).
func ValidateRequiredEnv(requiredEnv string) error {
	if requiredEnv == "" {
		return nil
	}
	parsed := ParseRequiredEnv(requiredEnv)
	if len(parsed) == 0 {
		return fmt.Errorf("required_env %q contains no valid KEY=VALUE pairs", requiredEnv)
	}
	return nil
}

// MatchesEnv returns true if the current process's environment satisfies
// all conditions in the required_env string. An empty required_env matches any node.
func MatchesEnv(requiredEnv string) bool {
	conditions := ParseRequiredEnv(requiredEnv)
	if conditions == nil {
		return true
	}
	for k, v := range conditions {
		if os.Getenv(k) != v {
			return false
		}
	}
	return true
}

// KafkaSourceStore provides CRUD operations for the _kafka_source table.
type KafkaSourceStore struct {
	db  *sql.DB
	log *zap.Logger
}

// NewKafkaSourceStore creates a KafkaSourceStore.
func NewKafkaSourceStore(db *sql.DB, log *zap.Logger) *KafkaSourceStore {
	return &KafkaSourceStore{db: db, log: log}
}

// Register inserts a new Kafka source and seeds a corresponding
// _kafka_assignment row (unclaimed). Returns true if a new source was
// created, false if it already existed (idempotent).
func (s *KafkaSourceStore) Register(ctx context.Context, src *KafkaSource) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("register kafka source: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	now := timeutil.EpochNanos()

	// INSERT IGNORE: if the (table_name, source_name) PK already exists, skip.
	insertSQL := fmt.Sprintf(
		"INSERT IGNORE INTO %s (table_name, source_name, topic, bootstrap_servers, record_transformer, consumer_group_id, required_env, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		KafkaSourceTable)
	res, err := tx.ExecContext(ctx, insertSQL,
		src.TableName, src.SourceName, src.Topic, src.BootstrapServers,
		src.RecordTransformer, nullIfEmpty(src.ConsumerGroupID), nullIfEmpty(src.RequiredEnv), now)
	if err != nil {
		return false, fmt.Errorf("register kafka source: insert: %w", err)
	}
	affected, _ := res.RowsAffected()

	// Seed assignment row (unclaimed) — also idempotent.
	assignSQL := fmt.Sprintf(
		"INSERT IGNORE INTO %s (table_name, source_name) VALUES (?, ?)",
		KafkaAssignmentTable)
	if _, err := tx.ExecContext(ctx, assignSQL, src.TableName, src.SourceName); err != nil {
		return false, fmt.Errorf("register kafka source: seed assignment: %w", err)
	}

	return affected > 0, tx.Commit()
}

// Delete removes a Kafka source and its assignment row (CASCADE).
func (s *KafkaSourceStore) Delete(ctx context.Context, tableName, sourceName string) error {
	query, args, _ := sq.Delete(KafkaSourceTable).
		Where(sq.Eq{"table_name": tableName, "source_name": sourceName}).
		ToSql()
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("delete kafka source: %w", err)
	}
	return nil
}

// ListSources returns all Kafka sources for a table.
func (s *KafkaSourceStore) ListSources(ctx context.Context, tableName string) ([]*KafkaSource, error) {
	query, args, _ := sq.Select("table_name", "source_name", "topic", "bootstrap_servers",
		"record_transformer", "consumer_group_id", "required_env").
		From(KafkaSourceTable).
		Where(sq.Eq{"table_name": tableName}).
		ToSql()
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list kafka sources: %w", err)
	}
	defer rows.Close() //nolint:errcheck // cleanup

	return scanSources(rows)
}

// ListAllSources returns all Kafka sources across all tables.
func (s *KafkaSourceStore) ListAllSources(ctx context.Context) ([]*KafkaSource, error) {
	query, args, _ := sq.Select("table_name", "source_name", "topic", "bootstrap_servers",
		"record_transformer", "consumer_group_id", "required_env").
		From(KafkaSourceTable).
		ToSql()
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list all kafka sources: %w", err)
	}
	defer rows.Close() //nolint:errcheck // cleanup

	return scanSources(rows)
}

func scanSources(rows *sql.Rows) ([]*KafkaSource, error) {
	var sources []*KafkaSource
	for rows.Next() {
		src := &KafkaSource{}
		var cgID, requiredEnv sql.NullString
		var transformer sql.NullString
		if err := rows.Scan(&src.TableName, &src.SourceName, &src.Topic, &src.BootstrapServers,
			&transformer, &cgID, &requiredEnv); err != nil {
			return nil, err
		}
		src.RecordTransformer = transformer.String
		src.ConsumerGroupID = cgID.String
		src.RequiredEnv = requiredEnv.String
		sources = append(sources, src)
	}
	return sources, rows.Err()
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
