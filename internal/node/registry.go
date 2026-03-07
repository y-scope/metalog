package node

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/metastore"
	schemadef "github.com/y-scope/metalog/schema"
)

// CoordinatorRegistry handles table assignment, heartbeat, and HA operations.
type CoordinatorRegistry struct {
	db        *sql.DB
	nodeID    string
	isMariaDB bool
	log       *zap.Logger
}

// NewCoordinatorRegistry creates a CoordinatorRegistry.
func NewCoordinatorRegistry(db *sql.DB, nodeID string, isMariaDB bool, log *zap.Logger) *CoordinatorRegistry {
	return &CoordinatorRegistry{db: db, nodeID: nodeID, isMariaDB: isMariaDB, log: log}
}

// EnsureSystemTables executes the embedded schema.sql to create all system
// tables (registry, task queue, column registries, template) if they don't exist.
func (r *CoordinatorRegistry) EnsureSystemTables(ctx context.Context) error {
	stmts := splitSQLStatements(schemadef.SQL)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := r.db.ExecContext(ctx, stmt); err != nil {
			// ALTER TABLE ... IF EXISTS/IF NOT EXISTS may fail on fresh installs
			if strings.Contains(stmt, "ALTER TABLE") {
				r.log.Debug("skipping migration statement", zap.Error(err))
				continue
			}
			return fmt.Errorf("ensure system tables: %w", err)
		}
	}
	return nil
}

// splitSQLStatements splits a SQL script into individual statements.
// Handles single-line comments (--) and semicolons inside string literals.
func splitSQLStatements(sqlText string) []string {
	var stmts []string
	var current strings.Builder
	inSingleQuote := false

	lines := strings.Split(sqlText, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !inSingleQuote && strings.HasPrefix(trimmed, "--") {
			continue
		}

		for i := 0; i < len(line); i++ {
			ch := line[i]
			if ch == '\'' {
				if inSingleQuote && i+1 < len(line) && line[i+1] == '\'' {
					// Escaped single quote ('') inside string literal
					current.WriteByte(ch)
					current.WriteByte(ch)
					i++
					continue
				}
				inSingleQuote = !inSingleQuote
				current.WriteByte(ch)
			} else if ch == ';' && !inSingleQuote {
				stmt := strings.TrimSpace(current.String())
				if stmt != "" {
					stmts = append(stmts, stmt)
				}
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		}
		current.WriteByte('\n')
	}

	// Remaining content after last semicolon
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		stmts = append(stmts, stmt)
	}
	return stmts
}

// ValidateSchemaReady checks that all required system tables exist.
// Call after EnsureSystemTables to verify the schema is in the expected state.
func (r *CoordinatorRegistry) ValidateSchemaReady(ctx context.Context) error {
	requiredTables := []string{
		metastore.TableRegistry,
		metastore.TableRegistryKafka,
		metastore.TableRegistryAssignment,
		metastore.TableRegistryConfig,
		metastore.DimRegistryTable,
		metastore.AggRegistryTable,
		metastore.SketchRegistryTable,
		metastore.NodeRegistryTable,
	}
	for _, tbl := range requiredTables {
		var count int
		err := r.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
			tbl,
		).Scan(&count)
		if err != nil {
			return fmt.Errorf("validate schema: check %s: %w", tbl, err)
		}
		if count == 0 {
			return fmt.Errorf("validate schema: required table %s not found", tbl)
		}
	}
	r.log.Info("schema validation passed", zap.Int("tables", len(requiredTables)))
	return nil
}

// UpsertTables registers tables from the config into the registry.
// INSERT ... ON DUPLICATE KEY UPDATE is MySQL-specific and not supported by squirrel.
// Note: VALUES() in ON DUPLICATE KEY UPDATE is deprecated in MySQL 8.0.20+ but
// fully supported in MariaDB 10.6+ (our target). If migrating to MySQL 8.0.20+,
// use the alias form: INSERT INTO ... AS new_row ... ON DUPLICATE KEY UPDATE col = new_row.col.
func (r *CoordinatorRegistry) UpsertTables(ctx context.Context, tables []config.TableConfig) error {
	for _, t := range tables {
		var tableUpsert string
		if r.isMariaDB {
			tableUpsert = "INSERT INTO " + metastore.TableRegistry + " (table_name, display_name) VALUES (?, ?) " +
				"ON DUPLICATE KEY UPDATE display_name = COALESCE(VALUES(display_name), display_name)"
		} else {
			tableUpsert = "INSERT INTO " + metastore.TableRegistry + " (table_name, display_name) VALUES (?, ?) AS new " +
				"ON DUPLICATE KEY UPDATE display_name = COALESCE(new.display_name, display_name)"
		}
		_, err := r.db.ExecContext(ctx, tableUpsert, t.Name, t.DisplayName)
		if err != nil {
			return fmt.Errorf("upsert table %s: %w", t.Name, err)
		}

		_, err = r.db.ExecContext(ctx,
			"INSERT IGNORE INTO "+metastore.TableRegistryAssignment+" (table_name) VALUES (?)",
			t.Name,
		)
		if err != nil {
			return fmt.Errorf("ensure assignment %s: %w", t.Name, err)
		}

		if t.Kafka.Topic != "" {
			var kafkaUpsert string
			if r.isMariaDB {
				kafkaUpsert = "INSERT INTO " + metastore.TableRegistryKafka +
					" (table_name, kafka_topic, kafka_bootstrap_servers, record_transformer) VALUES (?, ?, ?, ?) " +
					"ON DUPLICATE KEY UPDATE kafka_topic = VALUES(kafka_topic), " +
					"kafka_bootstrap_servers = VALUES(kafka_bootstrap_servers), " +
					"record_transformer = VALUES(record_transformer)"
			} else {
				kafkaUpsert = "INSERT INTO " + metastore.TableRegistryKafka +
					" (table_name, kafka_topic, kafka_bootstrap_servers, record_transformer) VALUES (?, ?, ?, ?) AS new " +
					"ON DUPLICATE KEY UPDATE kafka_topic = new.kafka_topic, " +
					"kafka_bootstrap_servers = new.kafka_bootstrap_servers, " +
					"record_transformer = new.record_transformer"
			}
			_, err = r.db.ExecContext(ctx, kafkaUpsert, t.Name, t.Kafka.Topic, t.Kafka.BootstrapServers, t.Kafka.RecordTransformer)
			if err != nil {
				return fmt.Errorf("upsert kafka config %s: %w", t.Name, err)
			}
		}
	}
	return nil
}

// ClaimTable attempts to claim an unassigned table for this node.
func (r *CoordinatorRegistry) ClaimTable(ctx context.Context, tableName string) (bool, error) {
	now := time.Now().Unix()
	query, args, err := sq.Update(metastore.TableRegistryAssignment).
		Set("node_id", r.nodeID).
		Set("node_assigned_at", now).
		Where(sq.Eq{"table_name": tableName}).
		Where("node_id IS NULL").
		ToSql()
	if err != nil {
		return false, fmt.Errorf("build claim query: %w", err)
	}

	res, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("claim table: %w", err)
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		r.log.Info("claimed table", zap.String("table", tableName))
	}
	return n > 0, nil
}

// GetAssignedTables returns tables assigned to this node.
func (r *CoordinatorRegistry) GetAssignedTables(ctx context.Context) ([]string, error) {
	query, args, err := sq.Select("table_name").
		From(metastore.TableRegistryAssignment).
		Where(sq.Eq{"node_id": r.nodeID}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build assigned query: %w", err)
	}
	return r.scanTableNames(ctx, query, args...)
}

// GetUnassignedTables returns tables not claimed by any node.
func (r *CoordinatorRegistry) GetUnassignedTables(ctx context.Context) ([]string, error) {
	query, args, err := sq.Select("table_name").
		From(metastore.TableRegistryAssignment).
		Where("node_id IS NULL").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build unassigned query: %w", err)
	}
	return r.scanTableNames(ctx, query, args...)
}

// GetAllRegisteredTables returns all table names in the registry.
func (r *CoordinatorRegistry) GetAllRegisteredTables(ctx context.Context) ([]string, error) {
	query, args, err := sq.Select("table_name").
		From(metastore.TableRegistry).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build all tables query: %w", err)
	}
	return r.scanTableNames(ctx, query, args...)
}

// ReleaseTable releases a table assignment from this node.
func (r *CoordinatorRegistry) ReleaseTable(ctx context.Context, tableName string) error {
	query, args, err := sq.Update(metastore.TableRegistryAssignment).
		Set("node_id", nil).
		Set("node_assigned_at", nil).
		Where(sq.Eq{"table_name": tableName, "node_id": r.nodeID}).
		ToSql()
	if err != nil {
		return fmt.Errorf("build release query: %w", err)
	}
	_, err = r.db.ExecContext(ctx, query, args...)
	return err
}

// ReleaseAllTables releases all tables assigned to this node.
func (r *CoordinatorRegistry) ReleaseAllTables(ctx context.Context) error {
	query, args, err := sq.Update(metastore.TableRegistryAssignment).
		Set("node_id", nil).
		Set("node_assigned_at", nil).
		Where(sq.Eq{"node_id": r.nodeID}).
		ToSql()
	if err != nil {
		return fmt.Errorf("build release-all query: %w", err)
	}
	_, err = r.db.ExecContext(ctx, query, args...)
	return err
}

// SendHeartbeat updates the heartbeat timestamp for this node.
// INSERT ... ON DUPLICATE KEY UPDATE is MySQL-specific and not supported by squirrel.
func (r *CoordinatorRegistry) SendHeartbeat(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx,
		"INSERT INTO "+metastore.NodeRegistryTable+" (node_id, last_heartbeat_at) VALUES (?, UNIX_TIMESTAMP()) "+
			"ON DUPLICATE KEY UPDATE last_heartbeat_at = UNIX_TIMESTAMP()",
		r.nodeID,
	)
	return err
}

// ClaimOrphansHeartbeat claims tables from nodes whose heartbeat is stale.
// Uses LEFT JOIN to also catch nodes that never registered (crashed before first heartbeat).
func (r *CoordinatorRegistry) ClaimOrphansHeartbeat(ctx context.Context, deadThresholdSeconds int) ([]string, error) {
	// Find orphans: owner is dead (stale heartbeat) or never registered (LEFT JOIN NULL)
	query := "SELECT a.table_name, a.node_id FROM " + metastore.TableRegistryAssignment + " a " +
		"JOIN " + metastore.TableRegistry + " t ON a.table_name = t.table_name " +
		"LEFT JOIN " + metastore.NodeRegistryTable + " n ON a.node_id = n.node_id " +
		"WHERE t.active = true AND a.node_id IS NOT NULL AND a.node_id != ? " +
		"AND (n.node_id IS NULL OR n.last_heartbeat_at < UNIX_TIMESTAMP() - ?)"

	rows, err := r.db.QueryContext(ctx, query, r.nodeID, deadThresholdSeconds)
	if err != nil {
		return nil, fmt.Errorf("find heartbeat orphans: %w", err)
	}
	defer rows.Close()

	type orphan struct {
		tableName string
		deadOwner string
	}
	var orphans []orphan
	for rows.Next() {
		var o orphan
		if err := rows.Scan(&o.tableName, &o.deadOwner); err != nil {
			return nil, fmt.Errorf("scan orphan: %w", err)
		}
		orphans = append(orphans, o)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Claim each orphan with compare-and-swap on dead owner's node_id
	var claimed []string
	for _, o := range orphans {
		now := time.Now().Unix()
		res, err := r.db.ExecContext(ctx,
			"UPDATE "+metastore.TableRegistryAssignment+
				" SET node_id = ?, node_assigned_at = ?, assignment_updated_at = ?"+
				" WHERE table_name = ? AND node_id = ?",
			r.nodeID, now, now, o.tableName, o.deadOwner,
		)
		if err != nil {
			r.log.Warn("failed to claim orphan", zap.String("table", o.tableName), zap.Error(err))
			continue
		}
		if n, _ := res.RowsAffected(); n > 0 {
			r.log.Info("claimed orphan table (heartbeat)", zap.String("table", o.tableName),
				zap.String("deadOwner", o.deadOwner))
			claimed = append(claimed, o.tableName)
		}
	}
	return claimed, nil
}

// ClaimOrphansLease claims tables whose lease has expired or is NULL.
func (r *CoordinatorRegistry) ClaimOrphansLease(ctx context.Context, leaseTTLSeconds int) ([]string, error) {
	query := "SELECT a.table_name FROM " + metastore.TableRegistryAssignment + " a " +
		"JOIN " + metastore.TableRegistry + " t ON a.table_name = t.table_name " +
		"WHERE t.active = true AND a.node_id IS NOT NULL AND a.node_id != ? " +
		"AND (a.lease_expiry < UNIX_TIMESTAMP() OR a.lease_expiry IS NULL)"

	orphans, err := r.scanTableNames(ctx, query, r.nodeID)
	if err != nil {
		return nil, fmt.Errorf("find lease orphans: %w", err)
	}

	var claimed []string
	now := time.Now().Unix()
	for _, t := range orphans {
		res, err := r.db.ExecContext(ctx,
			"UPDATE "+metastore.TableRegistryAssignment+
				" SET node_id = ?, node_assigned_at = ?, assignment_updated_at = ?, lease_expiry = ?"+
				" WHERE table_name = ? AND (lease_expiry < UNIX_TIMESTAMP() OR lease_expiry IS NULL)",
			r.nodeID, now, now, now+int64(leaseTTLSeconds), t,
		)
		if err != nil {
			r.log.Warn("failed to claim orphan", zap.String("table", t), zap.Error(err))
			continue
		}
		if n, _ := res.RowsAffected(); n > 0 {
			r.log.Info("claimed orphan table (lease)", zap.String("table", t))
			claimed = append(claimed, t)
		}
	}
	return claimed, nil
}

// RenewLeases extends lease_expiry for all tables owned by this node.
func (r *CoordinatorRegistry) RenewLeases(ctx context.Context, leaseTTLSeconds int) error {
	_, err := r.db.ExecContext(ctx,
		"UPDATE "+metastore.TableRegistryAssignment+
			" SET lease_expiry = UNIX_TIMESTAMP() + ? WHERE node_id = ?",
		leaseTTLSeconds, r.nodeID,
	)
	if err != nil {
		return fmt.Errorf("renew leases: %w", err)
	}
	return nil
}

// CountActiveNodes returns the number of nodes with a recent heartbeat.
func (r *CoordinatorRegistry) CountActiveNodes(ctx context.Context, deadThresholdSeconds int) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+metastore.NodeRegistryTable+
			" WHERE last_heartbeat_at >= UNIX_TIMESTAMP() - ?",
		deadThresholdSeconds,
	).Scan(&count)
	return count, err
}

// CountAssignedTables returns the number of tables assigned to any node.
func (r *CoordinatorRegistry) CountAssignedTables(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+metastore.TableRegistryAssignment+" WHERE node_id IS NOT NULL",
	).Scan(&count)
	return count, err
}

// CountMyTables returns the number of tables assigned to this node.
func (r *CoordinatorRegistry) CountMyTables(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+metastore.TableRegistryAssignment+" WHERE node_id = ?",
		r.nodeID,
	).Scan(&count)
	return count, err
}

// UpdateProgress writes last_progress_at for operator visibility.
func (r *CoordinatorRegistry) UpdateProgress(ctx context.Context, tableName string) error {
	_, err := r.db.ExecContext(ctx,
		"UPDATE "+metastore.TableRegistryAssignment+
			" SET last_progress_at = UNIX_TIMESTAMP() WHERE table_name = ? AND node_id = ?",
		tableName, r.nodeID,
	)
	return err
}

// scanTableNames is a helper that executes a query and scans a single string column.
func (r *CoordinatorRegistry) scanTableNames(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}
