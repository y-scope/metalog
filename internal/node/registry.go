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
	db "github.com/y-scope/metalog/internal/db"
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
// Note: VALUES() in ON DUPLICATE KEY UPDATE is deprecated in MySQL 8.0.20+ but
// fully supported in MariaDB 10.6+ (our target). If migrating to MySQL 8.0.20+,
// use the alias form: INSERT INTO ... AS new ON DUPLICATE KEY UPDATE col = new.col.
func (r *CoordinatorRegistry) UpsertTables(ctx context.Context, tables []config.TableConfig) error {
	for _, t := range tables {
		tableInsert := sq.Insert(metastore.TableRegistry).
			Columns("table_name", "display_name").
			Values(t.Name, t.DisplayName)
		if r.isMariaDB {
			tableInsert = tableInsert.Suffix("ON DUPLICATE KEY UPDATE display_name = COALESCE(VALUES(display_name), display_name)")
		} else {
			tableInsert = tableInsert.Suffix("AS new ON DUPLICATE KEY UPDATE display_name = COALESCE(new.display_name, display_name)")
		}
		query, args, _ := tableInsert.ToSql()
		if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("upsert table %s: %w", t.Name, err)
		}

		assignQuery, assignArgs, _ := sq.Insert(metastore.TableRegistryAssignment).Options("IGNORE").
			Columns("table_name").
			Values(t.Name).
			ToSql()
		if _, err := r.db.ExecContext(ctx, assignQuery, assignArgs...); err != nil {
			return fmt.Errorf("ensure assignment %s: %w", t.Name, err)
		}

		if t.Kafka.Topic != "" {
			kafkaInsert := sq.Insert(metastore.TableRegistryKafka).
				Columns("table_name", "kafka_topic", "kafka_bootstrap_servers", "record_transformer").
				Values(t.Name, t.Kafka.Topic, t.Kafka.BootstrapServers, t.Kafka.RecordTransformer)
			kafkaCols := []string{"kafka_topic", "kafka_bootstrap_servers", "record_transformer"}
			if r.isMariaDB {
				kafkaInsert = kafkaInsert.Suffix(db.OnDuplicateKeyUpdateValues(kafkaCols...))
			} else {
				kafkaInsert = kafkaInsert.Suffix(db.OnDuplicateKeyUpdateAlias("new", kafkaCols...))
			}
			kQuery, kArgs, _ := kafkaInsert.ToSql()
			if _, err := r.db.ExecContext(ctx, kQuery, kArgs...); err != nil {
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
func (r *CoordinatorRegistry) SendHeartbeat(ctx context.Context) error {
	query, args, _ := sq.Insert(metastore.NodeRegistryTable).
		Columns("node_id", "last_heartbeat_at").
		Values(r.nodeID, sq.Expr("UNIX_TIMESTAMP()")).
		Suffix("ON DUPLICATE KEY UPDATE last_heartbeat_at = UNIX_TIMESTAMP()").
		ToSql()
	_, err := r.db.ExecContext(ctx, query, args...)
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
		claimQuery, claimArgs, _ := sq.Update(metastore.TableRegistryAssignment).
			Set("node_id", r.nodeID).
			Set("node_assigned_at", now).
			Set("assignment_updated_at", now).
			Where(sq.Eq{"table_name": o.tableName, "node_id": o.deadOwner}).
			ToSql()
		res, err := r.db.ExecContext(ctx, claimQuery, claimArgs...)
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
		claimQuery, claimArgs, _ := sq.Update(metastore.TableRegistryAssignment).
			Set("node_id", r.nodeID).
			Set("node_assigned_at", now).
			Set("assignment_updated_at", now).
			Set("lease_expiry", now+int64(leaseTTLSeconds)).
			Where(sq.Eq{"table_name": t}).
			Where("(lease_expiry < UNIX_TIMESTAMP() OR lease_expiry IS NULL)").
			ToSql()
		res, err := r.db.ExecContext(ctx, claimQuery, claimArgs...)
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
	query, args, err := sq.Update(metastore.TableRegistryAssignment).
		Set("lease_expiry", sq.Expr("UNIX_TIMESTAMP() + ?", leaseTTLSeconds)).
		Where(sq.Eq{"node_id": r.nodeID}).
		ToSql()
	if err != nil {
		return fmt.Errorf("renew leases: build query: %w", err)
	}
	if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("renew leases: %w", err)
	}
	return nil
}

// CountActiveNodes returns the number of nodes with a recent heartbeat.
func (r *CoordinatorRegistry) CountActiveNodes(ctx context.Context, deadThresholdSeconds int) (int, error) {
	query, args, _ := sq.Select("COUNT(*)").
		From(metastore.NodeRegistryTable).
		Where("last_heartbeat_at >= UNIX_TIMESTAMP() - ?", deadThresholdSeconds).
		ToSql()
	var count int
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

// CountAssignedTables returns the number of tables assigned to any node.
func (r *CoordinatorRegistry) CountAssignedTables(ctx context.Context) (int, error) {
	query, args, _ := sq.Select("COUNT(*)").
		From(metastore.TableRegistryAssignment).
		Where("node_id IS NOT NULL").
		ToSql()
	var count int
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

// CountMyTables returns the number of tables assigned to this node.
func (r *CoordinatorRegistry) CountMyTables(ctx context.Context) (int, error) {
	query, args, _ := sq.Select("COUNT(*)").
		From(metastore.TableRegistryAssignment).
		Where(sq.Eq{"node_id": r.nodeID}).
		ToSql()
	var count int
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

// UpdateProgress writes last_progress_at for operator visibility.
func (r *CoordinatorRegistry) UpdateProgress(ctx context.Context, tableName string) error {
	query, args, _ := sq.Update(metastore.TableRegistryAssignment).
		Set("last_progress_at", sq.Expr("UNIX_TIMESTAMP()")).
		Where(sq.Eq{"table_name": tableName, "node_id": r.nodeID}).
		ToSql()
	_, err := r.db.ExecContext(ctx, query, args...)
	return err
}

// GetTableID fetches the table_id UUID for a given table_name from the _table registry.
// The table_id is used to derive the Kafka consumer group ID, ensuring uniqueness
// across environments (e.g., prod and staging sharing the same Kafka cluster).
func (r *CoordinatorRegistry) GetTableID(ctx context.Context, tableName string) (string, error) {
	query, args, _ := sq.Select("table_id").
		From(metastore.TableRegistry).
		Where(sq.Eq{"table_name": tableName}).
		ToSql()
	var tableID string
	if err := r.db.QueryRowContext(ctx, query, args...).Scan(&tableID); err != nil {
		return "", fmt.Errorf("get table_id for %s: %w", tableName, err)
	}
	return tableID, nil
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
