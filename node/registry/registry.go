// Package registry handles table assignment, node liveness, and HA
// coordination for coordinator nodes. It provides the database operations
// for claiming tables, sending heartbeats or renewing leases, detecting
// dead nodes, and reclaiming orphaned table assignments.
package registry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	db "github.com/y-scope/metalog/db"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/schema"
)

// Registry handles table assignment, heartbeat, and HA operations.
type Registry struct {
	db        *sql.DB
	log       *zap.Logger
	nodeID    string
	isMariaDB bool
}

// New creates a Registry.
func New(db *sql.DB, nodeID string, isMariaDB bool, log *zap.Logger) *Registry {
	return &Registry{db: db, nodeID: nodeID, isMariaDB: isMariaDB, log: log}
}

// EnsureSystemTables executes the embedded schema.sql to create all system
// tables (registry, task queue, column registries, template) if they don't exist.
func (r *Registry) EnsureSystemTables(ctx context.Context) error {
	stmts := splitSQLStatements(schema.SchemaSQL)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		// Strip MariaDB-specific IF [NOT] EXISTS from ALTER TABLE statements
		// for MySQL compatibility. The idempotent error handler below catches
		// the equivalent MySQL errors (1060, 1091).
		if !r.isMariaDB && strings.Contains(stmt, "ALTER TABLE") {
			stmt = strings.ReplaceAll(stmt, " IF NOT EXISTS", "")
			stmt = strings.ReplaceAll(stmt, " IF EXISTS", "")
		}
		if _, err := r.db.ExecContext(ctx, stmt); err != nil {
			// ALTER TABLE migrations are idempotent — tolerate "already exists"
			// and "can't DROP" errors but propagate real failures (permissions,
			// syntax, type mismatches).
			if strings.Contains(stmt, "ALTER TABLE") && isIdempotentDDLError(err) {
				r.log.Debug("skipping already-applied migration", zap.Error(err))
				continue
			}
			return fmt.Errorf("ensure system tables: %w", err)
		}
	}
	return nil
}

// splitSQLStatements splits a SQL script into individual statements.
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

	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		stmts = append(stmts, stmt)
	}
	return stmts
}

// ValidateSchemaReady checks that all required system tables exist.
func (r *Registry) ValidateSchemaReady(ctx context.Context) error {
	requiredTables := []string{
		metastore.TableRegistry,
		metastore.TableRegistryAssignment,
		metastore.TableRegistryConfig,
		metastore.DimRegistryTable,
		metastore.AggRegistryTable,
		metastore.SketchRegistryTable,
		metastore.NodeRegistryTable,
		metastore.KafkaSourceTable,
		metastore.KafkaAssignmentTable,
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

func (r *Registry) ClaimTable(ctx context.Context, tableName string, leaseTTL time.Duration) (bool, error) {
	now := time.Now().UnixNano()
	update := sq.Update(metastore.TableRegistryAssignment).
		Set("node_id", r.nodeID).
		Set("node_assigned_at", now)
	if leaseTTL > 0 {
		update = update.Set("lease_expiry", now+leaseTTL.Nanoseconds())
	}
	query, args, err := update.
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

func (r *Registry) GetAssignedTables(ctx context.Context) ([]string, error) {
	query, args, err := sq.Select("table_name").
		From(metastore.TableRegistryAssignment).
		Where(sq.Eq{"node_id": r.nodeID}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build assigned query: %w", err)
	}
	return r.scanTableNames(ctx, query, args...)
}

func (r *Registry) GetUnassignedTables(ctx context.Context) ([]string, error) {
	query, args, err := sq.Select("table_name").
		From(metastore.TableRegistryAssignment).
		Where("node_id IS NULL").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build unassigned query: %w", err)
	}
	return r.scanTableNames(ctx, query, args...)
}

func (r *Registry) GetAllRegisteredTables(ctx context.Context) ([]string, error) {
	query, args, err := sq.Select("table_name").
		From(metastore.TableRegistry).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build all tables query: %w", err)
	}
	return r.scanTableNames(ctx, query, args...)
}

func (r *Registry) ReleaseTable(ctx context.Context, tableName string) error {
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

func (r *Registry) ReleaseAllTables(ctx context.Context) error {
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

func (r *Registry) SendHeartbeat(ctx context.Context) error {
	now := time.Now().UnixNano()
	insert := sq.Insert(metastore.NodeRegistryTable).
		Columns("node_id", "last_heartbeat_at", "started_at").
		Values(r.nodeID, now, now)
	if r.isMariaDB {
		insert = insert.Suffix(db.OnDuplicateKeyUpdateValues("last_heartbeat_at"))
	} else {
		insert = insert.Suffix(db.OnDuplicateKeyUpdateAlias("new", "last_heartbeat_at"))
	}
	query, args, _ := insert.ToSql()
	_, err := r.db.ExecContext(ctx, query, args...)
	return err
}

func (r *Registry) ClaimOrphansHeartbeat(ctx context.Context, deadThreshold time.Duration) ([]string, error) {
	cutoff := time.Now().Add(-deadThreshold).UnixNano()
	query := "SELECT a.table_name, a.node_id FROM " + metastore.TableRegistryAssignment + " a " +
		"JOIN " + metastore.TableRegistry + " t ON a.table_name = t.table_name " +
		"LEFT JOIN " + metastore.NodeRegistryTable + " n ON a.node_id = n.node_id " +
		"WHERE t.active = true AND a.node_id IS NOT NULL AND a.node_id != ? " +
		"AND (n.last_heartbeat_at < ? OR (n.node_id IS NULL AND (a.node_assigned_at IS NULL OR a.node_assigned_at < ?)))"

	rows, err := r.db.QueryContext(ctx, query, r.nodeID, cutoff, cutoff)
	if err != nil {
		return nil, fmt.Errorf("find heartbeat orphans: %w", err)
	}
	defer rows.Close() //nolint:errcheck // cleanup

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

	var claimed []string
	for _, o := range orphans {
		now := time.Now().UnixNano()
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

func (r *Registry) ClaimOrphansLease(ctx context.Context, leaseTTL time.Duration) ([]string, error) {
	now := time.Now().UnixNano()
	query := "SELECT a.table_name FROM " + metastore.TableRegistryAssignment + " a " +
		"JOIN " + metastore.TableRegistry + " t ON a.table_name = t.table_name " +
		"WHERE t.active = true AND a.node_id IS NOT NULL AND a.node_id != ? " +
		"AND (a.lease_expiry < ? OR a.lease_expiry IS NULL)"

	orphans, err := r.scanTableNames(ctx, query, r.nodeID, now)
	if err != nil {
		return nil, fmt.Errorf("find lease orphans: %w", err)
	}

	var claimed []string
	for _, t := range orphans {
		claimNow := time.Now().UnixNano()
		claimQuery, claimArgs, _ := sq.Update(metastore.TableRegistryAssignment).
			Set("node_id", r.nodeID).
			Set("node_assigned_at", claimNow).
			Set("assignment_updated_at", claimNow).
			Set("lease_expiry", claimNow+leaseTTL.Nanoseconds()).
			Where(sq.Eq{"table_name": t}).
			Where(sq.Or{sq.Lt{"lease_expiry": claimNow}, sq.Eq{"lease_expiry": nil}}).
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

func (r *Registry) RenewLeases(ctx context.Context, leaseTTL time.Duration) error {
	query, args, err := sq.Update(metastore.TableRegistryAssignment).
		Set("lease_expiry", time.Now().Add(leaseTTL).UnixNano()).
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

func (r *Registry) CountActiveNodesHeartbeat(ctx context.Context, deadThreshold time.Duration) (int, error) {
	cutoff := time.Now().Add(-deadThreshold).UnixNano()
	query, args, _ := sq.Select("COUNT(*)").
		From(metastore.NodeRegistryTable).
		Where(sq.GtOrEq{"last_heartbeat_at": cutoff}).
		ToSql()
	var count int
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

func (r *Registry) CountActiveNodesLease(ctx context.Context) (int, error) {
	now := time.Now().UnixNano()
	query, args, _ := sq.Select("COUNT(DISTINCT node_id)").
		From(metastore.TableRegistryAssignment).
		Where("node_id IS NOT NULL").
		Where(sq.GtOrEq{"lease_expiry": now}).
		ToSql()
	var count int
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

func (r *Registry) CountAssignedTables(ctx context.Context) (int, error) {
	query, args, _ := sq.Select("COUNT(*)").
		From(metastore.TableRegistryAssignment).
		Where("node_id IS NOT NULL").
		ToSql()
	var count int
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

func (r *Registry) CountMyTables(ctx context.Context) (int, error) {
	query, args, _ := sq.Select("COUNT(*)").
		From(metastore.TableRegistryAssignment).
		Where(sq.Eq{"node_id": r.nodeID}).
		ToSql()
	var count int
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

func (r *Registry) UpdateProgress(ctx context.Context, tableName string) error {
	query, args, _ := sq.Update(metastore.TableRegistryAssignment).
		Set("last_progress_at", time.Now().UnixNano()).
		Where(sq.Eq{"table_name": tableName, "node_id": r.nodeID}).
		ToSql()
	_, err := r.db.ExecContext(ctx, query, args...)
	return err
}

func (r *Registry) GetTableID(ctx context.Context, tableName string) (string, error) {
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

func (r *Registry) GetTableConfig(ctx context.Context, tableName string) (metastore.TableConfig, error) {
	query, args, _ := sq.Select("config").
		From(metastore.TableRegistryConfig).
		Where(sq.Eq{"table_name": tableName}).
		ToSql()

	var blob []byte
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&blob)
	if err == sql.ErrNoRows {
		return metastore.DefaultTableConfig(), nil
	}
	if err != nil {
		return metastore.TableConfig{}, fmt.Errorf("get table config for %s: %w", tableName, err)
	}
	return metastore.DecodeTableConfig(blob)
}

func (r *Registry) DeregisterNode(ctx context.Context) error {
	query, args, _ := sq.Delete(metastore.NodeRegistryTable).
		Where(sq.Eq{"node_id": r.nodeID}).
		ToSql()
	_, err := r.db.ExecContext(ctx, query, args...)
	return err
}

func (r *Registry) scanTableNames(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // cleanup

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

// --- Kafka source assignment ---

// KafkaSourceAssignment represents a claimed Kafka source.
type KafkaSourceAssignment struct {
	TableName  string
	SourceName string
}

// GetUnclaimedKafkaSources returns Kafka sources available for claiming:
// either node_id IS NULL (never claimed) or lease_expiry has passed
// (previous owner died without releasing).
func (r *Registry) GetUnclaimedKafkaSources(ctx context.Context) ([]*metastore.KafkaSource, error) {
	now := time.Now().UnixNano()
	query, args, _ := sq.Select(
		"ks.table_name", "ks.source_name", "ks.topic", "ks.bootstrap_servers",
		"ks.record_transformer", "ks.consumer_group_id", "ks.required_env",
	).
		From(metastore.KafkaSourceTable + " ks").
		LeftJoin(metastore.KafkaAssignmentTable + " ka USING (table_name, source_name)").
		Where(sq.Or{
			sq.Expr("ka.node_id IS NULL"),
			sq.Lt{"ka.lease_expiry": now},
		}).
		ToSql()

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get unclaimed kafka sources: %w", err)
	}
	defer rows.Close() //nolint:errcheck // cleanup

	var sources []*metastore.KafkaSource
	for rows.Next() {
		src := &metastore.KafkaSource{}
		var cgID, requiredEnv, transformer sql.NullString
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

// ClaimOrphanKafkaSourcesHeartbeat reclaims Kafka sources owned by nodes
// whose heartbeat is stale or whose _node_registry row no longer exists
// (dead nodes in heartbeat mode). Excludes this node's own sources to
// prevent self-reclaim during a brief heartbeat delay.
func (r *Registry) ClaimOrphanKafkaSourcesHeartbeat(ctx context.Context, deadThreshold time.Duration) error {
	now := time.Now().UnixNano()
	cutoff := now - deadThreshold.Nanoseconds()

	// LEFT JOIN: also catches sources whose owning node was deregistered
	// (graceful shutdown completed DeregisterNode but failed to release sources).
	query := fmt.Sprintf(
		"UPDATE %s ka LEFT JOIN %s nr ON ka.node_id = nr.node_id "+
			"SET ka.node_id = NULL, ka.claimed_at = NULL, ka.lease_expiry = NULL "+
			"WHERE ka.node_id IS NOT NULL AND ka.node_id != ? "+
			"AND (nr.last_heartbeat_at < ? OR nr.node_id IS NULL)",
		metastore.KafkaAssignmentTable, metastore.NodeRegistryTable)
	res, err := r.db.ExecContext(ctx, query, r.nodeID, cutoff)
	if err != nil {
		return fmt.Errorf("claim orphan kafka sources heartbeat: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected > 0 {
		r.log.Info("reclaimed orphan kafka sources from dead nodes",
			zap.Int64("count", affected))
	}
	return nil
}

// ClaimKafkaSource attempts to claim a Kafka source for this node.
// Returns true if the claim succeeded (CAS: node_id IS NULL or lease expired → this node).
func (r *Registry) ClaimKafkaSource(ctx context.Context, tableName, sourceName string, leaseTTL time.Duration) (bool, error) {
	now := time.Now().UnixNano()
	update := sq.Update(metastore.KafkaAssignmentTable).
		Set("node_id", r.nodeID).
		Set("claimed_at", now)
	if leaseTTL > 0 {
		update = update.Set("lease_expiry", now+leaseTTL.Nanoseconds())
	}
	query, args, err := update.
		Where(sq.Eq{"table_name": tableName, "source_name": sourceName}).
		Where(sq.Or{
			sq.Expr("node_id IS NULL"),
			sq.Lt{"lease_expiry": now},
		}).
		ToSql()
	if err != nil {
		return false, fmt.Errorf("build kafka claim query: %w", err)
	}

	res, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("claim kafka source: %w", err)
	}
	affected, _ := res.RowsAffected()
	return affected > 0, nil
}

// ReleaseKafkaSource releases a claimed Kafka source.
func (r *Registry) ReleaseKafkaSource(ctx context.Context, tableName, sourceName string) error {
	query, args, _ := sq.Update(metastore.KafkaAssignmentTable).
		Set("node_id", nil).
		Set("claimed_at", nil).
		Set("lease_expiry", nil).
		Where(sq.Eq{"table_name": tableName, "source_name": sourceName, "node_id": r.nodeID}).
		ToSql()
	_, err := r.db.ExecContext(ctx, query, args...)
	return err
}

// GetMyKafkaSources returns all Kafka sources assigned to this node.
func (r *Registry) GetMyKafkaSources(ctx context.Context) ([]KafkaSourceAssignment, error) {
	query, args, _ := sq.Select("table_name", "source_name").
		From(metastore.KafkaAssignmentTable).
		Where(sq.Eq{"node_id": r.nodeID}).
		ToSql()

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get my kafka sources: %w", err)
	}
	defer rows.Close() //nolint:errcheck // cleanup

	var assignments []KafkaSourceAssignment
	for rows.Next() {
		var a KafkaSourceAssignment
		if err := rows.Scan(&a.TableName, &a.SourceName); err != nil {
			return nil, err
		}
		assignments = append(assignments, a)
	}
	return assignments, rows.Err()
}

// GetMyKafkaSourceConfigs returns the full KafkaSource config for a specific source
// assigned to this node. Used to restart units after crash recovery.
func (r *Registry) GetMyKafkaSourceConfigs(ctx context.Context, tableName, sourceName string) ([]*metastore.KafkaSource, error) {
	query, args, _ := sq.Select(
		"ks.table_name", "ks.source_name", "ks.topic", "ks.bootstrap_servers",
		"ks.record_transformer", "ks.consumer_group_id", "ks.required_env",
	).
		From(metastore.KafkaSourceTable + " ks").
		Join(metastore.KafkaAssignmentTable + " ka USING (table_name, source_name)").
		Where(sq.Eq{"ka.node_id": r.nodeID, "ks.table_name": tableName, "ks.source_name": sourceName}).
		ToSql()

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get kafka source config: %w", err)
	}
	defer rows.Close() //nolint:errcheck // cleanup

	var sources []*metastore.KafkaSource
	for rows.Next() {
		src := &metastore.KafkaSource{}
		var cgID, requiredEnv, transformer sql.NullString
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

// RenewKafkaSourceLeases extends lease_expiry for all Kafka sources owned by this node.
func (r *Registry) RenewKafkaSourceLeases(ctx context.Context, ttl time.Duration) error {
	now := time.Now().UnixNano()
	query, args, _ := sq.Update(metastore.KafkaAssignmentTable).
		Set("lease_expiry", now+ttl.Nanoseconds()).
		Where(sq.Eq{"node_id": r.nodeID}).
		ToSql()
	_, err := r.db.ExecContext(ctx, query, args...)
	return err
}

// isIdempotentDDLError returns true for MySQL/MariaDB errors that indicate
// a DDL migration was already applied (column/index already exists, or
// column/index to drop doesn't exist).
func isIdempotentDDLError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	switch mysqlErr.Number {
	case 1060: // ER_DUP_FIELDNAME — duplicate column name
		return true
	case 1061: // ER_DUP_KEYNAME — duplicate key name
		return true
	case 1091: // ER_CANT_DROP_FIELD_OR_KEY — can't DROP; check that column/key exists
		return true
	default:
		return false
	}
}
