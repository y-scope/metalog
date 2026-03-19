package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/metastore"
)

// BaseSchemaValidator verifies that system and template tables have the
// expected columns, types, and indexes. Run at startup after
// EnsureSystemTables to catch stale schemas before any data operations.
type BaseSchemaValidator struct {
	db  *sql.DB
	log *zap.Logger
}

// NewBaseSchemaValidator creates a validator.
func NewBaseSchemaValidator(db *sql.DB, log *zap.Logger) *BaseSchemaValidator {
	return &BaseSchemaValidator{db: db, log: log}
}

// columnSpec describes an expected column.
type columnSpec struct {
	name       string
	typePrefix string // matched case-insensitively against COLUMN_TYPE prefix
}

// indexSpec describes an expected index.
type indexSpec struct {
	name   string
	unique bool
}

// templateColumns are the base columns that must exist on _clp_template
// (and every cloned data table). Type prefixes handle dialect differences
// (e.g., MariaDB "bigint(20)" vs MySQL 8.0 "bigint").
var templateColumns = []columnSpec{
	{"id", "bigint"},
	{"min_timestamp", "bigint"},
	{"max_timestamp", "bigint"},
	{"clp_archive_created_at", "bigint"},
	{"clp_ir_storage_backend", "varchar"},
	{"clp_ir_bucket", "varchar"},
	{"clp_ir_path", "varchar"},
	{"clp_archive_storage_backend", "varchar"},
	{"clp_archive_bucket", "varchar"},
	{"clp_archive_path", "varchar"},
	{"clp_ir_path_hash", "binary"},
	{"clp_archive_path_hash", "binary"},
	{"state", "enum"},
	{"record_count", "int"},
	{"raw_size_bytes", "bigint"},
	{"clp_ir_size_bytes", "int"},
	{"clp_archive_size_bytes", "int"},
	{"retention_days", "smallint"},
	{"expires_at", "bigint"},
	{"sketches", "set"},
	{"ext", "mediumblob"},
}

// templateIndexes are the indexes that must exist on _clp_template.
var templateIndexes = []indexSpec{
	{"PRIMARY", false},
	{"idx_id", false},
	{"idx_clp_ir_hash", true},
	{"idx_clp_archive_hash", false},
	{"idx_consolidation", false},
	{"idx_expiration", false},
	{"idx_max_timestamp", false},
}

// systemTableColumns maps system table names to their critical columns.
// Only columns that the Go code reads/writes are listed — missing any of
// these would cause runtime failures.
var systemTableColumns = map[string][]columnSpec{
	metastore.TableRegistry: {
		{"table_id", "char"},
		{"table_name", "varchar"},
		{"active", ""},
	},
	metastore.TableRegistryConfig: {
		{"table_name", "varchar"},
		{"config", "mediumtext"},
	},
	metastore.TableRegistryAssignment: {
		{"table_name", "varchar"},
		{"node_id", "varchar"},
		{"node_assigned_at", "bigint"},
		{"last_progress_at", "bigint"},
		{"lease_expiry", "bigint"},
	},
	metastore.NodeRegistryTable: {
		{"node_id", "varchar"},
		{"last_heartbeat_at", "bigint"},
		{"started_at", "bigint"},
	},
	metastore.DimRegistryTable: {
		{"table_name", "varchar"},
		{"column_name", "varchar"},
		{"base_type", "enum"},
		{"width", "smallint"},
		{"dim_key", "varchar"},
		{"alias_column", "varchar"},
		{"state", "enum"},
		{"created_at", "bigint"},
	},
	metastore.AggRegistryTable: {
		{"table_name", "varchar"},
		{"column_name", "varchar"},
		{"agg_key", "varchar"},
		{"agg_value", "varchar"},
		{"aggregation_type", "enum"},
		{"value_type", "enum"},
		{"alias_column", "varchar"},
		{"state", "enum"},
		{"created_at", "bigint"},
	},
	metastore.SketchRegistryTable: {
		{"table_name", "varchar"},
		{"sketch_name", "varchar"},
		{"state", "enum"},
		{"created_at", "bigint"},
	},
}

// Validate checks all system tables and the template table.
func (v *BaseSchemaValidator) Validate(ctx context.Context) error {
	// Validate system tables.
	for table, cols := range systemTableColumns {
		if err := v.validateColumns(ctx, table, cols); err != nil {
			return fmt.Errorf("system table %s: %w", table, err)
		}
	}

	// Validate template table columns and indexes.
	if err := v.validateColumns(ctx, metastore.TemplateTable, templateColumns); err != nil {
		return fmt.Errorf("template table: %w", err)
	}
	if err := v.validateIndexes(ctx, metastore.TemplateTable, templateIndexes); err != nil {
		return fmt.Errorf("template table: %w", err)
	}

	// Validate template table is partitioned.
	if err := v.validatePartitioned(ctx, metastore.TemplateTable); err != nil {
		return fmt.Errorf("template table: %w", err)
	}

	// Validate sketch slot count: the SET members in the sketches column must
	// match the sketchSlotCount constant used by prepopulateSketchSlots.
	if err := v.validateSketchSlots(ctx, metastore.TemplateTable); err != nil {
		return fmt.Errorf("template table: %w", err)
	}

	v.log.Info("base schema validation passed")
	return nil
}

// validateColumns checks that all expected columns exist with correct type
// prefixes on the given table.
func (v *BaseSchemaValidator) validateColumns(ctx context.Context, tableName string, expected []columnSpec) error {
	rows, err := v.db.QueryContext(ctx,
		"SELECT COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS "+
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? "+
			"ORDER BY ORDINAL_POSITION",
		tableName,
	)
	if err != nil {
		return fmt.Errorf("query columns: %w", err)
	}
	defer rows.Close()

	actual := make(map[string]string) // column_name -> column_type
	for rows.Next() {
		var name, colType string
		if err := rows.Scan(&name, &colType); err != nil {
			return err
		}
		actual[name] = colType
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(actual) == 0 {
		return fmt.Errorf("table %s has no columns (does it exist?)", tableName)
	}

	var missing []string
	var typeMismatch []string
	for _, spec := range expected {
		colType, ok := actual[spec.name]
		if !ok {
			missing = append(missing, spec.name)
			continue
		}
		if spec.typePrefix != "" && !strings.HasPrefix(strings.ToLower(colType), spec.typePrefix) {
			typeMismatch = append(typeMismatch, fmt.Sprintf(
				"%s: want %s*, got %s", spec.name, spec.typePrefix, colType))
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing columns: %s", strings.Join(missing, ", "))
	}
	if len(typeMismatch) > 0 {
		return fmt.Errorf("type mismatches: %s", strings.Join(typeMismatch, "; "))
	}
	return nil
}

// validateIndexes checks that all expected indexes exist on the given table.
func (v *BaseSchemaValidator) validateIndexes(ctx context.Context, tableName string, expected []indexSpec) error {
	rows, err := v.db.QueryContext(ctx,
		"SELECT DISTINCT INDEX_NAME, NON_UNIQUE FROM INFORMATION_SCHEMA.STATISTICS "+
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		tableName,
	)
	if err != nil {
		return fmt.Errorf("query indexes: %w", err)
	}
	defer rows.Close()

	type idxInfo struct {
		unique bool
	}
	actual := make(map[string]idxInfo)
	for rows.Next() {
		var name string
		var nonUnique int
		if err := rows.Scan(&name, &nonUnique); err != nil {
			return err
		}
		actual[name] = idxInfo{unique: nonUnique == 0}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	var missing []string
	var wrongUniqueness []string
	for _, spec := range expected {
		info, ok := actual[spec.name]
		if !ok {
			missing = append(missing, spec.name)
			continue
		}
		if spec.unique && !info.unique {
			wrongUniqueness = append(wrongUniqueness, spec.name+" (expected UNIQUE)")
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing indexes: %s", strings.Join(missing, ", "))
	}
	if len(wrongUniqueness) > 0 {
		v.log.Warn("index uniqueness mismatch", zap.Strings("indexes", wrongUniqueness))
	}
	return nil
}

// validateSketchSlots checks that the SET members in the sketches column match
// sketchSlotCount. This catches drift between the DDL template and the Go constant.
func (v *BaseSchemaValidator) validateSketchSlots(ctx context.Context, tableName string) error {
	var colType string
	err := v.db.QueryRowContext(ctx,
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS "+
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = 'sketches'",
		tableName,
	).Scan(&colType)
	if err != nil {
		return fmt.Errorf("query sketches column type: %w", err)
	}

	// COLUMN_TYPE looks like: set('s01','s02',...,'s64')
	// Count the members by counting commas + 1.
	inner := strings.TrimPrefix(strings.ToLower(colType), "set(")
	inner = strings.TrimSuffix(inner, ")")
	memberCount := strings.Count(inner, ",") + 1

	if memberCount != sketchSlotCount {
		return fmt.Errorf("sketches SET has %d members, expected %d (sketchSlotCount)",
			memberCount, sketchSlotCount)
	}
	return nil
}

// validatePartitioned checks that the table uses RANGE partitioning.
func (v *BaseSchemaValidator) validatePartitioned(ctx context.Context, tableName string) error {
	var method sql.NullString
	err := v.db.QueryRowContext(ctx,
		"SELECT PARTITION_METHOD FROM INFORMATION_SCHEMA.PARTITIONS "+
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? LIMIT 1",
		tableName,
	).Scan(&method)
	if err != nil {
		return fmt.Errorf("query partition method: %w", err)
	}
	if !method.Valid || !strings.HasPrefix(strings.ToUpper(method.String), "RANGE") {
		return fmt.Errorf("expected RANGE partitioning, got %q", method.String)
	}
	return nil
}
