package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.uber.org/zap"

	schemadef "github.com/y-scope/metalog/schema"

	"github.com/y-scope/metalog/internal/db"
	"github.com/y-scope/metalog/internal/metastore"
)

const (
	sketchSlotCount         = 32
	partitionLookaheadDays  = 7
	partitionCleanupAgeDays = 180
)

// EnsureTable idempotently provisions a metadata table and all registry rows.
// Steps: create physical table, insert registry rows, pre-populate sketch slots,
// create lookahead partitions.
// compressionOverride can be "lz4", "none", or "" (auto-detect from isMariaDB).
func EnsureTable(ctx context.Context, database *sql.DB, tableName string, isMariaDB bool, compressionOverride string, log *zap.Logger) error {
	if err := db.ValidateSQLIdentifier(tableName); err != nil {
		return err
	}

	if err := createPhysicalTable(ctx, database, tableName, isMariaDB, compressionOverride); err != nil {
		return err
	}
	if err := insertRegistryRows(ctx, database, tableName); err != nil {
		return err
	}
	if err := prepopulateSketchSlots(ctx, database, tableName); err != nil {
		return err
	}

	log.Info("table provisioned", zap.String("table", tableName))
	return nil
}

func createPhysicalTable(ctx context.Context, database *sql.DB, tableName string, isMariaDB bool, compressionOverride string) error {
	ddl, err := loadTemplateDDL()
	if err != nil {
		return err
	}

	// Determine compression clause
	compression := compressionOverride
	if compression == "" {
		if isMariaDB {
			compression = "page_compressed"
		} else {
			compression = "lz4"
		}
	}

	var compressionClause string
	switch compression {
	case "none":
		compressionClause = ""
	case "page_compressed":
		compressionClause = "\n  PAGE_COMPRESSED=1"
	case "lz4":
		compressionClause = "\n  COMPRESSION='lz4'"
	case "zlib":
		compressionClause = "\n  COMPRESSION='zlib'"
	case "zstd":
		compressionClause = "\n  COMPRESSION='zstd'"
	default:
		return fmt.Errorf("unsupported compression %q: must be none, page_compressed, lz4, zlib, or zstd", compression)
	}

	if compressionClause != "" {
		ddl = strings.Replace(ddl, "COLLATE=utf8mb4_bin", "COLLATE=utf8mb4_bin"+compressionClause, 1)
	}

	// Substitute table name
	ddl = strings.ReplaceAll(ddl, "_clp_template", tableName)

	_, err = database.ExecContext(ctx, ddl)
	if err != nil && !db.IsTableExists(err) {
		return fmt.Errorf("create table %s: %w", tableName, err)
	}
	return nil
}

func loadTemplateDDL() (string, error) {
	content := schemadef.SQL
	// Strip SQL comments
	lines := strings.Split(content, "\n")
	var filtered []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "--") {
			filtered = append(filtered, line)
		}
	}
	content = strings.Join(filtered, "\n")

	// Find the CREATE TABLE IF NOT EXISTS _clp_template statement
	stmts := strings.Split(content, ";")
	for _, stmt := range stmts {
		trimmed := strings.TrimSpace(stmt)
		if strings.Contains(trimmed, "CREATE TABLE IF NOT EXISTS _clp_template") {
			return trimmed, nil
		}
	}

	return "", fmt.Errorf("CREATE TABLE IF NOT EXISTS _clp_template not found in schema.sql")
}

func insertRegistryRows(ctx context.Context, database *sql.DB, tableName string) error {
	// _table: identity row
	_, err := database.ExecContext(ctx,
		"INSERT IGNORE INTO "+metastore.TableRegistry+" (table_name, display_name) VALUES (?, ?)",
		tableName, tableName)
	if err != nil {
		return fmt.Errorf("insert _table: %w", err)
	}

	// _table_config
	_, err = database.ExecContext(ctx,
		"INSERT IGNORE INTO "+metastore.TableRegistryConfig+" (table_name, kafka_poller_enabled) VALUES (?, FALSE)",
		tableName)
	if err != nil {
		return fmt.Errorf("insert _table_config: %w", err)
	}

	// _table_assignment: node_id NULL for fight-for-master
	_, err = database.ExecContext(ctx,
		"INSERT IGNORE INTO "+metastore.TableRegistryAssignment+" (table_name) VALUES (?)",
		tableName)
	if err != nil {
		return fmt.Errorf("insert _table_assignment: %w", err)
	}

	return nil
}

func prepopulateSketchSlots(ctx context.Context, database *sql.DB, tableName string) error {
	for i := 1; i <= sketchSlotCount; i++ {
		member := fmt.Sprintf("s%02d", i)
		_, err := database.ExecContext(ctx,
			"INSERT IGNORE INTO "+metastore.SketchRegistryTable+
				" (table_name, sketch_name, state) VALUES (?, ?, 'AVAILABLE')",
			tableName, member)
		if err != nil {
			return fmt.Errorf("insert sketch slot: %w", err)
		}
	}
	return nil
}
