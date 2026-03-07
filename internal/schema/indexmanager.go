package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/db"
)

// IndexConfig defines a desired index from index.yaml.
type IndexConfig struct {
	Table   string   `yaml:"table"`
	Columns []string `yaml:"columns"`
}

// IndexManager reconciles dynamic indexes on dimension columns.
type IndexManager struct {
	db  *sql.DB
	log *zap.Logger
}

// NewIndexManager creates an IndexManager.
func NewIndexManager(db *sql.DB, log *zap.Logger) *IndexManager {
	return &IndexManager{db: db, log: log}
}

// EnsureIndex creates an index on a dimension column if it doesn't exist.
func (im *IndexManager) EnsureIndex(ctx context.Context, tableName, colName string) error {
	if err := db.ValidateSQLIdentifier(tableName); err != nil {
		return err
	}
	if err := db.ValidateSQLIdentifier(colName); err != nil {
		return err
	}

	indexName := "idx_" + colName

	// Check if index already exists
	var count int
	err := im.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
		tableName, indexName,
	).Scan(&count)
	if err != nil {
		return fmt.Errorf("check index existence: %w", err)
	}
	if count > 0 {
		return nil
	}

	// Create index (online DDL)
	ddl := fmt.Sprintf("ALTER TABLE %s ADD INDEX %s (%s), ALGORITHM=INPLACE, LOCK=NONE",
		db.QuoteIdentifier(tableName), db.QuoteIdentifier(indexName), db.QuoteIdentifier(colName))

	_, err = im.db.ExecContext(ctx, ddl)
	if err != nil {
		return fmt.Errorf("create index %s: %w", indexName, err)
	}

	im.log.Info("created index", zap.String("table", tableName), zap.String("index", indexName))
	return nil
}

// DropIndex removes a dynamic index if it exists.
func (im *IndexManager) DropIndex(ctx context.Context, tableName, colName string) error {
	if err := db.ValidateSQLIdentifier(tableName); err != nil {
		return err
	}
	if err := db.ValidateSQLIdentifier(colName); err != nil {
		return err
	}

	indexName := "idx_" + colName
	ddl := fmt.Sprintf("ALTER TABLE %s DROP INDEX %s",
		db.QuoteIdentifier(tableName), db.QuoteIdentifier(indexName))

	_, err := im.db.ExecContext(ctx, ddl)
	if err != nil {
		// Ignore "index doesn't exist" errors.
		if strings.Contains(err.Error(), "check that it exists") || strings.Contains(err.Error(), "Can't DROP") {
			return nil
		}
		return fmt.Errorf("drop index %s: %w", indexName, err)
	}

	im.log.Info("dropped index", zap.String("table", tableName), zap.String("index", indexName))
	return nil
}

// Reconcile ensures all desired indexes exist and removes stale ones.
// desiredIndexes maps table name to a set of column names that should be indexed.
func (im *IndexManager) Reconcile(ctx context.Context, desired []IndexConfig) error {
	// Build desired set: table -> set of columns
	desiredSet := make(map[string]map[string]bool)
	for _, ic := range desired {
		if _, ok := desiredSet[ic.Table]; !ok {
			desiredSet[ic.Table] = make(map[string]bool)
		}
		for _, col := range ic.Columns {
			desiredSet[ic.Table][col] = true
		}
	}

	// For each table, get existing dynamic indexes and reconcile.
	for tableName, desiredCols := range desiredSet {
		existing, err := im.listDynamicIndexes(ctx, tableName)
		if err != nil {
			im.log.Warn("failed to list indexes", zap.String("table", tableName), zap.Error(err))
			continue
		}

		// Create missing indexes.
		for col := range desiredCols {
			if !existing[col] {
				if err := im.EnsureIndex(ctx, tableName, col); err != nil {
					im.log.Warn("failed to create index", zap.String("table", tableName),
						zap.String("column", col), zap.Error(err))
				}
			}
		}

		// Drop stale indexes.
		for col := range existing {
			if !desiredCols[col] {
				if err := im.DropIndex(ctx, tableName, col); err != nil {
					im.log.Warn("failed to drop index", zap.String("table", tableName),
						zap.String("column", col), zap.Error(err))
				}
			}
		}
	}

	return nil
}

// listDynamicIndexes returns all dim_f/agg_f indexes on a table.
func (im *IndexManager) listDynamicIndexes(ctx context.Context, tableName string) (map[string]bool, error) {
	rows, err := im.db.QueryContext(ctx,
		"SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS "+
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME LIKE 'idx_%'",
		tableName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]bool)
	for rows.Next() {
		var indexName string
		if err := rows.Scan(&indexName); err != nil {
			return nil, err
		}
		// Strip "idx_" prefix to get column name.
		if len(indexName) > 4 {
			col := indexName[4:]
			result[col] = true
		}
	}
	return result, rows.Err()
}
