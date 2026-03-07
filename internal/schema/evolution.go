package schema

import (
	"context"
	"database/sql"
	"fmt"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/db"
)

// SchemaEvolution handles online DDL for adding new columns to metadata tables.
type SchemaEvolution struct {
	db  *sql.DB
	log *zap.Logger
}

// NewSchemaEvolution creates a SchemaEvolution.
func NewSchemaEvolution(db *sql.DB, log *zap.Logger) *SchemaEvolution {
	return &SchemaEvolution{db: db, log: log}
}

// AddColumn adds a new column to the table using online DDL (ALGORITHM=INPLACE, LOCK=NONE).
func (se *SchemaEvolution) AddColumn(ctx context.Context, tableName, colName, sqlType string) error {
	if err := db.ValidateSQLIdentifier(tableName); err != nil {
		return err
	}
	if err := db.ValidateSQLIdentifier(colName); err != nil {
		return err
	}

	ddl := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NULL, ALGORITHM=INPLACE, LOCK=NONE",
		db.QuoteIdentifier(tableName), db.QuoteIdentifier(colName), sqlType)

	_, err := se.db.ExecContext(ctx, ddl)
	if err != nil {
		return fmt.Errorf("add column %s.%s: %w", tableName, colName, err)
	}

	se.log.Info("added column via online DDL",
		zap.String("table", tableName),
		zap.String("column", colName),
		zap.String("type", sqlType),
	)
	return nil
}
