package schema

import (
	"context"
	"database/sql"
	"fmt"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/db"
)

// Evolver handles online DDL for adding new columns to metadata tables.
type Evolver struct {
	db  *sql.DB
	log *zap.Logger
}

// NewEvolver creates an Evolver.
func NewEvolver(db *sql.DB, log *zap.Logger) *Evolver {
	return &Evolver{db: db, log: log}
}

// AddColumn adds a new column to the table using online DDL (ALGORITHM=INPLACE, LOCK=NONE).
func (se *Evolver) AddColumn(ctx context.Context, tableName, colName, sqlType string) error {
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
