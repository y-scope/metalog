// Package db provides database connection pooling, transaction helpers, and
// SQL utilities for MySQL and MariaDB.
//
// Key facilities:
//   - [NewPool] creates a configured *sql.DB connection pool
//   - [WithTx] and [WithDeadlockRetry] manage transactions with automatic
//     rollback and deadlock retry with jitter
//   - [DetectDatabaseType] identifies MySQL vs MariaDB at runtime
//   - [ValidateSQLIdentifier] and [QuoteIdentifier] guard against SQL injection
//     in dynamic DDL and queries
package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/y-scope/metalog/config"
)

// NewPool creates a configured *sql.DB connection pool from a DatabaseConfig.
func NewPool(cfg config.DatabaseConfig) (*sql.DB, error) {
	db, err := sql.Open("mysql", cfg.DSN())
	if err != nil {
		return nil, err
	}

	poolSize := cfg.PoolSize
	if poolSize == 0 {
		poolSize = 5
	}
	minIdle := cfg.PoolMinIdle
	if minIdle == 0 {
		minIdle = 2
	}

	db.SetMaxOpenConns(poolSize)
	db.SetMaxIdleConns(minIdle)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return db, nil
}
