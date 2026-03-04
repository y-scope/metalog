package db

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/y-scope/metalog/internal/config"
)

// NewPool creates a configured *sql.DB connection pool from a DatabaseConfig.
func NewPool(cfg config.DatabaseConfig) (*sql.DB, error) {
	db, err := sql.Open("mysql", cfg.DSN())
	if err != nil {
		return nil, err
	}

	poolSize := cfg.PoolSize
	if poolSize == 0 {
		poolSize = 20
	}
	minIdle := cfg.PoolMinIdle
	if minIdle == 0 {
		minIdle = 5
	}

	db.SetMaxOpenConns(poolSize)
	db.SetMaxIdleConns(minIdle)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)

	return db, nil
}
