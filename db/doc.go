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
