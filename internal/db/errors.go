package db

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

// MySQL/MariaDB error codes.
const (
	errDeadlock        = 1213
	errDupEntry        = 1062
	errDupColumn       = 1060
	errLockWaitTimeout = 1205
	errTableExists     = 1050
)

// IsDeadlock returns true if the error is a MySQL deadlock (ER_LOCK_DEADLOCK).
func IsDeadlock(err error) bool {
	return isMySQLError(err, errDeadlock)
}

// IsDuplicateKey returns true if the error is a MySQL duplicate key (ER_DUP_ENTRY).
func IsDuplicateKey(err error) bool {
	return isMySQLError(err, errDupEntry)
}

// IsLockWaitTimeout returns true if the error is a lock wait timeout.
func IsLockWaitTimeout(err error) bool {
	return isMySQLError(err, errLockWaitTimeout)
}

// IsDuplicateColumn returns true if the error is a MySQL duplicate column name (ER_DUP_FIELDNAME).
func IsDuplicateColumn(err error) bool {
	return isMySQLError(err, errDupColumn)
}

// IsTableExists returns true if the error is "table already exists".
func IsTableExists(err error) bool {
	return isMySQLError(err, errTableExists)
}

func isMySQLError(err error, code uint16) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == code
	}
	return false
}
