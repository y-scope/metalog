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
	errSameNamePart    = 1517
	errCantDropKey     = 1091
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

// IsDuplicatePartition returns true if the error is a MySQL duplicate partition
// name (ER_SAME_NAME_PARTITION, error 1517).
func IsDuplicatePartition(err error) bool {
	return isMySQLError(err, errSameNamePart)
}

// IsCantDropKey returns true if the error is "Can't DROP; check that it exists"
// (ER_CANT_DROP_FIELD_OR_KEY, error 1091).
func IsCantDropKey(err error) bool {
	return isMySQLError(err, errCantDropKey)
}

func isMySQLError(err error, code uint16) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == code
	}
	return false
}
