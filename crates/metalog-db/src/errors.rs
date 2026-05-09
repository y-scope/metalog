use sqlx::mysql::MySqlDatabaseError;
use sqlx::Error as SqlxError;

// MySQL/MariaDB error codes.
const ERR_DUPLICATE_KEY: u16 = 1062;
const ERR_DEADLOCK: u16 = 1213;
const ERR_LOCK_WAIT_TIMEOUT: u16 = 1205;
const ERR_DUPLICATE_COLUMN: u16 = 1060;
const ERR_TABLE_EXISTS: u16 = 1050;
const ERR_DUPLICATE_PARTITION: u16 = 1517;
const ERR_CANT_DROP_KEY: u16 = 1091;

/// Extracts the MySQL native error number from a sqlx error, if present.
pub fn mysql_error_code(err: &SqlxError) -> Option<u16> {
    err.as_database_error()
        .map(|db_err| db_err.downcast_ref::<MySqlDatabaseError>().number())
}

fn is_mysql_error(err: &SqlxError, code: u16) -> bool {
    mysql_error_code(err) == Some(code)
}

/// Returns true if the error is a deadlock (MySQL 1213).
pub fn is_deadlock(err: &SqlxError) -> bool {
    is_mysql_error(err, ERR_DEADLOCK)
}

/// Returns true if the error is a duplicate key violation (MySQL 1062).
pub fn is_duplicate_key(err: &SqlxError) -> bool {
    is_mysql_error(err, ERR_DUPLICATE_KEY)
}

/// Returns true if the error is a lock wait timeout (MySQL 1205).
pub fn is_lock_wait_timeout(err: &SqlxError) -> bool {
    is_mysql_error(err, ERR_LOCK_WAIT_TIMEOUT)
}

/// Returns true if the error is a duplicate column (MySQL 1060).
pub fn is_duplicate_column(err: &SqlxError) -> bool {
    is_mysql_error(err, ERR_DUPLICATE_COLUMN)
}

/// Returns true if the error is a table-already-exists (MySQL 1050).
pub fn is_table_exists(err: &SqlxError) -> bool {
    is_mysql_error(err, ERR_TABLE_EXISTS)
}

/// Returns true if the error is a duplicate partition (MySQL 1517).
pub fn is_duplicate_partition(err: &SqlxError) -> bool {
    is_mysql_error(err, ERR_DUPLICATE_PARTITION)
}

/// Returns true if the error is a "can't drop key" (MySQL 1091, index doesn't exist).
pub fn is_cant_drop_key(err: &SqlxError) -> bool {
    is_mysql_error(err, ERR_CANT_DROP_KEY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_database_error_returns_none() {
        let err = SqlxError::RowNotFound;
        assert_eq!(mysql_error_code(&err), None);
        assert!(!is_deadlock(&err));
        assert!(!is_duplicate_key(&err));
    }
}
