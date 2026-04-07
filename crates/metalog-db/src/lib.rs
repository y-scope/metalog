mod dialect;
mod errors;
mod pool;
mod sqlident;
mod tx;
mod upsert;

pub use dialect::{detect_database_type, DatabaseType};
pub use errors::{
    is_cant_drop_key,
    is_deadlock,
    is_duplicate_column,
    is_duplicate_key,
    is_duplicate_partition,
    is_lock_wait_timeout,
    is_table_exists,
    mysql_error_code,
};
pub use pool::new_pool;
pub use sqlident::{quote_identifier, try_quote_identifier, validate_sql_identifier};
pub use tx::{with_deadlock_retry, with_tx, DEFAULT_MAX_RETRIES};
pub use upsert::{on_duplicate_key_update_alias, on_duplicate_key_update_values};
