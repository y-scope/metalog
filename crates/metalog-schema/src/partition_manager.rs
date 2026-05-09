use metalog_db::{quote_identifier, validate_sql_identifier};
use metalog_metastore::AdvisoryLock;
use metalog_timeutil::{add_days_nanos, day_boundary_nanos, day_partition_name, epoch_nanos};
use sqlx::MySqlPool;

/// Default number of days to create partitions ahead.
const DEFAULT_LOOKAHEAD_DAYS: i32 = 7;

/// Default lookahead used during table provisioning.
pub const DEFAULT_PROVISION_LOOKAHEAD_DAYS: i32 = 7;

/// Structural catch-all partition for timestamps beyond the last daily partition.
const PART_FUTURE: &str = "p_future";

/// Manages daily RANGE partitions on `min_timestamp` for a single table.
///
/// Creates lookahead partitions (7 days ahead by default). Runs periodically
/// via CoordinatorUnit, with an initial blocking call at startup.
pub struct PartitionManager {
    db: MySqlPool,
    table_name: String,
    lookahead_days: i32,
}

impl PartitionManager {
    pub fn new(db: MySqlPool, table_name: &str, lookahead_days: i32) -> Self {
        Self {
            db,
            table_name: table_name.to_string(),
            lookahead_days: if lookahead_days > 0 {
                lookahead_days
            } else {
                DEFAULT_LOOKAHEAD_DAYS
            },
        }
    }

    /// Runs a single maintenance cycle: create lookahead partitions.
    ///
    /// Acquires a non-blocking advisory lock. If another node holds the lock,
    /// this call returns immediately (partition maintenance is best-effort).
    pub async fn run_maintenance(&self) -> Result<(), PartitionError> {
        let lock_name = format!("pm_{}", self.table_name);
        let mut advisory = match AdvisoryLock::acquire(&self.db, &lock_name, 0).await {
            Ok(lock) => lock,
            Err(_) => {
                tracing::debug!(
                    table = %self.table_name,
                    "partition maintenance lock held, skipping"
                );
                return Ok(());
            }
        };

        if let Err(e) = self.ensure_lookahead_partitions().await {
            tracing::warn!(
                table = %self.table_name,
                error = %e,
                "partition lookahead failed"
            );
        }

        let _ = advisory.release().await;
        Ok(())
    }

    /// Creates daily partitions from today to today + lookahead_days.
    pub async fn ensure_lookahead_partitions(&self) -> Result<u32, PartitionError> {
        ensure_lookahead_partitions_inner(&self.db, &self.table_name, self.lookahead_days).await
    }
}

/// Creates daily partitions from today through today + `lookahead_days` by
/// reorganizing `p_future`. Used during table provisioning.
pub async fn create_lookahead_partitions(
    db: &MySqlPool,
    table_name: &str,
    lookahead_days: i32,
) -> Result<u32, PartitionError> {
    ensure_lookahead_partitions_inner(db, table_name, lookahead_days).await
}

async fn ensure_lookahead_partitions_inner(
    db: &MySqlPool,
    table_name: &str,
    lookahead_days: i32,
) -> Result<u32, PartitionError> {
    let today = day_boundary_nanos(epoch_nanos());
    let mut created = 0u32;

    for day in 0..=lookahead_days {
        let part_day = add_days_nanos(today, day);
        let part_name = day_partition_name(part_day);

        let boundary = add_days_nanos(part_day, 1);
        let sql = format!(
            "ALTER TABLE {} REORGANIZE PARTITION {} INTO (PARTITION {} VALUES LESS THAN \
             ({boundary}), PARTITION {} VALUES LESS THAN MAXVALUE)",
            quote_identifier(table_name),
            PART_FUTURE,
            quote_identifier(&part_name),
            PART_FUTURE,
        );

        match sqlx::query(&sql).execute(db).await {
            Ok(_) => created += 1,
            Err(e) if metalog_db::is_duplicate_partition(&e) => {
                tracing::debug!(
                    partition = %part_name,
                    "partition already exists (concurrent creation)"
                );
            }
            Err(e) => {
                return Err(PartitionError::from(e));
            }
        }
    }

    if created > 0 {
        tracing::info!(
            table = table_name,
            created,
            "created lookahead partitions"
        );
    }

    Ok(created)
}

#[derive(Debug, thiserror::Error)]
pub enum PartitionError {
    #[error("sql: {0}")]
    Sql(#[from] sqlx::Error),

    #[error("advisory lock: {0}")]
    Lock(#[from] metalog_metastore::AdvisoryLockError),
}

/// Index reconciliation for dimension columns.
pub struct IndexManager {
    db: MySqlPool,
}

impl IndexManager {
    pub fn new(db: MySqlPool) -> Self {
        Self { db }
    }

    /// Ensures an index exists on the given column.
    pub async fn ensure_index(&self, table_name: &str, col_name: &str) -> Result<(), sqlx::Error> {
        if validate_sql_identifier(col_name).is_err() {
            tracing::warn!(column = %col_name, "skipping index on invalid column name");
            return Ok(());
        }
        let idx_name = format!("idx_{col_name}");
        let sql = format!(
            "ALTER TABLE `{table_name}` ADD INDEX `{idx_name}` (`{col_name}`), ALGORITHM=INPLACE, \
             LOCK=NONE"
        );
        match sqlx::query(&sql).execute(&self.db).await {
            Ok(_) => {
                tracing::info!(table = table_name, index = %idx_name, "created index");
                Ok(())
            }
            Err(e) if metalog_db::is_duplicate_key(&e) => {
                tracing::debug!(index = %idx_name, "index already exists");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Drops an index if it exists.
    pub async fn drop_index(&self, table_name: &str, col_name: &str) -> Result<(), sqlx::Error> {
        if validate_sql_identifier(col_name).is_err() {
            return Ok(());
        }
        let idx_name = format!("idx_{col_name}");
        let sql = format!("ALTER TABLE `{table_name}` DROP INDEX `{idx_name}`");
        match sqlx::query(&sql).execute(&self.db).await {
            Ok(_) => {
                tracing::info!(table = table_name, index = %idx_name, "dropped index");
                Ok(())
            }
            Err(e) if metalog_db::is_cant_drop_key(&e) => {
                tracing::debug!(index = %idx_name, "index does not exist");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_error_display() {
        let err = PartitionError::Sql(sqlx::Error::RowNotFound);
        assert!(err.to_string().contains("sql:"));
    }

    #[test]
    fn default_constants() {
        assert_eq!(DEFAULT_LOOKAHEAD_DAYS, 7);
    }

    #[test]
    fn structural_partition_names() {
        assert_eq!(PART_FUTURE, "p_future");
    }

    #[tokio::test]
    async fn partition_manager_constructor_defaults() {
        let pool = MySqlPool::connect_lazy("mysql://u@h/d").unwrap();
        let pm = PartitionManager::new(pool, "t", 0);
        assert_eq!(pm.lookahead_days, DEFAULT_LOOKAHEAD_DAYS);
    }

    #[tokio::test]
    async fn partition_manager_constructor_custom() {
        let pool = MySqlPool::connect_lazy("mysql://u@h/d").unwrap();
        let pm = PartitionManager::new(pool, "t", 14);
        assert_eq!(pm.lookahead_days, 14);
    }
}
