use metalog_metastore::AdvisoryLock;
use metalog_timeutil::{add_days_nanos, day_boundary_nanos, day_partition_name, epoch_nanos};
use sqlx::MySqlPool;

/// Default number of days to create partitions ahead.
const DEFAULT_LOOKAHEAD_DAYS: i32 = 7;

/// Default age (in days) after which sparse partitions are cleaned up.
const DEFAULT_CLEANUP_AGE_DAYS: i32 = 90;

/// Manages daily RANGE partitions on `min_timestamp` for a single table.
///
/// Creates lookahead partitions (7 days ahead by default) and cleans up old
/// sparse partitions by merging them into `p_floor`.
pub struct PartitionManager {
    db: MySqlPool,
    table_name: String,
    lookahead_days: i32,
    _cleanup_age_days: i32,
}

impl PartitionManager {
    pub fn new(
        db: MySqlPool,
        table_name: &str,
        lookahead_days: i32,
        cleanup_age_days: i32,
    ) -> Self {
        Self {
            db,
            table_name: table_name.to_string(),
            lookahead_days: if lookahead_days > 0 {
                lookahead_days
            } else {
                DEFAULT_LOOKAHEAD_DAYS
            },
            _cleanup_age_days: if cleanup_age_days > 0 {
                cleanup_age_days
            } else {
                DEFAULT_CLEANUP_AGE_DAYS
            },
        }
    }

    /// Runs a single maintenance cycle: create lookahead partitions.
    ///
    /// Acquires a non-blocking advisory lock. If another node holds the lock,
    /// this call returns immediately (partition maintenance is best-effort).
    pub async fn run_maintenance(&self) -> Result<(), PartitionError> {
        let lock_name = format!("metalog_part_{}", self.table_name);
        let lock_result = AdvisoryLock::acquire(&self.db, &lock_name, 0).await;

        let mut advisory = match lock_result {
            Ok(lock) => lock,
            Err(_) => {
                tracing::debug!(
                    table = %self.table_name,
                    "partition lock held, skipping"
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
        let today = day_boundary_nanos(epoch_nanos());
        let mut created = 0u32;

        for day in 0..self.lookahead_days {
            let boundary = add_days_nanos(today, day + 1);
            let part_name = day_partition_name(add_days_nanos(today, day));

            let sql = format!(
                "ALTER TABLE `{}` REORGANIZE PARTITION p_future INTO (PARTITION `{part_name}` \
                 VALUES LESS THAN ({boundary}), PARTITION p_future VALUES LESS THAN MAXVALUE)",
                self.table_name,
            );

            match sqlx::query(&sql).execute(&self.db).await {
                Ok(_) => created += 1,
                Err(e) if metalog_db::is_duplicate_partition(&e) => {}
                Err(e) => {
                    tracing::debug!(
                        partition = %part_name,
                        error = %e,
                        "partition creation failed"
                    );
                }
            }
        }

        if created > 0 {
            tracing::info!(
                table = %self.table_name,
                created,
                "created lookahead partitions"
            );
        }

        Ok(created)
    }
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
        assert_eq!(DEFAULT_CLEANUP_AGE_DAYS, 90);
    }
}
