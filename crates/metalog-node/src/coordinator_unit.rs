use std::{sync::Arc, time::Duration};

use metalog_coordinator::ProgressTracker;
use metalog_logutil::FailureLogger;
use metalog_metastore::{FileRecords, TableConfig};
use metalog_retention::{RetentionPolicy, RetentionModule};
use metalog_schema::{ColumnRegistry, PartitionManager};
use metalog_storage::Registry as StorageRegistry;
use metalog_timeutil::NANOS_PER_SECOND;
use sqlx::MySqlPool;
use tokio_util::sync::CancellationToken;

/// Partition maintenance interval (1 hour).
pub const PARTITION_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(3600);

/// Alias refresh interval (1 minute).
pub const ALIAS_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Default lookahead days for partition manager.
const DEFAULT_LOOKAHEAD_DAYS: i32 = 7;

/// Manages coordinator tasks for a single table.
///
/// Community edition: partition maintenance, alias refresh.
/// Premium: retention lifecycle, consolidation planner.
pub struct CoordinatorUnit {
    table_name: String,
    table_cfg: TableConfig,
    db: MySqlPool,
    partition: PartitionManager,
    retention: Option<Arc<RetentionModule>>,
    storage: Option<Arc<StorageRegistry>>,
    _registry: Arc<ColumnRegistry>,
    progress: Arc<ProgressTracker>,
    token: CancellationToken,
}

impl CoordinatorUnit {
    pub fn new(
        table_name: &str,
        table_cfg: TableConfig,
        registry: Arc<ColumnRegistry>,
        db: MySqlPool,
        storage: Option<Arc<StorageRegistry>>,
        stall_timeout: Duration,
    ) -> Self {
        let partition = PartitionManager::new(db.clone(), table_name, DEFAULT_LOOKAHEAD_DAYS);

        let retention = if table_cfg.retention.enabled {
            Some(Arc::new(RetentionModule::with_config(
                Duration::from_secs(table_cfg.retention.scan_interval_secs),
                table_cfg.retention.delete_rate,
            )))
        } else {
            None
        };

        Self {
            table_name: table_name.to_string(),
            table_cfg,
            db,
            partition,
            retention,
            storage,
            _registry: registry,
            progress: Arc::new(ProgressTracker::new(stall_timeout)),
            token: CancellationToken::new(),
        }
    }

    /// Returns the table name.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Returns the table config.
    pub fn table_config(&self) -> &TableConfig {
        &self.table_cfg
    }

    /// Returns true if the coordinator has stalled.
    pub fn is_stalled(&self) -> bool {
        self.progress.is_stalled()
    }

    /// Returns the cancellation token for this unit.
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Records progress (resets stall timer).
    pub fn record_progress(&self) {
        self.progress.record_progress();
    }

    /// Stops the coordinator by cancelling its token.
    pub fn stop(&self) {
        tracing::info!(table = %self.table_name, "stopping coordinator unit");
        self.token.cancel();
    }

    /// Blocking startup: ensures daily partitions exist before returning.
    ///
    /// Must be called before spawning background tasks. Blocks until the
    /// table has daily partitions for the next `lookahead_days` — either
    /// created here or already created by another node.
    pub async fn ensure_partitions_ready(&self) {
        match self.partition.ensure_lookahead_partitions().await {
            Ok(created) => {
                tracing::info!(
                    table = %self.table_name,
                    created,
                    "partitions ready"
                );
            }
            Err(e) => {
                tracing::warn!(
                    table = %self.table_name,
                    error = %e,
                    "startup partition creation failed, will retry in background"
                );
            }
        }
        self.progress.record_progress();
    }

    /// Runs the partition maintenance cycle once.
    async fn run_partition_maintenance(&self) {
        if let Err(e) = self.partition.run_maintenance().await {
            tracing::warn!(
                table = %self.table_name,
                error = %e,
                "partition maintenance failed"
            );
        }
        self.progress.record_progress();
    }

    /// Runs one retention cycle using the policy trait.
    async fn run_retention_once(&self) {
        let Some(ref retention) = self.retention else {
            return;
        };
        let file_recs = FileRecords::new(self.db.clone(), &self.table_name);
        let grace_period_nanos = (self.table_cfg.retention.grace_period_secs as i64)
            .saturating_mul(NANOS_PER_SECOND);
        match retention
            .run_once(
                &file_recs,
                self.storage.as_ref().map(|v| v.as_ref()),
                grace_period_nanos,
                &self.token,
            )
            .await
        {
            Ok(deleted) => {
                if deleted > 0 {
                    tracing::info!(table = %self.table_name, deleted, "retention cleanup");
                }
            }
            Err(e) => {
                tracing::warn!(
                    table = %self.table_name,
                    error = %e,
                    "retention scan failed"
                );
            }
        }
    }

    /// Spawns all background tasks for this coordinator.
    pub fn spawn_background_tasks(self: &Arc<Self>) {
        let unit = Arc::clone(self);
        let token = self.token.clone();

        // Partition maintenance (hourly).
        let pm_token = token.clone();
        let pm_unit = Arc::clone(&unit);
        tokio::spawn(async move {
            run_periodic(pm_token, PARTITION_MAINTENANCE_INTERVAL, || {
                let u = Arc::clone(&pm_unit);
                async move { u.run_partition_maintenance().await }
            })
            .await;
        });

        // Retention scanner (configurable interval, owned by run_periodic).
        if self.retention.is_some() {
            let ret_unit = Arc::clone(&unit);
            let ret_token = token.clone();
            let scan_interval =
                Duration::from_secs(self.table_cfg.retention.scan_interval_secs);
            let fl = Arc::new(FailureLogger::new(Duration::from_secs(60)));
            tokio::spawn(async move {
                run_periodic(ret_token, scan_interval, move || {
                    let u = Arc::clone(&ret_unit);
                    let fl = Arc::clone(&fl);
                    async move {
                        u.run_retention_once().await;
                        fl.ok();
                    }
                })
                .await;
            });
        }
    }
}

/// Runs a closure periodically until the token is cancelled.
pub async fn run_periodic<F, Fut>(token: CancellationToken, interval: Duration, f: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip first immediate tick.
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            _ = ticker.tick() => f().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_periodic_cancels() {
        let token = CancellationToken::new();
        let t = token.clone();

        let handle = tokio::spawn(async move {
            run_periodic(t, Duration::from_secs(3600), || async {}).await;
        });

        token.cancel();
        handle.await.unwrap();
    }

    #[test]
    fn coordinator_unit_basics() {
        assert_eq!(PARTITION_MAINTENANCE_INTERVAL, Duration::from_secs(3600));
        assert_eq!(ALIAS_REFRESH_INTERVAL, Duration::from_secs(60));
    }
}
