use std::{sync::Arc, time::Duration};

use metalog_coordinator::ProgressTracker;
use metalog_metastore::TableConfig;
use metalog_schema::ColumnRegistry;
use tokio_util::sync::CancellationToken;

/// Partition maintenance interval (1 hour).
pub const PARTITION_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(3600);

/// Alias refresh interval (1 minute).
pub const ALIAS_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Manages coordinator tasks for a single table.
///
/// Base community edition runs: partition maintenance, alias refresh.
/// Premium consolidation planner is injected via ConsolidationProvider.
pub struct CoordinatorUnit {
    table_name: String,
    table_cfg: TableConfig,
    _registry: Arc<ColumnRegistry>,
    progress: Arc<ProgressTracker>,
    token: CancellationToken,
}

impl CoordinatorUnit {
    pub fn new(
        table_name: &str,
        table_cfg: TableConfig,
        registry: Arc<ColumnRegistry>,
        stall_timeout: Duration,
    ) -> Self {
        Self {
            table_name: table_name.to_string(),
            table_cfg,
            _registry: registry,
            progress: Arc::new(ProgressTracker::new(stall_timeout)),
            token: CancellationToken::new(),
        }
    }

    /// Creates a coordinator unit whose cancellation token is a child of `parent_token`.
    ///
    /// Cancelling `parent_token` (e.g. on node shutdown) automatically cancels this unit.
    pub fn new_with_token(
        table_name: &str,
        table_cfg: TableConfig,
        registry: Arc<ColumnRegistry>,
        stall_timeout: Duration,
        parent_token: &CancellationToken,
    ) -> Self {
        Self {
            table_name: table_name.to_string(),
            table_cfg,
            _registry: registry,
            progress: Arc::new(ProgressTracker::new(stall_timeout)),
            token: parent_token.child_token(),
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
}

/// Runs a closure periodically until the token is cancelled.
pub async fn run_periodic<F, Fut>(token: CancellationToken, interval: Duration, f: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = ()>, {
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
