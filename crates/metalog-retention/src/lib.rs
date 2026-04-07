use std::{sync::Arc, time::Duration};

use metalog_logutil::FailureLogger;
use metalog_metastore::FileRecords;
use metalog_storage::Registry as StorageRegistry;
use metalog_timeutil::epoch_nanos;
use metalog_types::processors::RetentionProvider;
use tokio_util::sync::CancellationToken;

/// Default scan interval.
const DEFAULT_SCAN_INTERVAL: Duration = Duration::from_secs(60);

/// Default max storage deletions per second.
const DEFAULT_DELETE_RATE: u32 = 500;

/// Premium retention lifecycle module.
///
/// 3-phase retention:
/// 1. Transition expired files to PURGING state
/// 2. Delete PURGING metadata rows, collect storage paths
/// 3. Delete from object storage (rate-limited, best-effort)
pub struct RetentionModule {
    scan_interval: Duration,
    _delete_rate: u32,
}

impl RetentionModule {
    pub fn new() -> Self {
        Self {
            scan_interval: DEFAULT_SCAN_INTERVAL,
            _delete_rate: DEFAULT_DELETE_RATE,
        }
    }

    /// Runs the retention scanner for a table until cancelled.
    pub async fn run_scanner(
        &self,
        token: CancellationToken,
        file_recs: Arc<FileRecords>,
        _storage: Option<Arc<StorageRegistry>>,
    ) {
        let fl = FailureLogger::new(Duration::from_secs(60));
        let mut interval = tokio::time::interval(self.scan_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = interval.tick() => {
                    match self.run_once(&file_recs).await {
                        Ok(deleted) => {
                            if deleted > 0 {
                                tracing::info!(deleted, "retention cleanup");
                            }
                            fl.ok();
                        }
                        Err(e) => {
                            fl.fail(&format!("retention failed: {e}"));
                        }
                    }
                }
            }
        }
    }

    async fn run_once(
        &self,
        file_recs: &FileRecords,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let now = epoch_nanos();

        // Phase 1: transition expired → PURGING.
        let transitioned = file_recs.transition_expired_to_purging(now).await?;
        if transitioned > 0 {
            tracing::debug!(transitioned, "transitioned to PURGING");
        }

        // Phase 2: delete PURGING rows, collect paths.
        let result = file_recs.delete_expired_files(now).await?;

        // Phase 3: TODO — delete from storage (rate-limited).

        Ok(result.deleted_count)
    }
}

impl Default for RetentionModule {
    fn default() -> Self {
        Self::new()
    }
}

impl RetentionProvider for RetentionModule {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_module_creates() {
        let module = RetentionModule::new();
        assert_eq!(module.scan_interval, Duration::from_secs(60));
        assert_eq!(DEFAULT_DELETE_RATE, 500);
        assert_eq!(module.name(), "retention");
    }
}
