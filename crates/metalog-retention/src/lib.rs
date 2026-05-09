use std::time::Duration;

use metalog_metastore::{DeletionResult, FileRecords, StoragePath};
use metalog_storage::Registry as StorageRegistry;
use metalog_timeutil::epoch_nanos;
use metalog_types::processors::RetentionProvider;
use tokio_util::sync::CancellationToken;

/// Default scan interval.
const DEFAULT_SCAN_INTERVAL: Duration = Duration::from_secs(60);

/// Default max storage deletions per second.
const DEFAULT_DELETE_RATE: u32 = 500;

/// Behavioral trait for a single retention cleanup cycle.
///
/// Defined in the retention crate (not `metalog-types`) because it
/// references concrete types like `FileRecords` and `StorageRegistry`.
/// The marker trait `RetentionProvider` in `metalog-types` allows
/// community-edition code to hold `Option<Arc<dyn RetentionProvider>>`
/// without depending on this crate.
pub trait RetentionPolicy: Send + Sync {
    /// Run one retention cycle. Returns number of storage objects deleted.
    ///
    /// The `token` allows cancellation during Phase 3 storage deletions.
    fn run_once(
        &self,
        file_recs: &FileRecords,
        storage: Option<&StorageRegistry>,
        grace_period_nanos: i64,
        token: &CancellationToken,
    ) -> impl std::future::Future<Output = Result<u64, sqlx::Error>> + Send;
}

/// Premium retention lifecycle module.
///
/// 3-phase retention:
/// 1. Transition expired files to PURGING state
/// 2. Delete PURGING metadata rows, collect storage paths
/// 3. Delete from object storage (rate-limited, best-effort)
pub struct RetentionModule {
    scan_interval: Duration,
    delete_rate: u32,
}

impl RetentionModule {
    pub fn new() -> Self {
        Self {
            scan_interval: DEFAULT_SCAN_INTERVAL,
            delete_rate: DEFAULT_DELETE_RATE,
        }
    }

    /// Creates a module with config-driven parameters.
    pub fn with_config(scan_interval: Duration, delete_rate: u32) -> Self {
        Self {
            scan_interval,
            delete_rate: if delete_rate > 0 {
                delete_rate
            } else {
                DEFAULT_DELETE_RATE
            },
        }
    }

    /// Returns the configured scan interval.
    pub fn scan_interval(&self) -> Duration {
        self.scan_interval
    }

    /// Deletes storage paths from their respective backends, rate-limited.
    ///
    /// Respects cancellation via `token` between deletions.
    async fn delete_storage_paths(
        &self,
        token: &CancellationToken,
        storage: &StorageRegistry,
        paths: &[StoragePath],
    ) -> u64 {
        if paths.is_empty() {
            return 0;
        }

        let throttle = Duration::from_secs_f64(1.0 / self.delete_rate as f64);
        let mut deleted = 0u64;

        for (i, p) in paths.iter().enumerate() {
            if p.backend.is_empty() || p.path.is_empty() {
                continue;
            }

            // Rate-limit: sleep between deletions (not before the first one).
            if i > 0 {
                tokio::select! {
                    _ = token.cancelled() => return deleted,
                    _ = tokio::time::sleep(throttle) => {}
                }
            }

            let Some(backend) = storage.get(&p.backend) else {
                tracing::warn!(
                    backend = %p.backend,
                    path = %p.path,
                    "retention: unknown storage backend"
                );
                continue;
            };

            match backend.delete(&p.bucket, &p.path).await {
                Ok(()) => deleted += 1,
                Err(e) => {
                    tracing::warn!(
                        backend = %p.backend,
                        bucket = %p.bucket,
                        path = %p.path,
                        error = %e,
                        "retention: failed to delete storage object"
                    );
                }
            }
        }

        deleted
    }
}

impl RetentionPolicy for RetentionModule {
    async fn run_once(
        &self,
        file_recs: &FileRecords,
        storage: Option<&StorageRegistry>,
        grace_period_nanos: i64,
        token: &CancellationToken,
    ) -> Result<u64, sqlx::Error> {
        let now = epoch_nanos();

        // Phase 1: transition expired → PURGING.
        // Grace period: files with expires_at between (now - grace) and now
        // are spared — they get extra time before purging.
        let cutoff = now.saturating_sub(grace_period_nanos);
        let transitioned = file_recs.transition_expired_to_purging(cutoff).await?;
        if transitioned > 0 {
            tracing::debug!(transitioned, "transitioned to PURGING");
        }

        // Phase 2: delete PURGING rows, collect paths.
        let result: DeletionResult = file_recs.delete_expired_files(cutoff).await?;

        // Phase 3: delete from object storage (rate-limited, best-effort).
        let mut storage_deleted = 0u64;
        if let Some(reg) = storage {
            if token.is_cancelled() {
                return Ok(storage_deleted);
            }
            storage_deleted += self
                .delete_storage_paths(token, reg, &result.ir_paths)
                .await;
            storage_deleted += self
                .delete_storage_paths(token, reg, &result.archive_paths)
                .await;
        }

        Ok(storage_deleted)
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

    #[test]
    fn retention_module_with_config() {
        let module = RetentionModule::with_config(Duration::from_secs(30), 100);
        assert_eq!(module.scan_interval(), Duration::from_secs(30));
        assert_eq!(module.delete_rate, 100);
    }

    #[test]
    fn retention_module_with_config_zero_rate() {
        let module = RetentionModule::with_config(Duration::from_secs(30), 0);
        assert_eq!(module.delete_rate, DEFAULT_DELETE_RATE);
    }
}
