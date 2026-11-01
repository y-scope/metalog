use std::{sync::Arc, time::Duration};

use metalog_logutil::FailureLogger;
use metalog_metastore::FileRecords;
use metalog_timeutil::epoch_nanos;
use metalog_types::file_state::FileState;
use tokio_util::sync::CancellationToken;

use crate::{
    inflight::InFlightSet,
    policy::Policy,
    task_queue::{ConsolidationPayload, Queue, TaskPayload},
};

/// Maximum pending+processing tasks before skipping new task creation.
const MAX_BACKPRESSURE_DEPTH: i64 = 100;

/// Consolidation planner: scans for pending files, groups them via policy,
/// creates tasks in the queue.
pub struct Planner {
    file_recs: Arc<FileRecords>,
    queue: Arc<Queue>,
    policy: Arc<dyn Policy>,
    storage: Option<Arc<metalog_storage::Registry>>,
    planning_enabled: bool,
    table_name: String,
    archive_backend: String,
    archive_bucket: String,
    interval: Duration,
    stale_threshold: Duration,
    /// The buffering state to promote from (e.g., `IrArchiveBuffering` or `FileArchiveBuffering`).
    buffering_state: FileState,
    /// The consolidation-pending state to promote to and query for candidates.
    pending_state: FileState,
}

/// Configuration for creating a Planner.
pub struct PlannerConfig {
    pub file_recs: Arc<FileRecords>,
    pub queue: Arc<Queue>,
    pub policy: Arc<dyn Policy>,
    /// Retained for callers of the original planner API. Reservations now live
    /// in `_consolidation_file`; this process-local set is not used for safety.
    pub in_flight: Arc<InFlightSet>,
    pub table_name: String,
    pub archive_backend: String,
    pub archive_bucket: String,
    pub interval: Duration,
    pub stale_threshold: Duration,
    /// The buffering state to promote from (e.g., `IrArchiveBuffering` or `FileArchiveBuffering`).
    pub buffering_state: FileState,
    /// The consolidation-pending state to promote to and query for candidates.
    pub pending_state: FileState,
}

impl Planner {
    pub fn new(config: PlannerConfig) -> Self {
        Self {
            file_recs: config.file_recs,
            queue: config.queue,
            policy: config.policy,
            storage: None,
            planning_enabled: true,
            table_name: config.table_name,
            archive_backend: config.archive_backend,
            archive_bucket: config.archive_bucket,
            interval: config.interval,
            stale_threshold: config.stale_threshold,
            buffering_state: config.buffering_state,
            pending_state: config.pending_state,
        }
    }

    pub fn with_storage(mut self, storage: Arc<metalog_storage::Registry>) -> Self {
        self.storage = Some(storage);
        self
    }

    pub fn with_planning_enabled(mut self, enabled: bool) -> Self {
        self.planning_enabled = enabled;
        self
    }

    /// Runs the planning loop until cancelled.
    pub async fn run(&self, token: CancellationToken) {
        let fl = FailureLogger::new(Duration::from_secs(60));
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        tracing::info!(table = %self.table_name, "planner started");

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    tracing::info!(table = %self.table_name, "planner stopped");
                    return;
                }
                _ = ticker.tick() => {
                    tokio::select! {
                        _ = token.cancelled() => return,
                        result = self.plan_once() => match result {
                            Ok(()) => fl.ok(),
                            Err(e) => fl.fail(&format!("planning failed: {e}")),
                        }
                    }
                }
            }
        }
    }

    pub async fn plan_once(&self) -> Result<(), PlannerError> {
        if !self.planning_enabled {
            let has_work: i64 =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _task_queue WHERE table_name = ?)")
                    .bind(&self.table_name)
                    .fetch_one(self.queue.pool())
                    .await?;
            if has_work == 0 {
                return Ok(());
            }
        }
        let mut planning_lock = match metalog_metastore::AdvisoryLock::acquire_scoped(
            self.queue.pool(),
            "consolidation_planner",
            &self.table_name,
            0,
        )
        .await
        {
            Ok(lock) => lock,
            Err(metalog_metastore::AdvisoryLockError::NotAcquired(_)) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let result = self.plan_locked().await;
        planning_lock.release().await?;
        result
    }

    async fn plan_locked(&self) -> Result<(), PlannerError> {
        if let Some(storage) = &self.storage {
            self.file_recs
                .audit_canonical_storage_locations(&storage.canonical_backend_names())
                .await?;
            crate::finalizer::Finalizer {
                queue: self.queue.clone(),
                files: self.file_recs.clone(),
                storage: storage.clone(),
            }
            .run_once()
            .await?;
        }
        for task in self
            .queue
            .find_stale_tasks(&self.table_name, Duration::from_secs(300))
            .await?
        {
            let mut lock = match self.queue.task_lock(task.task_id).await {
                Ok(lock) => lock,
                Err(metalog_metastore::AdvisoryLockError::NotAcquired(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            self.queue.reclaim_task(task.task_id).await?;
            lock.release().await?;
        }
        if !self.planning_enabled {
            return Ok(());
        }
        let active = self.queue.count_active_tasks(&self.table_name).await?;
        if active >= MAX_BACKPRESSURE_DEPTH {
            return Ok(());
        }
        if !self.stale_threshold.is_zero() {
            self.file_recs
                .promote_stuck_buffering(
                    self.buffering_state,
                    self.pending_state,
                    epoch_nanos().saturating_sub(self.stale_threshold.as_nanos() as i64),
                )
                .await?;
        }
        // Resolve on every pass so newly allocated dimensions participate without
        // restarting the coordinator. Non-string values follow Go's ungrouped rule.
        let mut mappings = Vec::new();
        for key in self.policy.required_dims() {
            let column: Option<String> = sqlx::query_scalar(
                "SELECT column_name FROM _dim_registry WHERE table_name = ? AND dim_key = ? AND \
                 state = 'ACTIVE' AND base_type IN ('str', 'str_utf8') LIMIT 1",
            )
            .bind(&self.table_name)
            .bind(&key)
            .fetch_optional(self.queue.pool())
            .await?;
            if let Some(physical_col) = column {
                mappings.push(metalog_types::column::ColumnMapping {
                    physical_col,
                    logical_key: key,
                });
            }
        }
        let candidates = self
            .file_recs
            .find_consolidation_pending(self.pending_state, &mappings, &[])
            .await?;
        // A task has one input backend, but can include multiple buckets. Split
        // before policy selection so min/max grouping constraints still hold.
        let mut backends =
            std::collections::BTreeMap::<String, Vec<metalog_types::FileRecord>>::new();
        for pf in candidates {
            let Some(backend) = pf.file_storage_backend.as_ref().filter(|b| !b.is_empty()) else {
                continue;
            };
            backends
                .entry(backend.clone())
                .or_default()
                .push(metalog_types::FileRecord {
                    id: pf.id,
                    min_timestamp: pf.min_timestamp,
                    max_timestamp: pf.max_timestamp,
                    file_storage_backend: pf.file_storage_backend,
                    file_bucket: pf.file_bucket,
                    file_path: pf.file_path,
                    dims: pf.dims,
                    state: self.pending_state,
                    ..Default::default()
                });
        }
        let mut budget = (MAX_BACKPRESSURE_DEPTH - active) as usize;
        for records in backends.into_values() {
            for group in self.policy.select_files(&records) {
                if budget == 0 {
                    return Ok(());
                }
                let payload = TaskPayload {
                    table_name: self.table_name.clone(),
                    consolidation: Some(ConsolidationPayload {
                        ir_backend: group
                            .records
                            .first()
                            .and_then(|r| r.file_storage_backend.clone())
                            .unwrap_or_default(),
                        ir_paths: group
                            .records
                            .iter()
                            .map(|r| r.file_path.clone().unwrap_or_default())
                            .collect(),
                        ir_buckets: group
                            .records
                            .iter()
                            .map(|r| r.file_bucket.clone().unwrap_or_default())
                            .collect(),
                        archive_backend: if group.archive_backend.is_empty() {
                            self.archive_backend.clone()
                        } else {
                            group.archive_backend
                        },
                        archive_bucket: if group.archive_bucket.is_empty() {
                            self.archive_bucket.clone()
                        } else {
                            group.archive_bucket
                        },
                        archive_path: group.archive_path,
                        file_ids: group.records.iter().map(|r| r.id).collect(),
                        min_timestamp: group
                            .records
                            .iter()
                            .map(|r| r.min_timestamp)
                            .min()
                            .unwrap_or(0),
                    }),
                };
                let input = payload.consolidation.as_ref().expect("set above");
                input.validate()?;
                if let Some(storage) = &self.storage {
                    if storage.get(&input.ir_backend).is_none()
                        || storage.get(&input.archive_backend).is_none()
                    {
                        return Err(PlannerError::Invalid(
                            "consolidation requires configured source and archive backends".into(),
                        ));
                    }
                    for (index, path) in input.ir_paths.iter().enumerate() {
                        storage.validate_location(&input.ir_backend, input.bucket(index), path)?;
                    }
                    storage.validate_location(
                        &input.archive_backend,
                        &input.archive_bucket,
                        &input.archive_path,
                    )?;
                }
                if self
                    .queue
                    .create_reserved_task(&payload, &group.records)
                    .await?
                    .is_some()
                {
                    budget -= 1;
                }
            }
        }
        Ok(())
    }
}

/// Errors from the planner loop.
#[derive(Debug, thiserror::Error)]
pub enum PlannerError {
    #[error(transparent)]
    Lock(#[from] metalog_metastore::AdvisoryLockError),
    #[error(transparent)]
    Mutation(#[from] metalog_metastore::StorageMutationError),
    #[error(transparent)]
    Storage(#[from] metalog_storage::StorageError),
    #[error(transparent)]
    Archive(#[from] crate::ArchiveError),
    #[error("{0}")]
    Invalid(String),
    #[error("sql: {0}")]
    Sql(#[from] sqlx::Error),

    #[error("codec: {0}")]
    Codec(#[from] metalog_encoding::CodecError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backpressure_threshold() {
        assert_eq!(MAX_BACKPRESSURE_DEPTH, 100);
    }
}
