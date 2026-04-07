use std::{sync::Arc, time::Duration};

use metalog_logutil::FailureLogger;
use metalog_metastore::FileRecords;
use metalog_timeutil::epoch_nanos;
use tokio_util::sync::CancellationToken;

use crate::{
    inflight::InFlightSet,
    policy::Policy,
    task_queue::{marshal_payload, ConsolidationPayload, Queue, TaskPayload, TASK_PAYLOAD_VERSION},
};

/// Maximum pending+processing tasks before skipping new task creation.
const MAX_BACKPRESSURE_DEPTH: i64 = 100;

/// Consolidation planner: scans for pending files, groups them via policy,
/// creates tasks in the queue.
pub struct Planner {
    file_recs: Arc<FileRecords>,
    queue: Arc<Queue>,
    policy: Arc<dyn Policy>,
    in_flight: Arc<InFlightSet>,
    table_name: String,
    archive_backend: String,
    archive_bucket: String,
    interval: Duration,
    stale_threshold: Duration,
}

/// Configuration for creating a Planner.
pub struct PlannerConfig {
    pub file_recs: Arc<FileRecords>,
    pub queue: Arc<Queue>,
    pub policy: Arc<dyn Policy>,
    pub in_flight: Arc<InFlightSet>,
    pub table_name: String,
    pub archive_backend: String,
    pub archive_bucket: String,
    pub interval: Duration,
    pub stale_threshold: Duration,
}

impl Planner {
    pub fn new(config: PlannerConfig) -> Self {
        Self {
            file_recs: config.file_recs,
            queue: config.queue,
            policy: config.policy,
            in_flight: config.in_flight,
            table_name: config.table_name,
            archive_backend: config.archive_backend,
            archive_bucket: config.archive_bucket,
            interval: config.interval,
            stale_threshold: config.stale_threshold,
        }
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
                    match self.plan_once().await {
                        Ok(()) => fl.ok(),
                        Err(e) => fl.fail(&format!("planning failed: {e}")),
                    }
                }
            }
        }
    }

    async fn plan_once(&self) -> Result<(), PlannerError> {
        // Step 1: Reclaim stale tasks.
        let stale_tasks = self
            .queue
            .find_stale_tasks(&self.table_name, Duration::from_secs(300))
            .await?;
        for task in &stale_tasks {
            self.queue.reclaim_task(task.task_id).await?;
            tracing::info!(task_id = task.task_id, "reclaimed stale task");
        }

        // Step 2: Backpressure check.
        let active = self.queue.count_active_tasks(&self.table_name).await?;
        if active >= MAX_BACKPRESSURE_DEPTH {
            tracing::debug!(active, "backpressure: skipping task creation");
            return Ok(());
        }

        // Step 3: Promote stuck buffering.
        if self.stale_threshold > Duration::ZERO {
            let stale_before = epoch_nanos() - self.stale_threshold.as_nanos() as i64;
            let promoted = self.file_recs.promote_stuck_buffering(stale_before).await?;
            if promoted > 0 {
                tracing::info!(promoted, "promoted stuck buffering files");
            }
        }

        // Step 4: Find candidates.
        let candidates = self.file_recs.find_consolidation_pending(&[], &[]).await?;
        if candidates.is_empty() {
            return Ok(());
        }

        // Step 5: Apply policy.
        // Convert PendingFile to FileRecord for the policy interface.
        let records: Vec<metalog_types::FileRecord> = candidates
            .iter()
            .map(|pf| metalog_types::FileRecord {
                id: pf.id,
                min_timestamp: pf.min_timestamp,
                max_timestamp: pf.max_timestamp,
                file_storage_backend: pf.file_storage_backend.clone(),
                file_bucket: pf.file_bucket.clone(),
                file_path: pf.file_path.clone(),
                ..metalog_types::FileRecord::default()
            })
            .collect();

        let groups = self.policy.select_files(&records);

        // Step 6: Build payloads and create tasks.
        let mut inputs = Vec::new();
        for group in &groups {
            let ir_paths: Vec<String> = group
                .records
                .iter()
                .filter_map(|r| r.file_path.clone())
                .collect();
            if ir_paths.is_empty() || !self.in_flight.try_add(&ir_paths) {
                continue;
            }

            let payload = TaskPayload {
                table_name: self.table_name.clone(),
                consolidation: Some(ConsolidationPayload {
                    ir_backend: group
                        .records
                        .first()
                        .and_then(|r| r.file_storage_backend.clone())
                        .unwrap_or_default(),
                    ir_paths: ir_paths.clone(),
                    ir_buckets: group
                        .records
                        .iter()
                        .map(|r| r.file_bucket.clone().unwrap_or_default())
                        .collect(),
                    archive_backend: if group.archive_backend.is_empty() {
                        self.archive_backend.clone()
                    } else {
                        group.archive_backend.clone()
                    },
                    archive_bucket: if group.archive_bucket.is_empty() {
                        self.archive_bucket.clone()
                    } else {
                        group.archive_bucket.clone()
                    },
                    archive_path: group.archive_path.clone(),
                    file_ids: group.records.iter().map(|r| r.id).collect(),
                    min_timestamp: group
                        .records
                        .iter()
                        .map(|r| r.min_timestamp)
                        .min()
                        .unwrap_or(0),
                }),
            };

            match marshal_payload(&payload) {
                Ok(data) => inputs.push(data),
                Err(e) => {
                    self.in_flight.remove(&ir_paths);
                    tracing::error!(error = %e, "marshal payload failed");
                }
            }
        }

        if !inputs.is_empty() {
            let n = self
                .queue
                .create_tasks(&self.table_name, TASK_PAYLOAD_VERSION, &inputs)
                .await?;
            tracing::info!(
                tasks = n,
                groups = inputs.len(),
                "created consolidation tasks"
            );
        }

        Ok(())
    }
}

/// Errors from the planner loop.
#[derive(Debug, thiserror::Error)]
pub enum PlannerError {
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
