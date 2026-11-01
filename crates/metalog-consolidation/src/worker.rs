use std::{sync::Arc, time::Duration};

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::task_queue::{
    marshal_result,
    unmarshal_payload,
    Queue,
    Task,
    TaskResult,
    TASK_PAYLOAD_VERSION,
};

/// Worker drain timeout (after prefetcher stops, wait this long for
/// in-flight tasks before force-cancelling).
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Default backoff interval when no tasks are available.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Maximum backoff between polls.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Prefetcher: batch-claims tasks and feeds them into a channel.
pub struct Prefetcher {
    queue: Arc<Queue>,
    table_name: String,
    worker_id: String,
    batch_size: i32,
}

impl Prefetcher {
    pub fn new(queue: Arc<Queue>, table_name: &str, worker_id: &str, batch_size: i32) -> Self {
        Self {
            queue,
            table_name: table_name.to_string(),
            worker_id: worker_id.to_string(),
            batch_size,
        }
    }

    /// Runs the prefetch loop, sending tasks to `tx` until cancelled.
    pub async fn run(&self, token: CancellationToken, tx: async_channel::Sender<Task>) {
        let mut backoff = POLL_INTERVAL;

        tracing::info!(table = %self.table_name, "prefetcher started");

        loop {
            if token.is_cancelled() {
                break;
            }

            match self
                .queue
                .claim_tasks(&self.table_name, &self.worker_id, self.batch_size)
                .await
            {
                Ok(tasks) => {
                    if tasks.is_empty() {
                        tokio::select! {
                            _ = token.cancelled() => break,
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                    } else {
                        backoff = POLL_INTERVAL;
                        for task in tasks {
                            tokio::select! {
                                _ = token.cancelled() => return,
                                result = tx.send(task) => if result.is_err() { return; },
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "claim tasks failed");
                    tokio::select! {
                        _ = token.cancelled() => break,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }

        tracing::info!(table = %self.table_name, "prefetcher stopped");
    }
}

/// Each executor claims only when it has capacity. Pending tasks remain durable
/// in the database instead of aging in a prefetched, un-heartbeated local queue.
pub struct WorkerUnit {
    queue: Arc<Queue>,
    archive: Arc<crate::ArchiveCreator>,
    worker_id: String,
    concurrency: usize,
}

impl WorkerUnit {
    pub fn new(
        queue: Arc<Queue>,
        archive: Arc<crate::ArchiveCreator>,
        worker_id: &str,
        concurrency: usize,
    ) -> Self {
        Self {
            queue,
            archive,
            worker_id: worker_id.into(),
            concurrency,
        }
    }

    pub async fn run(&self, token: CancellationToken) {
        let mut workers = JoinSet::new();
        for _ in 0..self.concurrency {
            let queue = self.queue.clone();
            let archive = self.archive.clone();
            let worker_id = self.worker_id.clone();
            let stop = token.clone();
            workers.spawn(async move {
                loop {
                    let tasks = tokio::select! {
                        _ = stop.cancelled() => return,
                        tasks = queue.claim_tasks("", &worker_id, 1) => tasks,
                    };
                    match tasks {
                        Ok(tasks) if !tasks.is_empty() => {
                            for task in tasks {
                                execute_task(&queue, &archive, task).await;
                            }
                        }
                        tasks => {
                            if let Err(error) = tasks {
                                tracing::warn!(%error, "claim consolidation task failed");
                            }
                            tokio::select! {
                                _ = stop.cancelled() => return,
                                _ = tokio::time::sleep(POLL_INTERVAL) => {},
                            }
                        }
                    }
                }
            });
        }
        token.cancelled().await;
        if tokio::time::timeout(DRAIN_TIMEOUT, async {
            while workers.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            tracing::warn!("consolidation drain timed out; unfinished attempts will be reclaimed");
            workers.abort_all();
            while workers.join_next().await.is_some() {}
        }
    }
}

/// Holds a dedicated task lock through upload and completion. All progress SQL
/// uses that connection: losing it cancels compression instead of silently
/// continuing on a replacement pooled connection.
pub async fn execute_task(queue: &Queue, archive: &crate::ArchiveCreator, task: Task) {
    if let Err(error) = execute_inner(queue, archive, &task).await {
        tracing::warn!(
            task_id = task.task_id, %error,
            "consolidation attempt interrupted; left for recovery"
        );
    }
}

async fn execute_inner(
    queue: &Queue,
    archive: &crate::ArchiveCreator,
    task: &Task,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut lock = queue.task_lock(task.task_id).await?;
    let active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM _task_queue WHERE task_id = ? AND state = 'processing'",
    )
    .bind(task.task_id)
    .fetch_one(lock.connection())
    .await?;
    if active != 1 {
        lock.release().await?;
        return Ok(());
    }
    let payload = unmarshal_payload(&task.input);
    let valid = payload
        .as_ref()
        .ok()
        .filter(|p| p.table_name == task.table_name)
        .and_then(|p| p.consolidation.as_ref())
        .filter(|p| p.validate().is_ok());
    let Some(input) = valid.filter(|_| task.version == TASK_PAYLOAD_VERSION) else {
        sqlx::query(
            "UPDATE _task_queue SET state = 'dead_letter', completed_at = ? WHERE task_id = ? AND \
             state = 'processing'",
        )
        .bind(metalog_timeutil::epoch_nanos())
        .bind(task.task_id)
        .execute(lock.connection())
        .await?;
        tracing::error!(
            task_id = task.task_id,
            "invalid consolidation payload; dead-lettered without modifying sources"
        );
        lock.release().await?;
        return Ok(());
    };
    queue.ensure_reservations(task, input).await?;
    let create = archive.create(task.task_id, input);
    tokio::pin!(create);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    let result = loop {
        tokio::select! {
            result = &mut create => break result,
            _ = heartbeat.tick() => {
                let updated = sqlx::query(
                    "UPDATE _task_queue SET claimed_at = ? WHERE task_id = ? \
                     AND state = 'processing'"
                )
                .bind(metalog_timeutil::epoch_nanos())
                .bind(task.task_id)
                .execute(lock.connection())
                .await?;
                if updated.rows_affected() != 1 { return Err("task ownership lost".into()); }
            }
        }
    };
    let result = result.unwrap_or_else(|error| TaskResult {
        error: error.to_string().chars().take(4096).collect(),
        ..TaskResult::default()
    });
    if !result.error.is_empty() {
        tracing::warn!(task_id = task.task_id, error = %result.error, "archive creation failed");
    }
    if result.error.is_empty() {
        tracing::info!(
            task_id = task.task_id, files = input.ir_paths.len(),
            archive_path = %result.archive_path, bytes = result.archive_size_bytes,
            "archive uploaded"
        );
    }
    let output = marshal_result(&result)?;
    sqlx::query(
        "UPDATE _task_queue SET state = 'completed', output = ?, completed_at = ? WHERE task_id = \
         ? AND state = 'processing'",
    )
    .bind(output)
    .bind(metalog_timeutil::epoch_nanos())
    .bind(task.task_id)
    .execute(lock.connection())
    .await?;
    lock.release().await?;
    Ok(())
}
