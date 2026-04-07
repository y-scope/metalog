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
                            if tx.send(task).await.is_err() {
                                break; // Channel closed.
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

/// Worker unit: runs N concurrent task executors consuming from the prefetcher.
pub struct WorkerUnit {
    queue: Arc<Queue>,
    prefetcher: Prefetcher,
    concurrency: usize,
}

impl WorkerUnit {
    pub fn new(
        queue: Arc<Queue>,
        table_name: &str,
        worker_id: &str,
        batch_size: i32,
        concurrency: usize,
    ) -> Self {
        Self {
            queue: queue.clone(),
            prefetcher: Prefetcher::new(queue, table_name, worker_id, batch_size),
            concurrency,
        }
    }

    /// Starts the worker unit: prefetcher + N worker tasks.
    /// Returns when the token is cancelled and all tasks drain.
    pub async fn run(&self, token: CancellationToken) {
        // async-channel supports multiple receivers without Mutex.
        let (tx, rx) = async_channel::bounded::<Task>(self.concurrency * 2);

        // Start prefetcher.
        let pf_token = token.clone();
        let pf_handle = {
            let prefetcher_queue = self.prefetcher.queue.clone();
            let table_name = self.prefetcher.table_name.clone();
            let worker_id = self.prefetcher.worker_id.clone();
            let batch_size = self.prefetcher.batch_size;
            tokio::spawn(async move {
                let pf = Prefetcher::new(prefetcher_queue, &table_name, &worker_id, batch_size);
                pf.run(pf_token, tx).await;
            })
        };

        // Start N worker tasks (each gets its own rx clone — MPMC).
        let mut workers = JoinSet::new();
        for _ in 0..self.concurrency {
            let rx = rx.clone();
            let queue = self.queue.clone();
            workers.spawn(async move {
                while let Ok(task) = rx.recv().await {
                    execute_task(&queue, task).await;
                }
            });
        }

        // Wait for prefetcher to finish (cancelled).
        let _ = pf_handle.await;

        // Two-phase shutdown: wait for workers to drain or timeout.
        let drain = async { while workers.join_next().await.is_some() {} };
        tokio::select! {
            _ = drain => {
                tracing::info!("workers drained cleanly");
            }
            _ = tokio::time::sleep(DRAIN_TIMEOUT) => {
                tracing::warn!("worker drain timeout, aborting remaining tasks");
                workers.abort_all();
            }
        }
    }
}

/// Executes a single task.
async fn execute_task(queue: &Queue, task: Task) {
    // Validate version.
    if task.version != TASK_PAYLOAD_VERSION {
        tracing::error!(
            task_id = task.task_id,
            version = task.version,
            "version mismatch"
        );
        let _ = queue.fail_task(task.task_id).await;
        return;
    }

    // Unmarshal payload.
    let payload = match unmarshal_payload(&task.input) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(task_id = task.task_id, error = %e, "unmarshal failed");
            let _ = queue.fail_task(task.task_id).await;
            return;
        }
    };

    let cons = match &payload.consolidation {
        Some(c) => c,
        None => {
            tracing::error!(task_id = task.task_id, "missing consolidation data");
            let _ = queue.fail_task(task.task_id).await;
            return;
        }
    };

    // TODO: actually create archive (download IR, compress, upload).
    // For now, just complete with a placeholder result.
    tracing::info!(
        task_id = task.task_id,
        ir_paths = cons.ir_paths.len(),
        archive_path = %cons.archive_path,
        "executing consolidation task (placeholder)"
    );

    let result = TaskResult {
        archive_path: cons.archive_path.clone(),
        archive_size_bytes: 0,
        error: String::new(),
        created_at: metalog_timeutil::epoch_nanos(),
    };

    match marshal_result(&result) {
        Ok(output) => {
            let _ = queue.complete_task(task.task_id, &output).await;
        }
        Err(e) => {
            tracing::error!(task_id = task.task_id, error = %e, "marshal result failed");
            let _ = queue.fail_task(task.task_id).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_timeout() {
        assert_eq!(DRAIN_TIMEOUT, Duration::from_secs(30));
    }

    #[test]
    fn poll_constants() {
        assert_eq!(POLL_INTERVAL, Duration::from_secs(2));
        assert_eq!(MAX_BACKOFF, Duration::from_secs(30));
    }
}
