use std::time::Duration;

use metalog_encoding::{marshal, unmarshal};
use metalog_timeutil::epoch_nanos;
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;

/// Task state in the queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Processing,
    Completed,
    Failed,
    TimedOut,
    DeadLetter,
}

impl TaskState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::DeadLetter => "dead_letter",
        }
    }
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A task in the queue.
#[derive(Debug, Clone)]
pub struct Task {
    pub task_id: i64,
    pub table_name: String,
    pub state: String,
    pub input: Vec<u8>,
    pub output: Option<Vec<u8>>,
    pub version: u8,
    pub retry_count: u8,
}

/// Task payload (LZ4+msgpack serialized).
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskPayload {
    pub table_name: String,
    pub consolidation: Option<ConsolidationPayload>,
}

/// Consolidation-specific task data.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsolidationPayload {
    pub ir_backend: String,
    pub ir_paths: Vec<String>,
    pub ir_buckets: Vec<String>,
    pub archive_backend: String,
    pub archive_bucket: String,
    pub archive_path: String,
    pub file_ids: Vec<i64>,
    pub min_timestamp: i64,
}

/// Task result (LZ4+msgpack serialized).
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskResult {
    pub archive_path: String,
    pub archive_size_bytes: i64,
    pub error: String,
    pub created_at: i64,
}

/// Current payload version.
pub const TASK_PAYLOAD_VERSION: u8 = 1;

/// Default max retries before dead_letter.
const MAX_RETRIES: u8 = 3;

/// Database-backed task queue with FOR UPDATE SKIP LOCKED claiming.
pub struct Queue {
    db: MySqlPool,
}

impl Queue {
    pub fn new(db: MySqlPool) -> Self {
        Self { db }
    }

    /// Maximum rows per multi-row INSERT to stay within `max_allowed_packet`.
    const MAX_ROWS_PER_INSERT: usize = 100;

    /// Creates tasks in batch (all pending) using multi-row INSERT.
    pub async fn create_tasks(
        &self,
        table_name: &str,
        version: u8,
        inputs: &[Vec<u8>],
    ) -> Result<u64, sqlx::Error> {
        if inputs.is_empty() {
            return Ok(0);
        }
        let now = epoch_nanos();
        let mut total = 0u64;

        for chunk in inputs.chunks(Self::MAX_ROWS_PER_INSERT) {
            let placeholders: String = chunk
                .iter()
                .map(|_| "(?, 'pending', ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "INSERT INTO _task_queue (table_name, state, version, input, created_at) VALUES \
                 {placeholders}"
            );

            let mut query = sqlx::query(&sql);
            for input in chunk {
                query = query.bind(table_name).bind(version).bind(input).bind(now);
            }
            let result = query.execute(&self.db).await?;
            total += result.rows_affected();
        }

        Ok(total)
    }

    /// Claims up to `batch_size` pending tasks using FOR UPDATE SKIP LOCKED.
    pub async fn claim_tasks(
        &self,
        table_name: &str,
        worker_id: &str,
        batch_size: i32,
    ) -> Result<Vec<Task>, sqlx::Error> {
        let now = epoch_nanos();

        // Use a dedicated connection for the transaction.
        let mut conn = self.db.acquire().await?;

        // Begin transaction with READ COMMITTED.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *conn)
            .await?;
        sqlx::query("START TRANSACTION").execute(&mut *conn).await?;

        // SELECT FOR UPDATE SKIP LOCKED.
        let rows: Vec<(i64, Vec<u8>, u8, u8)> = sqlx::query_as(
            "SELECT task_id, input, version, retry_count FROM _task_queue WHERE table_name = ? \
             AND state = 'pending' ORDER BY created_at ASC LIMIT ? FOR UPDATE SKIP LOCKED",
        )
        .bind(table_name)
        .bind(batch_size)
        .fetch_all(&mut *conn)
        .await?;

        if rows.is_empty() {
            sqlx::query("ROLLBACK").execute(&mut *conn).await?;
            return Ok(vec![]);
        }

        // Update claimed tasks.
        let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let placeholders: String = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let update_sql = format!(
            "UPDATE _task_queue SET state = 'processing', worker_id = ?, claimed_at = ? WHERE \
             task_id IN ({placeholders})"
        );
        let mut query = sqlx::query(&update_sql).bind(worker_id).bind(now);
        for id in &ids {
            query = query.bind(id);
        }
        query.execute(&mut *conn).await?;

        sqlx::query("COMMIT").execute(&mut *conn).await?;

        Ok(rows
            .into_iter()
            .map(|(task_id, input, version, retry_count)| Task {
                task_id,
                table_name: table_name.to_string(),
                state: "processing".to_string(),
                input,
                output: None,
                version,
                retry_count,
            })
            .collect())
    }

    /// Marks a task as completed with output.
    pub async fn complete_task(&self, task_id: i64, output: &[u8]) -> Result<u64, sqlx::Error> {
        let now = epoch_nanos();
        let result = sqlx::query(
            "UPDATE _task_queue SET state = 'completed', output = ?, completed_at = ? WHERE \
             task_id = ? AND state = 'processing'",
        )
        .bind(output)
        .bind(now)
        .bind(task_id)
        .execute(&self.db)
        .await?;
        Ok(result.rows_affected())
    }

    /// Marks a task as failed (increments retry, dead-letters if max retries).
    pub async fn fail_task(&self, task_id: i64) -> Result<u64, sqlx::Error> {
        let now = epoch_nanos();
        let result = sqlx::query(
            "UPDATE _task_queue SET state = IF(retry_count + 1 >= ?, 'dead_letter', 'failed'), \
             retry_count = retry_count + 1, completed_at = ? WHERE task_id = ? AND state = \
             'processing'",
        )
        .bind(MAX_RETRIES)
        .bind(now)
        .bind(task_id)
        .execute(&self.db)
        .await?;
        Ok(result.rows_affected())
    }

    /// Finds stale processing tasks (older than timeout).
    pub async fn find_stale_tasks(
        &self,
        table_name: &str,
        timeout: Duration,
    ) -> Result<Vec<Task>, sqlx::Error> {
        let threshold = epoch_nanos() - timeout.as_nanos() as i64;
        let rows: Vec<(i64, Vec<u8>, u8, u8)> = sqlx::query_as(
            "SELECT task_id, input, version, retry_count FROM _task_queue WHERE table_name = ? \
             AND state = 'processing' AND claimed_at < ?",
        )
        .bind(table_name)
        .bind(threshold)
        .fetch_all(&self.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(task_id, input, version, retry_count)| Task {
                task_id,
                table_name: table_name.to_string(),
                state: "processing".to_string(),
                input,
                output: None,
                version,
                retry_count,
            })
            .collect())
    }

    /// Reclaims a stale task (re-enqueues as pending or dead-letters).
    pub async fn reclaim_task(&self, task_id: i64) -> Result<(), sqlx::Error> {
        let now = epoch_nanos();
        // If retry_count >= MAX_RETRIES → dead_letter, else → pending.
        sqlx::query(
            "UPDATE _task_queue SET state = IF(retry_count >= ?, 'dead_letter', 'pending'), \
             worker_id = NULL, claimed_at = NULL, completed_at = ? WHERE task_id = ? AND state = \
             'processing'",
        )
        .bind(MAX_RETRIES)
        .bind(now)
        .bind(task_id)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    /// Counts active (pending + processing) tasks.
    pub async fn count_active_tasks(&self, table_name: &str) -> Result<i64, sqlx::Error> {
        let (count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM _task_queue WHERE table_name = ? AND state IN ('pending', \
             'processing')",
        )
        .bind(table_name)
        .fetch_one(&self.db)
        .await?;
        Ok(count)
    }

    /// Cleans up old terminal tasks.
    pub async fn cleanup_old_tasks(
        &self,
        table_name: &str,
        max_age: Duration,
    ) -> Result<u64, sqlx::Error> {
        let threshold = epoch_nanos() - max_age.as_nanos() as i64;
        let result = sqlx::query(
            "DELETE FROM _task_queue WHERE table_name = ? AND state IN ('completed', 'failed', \
             'timed_out') AND completed_at < ? LIMIT 1000",
        )
        .bind(table_name)
        .bind(threshold)
        .execute(&self.db)
        .await?;
        Ok(result.rows_affected())
    }
}

/// Serializes a task payload.
pub fn marshal_payload(p: &TaskPayload) -> Result<Vec<u8>, metalog_encoding::CodecError> {
    marshal(p)
}

/// Deserializes a task payload.
pub fn unmarshal_payload(data: &[u8]) -> Result<TaskPayload, metalog_encoding::CodecError> {
    unmarshal(data)
}

/// Serializes a task result.
pub fn marshal_result(r: &TaskResult) -> Result<Vec<u8>, metalog_encoding::CodecError> {
    marshal(r)
}

/// Deserializes a task result.
pub fn unmarshal_result(data: &[u8]) -> Result<TaskResult, metalog_encoding::CodecError> {
    unmarshal(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_roundtrip() {
        let payload = TaskPayload {
            table_name: "test".into(),
            consolidation: Some(ConsolidationPayload {
                ir_backend: "s3".into(),
                ir_paths: vec!["/a.ir".into(), "/b.ir".into()],
                ir_buckets: vec!["logs".into(), "logs".into()],
                archive_backend: "s3".into(),
                archive_bucket: "archives".into(),
                archive_path: "/out.clp".into(),
                file_ids: vec![1, 2],
                min_timestamp: 1000,
            }),
        };

        let encoded = marshal_payload(&payload).unwrap();
        let decoded = unmarshal_payload(&encoded).unwrap();
        assert_eq!(decoded.table_name, "test");
        let cons = decoded.consolidation.unwrap();
        assert_eq!(cons.ir_paths.len(), 2);
        assert_eq!(cons.archive_path, "/out.clp");
    }

    #[test]
    fn result_roundtrip() {
        let result = TaskResult {
            archive_path: "/out.clp".into(),
            archive_size_bytes: 12345,
            error: String::new(),
            created_at: 99999,
        };

        let encoded = marshal_result(&result).unwrap();
        let decoded = unmarshal_result(&encoded).unwrap();
        assert_eq!(decoded.archive_size_bytes, 12345);
    }

    #[test]
    fn task_state_strings() {
        assert_eq!(TaskState::Pending.as_str(), "pending");
        assert_eq!(TaskState::DeadLetter.as_str(), "dead_letter");
    }
}
