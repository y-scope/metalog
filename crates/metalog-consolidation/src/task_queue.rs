use std::time::Duration;

use metalog_encoding::{marshal_frame as marshal, unmarshal_frame as unmarshal};
use metalog_metastore::{AdvisoryLock, StorageMutationGuard, StoragePath};
use metalog_timeutil::epoch_nanos;
use metalog_types::FileRecord;
use serde::{Deserialize, Serialize};
use sqlx::{Connection, MySqlPool, Row};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskPayload {
    pub table_name: String,
    pub consolidation: Option<ConsolidationPayload>,
}

/// Consolidation-specific task data.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
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
    cleanup_cursors: std::sync::Mutex<std::collections::HashMap<String, i64>>,
}

impl Queue {
    /// Maximum rows per multi-row INSERT to stay within `max_allowed_packet`.
    const MAX_ROWS_PER_INSERT: usize = 100;

    pub fn new(db: MySqlPool) -> Self {
        Self {
            db,
            cleanup_cursors: Default::default(),
        }
    }

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
        if batch_size <= 0 {
            return Ok(Vec::new());
        }
        let mut tx = self.db.begin().await?;
        let rows = sqlx::query(
            "SELECT task_id, table_name, input, version, retry_count FROM _task_queue WHERE (? = \
             '' OR table_name = ?) AND state = 'pending' ORDER BY task_id LIMIT ? FOR UPDATE SKIP \
             LOCKED",
        )
        .bind(table_name)
        .bind(table_name)
        .bind(batch_size)
        .fetch_all(&mut *tx)
        .await?;
        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id: i64 = row.try_get("task_id")?;
            sqlx::query(
                "UPDATE _task_queue SET state = 'processing', worker_id = ?, claimed_at = ? WHERE \
                 task_id = ?",
            )
            .bind(worker_id)
            .bind(epoch_nanos())
            .bind(task_id)
            .execute(&mut *tx)
            .await?;
            tasks.push(Task {
                task_id,
                table_name: row.try_get("table_name")?,
                state: "processing".into(),
                input: row.try_get("input")?,
                output: None,
                version: row.try_get("version")?,
                retry_count: row.try_get("retry_count")?,
            });
        }
        tx.commit().await?;
        Ok(tasks)
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

    /// Records failure; the coordinator atomically schedules a new attempt or
    /// dead-letters it after the retry limit.
    pub async fn fail_task(&self, task_id: i64) -> Result<u64, sqlx::Error> {
        Ok(sqlx::query(
            "UPDATE _task_queue SET state = 'failed', completed_at = ? WHERE task_id = ? AND \
             state = 'processing'",
        )
        .bind(epoch_nanos())
        .bind(task_id)
        .execute(&self.db)
        .await?
        .rows_affected())
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
        self.retry_task(task_id, "processing").await?;
        Ok(())
    }

    /// Each retry has its own ID and destination. A late completion cannot win
    /// against the replacement attempt. Reservations move in the same transaction.
    pub async fn retry_task(
        &self,
        task_id: i64,
        expected_state: &str,
    ) -> Result<Option<i64>, sqlx::Error> {
        let mut tx = self.db.begin().await?;
        let row = sqlx::query(
            "SELECT table_name, input, version, retry_count FROM _task_queue WHERE task_id = ? \
             AND state = ? FOR UPDATE",
        )
        .bind(task_id)
        .bind(expected_state)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let retries: u8 = row.try_get("retry_count")?;
        let terminal = if retries >= MAX_RETRIES {
            "dead_letter"
        } else {
            "timed_out"
        };
        sqlx::query("UPDATE _task_queue SET state = ?, completed_at = ? WHERE task_id = ?")
            .bind(terminal)
            .bind(epoch_nanos())
            .bind(task_id)
            .execute(&mut *tx)
            .await?;
        let new_id = if retries < MAX_RETRIES {
            let result = sqlx::query(
                "INSERT INTO _task_queue (table_name, state, input, version, retry_count, \
                 created_at) VALUES (?, 'pending', ?, ?, ?, ?)",
            )
            .bind(row.try_get::<String, _>("table_name")?)
            .bind(row.try_get::<Vec<u8>, _>("input")?)
            .bind(row.try_get::<u8, _>("version")?)
            .bind(retries + 1)
            .bind(epoch_nanos())
            .execute(&mut *tx)
            .await?;
            let id = result.last_insert_id() as i64;
            sqlx::query("UPDATE _consolidation_file SET task_id = ? WHERE task_id = ?")
                .bind(id)
                .bind(task_id)
                .execute(&mut *tx)
                .await?;
            Some(id)
        } else {
            None
        };
        tx.commit().await?;
        if let Some(next) = new_id {
            tracing::warn!(
                task_id,
                next_task_id = next,
                retry_count = retries + 1,
                "consolidation retry scheduled"
            );
        } else {
            tracing::error!(task_id, "consolidation retries exhausted; dead-lettered");
        }
        Ok(new_id)
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.db
    }

    pub async fn task_lock(
        &self,
        id: i64,
    ) -> Result<AdvisoryLock, metalog_metastore::AdvisoryLockError> {
        AdvisoryLock::acquire_scoped(&self.db, "consolidation_task", &id.to_string(), 0).await
    }

    /// Atomically reserve exact file identities and enqueue work. Retention and
    /// concurrent planners cannot retire or reserve these inputs between checks.
    pub async fn create_reserved_task(
        &self,
        payload: &TaskPayload,
        records: &[FileRecord],
    ) -> Result<Option<i64>, crate::PlannerError> {
        let data = marshal_payload(payload)?;
        let mut guard = StorageMutationGuard::acquire(&self.db).await?;
        let paths: Vec<_> = records
            .iter()
            .map(|r| StoragePath {
                backend: r.file_storage_backend.clone().unwrap_or_default(),
                bucket: r.file_bucket.clone().unwrap_or_default(),
                path: r.file_path.clone().unwrap_or_default(),
            })
            .collect();
        guard.ensure_writable(&paths).await?;
        let mut tx = guard.connection().begin().await?;
        let table = metalog_db::quote_identifier(&payload.table_name);
        for (record, path) in records.iter().zip(&paths) {
            let valid: i64 = sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM {table} WHERE min_timestamp = ? AND id = ? AND state = ? \
                 AND BINARY file_storage_backend = BINARY ? AND BINARY file_bucket = BINARY ? AND \
                 BINARY file_path = BINARY ? AND (expires_at = 0 OR expires_at > ?) FOR UPDATE"
            ))
            .bind(record.min_timestamp)
            .bind(record.id)
            .bind(record.state.as_db_str())
            .bind(&path.backend)
            .bind(&path.bucket)
            .bind(&path.path)
            .bind(epoch_nanos())
            .fetch_one(&mut *tx)
            .await?;
            let reserved: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM _consolidation_file WHERE table_name = ? AND min_timestamp \
                 = ? AND file_id = ?",
            )
            .bind(&payload.table_name)
            .bind(record.min_timestamp)
            .bind(record.id)
            .fetch_one(&mut *tx)
            .await?;
            if valid != 1 || reserved != 0 {
                return Ok(None);
            }
        }
        let result = sqlx::query(
            "INSERT INTO _task_queue (table_name, state, version, input, created_at) VALUES (?, \
             'pending', ?, ?, ?)",
        )
        .bind(&payload.table_name)
        .bind(TASK_PAYLOAD_VERSION)
        .bind(data)
        .bind(epoch_nanos())
        .execute(&mut *tx)
        .await?;
        let id = result.last_insert_id() as i64;
        for (record, path) in records.iter().zip(paths) {
            sqlx::query(
                "INSERT INTO _consolidation_file (table_name, min_timestamp, file_id, task_id, \
                 backend, bucket, path, path_hash) VALUES (?, ?, ?, ?, ?, ?, ?, UNHEX(MD5(?)))",
            )
            .bind(&payload.table_name)
            .bind(record.min_timestamp)
            .bind(record.id)
            .bind(id)
            .bind(path.backend)
            .bind(path.bucket)
            .bind(&path.path)
            .bind(&path.path)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        guard.release().await?;
        Ok(Some(id))
    }

    /// Adopt queued Go v1 tasks before touching storage. Legacy IDs must resolve
    /// unambiguously to a complete primary key and an unchanged pending source.
    pub async fn ensure_reservations(
        &self,
        task: &Task,
        input: &ConsolidationPayload,
    ) -> Result<(), crate::PlannerError> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM _consolidation_file WHERE task_id = ?")
                .bind(task.task_id)
                .fetch_one(&self.db)
                .await?;
        if count == input.file_ids.len() as i64 {
            return Ok(());
        }
        if count != 0 {
            return Err(crate::PlannerError::Invalid(
                "incomplete task reservations".into(),
            ));
        }
        let mut guard = StorageMutationGuard::acquire(&self.db).await?;
        let paths: Vec<_> = input
            .ir_paths
            .iter()
            .enumerate()
            .map(|(index, path)| StoragePath {
                backend: input.ir_backend.clone(),
                bucket: input.bucket(index).into(),
                path: path.clone(),
            })
            .collect();
        guard.ensure_writable(&paths).await?;
        let mut tx = guard.connection().begin().await?;
        let table = metalog_db::quote_identifier(&task.table_name);
        for (index, path) in paths.iter().enumerate() {
            let timestamps: Vec<i64> = sqlx::query_scalar(&format!(
                "SELECT min_timestamp FROM {table} WHERE id = ? AND state IN \
                 ('IR_ARCHIVE_CONSOLIDATION_PENDING','FILE_ARCHIVE_CONSOLIDATION_PENDING') AND \
                 BINARY file_storage_backend = BINARY ? AND BINARY file_bucket = BINARY ? AND \
                 BINARY file_path = BINARY ? FOR UPDATE"
            ))
            .bind(input.file_ids[index])
            .bind(&path.backend)
            .bind(&path.bucket)
            .bind(&path.path)
            .fetch_all(&mut *tx)
            .await?;
            if timestamps.len() != 1 {
                return Err(crate::PlannerError::Invalid(
                    "legacy source identity is missing or ambiguous".into(),
                ));
            }
            sqlx::query(
                "INSERT INTO _consolidation_file (table_name, min_timestamp, file_id, task_id, \
                 backend, bucket, path, path_hash) VALUES (?, ?, ?, ?, ?, ?, ?, UNHEX(MD5(?)))",
            )
            .bind(&task.table_name)
            .bind(timestamps[0])
            .bind(input.file_ids[index])
            .bind(task.task_id)
            .bind(&path.backend)
            .bind(&path.bucket)
            .bind(&path.path)
            .bind(&path.path)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        guard.release().await?;
        Ok(())
    }

    pub async fn terminal_tasks(&self, table_name: &str) -> Result<Vec<Task>, sqlx::Error> {
        let cursor = *self
            .cleanup_cursors
            .lock()
            .expect("cursor mutex")
            .get(table_name)
            .unwrap_or(&0);
        let fetch = |after| {
            sqlx::query(
                "SELECT task_id, table_name, state, input, output, version, retry_count FROM \
                 _task_queue WHERE table_name = ? AND task_id > ? AND state IN ('completed', \
                 'failed', 'timed_out', 'dead_letter') ORDER BY task_id LIMIT 32",
            )
            .bind(table_name)
            .bind(after)
            .fetch_all(&self.db)
        };
        let mut rows = fetch(cursor).await?;
        if rows.is_empty() && cursor > 0 {
            rows = fetch(0).await?;
        }
        let next = if rows.len() < 32 {
            0
        } else {
            rows.last().expect("nonempty page").try_get("task_id")?
        };
        self.cleanup_cursors
            .lock()
            .expect("cursor mutex")
            .insert(table_name.into(), next);
        rows.into_iter()
            .map(|row| {
                Ok(Task {
                    task_id: row.try_get("task_id")?,
                    table_name: row.try_get("table_name")?,
                    state: row.try_get("state")?,
                    input: row.try_get("input")?,
                    output: row.try_get("output")?,
                    version: row.try_get("version")?,
                    retry_count: row.try_get("retry_count")?,
                })
            })
            .collect()
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
    fn reads_go_payload_and_omitted_error_result() {
        let payload = unmarshal_payload(include_bytes!(
            "../../../tests/fixtures/consolidation/go-payload.lz4"
        ))
        .unwrap();
        assert_eq!(payload.table_name, "go_tasks");
        let input = payload.consolidation.unwrap();
        input.validate().unwrap();
        assert_eq!(input.file_ids, [1, 2]);
        assert_eq!(input.bucket(1), "logs");
        let result = unmarshal_result(include_bytes!(
            "../../../tests/fixtures/consolidation/go-result.lz4"
        ))
        .unwrap();
        assert!(result.error.is_empty());
        assert_eq!(result.archive_size_bytes, 321);
        assert_eq!(
            unmarshal_result(include_bytes!(
                "../../../tests/fixtures/consolidation/go-error.lz4"
            ))
            .unwrap()
            .error,
            "compression failed"
        );
    }

    #[test]
    fn task_state_strings() {
        assert_eq!(TaskState::Pending.as_str(), "pending");
        assert_eq!(TaskState::DeadLetter.as_str(), "dead_letter");
    }
}
