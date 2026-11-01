use std::sync::Arc;

use metalog_metastore::{AdvisoryLockError, FileRecords, StorageMutationGuard, StoragePath};
use metalog_storage::Registry;
use sqlx::{Connection, Row};

use crate::{
    unmarshal_payload,
    unmarshal_result,
    ConsolidationPayload,
    PlannerError,
    Queue,
    Task,
    TaskResult,
    TASK_PAYLOAD_VERSION,
};

pub(crate) struct Finalizer {
    pub queue: Arc<Queue>,
    pub files: Arc<FileRecords>,
    pub storage: Arc<Registry>,
}

impl Finalizer {
    pub async fn run_once(&self) -> Result<(), PlannerError> {
        for task in self.queue.terminal_tasks(self.files.table_name()).await? {
            let mut lock = match self.queue.task_lock(task.task_id).await {
                Ok(lock) => lock,
                Err(AdvisoryLockError::NotAcquired(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            if let Err(error) = self.finish(&task).await {
                tracing::warn!(
                    task_id = task.task_id, %error,
                    "consolidation finalization deferred"
                );
            }
            lock.release().await?;
        }
        Ok(())
    }

    async fn finish(&self, task: &Task) -> Result<(), PlannerError> {
        let payload = unmarshal_payload(&task.input)?;
        let input = payload
            .consolidation
            .as_ref()
            .ok_or_else(|| PlannerError::Invalid("missing consolidation payload".into()))?;
        input.validate()?;
        if task.version != TASK_PAYLOAD_VERSION || payload.table_name != task.table_name {
            return Err(PlannerError::Invalid(
                "task version or table mismatch".into(),
            ));
        }
        if task.state == "failed" {
            self.queue.retry_task(task.task_id, "failed").await?;
            return Ok(());
        }
        if task.state == "completed" {
            let result = task
                .output
                .as_deref()
                .map(unmarshal_result)
                .transpose()?
                .ok_or_else(|| PlannerError::Invalid("completed task has no result".into()))?;
            if !result.error.is_empty() {
                self.queue.retry_task(task.task_id, "completed").await?;
                return Ok(());
            }
            if (result.archive_path != input.attempt_path(task.task_id)
                && result.archive_path != input.archive_path)
                || result.archive_size_bytes <= 0
                || result.created_at <= 0
            {
                return Err(PlannerError::Invalid(
                    "archive result does not match its attempt".into(),
                ));
            }
            self.queue.ensure_reservations(task, input).await?;
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM _consolidation_file WHERE task_id = ?")
                    .bind(task.task_id)
                    .fetch_one(self.queue.pool())
                    .await?;
            if count != input.file_ids.len() as i64 {
                return Err(PlannerError::Invalid(
                    "missing durable source reservations".into(),
                ));
            }
            let unpublished: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM _consolidation_file WHERE task_id = ? AND released = FALSE",
            )
            .bind(task.task_id)
            .fetch_one(self.queue.pool())
            .await?;
            if unpublished > 0 {
                let backend = self.backend(&input.archive_backend)?;
                if !backend
                    .exists(&input.archive_bucket, &result.archive_path)
                    .await?
                {
                    // Nothing has been published yet. Retain the sources and
                    // rebuild under a new attempt identity if the object vanished.
                    self.queue.retry_task(task.task_id, "completed").await?;
                    return Ok(());
                }
                self.publish(task, input, &result).await?;
            }
            // Released reservations record the commit, even if retention has
            // since removed the metadata. Never infer an orphan from UPDATE=0.
            for (index, path) in input.ir_paths.iter().enumerate() {
                self.delete_unreferenced(&StoragePath {
                    backend: input.ir_backend.clone(),
                    bucket: input.bucket(index).into(),
                    path: path.clone(),
                })
                .await?;
            }
            // Handles a group whose rows were removed administratively before publication.
            self.delete_unreferenced(&StoragePath {
                backend: input.archive_backend.clone(),
                bucket: input.archive_bucket.clone(),
                path: result.archive_path,
            })
            .await?;
            sqlx::query("DELETE FROM _task_queue WHERE task_id = ? AND state = 'completed'")
                .bind(task.task_id)
                .execute(self.queue.pool())
                .await?;
            tracing::info!(task_id = task.task_id, "consolidation finalized");
        } else if matches!(task.state.as_str(), "timed_out" | "dead_letter") {
            self.delete_unreferenced(&StoragePath {
                backend: input.archive_backend.clone(),
                bucket: input.archive_bucket.clone(),
                path: input.attempt_path(task.task_id),
            })
            .await?;
            // Retain failed attempt destinations for a day, retrying deletion on
            // every pass in case a disconnected upload completed late. Dead
            // letters retain reservations for diagnosis/manual retry indefinitely.
            if task.state == "timed_out" {
                sqlx::query(
                    "DELETE FROM _task_queue WHERE task_id = ? AND state = 'timed_out' AND \
                     completed_at < ?",
                )
                .bind(task.task_id)
                .bind(metalog_timeutil::epoch_nanos() - 86_400_000_000_000i64)
                .execute(self.queue.pool())
                .await?;
            }
        }
        Ok(())
    }

    fn backend(&self, name: &str) -> Result<Arc<dyn metalog_storage::Backend>, PlannerError> {
        self.storage
            .get(name)
            .ok_or_else(|| PlannerError::Invalid(format!("unknown storage backend {name}")))
    }

    async fn delete_unreferenced(&self, path: &StoragePath) -> Result<(), PlannerError> {
        let backend = self.backend(&path.backend)?;
        self.storage
            .validate_location(&path.backend, &path.bucket, &path.path)?;
        if self.files.retire_storage_path(path, i64::MIN).await? {
            match backend.delete(&path.bucket, &path.path).await {
                Ok(()) | Err(metalog_storage::StorageError::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    async fn publish(
        &self,
        task: &Task,
        input: &ConsolidationPayload,
        result: &TaskResult,
    ) -> Result<(), PlannerError> {
        let mut guard = StorageMutationGuard::acquire(self.queue.pool()).await?;
        guard
            .ensure_writable(&[StoragePath {
                backend: input.archive_backend.clone(),
                bucket: input.archive_bucket.clone(),
                path: result.archive_path.clone(),
            }])
            .await?;
        let mut tx = guard.connection().begin().await?;
        let rows = sqlx::query(
            "SELECT min_timestamp, file_id, CAST(backend AS CHAR) AS backend, CAST(bucket AS \
             CHAR) AS bucket, path FROM _consolidation_file WHERE task_id = ? FOR UPDATE",
        )
        .bind(task.task_id)
        .fetch_all(&mut *tx)
        .await?;
        let table = metalog_db::quote_identifier(&task.table_name);
        for row in rows {
            let id: i64 = row.try_get("file_id")?;
            let timestamp: i64 = row.try_get("min_timestamp")?;
            let backend: String = row.try_get("backend")?;
            let bucket: String = row.try_get("bucket")?;
            let path: String = row.try_get("path")?;
            if backend != input.ir_backend
                || !input.file_ids.iter().enumerate().any(|(i, file_id)| {
                    *file_id == id && input.ir_paths[i] == path && input.bucket(i) == bucket
                })
            {
                return Err(PlannerError::Invalid(
                    "reservation does not match task input".into(),
                ));
            }
            // Preserve each row's dimensions, sketches, time bounds and source
            // provenance. Never update by path or auto-increment ID alone.
            sqlx::query(&format!(
                "UPDATE {table} SET state = 'ARCHIVE_CLOSED', archive_storage_backend = ?, \
                 archive_bucket = ?, archive_path = ?, archive_size_bytes = ?, archive_created_at \
                 = ? WHERE min_timestamp = ? AND id = ? AND state IN \
                 ('IR_ARCHIVE_CONSOLIDATION_PENDING', 'FILE_ARCHIVE_CONSOLIDATION_PENDING') AND \
                 BINARY file_storage_backend = BINARY ? AND BINARY file_bucket = BINARY ? AND \
                 BINARY file_path = BINARY ?"
            ))
            .bind(&input.archive_backend)
            .bind(&input.archive_bucket)
            .bind(&result.archive_path)
            .bind(result.archive_size_bytes)
            .bind(result.created_at)
            .bind(timestamp)
            .bind(id)
            .bind(&backend)
            .bind(&bucket)
            .bind(&path)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("UPDATE _consolidation_file SET released = TRUE WHERE task_id = ?")
            .bind(task.task_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        guard.release().await?;
        Ok(())
    }
}
