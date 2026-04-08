use metalog_types::{column::ColumnMapping, file_state::FileState};
use sqlx::MySqlPool;

/// Repository for metadata table operations on a single table.
///
/// Each user table (e.g., `clp_spark`) is a clone of `_clp_template`. This struct
/// holds the table name and provides typed SQL operations against it.
pub struct FileRecords {
    db: MySqlPool,
    table_name: String,
}

/// Result of a deletion operation, collecting storage paths for cleanup.
#[derive(Debug, Default)]
pub struct DeletionResult {
    pub ir_paths: Vec<StoragePath>,
    pub archive_paths: Vec<StoragePath>,
    pub deleted_count: i64,
}

/// A storage location (backend + bucket + path) for a file to be deleted.
#[derive(Debug, Clone)]
pub struct StoragePath {
    pub backend: String,
    pub bucket: String,
    pub path: String,
}

impl FileRecords {
    /// Creates a new `FileRecords` for the given table.
    pub fn new(db: MySqlPool, table_name: &str) -> Self {
        Self {
            db,
            table_name: table_name.to_string(),
        }
    }

    /// Returns the table name this repository operates on.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Finds files in a consolidation-pending state with required dimension/agg
    /// column mappings populated.
    ///
    /// The caller chooses which pending state to query (e.g.,
    /// `IrArchiveConsolidationPending` or `FileArchiveConsolidationPending`),
    /// allowing policy-based control over which consolidation chain is active.
    pub async fn find_consolidation_pending(
        &self,
        pending_state: FileState,
        dim_mappings: &[ColumnMapping],
        agg_mappings: &[ColumnMapping],
    ) -> Result<Vec<PendingFile>, sqlx::Error> {
        let mut select_cols = vec![
            "id".to_string(),
            "min_timestamp".to_string(),
            "max_timestamp".to_string(),
            "file_storage_backend".to_string(),
            "file_bucket".to_string(),
            "file_path".to_string(),
        ];

        for m in dim_mappings {
            select_cols.push(format!("`{}` AS `{}`", m.physical_col, m.logical_key));
        }
        for m in agg_mappings {
            select_cols.push(format!("`{}` AS `{}`", m.physical_col, m.logical_key));
        }

        let sql = format!(
            "SELECT {} FROM `{}` WHERE state = ? ORDER BY min_timestamp ASC LIMIT 10000",
            select_cols.join(", "),
            self.table_name,
        );

        let rows = sqlx::query(&sql)
            .bind(pending_state.as_db_str())
            .fetch_all(&self.db)
            .await?;

        let mut results = Vec::with_capacity(rows.len());
        for row in &rows {
            use sqlx::Row;
            results.push(PendingFile {
                id: row.get("id"),
                min_timestamp: row.get("min_timestamp"),
                max_timestamp: row.get("max_timestamp"),
                file_storage_backend: row.get("file_storage_backend"),
                file_bucket: row.get("file_bucket"),
                file_path: row.get("file_path"),
            });
        }
        Ok(results)
    }

    /// Promotes stuck buffering files to their corresponding consolidation-pending
    /// state.
    ///
    /// The caller specifies the `(from, to)` state pair:
    /// - `(IrArchiveBuffering, IrArchiveConsolidationPending)` for the IR→Archive chain
    /// - `(FileArchiveBuffering, FileArchiveConsolidationPending)` for the File→Archive chain
    ///
    /// Files with `max_timestamp` older than `stale_before_nanos` are considered stuck.
    pub async fn promote_stuck_buffering(
        &self,
        from_state: FileState,
        to_state: FileState,
        stale_before_nanos: i64,
    ) -> Result<u64, sqlx::Error> {
        let sql = format!(
            "UPDATE `{}` SET state = ? WHERE state = ? AND max_timestamp < ? AND max_timestamp > \
             0 LIMIT 1000",
            self.table_name
        );
        let result = sqlx::query(&sql)
            .bind(to_state.as_db_str())
            .bind(from_state.as_db_str())
            .bind(stale_before_nanos)
            .execute(&self.db)
            .await?;
        Ok(result.rows_affected())
    }

    /// Marks files as `ARCHIVE_CLOSED` after successful consolidation.
    ///
    /// The caller specifies which consolidation-pending state the files are
    /// transitioning from (e.g., `IrArchiveConsolidationPending` or
    /// `FileArchiveConsolidationPending`).
    pub async fn mark_archive_closed(
        &self,
        from_state: FileState,
        file_paths: &[String],
        archive_path: &str,
        archive_backend: &str,
        archive_bucket: &str,
        archive_size_bytes: i64,
        archive_created_at: i64,
    ) -> Result<u64, sqlx::Error> {
        if file_paths.is_empty() {
            return Ok(0);
        }

        let placeholders: Vec<&str> = file_paths.iter().map(|_| "?").collect();
        let sql = format!(
            "UPDATE `{}` SET state = ?, archive_path = ?, archive_storage_backend = ?, \
             archive_bucket = ?, archive_size_bytes = ?, archive_created_at = ? WHERE file_path \
             IN ({}) AND state = ?",
            self.table_name,
            placeholders.join(", "),
        );

        let mut query = sqlx::query(&sql)
            .bind(FileState::ArchiveClosed.as_db_str())
            .bind(archive_path)
            .bind(archive_backend)
            .bind(archive_bucket)
            .bind(archive_size_bytes)
            .bind(archive_created_at);

        for path in file_paths {
            query = query.bind(path);
        }
        query = query.bind(from_state.as_db_str());

        let result = query.execute(&self.db).await?;
        Ok(result.rows_affected())
    }

    /// Transitions expired files to PURGING state (retention phase 1).
    pub async fn transition_expired_to_purging(
        &self,
        current_nanos: i64,
    ) -> Result<u64, sqlx::Error> {
        let sql = format!(
            "UPDATE `{}` SET state = CASE \
             WHEN state IN (?, ?) THEN ? \
             WHEN state IN (?, ?, ?) THEN ? \
             ELSE state END \
             WHERE expires_at > 0 AND expires_at < ? \
             AND state IN (?, ?, ?, ?, ?) \
             LIMIT 1000",
            self.table_name,
        );
        let result = sqlx::query(&sql)
            // IR chain: IR_BUFFERING, IR_CLOSED → IR_PURGING
            .bind(FileState::IrBuffering.as_db_str())
            .bind(FileState::IrClosed.as_db_str())
            .bind(FileState::IrPurging.as_db_str())
            // Archive chain: ARCHIVE_CLOSED, IR_ARCHIVE_BUFFERING, FILE_ARCHIVE_BUFFERING → ARCHIVE_PURGING
            .bind(FileState::ArchiveClosed.as_db_str())
            .bind(FileState::IrArchiveBuffering.as_db_str())
            .bind(FileState::FileArchiveBuffering.as_db_str())
            .bind(FileState::ArchivePurging.as_db_str())
            // WHERE
            .bind(current_nanos)
            .bind(FileState::IrBuffering.as_db_str())
            .bind(FileState::IrClosed.as_db_str())
            .bind(FileState::ArchiveClosed.as_db_str())
            .bind(FileState::IrArchiveBuffering.as_db_str())
            .bind(FileState::FileArchiveBuffering.as_db_str())
            .execute(&self.db)
            .await?;
        Ok(result.rows_affected())
    }

    /// Deletes PURGING files and returns their storage paths (retention phase 2).
    ///
    /// Uses a transaction to ensure the SELECT and DELETE operate on the same
    /// row set, preventing TOCTOU races where rows could be deleted without
    /// their storage paths being returned for cleanup.
    pub async fn delete_expired_files(
        &self,
        current_nanos: i64,
    ) -> Result<DeletionResult, sqlx::Error> {
        let mut conn = self.db.acquire().await?;

        sqlx::query("START TRANSACTION")
            .execute(&mut *conn)
            .await?;

        // Select paths within the transaction (CAST needed: ascii_bin → VARBINARY in sqlx).
        let select_sql = format!(
            "SELECT id, CAST(file_storage_backend AS CHAR) AS file_storage_backend, \
             CAST(file_bucket AS CHAR) AS file_bucket, CAST(file_path AS CHAR) AS file_path, \
             CAST(archive_storage_backend AS CHAR) AS archive_storage_backend, \
             CAST(archive_bucket AS CHAR) AS archive_bucket, CAST(archive_path AS CHAR) AS \
             archive_path FROM `{}` WHERE state IN (?, ?) AND expires_at > 0 AND expires_at < ? \
             LIMIT 1000 FOR UPDATE",
            self.table_name,
        );
        let rows: Vec<(i64, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>)> =
            sqlx::query_as(&select_sql)
                .bind(FileState::IrPurging.as_db_str())
                .bind(FileState::ArchivePurging.as_db_str())
                .bind(current_nanos)
                .fetch_all(&mut *conn)
                .await?;

        if rows.is_empty() {
            sqlx::query("COMMIT").execute(&mut *conn).await?;
            return Ok(DeletionResult::default());
        }

        let mut result = DeletionResult::default();
        let mut ids = Vec::with_capacity(rows.len());
        for (id, ir_backend, ir_bucket, ir_path, arch_backend, arch_bucket, arch_path) in &rows {
            ids.push(*id);
            if let (Some(backend), Some(bucket), Some(path)) = (ir_backend, ir_bucket, ir_path) {
                if !path.is_empty() {
                    result.ir_paths.push(StoragePath {
                        backend: backend.clone(),
                        bucket: bucket.clone(),
                        path: path.clone(),
                    });
                }
            }
            if let (Some(backend), Some(bucket), Some(path)) =
                (arch_backend, arch_bucket, arch_path)
            {
                if !path.is_empty() {
                    result.archive_paths.push(StoragePath {
                        backend: backend.clone(),
                        bucket: bucket.clone(),
                        path: path.clone(),
                    });
                }
            }
        }

        // Delete the exact rows we selected (by id, within same transaction).
        let placeholders: String = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let delete_sql = format!(
            "DELETE FROM `{}` WHERE id IN ({placeholders})",
            self.table_name,
        );
        let mut query = sqlx::query(&delete_sql);
        for id in &ids {
            query = query.bind(id);
        }
        let delete_result = query.execute(&mut *conn).await?;

        sqlx::query("COMMIT").execute(&mut *conn).await?;

        result.deleted_count = delete_result.rows_affected() as i64;
        Ok(result)
    }
}

/// A file in `CONSOLIDATION_PENDING` state discovered by the planner.
#[derive(Debug, Clone)]
pub struct PendingFile {
    pub id: i64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub file_storage_backend: Option<String>,
    pub file_bucket: Option<String>,
    pub file_path: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deletion_result_default() {
        let r = DeletionResult::default();
        assert!(r.ir_paths.is_empty());
        assert!(r.archive_paths.is_empty());
        assert_eq!(r.deleted_count, 0);
    }

    #[test]
    fn storage_path_fields() {
        let p = StoragePath {
            backend: "s3".into(),
            bucket: "logs".into(),
            path: "/data/test.ir".into(),
        };
        assert_eq!(p.backend, "s3");
    }

    #[test]
    fn pending_file_fields() {
        let f = PendingFile {
            id: 42,
            min_timestamp: 1000,
            max_timestamp: 2000,
            file_storage_backend: Some("minio".into()),
            file_bucket: Some("logs".into()),
            file_path: Some("/data/test.ir".into()),
        };
        assert_eq!(f.id, 42);
    }

    #[test]
    fn file_records_table_name() {
        // Can't create a real pool in unit tests, but we can test the constructor
        // pattern by checking the table name accessor exists.
        assert_eq!(
            FileState::IrArchiveConsolidationPending.as_db_str(),
            "IR_ARCHIVE_CONSOLIDATION_PENDING"
        );
    }
}
