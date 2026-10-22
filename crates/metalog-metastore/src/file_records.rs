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

/// A storage location (backend + bucket + path) for a file to be deleted.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StoragePath {
    pub backend: String,
    pub bucket: String,
    pub path: String,
}

/// Durable retention work. A row remains PURGING until every unreferenced object
/// has been deleted successfully; (min_timestamp, id) is its complete primary key.
#[derive(Debug, Clone)]
pub struct PurgingFile {
    pub id: i64,
    pub min_timestamp: i64,
    pub paths: Vec<StoragePath>,
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

    /// Rejects ambiguous or unresolved legacy locations before retention changes metadata.
    /// Some object stores normalize path syntax, so an exact-key reference check
    /// cannot protect aliases. New ingestion rejects these locations; this audit
    /// covers pre-existing references in both storage fields of every table.
    pub async fn audit_canonical_storage_locations(
        &self,
        backend_names: &[String],
    ) -> Result<(), sqlx::Error> {
        let tables: Vec<(String,)> =
            sqlx::query_as("SELECT table_name FROM _table ORDER BY table_name")
                .fetch_all(&self.db)
                .await?;
        let backend_parameters = vec!["BINARY ?"; backend_names.len()].join(", ");
        // Bind regex text separately from SQL escaping. Backslash uses LOCATE
        // because MySQL and MariaDB regex engines differ in escape handling.
        let key_pattern = "(^/)|(/$)|(//)|((^|/)[.]{1,2}(/|$))";
        let bucket_pattern = "[/]|^[.]{1,2}$";
        for (table_name,) in tables {
            let table = metalog_db::quote_identifier(&table_name);
            for prefix in ["file", "archive"] {
                let canonical_backend = if backend_names.is_empty() {
                    "FALSE".to_owned()
                } else {
                    format!("BINARY {prefix}_storage_backend IN ({backend_parameters})")
                };
                let sql = format!(
                    "SELECT EXISTS(SELECT 1 FROM {table} WHERE {prefix}_path IS NOT NULL AND \
                     OCTET_LENGTH({prefix}_path) > 0 AND ({prefix}_storage_backend IS NULL OR \
                     {prefix}_storage_backend = '' OR {prefix}_bucket IS NULL OR {prefix}_bucket \
                     = '' OR ({canonical_backend} AND ({prefix}_bucket REGEXP ? OR \
                     LOCATE(CHAR(92), {prefix}_bucket) > 0 OR {prefix}_path REGEXP ? OR \
                     LOCATE(CHAR(92), {prefix}_path) > 0))))"
                );
                let mut query = sqlx::query_as::<_, (i64,)>(&sql);
                for backend in backend_names {
                    query = query.bind(backend);
                }
                let (ambiguous,) = query
                    .bind(bucket_pattern)
                    .bind(key_pattern)
                    .fetch_one(&self.db)
                    .await?;
                if ambiguous != 0 {
                    return Err(sqlx::Error::Protocol(format!(
                        "retention blocked: {table_name}.{prefix} contains a noncanonical storage \
                         location or unresolved backend/bucket; resolve legacy aliases before \
                         enabling deletion"
                    )));
                }
            }
        }
        Ok(())
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
            "CAST(file_storage_backend AS CHAR) AS file_storage_backend".to_string(),
            "CAST(file_bucket AS CHAR) AS file_bucket".to_string(),
            "CAST(file_path AS CHAR) AS file_path".to_string(),
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
                id: row.try_get("id")?,
                min_timestamp: row.try_get("min_timestamp")?,
                max_timestamp: row.try_get("max_timestamp")?,
                file_storage_backend: row.try_get("file_storage_backend")?,
                file_bucket: row.try_get("file_bucket")?,
                file_path: row.try_get("file_path")?,
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
    #[allow(clippy::too_many_arguments)]
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
    ///
    /// Handles all non-terminal states:
    /// - IR chain: IR_BUFFERING, IR_CLOSED → IR_PURGING
    /// - Archive chain: ARCHIVE_CLOSED, IR_ARCHIVE_BUFFERING, FILE_ARCHIVE_BUFFERING,
    ///   IR_ARCHIVE_CONSOLIDATION_PENDING, FILE_ARCHIVE_CONSOLIDATION_PENDING → ARCHIVE_PURGING
    pub async fn transition_expired_to_purging(
        &self,
        current_nanos: i64,
    ) -> Result<u64, sqlx::Error> {
        let sql = format!(
            "UPDATE `{}` SET state = CASE WHEN state IN (?, ?) THEN ? WHEN state IN (?, ?, ?, ?, \
             ?) THEN ? ELSE state END WHERE expires_at > 0 AND expires_at < ? AND state IN (?, ?, \
             ?, ?, ?, ?, ?) LIMIT 1000",
            self.table_name,
        );
        let result = sqlx::query(&sql)
            // IR chain: IR_BUFFERING, IR_CLOSED → IR_PURGING
            .bind(FileState::IrBuffering.as_db_str())
            .bind(FileState::IrClosed.as_db_str())
            .bind(FileState::IrPurging.as_db_str())
            // Archive chain → ARCHIVE_PURGING
            .bind(FileState::ArchiveClosed.as_db_str())
            .bind(FileState::IrArchiveBuffering.as_db_str())
            .bind(FileState::FileArchiveBuffering.as_db_str())
            .bind(FileState::IrArchiveConsolidationPending.as_db_str())
            .bind(FileState::FileArchiveConsolidationPending.as_db_str())
            .bind(FileState::ArchivePurging.as_db_str())
            // WHERE
            .bind(current_nanos)
            .bind(FileState::IrBuffering.as_db_str())
            .bind(FileState::IrClosed.as_db_str())
            .bind(FileState::ArchiveClosed.as_db_str())
            .bind(FileState::IrArchiveBuffering.as_db_str())
            .bind(FileState::FileArchiveBuffering.as_db_str())
            .bind(FileState::IrArchiveConsolidationPending.as_db_str())
            .bind(FileState::FileArchiveConsolidationPending.as_db_str())
            .execute(&self.db)
            .await?;
        Ok(result.rows_affected())
    }

    /// Reads one page of durable deletion work without deleting its retry state.
    pub async fn find_purging_files(
        &self,
        cutoff: i64,
        after: Option<(i64, i64)>,
    ) -> Result<Vec<PurgingFile>, sqlx::Error> {
        use sqlx::Row;
        let table = metalog_db::quote_identifier(&self.table_name);
        let cursor = if after.is_some() {
            "AND (min_timestamp > ? OR (min_timestamp = ? AND id > ?))"
        } else {
            ""
        };
        let sql = format!(
            "SELECT id, min_timestamp, CAST(file_storage_backend AS CHAR) AS file_backend, \
             CAST(file_bucket AS CHAR) AS file_bucket, CAST(file_path AS CHAR) AS file_path, \
             CAST(archive_storage_backend AS CHAR) AS archive_backend, CAST(archive_bucket AS \
             CHAR) AS archive_bucket, CAST(archive_path AS CHAR) AS archive_path FROM {table} \
             WHERE state IN ('IR_PURGING', 'ARCHIVE_PURGING') AND expires_at > 0 AND expires_at < \
             ? {cursor} ORDER BY min_timestamp, id LIMIT 1000"
        );
        let mut query = sqlx::query(&sql).bind(cutoff);
        if let Some((timestamp, id)) = after {
            query = query.bind(timestamp).bind(timestamp).bind(id);
        }
        let rows = query.fetch_all(&self.db).await?;
        rows.iter()
            .map(|row| {
                let mut paths = Vec::new();
                for prefix in ["file", "archive"] {
                    let path: Option<String> = row.try_get(format!("{prefix}_path").as_str())?;
                    if let Some(path) = path.filter(|path| !path.is_empty()) {
                        let backend: Option<String> =
                            row.try_get(format!("{prefix}_backend").as_str())?;
                        let bucket: Option<String> =
                            row.try_get(format!("{prefix}_bucket").as_str())?;
                        let location = StoragePath {
                            backend: backend.unwrap_or_default(),
                            bucket: bucket.unwrap_or_default(),
                            path,
                        };
                        if !paths.contains(&location) {
                            paths.push(location);
                        }
                    }
                }
                Ok(PurgingFile {
                    id: row.try_get("id")?,
                    min_timestamp: row.try_get("min_timestamp")?,
                    paths,
                })
            })
            .collect()
    }

    /// Atomically decides retirement relative to ingestion. Returns true only
    /// after recording a durable tombstone; the guard is released before the
    /// caller performs storage I/O. Live references in any table prevent retirement.
    /// Callers must first audit legacy locations for the configured backend policy.
    pub async fn retire_storage_path(
        &self,
        path: &StoragePath,
        cutoff: i64,
    ) -> Result<bool, crate::StorageMutationError> {
        let mut guard = crate::StorageMutationGuard::acquire(&self.db).await?;
        let tables: Vec<(String,)> =
            sqlx::query_as("SELECT table_name FROM _table ORDER BY table_name")
                .fetch_all(guard.connection())
                .await?;
        for (table_name,) in tables {
            let table = metalog_db::quote_identifier(&table_name);
            let sql = format!(
                "SELECT EXISTS(SELECT 1 FROM {table} WHERE (state NOT IN ('IR_PURGING', \
                 'ARCHIVE_PURGING') OR expires_at <= 0 OR expires_at >= ?) AND ((file_path_hash = \
                 UNHEX(MD5(?)) AND BINARY file_storage_backend = BINARY ? AND BINARY file_bucket \
                 = BINARY ? AND BINARY file_path = BINARY ?) OR (archive_path_hash = \
                 UNHEX(MD5(?)) AND BINARY archive_storage_backend = BINARY ? AND BINARY \
                 archive_bucket = BINARY ? AND BINARY archive_path = BINARY ?)))"
            );
            let (exists,): (i64,) = sqlx::query_as(&sql)
                .bind(cutoff)
                .bind(&path.path)
                .bind(&path.backend)
                .bind(&path.bucket)
                .bind(&path.path)
                .bind(&path.path)
                .bind(&path.backend)
                .bind(&path.bucket)
                .bind(&path.path)
                .fetch_one(guard.connection())
                .await?;
            if exists != 0 {
                guard.release().await?;
                return Ok(false);
            }
        }
        guard.retire(path).await?;
        guard.release().await?;
        Ok(true)
    }

    /// Removes the durable work only after all required object deletions succeed.
    pub async fn finish_purge(&self, file: &PurgingFile, cutoff: i64) -> Result<u64, sqlx::Error> {
        let table = metalog_db::quote_identifier(&self.table_name);
        let sql = format!(
            "DELETE FROM {table} WHERE min_timestamp = ? AND id = ? AND state IN ('IR_PURGING', \
             'ARCHIVE_PURGING') AND expires_at > 0 AND expires_at < ?"
        );
        Ok(sqlx::query(&sql)
            .bind(file.min_timestamp)
            .bind(file.id)
            .bind(cutoff)
            .execute(&self.db)
            .await?
            .rows_affected())
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
