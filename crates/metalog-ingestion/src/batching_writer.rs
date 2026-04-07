use std::{collections::HashMap, sync::Arc, time::Duration};

use metalog_schema::ColumnRegistry;
use metalog_types::FileRecord;
use sqlx::MySqlPool;
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinSet,
};

/// Default batch size (records per flush).
const DEFAULT_BATCH_SIZE: usize = 5000;

/// Default flush interval.
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// Channel capacity per table.
const CHANNEL_CAPACITY: usize = 5000;

/// Error returned when the per-table channel is full (non-blocking submit).
#[derive(Debug, thiserror::Error)]
#[error("channel full for table {0}")]
pub struct ChannelFullError(pub String);

/// Batches file records per table and flushes to the database.
///
/// Lazily creates one tokio task per active table on first record arrival.
/// Each task batches records and flushes when:
/// - Batch size is reached (default 5000), or
/// - Timer fires (default 1 second)
///
/// Premium processors (aggs, sketches) are injected as `Option<Arc<dyn T>>`
/// and called during the flush pipeline if present.
pub struct BatchingWriter {
    db: MySqlPool,
    is_mariadb: bool,
    writers: Arc<RwLock<HashMap<String, mpsc::Sender<FileRecord>>>>,
    registries: Arc<RwLock<HashMap<String, Arc<ColumnRegistry>>>>,
    join_set: Arc<tokio::sync::Mutex<JoinSet<()>>>,
    batch_size: usize,
    flush_interval: Duration,
}

impl BatchingWriter {
    /// Creates a new `BatchingWriter`.
    pub fn new(db: MySqlPool, is_mariadb: bool) -> Self {
        Self {
            db,
            is_mariadb,
            writers: Arc::new(RwLock::new(HashMap::new())),
            registries: Arc::new(RwLock::new(HashMap::new())),
            join_set: Arc::new(tokio::sync::Mutex::new(JoinSet::new())),
            batch_size: DEFAULT_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        }
    }

    /// Sets the batch size.
    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }

    /// Sets the flush interval.
    pub fn with_flush_interval(mut self, d: Duration) -> Self {
        self.flush_interval = d;
        self
    }

    /// Associates a column registry with a table.
    pub async fn set_registry(&self, table_name: &str, registry: Arc<ColumnRegistry>) {
        let mut regs = self.registries.write().await;
        regs.insert(table_name.to_string(), registry);
    }

    /// Non-blocking submit. Returns `ChannelFullError` if the channel is full.
    pub async fn submit(&self, table_name: &str, rec: FileRecord) -> Result<(), ChannelFullError> {
        let tx = self.get_or_create_writer(table_name).await;
        tx.try_send(rec)
            .map_err(|_| ChannelFullError(table_name.to_string()))
    }

    /// Blocking submit. Waits until channel has space or context is cancelled.
    pub async fn submit_wait(
        &self,
        table_name: &str,
        rec: FileRecord,
    ) -> Result<(), ChannelFullError> {
        let tx = self.get_or_create_writer(table_name).await;
        tx.send(rec)
            .await
            .map_err(|_| ChannelFullError(table_name.to_string()))
    }

    /// Stops all writer tasks and waits for completion.
    pub async fn stop(&self) {
        // Drop all senders to signal tasks to drain and exit.
        {
            let mut writers = self.writers.write().await;
            writers.clear();
        }

        // Wait for all tasks to complete.
        let mut js = self.join_set.lock().await;
        while js.join_next().await.is_some() {}
    }

    async fn get_or_create_writer(&self, table_name: &str) -> mpsc::Sender<FileRecord> {
        // Fast path: read lock.
        {
            let writers = self.writers.read().await;
            if let Some(tx) = writers.get(table_name) {
                return tx.clone();
            }
        }

        // Slow path: write lock + spawn task.
        let mut writers = self.writers.write().await;

        // Double-check under write lock.
        if let Some(tx) = writers.get(table_name) {
            return tx.clone();
        }

        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        writers.insert(table_name.to_string(), tx.clone());

        let table = table_name.to_string();
        let db = self.db.clone();
        let batch_size = self.batch_size;
        let flush_interval = self.flush_interval;
        let is_mariadb = self.is_mariadb;
        let registries = self.registries.clone();

        let mut js = self.join_set.lock().await;
        js.spawn(table_writer_loop(
            table,
            db,
            rx,
            registries,
            batch_size,
            flush_interval,
            is_mariadb,
        ));

        tx
    }
}

/// Per-table writer loop: batches records and flushes to DB.
async fn table_writer_loop(
    table_name: String,
    db: MySqlPool,
    mut rx: mpsc::Receiver<FileRecord>,
    registries: Arc<RwLock<HashMap<String, Arc<ColumnRegistry>>>>,
    batch_size: usize,
    flush_interval: Duration,
    _is_mariadb: bool,
) {
    let mut batch: Vec<FileRecord> = Vec::with_capacity(batch_size);
    let mut interval = tokio::time::interval(flush_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    tracing::info!(table = %table_name, "table writer started");

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Some(rec) => {
                        batch.push(rec);
                        if batch.len() >= batch_size {
                            flush_batch(&table_name, &db, &registries, &mut batch).await;
                        }
                    }
                    None => {
                        // Channel closed — drain remaining records and exit.
                        if !batch.is_empty() {
                            flush_batch(&table_name, &db, &registries, &mut batch).await;
                        }
                        break;
                    }
                }
            }
            _ = interval.tick() => {
                if !batch.is_empty() {
                    flush_batch(&table_name, &db, &registries, &mut batch).await;
                }
            }
        }
    }

    tracing::info!(table = %table_name, "table writer stopped");
}

/// Maximum rows per INSERT statement (MySQL parameter limit).
const MAX_ROWS_PER_INSERT: usize = 1000;

/// Flushes a batch of records to the database via multi-row guarded UPSERT.
///
/// 1. Resolves dim keys → physical columns via ColumnRegistry (if available)
/// 2. Builds multi-row INSERT with guarded ON DUPLICATE KEY UPDATE
/// 3. Executes in chunks of 1000 rows
async fn flush_batch(
    table_name: &str,
    db: &MySqlPool,
    registries: &Arc<RwLock<HashMap<String, Arc<ColumnRegistry>>>>,
    batch: &mut Vec<FileRecord>,
) {
    let count = batch.len();
    tracing::debug!(table = table_name, count, "flushing batch");

    // Resolve dim columns if registry is available.
    let registry = {
        let regs = registries.read().await;
        regs.get(table_name).cloned()
    };

    // Collect unique dim keys and resolve to physical columns.
    let mut dim_mappings: Vec<(String, String)> = Vec::new(); // (logical_key, physical_col)
    if let Some(reg) = &registry {
        let mut seen_keys = std::collections::HashSet::new();
        for rec in batch.iter() {
            for meta in &rec.dim_meta {
                if seen_keys.insert(meta.key.clone()) {
                    match reg
                        .resolve_or_allocate_dim(&meta.key, &meta.base_type, meta.width)
                        .await
                    {
                        Ok(col) => dim_mappings.push((meta.key.clone(), col)),
                        Err(e) => {
                            tracing::warn!(dim_key = %meta.key, error = %e, "dim allocation failed");
                        }
                    }
                }
            }
        }
    }

    // Build and execute INSERT in chunks.
    for chunk in batch.chunks(MAX_ROWS_PER_INSERT) {
        if let Err(e) = execute_upsert(db, table_name, chunk, &dim_mappings).await {
            tracing::error!(table = table_name, error = %e, "upsert failed");
        }
    }

    batch.clear();
}

/// Builds and executes a multi-row INSERT with guarded ON DUPLICATE KEY UPDATE.
async fn execute_upsert(
    db: &MySqlPool,
    table_name: &str,
    records: &[FileRecord],
    dim_mappings: &[(String, String)],
) -> Result<(), sqlx::Error> {
    if records.is_empty() {
        return Ok(());
    }

    // Base columns always present in INSERT.
    let mut col_names: Vec<&str> = vec![
        "min_timestamp",
        "max_timestamp",
        "state",
        "clp_ir_storage_backend",
        "clp_ir_bucket",
        "clp_ir_path",
        "clp_ir_size_bytes",
        "clp_archive_storage_backend",
        "clp_archive_bucket",
        "clp_archive_path",
        "clp_archive_size_bytes",
        "clp_archive_created_at",
        "raw_size_bytes",
        "record_count",
        "retention_days",
        "expires_at",
    ];

    // Add dim columns.
    let dim_col_strings: Vec<String> = dim_mappings.iter().map(|(_, col)| col.clone()).collect();
    for col in &dim_col_strings {
        col_names.push(col);
    }

    let cols_sql = col_names
        .iter()
        .map(|c| format!("`{c}`"))
        .collect::<Vec<_>>()
        .join(", ");

    let params_per_row = col_names.len();
    let placeholders_row = format!(
        "({})",
        (0..params_per_row)
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ")
    );
    let all_placeholders = (0..records.len())
        .map(|_| placeholders_row.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    // Guarded ON DUPLICATE KEY UPDATE (MariaDB VALUES() syntax).
    let update_cols: Vec<&str> = vec![
        "max_timestamp",
        "state",
        "clp_ir_storage_backend",
        "clp_ir_bucket",
        "clp_ir_size_bytes",
        "clp_archive_storage_backend",
        "clp_archive_bucket",
        "clp_archive_path",
        "clp_archive_size_bytes",
        "clp_archive_created_at",
        "raw_size_bytes",
        "record_count",
    ];

    let guard_states = metalog_types::FileState::upsert_guard_states()
        .iter()
        .map(|s| format!("'{}'", s.as_db_str()))
        .collect::<Vec<_>>()
        .join(",");

    let mut update_parts: Vec<String> = Vec::new();
    for col in &update_cols {
        update_parts.push(format!(
            "`{col}` = IF(`state` NOT IN ({guard_states}) AND VALUES(`max_timestamp`) > \
             `max_timestamp`, VALUES(`{col}`), `{col}`)"
        ));
    }
    // Dim columns also guarded.
    for col in &dim_col_strings {
        update_parts.push(format!(
            "`{col}` = IF(`state` NOT IN ({guard_states}) AND VALUES(`max_timestamp`) > \
             `max_timestamp`, VALUES(`{col}`), `{col}`)"
        ));
    }

    let sql = format!(
        "INSERT INTO `{table_name}` ({cols_sql}) VALUES {all_placeholders} ON DUPLICATE KEY \
         UPDATE {}",
        update_parts.join(", ")
    );

    // Bind parameters.
    let mut query = sqlx::query(&sql);
    for rec in records {
        query = query
            .bind(rec.min_timestamp)
            .bind(rec.max_timestamp)
            .bind(rec.state.as_db_str())
            .bind(&rec.clp_ir_storage_backend)
            .bind(&rec.clp_ir_bucket)
            .bind(&rec.clp_ir_path)
            .bind(rec.clp_ir_size_bytes)
            .bind(&rec.clp_archive_storage_backend)
            .bind(&rec.clp_archive_bucket)
            .bind(&rec.clp_archive_path)
            .bind(rec.clp_archive_size_bytes)
            .bind(rec.clp_archive_created_at)
            .bind(rec.raw_size_bytes)
            .bind(rec.record_count)
            .bind(rec.retention_days)
            .bind(rec.expires_at);

        // Bind dim values in the same order as dim_mappings.
        for (logical_key, _) in dim_mappings {
            let val = rec.dims.get(logical_key);
            match val {
                Some(serde_json::Value::String(s)) => query = query.bind(Some(s.as_str())),
                Some(serde_json::Value::Number(n)) => {
                    query = query.bind(n.as_i64());
                }
                Some(serde_json::Value::Bool(b)) => query = query.bind(Some(*b)),
                _ => query = query.bind(Option::<String>::None),
            }
        }
    }

    query.execute(db).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_full_error_display() {
        let err = ChannelFullError("test_table".into());
        assert!(err.to_string().contains("test_table"));
    }

    #[test]
    fn default_batch_size() {
        assert_eq!(DEFAULT_BATCH_SIZE, 5000);
    }

    #[test]
    fn default_flush_interval() {
        assert_eq!(DEFAULT_FLUSH_INTERVAL, Duration::from_secs(1));
    }

    #[test]
    fn channel_capacity() {
        assert_eq!(CHANNEL_CAPACITY, 5000);
    }
}
